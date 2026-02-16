# Defining Profiles

This document explains how to write a Profile YAML and the practical constraints of `delegation_policy`.

## 1. Profile YAML and API Structure

### 1.1 Upload Parameters (`POST /projects/{project_id}/profiles`)

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `name` | Yes | - | Profile name. Duplicate names are checked case-insensitively on upload. |
| `worker_target` | Yes | - | Matches worker subscriptions. |
| `tags` | No | `[]` | A list of strings is allowed. If a string is provided, it will be split by `,`. |
| `description` | No | `null` | Used only for context display. |
| `system_prompt_template` | No | `""` | Used as the system prompt template. |
| `system_prompt` | No | none (compatibility alias) | Equivalent to `system_prompt_template`; when both are present, `system_prompt_template` takes precedence. |
| `delegation_policy` | No | `null` | See semantics below. |
| `allowed_tools` | No | `[]` | External tool allowlist. `[]` means no external tools are allowed. |
| `allowed_internal_tools` | No | `[]` | Internal tool allowlist, e.g. `submit_result`. |
| `must_end_with` | No | `[]` | List of required final tools. |
| `llm_config` | No | See defaults below | Initialized by `core.llm.LLMConfig`. |

### 1.2 Returned Model (`Profile`)

After a successful upload, the management API returns `Profile` from `services/api/profile_models.py`, not the complete YAML.

- `project_id`
- `profile_box_id`
- `name`
- `worker_target`
- `tags`
- `system_prompt_compiled`
- `updated_at`

`system_prompt_compiled` is a cached DB value. In the current implementation, it is stored directly as the uploaded `system_prompt_template` without recompilation.

## 2. Minimal Profile

```yaml
name: "Associate_Search"
worker_target: "worker_generic"
tags:
  - "associate"
description: "Search specialist"
system_prompt_template: |
  You are a research assistant.
allowed_tools:
  - "web_search"
allowed_internal_tools: []
must_end_with: []
llm_config:
  provider: "gemini"
  model: "gemini/gemini-2.5-flash"
  temperature: 0.2
delegation_policy:
  target_tags: ["associate", "specialist"]
  target_profiles:
    - "Associate_Search"
```

Reference files: `examples/profiles/*.yaml`

## 2. delegation_policy Semantics

- `target_tags`: Whitelist of delegation target tags (matches any). 
- `target_profiles`: Whitelist by profile name. 
- `target_profiles: null`: No restriction by name (still constrained by tags).
- `target_profiles: []`: Delegation is prohibited.

When there is no `delegation_policy`, `delegation_guard` applies a hard-deny strategy (delegation is not allowed), and `context_loader` receives an empty downstream list.

## 3. Profile Load Path

After uploading YAML, the actual persistence flow is:

- `POST /projects/{project_id}/profiles` accepts YAML and normalizes it through `normalize_profile_yaml`.
- The parsed content is written to a `sys.profile` CardBox card. After `cardbox.save_card`, it is placed in a new box and returns `profile_box_id`.
- `ProfileStore.upsert_profile` writes `project_id/profile_box_id/name/worker_target/tags/system_prompt_template` to `resource.profiles` (the `system_prompt_compiled` field also stores the template text).
- On startup/delegation, it first resolves `profile_name -> profile_box_id`, then reads `resource.profiles` and roster using `profile_box_id`.
- `ResourceLoader.load_profile` then fetches `profile_box_id` from cardbox, selects `sys.profile`, and fills runtime fields.

Delegation-related interfaces also first call `resource.profiles.find_profile_by_name` (case-insensitive name matching) to verify the target profile exists, then follow the same loading path above.

## 4. Common Field Recommendations

- `worker_target`: Must be consistent with target worker subscription configuration.
- `tags`: Used for capability layering; avoid placing environment details here.
- `allowed_tools`: Use least-privilege authorization and avoid broad permissions.
- `must_end_with`: Used to enforce termination behavior (for example, allow only `submit_result` to converge).

## 5. LLM Default Values (`llm_config`)

When `llm_config` is not explicitly provided, defaults from `core/llm.py` apply:

- `model = "gpt-3.5-turbo"`
- `temperature = 1.0`
- `stream = true`
- `api_key = null`
- `api_base = null`
- `max_tokens = null`
- `top_p = null`
- `stop = null`
- `tool_choice = null`
- `save_reasoning_content = true`
- `provider_options = null`
- `stream_options = null`

### Switching LLM Providers (LiteLLM Supported)

The system routes models through LiteLLM. You can switch provider in `llm_config` inside Profile YAML, or override at runtime with environment variables.

Example 1: OpenAI (GPT-5 series)

```yaml
llm_config:
  model: "gpt-5-mini"
  temperature: 0.7
  provider_options:
    custom_llm_provider: "openai"
```

Set before start:

```bash
export OPENAI_API_KEY="sk-..."
```

Example 2: Kimi (Moonshot, OpenAI-compatible)

```yaml
llm_config:
  model: "moonshot/kimi-k2.5"
  provider_options:
    custom_llm_provider: "moonshot"
```

Set before start:

```bash
export MOONSHOT_API_KEY="sk-..."
```

Notes:
- The `model` and provider identifiers must follow a LiteLLM-recognized format.
- When both `config.toml` and environment variable overrides are present, environment variables take precedence.

## 6. Upload and Validation

Upload:
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/<project_id>/profiles \
  -F file=@examples/profiles/associate_search.yaml
```

Query:
```bash
curl -sS http://127.0.0.1:8099/projects/<project_id>/profiles
```

## 7. Further Reading

- API and examples: `docs/EN/01_getting_started/quick_start.md`
- Protocol constraints: `docs/EN/04_protocol_l0/state_machine.md`
