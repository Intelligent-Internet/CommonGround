# Quick Start

This page gives an end-to-end path: environment preparation -> initialization -> starting services -> resource setup -> running the demo. For more protocol and implementation details, see the L0/L1 documentation.

## Compatibility Notes

- The project currently supports Linux/macOS path semantics more completely. If you use Windows, it is recommended to run this guide under **WSL2** or **Docker** to avoid path and process-detection compatibility issues.
- Paths referenced in sandbox/tooling config such as `/tmp/skills-local`, `/var/lib/skills-cache`, and `/proc/<pid>/cmdline` may require additional adaptation on native Windows.

## 1. Environment Preparation

> If this is your first time pulling the repository from GitHub, it is recommended to clone recursively to include the `card-box-cg` submodule:
> `git clone --recursive https://github.com/Intelligent-Internet/CommonGround.git`
> If you already cloned without submodule contents, run from the repository root:
> `git submodule update --init --recursive`

### Dependencies
- Python 3.12+ (main branch is based on 3.12)
- `uv`
- Postgres (recommended 15+)
- NATS (2.10+, with JetStream enabled)

### Install dependencies
```bash
uv sync
```

### Start infrastructure (optional; skip if already available locally)
```bash
docker compose up -d nats postgres
```

### Default model and key strategy (to avoid startup failures)

By default:

- `judge` uses `[judge].model` in `config.toml`; the sample currently sets it to `gemini/gemini-2.5-flash`.
- `mock_search` uses `MOCK_SEARCH_LLM_MODEL` / `MOCK_SEARCH_LLM_PROVIDER` from `services.tools.mock_search`; the sample currently sets them to `gemini/gemini-2.5-flash` + `gemini`.

If you do not have `GEMINI_API_KEY`, switch to another model before starting:

```bash
# switch only Judge
export CG__JUDGE__MODEL="gpt-5-mini"  # or moonshot/kimi-k2.5
export OPENAI_API_KEY="..."

# switch only mock_search
export MOCK_SEARCH_LLM_PROVIDER="openai"
export MOCK_SEARCH_LLM_MODEL="gpt-5-mini" # or moonshot/kimi-k2.5
export OPENAI_API_KEY="..."
export MOONSHOT_API_KEY="..."
```

You can also edit `config.toml` directly: `[judge].model`, `[tools.mock_search].llm_model`, `[tools.mock_search].llm_provider`.

> Note: `CG__SECTION__KEY` (such as `CG__JUDGE__MODEL`) is synchronized with `config.toml`, and environment variables take precedence.

### Configuration file
```bash
cp config.toml.sample config.toml
```
Edit `[protocol]`, `[nats]`, and `[cardbox]` in `config.toml` as needed, and set the DB DSN.

> Important: `[protocol].version` must be `v1r3` (and match your deployed protocol). If missing or incorrect, NATS subjects may not match and Worker/PMO may not receive messages.

> Port note: default docker-compose ports are NATS 4222 and Postgres 5432; the repository example DSN may use 5433, so rely on your local `config.toml`.

## 2. Initialize Database

> Note: this operation rebuilds `resource/state` related tables and **also clears CardBox data** (including `cards`, `card_boxes`, etc.).

```bash
PG_DSN="postgresql://postgres:postgres@localhost:5433/cardbox" uv run -m scripts.setup.reset_db
uv run -m scripts.setup.seed
```

If your local PostgreSQL is listening on `5432`, use:

```bash
PG_DSN="postgresql://postgres:postgres@localhost:5432/cardbox" uv run -m scripts.setup.reset_db
uv run -m scripts.setup.seed
```

## 3. Start Services

> Demo 2/3 depends on management API and resource setup; starting API in parallel is recommended.

### PMO
```bash
uv run -m services.pmo.service
```

### Worker
```bash
uv run -m services.agent_worker.loop
```

### Management API (optional)
```bash
uv run -m services.api
```

### Mock Tool (optional)
```bash
uv run -m services.tools.mock_search
```

### UI Worker (optional; only for UI Action Demo)
```bash
uv run -m services.ui_worker.loop
```

## 4. Prepare Resources (Recommended)

> 💡 Note: Artifact and Skills APIs depend on Google Cloud Storage (GCS).
> If `[gcs]` is not configured in `config.toml` or valid credentials are missing, the system will gracefully degrade and disable related capabilities such as `/skills:upload` and `/artifacts:upload`.
> If you only want to try the core Agent orchestration flow, you can ignore this limitation for now.

### Method A: Bootstrap project and resources through API
```bash
curl -sS -X POST http://127.0.0.1:8099/projects \
  -H 'Content-Type: application/json' \
  -d '{"project_id":"proj_demo_01","title":"Demo","owner_id":"user_seed","bootstrap":true}'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/associate_search.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/principal_planner_fullflow.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/chat_assistant.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/tools \
  -F file=@examples/tools/web_search_tool.yaml
```

### Method B: Scripted resource readiness
`examples/quickstarts/demo_principal_fullflow.py` enables bootstrap and profile/tool uploads by default; use `--no-ensure-resources` to disable it.

## 5. Run Demos

### Demo 1: Minimal Streaming Output
```bash
uv run -m examples.quickstarts.demo_simple_principal_stream \
  --project proj_demo_01 \
  --channel public \
  --agent demo_stream_01 \
  --question "Hello"
```

### Demo 2: Full Flow (`fork_join` -> `submit_result`)
```bash
uv run -m examples.quickstarts.demo_principal_fullflow \
  --project proj_demo_01 \
  --channel public \
  --profile-name Principal_Planner_FullFlow \
  "help me to do a research on k8s"
```

### Demo 3: `fork_join` Map-Reduce (Word Count)
Start the deterministic tool service first:
```bash
uv run -m services.tools.word_count
```

Then run the online demo (with input text file):
```bash
uv run -m examples.quickstarts.demo_fork_join_word_count \
  --project proj_demo_01 \
  --channel public \
  --text-file /path/to/input.txt
```

### Demo 4: UI Action Chat (new entry)
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"ui_user_demo",
    "profile_name":"UI_Actor_Profile",
    "worker_target":"ui_worker",
    "tags":["ui"],
    "display_name":"UI Session",
    "owner_agent_id":"user_demo",
    "metadata":{"is_ui_agent":true},
    "init_state":true,
    "channel_id":"public"
  }'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"chat_agent_demo",
    "profile_name":"Chat_Assistant",
    "worker_target":"worker_generic",
    "tags":["partner"],
    "display_name":"Chat Agent",
    "owner_agent_id":"user_demo",
    "init_state":true,
    "channel_id":"public"
  }'

uv run -m examples.quickstarts.demo_ui_action \
  --project proj_demo_01 \
  --channel public \
  --ui-agent-id ui_user_demo \
  --chat-agent-id chat_agent_demo
```

## 6. Debug and Observability (Optional)

### 6.1 Subscribe to Streaming Output
```bash
nats sub "cg.v1r3.proj_demo_01.public.str.agent.*.chunk"
```

### 6.2 Agent scheduling inspection (script)
```bash
uv run -m scripts.admin.inspect_turn --project proj_demo_01 --agent-id demo_stream_01
```

## 7. Advanced: Top-level orchestration with `fork_join`

When you need to implement "task decomposition -> parallel execution -> aggregation writeback" inside Principal, refer to `docs/EN/03_kernel_l1/batch_manager.md`.

## 8. Environment Variable Appendix

- `CG_CONFIG_TOML`: specify the config file path (default `./config.toml`).
- `CG__SECTION__KEY`: config override (supports JSON values, highest precedence). Example: `CG__NATS__SERVERS='["nats://nats:4222"]'`.
- `PG_DSN`: override `[cardbox].postgres_dsn` (compatible with scripts/tools services).
- `NATS_SERVERS`: override `[nats].servers` (comma-separated).
- `API_URL` / `PROJECT_ID`: convenience settings for seed scripts.
- `CG_SENDER`: NATS header metadata.
- `CG__PROTOCOL__VERSION`: override protocol version (equivalent to `[protocol].version` in `config.toml`).
- `GEMINI_API_KEY`: API key for the default Gemini workflow (or equivalent provider key usage).
- `CG__JUDGE__MODEL`: override `config.toml` `[judge].model` (for example: `gpt-5-mini`, `moonshot/kimi-k2.5`).
- `MOCK_SEARCH_LLM_MODEL`: override `mock_search` LLM model (for example: `gpt-5-mini`, `moonshot/kimi-k2.5`).
- `MOCK_SEARCH_LLM_PROVIDER`: override `mock_search` provider (for example: `openai`, `moonshot`).
- `OPENAI_API_KEY` / `MOONSHOT_API_KEY`: API keys for corresponding providers.
- `MOCK_SEARCH_DEBUG`: enable debug output for `services.tools.mock_search` (`1/true/yes/on`).
- OpenAPI (Jina) tool key: injected by default through `JINA_API_KEY` (or via `options.jina.auth_env` to specify an env-var name), **do not write this into cards/DB/logs**.

> Advanced: if you want to define provider and model routing directly in a Profile, see the section "Switching LLM providers (LiteLLM support)" in `docs/EN/02_building_agents/defining_profiles.md`.

> Note: by default, `config.toml` is read first; environment variables are used to override config or provide runtime keys for scripts/tools services.
