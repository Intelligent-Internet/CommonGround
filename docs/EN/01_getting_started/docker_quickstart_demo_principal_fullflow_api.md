# Quick Start

This page covers the full flow: environment preparation → initialization → service startup → resource setup → run the demo. See the L0/L1 docs for additional protocol and implementation details.

> Topology concepts and patterns are described in `architecture_intro.md` (architecture overview). For implementation and behavior, the L0 protocol document is authoritative.

## 1. Environment Preparation

> If this is your first time pulling the repository from GitHub, use recursive clone to include the `card-box-cg` submodule:
> `git clone --recursive https://github.com/Intelligent-Internet/CommonGround.git`
> If you already cloned but the submodule contents are missing, run in the repo root:
> `git submodule update --init --recursive`

### Dependencies
- Python 3.12+ (repo baseline is based on 3.12)
- `uv`
- Postgres (recommended 15+)
- NATS (2.10+, with JetStream enabled)

> This document defaults to Docker Compose workflow; commands that run `uv` are shown in the `api` container.

### Install dependencies
```bash
uv sync
```

### Start infrastructure (optional, skip if already running locally)
```bash
docker-compose up -d nats postgres
```

### Default model and key strategy (to avoid startup failures)

By default:

- `judge` uses `[judge].model` from `config.toml`, with sample default `gemini/gemini-2.5-flash`.
- `mock_search` uses `MOCK_SEARCH_LLM_MODEL`/`MOCK_SEARCH_LLM_PROVIDER` from `services.tools.mock_search`, with sample default `gemini/gemini-2.5-flash` + `gemini`.

If you have not configured `GEMINI_API_KEY`, switch to another model before starting:

```bash
# Switch only Judge
export CG__JUDGE__MODEL="gpt-5-mini"  # or moonshot/kimi-k2.5
export OPENAI_API_KEY="..."

# Switch only mock_search
export MOCK_SEARCH_LLM_PROVIDER="openai"
export MOCK_SEARCH_LLM_MODEL="gpt-5-mini" # or moonshot/kimi-k2.5
export OPENAI_API_KEY="..."
export MOONSHOT_API_KEY="..."
```

You can also edit `config.toml` directly: `[judge].model`, `[tools.mock_search].llm_model`, `[tools.mock_search].llm_provider`.

> Note: `CG__SECTION__KEY` (such as `CG__JUDGE__MODEL`) is synchronized with `config.toml`, with environment variables taking higher precedence.

### Configuration file
```bash
cp config.toml.sample config.toml
```
Update `[nats]`, `[cardbox]`, DB DSN, and the default project in `config.toml` as needed.

> Important: `[protocol].version` must be `v1r3` (and match deployed protocol). If missing or wrong, NATS subject matching may fail and Worker/PMO might not receive messages.

> Port note: docker-compose defaults are NATS 4222 and Postgres 5432; sample DSN in the repo may use 5433, so use your local `config.toml` as the source of truth.

## 2. Initialize Database

> Note: this rebuilds `resource/state` related tables and will **also clear CardBox data** (`cards`, `card_boxes`, etc.).

```bash
docker compose exec api sh -lc 'uv run -m scripts.setup.reset_db'
docker compose exec api sh -lc 'uv run -m scripts.setup.seed --project proj_demo_01'
```

## 3. Start Services

> Demo 2/3 depends on management API and resource setup, so it is recommended to start the API at the same time.

```bash
docker compose up -d --build
```


## 4. Resource Preparation (recommended)

> 💡 Note: Artifact and Skills APIs depend on Google Cloud Storage (GCS).
> If `[gcs]` is not configured in `config.toml` or valid credentials are missing, the system gracefully degrades and disables related capabilities such as `/skills:upload` and `/artifacts:upload`.
> If you only need to try the main agent orchestration path, you can ignore this for now.

### Method A: bootstrap project and resources via API
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

### Method B: automatic resource bootstrap via script
`examples/quickstarts/demo_principal_fullflow.py` will by default automatically bootstrap and upload profiles/tools. Use `--no-ensure-resources` if you need to disable this.

## 5. Run the Demo

> For this Docker flow, run the commands inside the `api` container by default to avoid host environment differences.

### Demo 1: minimal streaming output
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_simple_principal_stream \
  --project proj_demo_01 \
  --channel public \
  --agent demo_stream_01 \
  --question "Hello"'
```

### Demo 2: full flow (fork_join → submit_result)
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_principal_fullflow \
  --project proj_demo_01 \
  --channel public \
  --profile-name Principal_Planner_FullFlow \
  "help me to do a research on k8s"'
```

### Demo 3: `fork_join` Map-Reduce (Word Count)
Start a deterministic tool service first:
```bash
docker compose exec api sh -lc 'uv run -m services.tools.word_count'
```

Then run the online demo (input a text file):
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_fork_join_word_count \
  --project proj_demo_01 \
  --channel public \
  --text-file /path/to/input.txt'
```

### Demo 4: UI Action chat (new entrypoint)
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

docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_ui_action \
  --project proj_demo_01 \
  --channel public \
  --ui-agent-id ui_user_demo \
  --chat-agent-id chat_agent_demo'
```

## 6. Debug and Observe (optional)

### 6.1 Subscribe to streaming output
```bash
nats sub "cg.v1r3.proj_demo_01.public.str.agent.*.chunk"
```

### 6.2 Observer (terminal UI)
```bash
docker compose exec api sh -lc 'uv run -m scripts.admin.inspect_turn \
  --project proj_demo_01 \
  --agent-id demo_stream_01'
```

> Note: this repo does not include a directly runnable `services.observer` entrypoint; to view real-time stream, keep `nats sub` in 6.1. `scripts.admin.inspect_turn` is suitable for replay/tracing the latest turn and step of a specific agent.

## 7. Advanced: top-level orchestration integration with `fork_join`

When you need to implement “task decomposition → parallel execution → aggregated writeback” in the Principal, refer to `docs/EN/03_kernel_l1/pmo_orchestration.md`.

## 8. Environment Variable Appendix

- `CG_CONFIG_TOML`: path to configuration file (default `./config.toml`).
- `CG__SECTION__KEY`: configuration overrides (supports JSON values, highest priority). Example: `CG__NATS__SERVERS='["nats://nats:4222"]'`.
- `PG_DSN`: override `[cardbox].postgres_dsn` (compatible with script/tool services).
- `NATS_SERVERS`: override `[nats].servers` (comma-separated).
- `API_URL` / `PROJECT_ID`: convenience fields for seed scripts.
- `CG_SENDER`: override default `CG-Sender` header value in NATS.
- `CG__PROTOCOL__VERSION`: override `[protocol].version` (recommended instead of setting in `config.toml`).
- `GEMINI_API_KEY`: default API key for Gemini flow (or equivalent provider key).
- `CG__JUDGE__MODEL`: override `config.toml` `[judge].model` (example: `gpt-5-mini`, `moonshot/kimi-k2.5`).
- `MOCK_SEARCH_LLM_MODEL`: override LLM model for `mock_search` (example: `gpt-5-mini`, `moonshot/kimi-k2.5`).
- `MOCK_SEARCH_LLM_PROVIDER`: override provider for `mock_search` (example: `openai`, `moonshot`).
- `OPENAI_API_KEY` / `MOONSHOT_API_KEY`: API keys for corresponding providers.

> Advanced: if you need to define provider/model routing directly in a Profile, see the "Switching LLM provider (LiteLLM support)" section in `docs/EN/02_building_agents/defining_profiles.md`.
- `MOCK_SEARCH_DEBUG`: enable debug output for `services.tools.mock_search` (`1/true/yes/on`).
- OpenAPI (Jina) tool key: defaults to `JINA_API_KEY` (or specify environment variable name via `options.jina.auth_env`), **do not write this into cards/DB/logs**.

> Note: defaults are loaded from `config.toml`; environment variables are used for overrides or to provide runtime keys for scripts/tools.
