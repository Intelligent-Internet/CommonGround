# Repository Guidelines

## HUMAN MAINTAINED SECTION - AI MODIFICATION PROHIBITED (EXCEPT TRANSLATION)
---- BEGIN HUMAN MAINTAINED SECTION ----
The system design principle is: Constrain at the kernel, freedom at the edges.

When working in this repository, follow these core constraints:
* Before any operation, understand the layers of the system.
    * L0 - Kernel Constraints: How different parts communicate with each other
        * NATS
            * JetStream
                * Event propagation - all in-boundary business actions must use Jetstream notifications.
                * Inside the boundary, it serves wakeup, doorbell, replay, and idempotency functions.
                * Keep audit replay retention short-lived only; do not keep long-term retention.
                * Minimize semantic payloads inside the boundary.
                * Semantic payloads are allowed when communicating outside the boundary (UI/external systems).
            * Core
                * Primarily serves as streaming/data-pipeline, without audit storage.
        * Postgres
            * The system's central ledger; all AGENT and tool activity is protected by DB transaction constraints.
                * Cardbox stores cross-component semantic audit history and internal audit for native system components.
                * Hosts Agent orchestration L0 primitives.
        * The detailed rules are in the L0 documentation under `docs`.
* Recommended operating mode
    * If your goal is to understand system design, start from L0.
    * If you develop applications or communicate with the system, understand L1 and L2.
    * If you are developing in this repo
        * Prioritize developing Workers and Tools first (see generic worker/OpenAPI Tool implementations).
        * Prefer existing L1 scheduling primitives.
        * If L1 scheduling primitives cannot meet requirements, use L0 primitives to implement L1 logic.
        * Unless absolutely necessary, application developers should avoid touching the L0 protocol.
        * You can contribute L0-related code, but any semantic-protocol changes should wait until at least the next minor protocol release.
            * Large behavioral changes should wait for a major version release.

---- END HUMAN MAINTAINED SECTION ----

## Project Structure & Modules
- Core Python code now lives under `core/` (protocol/constants) and `infra/` (NATS/CardBox/LLM clients); deployable services under `services/` (`pmo`, `agent_worker`, `ui_worker`, `api`, plus tool services in `services/tools/`).
- Domain docs are in `docs/` (architecture, principles, specs). Ops scripts sit in `scripts/` (`setup/` for initialization and maintenance, `admin/` for operations tooling, `benchmarks/` for load testing).
- Legacy prototypes were removed; current code of record is under `core/`, `infra/`, and `services/`. Card storage SDK/API remains in `card-box-cg/` with its own tests.
- Configuration defaults: copy `config.toml.sample` to `config.toml` and adjust `[nats]`, `[cardbox]`, DB DSNs, and tenant IDs.

## Minimal Local Run (Docker)
- For the fastest end-to-end validation with minimal host dependencies, prefer Docker Compose first. See `docs/EN/01_getting_started/docker_quickstart_demo_principal_fullflow_api.md`.

## Build, Test, and Development Commands
- Install deps: `uv sync` (root requires Python 3.13+; card-box-cg targets 3.8+).
- Run orchestrator: `uv run -m services.pmo`; run worker: `uv run -m services.agent_worker`; optional UI worker/API: `uv run -m services.ui_worker.loop`, `uv run -m services.api`.
- Bootstrap infra: `docker compose up -d nats postgres`; init DB: `uv run -m scripts.setup.reset_db`; seed baseline data: `uv run -m scripts.setup.seed`.
- Trigger demo flow: use `examples/quickstarts/` demo scripts (e.g., `uv run -m examples.quickstarts.demo_simple_principal_stream`).
- Inspect runtime stream/state: use `scripts/admin` operations commands (for example `uv run -m scripts.admin.inspect_turn`, `uv run -m scripts.admin.inspect_roster`); see `scripts/README.md` for details.
- Run tests (root): `uv run pytest tests card-box-cg/tests -m "not live"` for routine local validation; run full suite selectively (including `-m live`) when credentials/external deps are ready.

## Coding Style & Naming Conventions
- Python, 4-space indent, type hints expected on public functions. Favor pure functions in `core/` & `infra/`, keep services stateless aside from DB/NATS.
- Use models from `core/utp_protocol.py` and `core/events.py` for cross-service payloads; avoid ad-hoc dicts.
- Subjects/headers should reuse helpers in `core/subject.py`; service entrypoints stay in `services/*/loop.py|service.py`.
- File/module names: snake_case; class names: PascalCase; functions/vars: snake_case; constants: UPPER_SNAKE.

## Commit & Pull Request Guidelines
- PRs should state intent, entrypoints touched, and any required infra (NATS/Postgres). Link issues/tasks; include screenshots/log snippets when behavior changes.
- Add checklist for: configs updated (`config.toml`), migrations/scripts added, and tests run (`uv run pytest` or demo flow).

## Configuration & Operational Tips
- Ensure Postgres and NATS are reachable before starting services; mismatched DSNs will block worker startup.
- For card storage, `card-box-cg` auto-creates schema on first use; prefer setting dedicated roles/DBs as in `README.md`.
- Keep secrets out of config files; rely on env vars for credentials when possible.
- Tool subjects must match the active protocol version (`[protocol].version` in `config.toml`). If upgrading (e.g., `v1r2` → `v1r3`), re-upload tool YAMLs or update `resource.tools.target_subject` in the DB; otherwise tool calls can fail with `nats: no response from stream`.
