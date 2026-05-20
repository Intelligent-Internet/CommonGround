# Environment Variables

This reference lists environment variables read by the current repository code. Flags and config files can override several of these values; command help remains the closest executable reference.

## Repository-Wide

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `PG_DSN` | `cg service run`, `cg admission run`, `cg setup project`, database setup scripts, demo scripts | None | PostgreSQL DSN. Required for database-backed service, setup, admission, and full tests. |
| `CG_BASE_URL` | CLI clients, Admin Service runner, NanoBot runtimes, demo scripts | `http://127.0.0.1:8000` where a default is provided | CommonGround Service base URL. |
| `CG_PROJECT_ID` | Admin Service runner, NanoBot runtimes, demo scripts | `cg-demo` in local Admin/demo paths; required by NanoBot runtime entrypoints | Project namespace. |

## CommonGround Service

Read by `CommonGround/service/config.py` through `ServiceConfig.from_env()`.

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_HOST` | `127.0.0.1` | Bind host for `cg service run`. |
| `CG_PORT` | `8000` | Bind port. |
| `CG_LOG_LEVEL` | `info` | Uvicorn/service log level. |
| `CG_SERVICE_NAME` | `commonground-v3-service` | Service name in runtime settings. |
| `CG_CLAIM_TIMEOUT_SECONDS` | `30` | Claim lease timeout. |
| `CG_CLAIM_REAPER_INTERVAL_SECONDS` | `0` | Claim reaper interval. |
| `PG_DSN` | None | Required for PostgreSQL-backed service. |
| `CG_AGENT_PROJECT_HEADER` | `X-CG-Project-Id` | Header name used for Agent project identity. |
| `CG_AGENT_ID_HEADER` | `X-CG-Agent-Id` | Header name used for Agent id identity. |

## CLI Client

Read by `CommonGround/cli.py`, `CommonGround/cli_config.py`, and `CommonGround/cli_profiles.py`.

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_CONFIG_PATH` | `~/.config/commonground/config.json` when present or when writing | CLI JSON config path. Flags take precedence. |
| `CG_BASE_URL` | `http://127.0.0.1:8000` | CommonGround Service URL. |
| `CG_AGENT_CREDENTIAL_TOKEN` | None | Direct AgentCredential bearer token for CommonGround requests. |
| `CG_AGENT_CREDENTIAL_TOKEN_FILE` | None | File containing AgentCredential token. File permissions must be `0600`. |
| `CG_CALLER_PROJECT_ID` | None | Caller project id when identity is not inferred from command arguments or profile. |
| `CG_CALLER_AGENT_ID` | None | Caller agent id; must pair with caller project id. |
| `CG_ADMIN_BASE_URL` | None for general CLI, `http://127.0.0.1:8001` in setup client-config defaults | Admin Service URL for profile bootstrap. |
| `CG_ADMIN_AUTH_TOKEN` | None | Direct Admin Service API bearer token. |
| `CG_ADMIN_AUTH_TOKEN_FILE` | Default setup path for local admission/setup flows; none for generic CLI unless config or env supplies it | File containing Admin Service API bearer token. File permissions must be `0600`. |
| `PG_DSN` | None | Used by `cg setup project seed/status`; required unless `--pg-dsn` is passed. |

`cg worker claim run` also injects these variables into the child process:

| Variable | Meaning |
| --- | --- |
| `CG_PROJECT_ID` | Claimed turn project id. |
| `CG_TURN_ID` | Claimed turn id. |
| `CG_AGENT_ID` | Claiming Agent id. |
| `CG_CLAIM_FILE` | Path to JSON claim file. |
| `CG_CONTEXT_FILE` | Path to JSON context file. |
| `CG_CLAIM_TOKEN` | JSON-serialized claim handle. |

## Admin Service Admission

Read by `Integrations/admin_service/admission_runner.py` and setup helpers.

| Variable | Default | Notes |
| --- | --- | --- |
| `PG_DSN` | None | Required for `cg admission run` and project setup. |
| `CG_BASE_URL` | `http://127.0.0.1:8000` | CommonGround Service URL used by the Admin Service Agent. |
| `CG_PROJECT_ID` | `cg-demo` | Project admitted by the local Admin Service API. |
| `CG_ADMIN_SERVICE_TOKEN_FILE` | `~/.local/share/commonground/operator/projects/<project_id>/admin-service.cgac` | File containing the Admin Service AgentCredential. |
| `CG_ADMIN_AUTH_TOKEN_FILE` | `~/.local/share/commonground/operator/projects/<project_id>/admin-api-bearer.token` | File containing the Admin Service API bearer token. |
| `CG_ADMIN_HOST` | `127.0.0.1` | Bind host for `cg admission run`. |
| `CG_ADMIN_PORT` | `8001` | Bind port. |
| `CG_ADMIN_INVITE_CONFIG_JSON` | None | Optional invite config JSON for `byoa.conversation_worker.v1`. |
| `CG_ADMIN_LOG_LEVEL` | `info` | Uvicorn log level. |

## NanoBot Runtime

Read by `Integrations/nanobot/runtime/*` and provision handlers.

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `CG_BASE_URL` | Leaf worker, self-root frontside, runtime clients | `http://127.0.0.1:8000` | CommonGround Service URL. |
| `CG_PROJECT_ID` | Leaf worker, self-root frontside, demo configs | Required by runtime entrypoints | Project id. |
| `CG_AGENT_ID` | Leaf worker, self-root frontside | Required | Runtime Agent id. |
| `CG_AGENT_CREDENTIAL_TOKEN` | Leaf worker, self-root frontside, runtime client auth, demo configs | Required by runtime clients | AgentCredential bearer token. |
| `CG_AGENT_CREDENTIAL_TOKEN_FILE` | Demo NanoBot config fragments | None | Passed through configs for runtimes that resolve token files outside the Python entrypoint. |
| `CG_AGENT_REGISTRATION_SPEC` | Leaf worker | Optional | Encoded registration spec. When present, role, capabilities, grants, and `accepts_work` must match the registered leaf Agent. |
| `CG_PROVISION_TURN_ID` | Leaf worker | Optional | Source provision turn id passed to provisioned leaf workers. |
| `NANOBOT_REPO_ROOT` | Runtime factory, leaf worker, provision launch env | Optional; adjacent checkout is auto-detected only when present | Optional NanoBot source checkout used to override the installed `nanobot` package. |
| `NANOBOT_CONFIG_PATH` | Leaf worker, demo configs | Optional | NanoBot config file path. |
| `NANOBOT_WORKSPACE` | Leaf worker | Optional | Workspace path for the provisioned runtime. |
| `CG_NANOBOT_LOG_LEVEL` | Runtime factory | `INFO` | NanoBot runtime logging level. |
| `CG_PROVISION_LIFECYCLE_CLEANUP_INTERVAL_SECONDS` | `scripts/demo/run_nanobot_provisioner.py` | `30.0` | Provision lifecycle cleanup interval for the demo provisioner. |
| `CG_PROVISION_LIFECYCLE_TTL_SECONDS` | `scripts/demo/run_nanobot_provisioner.py` | None | Optional provision lifecycle TTL for the demo provisioner. |

Provision handlers may construct child runtime environments with `CG_BASE_URL`, `CG_PROJECT_ID`, `CG_AGENT_ID`, `CG_AGENT_CREDENTIAL_TOKEN`, `CG_PROVISION_TURN_ID`, `NANOBOT_REPO_ROOT`, `NANOBOT_CONFIG_PATH`, and `NANOBOT_WORKSPACE`. The process substrate removes `CG_*CLAIM*TOKEN*` variables before launching child processes.

## Demo Scripts And Examples

Read by scripts under `scripts/demo/` and configs under `examples/nanobot/`.

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_DEMO_LOG_LEVEL` | `INFO` | Demo logging level in common demo helpers. |
| `CG_HTTP_LOG_LEVEL` | `WARNING` | HTTP library logging level in common demo helpers. |
| `CG_FRONTSIDE_AGENT_ID` | `frontside` in common demo helpers | Frontside/requester Agent id for demos. |
| `CG_NANOBOT_A_AGENT_ID` | `nanobot_a` | Demo NanoBot A Agent id. |
| `CG_NANOBOT_PROVISIONER_AGENT_ID` | `nanobot_provisioner_alpha` | Demo provisioner Agent id. |
| `NANOBOT_REQUESTER_WORKSPACE` | `~/.nanobot-cg-requester` | Requester workspace for demo helpers. |
| `NANOBOT_A_WORKSPACE` | `~/.nanobot-a` | NanoBot A workspace. |
| `NANOBOT_LEAF_WORKSPACE` | `~/.nanobot-leaf` | Leaf workspace. |
| `NANOBOT_PROVISIONER_WORKSPACE` | `~/.nanobot-provisioner` | Provisioner workspace. |
| `CG_BIN` | Repository `.venv/bin/cg` in the cg-skill roundtrip demo | Override path to the `cg` executable for demo scripts. |
| `CG_AUTH_TOKEN` | None | Allowed through `scripts/demo/send_demo_request.py` requester config; current `cg` CLI auth uses `CG_AGENT_CREDENTIAL_TOKEN`. |
| `CG_AUTH_TOKEN_FILE` | None | Allowed through `scripts/demo/send_demo_request.py` requester config; current `cg` CLI auth uses `CG_AGENT_CREDENTIAL_TOKEN_FILE`. |

Public docs and examples must use placeholders for DSNs, bearer tokens, provider API keys, and workstation-specific paths.
