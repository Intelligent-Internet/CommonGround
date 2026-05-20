# 环境变量

本文列出当前仓库代码读取的环境变量。部分值可被 flags 或 config files 覆盖；command help 仍是最近的 executable reference。

## 仓库通用

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `PG_DSN` | `cg service run`、`cg admission run`、`cg setup project`、database setup scripts、demo scripts | 无 | PostgreSQL DSN。Database-backed service、setup、admission 和 full tests 需要它。 |
| `CG_BASE_URL` | CLI clients、Admin Service runner、NanoBot runtimes、demo scripts | 提供默认时为 `http://127.0.0.1:8000` | CommonGround Service base URL。 |
| `CG_PROJECT_ID` | Admin Service runner、NanoBot runtimes、demo scripts | local Admin/demo paths 中为 `cg-demo`；NanoBot runtime entrypoints 中 required | Project namespace。 |

## CommonGround Service

由 `CommonGround/service/config.py` 的 `ServiceConfig.from_env()` 读取。

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_HOST` | `127.0.0.1` | `cg service run` bind host。 |
| `CG_PORT` | `8000` | Bind port。 |
| `CG_LOG_LEVEL` | `info` | Uvicorn/service log level。 |
| `CG_SERVICE_NAME` | `commonground-v3-service` | Runtime settings 中的 service name。 |
| `CG_CLAIM_TIMEOUT_SECONDS` | `30` | Claim lease timeout。 |
| `CG_CLAIM_REAPER_INTERVAL_SECONDS` | `0` | Claim reaper interval。 |
| `PG_DSN` | 无 | PostgreSQL-backed service 必需。 |
| `CG_AGENT_PROJECT_HEADER` | `X-CG-Project-Id` | Agent project identity 使用的 header name。 |
| `CG_AGENT_ID_HEADER` | `X-CG-Agent-Id` | Agent id identity 使用的 header name。 |

## CLI Client

由 `CommonGround/cli.py`、`CommonGround/cli_config.py` 和 `CommonGround/cli_profiles.py` 读取。

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_CONFIG_PATH` | 存在或写入时为 `~/.config/commonground/config.json` | CLI JSON config path。Flags 优先。 |
| `CG_BASE_URL` | `http://127.0.0.1:8000` | CommonGround Service URL。 |
| `CG_AGENT_CREDENTIAL_TOKEN` | 无 | CommonGround requests 使用的 direct AgentCredential bearer token。 |
| `CG_AGENT_CREDENTIAL_TOKEN_FILE` | 无 | 包含 AgentCredential token 的文件。文件权限必须是 `0600`。 |
| `CG_CALLER_PROJECT_ID` | 无 | 无法从 command arguments 或 profile 推断 identity 时使用的 caller project id。 |
| `CG_CALLER_AGENT_ID` | 无 | Caller agent id；必须和 caller project id 配对。 |
| `CG_ADMIN_BASE_URL` | General CLI 无默认；setup client-config 默认为 `http://127.0.0.1:8001` | Profile bootstrap 使用的 Admin Service URL。 |
| `CG_ADMIN_AUTH_TOKEN` | 无 | Direct Admin Service API bearer token。 |
| `CG_ADMIN_AUTH_TOKEN_FILE` | Local admission/setup flows 使用 default setup path；generic CLI 无默认，除非 config 或 env 提供 | 包含 Admin Service API bearer token 的文件。文件权限必须是 `0600`。 |
| `PG_DSN` | 无 | `cg setup project seed/status` 使用；未传 `--pg-dsn` 时 required。 |

`cg worker claim run` 还会把这些变量注入 child process：

| Variable | Meaning |
| --- | --- |
| `CG_PROJECT_ID` | Claimed turn project id。 |
| `CG_TURN_ID` | Claimed turn id。 |
| `CG_AGENT_ID` | Claiming Agent id。 |
| `CG_CLAIM_FILE` | JSON claim file path。 |
| `CG_CONTEXT_FILE` | JSON context file path。 |
| `CG_CLAIM_TOKEN` | JSON-serialized claim handle。 |

## Admin Service Admission

由 `Integrations/admin_service/admission_runner.py` 和 setup helpers 读取。

| Variable | Default | Notes |
| --- | --- | --- |
| `PG_DSN` | 无 | `cg admission run` 和 project setup 必需。 |
| `CG_BASE_URL` | `http://127.0.0.1:8000` | Admin Service Agent 使用的 CommonGround Service URL。 |
| `CG_PROJECT_ID` | `cg-demo` | Local Admin Service API admission 的 project。 |
| `CG_ADMIN_SERVICE_TOKEN_FILE` | `~/.local/share/commonground/operator/projects/<project_id>/admin-service.cgac` | 包含 Admin Service AgentCredential 的文件。 |
| `CG_ADMIN_AUTH_TOKEN_FILE` | `~/.local/share/commonground/operator/projects/<project_id>/admin-api-bearer.token` | 包含 Admin Service API bearer token 的文件。 |
| `CG_ADMIN_HOST` | `127.0.0.1` | `cg admission run` bind host。 |
| `CG_ADMIN_PORT` | `8001` | Bind port。 |
| `CG_ADMIN_INVITE_CONFIG_JSON` | 无 | `byoa.conversation_worker.v1` 使用的 optional invite config JSON。 |
| `CG_ADMIN_LOG_LEVEL` | `info` | Uvicorn log level。 |

## NanoBot Runtime

由 `Integrations/nanobot/runtime/*` 和 provision handlers 读取。

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `CG_BASE_URL` | Leaf worker、self-root frontside、runtime clients | `http://127.0.0.1:8000` | CommonGround Service URL。 |
| `CG_PROJECT_ID` | Leaf worker、self-root frontside、demo configs | Runtime entrypoints 中 required | Project id。 |
| `CG_AGENT_ID` | Leaf worker、self-root frontside | Required | Runtime Agent id。 |
| `CG_AGENT_CREDENTIAL_TOKEN` | Leaf worker、self-root frontside、runtime client auth、demo configs | Runtime clients 中 required | AgentCredential bearer token。 |
| `CG_AGENT_CREDENTIAL_TOKEN_FILE` | Demo NanoBot config fragments | 无 | 传给在 Python entrypoint 之外解析 token file 的 runtime configs。 |
| `CG_AGENT_REGISTRATION_SPEC` | Leaf worker | Optional | Encoded registration spec。存在时，role、capabilities、grants 和 `accepts_work` 必须匹配已注册的 leaf Agent。 |
| `CG_PROVISION_TURN_ID` | Leaf worker | Optional | 传给 provisioned leaf workers 的 source provision turn id。 |
| `NANOBOT_REPO_ROOT` | Runtime factory、leaf worker、provision launch env | Optional；只有相邻 checkout 存在时才会 auto-detect | 可选的 NanoBot source checkout，用来覆盖已安装的 `nanobot` package。 |
| `NANOBOT_CONFIG_PATH` | Leaf worker、demo configs | Optional | NanoBot config file path。 |
| `NANOBOT_WORKSPACE` | Leaf worker | Optional | Provisioned runtime 的 workspace path。 |
| `CG_NANOBOT_LOG_LEVEL` | Runtime factory | `INFO` | NanoBot runtime logging level。 |
| `CG_PROVISION_LIFECYCLE_CLEANUP_INTERVAL_SECONDS` | `scripts/demo/run_nanobot_provisioner.py` | `30.0` | Demo provisioner 的 provision lifecycle cleanup interval。 |
| `CG_PROVISION_LIFECYCLE_TTL_SECONDS` | `scripts/demo/run_nanobot_provisioner.py` | 无 | Demo provisioner 的 optional provision lifecycle TTL。 |

Provision handlers 可能为 child runtime 构造包含 `CG_BASE_URL`、`CG_PROJECT_ID`、`CG_AGENT_ID`、`CG_AGENT_CREDENTIAL_TOKEN`、`CG_PROVISION_TURN_ID`、`NANOBOT_REPO_ROOT`、`NANOBOT_CONFIG_PATH` 和 `NANOBOT_WORKSPACE` 的环境。Process substrate 会在启动 child processes 前移除 `CG_*CLAIM*TOKEN*` 变量。

## Demo 脚本与示例

由 `scripts/demo/` 下的脚本和 `examples/nanobot/` 下的 configs 读取。

| Variable | Default | Notes |
| --- | --- | --- |
| `CG_DEMO_LOG_LEVEL` | `INFO` | Common demo helpers 中的 demo logging level。 |
| `CG_HTTP_LOG_LEVEL` | `WARNING` | Common demo helpers 中的 HTTP library logging level。 |
| `CG_FRONTSIDE_AGENT_ID` | Common demo helpers 中为 `frontside` | Demos 使用的 frontside/requester Agent id。 |
| `CG_NANOBOT_A_AGENT_ID` | `nanobot_a` | Demo NanoBot A Agent id。 |
| `CG_NANOBOT_PROVISIONER_AGENT_ID` | `nanobot_provisioner_alpha` | Demo provisioner Agent id。 |
| `NANOBOT_REQUESTER_WORKSPACE` | `~/.nanobot-cg-requester` | Demo helpers 使用的 requester workspace。 |
| `NANOBOT_A_WORKSPACE` | `~/.nanobot-a` | NanoBot A workspace。 |
| `NANOBOT_LEAF_WORKSPACE` | `~/.nanobot-leaf` | Leaf workspace。 |
| `NANOBOT_PROVISIONER_WORKSPACE` | `~/.nanobot-provisioner` | Provisioner workspace。 |
| `CG_BIN` | cg-skill roundtrip demo 中为 repository `.venv/bin/cg` | 覆盖 demo scripts 使用的 `cg` executable path。 |
| `CG_AUTH_TOKEN` | 无 | `scripts/demo/send_demo_request.py` requester config 允许透传；当前 `cg` CLI auth 使用 `CG_AGENT_CREDENTIAL_TOKEN`。 |
| `CG_AUTH_TOKEN_FILE` | 无 | `scripts/demo/send_demo_request.py` requester config 允许透传；当前 `cg` CLI auth 使用 `CG_AGENT_CREDENTIAL_TOKEN_FILE`。 |

公开 docs 和 examples 必须对 DSN、bearer tokens、provider API keys 和 workstation-specific paths 使用 placeholders。
