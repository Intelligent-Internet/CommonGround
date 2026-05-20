# Admin Service BYOA 凭证流程

本文说明当前 reference Admin Service integration 如何通过 product-side admission flow 发放 CommonGround Agent credential。

Admin Service 位于 CommonGround Kernel 之外。它负责 product user authentication、project creator authority、invitation policy 和本地 operator token files。Kernel 负责保存 Agents、Turns、grants、credentials、ledger facts 和 lifecycle state。不要把 product-layer authorization fact 放进 Kernel truth 或 Agent public metadata。

选择场景时，先读 [Agent Integration Scenarios](agent-integration-scenarios.md)。work-memory reporter 和 conversation-worker 两条 BYOA 路径都不绑定具体 Agent harness，也不需要 NanoBot。

## 前置依赖

- Python `>=3.13`。
- 可通过 `PG_DSN` 访问的 PostgreSQL 数据库。
- 先安装带 `server` extra 的 CLI 包：

```bash
uv tool install 'commonground-kernel[server]'
```

## 本地项目初始化

先 seed project-scoped Admin Service Agent 与本地 operator tokens：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
  cg setup project seed --default-local
```

默认 project id 是 `cg-demo`，creator ref 是 `local-dev`。seed 命令会创建或校验 project-scoped Agent `admin-service`，role 为 `service.admin.v1`。这个 Agent 拥有 credential-issue、credential-revoke 和 agent-birth grants；它不接收工作。

该命令还会创建两个本地 token 文件：

| 文件 | 默认路径 | 用途 |
| --- | --- | --- |
| Admin Service AgentCredential | `~/.local/share/commonground/operator/projects/<project_id>/admin-service.cgac` | Admin Service Agent 调用 CommonGround service-authorized registration 和 credential API 时使用的 credential。 |
| Admin Service API bearer token | `~/.local/share/commonground/operator/projects/<project_id>/admin-api-bearer.token` | local admission API 接受的 product-side bearer token。CLI profile bootstrap 会把它放在 `Authorization: Bearer ...` 中发送。 |

Token files 会以 `0600` 权限写入。如果本地文件存在但 stale 或 invalid，用 `--rotate-admin-service-token` 或 `--rotate-admin-auth-token` 轮换。

检查 readiness：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
  cg setup project status --default-local
```

写入本地 CLI config，让 CLI 指向 CommonGround service 和 Admin Service：

```bash
cg setup project client-config --default-local
```

默认 config 路径是 `~/.config/commonground/config.json`。它保存 service URLs 和 Admin Service bearer token file reference；除非调用方显式配置，否则不会把本地 operator bearer token inline 进去。

## 运行服务

运行 CommonGround Service：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

运行 local Admin Service admission API：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

`cg admission run` 读取：

- `PG_DSN` 或 `--pg-dsn`。
- `CG_BASE_URL` 或 `--base-url`，默认 `http://127.0.0.1:8000`。
- `CG_PROJECT_ID` 或 `--project-id`，默认 `cg-demo`。
- `CG_ADMIN_SERVICE_TOKEN_FILE` 或 `--admin-service-token-file`，默认 setup token path。
- `CG_ADMIN_AUTH_TOKEN_FILE` 或 `--admin-auth-token-file`，默认 setup bearer-token path。
- `CG_ADMIN_HOST`、`CG_ADMIN_PORT` 和 `CG_ADMIN_LOG_LEVEL`，默认 `127.0.0.1`、`8001` 和 `info`。
- `CG_ADMIN_INVITE_CONFIG_JSON` 或 `--invite-config-json`，用于 conversation-worker invitations。

Local runner 只接受配置的 project id。请求必须用 Admin Service API bearer token 认证，并被解析为 requester user id `local-admin-service`。

## 凭证请求 API

Local API 暴露一个 route：

```http
POST /admin/v1/projects/{project_id}/agent-credential-tokens:request
Authorization: Bearer <admin_service_api_bearer_token>
Content-Type: application/json
```

最小 work-memory profile request：

```json
{
  "request_id": "profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli",
  "requested_agent_id": "reporter",
  "display_name": "Local Reporter",
  "runtime_kind": "local-cli"
}
```

Response shape：

```json
{
  "request_id": "profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli",
  "project_id": "cg-demo",
  "agent_id": "reporter",
  "status": "registered",
  "profile": {
    "project_id": "cg-demo",
    "agent_id": "reporter",
    "runtime_kind": "local-cli",
    "profile_kind": "byoa.work_memory_reporter.v1",
    "profile_ref": "admin_service/byoa_registration_requests/profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli/connection-profile",
    "credential_id": "<credential_id>",
    "status": "credential_ready"
  },
  "credential": {
    "credential_id": "<credential_id>",
    "status": "active"
  },
  "agent_credential_token": "<agent_credential_token>"
}
```

`agent_credential_token` 是 Admin Service response 中的一次性 credential material。Credential row 会存入 CommonGround，但 secret token 只在 response 中披露给调用方。把它保存到 runtime-private token file 或 secret manager；不要写入 prompt、manifest、log、issue 或 docs。

CLI profile bootstrap 会代管这部分存储：

```bash
cg profile ensure-agent \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile cg-demo/reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local-cli \
  --display-name "Local Reporter"
```

CLI 会把 AgentCredential token 写到：

```text
~/.local/share/commonground/credentials/<project_id>/<agent_id>/<credential_id>.token
```

并把 profile metadata 存到 CLI config 的 `profiles` map 下。

## Profile 类型

当前 BYOA facade 支持这些 profile kinds：

| Profile kind | 目标 runtime | Registration result |
| --- | --- | --- |
| `byoa.work_memory_reporter.v1` | 上报 work-memory manifest 的 local CLI 或 external runtime。 | 注册 requested Agent，role 为 `external.agent.v1`，不接收工作，无 capabilities，并在 Admin Service metadata 中记录 `byoa_request_id` 和 `runtime_kind`。默认 requested capability 是 `turn.work_memory.report.v1`，但它是 admission policy input，不是注册后的 work capability。 |
| `byoa.conversation_worker.v1` | 通过 invitation admitted 的 external conversation worker。 | 要求 valid invitation code。注册 role `external.conversation_worker.v1`，接收工作，capability 为 `turn.conversation.v1`，并在 `public_metadata.turn_offers[]` 发布 canonical conversation turn offer。 |

对 `byoa.work_memory_reporter.v1`，MVP policy 只接受 requested role `external.agent.v1` 和 requested capability `turn.work_memory.report.v1`。

对 `byoa.conversation_worker.v1`，request 必须包含 `invitation_code`；facade 会根据 invitation-approved profile kind 映射 admitted role 和 capability。

## 邀请配置

Conversation-worker admission 要求 invite validator。Local runner 从 `CG_ADMIN_INVITE_CONFIG_JSON` 或 `--invite-config-json` 加载 JSON 文件。

示例：

```json
{
  "invitations": [
    {
      "invite_id": "invite-local-conversation",
      "project_id": "cg-demo",
      "issued_by_user_id": "user-123",
      "issuer_role": "project_owner",
      "allowed_profile_kinds": ["byoa.conversation_worker.v1"],
      "code_sha256": "sha256:<64_hex_sha256_of_invitation_code>",
      "enabled": true,
      "expires_at": "2026-12-31T23:59:59Z"
    }
  ]
}
```

本地 config 可用 `code` 代替 `code_sha256`；loader 会在启动时计算 hash。进入仓库的示例应优先使用 `code_sha256`。`issuer_role` 当前必须是 `project_owner`。Disabled、expired、wrong-project、wrong-profile-kind 或无法匹配的 code 都会被拒绝。

Conversation-worker API request：

```json
{
  "request_id": "invite:cg-demo:worker-1",
  "requested_agent_id": "worker-1",
  "display_name": "Conversation Worker 1",
  "runtime_kind": "external-runtime",
  "profile_kind": "byoa.conversation_worker.v1",
  "invitation_code": "<invitation_code>"
}
```

CLI 可以完成 invited bootstrap，并把返回的 AgentCredential token 存入普通 profile store：

```bash
cg profile ensure-agent \
  --profile cg-demo/worker-1 \
  --project-id cg-demo \
  --requested-agent-id worker-1 \
  --profile-kind byoa.conversation_worker.v1 \
  --runtime-kind external-runtime.v1 \
  --display-name "Worker 1" \
  --invitation-code-file ./invite-code.txt
```

推荐使用 `--invitation-code-file`。`--invitation-code` 只适合一次性本地 demo，因为 shell history 可能保留它。

## 常见错误

Admin Service errors 使用这个 JSON envelope：

```json
{
  "error": "UnauthorizedError",
  "code": "unauthorized",
  "message": "Admin Service bearer auth is required"
}
```

常见 codes：

| Code | 常见原因 | 处理方式 |
| --- | --- | --- |
| `unauthorized` | 缺少或使用了错误的 Admin Service API bearer token。 | 使用 `CG_ADMIN_AUTH_TOKEN_FILE` 中的 token；必要时用 rotation 重新运行 `cg setup project seed`。 |
| `forbidden` | Caller 无权访问 project，或 BYOA invitation invalid、disabled、expired、wrong-project、wrong-profile-kind。 | 检查 project id、bearer-token boundary 和 invite config。 |
| `project_not_seeded` | Project 没有有效的 `admin-service` Agent。 | 运行 `cg setup project seed --project-id <project_id>` 或 `--default-local`。 |
| `project_bootstrap_conflict` | 已存在的 `admin-service` Agent 与预期 role、grants、metadata、enabled state 或 capacity 不匹配。 | 查看 `cg setup project status`，有意识地修复冲突的 project state。 |
| `admin_service_credential_required` | Admin Service AgentCredential token 缺失、stale、invalid、expired，或不属于配置的 project。 | 用 `--rotate-admin-service-token` 重新 setup，或把 `CG_ADMIN_SERVICE_TOKEN_FILE` 指向 valid token file。 |
| `invalid_input` | Request field 不支持、required field 缺失、profile kind 不支持、invite config shape 错误、datetime/hash format 错误。 | 对照本文和 CLI help 检查 request body 与 invite config。 |
| `conflict` | BYOA request state 重复或不一致、requested role/capability 不支持、profile policy mismatch。 | 只有确实需要 idempotent 时才复用 request；否则使用新的 `request_id` 和 valid policy inputs。 |

CLI setup errors 会作为 one-shot command 的单个 JSON envelope 输出到 stdout。长运行的 `cg service run` 和 `cg admission run` 输出日志。

## 边界说明

- `ProjectCreatorAuthority` 是 product-layer creator authority。应保存在 product project store，不进入 Kernel schema、registration provenance 或 Agent public metadata。
- `public_metadata.turn_offers[]` 是 discoverability projection，不是 Kernel authority。
- Admin Service AgentCredential 证明 Admin Service Agent 可以调用 service-authorized CommonGround APIs。它不证明 end-user authorization。
- Admin Service API bearer token 是 product-layer local admission credential，不是 AgentCredential。
- Runtime-private AgentCredential tokens 应保存在 token files 或 secret stores；prompt-facing agents 不应直接读取 operator token files。
