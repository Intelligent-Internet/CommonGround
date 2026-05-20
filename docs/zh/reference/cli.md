# CommonGround CLI 参考

本文档描述 `CommonGround/cli.py` 当前实现的 `cg` 命令面，是面向开源用户和集成作者的最小 reference。最精确的本地参数说明仍以 `cg <command> --help` 为准。

## 选项面

`cg` 不再提供 root-level 共享认证参数。连接与认证参数应放在真正使用它们的 command 上：

- **operator** 命令，例如 `cg setup project *`、`cg local run`、`cg service run` 与 `cg admission run`，只暴露本地进程、数据库、setup artifact、host、port 与 token-file artifact 选项。
- **join** 命令 `cg agent join` 接受 server URL、join code、`--base-url`、`--admin-base-url` 与 `--config`。它 redeem join code 并写入本地 config/profile/token file；不需要已有 profile、AgentCredential 或 Admin bearer token。
- **admin** 命令 `cg admission invite create` 与 `cg profile ensure-agent` 接受 `--admin-base-url`、`--admin-auth-token`、`--admin-auth-token-file` 与 `--config`。`cg profile ensure-agent` 还接受 `--base-url` 与 `--profile`，因为它会写入或刷新本地 destination profile。
- **cg_service** 命令接受 `--base-url`、`--profile`、`--auth-token`、`--auth-token-file` 与 `--config`。没有自身 actor 字段的 read/projection 命令还可以接受 `--caller-project-id` 与 `--caller-agent-id`。

Admin Service bearer flags 不是 CommonGround Service auth；AgentCredential flags 也不是 Admin Service auth。精确参数面以 `cg <command> --help` 为准。

对于已经包含 actor 的 service 命令，CLI 会推断 caller identity：

- `cg dispatch` 使用 `--requested-by`。
- `cg turn resume` 使用 `--requested-by`。
- `cg agent drain` 与 `cg agent resume` 使用 `--requested-by`；省略时使用 `--agent-id`。
- `cg provision spawn` 使用 `--requested-by`。
- `cg report work-memory` 使用 `--agent-id`。
- `cg worker once` 与 `cg worker loop` 使用 `--profile`，或使用 `--project-id` 搭配 `--agent-id`。
- `cg worker claim next` 与 `cg worker claim run` 使用 `--agent-id`。
- `cg smoke pair` 使用 `--from`。
- 基于 claim file 的 worker command 会尽量从 claim file 推断 identity。

基于 claim file 的 worker command 仍然需要匹配的 AgentCredential；claim file 标识 claim 与 actor，但它不是独立 bearer token。

## 输出封套

一次性 CLI command 会向 stdout 写出一个 JSON envelope。

成功时：

```json
{
  "ok": true,
  "result": {}
}
```

可处理失败时：

```json
{
  "ok": false,
  "error": {
    "code": "invalid_arguments",
    "message": "human-readable detail",
    "status": 2
  }
}
```

`cg local run`、`cg service run` 与 `cg admission run` 是长运行本地 operator 入口，不输出这种一次性 JSON envelope。

## 本地设置

Seed 一个本地 project namespace：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project seed --default-local
```

检查本地 project readiness：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project status --default-local
```

这条命令检查 setup artifacts 与 bootstrap readiness，不是 live CommonGround service health check。

写入本地 CLI client config：

Single-port local bundle：

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

分离服务：

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8001
```

运行 CommonGround Service：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg service run
```

运行 single-port local first-run bundle：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

同机本地测试请使用 `127.0.0.1`。只有当另一台机器必须访问该 server 时才使用 `0.0.0.0`，并相应保护 host 与网络边界。

`cg local run` 在同一个 uvicorn 进程、同一个端口提供 `/v3r1` 与 `/admin/v1`。它只合并本地运行形态；`/v3r1` 仍使用 AgentCredential 与 claim fencing，`/admin/v1` 仍承载 product-layer admission 与 join policy。

运行 reference Admin Service admission API：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg admission run --project-id cg-demo
```

`cg local run`、`cg service run` 与 `cg admission run` 需要先安装带 `server` extra 的 CLI 包：

```bash
uv tool install 'commonground-kernel[server]'
```

缺少这些依赖时，CLI 会返回 `missing_extra`。

## Agent Onboarding

通过 Admin Service 创建 scoped join invite：

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id worker-1 \
  --join-base-url http://10.0.0.10:8000
```

默认会创建 single-use、24 小时有效的 conversation-worker invite，profile kind 为 `byoa.conversation_worker.v1`，runtime kind 为 `manual.shell.v1`。结果包含可复制的 canonical command：

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

在 Agent 机器上 redeem join code：

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

`cg agent join` 会写入 `base_url`、`admin_base_url`、本地 profile，以及权限为 `0600` 的 AgentCredential token file。它不需要也不会打印 Admin Service bearer token，receipt 也不会打印 AgentCredential secret。

## Profiles

Profile 让 CLI 通过 Admin Service 请求并保存 Agent credential。

```bash
cg profile ensure-agent \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Reporter"
```

Profile command 需要通过 `--admin-auth-token`、`--admin-auth-token-file` 或 config 提供 Admin Service bearer auth。生成的 Agent credential token 会写入本地 token file，并由 CLI config 引用。

对于 invited conversation-worker profile，通过文件传入 invitation code：

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

`--invitation-code` 也可用于一次性本地 demo，但推荐 `--invitation-code-file`，避免 shell history 暴露。

新的 conversation-worker onboarding 推荐使用 `cg admission invite create` 加 `cg agent join`。`cg profile ensure-agent` 仍适合 work-memory reporter profile 和较底层的 Admin Service 测试。

## Dispatch

Dispatch 一个 root Turn：

```bash
cg dispatch \
  --project-id cg-demo \
  --requested-by requester \
  --target-agent worker \
  --turn-kind turn.conversation.v1 \
  --request-id req-001 \
  --payload-json '{"prompt":"Summarize the repo status"}'
```

`--request-id` 与 `--dispatch-key` 是 birth-time causality / idempotency anchor。至少提供一个；只提供其中一个时，CLI 会把它镜像到另一个。

Payload 输入方式：

- `--payload-json '<json>'`
- `--payload-file payload.json`
- `--payload-stdin`

## Turns

获取 Turn snapshot：

```bash
cg turn get --project-id cg-demo --turn-id <turn_id>
```

获取 semantic context：

```bash
cg turn context \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --after-turn-seq 0 \
  --limit 100
```

等待 Turn closed：

```bash
cg turn wait \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --timeout-seconds 60 \
  --poll-interval-ms 500
```

恢复 suspended Turn：

```bash
cg turn resume \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --requested-by requester
```

## Agents

获取 Agent snapshot：

```bash
cg agent get --project-id cg-demo --agent-id worker
```

Drain 一个 Agent，使它停止接收新的 Turn：

```bash
cg agent drain \
  --project-id cg-demo \
  --agent-id worker \
  --requested-by operator
```

恢复接收新的 Turn：

```bash
cg agent resume \
  --project-id cg-demo \
  --agent-id worker \
  --requested-by operator
```

## 项目观察

通过 projection API 列出 Agents：

```bash
cg project agent list \
  --project-id cg-demo \
  --accepts-work-only \
  --limit 100
```

列出 canonical Turn offers：

```bash
cg project offer list \
  --project-id cg-demo \
  --turn-kind turn.conversation.v1
```

获取单个 Turn offer：

```bash
cg project offer get \
  --project-id cg-demo \
  --turn-kind turn.conversation.v1 \
  --agent-id worker
```

列出 Turns：

```bash
cg project turn list \
  --project-id cg-demo \
  --target-agent-id worker \
  --state closed \
  --limit 100
```

获取 Turn lineage：

```bash
cg project turn lineage \
  --project-id cg-demo \
  --turn-id <turn_id>
```

获取 project feed：

```bash
cg project feed \
  --project-id cg-demo \
  --after-ledger-seq 0 \
  --limit 100
```

## Worker 生命周期

运行一次 generic shell worker adapter：

```bash
cg worker once \
  --profile cg-demo/worker \
  --command ./worker-bin --flag
```

循环运行同一个 adapter：

```bash
cg worker loop \
  --profile cg-demo/worker \
  --command ./worker-bin --flag
```

`cg worker once` 与 `cg worker loop` 会 claim work，将 context 写入 `CG_CONTEXT_FILE`，在 child command 运行期间 renew claim，并根据 `CG_FINAL_FILE` 或 `CG_SUSPEND_FILE` finish 或 suspend Turn。这两条 adapter 路径不会把 active claim token 放进 stdout 或 child environment。

Claim 下一个可用 Turn：

```bash
cg worker claim next \
  --project-id cg-demo \
  --agent-id worker \
  --claim-out-file claim.json
```

Claim 一个 Turn，持续 renew lease，并运行子命令：

```bash
cg worker claim run \
  --project-id cg-demo \
  --agent-id worker \
  --claim-out-file claim.json \
  --context-out-file context.json \
  -- ./worker-bin --flag
```

Renew claim：

```bash
cg worker claim renew --claim-file claim.json
```

较低层的 `cg worker claim *` 命令与 `cg worker once/loop` 不同：claim file 以及 `cg worker claim run` 的 child environment 都包含 active claim authority。不要把这些 claim artifact 记录到日志、粘贴到外部系统、提交到仓库，或传给不可信 child process。

在 active claim 下追加 semantic record：

```bash
cg worker claim append \
  --claim-file claim.json \
  --payload-file progress.json \
  --role progress
```

从 parent claim dispatch child Turn：

```bash
cg worker claim dispatch-child \
  --claim-file claim.json \
  --requested-by worker \
  --target-agent reviewer \
  --payload-file child-payload.json \
  --dispatch-key child-001
```

Suspend 或 finish 当前 Turn：

```bash
cg worker claim suspend \
  --claim-file claim.json \
  --reason blocked \
  --note "Waiting for external input"

cg worker claim finish \
  --claim-file claim.json \
  --outcome succeeded \
  --payload-file final.json \
  --final-record-role deliverable
```

## Smoke

运行 pair collaboration smoke test：

```bash
cg smoke pair \
  --from cg-demo/requester \
  --to worker
```

该命令会检查 target offer discovery，dispatch 一个 `turn.conversation.v1`，等待 terminal state，读取 context，并返回 terminal payload。

## Provisioning

向 provisioner Agent dispatch 一个 provision request：

```bash
cg provision spawn \
  --project-id cg-demo \
  --requested-by requester \
  --provisioner-agent provisioner \
  --role researcher \
  --request-id provision-001
```

该命令使用 Turn kind `turn.provision.agent.spawn.v1`。Role discoverability 来自 canonical Turn offers 的 projection，不是 kernel authority。

## Work-Memory Reports

提交本地 work-memory manifest：

```bash
cg report work-memory \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --agent-id reporter \
  --manifest-file work-memory.json \
  --request-id report-001
```

提交前先通过 Admin Service bootstrap 或刷新 reporting Agent profile：

```bash
cg profile ensure-agent \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Reporter"
```

Manifest 必须是 JSON object，并且不能包含 `meta`。

## CLI 与 Integration API

CLI 适合本地操作、调试、脚本化 demo 和 operator workflow。它提供稳定的 command-level envelope 与 profile bootstrap 便利。

Runtime integration 应使用 Python clients 或 HTTP API：

- `CommonGround.agent_client.http_client.HttpAgentClient` 覆盖 Agent、Turn、claim、dispatch、credential 与 work-memory 操作。
- `CommonGround.projection_client.http_client.ProjectionHttpClient` 覆盖只读 project projection routes。

CLI 不是 kernel authority。它只是同一个 HTTP service 与 Admin Service surface 之上的 client。
