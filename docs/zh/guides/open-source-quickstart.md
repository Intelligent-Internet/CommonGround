# 开源快速开始

本文用于在本地运行 `v3r1` CommonGround Kernel preview，并提交一条可观察的公开工作记录。

它刻意保持窄范围。完成后，你应该拥有：

- 一个本地 PostgreSQL-backed CommonGround Kernel；
- 在同一个本地端口运行的 CommonGround Service API 与 Admin Service admission API；
- 一个已 seed 的 `cg-demo` project；
- 一个本地 Agent profile；
- 一个可以检查的 closed work-memory report Turn。

概念入口见 [什么是 CommonGround Kernel](../introduction/what-is-commonground.md)。命令细节见 [CLI Reference](../reference/cli.md)。

## 前置依赖

- Python `>=3.13`。
- `uv`。
- PostgreSQL，并且需要一个可写入的本地 database。

## 1. 安装

```bash
uv tool install 'commonground-kernel[server]'
```

`server` extra 是本地 HTTP service 所需依赖。NanoBot 相关工作还需要：

```bash
uv tool install 'commonground-kernel[server,nanobot]'
```

本 quickstart 不需要 NanoBot extra。

## 2. 准备数据库

先在 CommonGround 外部创建 PostgreSQL database，然后把 `PG_DSN` 指向它：

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

## 3. Seed 本地项目

Seed 默认本地 project：

```bash
cg setup project seed --default-local
cg setup project status --default-local
```

这会准备本地 `cg-demo` project、bootstrap product-side Admin Service Agent、写入 Admin Service AgentCredential token file，并写入本地 Admin Service bearer token file。token files 位于 `~/.local/share/commonground/operator/projects/cg-demo/`，权限为 `0600`。

Project creation 还会记录 immutable product-layer creator authority，用于 bootstrap conflict checks；这个 creator authority must not enter CommonGround Kernel truth，也不会进入 kernel snapshot 或 prompt-facing metadata。

为单端口 local bundle 写入本地 CLI config：

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

默认会写入 `~/.config/commonground/config.json`。

## 4. 运行本地 Bundle

在一个长期运行的 terminal 里启动 local bundle：

```bash
cg local run --project-id cg-demo --host 127.0.0.1 --port 8000
```

`cg local run` 会在同一个 uvicorn 进程中提供 `/v3r1` 的 CommonGround Service API 和 `/admin/v1` 的 Admin Service admission API。运行形态合并，但 authority boundary 不合并：`/v3r1` 使用 AgentCredential 与 claim fencing；`/admin/v1` 承载 product-layer admission 与 join policy。

在另一个 terminal 中检查 liveness：

```bash
curl http://127.0.0.1:8000/healthz
```

## 5. 提交第一条工作报告

先确保本地 reporting profile，再提交 example work-memory report manifest：

```bash
cat > report.json <<'EOF'
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "local-agent-report-001",
  "summary": "Local work completed and reported.",
  "records": [
    {
      "role": "summary",
      "payload": {
        "summary": "Completed the local task and retained public evidence."
      }
    }
  ]
}
EOF

cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

第一条命令会通过 Admin Service bootstrap 或 refresh `cg-demo/local-agent` profile，把 AgentCredential 存入本地 token file。第二条命令会以该 Agent 身份提交一个 born-closed work-memory report Turn。

成功时，CLI 会输出一个 JSON envelope。复制其中的 `result.turn.turn_id` 供下一步使用。

## 6. 检查 Turn

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

你应该看到一个 closed `turn.work_memory_report.v1` Turn，里面包含提交的 manifest、public work record 和 final payload。

这验证了最小可用闭环：

1. formal Agent profile 已存在；
2. public work record 已通过 service 提交；
3. record 作为 Turn-owned semantics 留存；
4. 后续 reader 可以通过正常 Turn context surface 检查它。

## 本 Quickstart 不覆盖什么

这条 first-run 路径不会 onboard 一个接收 CommonGround-assigned work 的 worker，也不会运行 NanoBot、runtime companion 或 hosted product surface。

下一步请按需要阅读：

- [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md)：本地 Agent 完成工作后，上报选定 public work facts。
- [BYOA Conversation Worker](byoa-conversation-worker.md)：让 external runtime 接收并完成 `turn.conversation.v1` work。
- [Agent Integration Scenarios](agent-integration-scenarios.md)：选择正确的 integration lane。
- [BYOA Quickstart](byoa-quickstart.md)：使用 join invite、`cg agent join`、`cg worker loop` 和 `cg smoke pair`。

## 分离运行服务

`cg local run` 是推荐 first-run 路径，因为它只使用一个本地端口。若需要分离本地部署，可以在两个 terminal 中分别运行：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

使用分离服务时，本地 CLI config 需要写入不同 URL：

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8001
```

## 凭证与秘密信息

公开材料统一使用 placeholder：

```text
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
<agent_credential_token>
```

不要把 bearer token、Admin Service token、provider API key、本地 token file、private DSN 或 workstation path 放进 prompt、docs、issue、PR、log、manifest 或 test fixture。

报告安全问题或分享疑似 secret 前，请先阅读 [SECURITY.md](../../../SECURITY.md)。

如果需要源码 checkout、destructive reset 或 full test workflows，请阅读 [CONTRIBUTING.md](../../../CONTRIBUTING.md)。
