# BYOA Work-Memory Reporter

浅层 BYOA work-memory reporting 不需要 NanoBot。它是一条不绑定具体 Agent harness 的路径：Agent 先在本地完成工作，然后把选定的公开工作事实上报给 CommonGround。

当 Agent 不需要接收 CommonGround Turn，也不应该拥有 worker 生命周期时，使用这条路径。

## 这条路径做什么

- 用 profile kind `byoa.work_memory_reporter.v1` 注册或刷新本地 Agent profile。
- 把返回的 AgentCredential token 存到 CLI 管理的本地 token file。
- 通过 `cg report work-memory` 提交一个 born-closed work-memory report Turn。
- 让后续 Agent 或人类可以通过普通 CommonGround read surface inspect 这个 report。

它不需要 worker claim、NanoBot gateway 配置、Slack、运行时 companion，prompt-facing Agent 也不需要直接访问数据库。

## Operator Setup

从 [Open Source Quickstart](open-source-quickstart.md) 中的本地 service setup 开始：

先安装带 `server` extra 的 CLI 包，再执行下面这些本地 service commands：

```bash
uv tool install 'commonground-kernel[server]'
```

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
cg setup project seed --default-local
cg setup project client-config --default-local
```

在两个独立终端运行本地 services：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

prompt-facing Agent 只应该拿到这些非 secret setup facts：

- project id，例如 `cg-demo`
- agent id，例如 `local-agent`
- CLI profile name，例如 `cg-demo/local-agent`
- CommonGround service URL
- Admin Service URL
- token-file reference，或已经写好的 CLI config

不要把 bearer token value 或 AgentCredential token value 放进 prompt、manifest、issue 或 log。

## Runtime Setup

显式 bootstrap profile：

```bash
cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind external-runtime.v1 \
  --display-name "Local Agent"
```

然后用准备好的 profile 提交 report：

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

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

## 最小 Manifest

manifest 是一个 JSON object，包含 request id 和至少一条 public work record：

```json
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
```

manifest 不能包含 `meta`、credentials、claim tokens、private scratchpad state 或 chain-of-thought。

## 验证

report command 会返回一个 JSON envelope。成功后保留 `result.turn` 和 `result.record_refs`。

inspect 提交后的 Turn：

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

预期结果是一个由 reporting Agent 拥有的 closed work-memory report Turn。这里没有 worker claim，也没有 `turn.conversation.v1` assignment。

## 什么时候换路径

- 如果运行时必须接收 `turn.conversation.v1`，使用 [BYOA Conversation Worker](byoa-conversation-worker.md)。
- 如果运行时 companion 要在 NanoBot 内拥有 claim 生命周期、child dispatch、suspend/resume 和 final absorption，使用高级 NanoBot runtime demos。
