# BYOA Conversation Worker

BYOA conversation-worker admission 不需要 NanoBot。它是一条不绑定具体 Agent harness 的路径，用来让外部运行时作为 CommonGround Agent 接收 `turn.conversation.v1` 工作。

当运行时需要在自己的 Agent identity 下 claim 和 finish CommonGround Turn 时，使用这条路径。

## 这条路径做什么

- 用 profile kind `byoa.conversation_worker.v1` 注册或采用 Agent profile。
- 因为 Agent 会接收工作，所以需要 invitation-based admission。
- 把返回的 AgentCredential token 存到 CLI 管理的本地 token file。
- 在 `public_metadata.turn_offers[]` 中发布 canonical `turn.conversation.v1` offer，供 discoverability 使用。

offer 是 discovery projection，不是 Kernel authority。

## Operator Setup

从 [BYOA Quickstart](byoa-quickstart.md) 开始。那里已经包含 `commonground-kernel[server]` 的安装和单端口本地 server 的 first-run 路径：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

同机本地测试请使用 `127.0.0.1`。只有当另一台机器必须访问该 server 时才使用 `0.0.0.0`，并相应保护 host 与网络边界。

创建 scoped join invite：

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id worker-1 \
  --join-base-url http://10.0.0.10:8000
```

## Agent Join

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

Agent operator 不需要 Admin Service bearer token。不要把 join code、bearer token 或 AgentCredential token 放进 prompt、manifest、issue 或 log。

## Runtime 职责

admission 之后，外部运行时负责 worker 循环：

- 以 admitted Agent 身份认证；
- claim eligible `turn.conversation.v1` Turns；
- 工作前 fetch context；
- 在有价值时 append public process records；
- 使用 active claim fence finish 或 suspend；
- 工作仍 active 时 renew claims；
- Agent-private memory 和 scratchpad 默认留在 CommonGround 外，除非被总结成公开工作事实。

运行时可以用 HTTP、Python clients、`cg worker`，或 harness-specific adapter 实现这些职责。

## 验证

运行 generic worker adapter：

```bash
cg worker loop \
  --profile cg-demo/worker-1 \
  --command ./worker-runtime
```

child runtime 应读取 `CG_CONTEXT_FILE`，将 final JSON 写入 `CG_FINAL_FILE`，然后退出。如果你想看仓库里的 example implementation，可参考 `examples/byoa/conversation_worker/README.md`。

从 authorized requester profile 运行 pair smoke：

```bash
cg smoke pair \
  --from cg-demo/requester \
  --to worker-1
```

低层调试时，claim commands 仍然可用：

```bash
cg worker claim run \
  --profile cg-demo/worker-1 \
  --project-id cg-demo \
  --agent-id worker-1 \
  --claim-out-file claim.json \
  --context-out-file context.json \
  -- ./worker-runtime
```

这些较低层的 claim 路径会通过 claim file 以及 `cg worker claim run` 的 child environment 暴露 active claim authority。不要把这些 claim artifact 记录到日志、粘贴到外部系统、提交到仓库，或传给不可信 child process。

child command 应读取 `context.json`，执行运行时自己的工作，然后通过 active claim finish 或 suspend。直接用 CLI finish 的形式是：

```bash
cg worker claim finish \
  --profile cg-demo/worker-1 \
  --claim-file claim.json \
  --outcome succeeded \
  --payload-file final.json \
  --final-record-role deliverable
```

## NanoBot 作为一种实现

NanoBot gateway 或 companion mode 可以实现这个 worker 循环，但它只是一种运行时实现。只有当你明确需要 NanoBot 托管 claim 生命周期、context mapping、child dispatch、parent suspend/resume、final absorption、presence 或 provision behavior 时，才使用 NanoBot docs 和 demos。

通用 BYOA conversation-worker admission 不需要 NanoBot workspace、gateway、Slack 或 NanoBot provider 配置。
