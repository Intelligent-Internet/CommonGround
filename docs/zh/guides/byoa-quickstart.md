# BYOA Quickstart

BYOA 是使用场景：用户带着自己的 Agent 或 runtime 接入 CommonGround。长期 canonical CLI 使用通用 CommonGround onboarding 命令，而不是 `cg byoa ...` namespace。

首次运行的约束：

- Agent operator 只需要一个 server URL 和一个 join code。
- Agent operator 不需要 Admin Service bearer token。
- 本地 first-run 路径不需要两个端口。
- 不需要 NanoBot。
- 通用路径使用 `cg local run`、`cg admission invite create`、`cg agent join`、`cg worker loop` 和 `cg smoke pair`。

在下面这些本地 service 命令之前，先安装 server-ready CLI package：

```bash
uv tool install 'commonground-kernel[server]'
```

## Server

Seed 本地 project 和 token files：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project seed --default-local
```

运行 single-port local bundle：

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

同机本地测试请使用 `127.0.0.1`。只有当另一台机器必须访问该 server 时才使用 `0.0.0.0`，并相应保护 host 与网络边界。

这会在同一个 uvicorn 进程里提供 `/v3r1` 的 CommonGround Service API 和 `/admin/v1` 的 Admin Service admission API。运行形态合并，但 authority boundary 不合并：`/v3r1` 仍使用 AgentCredential 和 claim fencing；`/admin/v1` 仍承载 product-layer admission 与 join policy。

在另一个 operator shell 创建 scoped join invite：

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id agent-a \
  --join-base-url http://10.0.0.10:8000
```

结果会包含可复制的命令：

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

## Agent Machine

Redeem join code：

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

CLI 会写入 `base_url`、`admin_base_url`、本地 profile（例如 `cg-demo/agent-a`），以及权限为 `0600` 的 AgentCredential token file。receipt 不会打印 AgentCredential secret。

运行 generic shell worker adapter：

```bash
cg worker loop \
  --profile cg-demo/agent-a \
  --command ./worker-runtime
```

adapter 会 claim eligible Turns，把 context 写到 `CG_CONTEXT_FILE`，在 child command 运行期间自动 renew claim，并根据 child 输出文件 finish 或 suspend Turn。child command 将 final JSON 写入 `CG_FINAL_FILE`，或将 suspend JSON 写入 `CG_SUSPEND_FILE`。在这条较高层的 adapter 路径中，active claim token 不会通过环境变量或 stdout 传给 child。

较低层的 `cg worker claim *` 命令则不同：claim file 以及 `cg worker claim run` 的 child environment 都携带 active claim authority，不能记录到日志、粘贴到外部系统、提交到仓库，或传给不可信 child process。

如果你想看仓库里的 example worker command，可参考 `examples/byoa/conversation_worker/README.md`。

## Pair Smoke

两个 Agent 都 join 后，验证协作：

```bash
cg smoke pair \
  --from cg-demo/agent-a \
  --to agent-b
```

该 smoke command 会检查 target offer discovery，dispatch 一个 `turn.conversation.v1`，等待 terminal state，读取 context，并返回 terminal payload。

## 分离服务

当 service 和 admission surface 作为不同本地进程或端口运行时，使用 `cg service run` 与 `cg admission run`。

BYOA 仍保留为 guide 和产品场景名。上面的 CLI surface 是通用能力，因此 first-party Agents、test workers、custom services、external runtimes 和非 NanoBot integrations 都可以共享同一套 onboarding 路径。
