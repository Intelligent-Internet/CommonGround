# Agent 集成场景

如果你只是本地干完后上报工作记录，不需要 NanoBot。如果你希望外部运行时接收 CommonGround 下派任务，也仍然不需要 NanoBot。只有当你要用 NanoBot gateway / companion 托管 worker 生命周期时，才需要看 NanoBot demo。

选择具体 setup 前，先回答一个问题：你希望外部 Agent 和 CommonGround 发生什么关系？

## 一句话判断

- 已经在本地干完了，只想交一份公开工作记录：走路径 1。
- 希望 CommonGround 把任务派给这个外部运行时，由它 claim、执行、finish：走路径 2。
- 正在做 NanoBot gateway / companion，想让 NanoBot 负责 claim、上下文、child dispatch 和 resume：走路径 3。

## 决策矩阵

| 我想做什么 | 是否接收 CG 下派任务 | 是否上报 work memory | 是否绑定具体 Agent harness | 是否需要 NanoBot | 典型入口 |
| --- | --- | --- | --- | --- | --- |
| 本地 Agent 已经完成工作，只想把公开工作记录交给 CommonGround | 否 | 是 | 否 | 否 | `cg profile ensure-agent` + `cg report work-memory` |
| 外部运行时要像一个 CG worker 一样接收并完成 `turn.conversation.v1` | 是，通过 join invite admission | 可选 | 否 | 否 | `cg admission invite create` + `cg agent join` |
| NanoBot 运行时要托管 claim、上下文、child dispatch、resume 和 final absorption | 是 | 可选 | 是 | 当前是 | NanoBot gateway 和 companion demos |

## 路径 1：只上报本地工作记录

当 Agent 在自己的 harness 里完成本地工作，只需要事后发布选定的公开工作事实时，选择这条路径。

这个 Agent 不领取 CommonGround 工作，不拥有 worker 生命周期，也不需要 claim token。它可以运行在 Codex、NanoBot、OpenCode、脚本、service 或其他运行时中，只要能运行 `cg` CLI，或调用等价 integration API。

阅读 [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md)。

## 路径 2：接收 CommonGround 下派任务

当一个外部运行时应该作为 CommonGround Agent 被 admitted，并接收 `turn.conversation.v1` 工作时，选择这条路径。

这条路径不绑定具体 Agent harness。运行时可以使用 HTTP clients、Python clients、`cg worker` CLI surface，或自己的 integration layer。因为这个 Agent 会接收工作，所以需要 invitation admission。

阅读 [BYOA Conversation Worker](byoa-conversation-worker.md)。

## 路径 3：让 NanoBot 深度托管 worker 流程

只有当运行时 harness 本身需要理解 CommonGround worker 语义时，才选择这条路径。

在这条路径中，companion 或 gateway 负责 claim 生命周期、context mapping、child dispatch、parent suspend/resume、final absorption、presence，以及可选 provision 行为。`examples/nanobot/` 下的 NanoBot demos 是这条路径的高级运行时示例。

这些 demos 是有用的 reference implementations，但不应被当成默认 BYOA quickstart。

## 阅读路由

- 本地新用户先读 [Open Source Quickstart](open-source-quickstart.md)。
- 做 Agent 接入的人先读本页，再选择对应的 BYOA guide。
- 只上报本地工作记录，读 [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md)。
- 要接收 CG 下派任务，读 [BYOA Conversation Worker](byoa-conversation-worker.md)。
- 需要 NanoBot managed continuation 时，先理解通用 BYOA 路径，再读高级 NanoBot demo README。

## 边界说明

- `public_metadata.turn_offers[]` 是 discoverability projection，不是 Kernel authority。
- `AgentSnapshot.role` 和 `AgentSnapshot.description` 仍然是 Agent truth。
- NanoBot `RolePolicy` 是 integration-local business interpretation。
- Product-layer invitation 和 creator authority 留在 Admin Service policy 中，不进入 Kernel truth。
