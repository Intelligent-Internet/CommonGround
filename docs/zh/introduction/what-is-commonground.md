# 什么是 CommonGround Kernel

CommonGround Kernel 是面向人类与多 Agent 协作的宪法化 ledger kernel。

它建立在这一第一性原理之上：

> Assume nothing beyond what constraints demand.

它要解决的不是“怎么调用一个模型”，而是更基础的问题：

> 当多个 Agent、人类、工具和外部 runtime 一起完成真实任务时，哪些工作事实必须被稳定保存下来，才能让任务可以交接、恢复、审计，并在之后被理解、复用和学习？

CommonGround 的答案是：

> 用 Agent、Turn 和 Turn-owned semantics 建立公共事实，让真实工作从一次临时会话变成可追溯、可审计、可解释、可复用的工作记录。

更短地说：

> CommonGround turns agent work into durable common ground.

v3r1 开源版本线先发布 Ledger Kernel。当前 service/API 实现版本是 `v3r1`。

## 为什么需要 CommonGround

多 Agent 协作真正困难的地方，通常不在于“能不能发出下一条消息”，而在于工作跨越多个主体之后，事实很快会散掉。

常见问题包括：

- 谁是稳定的执行主体？
- 一次任务的边界在哪里？
- 谁有权推进、暂停、恢复或结束这次工作？
- 一个 Agent 交给另一个 Agent 的输入到底是什么？
- child Agent 交回来的 deliverable 到底是什么？
- parent 是否、何时、如何吸收了 child 的结果？
- 中间关键判断、观察、验证、失败和 artifact 留在哪里？
- 后来的 Human 或 Agent 如何审计这次工作？
- 组织如何从这次真实工作中沉淀可复用知识？

如果这些问题只靠 runtime 内存、聊天记录、日志、通知、共享工作目录或某个中心调度器维持，系统会很快失去可恢复性和可解释性。

CommonGround 的目标，就是把这类协作中最难替代、最需要 durable truth 的部分收敛成一组很小但稳定的公共事实对象。

## 第一性原理

CommonGround 不应假设超出约束所要求的内容。

这也是 v3r1 Kernel 刻意保持小的原因。CommonGround 不假设自己拥有每个 Agent、runtime、scheduler、memory、topology、workflow 或业务决策。它只建模协作保持可恢复、可审计、可解释所需的最小 durable facts。

Kernel 把 Agent identity、Turn boundary、Turn-owned semantics、authority boundary、lifecycle 与 causal lineage 视为宪法级问题。其他问题应该位于显式上层，除非持续约束证明它们必须进入 Kernel。

## CommonGround 的最小模型

CommonGround Kernel 的最小法定对象只有三个：

- **Agent**
  稳定的逻辑执行主体。它可以由 LLM、人类、服务、脚本、外部 runtime 或混合系统承载，但在 CommonGround 中首先是可被委托、可承担责任、可被审计的正式主体。

- **Turn**
  最小 durable work boundary。一个 Turn 表达一次明确的工作、委托、交互或恢复闭环，而不是一条同步回复或一段临时会话。

- **完整语义**
  某个 Turn 正式拥有的语义事实边界。它承载这个 Turn 的输入、主动吸收的观察、过程中的公开记录、最终交付物与终止原因。

这三个对象共同建立了 CommonGround 的基本秩序：

> Agent 承担责任，Turn 承载工作，完整语义保存这次工作的公开事实。

## Turn 是工作容器

在 CommonGround 里，Turn 不是普通 request / response。

Turn 是一次工作的正式容器。围绕一个 Turn，系统可以稳定回答：

- 这次工作从哪里来？
- 它交给了哪个 Agent？
- 当前处于什么 lifecycle 状态？
- 谁正在持有执行 claim？
- 它派生了哪些 child Turn？
- 它收到过哪些输入和公开记录？
- 它最终交付了什么？
- 它为什么失败、停止或完成？

当任务跨越多个 Agent 时，每个 child 也是自己的 Turn。child 有自己的输入、过程记录、final deliverable 和 lifecycle；parent 只能观察 child 的事实，再由 parent 的合法控制边界决定是否吸收。

这避免了系统滑向一个隐式共享会话池，也避免 child 悄悄改写 parent 的工作事实。

## 今天是 Ledger Kernel，下一步探索 Memory Abstraction

CommonGround 不只是为了“把任务跑完”。v3r1 先开源 Ledger Kernel。

真实工作会自然产生很多组织未来可能复用的材料：

- 关键上下文
- 选择某个 Agent 的理由
- handoff 时传递的约束
- child 返回的结果
- 被 parent 采纳的观察
- 执行中的 checkpoint
- 验证结果
- artifact 引用
- 失败路径
- 最终 deliverable

CommonGround 的长期方向，是让这些公开工作材料围绕 Turn 留存下来，使当前 Agent、后续 Agent、Human、LLM-enabled Agent、外部系统和 Knowledge Team 都可以读取、审计、解释、汇总和提炼。

这里有一个重要边界：

- Agent 边界间的 I/O，例如 root input、dispatch input、child deliverable、parent final deliverable，必须 durable 留存。
- Agent 内部公开工作过程知识，例如观察、判断、验证、失败和 handoff rationale，应由 Agent 在行为规范指导下 best effort 上报到 Turn。
- Agent 私有记忆、runtime-local scratchpad、token-level reasoning 和 chain-of-thought，默认不是 CommonGround truth。
- 工作知识的解释与提炼，属于 Human、LLM-enabled Agent、外部系统、projection 和 Knowledge Team，不属于 Kernel。

所以 CommonGround 不是把所有历史粗暴存成 memory dump。

它要提供的是：

> Turn-owned、可追溯、可审计、可解释的公开工作事实。

这些事实可以成为当前 Agent 复盘自己、其他 Agent 学习相似任务、组织知识沉淀、routine / playbook / case / eval 生成的原材料，但它们不会仅仅因为被保存就自动获得正式效力。

这意味着当前系统是 memory-ready，而不是 memory-complete。更高层的 Memory Abstraction、search、dossier surface、routine、playbook 与 learning workflow 可以建立在 durable facts 之上，但它们默认不是 Kernel truth。

## CommonGround 负责什么

CommonGround Kernel 负责保存和协调最小公共事实：

- Agent identity
- Turn birth / lifecycle / terminal outcome
- claim、heartbeat、fencing
- Turn-owned semantic records
- dispatch 与 child lineage
- final result
- ledger / feed / pull-first observation basis

它保证这些事实可以被恢复、观察和审计。

围绕 Kernel，上层可以继续构建：

- projection / dashboard
- Agent directory
- turn offers
- runtime companion
- management portal
- Knowledge Team
- search / index / dossier
- organization learning substrate

但这些上层能力不能反向定义 Kernel truth。

## CommonGround 不是什么

CommonGround Kernel 明确不是：

- Agent 的大脑
- Agent 私有记忆系统
- LLM reasoning recorder
- runtime / container / workspace 管理器
- PMO 或中心调度器
- 自动路由引擎
- 消息总线
- 靠 push notification 保证正确性的事件系统
- 知识解释或自动 promotion 引擎

Kernel 不替 Agent 推理，不替组织做策略判断，也不因为某条记录被持久化就让它自动产生授权、身份连续性、contract effect 或 policy effect。

如果某项材料需要产生 machine-authoritative effect，它必须被显式建模。

## 一个典型工作流

一个简单的 CommonGround 工作流大致是：

1. 外部请求进入系统，形成 root Turn。
2. 目标 Agent claim 这个 Turn。
3. Agent 读取 Turn context，推进工作。
4. 如果需要协作，parent Agent 显式 dispatch child Turn。
5. child Agent claim child Turn，完成自己的工作并写出 final deliverable。
6. parent 通过 durable feed / lineage / snapshot 观察 child 结果。
7. parent 决定是否吸收 child 结果，并继续推进。
8. parent finish root Turn，写出 terminal result。
9. Human、Agent 或 Knowledge Team 后续读取 closed Turns，做审计、解释、复盘或知识提炼。

这条链路的重点不是“谁收到了通知”，而是：

> 即使通知丢失、runtime 重启、进程迁移，系统仍能从 durable facts 回到正确的工作判断。

## 当前仓库里有什么

在当前 v3r1 主线里：

- `CommonGround/`
  - contracts、kernel、infra、sdk、adapters、service 的主实现
- `Integrations/nanobot/`
  - external runtime / companion / dynamic provisioning 的主线集成示例
- `examples/`
  - BYOA、work-memory reporter、skill 与 NanoBot integration examples
- `tests/`
  - 当前真实回归入口
- `docs/`
  - 宪法、三平面、设计审查、Turn 工作知识愿景、设计文档与指南
- `CG-Cardbox/`
  - PostgreSQL schema reset 路径使用的 CardBox submodule

当前主线已经具备：

- agent-only execution model
- canonical dispatch
- claim / heartbeat / finish
- suspend / resume
- child dispatch / lineage / parent observe
- Turn-owned semantic records
- projection read surface
- NanoBot external runtime integration
- multi-hop local-only orchestration demo

## 继续阅读

如果你是第一次接触 CommonGround，建议继续按这个顺序读：

1. [how-to-read-this-repo.md](how-to-read-this-repo.md)
2. [../01-constitution.md](../01-constitution.md)
3. [../02-three-plane-model.md](../02-three-plane-model.md)
4. [../03-design-review-principles.md](../03-design-review-principles.md)
5. [../cg-history.md](../cg-history.md)
6. [../release-notes.md](../release-notes.md)

如果只记住一句话：

> CommonGround Kernel 让多 Agent 真实工作变成 durable public facts 和可复用工作知识，同时不假设超出约束所要求的内容。
