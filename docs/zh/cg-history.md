# CommonGround 历史沿革

本文是背景材料，用来解释当前 CommonGround 为什么长成现在这样；它不是实现 contract。

当前规则请以 [01-constitution.md](01-constitution.md)、[02-three-plane-model.md](02-three-plane-model.md) 与 [03-design-review-principles.md](03-design-review-principles.md) 为准。

## 1. 为什么 CommonGround 从强管控退出来

早期 CommonGround 设计带有更强的 orchestration 假设：系统可以拥有 workflow 结构、任务拆解方式，以及大部分参与 Agent 的生命周期。

随着外部 Agent 变得更强、更独立，这个假设越来越不适合实际协作。很多有能力的 Agent 活在其他 runtime、产品、工具或组织里。CommonGround 不能假设自己拥有它们的内部记忆、调度器、进程模型或生命周期。

因此设计压力变得很清楚：

- 保留共享协作底座；
- 不再假装底座拥有所有参与者；
- 记录事实和边界，而不是直接控制所有行为；
- 让外部 Agent 能协作，但不必被吸收到中央层级里。

## 2. 账本化转向

关键简化是把 CommonGround 视为 ledger-like substrate。

真正值得 durable 记录的问题变成：

- 哪个稳定 Agent identity 正在参与？
- 哪个 durable work boundary 被创建或推进？
- 产生了哪些语义事实、引用和结果？
- 不同工作之间有什么因果关系？

这让 CommonGround 从硬编码 workflow 假设转向共享协作记录。

重要的产品洞见不是“不要 orchestration”，而是“把 orchestration 放到正确层级”。Supervisor、provisioner、planner、reviewer、human operator、portal 与 runtime companion 都可以存在，但除非它们确实属于 durable CommonGround law，否则不应变成隐藏的 kernel ontology。

## 3. 非侵入式记忆

历史上最重要的一次修正，是拒绝 invasive memory write。

早期设计允许下级工作直接修改 parent 或 requester 的 memory space。这会制造紧耦合，也让责任难以审计。

当前方向保留了更好的部分：

- 工作产生 record；
- 结果可以被检查；
- parent、requester 或后续 Agent 可以自己决定信任、复用、总结或忽略什么；
- 中间 observation 不会自动变成另一个 Agent 的 private memory 或 final truth。

这也是 Turn-owned semantic record 和 public work knowledge 重要的原因。它们提供可复用 evidence，但不假装所有 consumer 都必须自动接受结果。

## 4. 为什么当前 kernel 很小

当前 CommonGround foundation 刻意保持 formal kernel 很小：

- Agent 是稳定逻辑执行主体。
- Turn 是最小 durable work boundary。
- 完整语义归属于 Turn。
- Projection 可以帮助 reader，但 projection 不是 truth。
- Push 可以加速感知，但 pull/read 才是正确性基线。

这是主要的宪法收敛。CommonGround 不是整个 socio-technical organization 本身，而是让多个组织、runtime 与 Agent 协作的共享协调内核。

## 5. 哪些东西移出了 kernel

很多有价值的东西仍然重要，但应位于 kernel 之上或旁边：

- team topology；
- planning and critique loop；
- provisioning policy；
- runtime-local subagent；
- operator policy；
- product UX；
- portal 与 management read model；
- private agent memory。

把它们放在 kernel 外不是降级，而是避免 implementation convenience 被偷偷固化成永久法律。

## 6. 现在应该怎样理解这段历史

这段历史真正值得保留的主线是：

1. 从强 orchestration 与 workflow 假设出发。
2. 发现强外部 Agent 不能被底座安全拥有。
3. 把 CommonGround 收缩到 durable facts、boundary、identity 与 causality。
4. 在这个基础之上，用显式上层结构重新构建大规模协作。

旧 service route、compatibility window 与 credential experiment 的细节不是当前真源。除非新设计明确重新采纳，否则它们不应回到 active documentation surface。
