# PMO Service 实现规范

本文属于 L1 内核层，描述 PMO/ExecutionService 作为资源单写者与执行编排者的职责。

## 1. 角色与职责
- **资源单写者**：维护 `resource.*`（projects/roster/tools/profiles/plan 等）。
- **身份供给**：通过 ProvisionIdentity 维护 `resource.project_agents` 与 `state.identity_edges`。
- **执行编排**：通过 Enqueue/Join/Report 写入 `state.agent_inbox` 与 `state.execution_edges`。
- **回收与守门**：回收超时 turn，推进 `turn_epoch` 使旧 turn 失效。

## 2. 并发模型与状态约束
- **单实例多协程**：单进程/单实例内启动固定数量协程，从内部队列并发处理事件与命令。
- **严格无状态**：严禁在 `self` 上保存任何与单次事件/命令相关的可变业务状态。
- **状态归属**：业务状态必须只存在于 WorkItem、函数局部变量或持久化存储中。
- **允许的 self 内容**：不可变配置、连接池/客户端、队列、日志器等基础设施对象。
- **锁的使用边界**：锁仅可用于无隔离需求的共享统计/限流/资源池管理；不得以“先共享业务态再加锁”替代隔离。

## 3. Enqueue（执行请求）
1) 生成 `DispatchRequest`（`context_box_id`/`output_box_id` 已拼装在上下文层；spawn 只声明 `target_agent_id`/可选 `target_channel_id`，child `agent_turn_id` 由 L0 分配）。
2) L0 `enqueue(message_type="turn")` 在同一事务写入：
   - `state.agent_inbox`（payload 指针化）
   - `state.execution_edges`（`primitive=enqueue`, `edge_phase=request`；若提供 `correlation_id`，它承载业务/编排相关性，而不是 child `agent_turn_id`）
3) 成功持久化后发 `cmd.agent.{target_agent_id}.wakeup`（或保留待触发语义，由 NATS/L0 负责最终唤醒边界）。
4) 返回 `DispatchResult`（`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/可选 `turn_epoch`/`trace_id`）。

## 3.1 ExecutionService / Dispatcher（统一派发器）
- 位置：`infra/agent_dispatcher.py`（当前统一派发入口，兼容 L0 事务语义）。
- 目标：统一 **出箱、写 inbox、记录执行边、发 wakeup**，避免协议分叉。
- 输入：`DispatchRequest`（`source_ctx`、`target_agent_id`、可选 `target_channel_id`、`profile_box_id`、`context_box_id`、可选 `correlation_id`、`output_box_id`、`lineage_ctx`）。
- 输出：`DispatchResult`（accepted/busy/rejected/error + `agent_turn_id`/可选 `turn_epoch`/`trace_id`）。
- 行为：
  - `ensure_box_id` 以保证 `output_box_id` 可写。
  - 写 `state.agent_inbox` + `state.execution_edges`。
  - 发布 `cmd.agent.{target}.wakeup`（目标由 `infra.agent_routing` 解析）。
  - 可选发 `evt.agent.*.state`（`emit_state_event=true`）。
- 适用调用方：PMO 内部 handler / `BatchManager`。

## 4. 回收与抢救
- 超时/崩溃：回收 state 并 `turn_epoch+=1` 使旧 turn 永久失效。
- state 丢失：可用最新 snapshot 重建（best-effort）。

## 5. BatchManager（内建编排）
- 写入 `state.pmo_batches` / `state.pmo_batch_tasks`。
- 分支（fork）：
  - `fork_join` 先通过 `BatchCreateRequest` 生成稳定 `task_specs`，并在 `state.pmo_batch_tasks` 中按 `task_index` 持久化 `parent->children` 关系。
  - 激活时先写 `join` 原语（`edge_phase=request`），再并发调度子任务。
  - 子任务通过 `AgentDispatcher` 派发，写 `agent_inbox` 并发出 `wakeup`。
  - 父侧跟踪应以 `agent_turn_id` 为主；`turn_epoch` 只是运行时 lease 细节，应从权威运行时状态解析，而不是做 inbox archaeology。
- 汇总（join）：
  - 子任务状态变化上报后，`BatchManager.handle_child_task_event` 会把任务置为终态（`success`/`failed`/`timeout` 等）并尝试收口。
  - `fail_fast` 生效时一旦有失败即终止批次；否则等待全部任务终态后判定 `success|partial|failed|timeout`。
  - 合并完成时先写 `join` 原语（`edge_phase=response`，`correlation_id=batch_id`），再向 parent 的 `tool_call_id` 写 `tool_result` 进行回写/通知。

## 6. 订阅与发布
- 发布：`cmd.agent.*.wakeup`（门铃）。
- 订阅：`evt.agent.*.task/step/state`（按需）。


---


# PMO Context Handling（Context Packing / Handover）

上下文打包的权威文档见 [`docs/CN/03_kernel_l1/pmo_context_handling.md`](../03_kernel_l1/pmo_context_handling.md)。

对 `v1r4` 而言，这里的关键运行时契约只有几条：

- 跨 Agent 上下文共享仍必须显式发生
- 继承 box 现在必须预授权，不能只做 exists 校验
- `clone` 可以授权来源 output box，但这份授权必须显式传递
- 当前内建编排仍通过统一的 `pack_context` 路径装配 context

本文件有意只保留 PMO 编排契约，把详细的 context packing 规则收敛到单独文档，避免两份规则继续漂移。
