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
1) 生成 `DispatchRequest`（`context_box_id`/`output_box_id` 已拼装在上下文层，`agent_turn_id` 由 PMO 侧可选下发）。
2) L0 `enqueue(message_type="turn")` 在同一事务写入：
   - `state.agent_inbox`（payload 指针化）
   - `state.execution_edges`（`primitive=enqueue`, `edge_phase=request`，通常 `correlation_id=agent_turn_id`）
3) 成功持久化后发 `cmd.agent.{target_agent_id}.wakeup`（或保留待触发语义，由 NATS/L0 负责最终唤醒边界）。
4) 返回 `DispatchResult`（`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/`turn_epoch`/`trace_id`）。

## 3.1 ExecutionService / Dispatcher（统一派发器）
- 位置：`infra/agent_dispatcher.py`（当前统一派发入口，兼容 L0 事务语义）。
- 目标：统一 **出箱、写 inbox、记录执行边、发 wakeup**，避免协议分叉。
- 输入：`DispatchRequest`（project/channel/agent/profile/context/output/trace/parent 等指针）。
- 输出：`DispatchResult`（accepted/busy/rejected/error + `agent_turn_id`/`turn_epoch`/`trace_id`）。
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
- 汇总（join）：
  - 子任务状态变化上报后，`BatchManager.handle_child_task_event` 会把任务置为终态（`success`/`failed`/`timeout` 等）并尝试收口。
  - `fail_fast` 生效时一旦有失败即终止批次；否则等待全部任务终态后判定 `success|partial|failed|timeout`。
  - 合并完成时先写 `join` 原语（`edge_phase=response`，`correlation_id=batch_id`），再向 parent 的 `tool_call_id` 写 `tool_result` 进行回写/通知。

## 6. 订阅与发布
- 发布：`cmd.agent.*.wakeup`（门铃）。
- 订阅：`evt.agent.*.task/step/state`（按需）。


---


# PMO Context Handling（Context Packing / Handover）

本文属于 L1 内核层，描述 **PMO 如何为下游 AgentTurn 构造 `context_box_id`**（即 context packing / handover）。它是“跨 Agent 共享上下文”的当前权威实现入口与可自定义扩展点。

## 1. 关键心智模型（P0）

- **中心化存储 ≠ 默认可见**：Cards/Boxes 在项目内中心化存储（CardBox/PG），但单个 Agent Turn 的 LLM 只会看到被装配进 `context_box_id` 的输入快照，以及本次 Turn 的 `output_box_id` 追加内容；Worker 不会自动读取项目内其他 box。
- **当前没有 ACL（TODO）**：目前系统没有基于 box 的访问控制；任何持有项目 CardBox/DB 访问能力的服务，只要拿到 `box_id` 就能读取该 box。`box_id` 不是安全边界。
- **跨 Agent 共享必须显式发生**：共享上下文不是“自动水合全项目历史”，而是通过显式传递 `box_id` 并生成新的 `context_box_id` 来实现（可由 handover 或内部工具 `delegate_async` / `fork_join` 显式拼装）。

## 2. 组件与边界

- **实现位置**：
  - `services/pmo/handover.py::HandoverPacker.pack_context`（handover）
  - `services/pmo/internal_handlers/delegate_async.py`（strategy + context_box_id）
  - `services/pmo/internal_handlers/fork_join.py`（任务级 context 拼装）
- **调用方（现状）**：
  - handover 路径：`launch_principal`
  - 直接拼装路径：`delegate_async` / `fork_join`
- **产物**：一个新的 `context_box_id`（指向一组按顺序装配的 card_ids），以及解析得到的 `target_profile_box_id`。
  - 注意：`pack_context` 只负责 **context_box** 的装配，`output_box_id` 由 dispatch/worker 侧决定；并发场景下应由上层/编排层分叉 output box。

## 3. pack_context 组装规则（当前实现）

`pack_context(project_id, tool_suffix, source_agent_id, arguments, handover)` 的行为可概括为：

1) **解析目标 Profile**
   - 优先使用 `arguments.profile_name`；否则使用 `handover.target_profile_config.profile_name`。
   - 通过 `resource_store.find_profile_by_name` 解析为 `profile_box_id`（当前实现要求必须解析成功）。

2) **按规则把参数写成卡片（pack_arguments）**
   - `handover.context_packing_config.pack_arguments[]` 每条规则会把 `arguments[arg_key]` 写成一张 Card。
   - `as_card_type` 决定卡片类型（默认 `task.instruction`）。
   - `card_metadata.role` 缺省为 `user`；`author_id` 使用 `tool_suffix`（便于审计“是谁打包的”）。
   - **`task.result_fields` 约束**：必须是字段对象列表（至少 `name` / `description`），会被封装为 `FieldsSchemaContent`；非 list 直接拒绝（BadRequest）。

3) **继承已有 box 的 card_ids（inherit_context）**
   - `handover.context_packing_config.inherit_context.include_boxes_from_args = ["input_box_ids", ...]`
   - 会从 `arguments` 读取对应字段，字段值可以是单个 `box_id` 或 `box_id` 列表。
   - PMO 读取这些 box，并把其中的 `card_ids` 扁平化追加进新 context（去重但保序）。

4) **可选：写入父指针（include_parent）**
   - 若 `inherit_context.include_parent` 为真，则追加 `meta.parent_pointer` 卡，指向 `source_agent_id`。

5) **生成新的 Context Box**
   - 将上述 card_ids 保存为一个新的 box，得到 `context_box_id`，并返回 `(context_box_id, target_profile_box_id, attached_card_ids)`。

## 4. 现有内建配方（示例）

这些配方体现了“handover 是可自定义点（customizable）”：

- `launch_principal`（`services/pmo/internal_handlers/launch_principal.py`）
  - pack：`instruction -> task.instruction`
  - 不继承其他 box
- `fork_join` 已改为内部直接拼装 context box（不再依赖 handover）。
  - 每个 task 固定写入 `instruction -> task.instruction`
  - 当前 task 输入模型支持 `context_box_id` 及 `target_strategy=clone|reuse|new`，且 clone 会追加来源 agent 当前输出 box 继承

## 4.1 直接拼装路径（非 handover）

- `delegate_async`
  - 通过 `target_strategy=new|reuse|clone` 选择目标身份
  - `context_box_id` 可把既有 box 作为继承输入并与当前 instruction 合并
  - `clone` 会额外继承来源 agent 当前 output_box 的全部历史卡片

## 5. 自定义扩展点（当前推荐做法）

> 目标：让“跨 Agent 共享上下文”是 **显式、可审计、可复现** 的工程流，而不是隐式拼接。

- **新增/修改 PMO internal handler**：为新的业务流程新增一个 internal handler，并在 handler 中定义自己的 `handover` 组装策略（`pack_arguments` / `inherit_context` / `include_parent` 等），然后调用 `pack_context`。
- **未来方向（TODO）**：将部分 handover 逻辑配置化或工具化，让上层（例如 Principal/业务工具）在受控权限下发起“读取/继承 box”的动作；上线前必须配套 ACL/审计策略。

## 6. 常见误区（导致设计/PRD 误读）

- “Agent 的 box 是私有存储”——不准确：存储中心化；“私有”是运行时可见性语义（不被装配进 context 就不可见）。
- “只要知道 box_id，LLM 就能看到内容”——不准确：LLM 只能看到 turn 的 `context_box_id`（以及自己的 `output_box_id`）；读取/继承别人的 box 需要 PMO/工具把它装配进去。
- “channel_id 能隔离卡片/box”——不成立：channel 主要是控制流/可见域；cards/boxes 当前按 `project_id` 存取。

## 7. 相关文档

- `01_getting_started/architecture_intro.md`（Box 可见性）
- `04_protocol_l0/state_machine.md`（Dual-Box 约束）
- `03_kernel_l1/agent_worker.md`（Worker 水合边界）
