# 状态机与生命周期

本文是 L0 权威定义，描述 AgentTurn 状态机、原子 I/O 顺序、交付契约与 Inbox/Wakeup 约束。
若需完整的 Agent/PMO I/O 契约，请参见 `agent_pmo_protocol.md`。

> 拓扑概念与模式请参见 `01_getting_started/architecture_intro.md`；实现与行为以本 L0 协议为准。

## 1. 核心不变量
- 同一 `agent_id` 严禁并发（单活跃 Turn）。
- 所有 state 更新必须受 `turn_epoch + active_agent_turn_id` 门禁保护。
- 终态必须存在 `task.deliverable`，并在 `evt.agent.*.task` 返回 `deliverable_card_id`。
- **Inbox 为执行面真源**：执行请求/回执必须写入 `state.agent_inbox`，NATS 仅用于唤醒。
- **一条 Enqueue 对应一个 Turn**：每个 Enqueue 由 **L0** 绑定独立 `agent_turn_id/turn_epoch` 并完成一次交付（`turn_epoch` 允许在 queued→pending 的派发阶段生成）。
- **每个 Turn 仅产生一次 `evt.agent.*.task`**。
- **Wakeup header 仅为门铃**：语义字段以 inbox 记录为准，header 不作为审计真源。

## 2. 状态机（state.agent_state_head.status）
枚举：`idle` → `dispatched` → `running` ↔ `suspended` → `idle`

- **idle**：空闲，无活跃 Turn。
- **dispatched**：Turn 已领取/派发，等待 Worker 进入处理。
- **running**：Worker 正在处理（包含 LLM 思考与工具调用）。
- **suspended**：已发起异步等待（典型为工具回调），等待集合以 `turn_waiting_tools` + `waiting_tool_count` 追踪。
- `suspended` 不再默认通过单一 `expecting_correlation_id` 做强绑定；当前主 worker 流程在 suspend 时将其置空，并以等待列表驱动恢复。

观测信号（非状态机门禁）：
- `evt.agent.*.state.metadata.activity`：`thinking | executing_tool | awaiting_tool_result`（best-effort）。
- `evt.agent.*.state.metadata.current_tool`：当前工具名（可选）。
- `evt.agent.*.state.metadata.waiting_tool_count`：待回执工具数（可选）。

状态字段：
- `expecting_correlation_id`：保留字段。当前主 worker 通路在 `suspended` 下通常清空，仅某些边界场景（如 UI worker）会写入动作相关 ID。
- `waiting_tool_count`：待完成工具数（`suspended` 的实际恢复依据之一）。
- `resume_deadline`：SUSPENDED 的超时恢复时间（到期自动注入超时报告）。

## 3. 原子 I/O 顺序（L0）

### 3.1 Enqueue（ExecutionService/PMO）
- 写入 `state.agent_inbox`（指针化 payload + trace/lineage）。
- 记录 `state.execution_edges`（`primitive="enqueue"`, `edge_phase="request"`）。
- 发布 `cmd.agent.{target}.wakeup`（控制面门铃）。
- `message_type="turn"` 走 L0 入队：
  - `inbox` 初始写为 `status='queued'`，无可用 `turn_epoch`。
  - 待 agent 空闲时，L0 `dispatch_next` 会 `lease` 成为 `dispatched`，并将该入队 `status` 从 `queued` promote 成 `pending`（同时回写 `turn_epoch`）。
- `message_type="ui_action"`（以及部分上层直推）会先做 `lease_agent_turn_with_conn`，将 head 直接 `dispatched`，再写 `inbox`（通常为 `pending`）。
 
### 3.1.1 边定义（execution_edges）
- `enqueue`（`primitive="enqueue"`，`edge_phase="request"`）：所有入队记录。
- `report`（`primitive="report"`，`edge_phase="response"`）：`tool_result/timeout` 等回执入库时记录。
- `join`（`primitive="join"`，`edge_phase="request|response"`）：用于子任务协作 join 语义记录。

### 3.2 Wakeup + Claim（Worker）
- 收到 wakeup 后 **先 claim pending inbox**（`FOR UPDATE SKIP LOCKED`，按 `created_at ASC`）。
- state 更新采用 **CAS 门禁**（`turn_epoch + active_agent_turn_id`），不依赖 state 行锁。
- Worker 不得自行生成 `agent_turn_id/turn_epoch`；inbox 缺失或非法视为协议错误（应拒绝处理）。
- `status=idle`：由 **L0** 在入队/lease 时生成/接管 `agent_turn_id` 并 `turn_epoch+=1`，写入 `trace_id/parent_step_id`，置 `status=dispatched`。
- `status=idle`（mailbox）：若存在 queued turn，由 **L0** 在 `dispatch_next` 阶段完成 lease 与 `queued→pending` 推进后唤醒处理。
- `status=dispatched`：进入处理后转 `running`。
- `status=running`：进入 **greedy batch**，按时间序取 `pending` inbox 批量处理。
- `status=suspended`：不会再按单一 `correlation_id` 过滤 claim 队列；唤醒后按实际消息路由（含 `tool_result/timeout/stop`）进入处理。
> Greedy batch 并不意味着“一个 Turn 处理多个 Enqueue”。每条 Enqueue 都由 **L0** 生成新的 `agent_turn_id/turn_epoch`；Worker 只消费该 Turn 并完成交付后回到 `idle`，再处理下一条 Enqueue。

### 3.3 Tool Call（Worker）
- 写 `tool.call` 卡。
- 发布 `cmd.tool.*` 或 `cmd.sys.pmo.internal.*`。
- 若 `after_execution=suspend`：
  - 设置 `waiting_tool_count`，记录 `resume_deadline`。
  - 写入/更新 `turn_waiting_tools`（`tool_call_id` 及 `step_id`）作为恢复集合。
  - 通常将 `expecting_correlation_id` 置空（兼容分支除外）。
  - `status=running → suspended`。
- 若 `after_execution=terminate`：继续执行并在写出交付后回到 `idle`。
- 注：**有效 after_execution** 以 `tool.result.content.result.__cg_control.after_execution` 为准（若存在且合法）。

### 3.4 Report（Tool/Child → Inbox）
- Report 被写入 `state.agent_inbox`（`message_type` 语义化：`tool_result/timeout/stop/...`，携带 `correlation_id`，主 resolver 为 `tool_call_id`）。
- `tool_result/timeout` 入队后走 resume 流程：
  - `resume_handler` 校验 `state= suspended` 且 `tool_call_id` 在当前 `turn_waiting_tools` 中。
  - 对应等待项接收后更新状态；若仍有待恢复项则继续 `suspended`（保留或更新 `resume_deadline`），否则按 `after_execution` 结果进入 `running` 或 `idle`。
  - `expecting_correlation_id` 一般不参与主流程匹配。

### 3.5 Deliver（Worker）
- 写 `task.deliverable`。
- 发布 `evt.agent.*.task`。
- 清理 `active_agent_turn_id` 并回到 `idle`。

### 3.6 超时恢复（Watchdog）
- 若 `status=suspended` 且 `NOW() > resume_deadline`：
  - 由 **Worker 看门狗**按 `turn_waiting_tools` 发现超时项，构造 `message_type=timeout` 的 Report（`correlation_id`=超时的 `tool_call_id`），入 `state.agent_inbox`。
  - 同步清理/记录 `resume_deadline`，唤醒 worker；resume 流程继续处理。
> 更完整的兜底与超时机制说明见 `04_protocol_l0/watchdog_safety_net.md`。

## 4. 结果交付契约（L0）

### 4.1 终态必须有 deliverable
- 任意终态（success/failed/stop/watchdog）必须 best-effort 写入 `task.deliverable`。
- `evt.agent.*.task` 必须携带 `deliverable_card_id`。
- `deliverable_card_id` 必须属于 `output_box_id` 的 card 列表。

### 4.2 交付结构
- `task.deliverable.content` 可为文本或字段列表（`fields[]`）。
- 如上游下发 `task.result_fields`，交付应覆盖 required fields。
- **`task.result_fields` 结构**：必须是字段对象列表并封装为 `FieldsSchemaContent`。
- `evt.agent.*.task` 不承载派生语义内容（summary/fields/links 等）；派生内容应通过回读 `deliverable_card_id` 获得。

### 4.3 stop / watchdog 也需交付
- 非正常结束路径（stop/watchdog）必须 best-effort 追加 deliverable 并返回 `deliverable_card_id`。

### 4.4 submit_result（Worker 内建工具）
- 不走 UTP，不依赖 `resource.tools`。
- 约束：同一 turn 仅允许该工具调用。
- 若 `context_box_id` 内存在 `task.result_fields`，需做最小字段校验。

## 5. Dual-Box（Simplified Task Memory）

- **context_box_id**：只读输入快照。
- **output_box_id**：只写输出容器。
- Worker 严禁向 `context_box_id` 写卡；所有新卡必须写入 `output_box_id`。

公式：`Output_Box = Agent(Context_Box, User_Prompt)`。

补充说明（可见性边界）：
- `context_box_id` 是上游显式提供的输入快照；Worker 不会自动扫描/读取项目内其他 box。
- 跨 Agent/跨会话共享上下文，需要显式传递 `box_id` 并生成新的 `context_box_id`（例如由 PMO context packing 完成；未来也可能由工具完成）。
- 参考：`03_kernel_l1/pmo_context_handling.md`
- **`output_box_id` 作为滚动记忆**：Worker 每轮会把 `output_box_id` 中的卡并入上下文（因此输出会被读回）。
- **Box 可替换**：当需要上下文压缩/重写时，上层可以生成新的 box 并替换 `context_box_id` / `output_box_id` 指针，协议语义不要求 box_id 永远不变。
- **并发约束**：并发 turn 不得共享同一 `output_box_id`；并发场景必须复制/分叉 output box（保证单写者）。

## 6. Hydration 顺序（Worker）
1) 读取 `state.agent_state_head` 并校验门禁。
2) 读取 `resource` 视图（roster/tools/profiles）。
3) 读取 cards/boxes（`context_box_id` / `output_box_id`）。
4) 进入 ReAct 循环，遵循“写卡 → 写 state → 发事件”。

## 7. 事件与载荷摘要
- `cmd.agent.{target}.wakeup`：`agent_id`（可选 `inbox_id/metadata`）。
- Inbox `tool_result`：`agent_turn_id/agent_id/turn_epoch/tool_call_id/status/after_execution/tool_result_card_id`。
- `evt.agent.{agent_id}.task`：`agent_turn_id/status/output_box_id/deliverable_card_id`。
- `evt.agent.{agent_id}.step`：`agent_turn_id/step_id/phase`。

## 8. 异常与恢复要点
- CAS 失败：立即停止执行，不产生副作用。
- Worker 崩溃：PMO 通过超时回收，递增 `turn_epoch` 使旧指令失效。
- ToolResult 乱序/重放：必须幂等处理；重复回调仅 ACK。
