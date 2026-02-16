# Traceability & Lineage（L0）

本文是 L0 权威协议，用于定义**链路追踪（trace_id）**与**跨 Agent 血缘（parent_step_id）**的最小落地规范。该协议只约束 I/O 与落库语义，不限制实现细节。

---

## 1. 术语

- **traceparent**：W3C Trace Context 头（`version-traceid-spanid-flags`），包含链路 ID 与父子调用关系。
- **tracestate**：W3C Trace Context 的可选扩展，承载厂商/系统自定义状态；不必解析，但应透传。
- **trace_id**：`traceparent` 中的 `trace-id`（32 hex），持久化到数据库与卡片元数据。
- **step_id**：单次执行步骤 ID（由 Worker 生成并写入 `state.agent_steps` 表）。
- **parent_step_id**：跨 Agent 血缘指针，指向派发当前 Agent 的父 Step（上游 `step_id`）。
- **parent_agent_turn_id/parent_turn_epoch**：仅用于调度回传目标与门禁（如 batch/fork_join），不作为血缘字段。

### 1.1 血缘与调用链分工
- **血缘主键**：`parent_step_id`（业务语义上的“由哪个 step 派生”）。
- **调用链主键**：W3C `traceparent`（观测语义上的 span 父子关系）。
- **审计还原**：以 `parent_step_id + traceparent (+ execution_edges)` 联合还原调用链；不要使用 `parent_turn_id` 口径。

---

## 2. Header 规范

- `traceparent` 为**第一公民**，所有 `cmd/evt/tool` 回调必须**按 W3C 传播**：子 span 由 **Execution 原语（Enqueue/Report/Join）** 创建并返回新的 `traceparent`；发布侧将上游 `traceparent` 作为父传入原语，并在对外发布时使用原语返回的 `traceparent`。其中 `trace-id` 为链路真源，`parent-span-id` 用于构建调用树。
- `tracestate` 为可选透传（保留跨系统状态，可忽略但不应丢弃）。
- `str.*` 不作为 L0 主调用链主线依据（主链可通过 `traceparent` + `execution_edges` 推导）。NATS 客户端发布时会在缺失头时兜底注入 `traceparent`。
- 若上游缺失 `traceparent`，`ensure_trace_headers` 与 NATS publish 都会补齐默认值；子 span 仍由 Execution 原语基于补齐后的父 `traceparent` 生成。
- `CG-Trace-ID` **已移除**，不再作为协议字段（不提供兼容路径）。

---

## 3. Payload 规范

### 3.1 `AgentTaskPayload`
- `trace_id` 为可选（由 `traceparent.trace-id` 生成；缺失时 PMO/Worker 可从 `traceparent` 补齐）。
- `parent_step_id` 可选（根派发允许 `NULL`）。

### 3.2 `Tool 回调`
- Inbox `tool_result` 必须携带 `tool_result_card_id` 与 `step_id`。
- Tool 回调应透传/补齐 `traceparent`，并由 **Report** 原语创建子 span 后注入新 `traceparent`（`tracestate` 可选）。

---

## 4. 存储规范

### 4.1 `state.agent_state_head`
- `parent_step_id` / `trace_id` 记录**当前活跃 turn**的血缘（父 step）与链路信息。

### 4.2 `state.agent_steps`
- 每个 step 写入 `parent_step_id` / `trace_id`（允许为空）。
- 每个 step 记录 `tool_call_ids`（该 step 的工具调用索引）；旧口径 `state.agent_turns.tool_call_ids` 不再使用。

### 4.3 `state.execution_edges`
- 执行原语 `enqueue` / `join` / `report` 需要落库到 `state.execution_edges`，并写入 `trace_id` / `parent_step_id` 以便跨指令链路追溯。
- 工具调用链路会写入 `primitive="tool_call"` 的边（通常是 `edge_phase=request`），用于工具调用追踪，不替代 L0 执行原语语义。
- 对于执行原语，`primitive` 与 `edge_phase` 组成时序语义，`edge_phase` 取值为 `request` / `response`。
- `source_agent_turn_id/target_agent_turn_id` 用于执行路由审计，不是血缘主键。

---

## 5. Card Metadata 规范

- Worker/PMO 产生的卡片应包含：
  - `trace_id`
  - `step_id`
  - `parent_step_id`（若存在）
- `tool.result` 卡通常需在 `metadata` 中携带 `trace_id`、`parent_step_id` 与 `step_id`（详见 `utp_tool_protocol.md`）。

---

## 6. 传播规则（最小实现）

- **PMO**
  - `cmd.sys.ui.action` / `delegate_async` / `fork_join` / `launch_principal`：透传/注入 `traceparent` 与 `trace_id` 后再执行子调度。
  - `Enqueue/Join/Report`：通常写入 `trace_id`，并尽量写入 `parent_step_id`（可空）；子 span 由原语创建，发布侧优先使用原语返回的 `traceparent`。

- **Agent Worker**
  - 若 payload 缺 `trace_id`，从 `traceparent` 读取并回写。
  - `step_store.insert_step` 写入 `parent_step_id`、`trace_id`（可空）。
  - 新卡片 metadata 携带 `trace_id/step_id/parent_step_id`。
  - 对外发布 `cmd.*` / `evt.*` 时优先使用 Execution 原语返回的 `traceparent`；非原语事件保留现有 headers，仅在需要时做 `ensure_trace_headers` 补齐。

- **Tool Service**
  - 读取 `tool.call` metadata 中的 `trace_id/parent_step_id/step_id`，必要时从 headers 做最小回填。
  - 写入 `tool.result` metadata，并通过 **Report** 原语生成子 span 后注入新的 `traceparent`。

---

## 7. 兼容性

- 旧链路允许缺失 `parent_step_id`，但会退化为“只有 trace 的链路聚合”。
- 新链路必须按本规范补齐字段，以保证可追溯性。
