# NATS 通信协议

本文是 L0 权威定义，规范 Subject、Header、Payload 以及可靠性与门禁约束。

## Turn / Step 定义（与 OpenAI 略有差异）
- **Turn**：一次完整交付闭环（从一次 Enqueue/用户输入出发，到最终 `task.deliverable` 结束）。  
  - 对齐 OpenAI 的 “用户输入 → assistant 最终回复” 这一层，但 **允许非用户触发**（如内部编排/派生），因此更广义。  
- **Step**：Turn 内的一次推理/工具迭代（ReAct 的单步），产生 `agent.thought` / `tool.call` / `tool.result` / `task.deliverable` 等卡片与事件。  
  - 对齐 OpenAI 的 “step/iteration”，但这里 **Step 仅描述单次推理迭代，不等同于一次完整交付**。  

## 1. Subject 格式（权威）

```
cg.{ver}.{project_id}.{channel_id}.{category}.{component}.{target}.{suffix}
```

- `ver`：协议版本（与 `core.config.PROTOCOL_VERSION` 对齐）
- `project_id`：租户/项目隔离
- `channel_id`：可见域/权限域（如 `public` / `u_{user}` / `t_{thread}`）；**不作为 CardBox 存储隔离维度**（cards/boxes 仍按 `project_id` 存取）
- `category`：`cmd` / `evt` / `str`
- `component`：`agent` / `sys` / `user` / `tool`
- `target`：路由目标（能力组或具体 `agent_id` / tool provider）
- `suffix`：动作/类型（`wakeup` / `turn` / `task` / `chunk` 等）

统一占位符格式示例：`cmd.agent.{target}.wakeup`。

## 2. 传输与可靠性

### 2.1 JetStream（cmd/evt）
- `cmd` 与 `evt` 走 JetStream（可靠投递）。
- 默认 stream：
  - `cg_cmd_{PROTOCOL_VERSION}`：`cg.{PROTOCOL_VERSION}.*.*.cmd.>`（保留 24h）
  - `cg_evt_{PROTOCOL_VERSION}`：`cg.{PROTOCOL_VERSION}.*.*.evt.>`（保留 7d）
- `subject_pattern()` 生成的范围与上述一致（`target`/`suffix` 支持通配、`suffix` 常见用于 `wakeup`/`task`/`step` 等）。
- 语义推进与回放的真源仍是 PG/CardBox；JetStream 只承载事件边沿。

### 2.2 Core NATS（str）
- `str` 走 Core NATS（低延迟、可丢失）。
- 仅用于流式输出与观测，不用于状态推进。
- `core.nats_client.publish_core`/`subscribe_core` 直接使用 Core NATS，不带 JetStream、`queue_group` 或持久化；不要求 `traceparent`。
- `str` 的消费方也不要把 `str` 作为状态变更触发条件。

## 3. Header 约定（L0 必遵）

除 `str.*` 外，`publish_event` 侧对 `cmd/evt` 有以下自动行为：
- `traceparent` 必有；若入参缺失，会补齐。
- 可选透传或补齐：`tracestate`。
- 自动注入：`CG-Timestamp` / `CG-Version` / `CG-Msg-Type` / `CG-Sender`（如缺失才补齐）。
- `CG-Recursion-Depth` 仍是 L0 阻尼核心约束，由调用链/入口 normalize 时统一校验与回填；缺失、非整数或负数在需要严格要求时会被视为 `ProtocolViolationError`。

可选扩展：`CG-User-ID` / `CG-Channel-ID` / `CG-Context-ID`（当前实现未做统一语义消费）。

约束：建议业务 payload 内不放 `CG-*` 前缀键；当前仅在 `publish_event` 发出告警，不会硬拒绝。

### 递归深度与阻尼规则（L0 必遵）
- 适用范围：`cmd.*` / `evt.*` / tool 回调；`str.*` 不适用。
- 非法处理：当调用链明确要求深度校验时，`CG-Recursion-Depth` 缺失、非整数或为负数会触发 `ProtocolViolationError`。
- 递增规则：
  - 用户入口任务从 `0` 起（例如 UI/外部入口触发的第一跳）。
  - `enqueue` 当前实现只支持 `mode=call`；`transfer/notify` 会返回 `ProtocolViolationError`。
  - `enqueue` 本身不做 `+1`，调用方若需要下钻语义应先在链路上通过 `next_recursion_depth` 提前计算再写入 header。
  - `enqueue` 会尝试按 `correlation_id` 回填/校验深度；若与 `correlation_id` 上一次记录不一致则报错。
  - `tool_result`/`timeout` 的 `report` 与 `join_response` 会要求深度与 `correlation_id` 对应值严格匹配。
- 阈值与拒绝：
  - 阈值来自 `[worker].max_recursion_depth`（默认 20）。
  - 若 `depth >= threshold`，发布端或消费端必须拒绝派单并中止调用链。
- 失败语义（建议统一）：
  - 递归超限：`evt.agent.{agent_id}.task` 置 `status=failed` + `error_code=recursion_depth_exceeded`。
  - 协议违规：`evt.agent.{agent_id}.task` 置 `status=failed` + `error_code=protocol_violation`。

### Trace 规则（权威）
- 以下规则适用于 `cmd.*` / `evt.*` / tool 回调；`str.*` 不适用。
- `traceparent` 为核心追踪字段；`trace_id` 由 `traceparent.trace-id` 派生并持久化。
- **发布侧会确保 `traceparent`**（缺失时会生成）；`traceparent` 变化代表链路衔接。
- tool service 回调时应**透传入站 `traceparent`**；Report 原语（ExecutionService）负责生成子 span 并写入 Inbox。
- 禁止仅在 payload 里传 trace id（payload 仅作兼容/调试）。
- fanout/join 场景通常沿用原始 `trace-id`，保持跨分支可追踪。
- 若调用外部 HTTP，建议注入 `traceparent` / `tracestate`。

## 4. 幂等与门禁（L0 核心不变量）

### 4.1 AgentTurn 门禁（CAS）
- 任意推进 state 的写入必须满足 `turn_epoch + active_agent_turn_id` 门禁。
- `UPDATE ... WHERE turn_epoch=? AND active_agent_turn_id=?` 受影响 0 行即视为过期/脑裂，必须停止副作用。

### 4.2 ToolResult 幂等
- Worker 以 `(agent_turn_id, tool_call_id)` 去重工具回调。
- 执行端允许重复投递同一 tool_result 以防丢消息。

## 5. 关键 Subject 家族与载荷

### 5.1 `cmd.agent.{target}.wakeup`
- 方向：PMO/ExecutionService → Worker
- 必填：`agent_id`
- 可选：`inbox_id` / `reason` / `metadata`
- 说明：**控制面门铃**，不承载语义数据；语义请求与回执存放在 `state.agent_inbox`。

### 5.2 Inbox `tool_result`（Report）
- 方向：Tool/PMO → ExecutionService（写入 Inbox + wakeup）
- 必填：`tool_call_id` / `agent_turn_id` / `turn_epoch` / `agent_id`
- 必填：`after_execution`（`suspend|terminate`）
- 必填：`status`（`success|failed|canceled|timeout|partial`）
- 必填：`tool_result_card_id`（指针）
- 禁止：inline `result`
- 说明：回调写入 `state.agent_inbox`（Report），由 Worker 从 Inbox 处理。
- 说明：若 `tool.result.content.result.__cg_control.after_execution` 存在，将覆盖有效 `after_execution`（保留字段）。

### 5.3 Inbox `stop`（Cancel）
- 方向：PMO/BatchManager → ExecutionService（写入 Inbox + wakeup）
- 必填：`agent_id` / `agent_turn_id` / `turn_epoch`
- 可选：`reason`

### 5.4 `cmd.tool.*`
- 方向：Worker → Tool Service
- subject 来自 `resource.tools.target_subject`
- 必填：`tool_call_id` / `agent_turn_id` / `turn_epoch` / `agent_id`
- 常见可选：`tool_name` / `after_execution` / `tool_call_card_id` / `context_box_id` / `dispatch_requested_at`
- 约束：工具入口中 `ToolCommandPayload` 默认禁止 `args` 直传；通常应携带 `tool_call_card_id` 并让卡片表达具体参数。

### 5.5 `cmd.sys.pmo.internal.*`
- 方向：Worker → PMO
- 与 `cmd.tool.*` 语义对齐（仍走 `resource.tools.target_subject` 解析到 `internal.*` 工具），执行者为 PMO 内建 handler。

### 5.6 `evt.agent.{agent_id}.step`
- 必填：`agent_turn_id` / `step_id` / `phase`
- 可选：`metadata`（如 `metadata.timing`，见 `05_operations/observability.md`）
- 用途：观测与审计

### 5.7 `evt.agent.{agent_id}.task`
- 必填：`agent_turn_id` / `status`
- 可选：`deliverable_card_id` / `output_box_id` / `tool_result_card_id` / `error` / `error_code` / `stats`（如 `stats.timing`，见 `05_operations/observability.md`）

### 5.8 `str.agent.{agent_id}.chunk`
- 必填：`agent_turn_id` / `step_id` / `chunk_type` / `content`
- 可选：`index` / `metadata`（如 `metadata.llm_request_started_at` / `metadata.llm_timing`）
- 说明：`str` 不携带 trace headers，不做 trace；仅依赖 payload 字段用于关联/观测。

### 5.9 `evt.agent.{agent_id}.state`
- 必填：`agent_id` / `agent_turn_id` / `status` / `turn_epoch` / `updated_at`
- 可选：`output_box_id` / `metadata`
- 语义：状态边沿信号，真源为 `state.agent_state_head`

### 5.10 `cmd.sys.ui.action`
- 方向：UI → UI Worker
- 必填：`action_id` / `agent_id` / `tool_name` / `args`
- 可选：`metadata` / `message_id` / `parent_step_id`
- 规则：
  - `action_id` 通常作为幂等键；`message_id` 可作为替代 idempotency 键。
  - 如提供 `message_id`（UUIDv7），UI Worker 可使用其作为幂等/关联键
  - `agent_id` 必须为 `worker_target=ui_worker`；工具必须 `ui_allowed=true`（缺省拒绝）
  - UI Worker 会发布 `evt.sys.ui.action_ack` 作为回执事件

### 5.11 `evt.sys.ui.action_ack`
- 方向：UI Worker → UI
- 必填：`action_id` / `agent_id` / `status`
- 可选：`message_id` / `tool_result_card_id` / `error_code` / `error`
- 语义：确认接收/待确认（pending）/拒绝/忙碌/完成
  - `status` 通常为 `accepted|busy|rejected`；兼容实现也可能回传其他字符串状态。

## 6. 消费者约定与消费模式 (L0 必遵)

### 6.1 系统级多播 (Multicast)
- **evt.* 默认多播**：所有事件存储在 JetStream 中。不同的业务系统（如 PMO 与 Observer）必须使用不同的 `durable_name`。
- NATS 会为每个唯一的 `durable_name` 维护独立的消费进度（Cursor）。这意味着增加一个监控者不会影响业务逻辑的事件接收。

### 6.2 业务组内竞争 (Competitive Consumption)
- **水平扩展支持**：同一业务系统的多个实例应共享相同的 `durable_name` 和 `queue_group`（Pull Consumer 下由 Durable 保证）。
- **效果**：同一事件在同一个 `durable_name` 下只会被投递给一个实例，实现负载均衡。

### 6.3 消费模式对比表
| 类型 | 物理实现 | 语义模式 | 是否持久化 | 隔离级别 |
| :--- | :--- | :--- | :--- | :--- |
| `cmd.*` | JetStream | **Work Queue** | 是 | 组内竞争 (唯一执行) |
| `evt.*` | JetStream | **Event Stream** | 是 | 系统级多播 / 组内竞争 |
| `str.*` | Core NATS | **Broadcast** | 否 | 完全广播 (Fire-and-forget) |

### 6.4 细节约定
- `cmd.*`：建议稳定 durable + queue group。
- `evt.*`：是否回放由业务决定；聚合场景可用 `deliver_policy=all`。

## 6.5 配置映射（当前实现）

- `[nats]`：
  - `servers`、`tls_enabled`、`cert_dir` 对应 NATS 客户端连接配置。
- `[nats.pull]`：
  - `batch_size`、`max_inflight`、`fetch_timeout_seconds`、`warmup_timeout_seconds` 对应 `NATSClient` pull 拉取行为（`core/app_config.py` `NATSConfig` 允许额外字段透传）。

## 7. 参考清单
- 自动生成 subject 出现点：`../../REFERENCE/nats_subjects.generated.md`（仅索引，不替代本文语义）
