# UTP 工具协议

本文是 L0 权威定义，约束工具调用的 Subject、Payload、时序与幂等规则。  
与 `Tool`、`internal tool` 的实际承载模型保持一致，`submit_result`（Worker 内建）不走 UTP。

## 1. 适用范围
- 覆盖：PMO 内建工具（`cmd.sys.pmo.internal.*`）与 Tool Service 工具（`cmd.tool.*`）。
- 不覆盖：Worker 内建流程工具（例如 `submit_result`）。

## 2. `resource.tools`（参考表结构）
代码中 `resource.tools` 的核心字段：`project_id/tool_name/description/parameters/target_subject/after_execution/options`。

## 3. 生命周期与幂等

### 3.1 `after_execution` 语义
- `suspend`：`tool result` 返回/超时/取消后可回到运行期，并继续或终止由回调内容决定。
- `terminate`：收到对应 tool 结果后，流程通常直接结束本 turn 并产出 deliverable。
- 覆盖约定：`tool.result.content.result.__cg_control.after_execution` 优先于 `tool.result`/报告中的 `after_execution`（若存在且合法）。

### 3.2 幂等规则
- 推荐以 `(agent_turn_id, tool_call_id)` 做回调去重锚点。
- `tool_call_id` 一致并按同一 `tool_call_card_id` 重入时应保证幂等行为。
- 回调幂等以 `tool.call` 关联上下文为准，不建议以 inline payload 字段做二次判等。

## 4. NATS Subject 与 Payload

### 4.1 工具命令（Worker → Tool Service / PMO）
- Subject：`resource.tools.target_subject`（通常是 `cg.<ver>.<project_id>.<channel_id>.cmd.tool.<target>...`）
- PMO 内置工具路径：`cg.<ver>.<project_id>.<channel_id>.cmd.sys.pmo.internal.<tool>`
- 最小必填（`cmd.tool.*`）：
  - `agent_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `tool_call_id`
  - `after_execution`（常见由 `resource.tools.after_execution` 推导）
  - `tool_call_card_id`（多数实现要求）
- PMO 内置最小必填：
  - `agent_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `step_id`
  - `tool_call_id`
  - `after_execution`
  - `tool_call_card_id`
  - `tool_name`（通常有值，可用于日志和审计）
- 可选：`tool_name`、`context_box_id`
- 约定：大多数 ToolService 会禁止内联参数（常见禁止字段为 `args`、`arguments`、`result`），改为通过 `tool_call_card_id` 读取 `tool.call` 卡。
- `tool_call_card_id` 为业务链路核心入口，建议保持非空；若缺失会触发验证失败路径。

### 4.2 工具回调（Tool Service / PMO → ExecutionService）
- 流程：写 `tool.result` 卡 → 走 `report` 入 Inbox → 触发 `cmd.agent.<target>.wakeup`。
- 必填字段（执行端最小要求）：
  - `tool_call_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `agent_id`
  - `tool_result_card_id`
  - `status`
  - `after_execution`（`suspend` / `terminate`）
- 推荐字段：
  - `step_id`（用于恢复重放链路建议携带）
- `status` 必须来自 `success|failed|canceled|timeout|partial`。
- 约束：回调应当不重复 inline `result`；返回语义信息请写入 `tool.result` 卡并以 `tool_result_card_id` 归档。
- Resume 消费会硬性校验 `tool_result_card_id`；`step_id` 缺失则按缺省行为回退，不会影响回放定位。

### 4.3 兼容性
- `payload` 允许扩展字段；接收端需忽略未知字段。
- 不允许篡改门禁与路由关键字段：`agent_turn_id` / `turn_epoch` / `tool_call_id` / `after_execution` / `tool_result_card_id`。
- 可通过 `result.__cg_control.after_execution` 覆盖有效 `after_execution`（保持幂等与可追溯）。
- `tool.result` 卡（`ToolResultContent`）中的 `error` 与 `status` 是失败语义主要来源；建议回调 payload 仅承载路由与指针字段。

## 5. 结果与错误约定
- 建议将结果语义写入 `tool.result` 卡（CardStore 真源）。
- 常见错误码（可扩展）：`tool_timeout` / `auth_failed` / `bad_request` / `upstream_unavailable` / `internal_error`。
- 推荐错误结构（`tool.result.content`）：
  - `result.error_code` / `result.error_message`
  - `error.code` / `error.message` / `error.detail`
