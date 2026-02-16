# UTP Tool Protocol

This is the L0 authoritative definition, constraining tool-call `Subject`, `Payload`, sequencing, and idempotency rules.
It aligns with the actual carrier models for `Tool` and `internal tool`; `submit_result` (Worker built-in) does not follow UTP.

## 1. Scope
- Covered: PMO built-in tools (`cmd.sys.pmo.internal.*`) and Tool Service tools (`cmd.tool.*`).
- Not covered: Worker built-in workflow tools (for example, `submit_result`).

## 2. `resource.tools` (reference table schema)
Core fields in code for `resource.tools`: `project_id/tool_name/description/parameters/target_subject/after_execution/options`.

## 3. Lifecycle and Idempotency

### 3.1 `after_execution` semantics
- `suspend`: after returning/timeouts/cancellation from `tool result`, execution can return to running and continue or stop based on callback content.
- `terminate`: after receiving the corresponding tool result, the workflow typically ends the current turn directly and emits the deliverable.
- Override rule: `tool.result.content.result.__cg_control.after_execution` takes precedence over `tool.result`/report `after_execution` when present and valid.

### 3.2 Idempotency rules
- It is recommended to use `(agent_turn_id, tool_call_id)` as the callback deduplication anchor.
- A repeated call with the same `tool_call_id` re-entering through the same `tool_call_card_id` must remain idempotent.
- Callback idempotency should be based on the `tool.call` context relation, not by re-comparing inline payload fields.

## 4. NATS Subject and Payload

### 4.1 Tool command (Worker → Tool Service / PMO)
- Subject: `resource.tools.target_subject` (usually `cg.<ver>.<project_id>.<channel_id>.cmd.tool.<target>...`)
- PMO built-in tool path: `cg.<ver>.<project_id>.<channel_id>.cmd.sys.pmo.internal.<tool>`
- Minimum required (`cmd.tool.*`):
  - `agent_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `tool_call_id`
  - `after_execution` (usually derived from `resource.tools.after_execution`)
  - `tool_call_card_id` (required by most implementations)
- PMO built-in minimum required:
  - `agent_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `step_id`
  - `tool_call_id`
  - `after_execution`
  - `tool_call_card_id`
  - `tool_name` (usually set, useful for logs and audit)
- Optional: `tool_name`, `context_box_id`
- Convention: most ToolService implementations forbid inline parameters (commonly banned fields are `args`, `arguments`, and `result`) and instead read parameters from `tool.call` via `tool_call_card_id`.
- `tool_call_card_id` is the core business-path entry and should be kept non-null; missing it triggers validation failure path.

### 4.2 Tool callback (Tool Service / PMO → ExecutionService)
- Flow: write `tool.result` card → go through `report` into Inbox → trigger `cmd.agent.<target>.wakeup`.
- Required fields (minimum execution-side requirements):
  - `tool_call_id`
  - `agent_turn_id`
  - `turn_epoch`
  - `agent_id`
  - `tool_result_card_id`
  - `status`
  - `after_execution` (`suspend` / `terminate`)
- Recommended:
  - `step_id` (carry for replay/resume path)
- `status` must be one of `success|failed|canceled|timeout|partial`.
- Constraint: callbacks should not inline `result`; semantic information should be written to `tool.result` card and archived using `tool_result_card_id`.
- Resume consumption strictly validates `tool_result_card_id`; if `step_id` is missing, execution falls back to default behavior and does not impact replay location.

### 4.3 Compatibility
- `payload` may contain extension fields; receivers must ignore unknown fields.
- Fields that must not be changed: `agent_turn_id` / `turn_epoch` / `tool_call_id` / `after_execution` / `tool_result_card_id`.
- Effective `after_execution` can be overridden via `result.__cg_control.after_execution` while keeping idempotency and traceability.
- In `ToolResult` card (`ToolResultContent`), `error` and `status` are the primary sources of failure semantics; callback payloads should carry only routing and pointer fields.

## 5. Result and error conventions
- Recommend placing result semantics in `tool.result` card (CardStore is the source of truth).
- Common error codes (extensible): `tool_timeout` / `auth_failed` / `bad_request` / `upstream_unavailable` / `internal_error`.
- Recommended error structure (`tool.result.content`):
  - `result.error_code` / `result.error_message`
  - `error.code` / `error.message` / `error.detail`
