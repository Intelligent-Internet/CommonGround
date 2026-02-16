# 时间审计（Timing Observability）

本文档说明 Agent Worker 的时间审计/时延观测能力，覆盖队列、inbox、LLM 与 tool dispatch 的时间点与耗时字段。

## 1. 开关与配置
- 配置位置：`config.toml` → `[observability.timing]`
- `enabled`：时间审计总开关。
- `step_event`：在 `evt.agent.*.step` 的 `metadata.timing` 中附带 timing。
- `task_event`：在 `evt.agent.*.task` 的 `stats.timing` 中附带 timing。
- `stream_metadata`：在 `str.agent.*.chunk` 中附带 LLM timing。
- `tool_dispatch`：在 `cmd.<tool_target>` payload 与 `tool.call` 卡 `metadata` 中写入 `dispatch_requested_at`。
- `worker_logs`：Worker 日志输出 `queue_wait_ms` / `inbox_age_ms` 等字段。

- 行为与继承关系：
  - 子开关未显式配置时，默认取 `enabled` 的值。
  - 子开关可独立显式设置为 `true/false` 覆盖 `enabled`。
  - 运行时是否记录基础 timing 为 `timing_capture = enabled || any(step_event/task_event/stream_metadata/tool_dispatch/worker_logs)`。

> 默认值：`core/config_defaults.py` 与 `config.toml.sample` 中全部为 `false`。

## 2. 产出位置
- `evt.agent.*.step`（`timing_step_event=true`）
  - `metadata.timing`
  - 额外展平字段：`metadata.llm_request_started_at` / `metadata.llm_response_received_at` / `metadata.llm_error_at` / `metadata.llm_timing`
- `evt.agent.*.task`（`timing_task_event=true`）
  - `stats.timing`
  - 额外展平字段：`stats.llm_request_started_at` / `stats.llm_response_received_at` / `stats.llm_error_at` / `stats.llm_timing`
- `str.agent.*.chunk`（`stream_metadata=true`）
  - `chunk_type="start"`：`metadata` 中附加 `llm_request_started_at`
  - `chunk_type="end"`：`metadata.llm_timing`
  - 通过 `publish_core` 发布，不落 JetStream（核心 NATS transient）
- `cmd.<tool_target>`（`tool_dispatch=true`）
  - payload 中额外携带 `dispatch_requested_at`
- `tool.call` 卡（`tool_dispatch=true`）
  - `metadata` 中携带 `dispatch_requested_at`
- Worker 日志（`worker_logs=true`）
  - `queue_wait_ms` / `inbox_age_ms` / `inbox_claim_lag_ms` / `post_claim_queue_lag_ms`
- 补充：`evt.agent.*.task` 的常规完成路径（`react_step._complete_turn`）会包含 `stats.duration_ms`，表示该次 turn 处理时长（当前由 `react_step` 在完成时注入）。

## 3. 字段与语义
以下字段均为可选字段，时间为 UTC ISO8601（`Z` 结尾）；毫秒字段为非负整数，内部按 `max(0, ...)` 截断。

- `queue_enqueued_at`：`AgentTurnWorkItem` 进入 Worker 内部队列时间。
- `worker_dequeued_at`：Worker 从队列取出并开始处理的时间。
- `queue_wait_ms`：`worker_dequeued_at - queue_enqueued_at`。
- `inbox_created_at`：`state.agent_inbox` 记录创建时间。
- `inbox_processed_at`：`inbox` 被 claim 并标记 `processing` 的时间。
- `inbox_age_ms`：`worker_dequeued_at - inbox_created_at`。
- `inbox_claim_lag_ms`：`inbox_processed_at - inbox_created_at`。
- `post_claim_queue_lag_ms`：`queue_enqueued_at - inbox_processed_at`。
- `step_id`：当前 step id。
- `step_started_at`：step 开始时间。
- `llm.request_started_at`：LLM 请求开始时间。
- `llm.first_token_at`：首 token 到达时间（仅流式）。
- `llm.first_token_ms`：首 token 延迟（毫秒，`llm_start` 到首 token）。
- `llm.response_received_at`：LLM 完整响应接收时间。
- `llm.duration_ms`：LLM 请求端到端耗时（毫秒）。
- `llm.error_at`：LLM 调用失败时间。
- `llm_request_started_at` / `llm_response_received_at` / `llm_error_at` / `llm_timing`：将 `timing.llm` 摘出并展平到 step/task 事件顶层字段，便于检索与聚合。
- `dispatch_requested_at`：tool dispatch 发起时间（写在 `cmd.<tool_target>` payload 与 `tool.call` metadata）。

## 4. 例子
```json
{
  "agent_turn_id": "turn_01H...",
  "step_id": "step_01H...",
  "phase": "executing",
  "metadata": {
    "llm_request_started_at": "2026-02-05T08:01:24Z",
    "llm_timing": {
      "request_started_at": "2026-02-05T08:01:24Z",
      "first_token_ms": 120,
      "response_received_at": "2026-02-05T08:01:25Z",
      "duration_ms": 780
    },
    "timing": {
      "queue_enqueued_at": "2026-02-05T08:01:23Z",
      "worker_dequeued_at": "2026-02-05T08:01:24Z",
      "queue_wait_ms": 950,
      "inbox_created_at": "2026-02-05T08:01:22Z",
      "inbox_age_ms": 1200,
      "step_started_at": "2026-02-05T08:01:24Z",
      "llm": {
        "request_started_at": "2026-02-05T08:01:24Z",
        "first_token_ms": 120,
        "response_received_at": "2026-02-05T08:01:25Z",
        "duration_ms": 780
      }
    }
  }
}
```

> 说明：`timing` 为最佳努力，字段可能缺失；对下游消费请保持向前兼容。
