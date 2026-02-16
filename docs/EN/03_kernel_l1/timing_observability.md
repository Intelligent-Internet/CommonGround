# Timing Observability

This document describes Agent Worker's timing observability and latency measurement capabilities, covering timing points and duration fields for queueing, inbox, LLM, and tool dispatch.

## 1. Switches and Configuration
- Configuration location: `config.toml` -> `[observability.timing]`
- `enabled`: global master switch for timing observability.
- `step_event`: include timing in `metadata.timing` of `evt.agent.*.step`.
- `task_event`: include timing in `stats.timing` of `evt.agent.*.task`.
- `stream_metadata`: include LLM timing in `str.agent.*.chunk`.
- `tool_dispatch`: write `dispatch_requested_at` into `cmd.<tool_target>` payload and `tool.call` card `metadata`.
- `worker_logs`: include `queue_wait_ms` / `inbox_age_ms` and related fields in Worker logs.

- Behavior and inheritance:
  - If a child switch is not explicitly configured, it defaults to the value of `enabled`.
  - A child switch can be explicitly set to `true/false` to override `enabled`.
  - Runtime base timing capture is enabled when `timing_capture = enabled || any(step_event/task_event/stream_metadata/tool_dispatch/worker_logs)`.

> Defaults: all are `false` in `core/config_defaults.py` and `config.toml.sample`.

## 2. Output Destinations
- `evt.agent.*.step` (`timing_step_event=true`)
  - `metadata.timing`
  - Additional flattened fields: `metadata.llm_request_started_at` / `metadata.llm_response_received_at` / `metadata.llm_error_at` / `metadata.llm_timing`
- `evt.agent.*.task` (`timing_task_event=true`)
  - `stats.timing`
  - Additional flattened fields: `stats.llm_request_started_at` / `stats.llm_response_received_at` / `stats.llm_error_at` / `stats.llm_timing`
- `str.agent.*.chunk` (`stream_metadata=true`)
  - `chunk_type="start"`: additional `llm_request_started_at` in `metadata`
  - `chunk_type="end"`: `metadata.llm_timing`
  - Published via `publish_core`, not persisted to JetStream (core NATS transient)
- `cmd.<tool_target>` (`tool_dispatch=true`)
  - payload includes additional `dispatch_requested_at`
- `tool.call` card (`tool_dispatch=true`)
  - `metadata` includes `dispatch_requested_at`
- Worker logs (`worker_logs=true`)
  - `queue_wait_ms` / `inbox_age_ms` / `inbox_claim_lag_ms` / `post_claim_queue_lag_ms`
- Additional: normal completion path of `evt.agent.*.task` (`react_step._complete_turn`) includes `stats.duration_ms`, indicating the duration of this turn processing (currently injected by `react_step` at completion).

## 3. Fields and Semantics
The following fields are optional. Times are UTC ISO8601 (`Z` suffix); millisecond fields are non-negative integers, clamped internally with `max(0, ...)`.

- `queue_enqueued_at`: when `AgentTurnWorkItem` enters the Worker internal queue.
- `worker_dequeued_at`: when Worker dequeues an item and starts processing.
- `queue_wait_ms`: `worker_dequeued_at - queue_enqueued_at`.
- `inbox_created_at`: time when `state.agent_inbox` record is created.
- `inbox_processed_at`: time when the inbox is claimed and marked as `processing`.
- `inbox_age_ms`: `worker_dequeued_at - inbox_created_at`.
- `inbox_claim_lag_ms`: `inbox_processed_at - inbox_created_at`.
- `post_claim_queue_lag_ms`: `queue_enqueued_at - inbox_processed_at`.
- `step_id`: current step id.
- `step_started_at`: step start time.
- `llm.request_started_at`: LLM request start time.
- `llm.first_token_at`: time the first token is received (streaming only).
- `llm.first_token_ms`: latency to first token in milliseconds (`llm_start` to first token).
- `llm.response_received_at`: time the full LLM response is received.
- `llm.duration_ms`: end-to-end LLM request latency in milliseconds.
- `llm.error_at`: time of LLM call failure.
- `llm_request_started_at` / `llm_response_received_at` / `llm_error_at` / `llm_timing`: flatten `timing.llm` into top-level fields on step/task events to simplify search and aggregation.
- `dispatch_requested_at`: tool dispatch request time, written to both `cmd.<tool_target>` payload and `tool.call` metadata.

## 4. Example
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

> Note: `timing` is best-effort; fields may be missing. Keep downstream consumers backward-compatible.
