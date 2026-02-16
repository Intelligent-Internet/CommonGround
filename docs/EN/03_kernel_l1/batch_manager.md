# Batch Orchestration Engine

This document describes the current `fork_join` BatchManager behavior in the L1 kernel layer, aligned with implementation (`services/pmo/internal_handlers/fork_join.py` + `services/pmo/l1_orchestrators/batch_manager.py`).

## 1. Goal and Scope
- Manages concurrent dispatching of forked child tasks, child event aggregation, timeout reclamation, and callback to the parent caller.
- Does not handle business-layer final consolidation or external persistence writes; `fork_join` only returns a unified `tool.result` payload to the caller.
- Batch semantics are driven by metadata and task state in the `pmo_batch` table; state transitions are persisted idempotently.

## 2. `fork_join` Input Contract (Current Implementation)
The current seed/code uses internal Pydantic model validation (`_ForkJoinArgs`):
- `tasks` (required, array, at least 1 item)
  - `target_strategy`: `new | reuse | clone`
  - `target_ref`: string
  - `instruction`: string
  - `context_box_id`: optional string (can be empty; still packed through context)
- `fail_fast` (optional, boolean, default `false`)
- `deadline_seconds` (optional, number, must be `> 0`)

Note: `retry_batch_id` and `retry_task_indexes` are no longer supported in the current implementation, and "historical batch retry" parameters are no longer available.

Example:
```json
{
  "tasks": [
    {
      "target_strategy": "new",
      "target_ref": "Associate_Search",
      "instruction": "Search for RAG evaluation papers from the last three months"
    },
    {
      "target_strategy": "reuse",
      "target_ref": "agent_search_2",
      "instruction": "Summarize reproducible experiment parameters"
    }
  ],
  "fail_fast": true,
  "deadline_seconds": 300
}
```

Change notes:
- Legacy fields such as `agent_profile/profile_name/module_id/task_id/agent_id/provision/conversation_mode/context_mode` are no longer part of the `fork_join` input contract.
- `fork_join` validation rejects unknown fields (`Pydantic extra="forbid"`).

## 3. Fork Construction and Dispatch Flow
1. `ForkJoinHandler.handle` parses and validates arguments, then gets parent recursion depth.
2. For each task it executes `preview_target`:
   - `new`: validates target profile and creates/starts a derivative instance in `materialize_target` (`provision=True`).
   - `reuse`: requires reusing an existing agent; duplicate `target_ref` is not allowed in the same batch.
   - `clone`: constructs and creates a derived instance from source agent output with `provision=True`.
3. `pack_context_with_instruction` generates a separate `context_box_id` for each task, carrying the current task instruction and inherited context.
4. `BatchManager.create_batch` creates:
   - one row in `state.pmo_batches` (`status=running`)
   - multiple rows in `state.pmo_batch_tasks` (`status=pending`)
   - `task_index` is stored in each task's `metadata.task_index` to ensure stable result mapping and avoid dependency on DB row order.
5. `activate_batch` first ACKs the parent `join(request)` and then triggers `dispatch_pending_tasks`.

## 4. `dispatch_pending_tasks` and Dispatch Retry
`dispatch_pending_tasks` only schedules tasks that meet all conditions:
- `status='pending'`
- `retryable=TRUE`
- `next_retry_at <= NOW()`
- parent batch `status='running'`

For each dispatch attempt:
- Generates a stable `agent_turn_id` and atomically reserves the task with `claim_task_dispatched`.
- `AgentDispatcher.dispatch` behavior:
  - `rejected` or `error` with `error_code != internal_error` is treated as deterministic failure, directly `mark_task_dispatch_failed` (`retryable=FALSE`), then attempts to finalize the batch.
  - `busy` or `error` (retryable) reorders into pending with exponential backoff (2, 4, 8, 16, 30). After 5 retries, it is marked failed (`dispatch_retry_exhausted`).
  - Success with both `agent_turn_id` and `turn_epoch` writes `current_agent_turn_id/current_turn_epoch`.
  - Success without epoch remains `dispatched`, waiting for subsequent ACK reconstruction.

Concurrency control:
- Each `dispatch_pending_tasks` batch applies dual throttling via `watchdog_limit` and `dispatch_concurrency`.

## 5. Fork Reconciliation (reconcile / refill)
`BatchManager.reconcile_dispatched_tasks` runs two passes per tick:
- After 2 seconds: scan tasks where `status='dispatched'` and `current_turn_epoch IS NULL`, read `agent_inbox` (`correlation_id = agent_turn_id`) and fill in turn epoch.
- After 60 seconds: for tasks still missing epoch, record warnings and metadata (`dispatch_warning_*`) only; trigger `dispatch_next` when needed. If the batch is `fail_fast` or the deadline has passed and inbox is not visible, mark task failed as `downstream_unavailable` and finalize the batch.

Important: the implementation explicitly states "do not auto rollback retries" to avoid duplicate replay from late ACKs.

## 6. Child Task Results and Batch Failure Aggregation
`handle_child_task_event` looks up the `task` by `agent_turn_id`, then:
- reads `deliverable_card_id`, extracts `result_fields` or `content` as `digest` (as `summary`);
- if `tool_result_card_id` exists and `structured_output=tool_result`, parses tool result payload;
- sets `task.status` to the event `status` (mapped to terminal set for batch tasks);
- calls `_maybe_finalize_batch`.

## 7. Terminal Batch Resolution and Parent Callback
`_maybe_finalize_batch` finalization strategy:
- If `fail_fast=True` and any task reaches failure set (`failed/canceled/timeout`), it immediately:
  - marks the batch `failed`
  - calls `_terminate_children` (stop/cancel all reachable child tasks)
  - calls `abort_remaining_tasks` (pending/dispatched)
  - writes back `tool_result` to parent
- When all tasks reach terminal states, batch status is set by terminal-state evaluation:
  - all success -> `success`
  - partial success with other non-fully-success states -> `partial`
  - contains failure but no success -> `failed`
  - contains timeout but no success/failed -> `timeout`
  - only `partial` -> `partial`
- Then sends parent `join(response)` plus a bell `tool_result`.

## 8. Timeout Reclamation (timeout)
`reap_timed_out_batches` scans running batches with `deadline_at < NOW()`:
1. On successful `claim_batch_terminal(final_status=timeout)`, it runs:
   - `_terminate_children`: sends stop to active child tasks with `turn_epoch`; for inbox-only tasks, cancel if status is `queued`, or extract epoch from inbox and send stop.
   - `abort_remaining_tasks`: marks remaining `pending/dispatched` as `canceled`
   - parent callback with `timeout`

## 9. Data Model (Postgres)
Key fields in actual schema (see `scripts/setup/init_db.sql`):
- `state.pmo_batches`
  - `status`: `running/success/failed/timeout/partial`
  - `task_count`, `deadline_at`, `metadata`
- `state.pmo_batch_tasks`
  - `status`: `pending/dispatched/success/failed/canceled/timeout/partial`
  - `attempt_count`, `retryable`, `next_retry_at`
  - `current_agent_turn_id/current_turn_epoch`, `output_box_id`, `error_detail`, `metadata` (includes `task_index`, `result_view`, etc.)

## 10. Parent Response Format
After batch completion, parent receives this via `tool.result`:
```json
{
  "status": "success|partial|failed|timeout",
  "results": [
    {
      "task_index": 0,
      "status": "success|failed|timeout|canceled|partial",
      "summary": "...",
      "output_box_id": "box_xxx",
      "error": "..."
    }
  ]
}
```

Notes:
- `summary` is derived from parseable `deliverable/result_fields`, using `summary` or a short text digest of `result_fields`.
- `output_box_id`, `summary`, and `error` are all optional; failed tasks try to include an error code when available (such as `downstream_unavailable`, `dispatch_*`, `missing_deliverable_card_id`).
