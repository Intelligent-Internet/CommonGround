# Timeout and Fallback Net (Watchdog Safety Net)

This document is an L0/implementation reference that constrains the fallback semantics and delivery guarantees when the system encounters scenarios with "no processing", "message loss", or "timeout". The implementation can be performed by the PMO/Worker watchdog, but terminal semantics and output contracts must match this document.

> Core principle: **Inbox is the source of truth; NATS is a doorbell only**. Fallback mechanisms must not bypass Inbox and State guard constraints.

## 1. Goals and Scope
- Prevent `dispatched` / `pending` from being stuck long-term without reaching a terminal state.
- Prevent `suspended` from waiting forever for tool results.
- Prevent deadlocks in processing that make inbox tasks invisible.
- Ensure best-effort production of `task.deliverable` and `evt.agent.*.task` under timeout/error paths.

## 2. Trigger Points and Fallback Actions
### 2.1 Worker: reclaiming timeout on `agent_inbox` processing
- Condition: `state.agent_inbox.status='processing'` and `processed_at` is expired (`[worker].inbox_processing_timeout_seconds`).
- Action: update status to `pending`, clear `processed_at` (also clear `archived_at`), allowing the queue to re-acquire the item.
- Goal: prevent a worker crash/hang from occupying a message for an extended period.

### 2.2 Worker: `suspended` + `resume_deadline` timeout
- Condition: `status='suspended'` and `resume_deadline < now`, with corresponding waiting item `turn_waiting_tools.wait_status='waiting'`.
- Action:
  - For each timed-out `tool_call_id`, emit a timeout report (`message_type='timeout'`, `status=timeout`, `error.code=tool_timeout`) and write it to `state.agent_inbox`, then send `cmd.agent.<target>.wakeup`.
  - A report card persisted to disk follows the normal recovery flow via `tool.result` consumption; the watchdog itself does not directly emit terminal events.
  - The turn-level `resume_deadline` is cleared after the first injection (while keeping `status='suspended'` for other waiting items so retries can continue).
- Exception: if the corresponding `tool.call` card is missing, skip timeout injection for that item.
- `resume_deadline` calculation:
  - Defaults to `[worker].suspend_timeout_seconds`.
  - If the tool definition includes `options.suspend_timeout_seconds` or `options.timeout_seconds`, use `max` against the default value.

### 2.3 Worker: resume fallback replay (addendum)
- Each tick runs `requeue_expired_resume_ledgers`: entries in `state.turn_resume_ledger` whose lease has expired are moved from `processing` back to `pending` (retryable).
- `reconcile` rebuilds `resume_data` from resume ledger / received `tool.result`, and requeues resume messages back into the worker’s internal processing queue.

### 2.4 PMO: replay undelivered `dispatched` items for retry
- Condition: `status='dispatched'` and `updated_at < now - dispatched_retry_seconds`.
- Action: reconstruct the turn envelope and write it to execution queue (`enqueue`), then send wakeup; keep `status='dispatched'` unchanged, as this is a doorbell resend.

### 2.5 PMO: reap `dispatched` timeout to terminal state
- Condition: `status='dispatched'` and `updated_at < now - dispatched_timeout_seconds`.
- Action:
  - `finish_turn_idle(expect_status='dispatched', bump_epoch=True)`: clear `active_agent_turn_id`, `active_channel_id`, and increment `turn_epoch += 1`.
  - `emit_agent_state`: `status='idle'`, `error='dispatch_timeout'` (for observability in downstream paths).
  - `record_force_termination(reason='dispatch_timeout')`.
  - Best-effort creation of `task.deliverable` fallback card and return of `deliverable_card_id` in `evt.agent.*.task`.
  - Final event `evt.agent.*.task` `status=timeout`, `error='dispatch_timeout'`.
  - Trigger `dispatch_next` to advance the next pending item into queue.

### 2.6 PMO: reap `running` timeout to terminal state
- Condition: `status='running'` and `updated_at < now - active_reap_seconds` (current implementation only triggers on `running`).
- Action:
  - `finish_turn_idle(bump_epoch=True)`, clear running state.
  - `emit_agent_state`: `status='idle'`, `error='timeout_reaped_by_watchdog'`.
  - `record_force_termination(reason='timeout_reaped_by_watchdog')`.
  - Best-effort creation of fallback `task.deliverable`.
  - Final event `evt.agent.*.task` `status=failed`, `error='timeout_reaped_by_watchdog'`.
  - Trigger `dispatch_next` to advance the next pending item into queue.

### 2.7 PMO: `pending` inbox doorbell supplementation
- Condition: `state.agent_inbox.status='pending'` and `created_at < now - pending_wakeup_seconds`.
- Action: send `cmd.agent.<target>.wakeup` without changing inbox status; this is doorbell-only behavior.
- Additional: if `channel_id` cannot be determined and waiting reaches `pending_wakeup_skip_seconds`, add `watchdog_error='missing_channel'`, `watchdog_at`, and mark the inbox as `skipped`.

## 3. Output Contract (must be satisfied)
- PMO watchdog terminal convergence (`dispatch_timeout`, `timeout_reaped_by_watchdog`) must best-effort append `task.deliverable` (if `output_box_id` does not exist, persistence may fail).
- `emit_agent_task` terminal events should attempt to include `deliverable_card_id` to support replay and observability.
- Reclaim actions should `bump turn_epoch` so stale states naturally expire and side effects are not replayed.
- Worker watchdog side does not directly emit `evt.agent.*.task`; it only injects recovery messages and heartbeat, while terminal status still comes from the normal delivery path.

## 4. Config Items (sources of defaults)
- PMO (`services/pmo/service.py` / `services/pmo/l0_guard/guard.py`):
  - `pmo.watchdog_interval_seconds`
  - `pmo.dispatched_retry_seconds`
  - `pmo.dispatched_timeout_seconds`
  - `pmo.pending_wakeup_seconds`
  - `pmo.pending_wakeup_skip_seconds`
  - `pmo.active_reap_seconds`
- Worker (`services/agent_worker/loop.py`):
  - `worker.inbox_processing_timeout_seconds`
  - `worker.watchdog_interval_seconds`
  - `worker.suspend_timeout_seconds`
- Tool overrides: `options.suspend_timeout_seconds` / `options.timeout_seconds`

## 5. Design Constraints and Boundaries
- NATS only retains doorbell semantics; all semantic and state convergence must go through Inbox/State.
- The fallback mechanism does not claim "zero-loss" behavior, but must ensure "terminal reachability" and "observability with replay".
- The fallback mechanism does not replace scaling strategy; it only fills observable failure paths.
