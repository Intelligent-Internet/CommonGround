# PMO Service Implementation Specification

This document belongs to the L1 kernel layer and describes the responsibilities of PMO/ExecutionService as a `resource.*` single writer and execution orchestrator.

## 1. Roles and Responsibilities
- **resource writer**: Maintain `resource.*` (projects/roster/tools/profiles/plan, etc.).
- **identity provider**: Maintain `resource.project_agents` and `state.identity_edges` via `ProvisionIdentity`.
- **execution orchestration**: Write `state.agent_inbox` and `state.execution_edges` via Enqueue/Join/Report.
- **reclaim and gatekeeping**: Reclaim timed-out turns and advance `turn_epoch` so old turns become invalid.

## 2. Concurrency Model and State Constraints
- **single instance, multi-coroutine**: Start a fixed number of coroutines per process/instance to process events and commands concurrently from an internal queue.
- **strict statelessness**: Do not store mutable business state related to individual events/commands on `self`.
- **state ownership**: Business state must exist only in WorkItem, local variables, or persistent storage.
- **allowed self contents**: Immutable config, connection pools/clients, queues, loggers, and other infrastructure objects.
- **lock boundary**: Locks may only be used for shared statistics/throttling/resource-pool management without isolation requirements; do not replace isolation by sharing business state under locks.

## 3. Enqueue (Execution Request)
1) Generate `DispatchRequest` (`context_box_id`/`output_box_id` are assembled at context layer; spawn names `target_agent_id`/optional `target_channel_id`, while child `agent_turn_id` is assigned by L0).
2) L0 `enqueue(message_type="turn")` writes in one transaction:
   - `state.agent_inbox` (payload-pointer style)
   - `state.execution_edges` (`primitive=enqueue`, `edge_phase=request`; `correlation_id` carries business/orchestration correlation when provided and is not the child `agent_turn_id`)
3) After successful persistence, publish `cmd.agent.{target_agent_id}.wakeup` (or retain deferred wake semantics, with NATS/L0 handling final wakeup behavior).
4) Return `DispatchResult` (`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/optional `turn_epoch`/`trace_id`).

## 3.1 ExecutionService / Dispatcher (Unified Dispatcher)
- Location: `infra/agent_dispatcher.py` (current unified dispatch entry, compatible with L0 transaction semantics).
- Goal: unify **outbox, inbox write, execution edge recording, wakeup emission** to avoid protocol divergence.
- Input: `DispatchRequest` (`source_ctx`, `target_agent_id`, optional `target_channel_id`, `profile_box_id`, `context_box_id`, optional `correlation_id`, `output_box_id`, `lineage_ctx`).
- Output: `DispatchResult` (`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/optional `turn_epoch`/`trace_id`).
- Behavior:
  - `ensure_box_id` ensures `output_box_id` is writable.
  - Write `state.agent_inbox` + `state.execution_edges`.
  - Publish `cmd.agent.{target}.wakeup` (target resolved by `infra.agent_routing`).
  - Optionally emit `evt.agent.*.state` (`emit_state_event=true`).
- Applicable callers: PMO internal handler / `BatchManager`.

## 4. Reclaim and Rescue
- Timeout/crash: reclaim state and apply `turn_epoch+=1` so previous turns become permanently invalid.
- State loss: latest snapshot can be used to rebuild best-effort.

## 5. BatchManager (Built-in Orchestration)
- Write to `state.pmo_batches` / `state.pmo_batch_tasks`.
- Fork:
  - `fork_join` first generates stable `task_specs` through `BatchCreateRequest`, then persists parent-to-children relationship in `state.pmo_batch_tasks` by `task_index`.
  - When activating, write `join` primitive first (`edge_phase=request`), then dispatch child tasks in parallel.
  - Child tasks are dispatched via `AgentDispatcher`; writes `agent_inbox` and emits `wakeup`.
  - Parent-side tracking should stay keyed by `agent_turn_id`; `turn_epoch` is a runtime lease detail and should be resolved from authoritative runtime state rather than inbox archaeology.
- Join / aggregate:
  - After child task status changes are reported, `BatchManager.handle_child_task_event` marks the task terminal (`success`/`failed`/`timeout`, etc.) and attempts closure.
  - With `fail_fast` enabled, any failure immediately terminates the batch; otherwise waits for all tasks to reach terminal states to decide `success|partial|failed|timeout`.
  - On merge completion, write the `join` primitive first (`edge_phase=response`, `correlation_id=batch_id`), then write back/notify to parent `tool_call_id` via `tool_result`.

## 6. Subscriptions and Publishing
- Publish: `cmd.agent.*.wakeup` (doorbell).
- Subscribe: `evt.agent.*.task/step/state` (as needed).

---


# PMO Context Handling (Context Packing / Handover)

The authoritative context-packing document is [`docs/EN/03_kernel_l1/pmo_context_handling.md`](../03_kernel_l1/pmo_context_handling.md).

For `v1r4`, the key runtime contract is:

- cross-agent context sharing remains explicit
- inherited boxes must be pre-authorized, not existence-only
- `clone` may authorize the source output box, but that authorization must be passed intentionally
- `pack_context` is still the common packing path used by current built-in orchestration flows

This file intentionally keeps only the PMO orchestration contract and defers detailed packing rules to the dedicated context-handling document, to avoid drift between two copies of the same rules.
