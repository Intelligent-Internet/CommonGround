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
1) Generate `DispatchRequest` (`context_box_id`/`output_box_id` are assembled at context layer; `agent_turn_id` can be optionally issued by PMO).
2) L0 `enqueue(message_type="turn")` writes in one transaction:
   - `state.agent_inbox` (payload-pointer style)
   - `state.execution_edges` (`primitive=enqueue`, `edge_phase=request`, usually `correlation_id=agent_turn_id`)
3) After successful persistence, publish `cmd.agent.{target_agent_id}.wakeup` (or retain deferred wake semantics, with NATS/L0 handling final wakeup behavior).
4) Return `DispatchResult` (`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/`turn_epoch`/`trace_id`).

## 3.1 ExecutionService / Dispatcher (Unified Dispatcher)
- Location: `infra/agent_dispatcher.py` (current unified dispatch entry, compatible with L0 transaction semantics).
- Goal: unify **outbox, inbox write, execution edge recording, wakeup emission** to avoid protocol divergence.
- Input: `DispatchRequest` (`project/channel/agent/profile/context/output/trace/parent`, etc. as pointers).
- Output: `DispatchResult` (`accepted`/`busy`/`rejected`/`error` + `agent_turn_id`/`turn_epoch`/`trace_id`).
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
- Join / aggregate:
  - After child task status changes are reported, `BatchManager.handle_child_task_event` marks the task terminal (`success`/`failed`/`timeout`, etc.) and attempts closure.
  - With `fail_fast` enabled, any failure immediately terminates the batch; otherwise waits for all tasks to reach terminal states to decide `success|partial|failed|timeout`.
  - On merge completion, write the `join` primitive first (`edge_phase=response`, `correlation_id=batch_id`), then write back/notify to parent `tool_call_id` via `tool_result`.

## 6. Subscriptions and Publishing
- Publish: `cmd.agent.*.wakeup` (doorbell).
- Subscribe: `evt.agent.*.task/step/state` (as needed).

---


# PMO Context Handling (Context Packing / Handover)

This document belongs to the L1 kernel layer and describes **how PMO constructs `context_box_id` for downstream AgentTurn** (that is, context packing / handover). It is currently the authoritative implementation entry and extension point for cross-agent shared context.

## 1. Core Mental Model (P0)

- **central storage does not imply default visibility**: Cards/Boxes are stored centrally in CardBox/PG per project, but a single Agent Turn's LLM only sees the snapshot assembled into `context_box_id` plus this turn's appended `output_box_id` content; Workers do not automatically read other boxes in the project.
- **no ACL today (TODO)**: The system currently has no box-level ACL; any service with project CardBox/DB access can read a box once it has a `box_id`. `box_id` is not a security boundary.
- **cross-agent sharing must be explicit**: context sharing is not automatic hydration of full project history; it happens only when a `box_id` is explicitly passed and a new `context_box_id` is generated (via handover or explicit tool-based `delegate_async` / `fork_join` assembly).

## 2. Components and Boundaries

- **Implementation location**:
  - `services/pmo/handover.py::HandoverPacker.pack_context` (handover)
  - `services/pmo/internal_handlers/delegate_async.py` (strategy + `context_box_id`)
  - `services/pmo/internal_handlers/fork_join.py` (task-level context assembly)
- **callers (current)**:
  - handover path: `launch_principal`
  - direct assembly path: `delegate_async` / `fork_join`
- **output**: a new `context_box_id` (pointing to an ordered set of `card_ids`), and resolved `target_profile_box_id`.
  - Note: `pack_context` only assembles **context_box**; `output_box_id` is determined by dispatch/worker side. In concurrent scenarios, output box for branching should be assigned by upper-orchestration layer.

## 3. `pack_context` Assembly Rules (Current Implementation)

`pack_context(project_id, tool_suffix, source_agent_id, arguments, handover)` currently works as follows:

1) **Resolve target profile**
   - Prefer `arguments.profile_name` first; otherwise use `handover.target_profile_config.profile_name`.
   - Resolve to `profile_box_id` through `resource_store.find_profile_by_name` (current implementation requires successful resolution).

2) **Write arguments as cards (`pack_arguments`)**
   - Each rule in `handover.context_packing_config.pack_arguments[]` writes `arguments[arg_key]` as a Card.
   - `as_card_type` defines card type (default `task.instruction`).
   - `card_metadata.role` defaults to `user`; `author_id` is `tool_suffix` (for audit visibility of who packed it).
   - **`task.result_fields` constraint**: must be a list of field objects (at minimum `name` / `description`), wrapped as `FieldsSchemaContent`; non-list values are rejected with BadRequest.

3) **Inherit existing box `card_ids` (`inherit_context`)**
   - `handover.context_packing_config.inherit_context.include_boxes_from_args = ["input_box_ids", ...]`
   - Read corresponding fields from `arguments`; a field can be a single `box_id` or a list of `box_id`s.
   - PMO reads these boxes and appends their `card_ids` to the new context, flattening then deduplicating while preserving order.

4) **Optional parent pointer (`include_parent`)**
   - If `inherit_context.include_parent` is true, add a `meta.parent_pointer` card pointing to `source_agent_id`.

5) **Create new Context Box**
   - Save the assembled `card_ids` as a new box, yielding `context_box_id`, and return `(context_box_id, target_profile_box_id, attached_card_ids)`.

## 4. Existing Built-in Recipes (Examples)

These recipes show that handover is a **customizable** point:

- `launch_principal` (`services/pmo/internal_handlers/launch_principal.py`)
  - pack: `instruction -> task.instruction`
  - do not inherit other boxes
- `fork_join` now performs direct inline context-box assembly (no longer depends on handover).
  - each task always writes `instruction -> task.instruction`
  - current task input model supports `context_box_id` and `target_strategy=clone|reuse|new`; `clone` appends the source agent current output box for inheritance.

## 4.1 Direct Assembly Path (Non-handover)

- `delegate_async`
  - chooses target identity by `target_strategy=new|reuse|clone`
  - `context_box_id` can merge an existing box as inheritance input with current instruction
  - `clone` additionally inherits all historical cards from the source agent's current output box

## 5. Custom Extension Points (Current Recommendation)

> Goal: make cross-agent context sharing an **explicit, auditable, and reproducible engineering workflow**, not implicit concatenation.

- **add/modify PMO internal handler**: add a new internal handler for a new business flow and define your own `handover` assembly strategy in the handler (`pack_arguments` / `inherit_context` / `include_parent`, etc.), then call `pack_context`.
- **future direction (TODO)**: make part of handover logic configurable or toolized so upper layers (for example, Principal/business tools) can trigger controlled “read/inherit box” actions; ACL + audit policy must be added before rollout.

## 6. Common Misconceptions (Cause of design/PRD misreads)

- “Agent boxes are private storage” — incorrect: storage is centralized; “private” is a runtime visibility contract (content not visible unless assembled into context).
- “Knowing a `box_id` means the LLM can see content” — incorrect: the LLM sees only this turn's `context_box_id` and its own `output_box_id`; reading/inheriting others' boxes requires PMO/tools to assemble them.
- “`channel_id` isolates cards/boxes” — incorrect: channel mainly controls flow/visibility domain; cards/boxes are still fetched by `project_id` in the current system.

## 7. Related Documents

- `01_getting_started/architecture_intro.md` (Box visibility)
- `04_protocol_l0/state_machine.md` (Dual-Box constraints)
- `03_kernel_l1/agent_worker.md` (Worker hydration boundaries)
