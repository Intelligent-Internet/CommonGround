# State Machine and Lifecycle

This is the L0 authoritative definition, describing the AgentTurn state machine, atomic I/O ordering, delivery contract, and Inbox/Wakeup constraints.
For the complete Agent/PMO I/O contract, see `agent_pmo_protocol.md`.

> Topology concepts and patterns are in `01_getting_started/architecture_intro.md`; implementation and behavior are governed by this L0 protocol.

## 1. Core invariants
- Concurrent execution for the same `agent_id` is forbidden (single active Turn).
- All state updates must be protected by the `turn_epoch + active_agent_turn_id` gating.
- Terminal states must contain `task.deliverable`, and include `deliverable_card_id` in `evt.agent.*.task`.
- **Inbox is the execution-side source of truth**: execution requests/receipts must be written to `state.agent_inbox`; NATS is used only for wakeup.
- **One Enqueue equals one Turn**: each Enqueue is bound by L0 to an independent `agent_turn_id/turn_epoch` and performs one delivery (`turn_epoch` can be created during queued→pending dispatch stage).
- **Each Turn emits exactly one `evt.agent.*.task`**.
- **Wakeup header is only a doorbell**: semantic fields follow the inbox records, and the header is not the authoritative audit source.

## 2. State Machine (`state.agent_state_head.status`)
State enum: `idle` → `dispatched` → `running` ↔ `suspended` → `idle`

- **idle**: no active Turn.
- **dispatched**: Turn has been claimed/dispatched and is waiting for Worker entry.
- **running**: Worker is processing (including LLM reasoning and tool calls).
- **suspended**: asynchronous wait has been started (typically tool callback); waiting set is tracked by `turn_waiting_tools` + `waiting_tool_count`.
- `suspended` no longer defaults to strong binding via a single `expecting_correlation_id`; the current main worker flow clears it during suspend and resumes based on the waiting list.

Observation signals (not state-machine guards):
- `evt.agent.*.state.metadata.activity`: `thinking | executing_tool | awaiting_tool_result` (best-effort).
- `evt.agent.*.state.metadata.current_tool`: current tool name (optional).
- `evt.agent.*.state.metadata.waiting_tool_count`: number of tool receipts pending (optional).

State fields:
- `expecting_correlation_id`: reserved field. The current main worker path normally clears this in `suspended`, while some boundary scenarios (such as UI worker) may write action-related IDs.
- `waiting_tool_count`: number of tools still pending (one of the actual resume criteria for `suspended`).
- `resume_deadline`: timeout-resume time for SUSPENDED (on expiry, a timeout report is injected automatically).

## 3. Atomic I/O Sequence (L0)

### 3.1 Enqueue (ExecutionService/PMO)
- Write to `state.agent_inbox` (pointerized payload + trace/lineage).
- Record in `state.execution_edges` (`primitive="enqueue"`, `edge_phase="request"`).
- Publish `cmd.agent.{target}.wakeup` (control-plane doorbell).
- For `message_type="turn"`, route through L0 queue:
  - initial inbox write uses `status='queued'` with no `turn_epoch` available.
  - when the agent is idle, L0 `dispatch_next` will `lease` it to `dispatched` and promote the queue `status` from `queued` to `pending` (while writing back `turn_epoch`).
- For `message_type="ui_action"` (and some upper-layer direct pushes), perform `lease_agent_turn_with_conn` first, set head directly to `dispatched`, then write to `inbox` (typically `pending`).
 
### 3.1.1 Edge definitions (`execution_edges`)
- `enqueue` (`primitive="enqueue"`, `edge_phase="request"`): records all enqueue entries.
- `report` (`primitive="report"`, `edge_phase="response"`): records report ingestion such as `tool_result/timeout`.
- `join` (`primitive="join"`, `edge_phase="request|response"`): records join-semantic subtask collaboration.

### 3.2 Wakeup + Claim (Worker)
- After receiving wakeup, **claim pending inbox** first (`FOR UPDATE SKIP LOCKED`, ordered by `created_at ASC`).
- State updates use **CAS gating** (`turn_epoch + active_agent_turn_id`) and do not rely on state row locks.
- Worker must not create `agent_turn_id/turn_epoch` itself; missing or illegal inbox is a protocol error and should be rejected.
- `status=idle`: L0 generates/takes over `agent_turn_id` at enqueue/lease time and increments `turn_epoch`, writes `trace_id/parent_step_id`, and sets `status=dispatched`.
- `status=idle` (mailbox): when a queued turn exists, L0 completes lease and `queued→pending` advancement during `dispatch_next`, then wakes processing.
- `status=dispatched`: transition to `running` when entering processing.
- `status=running`: enters **greedy batch**, processing pending inbox items in time order in batches.
- `status=suspended`: no longer filters claim queue by a single `correlation_id`; after wakeup, processing proceeds by actual message routing (including `tool_result/timeout/stop`).
> Greedy batch does not mean one Turn handles multiple Enqueue entries. Each Enqueue is assigned a new `agent_turn_id/turn_epoch` by L0. The Worker consumes that Turn, completes delivery, returns to `idle`, and then processes the next Enqueue.

### 3.3 Tool Call (Worker)
- Write `tool.call` card.
- Publish `cmd.tool.*` or `cmd.sys.pmo.internal.*`.
- If `after_execution=suspend`:
  - set `waiting_tool_count`, record `resume_deadline`.
  - write/update `turn_waiting_tools` (`tool_call_id` and `step_id`) as the resume set.
  - normally clear `expecting_correlation_id` (except compatibility branches).
  - `status=running → suspended`.
- If `after_execution=terminate`: continue execution and return to `idle` after writing delivery.
- Note: **effective after_execution** is determined by `tool.result.content.result.__cg_control.after_execution` when present and valid.

### 3.4 Report (Tool/Child → Inbox)
- Report is written to `state.agent_inbox` (`message_type` semanticized as `tool_result/timeout/stop/...`, carrying `correlation_id`, with primary resolver being `tool_call_id`).
- `tool_result/timeout` after enqueue follows the resume flow:
  - `resume_handler` validates `state= suspended` and that `tool_call_id` exists in current `turn_waiting_tools`.
  - receiving corresponding waiting item updates state; if pending items remain, keep `suspended` (preserve or refresh `resume_deadline`), otherwise transition to `running` or `idle` based on `after_execution`.
  - `expecting_correlation_id` is generally not involved in primary flow matching.

### 3.5 Deliver (Worker)
- Write `task.deliverable`.
- Publish `evt.agent.*.task`.
- Clear `active_agent_turn_id` and return to `idle`.

### 3.6 Timeout Recovery (Watchdog)
- If `status=suspended` and `NOW() > resume_deadline`:
  - the **Worker Watchdog** discovers timeout items via `turn_waiting_tools`, constructs a Report with `message_type=timeout` (`correlation_id` = timed-out `tool_call_id`), and writes to `state.agent_inbox`.
  - synchronize cleanup/recording of `resume_deadline`, wake worker; the resume flow then continues processing.
> For a more complete fallback and timeout mechanism, see `04_protocol_l0/watchdog_safety_net.md`.

## 4. Delivery Contract (L0)

### 4.1 Terminal states must have deliverable
- Any terminal state (success/failed/stop/watchdog) must best-effort write `task.deliverable`.
- `evt.agent.*.task` must carry `deliverable_card_id`.
- `deliverable_card_id` must be one of the cards in `output_box_id`.

### 4.2 Delivery structure
- `task.deliverable.content` may be text or field list (`fields[]`).
- If upstream provides `task.result_fields`, delivery should cover required fields.
- **`task.result_fields` structure**: must be a list of field objects and wrapped as `FieldsSchemaContent`.
- `evt.agent.*.task` does not carry derived semantic content (summary/fields/links, etc.); derived content must be obtained by rereading `deliverable_card_id`.

### 4.3 stop / watchdog must also deliver
- Non-normal termination paths (`stop/watchdog`) must best-effort append deliverable and return `deliverable_card_id`.

### 4.4 submit_result (Worker built-in tool)
- Not UTP-based and does not depend on `resource.tools`.
- Constraint: only one call to this tool is allowed per turn.
- If `task.result_fields` exists in `context_box_id`, minimal field validation is required.

## 5. Dual-Box (Simplified Task Memory)

- **context_box_id**: read-only input snapshot.
- **output_box_id**: write-only output container.
- Worker is forbidden from writing cards into `context_box_id`; all new cards must be written to `output_box_id`.

Formula: `Output_Box = Agent(Context_Box, User_Prompt)`.

Supplementary notes (visibility boundaries):
- `context_box_id` is an upstream-explicit input snapshot; Worker does not automatically scan/read other boxes in the project.
- Cross-Agent/cross-session context sharing requires explicit passing of `box_id` and generating a new `context_box_id` (e.g., PMO context packing; may also be done by tools in the future).
- Reference: `03_kernel_l1/pmo_context_handling.md`
- **`output_box_id` as rolling memory**: each round, Worker incorporates cards from `output_box_id` into context (so outputs are read back).
- **Box replacement**: when context compression/rewrite is needed, upper layers can generate new boxes and replace `context_box_id` / `output_box_id` pointers; protocol semantics do not require box_id to remain constant.
- **Concurrency constraint**: concurrent turns must not share the same `output_box_id`; concurrent scenarios must duplicate/fork the output box (single-writer guarantee).

## 6. Hydration Order (Worker)
1) Read `state.agent_state_head` and validate gating.
2) Read `resource` views (`roster/tools/profiles`).
3) Read cards/boxes (`context_box_id` / `output_box_id`).
4) Enter ReAct loop, following "write card → write state → emit event".

## 7. Events and Payload Summary
- `cmd.agent.{target}.wakeup`: `agent_id` (optional `inbox_id/metadata`).
- Inbox `tool_result`: `agent_turn_id/agent_id/turn_epoch/tool_call_id/status/after_execution/tool_result_card_id`.
- `evt.agent.{agent_id}.task`: `agent_turn_id/status/output_box_id/deliverable_card_id`.
- `evt.agent.{agent_id}.step`: `agent_turn_id/step_id/phase`.

## 8. Failures and Recovery Focus Points
- CAS failure: stop execution immediately, do not produce side effects.
- Worker crash: PMO recovers via timeout and increments `turn_epoch` to invalidate old instructions.
- ToolResult disorder/replay: must be idempotent; duplicate callbacks should only ACK.
