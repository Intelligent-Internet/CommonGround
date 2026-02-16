# UI Action Internal Flow (L1)

This document describes the internal processing flow of `cmd.sys.ui.action` as a reference for implementing the UI entry path and LLM reply pipeline. This flow is an **L1 implementation detail**; for external protocol behavior, refer to the L0 documentation.

---

## 0) UI startup: Register UI Agent (precondition)

When a UI session is created, it registers a UI agent via the management API (`worker_target=ui_worker`, does not participate in LLM/delegation), and initializes `state.agent_state_head` to `idle`.

Example highlights:
- `agent_id = ui_{user_uuid}_{session_uuid}`
- `worker_target = ui_worker`
- `tags = ["ui"]`
- `metadata.ui_user_id / ui_session_id`

---

## 1) UI → UI Worker (entry)

UI publishes via NATS:
`cg.{ver}.{project_id}.{channel_id}.cmd.sys.ui.action`

Payload should include `action_id` / `agent_id` / `tool_name` / `args` / `metadata`.
Compatibility notes:
- `agent_turn_id` / `turn_epoch` are optional; if missing, they are handled through compatibility paths during processing.

---

## 2) UI Worker (L0/L1 validation + enqueue)

Key steps in the UI Worker (`_handle_ui_action`):
- Subject/header validation through `parse_subject` / `normalize_headers`.
- Roster validation: `agent_id` exists and `worker_target=ui_worker`.
- Busy gate: if `state` exists and `status != idle`, return `busy` immediately (do not write Inbox, do not enqueue).
- Idempotency key: use `action_id`, can be overridden with `message_id`; used by wakeup phase idempotency-store reconciliation.
- Enqueue via `L0Engine.enqueue(message_type="ui_action", correlation_id=action_id, wakeup=True)`, `L0` writes `agent_turn_id / turn_epoch` to inbox for `ui_action`.
- Synchronously return `accepted / busy / rejected / error`.
- `tool.options.ui_allowed` is not validated at this entry stage; it is checked later in the wakeup phase.

UI Worker wakeup phase (`_process_ui_action`):
- If `agent_turn_id` is missing on first encounter, fall back compatibly to `turn_<action_id>`.
- `tool_definition` must exist, and `tool.options.ui_allowed == true` (default is reject).
- If `state.status != idle` and `active_agent_turn_id != agent_turn_id`, return `busy`; continuation with the same `agent_turn_id` is allowed.
- Idempotency key acquisition/readback: prevents duplicate execution under concurrency.
- Perform CAS via `state_store.update(expect_turn_epoch, expect_agent_turn_id=...)`; return `stale_turn/rejected` on failure.
- For asynchronous `dispatch` (such as `delegate_async`), set status to `suspended` and `expecting_correlation_id=action_id`.
- If `dispatch` fails immediately, rollback state to `idle` and send a failure ack back.

---

## 3) PMO Internal Tool: `delegate_async`

In the UI worker wakeup phase, `UTPDispatcher` will:
- Write `tool.call` card;
- Trigger the PMO internal handler via `cmd.sys.pmo.internal.delegate_async`.

Key steps of `DelegateAsyncHandler.handle`:
- Validate `target_strategy` / `target_ref` / `instruction`.
- Resolve target (reuse/new/clone) and profile/agent, and check dispatchability.
- Build `context_box` using `pack_context_with_instruction`, encoding `instruction` in context handover semantics.
- Start target agent `message_type="turn"` L0 enqueue through `dispatch_to_agent_transactional` + `AgentDispatcher`, binding target agent `agent_turn_id/turn_epoch` and publishing wakeup.
- On success, return `accepted + agent_turn_id + turn_epoch + output_box_id`; on failure, return `rejected/busy/error` via error codes and let UI worker forward `evt.sys.ui.action_ack`.

---

## 4) Target Agent Worker → LLM

The target agent handles `turn` on its wakeup:
- Load profile/context and run model inference;
- Write a deliverable card (assistant response);
- Publish `evt.agent.{agent_id}.task` (including `deliverable_card_id` / `output_box_id`).

---

## 5) PMO → UI Worker (tool result handback)

PMO does not publish UI action ACK state directly. Instead:
- `_reply_resume` first builds a `tool.result` card (contains `result/error/status/after_execution`).
- `publish_tool_result_report` performs `L0.report(message_type="tool_result")` and writes to the target UI agent inbox.
- L0 publishes `cmd.agent.<ui_worker>.wakeup`; UI Worker wakeup receives `message_type=tool_result` and enters finalization.

---

## 6) UI Worker → UI (ack)

For `tool.result`, UI Worker:
- Loads the `tool.result` card and appends it to `output_box`;
- Updates step status to completed/failed;
- Calls `finish_turn_idle` and publishes `evt.agent_state` (`idle`);
- Persists idempotent result and publishes `evt.sys.ui.action_ack`.
- Typical ack statuses: `done` (success) / `rejected` (failure) / `error` / `busy` (replay/concurrency guard).

---

## 7) UI reads final response

UI uses HTTP API to fetch card contents using `deliverable_card_id` / `output_box_id` and display them.

---

## 8) Manual verification (UI → Chat Agent)

Prerequisites: `services.api` / `services.pmo.service` / `services.agent_worker.loop` / `services.ui_worker.loop` are running.

1) Create project and upload profile:
```bash
curl -sS -X POST http://127.0.0.1:8099/projects \
  -H 'Content-Type: application/json' \
  -d '{"project_id":"proj_ui_chat_01","title":"UI Chat Demo","owner_id":"user_demo","bootstrap":true}'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/profiles \
  -F file=@examples/profiles/ui.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/profiles \
  -F file=@examples/profiles/chat_assistant.yaml
```

2) Register UI agent + target chat agent:
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"ui_user_01",
    "profile_name":"UI_Actor_Profile",
    "worker_target":"ui_worker",
    "tags":["ui"],
    "display_name":"UI Session",
    "owner_agent_id":"user_demo",
    "metadata":{"is_ui_agent":true},
    "init_state":true,
    "channel_id":"public"
  }'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"chat_agent_ui",
    "profile_name":"Chat_Assistant",
    "worker_target":"worker_generic",
    "tags":["partner"],
    "display_name":"Chat Agent",
    "owner_agent_id":"user_demo",
    "init_state":true,
    "channel_id":"public"
  }'
```

3) Subscribe to ack and task:
```bash
nats sub "cg.v1r3.proj_ui_chat_01.public.evt.sys.ui.action_ack"
nats sub "cg.v1r3.proj_ui_chat_01.public.evt.agent.chat_agent_ui.task"
```

4) Send UI action:
```bash
uv run python - <<'PY'
import uuid6
print(uuid6.uuid7().hex)
print(uuid6.uuid7().hex)
PY
```
Assume output:
- action_id = `AAA`

```bash
nats pub "cg.v1r3.proj_ui_chat_01.public.cmd.sys.ui.action" '{
  "action_id":"AAA",
  "agent_id":"ui_user_01",
  "tool_name":"delegate_async",
  "args":{
    "target_strategy":"reuse",
    "target_ref":"chat_agent_ui",
    "instruction":"hello chat via ui"
  },
  "metadata":{"source":"manual","client_ts":"2025-12-28T00:00:00Z"}
}'
```

5) Expected result:
- `evt.sys.ui.action_ack`: at entry you should see `status=accepted`; after tool execution you should see `status=done` (or `status=rejected` in failure cases).
- `evt.agent.chat_agent_ui.task`: `status=success` and includes `deliverable_card_id`.
