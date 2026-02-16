# Tool Development Guide

This document is intended for tool implementers and explains how to integrate with the UTP tool flow and quickly implement a tool executor based on OpenAPI.

## 1. Core Constraints
- Tool execution endpoints must not write to `state.*`.
- Tool parameters must be read from the `tool.call` card via the `tool_call_card_id` pointer; do not read `args/arguments` directly from the payload (the current backend rejects this by protocol).
- Callbacks must include `agent_turn_id/turn_epoch/tool_call_id/after_execution` as received, unchanged.
- To override a valid `after_execution`, write `suspend|terminate` to `tool.result.content.result.__cg_control.after_execution` (preserving the namespace); callback fields still need to be carried through unchanged (including `after_execution` from both command and writeback).
- Callback tracing must create a child span using the inbound `traceparent` as the parent, and inject a new `traceparent` (`tracestate` is optional).
- `tool.result` `metadata.trace_id/parent_step_id/step_id` must be copied **verbatim from the `tool.call` card metadata** (no payload-based backfilling).
- **Content structure requirements**: `tool.call` content must be `ToolCallContent`, and `tool.result` content must be `ToolResultContent`; arbitrary dicts or text are no longer accepted as content.
- Required fields for `ToolCommandPayload` on `cmd.tool.*` include: `tool_call_id/agent_turn_id/turn_epoch/agent_id`; for external tools, `tool_call_card_id/tool_name/after_execution` are commonly required too.

The authoritative protocol is: `04_protocol_l0/nats_protocol.md`.

## 2. Minimal Implementation Flow (Any Language)
1) Subscribe to the `cmd.tool.*` subject (with `{project_id}/{channel_id}` expanded) from the tool's `target_subject` in `resource.tools`.
2) Parse the command and validate `tool_call_id/agent_turn_id/turn_epoch/agent_id/tool_call_card_id`, typically also `tool_name/after_execution`.
3) Read the `tool.call` card (`tool_call_card_id`) and fetch parameters from `ToolCallContent.arguments`.
4) Execute business logic.
5) Construct a `tool.result` card and callback payload (including `tool_call_id/agent_turn_id/agent_id/turn_epoch/after_execution/status/tool_result_card_id/step_id`).
6) Call `publish_tool_result_report` to write `tool_result` to the L0 Inbox; L0 will trigger `cmd.agent.{target}.wakeup` to the target `worker_target` of the target agent by `agent_id`.

Note: The callback wakeup target is not fixed to `worker_generic`; it must be determined by the callee agent's current `worker_target` in roster.

## 3. Python Example (Minimal Skeleton)

```python
import json

from core.utp_protocol import ToolCallContent
from core.subject import parse_subject
from infra.l0.tool_reports import publish_tool_result_report
from infra.tool_executor import ToolResultBuilder, ToolResultContext
from core.utils import safe_str


async def handle_tool_command(msg, *, cardbox, nats, execution_store, resource_store=None, state_store=None):
    data = json.loads(msg.data)
    parts = parse_subject(msg.subject)
    if parts is None:
        raise RuntimeError("invalid subject")

    tool_call_id = data["tool_call_id"]
    agent_turn_id = data["agent_turn_id"]
    turn_epoch = data["turn_epoch"]
    after_exec = data.get("after_execution")
    tool_call_card_id = data["tool_call_card_id"]
    project_id = parts.project_id

    cards = await cardbox.get_cards([tool_call_card_id], project_id=project_id)
    if not cards:
        raise RuntimeError(f"tool_call_card not found: {tool_call_card_id}")
    tool_call_card = cards[0]
    args = {}
    if isinstance(tool_call_card.content, ToolCallContent):
        args = dict(tool_call_card.content.arguments or {})

    tool_call_meta = getattr(tool_call_card, "metadata", {}) or {}

    try:
        result = do_something(args)
        status = "success"
    except Exception as exc:
        result = {
            "error_code": "internal_error",
            "error_message": str(exc),
            "error": {"code": "internal_error", "message": str(exc)},
        }
        status = "failed"

    ctx = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=data,
        tool_call_meta=tool_call_meta,
    )
    payload, result_card = (
        ToolResultBuilder(ctx, author_id="tool.example", function_name=safe_str(data.get("tool_name")) or "unknown")
        .build(status=status, result=result)
    )
    await cardbox.save_card(result_card)

    await publish_tool_result_report(
        nats=nats,
        execution_store=execution_store,
        resource_store=resource_store,
        state_store=state_store,
        project_id=project_id,
        channel_id=parts.channel_id,
        payload=payload,
        headers={},
        source_agent_id="tool.example",
    )
    return {
        "tool_call_id": tool_call_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": turn_epoch,
        "after_execution": after_exec,
        "status": status,
    }
```

## 3.1 Error Structure Recommendation (Normalization)
- Prefer the `CGError` system from `core/errors.py` and generate using `build_error_result_from_exception`:
  - `result.error_code` / `result.error_message`
  - `error.code` / `error.message` / `error.detail`
  - This allows UI/Worker/PMO to parse errors and status consistently.

## 3.2 Tool Definition and Registration (`resource.tools`)
- Prefer uploading definitions via management API:
  - `POST /projects/{project_id}/tools` (YAML)
  - Fields: `tool_name / target_subject / after_execution / parameters / options / description`
- `ToolService` constraints for external tools:
  - `after_execution` can only be `suspend` or `terminate`
  - `target_subject` must be `cmd.tool.*` (not `cmd.sys.*`)
  - Tool name cannot be a PMO internal tool name (`delegate_async` / `launch_principal` / `ask_expert` / `fork_join` / `provision_agent`)
- Tool definitions are persisted in `resource.tools` with these fields: `project_id, tool_name, parameters, target_subject, after_execution, options`.
- The worker side resolves definitions by `target_subject` and writes `after_execution + tool_call_card_id + params` into the `cmd.tool.*` command; when Tool Service writes back, it injects trace/step lineage from the `tool.call` card metadata.

## 4. OpenAPI Tools (Jina Search/Reader)

`services/tools/openapi_service.py` has migrated to the Jina API:
- Search docs: `https://s.jina.ai/docs`
- Reader docs: `https://r.jina.ai/docs`

Recommend using the `jina` namespace in `resource.tools.options`:

```json
{
  "args": {
    "defaults": {"session_id": "{agent_turn_id}"},
    "fixed": {"project_id": "{project_id}"}
  },
  "envs": {"api_key": "SEARCH_API_KEY"},
  "jina": {
    "mode": "search",
    "base_url": "https://s.jina.ai",
    "path": "/search",
    "method": "GET",
    "auth_env": "JINA_API_KEY"
  }
}
```

Recommendations:
- `mode=search` uses `s.jina.ai`; `mode=reader` uses `r.jina.ai`.
- Authentication keys should only be read from environment variables (default `JINA_API_KEY`), and must never be persisted to cards/logs.
- `openapi_service` only allows access to the two domains `s.jina.ai` / `r.jina.ai`.

### Defaults / Fixed / envs
- `options.args.defaults`: inject only when LLM does not provide the parameter; explicit `null` is treated as provided.
- `options.args.fixed`: forcibly overwrite and do not expose to LLM.
- `options.envs`: parameter name -> environment variable name; only the variable name is recorded in cards, while actual value is injected by the executor at runtime.
- Tool executors can override `envs` with values from service configuration (service-side configuration takes priority).

Template variables (temporarily supporting only ID fields): `{project_id}` / `{channel_id}` / `{agent_id}` / `{agent_turn_id}` /
`{step_id}` / `{tool_call_id}` / `{parent_step_id}` / `{trace_id}` / `{turn_epoch}` / `{context_box_id}`.
