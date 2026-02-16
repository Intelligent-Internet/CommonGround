import json
import logging
import uuid6
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Tuple

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.utp_protocol import Card, FieldsSchemaContent, JsonContent, TextContent
from core.status import STATUS_FAILED, STATUS_SUCCESS
from infra.cardbox_client import CardBoxClient
from infra.tool_executor import ToolResultBuilder, ToolResultContext

from .models import ActionOutcome

logger = logging.getLogger("AgentWorker.BuiltinTools")

BUILTIN_AFTER_EXEC = {
    "submit_result": "terminate",
}


def _build_tool_result_card(
    *,
    project_id: str,
    cmd_data: Dict[str, Any],
    tool_call_meta: Dict[str, Any],
    status: str,
    result: Any,
    error: Any,
    author_id: str,
    function_name: str,
    after_execution: str,
) -> Card:
    result_context = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
    )
    builder = ToolResultBuilder(
        result_context,
        author_id=author_id,
        function_name=function_name,
        error_source="worker",
    )
    _, card = builder.build(
        status=status,
        result=result,
        error=error,
        function_name=function_name,
        after_execution=after_execution,
    )
    return card

def build_submit_result_tool_spec(*, required_fields: List[Dict[str, str]]) -> Dict[str, Any]:
    fields_lines: List[str] = []
    for f in required_fields or []:
        name = f.get("name") or ""
        desc = f.get("description") or ""
        if name:
            fields_lines.append(f"- {name}: {desc}".rstrip())
    fields_desc = "\n".join(fields_lines) if fields_lines else "- (no required fields provided)"

    description = (
        "Submit the final result and finish this turn.\n"
        "Hard rule: submit_result MUST be the ONLY tool call in this turn.\n\n"
        "submit_result will be rejected if the agent inbox still has pending messages.\n\n"
        "Fill the required fields with plain-text values:\n"
        f"{fields_desc}"
    )

    return {
        "type": "function",
        "function": {
            "name": "submit_result",
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {
                    "result": {
                        "type": "object",
                        "properties": {
                            "fields": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "value": {"type": "string"},
                                    },
                                    "required": ["name", "value"],
                                },
                            }
                        },
                        "required": ["fields"],
                    }
                },
                "required": ["result"],
            },
        },
    }


def _normalize_result_fields(fields_any: Any) -> List[Dict[str, str]]:
    if not isinstance(fields_any, list):
        return []
    normalized: List[Dict[str, str]] = []
    for item in fields_any:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        desc = item.get("description")
        if not isinstance(name, str) or not name.strip():
            continue
        normalized.append(
            {
                "name": name.strip(),
                "description": desc.strip() if isinstance(desc, str) else "",
            }
        )
    return normalized


def _extract_result_fields_from_card_content(content: Any) -> List[Dict[str, str]]:
    if not isinstance(content, FieldsSchemaContent):
        raise ProtocolViolationError("task.result_fields content must be FieldsSchemaContent")
    return _normalize_result_fields(
        [{"name": f.name, "description": f.description} for f in content.fields]
    )


def _normalize_submit_result_payload(
    result_any: Any, *, required_fields: List[Dict[str, str]]
) -> Tuple[Any, Optional[str]]:
    """Return (normalized_result, error_message)."""
    required_names = [f["name"] for f in required_fields if f.get("name")]

    if required_names:
        if isinstance(result_any, dict):
            if "fields" in result_any:
                fields_any = result_any.get("fields")
                fields: List[Dict[str, Any]] = fields_any if isinstance(fields_any, list) else []
            else:
                # Allow shorthand mapping {name: value} for convenience.
                fields = [{"name": k, "value": v} for k, v in result_any.items()]
        elif isinstance(result_any, list):
            fields = result_any if all(isinstance(x, dict) for x in result_any) else []
        else:
            fields = []

        normalized_fields: List[Dict[str, str]] = []
        seen: Dict[str, int] = {}
        for f in fields:
            name_any = f.get("name") if isinstance(f, dict) else None
            if not isinstance(name_any, str) or not name_any.strip():
                continue
            name = name_any.strip()
            value_any = f.get("value") if isinstance(f, dict) else None
            if isinstance(value_any, str):
                value = value_any
            elif value_any is None:
                value = ""
            else:
                value = json.dumps(value_any, ensure_ascii=False)
            normalized_fields.append({"name": name, "value": value})
            seen[name] = seen.get(name, 0) + 1

        missing = [n for n in required_names if seen.get(n, 0) == 0]
        if missing:
            return None, f"submit_result missing required fields: {missing}"
        dupes = [n for n, c in seen.items() if c > 1]
        if dupes:
            return None, f"submit_result has duplicate field names: {dupes}"

        # Keep stable order: required first, then extras.
        required_set = set(required_names)
        ordered: List[Dict[str, str]] = []
        for n in required_names:
            ordered.extend([f for f in normalized_fields if f["name"] == n])
        ordered.extend([f for f in normalized_fields if f["name"] not in required_set])
        return {"fields": ordered}, None

    # No required fields: accept string or structured payload as-is.
    if isinstance(result_any, dict) and "fields" in result_any:
        fields = result_any.get("fields")
        if isinstance(fields, list):
            return {
                "fields": [
                    {"name": str(f.get("name", "")).strip(), "value": str(f.get("value", ""))}
                    for f in fields
                    if isinstance(f, dict)
                ]
            }, None
    return result_any, None


async def try_execute_builtin_tool(
    *,
    fn_name: str,
    args: Dict[str, Any],
    cardbox: CardBoxClient,
    ctx: CGContext,
    tool_call_id: str,
    context_box_id: Optional[str],
) -> Optional[ActionOutcome]:
    if fn_name != "submit_result":
        return None

    project_id = str(ctx.project_id or "")
    agent_id = str(ctx.agent_id or "")
    agent_turn_id = str(ctx.agent_turn_id or "")
    step_id = str(ctx.step_id or "")
    parent_step_id: Optional[str] = ctx.parent_step_id
    trace_id: Optional[str] = ctx.trace_id
    if not project_id or not agent_id or not agent_turn_id or not step_id:
        raise ValueError("try_execute_builtin_tool requires ctx with project/agent/turn/step ids")

    result_any = args.get("result") if isinstance(args, dict) else None
    tool_call_meta = {
        "step_id": step_id,
        **({"trace_id": trace_id} if trace_id else {}),
        **({"parent_step_id": parent_step_id} if parent_step_id else {}),
    }
    cmd_data = {
        "tool_call_id": tool_call_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 0,
        "agent_id": agent_id,
        "after_execution": "suspend",
        "tool_name": "submit_result",
    }

    if result_any is None:
        tool_result_card = _build_tool_result_card(
            project_id=project_id,
            cmd_data=cmd_data,
            tool_call_meta=tool_call_meta,
            status=STATUS_FAILED,
            result=None,
            error="submit_result requires args.result",
            author_id=agent_id,
            function_name="submit_result",
            after_execution="suspend",
        )
        await cardbox.save_card(tool_result_card)
        return ActionOutcome(cards=[tool_result_card], suspend=False, mark_complete=False)

    required_fields: List[Dict[str, str]] = []
    if context_box_id:
        try:
            box = await cardbox.get_box(str(context_box_id), project_id=project_id)
            if box and box.card_ids:
                cards = await cardbox.get_cards(list(box.card_ids), project_id=project_id)
                for c in reversed(cards):
                    if getattr(c, "type", "") == "task.result_fields":
                        required_fields = _extract_result_fields_from_card_content(
                            getattr(c, "content", None)
                        )
                        break
        except Exception as exc:  # noqa: BLE001
            logger.warning("submit_result: failed to load task.result_fields: %s", exc)

    normalized, err = _normalize_submit_result_payload(result_any, required_fields=required_fields)
    if err:
        tool_result_card = _build_tool_result_card(
            project_id=project_id,
            cmd_data=cmd_data,
            tool_call_meta=tool_call_meta,
            status=STATUS_FAILED,
            result=None,
            error=err,
            author_id=agent_id,
            function_name="submit_result",
            after_execution="suspend",
        )
        await cardbox.save_card(tool_result_card)
        return ActionOutcome(cards=[tool_result_card], suspend=False, mark_complete=False)

    cmd_data["after_execution"] = "terminate"
    tool_result_card = _build_tool_result_card(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
        status=STATUS_SUCCESS,
        result=normalized,
        error=None,
        author_id=agent_id,
        function_name="submit_result",
        after_execution="terminate",
    )
    deliverable_content = (
        JsonContent(data=normalized)
        if isinstance(normalized, (dict, list))
        else TextContent(text=str(normalized))
    )
    deliverable_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="task.deliverable",
        content=deliverable_content,
        created_at=datetime.now(UTC),
        author_id=agent_id,
        metadata={
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "role": "assistant",
            **({"trace_id": trace_id} if trace_id else {}),
            **({"parent_step_id": parent_step_id} if parent_step_id else {}),
        },
    )
    await cardbox.save_card(tool_result_card)
    await cardbox.save_card(deliverable_card)
    return ActionOutcome(cards=[tool_result_card, deliverable_card], suspend=False, mark_complete=True)
