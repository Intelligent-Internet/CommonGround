from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Optional, Sequence

import uuid6

from core.cg_context import CGContext
from core.status import STATUS_SUCCESS
from core.utp_protocol import Card, JsonContent, TextContent
from core.utils import safe_str

from .tx_utils import call_with_optional_conn, state_store_transaction


async def ensure_output_box_and_append_tool_result(
    *,
    cardbox: Any,
    state_store: Any,
    ctx: CGContext,
    expect_status: str,
    current_output_box_id: Optional[str],
    tool_result_card_id: str,
    waiting_ctx: Optional[CGContext] = None,
) -> tuple[Any, bool]:
    async with state_store_transaction(state_store) as tx:
        output_box_id = await call_with_optional_conn(
            cardbox.ensure_box_id,
            project_id=ctx.project_id,
            box_id=current_output_box_id,
            conn=tx.conn,
        )
        if output_box_id != safe_str(current_output_box_id):
            updated = await call_with_optional_conn(
                state_store.update,
                ctx=ctx,
                expect_status=expect_status,
                output_box_id=output_box_id,
                conn=tx.conn,
            )
            if not updated:
                raise RuntimeError("resume_effects: output_box CAS update failed")
        if waiting_ctx is not None:
            claimed = await call_with_optional_conn(
                state_store.mark_turn_waiting_tool_applied,
                ctx=waiting_ctx,
                tool_result_card_id=tool_result_card_id,
                conn=tx.conn,
            )
            if not claimed:
                return output_box_id, False
        output_box = await call_with_optional_conn(
            cardbox.get_box,
            output_box_id,
            project_id=ctx.project_id,
            conn=tx.conn,
        )
        existing_ids = list(getattr(output_box, "card_ids", []) or [])
        if tool_result_card_id in existing_ids:
            return output_box_id, True
        output_box_id = await call_with_optional_conn(
            cardbox.append_to_box,
            output_box_id,
            [tool_result_card_id],
            project_id=ctx.project_id,
            conn=tx.conn,
        )
        return output_box_id, True


async def collect_output_box_tool_call_ids(
    *,
    cardbox: Any,
    project_id: str,
    output_box_id: str,
    expected_ids: Sequence[str],
    dispatch_step_id: str,
) -> set[str]:
    actual_ids: set[str] = set()
    expected = {str(x) for x in expected_ids}
    output_box = await cardbox.get_box(output_box_id, project_id=project_id)
    if not output_box or not output_box.card_ids:
        return actual_ids
    output_cards = await cardbox.get_cards(list(output_box.card_ids), project_id=project_id)
    for card in output_cards:
        if getattr(card, "type", None) != "tool.result":
            continue
        call_id = safe_str(getattr(card, "tool_call_id", ""))
        if call_id not in expected:
            continue
        meta = getattr(card, "metadata", None)
        step_id_val = meta.get("step_id") if isinstance(meta, dict) else None
        if safe_str(step_id_val) != safe_str(dispatch_step_id):
            continue
        actual_ids.add(call_id)
    return actual_ids


async def ensure_terminated_deliverable(
    *,
    cardbox: Any,
    ctx: CGContext,
    output_box_id: Any,
    result_payload: Any,
    resume_status: str,
    task_status: str,
) -> tuple[Any, Optional[str], Optional[str]]:
    _ = ctx.require_agent_turn_id
    step_id = ctx.require_step_id
    tool_call_id = ctx.tool_call_id

    deliverable_card_id: Optional[str] = None
    created_card_id: Optional[str] = None
    box = await cardbox.get_box(str(output_box_id), project_id=ctx.project_id)
    if not box or not box.card_ids:
        return output_box_id, deliverable_card_id, created_card_id
    existing_cards = await cardbox.get_cards(list(box.card_ids), project_id=ctx.project_id)
    for card in reversed(existing_cards):
        if getattr(card, "type", "") == "task.deliverable":
            deliverable_card_id = getattr(card, "card_id", None)
            break
    if deliverable_card_id is not None:
        return output_box_id, deliverable_card_id, created_card_id

    from core.deliverable_fallback import build_fallback_deliverable_data

    pretty_obj = result_payload
    if isinstance(pretty_obj, dict) and "result" in pretty_obj:
        pretty_obj = pretty_obj.get("result")
    pretty = (
        json.dumps(pretty_obj, ensure_ascii=False, indent=2)
        if isinstance(pretty_obj, (dict, list))
        else str(pretty_obj)
    )
    fallback_data = build_fallback_deliverable_data(
        reason=f"tool_terminated:{resume_status}",
        source="agent_worker.resume_effects.ensure_terminated_deliverable",
        last_thought=pretty,
        tool_result_count=1 if tool_call_id else 0,
        terminal_status=task_status,
        resume_status=resume_status,
        task_status=task_status,
        tool_call_id=tool_call_id,
        headline="Result (turn terminated by tool).",
    )
    deliverable = Card(
        card_id=uuid6.uuid7().hex,
        project_id=ctx.project_id,
        type="task.deliverable",
        content=JsonContent(data=fallback_data),
        created_at=datetime.now(UTC),
        author_id=ctx.agent_id,
        metadata={
            "agent_turn_id": ctx.agent_turn_id,
            "step_id": step_id,
            "role": "assistant",
            "partial": task_status != STATUS_SUCCESS,
            "terminated_by_tool": True,
            **ctx.to_lineage_meta(exclude_step_id=True),
        },
    )
    await cardbox.save_card(deliverable)
    output_box_id = await cardbox.append_to_box(
        output_box_id,
        [deliverable.card_id],
        project_id=ctx.project_id,
    )
    deliverable_card_id = deliverable.card_id
    created_card_id = deliverable.card_id
    return output_box_id, deliverable_card_id, created_card_id
