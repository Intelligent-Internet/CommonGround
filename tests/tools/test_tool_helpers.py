from __future__ import annotations

from types import SimpleNamespace

from core.cg_context import CGContext
from services.tools.tool_helpers import recover_validation_failure_context
import pytest


@pytest.mark.asyncio
async def test_recover_validation_failure_context_prefers_tool_call_card_metadata() -> None:
    ingress_ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=2,
        trace_id="trace_1",
    )
    card = SimpleNamespace(
        type="tool.call",
        tool_call_id="call_card",
        metadata={
            "agent_turn_id": "turn_card",
            "step_id": "step_card",
            "trace_id": "trace_card",
        },
    )
    cardbox = SimpleNamespace(get_cards=lambda ids, project_id: None)

    async def _get_cards(ids, project_id):  # noqa: ANN001
        assert ids == ["card_1"]
        assert project_id == "proj_1"
        return [card]

    cardbox.get_cards = _get_cards

    recovered = await recover_validation_failure_context(
        cardbox=cardbox,
        ingress_ctx=ingress_ctx,
        data={"tool_call_card_id": "card_1", "tool_call_id": "call_payload"},
    )

    assert recovered is not None
    assert recovered.tool_call_id == "call_card"
    assert recovered.agent_turn_id == "turn_card"
    assert recovered.step_id == "step_card"
    assert recovered.trace_id == "trace_card"


@pytest.mark.asyncio
async def test_recover_validation_failure_context_ignores_payload_lineage_without_tool_call_card() -> None:
    ingress_ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=2,
        tool_call_id="call_1",
        trace_id="trace_ingress",
        parent_step_id="parent_ingress",
        step_id="step_ingress",
    )

    recovered = await recover_validation_failure_context(
        cardbox=SimpleNamespace(),
        ingress_ctx=ingress_ctx,
        data={
            "trace_id": "trace_spoofed",
            "parent_step_id": "parent_spoofed",
            "step_id": "step_spoofed",
        },
    )

    assert recovered is not None
    assert recovered.tool_call_id == "call_1"
    assert recovered.agent_turn_id == "turn_1"
    assert recovered.trace_id == "trace_ingress"
    assert recovered.parent_step_id == "parent_ingress"
    assert recovered.step_id == "step_ingress"
