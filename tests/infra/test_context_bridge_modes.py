import pytest

from core.errors import ProtocolViolationError
from core.config import PROTOCOL_VERSION
from core.subject import parse_subject
from infra.messaging.ingress import build_nats_ingress_context
from infra.stores.context_hydration import build_replay_context


def test_build_ingress_context_strict_headers_prefers_subject_and_headers_only() -> None:
    subject = parse_subject(f"cg.{PROTOCOL_VERSION}.proj_subject.chan_subject.cmd.agent.agent_subject.task")
    assert subject is not None
    headers = {
        "CG-Project-Id": "proj_subject",
        "CG-Agent-Id": "agent_subject",
        "CG-Channel-Id": "chan_subject",
        "CG-Turn-Id": "turn_header",
        "CG-Turn-Epoch": "3",
        "CG-Step-Id": "step_header",
        "CG-Recursion-Depth": "2",
    }
    payload = {
        "instruction": "hello",
        "priority": "high",
    }

    ctx, safe_payload = build_nats_ingress_context(
        headers=headers,
        raw_payload=payload,
        subject_parts=subject,
    )

    assert ctx.project_id == "proj_subject"
    assert ctx.agent_id == "agent_subject"
    assert ctx.channel_id == "chan_subject"
    assert ctx.agent_turn_id == "turn_header"
    assert ctx.turn_epoch == 3
    assert ctx.step_id == "step_header"
    assert ctx.recursion_depth == 2
    assert safe_payload == payload


def test_build_ingress_context_cmd_rejects_payload_control_fields() -> None:
    subject = parse_subject(f"cg.{PROTOCOL_VERSION}.proj_1.public.cmd.tool.demo.execute")
    assert subject is not None
    headers = {
        "CG-Project-Id": "proj_1",
        "CG-Channel-Id": "public",
        "CG-Agent-Id": "agent_1",
        "CG-Tool-Call-Id": "call_1",
    }
    payload = {
        "tool_name": "demo",
        "tool_call_id": "call_payload",
    }

    with pytest.raises(ProtocolViolationError, match="forbidden control fields"):
        build_nats_ingress_context(
            headers=headers,
            raw_payload=payload,
            subject_parts=subject,
        )


def test_build_ingress_context_strict_headers_rejects_payload_identity_fallback() -> None:
    subject = parse_subject(f"cg.{PROTOCOL_VERSION}.proj_1.public.cmd.sys.ui.action")
    assert subject is not None
    headers = {
        "CG-Recursion-Depth": "0",
    }
    payload = {
        "action_id": "act_1",
        "tool_name": "tool.demo",
        "args": {},
    }

    with pytest.raises(ProtocolViolationError, match="missing required CG-Agent-Id"):
        build_nats_ingress_context(
            headers=headers,
            raw_payload=payload,
            subject_parts=subject,
        )


def test_build_ingress_context_evt_sanitizes_payload_control_fields() -> None:
    subject = parse_subject(f"cg.{PROTOCOL_VERSION}.proj_1.public.evt.agent.agent_1.task")
    assert subject is not None
    headers = {
        "CG-Project-Id": "proj_1",
        "CG-Channel-Id": "public",
        "CG-Agent-Id": "agent_1",
        "CG-Turn-Id": "turn_1",
        "CG-Turn-Epoch": "1",
    }
    payload = {
        "agent_turn_id": "turn_in_payload",
        "status": "running",
        "tool_result_card_id": "card_1",
    }

    ctx, safe_payload = build_nats_ingress_context(
        headers=headers,
        raw_payload=payload,
        subject_parts=subject,
    )

    assert ctx.agent_turn_id == "turn_1"
    assert safe_payload == {
        "status": "running",
        "tool_result_card_id": "card_1",
    }


def test_build_replay_context_rejects_payload_control_fields() -> None:
    payload = {
        "tool_call_id": "call_payload",
        "status": "success",
    }
    db_row = {
        "project_id": "proj_db",
        "agent_id": "agent_db",
        "message_type": "turn",
        "channel_id": "chan_db",
        "agent_turn_id": "turn_db",
        "turn_epoch": 7,
        "correlation_id": "turn_db_correlation",
        "context_headers": {
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            "CG-Recursion-Depth": "1",
            "CG-Step-Id": "step_db",
            "CG-Tool-Call-Id": "tool_db",
            "CG-Parent-Agent-Id": "agent_parent_db",
            "CG-Parent-Turn-Id": "turn_parent_db",
            "CG-Parent-Step-Id": "step_parent_db",
        },
    }

    with pytest.raises(ProtocolViolationError, match="forbidden control fields"):
        build_replay_context(
            raw_payload=payload,
            db_row=db_row,
        )


def test_build_replay_context_prefers_db_row_and_context_headers() -> None:
    payload = {
        "status": "queued",
        "tool_result_card_id": "card_1",
    }
    db_row = {
        "project_id": "proj_db",
        "agent_id": "agent_db",
        "message_type": "turn",
        "channel_id": "chan_db",
        "agent_turn_id": "turn_db",
        "turn_epoch": 7,
        "correlation_id": "turn_db_correlation",
        "context_headers": {
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            "tracestate": "vendor=a",
            "CG-Parent-Traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
            "CG-Recursion-Depth": "4",
            "CG-Step-Id": "step_db",
            "CG-Tool-Call-Id": "tool_db",
            "CG-Parent-Agent-Id": "agent_parent_db",
            "CG-Parent-Turn-Id": "turn_parent_db",
            "CG-Parent-Step-Id": "step_parent_db",
        },
    }

    ctx, safe_payload = build_replay_context(
        raw_payload=payload,
        db_row=db_row,
    )

    assert ctx.project_id == "proj_db"
    assert ctx.agent_id == "agent_db"
    assert ctx.channel_id == "chan_db"
    assert ctx.agent_turn_id == "turn_db"
    assert ctx.turn_epoch == 7
    assert ctx.step_id == "step_db"
    assert ctx.tool_call_id == "tool_db"
    assert ctx.parent_agent_id == "agent_parent_db"
    assert ctx.parent_agent_turn_id == "turn_parent_db"
    assert ctx.parent_step_id == "step_parent_db"
    assert ctx.recursion_depth == 4
    assert safe_payload == payload
