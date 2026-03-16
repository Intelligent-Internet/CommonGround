from core.cg_context import CGContext
from core.headers import (
    CG_AGENT_ID,
    CG_AGENT_TURN_ID,
    CG_CHANNEL_ID,
    CG_PARENT_AGENT_ID,
    CG_PARENT_AGENT_TURN_ID,
    CG_PARENT_STEP_ID,
    CG_PARENT_TRACEPARENT_HEADER,
    CG_PROJECT_ID,
    CG_TURN_EPOCH,
    RECURSION_DEPTH_HEADER,
)
from core.trace import TRACEPARENT_HEADER


def test_from_sys_dict_round_trip_with_header_override() -> None:
    source = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=3,
        step_id="step_1",
        tool_call_id="tc_1",
        trace_id="trace_1",
        recursion_depth=2,
        parent_agent_id="agent_parent",
        parent_agent_turn_id="turn_parent",
        parent_step_id="step_parent",
        headers={"h0": "v0"},
    )
    restored = CGContext.from_sys_dict(source.to_sys_dict(), headers={"h1": "v1"})
    assert restored.to_sys_dict() == source.to_sys_dict()
    assert restored.headers == {"h1": "v1"}


def test_from_sys_dict_uses_embedded_headers_by_default() -> None:
    restored = CGContext.from_sys_dict(
        {
            "project_id": "proj_1",
            "agent_id": "agent_1",
            "headers": {"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
        }
    )
    assert restored.project_id == "proj_1"
    assert restored.agent_id == "agent_1"
    assert restored.channel_id == "public"
    assert "traceparent" in restored.headers


def test_with_trace_transport_overrides_trace_headers() -> None:
    ctx = CGContext(
        project_id="proj_1",
        agent_id="agent_1",
        channel_id="public",
        trace_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        recursion_depth=2,
        headers={TRACEPARENT_HEADER: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
    )
    next_ctx = ctx.with_trace_transport(
        traceparent="00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
        parent_traceparent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        trace_id="cccccccccccccccccccccccccccccccc",
        default_depth=3,
    )
    assert next_ctx.trace_id == "cccccccccccccccccccccccccccccccc"
    assert next_ctx.recursion_depth == 3
    assert (
        next_ctx.headers.get(TRACEPARENT_HEADER)
        == "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"
    )
    assert (
        next_ctx.headers.get(CG_PARENT_TRACEPARENT_HEADER)
        == "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
    )


def test_to_nats_headers_can_omit_topology_fields_for_doorbell() -> None:
    ctx = CGContext(
        project_id="proj_1",
        agent_id="agent_1",
        channel_id="public",
        agent_turn_id="turn_1",
        turn_epoch=2,
        step_id="step_1",
        tool_call_id="tc_1",
        trace_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        recursion_depth=3,
        parent_agent_id="agent_parent",
        parent_agent_turn_id="turn_parent",
        parent_step_id="step_parent",
        headers={TRACEPARENT_HEADER: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
    )

    headers = ctx.to_nats_headers(include_topology=False)

    assert headers[CG_PROJECT_ID] == "proj_1"
    assert headers[CG_AGENT_ID] == "agent_1"
    assert headers[CG_CHANNEL_ID] == "public"
    assert headers[CG_AGENT_TURN_ID] == "turn_1"
    assert headers[CG_TURN_EPOCH] == "2"
    assert TRACEPARENT_HEADER in headers

    assert RECURSION_DEPTH_HEADER not in headers
    assert CG_PARENT_AGENT_ID not in headers
    assert CG_PARENT_AGENT_TURN_ID not in headers
    assert CG_PARENT_STEP_ID not in headers
