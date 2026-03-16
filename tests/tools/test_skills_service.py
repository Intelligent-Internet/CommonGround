from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.cg_context import CGContext
from services.tools.skills_service import SkillsToolService


class _DummyTx:
    async def __aenter__(self):  # noqa: ANN201
        return None

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        _ = exc_type, exc, tb
        return False


class _DummyConn:
    def transaction(self) -> _DummyTx:
        return _DummyTx()


class _DummyConnCtx:
    async def __aenter__(self) -> _DummyConn:  # noqa: ANN201
        return _DummyConn()

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        _ = exc_type, exc, tb
        return False


class _DummyPool:
    def connection(self) -> _DummyConnCtx:
        return _DummyConnCtx()


def _headers() -> dict[str, str]:
    return {
        "CG-Agent-Id": "agent_1",
        "CG-Turn-Id": "turn_1",
        "CG-Turn-Epoch": "1",
        "CG-Tool-Call-Id": "call_1",
    }


def _subject() -> str:
    return "cg.v1r4.proj_1.public.cmd.tool.skills.call"


def _ctx(agent_id: str) -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id=agent_id,
        agent_turn_id="turn_1",
        turn_epoch=1,
        tool_call_id="call_1",
        trace_id="11111111111111111111111111111111",
        recursion_depth=0,
        headers={},
    )


@pytest.mark.asyncio
async def test_skills_service_validation_failure_publishes_failed_tool_result() -> None:
    service = SkillsToolService.__new__(SkillsToolService)
    service.cardbox = SimpleNamespace(save_card=AsyncMock())
    service.execution_store = SimpleNamespace(pool=_DummyPool())
    service.l0 = SimpleNamespace(
        report_intent=AsyncMock(
            return_value=SimpleNamespace(status="accepted", error_code=None, wakeup_signals=())
        ),
        publish_wakeup_signals=AsyncMock(),
    )

    await SkillsToolService._handle_cmd(
        service,
        _subject(),
        {
            "tool_name": "skills.run_cmd",
            "tool_call_card_id": "card_1",
        },
        _headers(),
    )

    service.cardbox.save_card.assert_awaited_once()
    service.l0.report_intent.assert_awaited_once()
    intent = service.l0.report_intent.await_args.kwargs["intent"]
    assert intent.message_type == "tool_result"
    assert intent.correlation_id == "call_1"
    assert intent.payload["status"] == "failed"


@pytest.mark.asyncio
async def test_skills_service_report_failure_is_not_silent() -> None:
    service = SkillsToolService.__new__(SkillsToolService)
    service.execution_store = SimpleNamespace(pool=_DummyPool())
    service.l0 = SimpleNamespace(
        report_intent=AsyncMock(
            return_value=SimpleNamespace(status="rejected", error_code="protocol_violation", wakeup_signals=())
        ),
        publish_wakeup_signals=AsyncMock(),
    )

    ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=1,
        tool_call_id="call_1",
        trace_id="11111111111111111111111111111111",
        recursion_depth=0,
        headers={},
    )

    with pytest.raises(RuntimeError, match="rejected"):
        await SkillsToolService._report_tool_result_and_wakeup(
            service,
            payload={"status": "failed", "tool_result_card_id": "card_1"},
            correlation_id="call_1",
            safe_headers=_headers(),
            source_ctx=ctx,
        )


@pytest.mark.asyncio
async def test_skills_service_validation_failure_recovers_tool_call_id_from_card_not_payload() -> None:
    service = SkillsToolService.__new__(SkillsToolService)
    service.cardbox = SimpleNamespace(
        get_cards=AsyncMock(
            return_value=[
                SimpleNamespace(
                    type="tool.call",
                    tool_call_id="call_from_card",
                    metadata={"agent_turn_id": "turn_1", "step_id": "step_card"},
                )
            ]
        ),
        save_card=AsyncMock(),
    )
    service.execution_store = SimpleNamespace(pool=_DummyPool())
    service.l0 = SimpleNamespace(
        report_intent=AsyncMock(
            return_value=SimpleNamespace(status="accepted", error_code=None, wakeup_signals=())
        ),
        publish_wakeup_signals=AsyncMock(),
    )

    await SkillsToolService._handle_cmd(
        service,
        _subject(),
        {
            "tool_call_id": "call_from_payload",
            "tool_call_card_id": "card_1",
            "after_execution": "suspend",
        },
        {
            "CG-Agent-Id": "agent_1",
            "CG-Turn-Id": "turn_1",
            "CG-Turn-Epoch": "1",
        },
    )

    intent = service.l0.report_intent.await_args.kwargs["intent"]
    assert intent.correlation_id == "call_from_card"
    service.cardbox.get_cards.assert_awaited_once()


@pytest.mark.asyncio
async def test_skills_service_session_key_is_owner_scoped() -> None:
    service = SkillsToolService.__new__(SkillsToolService)

    session_id = "demo"
    key_a = SkillsToolService._resolve_session_key(service, ctx=_ctx("agent_a"), session_id=session_id)
    key_b = SkillsToolService._resolve_session_key(service, ctx=_ctx("agent_b"), session_id=session_id)

    assert key_a != key_b
    assert key_a == "proj_1_agent_a_demo"
    assert key_b == "proj_1_agent_b_demo"


@pytest.mark.asyncio
async def test_skills_service_conflict_detection_uses_session_key_not_alias() -> None:
    service = SkillsToolService.__new__(SkillsToolService)
    service._session_locks = {}
    service._jobs = {}
    service._bg_tasks = {
        "task_1": {
            "task_id": "task_1",
            "status": "running",
            "session_id": "demo",
            "session_key": "proj_1_agent_a_demo",
            "workdir": "skill",
        }
    }
    service._jobs_lock = asyncio.Lock()
    service._bg_lock = asyncio.Lock()

    conflict = await SkillsToolService._find_session_conflict(
        service,
        session_key="proj_1_agent_b_demo",
        workdir="skill",
    )

    assert conflict is None
