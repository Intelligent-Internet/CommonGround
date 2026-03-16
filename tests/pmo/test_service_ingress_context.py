from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from core.errors import ProtocolViolationError
from services.pmo.service import PMOService


@pytest.mark.asyncio
async def test_handle_tool_command_ingress_violation_still_replies_best_effort() -> None:
    service = PMOService.__new__(PMOService)
    service._reply_failed_best_effort = AsyncMock()

    subject = "cg.v1r4.proj_1.public.cmd.sys.pmo.internal.delegate_async"
    headers = {
        "CG-Agent-Id": "agent_1",
        "CG-Turn-Id": "turn_1",
        "CG-Turn-Epoch": "1",
        "CG-Tool-Call-Id": "tc_1",
    }
    data = {
        # Forbidden control field for cmd ingress: should trigger ProtocolViolationError
        "tool_call_id": "tc_1",
        "tool_call_card_id": "card_1",
        "tool_name": "delegate_async",
        "after_execution": "suspend",
    }

    await PMOService._handle_tool_command(service, subject, data, headers)

    service._reply_failed_best_effort.assert_awaited_once()
    kwargs = service._reply_failed_best_effort.await_args.kwargs
    assert kwargs["ingress_ctx"].tool_call_id == "tc_1"
    assert kwargs["ingress_ctx"].agent_turn_id == "turn_1"
    assert isinstance(kwargs["exc"], ProtocolViolationError)


@pytest.mark.asyncio
async def test_handle_tool_command_unreplyable_ingress_failure_is_logged() -> None:
    service = PMOService.__new__(PMOService)
    service._reply_failed_best_effort = AsyncMock()
    service._log_unreplyable_ingress_failure = Mock()

    subject = "cg.v1r4.proj_1.public.cmd.sys.pmo.internal.delegate_async"
    headers = {
        "CG-Turn-Id": "turn_1",
        "CG-Turn-Epoch": "1",
        "CG-Tool-Call-Id": "tc_1",
    }
    data = {
        "tool_call_id": "tc_1",
        "tool_call_card_id": "card_1",
        "tool_name": "delegate_async",
        "after_execution": "suspend",
    }

    await PMOService._handle_tool_command(service, subject, data, headers)

    service._reply_failed_best_effort.assert_not_awaited()
    service._log_unreplyable_ingress_failure.assert_called_once()
