from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.pmo.service import PMOService


@pytest.mark.asyncio
async def test_resolve_internal_handler_rejects_removed_fork_join_route() -> None:
    service = PMOService.__new__(PMOService)

    async def _fetch_tool_definition_model(project_id: str, tool_name: str):  # noqa: ANN001
        assert project_id == "proj_1"
        assert tool_name == "fork_join"
        return SimpleNamespace(after_execution="suspend")

    reply_calls: list[dict] = []

    async def _reply_validation_failure(**kwargs):  # noqa: ANN003
        reply_calls.append(dict(kwargs))

    service.resource_store = SimpleNamespace(fetch_tool_definition_model=_fetch_tool_definition_model)
    service._reply_validation_failure = _reply_validation_failure
    service._get_internal_handler = lambda _: None

    meta = SimpleNamespace(project_id="proj_1", tool_suffix="internal.fork_join")
    cmd = SimpleNamespace(tool_name="fork_join", after_execution="suspend")

    db_after, handler = await PMOService._resolve_internal_handler(
        service,
        meta=meta,
        cmd=cmd,
        headers={},
    )

    assert db_after == "suspend"
    assert handler is None
    assert len(reply_calls) == 1
    assert reply_calls[0]["error_code"] == "not_implemented"


@pytest.mark.asyncio
async def test_resolve_internal_handler_allows_fork_join() -> None:
    service = PMOService.__new__(PMOService)
    handler_obj = object()

    async def _fetch_tool_definition_model(project_id: str, tool_name: str):  # noqa: ANN001
        assert project_id == "proj_1"
        assert tool_name == "fork_join"
        return SimpleNamespace(after_execution="suspend")

    async def _reply_validation_failure(**kwargs):  # noqa: ANN003
        raise AssertionError(f"unexpected validation failure: {kwargs}")

    service.resource_store = SimpleNamespace(fetch_tool_definition_model=_fetch_tool_definition_model)
    service._reply_validation_failure = _reply_validation_failure
    service._get_internal_handler = lambda _: handler_obj

    meta = SimpleNamespace(project_id="proj_1", tool_suffix="internal.fork_join")
    cmd = SimpleNamespace(tool_name="fork_join", after_execution="suspend")

    db_after, handler = await PMOService._resolve_internal_handler(
        service,
        meta=meta,
        cmd=cmd,
        headers={},
    )

    assert db_after == "suspend"
    assert handler is handler_obj
