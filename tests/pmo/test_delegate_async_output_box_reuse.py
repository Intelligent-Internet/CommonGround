import types

import pytest

from core.cg_context import CGContext
from services.pmo.internal_handlers.base import InternalHandlerDeps
from services.pmo.internal_handlers.delegate_async import DelegateAsyncHandler
import services.pmo.internal_handlers.delegate_async as delegate_async_module


class _DummyTx:
    conn = object()

    async def __aenter__(self):  # noqa: ANN201
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False


class _DummyUnitOfWork:
    def __init__(self, _pool) -> None:
        self._pool = _pool

    def transaction(self) -> _DummyTx:
        return _DummyTx()


def _deps() -> InternalHandlerDeps:
    return InternalHandlerDeps(
        handover=None,
        batch_manager=None,
        state_store=types.SimpleNamespace(pool=object()),
        resource_store=None,
        cardbox=None,
        nats=None,
        execution_store=None,
        identity_store=None,
    )


def _ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_parent",
        agent_turn_id="turn_parent",
        turn_epoch=1,
        step_id="step_parent",
        tool_call_id="call_parent",
        recursion_depth=0,
        headers={"CG-Recursion-Depth": "0"},
    )


def _cmd(*, target_strategy: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        tool_name="delegate_async",
        arguments={
            "target_strategy": target_strategy,
            "target_ref": "agent_chat",
            "instruction": "hello",
        },
        source_agent_id="agent_parent",
        agent_turn_id="turn_parent",
        turn_epoch=1,
        tool_call_id="call_parent",
        step_id="step_parent",
    )


def _stub_dispatcher(monkeypatch: pytest.MonkeyPatch, dispatch_fn) -> None:  # noqa: ANN001
    class _DummyDispatcher:
        def __init__(self, **kwargs) -> None:  # noqa: ANN003
            _ = kwargs
            self.l0 = types.SimpleNamespace(publish_wakeup_signals=_publish_wakeup_signals)

        async def dispatch_transactional(self, req, *, conn):  # noqa: ANN001
            return await dispatch_fn(req=req, conn=conn)

    async def _publish_wakeup_signals(signals):  # noqa: ANN001
        _ = signals
        return None

    async def _emit_agent_state(**kwargs):  # noqa: ANN001
        _ = kwargs
        return None

    monkeypatch.setattr(delegate_async_module, "AgentDispatcher", _DummyDispatcher)
    monkeypatch.setattr(delegate_async_module, "emit_agent_state", _emit_agent_state)


@pytest.mark.asyncio
async def test_delegate_async_reuse_passes_existing_output_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(delegate_async_module, "UnitOfWork", _DummyUnitOfWork)

    async def _preview_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            profile_name="chat_profile",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
            reuse_agent_id="agent_chat",
            clone_source_agent_id=None,
        )

    async def _validate_box_ids_exist(**kwargs):  # noqa: ANN001
        _ = kwargs
        return []

    async def _pack_context_with_instruction(**kwargs):  # noqa: ANN001
        _ = kwargs
        return "context_box_1"

    async def _materialize_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            agent_id="agent_chat",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
        )

    async def _find_current_output_box_id(**kwargs):  # noqa: ANN001
        _ = kwargs
        return "output_box_existing"

    captured: dict = {}

    async def _dispatch_transactional(*, req, conn):  # noqa: ANN001
        _ = conn
        captured["output_box_id"] = req.output_box_id
        return (
            types.SimpleNamespace(
                status="accepted",
                agent_turn_id="turn_chat_1",
                turn_epoch=2,
                output_box_id="output_box_existing",
                trace_id=None,
            ),
            [],
        )

    monkeypatch.setattr(delegate_async_module, "preview_target", _preview_target)
    monkeypatch.setattr(delegate_async_module, "validate_box_ids_exist", _validate_box_ids_exist)
    monkeypatch.setattr(delegate_async_module, "pack_context_with_instruction", _pack_context_with_instruction)
    monkeypatch.setattr(delegate_async_module, "materialize_target", _materialize_target)
    monkeypatch.setattr(delegate_async_module, "find_current_output_box_id", _find_current_output_box_id)
    _stub_dispatcher(monkeypatch, _dispatch_transactional)

    handler = DelegateAsyncHandler()
    done, payload = await handler.handle(
        deps=_deps(),
        ctx=_ctx(),
        cmd=_cmd(target_strategy="reuse"),
        parent_after_execution="suspend",
    )

    assert done is False
    assert payload["status"] == "accepted"
    assert captured["output_box_id"] == "output_box_existing"


@pytest.mark.asyncio
async def test_delegate_async_reuse_without_existing_output_box_falls_back_to_new_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(delegate_async_module, "UnitOfWork", _DummyUnitOfWork)

    async def _preview_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            profile_name="chat_profile",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
            reuse_agent_id="agent_chat",
            clone_source_agent_id=None,
        )

    async def _validate_box_ids_exist(**kwargs):  # noqa: ANN001
        _ = kwargs
        return []

    async def _pack_context_with_instruction(**kwargs):  # noqa: ANN001
        _ = kwargs
        return "context_box_1"

    async def _materialize_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            agent_id="agent_chat",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
        )

    async def _find_current_output_box_id(**kwargs):  # noqa: ANN001
        _ = kwargs
        return None

    captured: dict = {}

    async def _dispatch_transactional(*, req, conn):  # noqa: ANN001
        _ = conn
        captured["output_box_id"] = req.output_box_id
        return (
            types.SimpleNamespace(
                status="accepted",
                agent_turn_id="turn_chat_1",
                turn_epoch=2,
                output_box_id="output_box_new",
                trace_id=None,
            ),
            [],
        )

    monkeypatch.setattr(delegate_async_module, "preview_target", _preview_target)
    monkeypatch.setattr(delegate_async_module, "validate_box_ids_exist", _validate_box_ids_exist)
    monkeypatch.setattr(delegate_async_module, "pack_context_with_instruction", _pack_context_with_instruction)
    monkeypatch.setattr(delegate_async_module, "materialize_target", _materialize_target)
    monkeypatch.setattr(delegate_async_module, "find_current_output_box_id", _find_current_output_box_id)
    _stub_dispatcher(monkeypatch, _dispatch_transactional)

    handler = DelegateAsyncHandler()
    done, payload = await handler.handle(
        deps=_deps(),
        ctx=_ctx(),
        cmd=_cmd(target_strategy="reuse"),
        parent_after_execution="suspend",
    )

    assert done is False
    assert payload["status"] == "accepted"
    assert captured["output_box_id"] is None


@pytest.mark.asyncio
async def test_delegate_async_new_does_not_pass_reused_output_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(delegate_async_module, "UnitOfWork", _DummyUnitOfWork)

    async def _preview_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            profile_name="chat_profile",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
            reuse_agent_id=None,
            clone_source_agent_id=None,
        )

    async def _validate_box_ids_exist(**kwargs):  # noqa: ANN001
        _ = kwargs
        return []

    async def _pack_context_with_instruction(**kwargs):  # noqa: ANN001
        _ = kwargs
        return "context_box_1"

    async def _materialize_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            agent_id="agent_new",
            profile_box_id="profile_box_1",
            display_name="Chat Agent",
        )

    captured: dict = {}

    async def _dispatch_transactional(*, req, conn):  # noqa: ANN001
        _ = conn
        captured["output_box_id"] = req.output_box_id
        return (
            types.SimpleNamespace(
                status="accepted",
                agent_turn_id="turn_chat_1",
                turn_epoch=2,
                output_box_id="output_box_new",
                trace_id=None,
            ),
            [],
        )

    monkeypatch.setattr(delegate_async_module, "preview_target", _preview_target)
    monkeypatch.setattr(delegate_async_module, "validate_box_ids_exist", _validate_box_ids_exist)
    monkeypatch.setattr(delegate_async_module, "pack_context_with_instruction", _pack_context_with_instruction)
    monkeypatch.setattr(delegate_async_module, "materialize_target", _materialize_target)
    _stub_dispatcher(monkeypatch, _dispatch_transactional)

    handler = DelegateAsyncHandler()
    done, payload = await handler.handle(
        deps=_deps(),
        ctx=_ctx(),
        cmd=_cmd(target_strategy="new"),
        parent_after_execution="suspend",
    )

    assert done is False
    assert payload["status"] == "accepted"
    assert captured["output_box_id"] is None
