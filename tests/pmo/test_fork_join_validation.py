import types

import pytest

from core.cg_context import CGContext
from core.errors import BadRequestError
from services.pmo.internal_handlers.base import InternalHandlerDeps
from services.pmo.internal_handlers.fork_join import ForkJoinHandler
import services.pmo.internal_handlers.fork_join as fork_join_module


class _DummyBatchManager:
    def __init__(self) -> None:
        self.calls: list = []
        self.activations: list = []

    async def create_batch(self, *, req, conn):  # noqa: ANN001
        self.calls.append((req, conn))
        return "batch_1"

    async def activate_batch(self, *, req, batch_id, task_count):  # noqa: ANN001
        self.activations.append((req, batch_id, task_count))


class _DummyTx:
    async def __aenter__(self):  # noqa: ANN201
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False


class _DummyConn:
    def transaction(self) -> _DummyTx:
        return _DummyTx()

    async def __aenter__(self):  # noqa: ANN201
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False


class _DummyPool:
    def connection(self) -> _DummyConn:
        return _DummyConn()


def _deps(*, batch_manager=None) -> InternalHandlerDeps:
    return InternalHandlerDeps(
        handover=None,
        batch_manager=batch_manager or _DummyBatchManager(),
        state_store=types.SimpleNamespace(pool=_DummyPool()),
        resource_store=None,
        cardbox=None,
        nats=None,
        execution_store=None,
        identity_store=None,
    )


def _inputs(arguments: dict, *, deps: InternalHandlerDeps | None = None):
    resolved_deps = deps or _deps()
    cmd = types.SimpleNamespace(
        tool_name="fork_join",
        arguments=arguments,
        source_agent_id="agent_parent",
        agent_turn_id="turn_1",
        turn_epoch=1,
        tool_call_id="call_1",
        step_id="step_1",
        tool_call_card_id=None,
        tool_call_parent_step_id=None,
    )
    ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id=cmd.source_agent_id,
        agent_turn_id=cmd.agent_turn_id,
        turn_epoch=int(cmd.turn_epoch),
        step_id=cmd.step_id,
        tool_call_id=cmd.tool_call_id,
        headers={"CG-Recursion-Depth": "0"},
    )
    return resolved_deps, cmd, ctx


@pytest.mark.asyncio
async def test_fork_join_rejects_non_list_tasks() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError, match="invalid args.tasks"):
        deps, cmd, ctx = _inputs({"tasks": "oops"})
        await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_rejects_missing_target_fields() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError, match="invalid args.tasks.0.target_strategy"):
        deps, cmd, ctx = _inputs({"tasks": [{"instruction": "collect latest papers"}]})
        await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_rejects_legacy_retry_args() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError):
        deps, cmd, ctx = _inputs(
            {
                "retry_batch_id": "batch_old",
                "retry_task_indexes": [0],
            }
        )
        await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_rejects_tasks_above_default_fanout_limit() -> None:
    handler = ForkJoinHandler()
    tasks = [
        {
            "target_strategy": "reuse",
            "target_ref": f"agent_{idx}",
            "instruction": f"do task {idx}",
        }
        for idx in range(9)
    ]
    with pytest.raises(
        BadRequestError,
        match=r"tasks fanout exceeds max limit \(got=9, max=8\)",
    ):
        deps, cmd, ctx = _inputs({"tasks": tasks})
        await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_rejects_tasks_above_configured_fanout_limit() -> None:
    handler = ForkJoinHandler(max_tasks=2)
    tasks = [
        {
            "target_strategy": "reuse",
            "target_ref": f"agent_{idx}",
            "instruction": f"do task {idx}",
        }
        for idx in range(3)
    ]
    with pytest.raises(
        BadRequestError,
        match=r"tasks fanout exceeds max limit \(got=3, max=2\)",
    ):
        deps, cmd, ctx = _inputs({"tasks": tasks})
        await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_translates_tasks_and_dispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ForkJoinHandler()
    deps = _deps()
    captured: dict = {}

    async def _fake_build_task_specs(*, deps, cmd, ctx, parsed_args, conn):  # noqa: ANN001
        _ = deps
        _ = cmd
        _ = ctx
        assert conn is not None
        translated = [
            {
                "target_strategy": t.target_strategy,
                "target_ref": t.target_ref,
                "instruction": t.instruction,
                "context_box_id": t.context_box_id,
            }
            for t in parsed_args.tasks
        ]
        captured["translated_tasks"] = translated
        task_specs = [
            {
                "agent_id": f"agent_{idx + 1}",
                "provision": False,
                "profile_box_id": f"profile_{idx + 1}",
                "context_box_id": f"context_{idx + 1}",
                "task_args": {"task_index": idx},
                "runtime_config": {},
            }
            for idx, _ in enumerate(translated)
        ]
        return task_specs

    monkeypatch.setattr(handler, "_build_task_specs", _fake_build_task_specs)

    handler_deps, cmd, ctx = _inputs(
        {
            "tasks": [
                {
                    "target_strategy": "reuse",
                    "target_ref": "agent_researcher",
                    "instruction": "search papers",
                },
                {
                    "target_strategy": "new",
                    "target_ref": "Associate_Search",
                    "instruction": "crawl site",
                    "context_box_id": "box_ctx_1",
                },
            ],
            "fail_fast": "true",
            "deadline_seconds": 42,
        },
        deps=deps,
    )
    defer_resume, payload = await handler.handle(
        deps=handler_deps,
        ctx=ctx,
        cmd=cmd,
        parent_after_execution="suspend",
    )

    assert defer_resume is True
    assert payload == {"status": "accepted", "batch_id": "batch_1", "task_count": 2}
    assert captured["translated_tasks"] == [
        {
            "target_strategy": "reuse",
            "target_ref": "agent_researcher",
            "instruction": "search papers",
            "context_box_id": None,
        },
        {
            "target_strategy": "new",
            "target_ref": "Associate_Search",
            "instruction": "crawl site",
            "context_box_id": "box_ctx_1",
        },
    ]

    req, conn = deps.batch_manager.calls[0]
    assert conn is not None
    assert req.parent_ctx.project_id == "proj_1"
    assert req.parent_ctx.channel_id == "public"
    assert req.fail_fast is True
    assert req.deadline_seconds == 42.0
    assert req.structured_output == "digest"
    assert req.parent_ctx.recursion_depth == 0
    assert len(req.task_specs) == 2
    assert deps.batch_manager.activations == [(req, "batch_1", 2)]


@pytest.mark.asyncio
async def test_fork_join_reuse_passes_existing_output_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = ForkJoinHandler()
    deps, cmd, ctx = _inputs(
        {
            "tasks": [
                {
                    "target_strategy": "reuse",
                    "target_ref": "agent_researcher",
                    "instruction": "search papers",
                }
            ]
        }
    )
    parsed_args = handler._parse_args(cmd.arguments)

    async def _preview_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            profile_name="Associate_Search",
            profile_box_id="profile_box_1",
            display_name="Associate Search",
            reuse_agent_id="agent_researcher",
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
            agent_id="agent_researcher",
            profile_box_id="profile_box_1",
            profile_name="Associate_Search",
        )

    async def _find_current_output_box_id(**kwargs):  # noqa: ANN001
        _ = kwargs
        return "output_box_existing"

    monkeypatch.setattr(fork_join_module, "preview_target", _preview_target)
    monkeypatch.setattr(fork_join_module, "validate_box_ids_exist", _validate_box_ids_exist)
    monkeypatch.setattr(
        fork_join_module,
        "pack_context_with_instruction",
        _pack_context_with_instruction,
    )
    monkeypatch.setattr(fork_join_module, "materialize_target", _materialize_target)
    monkeypatch.setattr(
        fork_join_module,
        "find_current_output_box_id",
        _find_current_output_box_id,
    )

    task_specs = await handler._build_task_specs(
        deps=deps,
        cmd=cmd,
        ctx=ctx,
        parsed_args=parsed_args,
        conn=object(),
    )

    assert len(task_specs) == 1
    assert task_specs[0]["agent_id"] == "agent_researcher"
    assert task_specs[0]["output_box_id"] == "output_box_existing"


@pytest.mark.asyncio
async def test_fork_join_reuse_without_existing_output_box_falls_back_to_new_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = ForkJoinHandler()
    deps, cmd, ctx = _inputs(
        {
            "tasks": [
                {
                    "target_strategy": "reuse",
                    "target_ref": "agent_researcher",
                    "instruction": "search papers",
                }
            ]
        }
    )
    parsed_args = handler._parse_args(cmd.arguments)

    async def _preview_target(**kwargs):  # noqa: ANN001
        _ = kwargs
        return types.SimpleNamespace(
            profile_name="Associate_Search",
            profile_box_id="profile_box_1",
            display_name="Associate Search",
            reuse_agent_id="agent_researcher",
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
            agent_id="agent_researcher",
            profile_box_id="profile_box_1",
            profile_name="Associate_Search",
        )

    async def _find_current_output_box_id(**kwargs):  # noqa: ANN001
        _ = kwargs
        return None

    monkeypatch.setattr(fork_join_module, "preview_target", _preview_target)
    monkeypatch.setattr(fork_join_module, "validate_box_ids_exist", _validate_box_ids_exist)
    monkeypatch.setattr(
        fork_join_module,
        "pack_context_with_instruction",
        _pack_context_with_instruction,
    )
    monkeypatch.setattr(fork_join_module, "materialize_target", _materialize_target)
    monkeypatch.setattr(
        fork_join_module,
        "find_current_output_box_id",
        _find_current_output_box_id,
    )

    task_specs = await handler._build_task_specs(
        deps=deps,
        cmd=cmd,
        ctx=ctx,
        parsed_args=parsed_args,
        conn=object(),
    )

    assert len(task_specs) == 1
    assert task_specs[0]["agent_id"] == "agent_researcher"
    assert task_specs[0]["output_box_id"] is None


@pytest.mark.asyncio
async def test_fork_join_applies_default_deadline_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ForkJoinHandler(default_deadline_seconds=120.0)
    deps = _deps()

    async def _fake_build_task_specs(*, deps, cmd, ctx, parsed_args, conn):  # noqa: ANN001
        _ = (deps, cmd, ctx, parsed_args)
        assert conn is not None
        return [
            {
                "agent_id": "agent_1",
                "provision": False,
                "profile_box_id": "profile_1",
                "context_box_id": "context_1",
                "task_args": {"task_index": 0},
                "runtime_config": {},
            }
        ]

    monkeypatch.setattr(handler, "_build_task_specs", _fake_build_task_specs)

    handler_deps, cmd, ctx = _inputs(
        {
            "tasks": [
                {
                    "target_strategy": "reuse",
                    "target_ref": "agent_researcher",
                    "instruction": "search papers",
                },
            ],
        },
        deps=deps,
    )
    defer_resume, payload = await handler.handle(
        deps=handler_deps,
        ctx=ctx,
        cmd=cmd,
        parent_after_execution="suspend",
    )

    assert defer_resume is True
    assert payload == {"status": "accepted", "batch_id": "batch_1", "task_count": 1}
    req, conn = deps.batch_manager.calls[0]
    assert conn is not None
    assert req.deadline_seconds == 120.0
    assert deps.batch_manager.activations == [(req, "batch_1", 1)]
