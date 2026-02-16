import types

import pytest

from core.errors import BadRequestError
from services.pmo.internal_handlers.base import InternalHandlerDeps
from services.pmo.internal_handlers.context import InternalHandlerContext
from services.pmo.internal_handlers.fork_join import ForkJoinHandler


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


def _ctx(arguments: dict, *, deps: InternalHandlerDeps | None = None) -> InternalHandlerContext:
    meta = types.SimpleNamespace(project_id="proj_1", channel_id="public", tool_suffix="internal.fork_join")
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
    return InternalHandlerContext(
        deps=deps or _deps(),
        meta=meta,
        cmd=cmd,
        headers={"CG-Recursion-Depth": "0"},
    )


@pytest.mark.asyncio
async def test_fork_join_rejects_non_list_tasks() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError, match="invalid args.tasks"):
        await handler.handle(ctx=_ctx({"tasks": "oops"}), parent_after_execution="suspend")


@pytest.mark.asyncio
async def test_fork_join_rejects_missing_target_fields() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError, match="invalid args.tasks.0.target_strategy"):
        await handler.handle(
            ctx=_ctx({"tasks": [{"instruction": "collect latest papers"}]}),
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_rejects_legacy_retry_args() -> None:
    handler = ForkJoinHandler()
    with pytest.raises(BadRequestError):
        await handler.handle(
            ctx=_ctx(
                {
                    "retry_batch_id": "batch_old",
                    "retry_task_indexes": [0],
                }
            ),
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
        await handler.handle(
            ctx=_ctx({"tasks": tasks}),
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
        await handler.handle(
            ctx=_ctx({"tasks": tasks}),
            parent_after_execution="suspend",
        )


@pytest.mark.asyncio
async def test_fork_join_translates_tasks_and_dispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ForkJoinHandler()
    deps = _deps()
    captured: dict = {}

    async def _fake_build_task_specs(*, ctx, parsed_args, conn):  # noqa: ANN001
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

    defer_resume, payload = await handler.handle(
        ctx=_ctx(
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
        ),
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
    assert req.project_id == "proj_1"
    assert req.channel_id == "public"
    assert req.fail_fast is True
    assert req.deadline_seconds == 42.0
    assert req.structured_output == "digest"
    assert req.recursion_depth == 0
    assert len(req.task_specs) == 2
    assert deps.batch_manager.activations == [(req, "batch_1", 2)]


@pytest.mark.asyncio
async def test_fork_join_applies_default_deadline_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = ForkJoinHandler(default_deadline_seconds=120.0)
    deps = _deps()

    async def _fake_build_task_specs(*, ctx, parsed_args, conn):  # noqa: ANN001
        _ = (ctx, parsed_args)
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

    defer_resume, payload = await handler.handle(
        ctx=_ctx(
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
        ),
        parent_after_execution="suspend",
    )

    assert defer_resume is True
    assert payload == {"status": "accepted", "batch_id": "batch_1", "task_count": 1}
    req, conn = deps.batch_manager.calls[0]
    assert conn is not None
    assert req.deadline_seconds == 120.0
    assert deps.batch_manager.activations == [(req, "batch_1", 1)]
