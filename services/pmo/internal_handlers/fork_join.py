from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from core.config_defaults import DEFAULT_PMO_FORK_JOIN_MAX_TASKS
from core.errors import BadRequestError, ProtocolViolationError
from core.headers import require_recursion_depth
from core.trace import trace_id_from_headers
from core.utils import safe_str
from infra.db.uow import UnitOfWork

from ..l1_orchestrators.base import BatchCreateRequest
from .base import InternalHandlerResult
from .context import InternalHandlerContext
from .orchestration_primitives import (
    materialize_target,
    pack_context_with_instruction,
    preview_target,
    resolve_current_output_box_id,
    validate_box_ids_exist,
)


logger = logging.getLogger("PMO.Internal")


def _coerce_non_empty(value: Any, *, field_name: str) -> str:
    text = safe_str(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _validation_error_to_bad_request(prefix: str, exc: ValidationError) -> BadRequestError:
    details = exc.errors()
    if not details:
        return BadRequestError(f"{prefix}: invalid arguments")
    first = details[0]
    loc = ".".join(str(part) for part in first.get("loc") or [])
    msg = str(first.get("msg") or "invalid value")
    if loc:
        return BadRequestError(f"{prefix}: invalid args.{loc}: {msg}")
    return BadRequestError(f"{prefix}: invalid arguments: {msg}")


class _ForkJoinTask(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target_strategy: str
    target_ref: str
    instruction: str
    context_box_id: Optional[str] = None

    @field_validator("target_strategy", mode="before")
    @classmethod
    def _normalize_target_strategy(cls, value: Any) -> str:
        text = _coerce_non_empty(value, field_name="target_strategy").lower()
        if text not in ("new", "reuse", "clone"):
            raise ValueError("must be one of new|reuse|clone")
        return text

    @field_validator("target_ref", mode="before")
    @classmethod
    def _normalize_target_ref(cls, value: Any) -> str:
        return _coerce_non_empty(value, field_name="target_ref")

    @field_validator("instruction", mode="before")
    @classmethod
    def _normalize_instruction(cls, value: Any) -> str:
        return _coerce_non_empty(value, field_name="instruction")

    @field_validator("context_box_id", mode="before")
    @classmethod
    def _normalize_context_box_id(cls, value: Any) -> Optional[str]:
        text = safe_str(value)
        return text or None


class _ForkJoinArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tasks: List[_ForkJoinTask] = Field(min_length=1)
    fail_fast: bool = False
    deadline_seconds: Optional[float] = None

    @field_validator("fail_fast", mode="before")
    @classmethod
    def _normalize_fail_fast(cls, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "1", "yes", "on"):
                return True
            if lowered in ("false", "0", "no", "off", ""):
                return False
        raise ValueError("must be a boolean or boolean-like string")

    @field_validator("deadline_seconds", mode="before")
    @classmethod
    def _normalize_deadline(cls, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError("must be a positive number")
        try:
            parsed = float(value)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("must be a positive number") from exc
        if parsed <= 0:
            raise ValueError("must be a positive number")
        return parsed


class ForkJoinHandler:
    name = "fork_join"

    def __init__(
        self,
        *,
        default_deadline_seconds: Optional[float] = None,
        max_tasks: int = DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
    ) -> None:
        self.default_deadline_seconds: Optional[float] = None
        if default_deadline_seconds is not None:
            parsed = float(default_deadline_seconds)
            if parsed > 0:
                self.default_deadline_seconds = parsed
        parsed_max_tasks = int(max_tasks)
        if parsed_max_tasks <= 0:
            parsed_max_tasks = int(DEFAULT_PMO_FORK_JOIN_MAX_TASKS)
        self.max_tasks = parsed_max_tasks

    @staticmethod
    def _parse_args(payload: Any) -> _ForkJoinArgs:
        try:
            return _ForkJoinArgs.model_validate(payload)
        except ValidationError as exc:
            raise _validation_error_to_bad_request("fork_join", exc) from None

    async def _build_task_specs(
        self,
        *,
        ctx: InternalHandlerContext,
        parsed_args: _ForkJoinArgs,
        conn: Any = None,
    ) -> List[Dict[str, Any]]:
        prepared_tasks: List[Dict[str, Any]] = []
        task_specs: List[Dict[str, Any]] = []
        seen_reuse_agent_ids: set[str] = set()

        for idx, task in enumerate(parsed_args.tasks):
            preview = await preview_target(
                ctx=ctx,
                target_strategy=task.target_strategy,
                target_ref=task.target_ref,
                conn=conn,
            )

            inherit_box_ids: List[str] = []
            if task.context_box_id:
                inherit_box_ids.append(task.context_box_id)
            if task.target_strategy == "clone":
                clone_source_agent_id = (
                    preview.clone_source_agent_id
                    if preview.clone_source_agent_id
                    else task.target_ref
                )
                clone_output_box_id = await resolve_current_output_box_id(
                    deps=ctx.deps,
                    project_id=ctx.meta.project_id,
                    agent_id=clone_source_agent_id,
                    conn=conn,
                )
                inherit_box_ids.append(clone_output_box_id)
            inherit_box_ids = await validate_box_ids_exist(
                deps=ctx.deps,
                project_id=ctx.meta.project_id,
                box_ids=inherit_box_ids,
                conn=conn,
            )

            if task.target_strategy == "reuse":
                reuse_agent_id = safe_str(preview.reuse_agent_id)
                if reuse_agent_id in seen_reuse_agent_ids:
                    raise BadRequestError(
                        f"fork_join: duplicate reuse target agent_id in batch: {reuse_agent_id}"
                    )
                seen_reuse_agent_ids.add(reuse_agent_id)
            prepared_tasks.append(
                {
                    "task_index": idx,
                    "task": task,
                    "preview": preview,
                    "inherit_box_ids": inherit_box_ids,
                }
            )

        for item in prepared_tasks:
            idx = int(item["task_index"])
            task = item["task"]
            preview = item["preview"]
            inherit_box_ids = item["inherit_box_ids"]

            context_box_id = await pack_context_with_instruction(
                deps=ctx.deps,
                project_id=ctx.meta.project_id,
                source_agent_id=ctx.cmd.source_agent_id,
                tool_suffix=ctx.meta.tool_suffix,
                profile_name=preview.profile_name,
                instruction=task.instruction,
                inherit_box_ids=inherit_box_ids,
                conn=conn,
            )
            resolved = await materialize_target(ctx=ctx, preview=preview, conn=conn)

            task_specs.append(
                {
                    "agent_id": resolved.agent_id,
                    "provision": False,
                    "profile_box_id": resolved.profile_box_id,
                    "context_box_id": context_box_id,
                    "task_args": {
                        "task_index": idx,
                        "label": f"task_{idx + 1}",
                        "instruction": task.instruction,
                        "target_strategy": task.target_strategy,
                        "target_ref": task.target_ref,
                        "profile_name": resolved.profile_name,
                    },
                    "runtime_config": {},
                }
            )
        return task_specs

    async def handle(
        self,
        *,
        ctx: InternalHandlerContext,
        parent_after_execution: str,
    ) -> InternalHandlerResult:
        cmd = ctx.cmd
        if cmd.tool_name != self.name:
            raise ProtocolViolationError(
                f"tool_name mismatch: expected {self.name}, got {safe_str(cmd.tool_name)}"
            )

        args_map = cmd.arguments if isinstance(cmd.arguments, dict) else {}
        parsed_args = self._parse_args(args_map)
        task_count = len(parsed_args.tasks)
        if task_count > self.max_tasks:
            raise BadRequestError(
                "fork_join: tasks fanout exceeds max limit "
                f"(got={task_count}, max={self.max_tasks}); "
                "split into smaller batches or adjust [pmo].fork_join_max_tasks"
            )

        effective_deadline = parsed_args.deadline_seconds
        if effective_deadline is None:
            effective_deadline = self.default_deadline_seconds

        try:
            parent_depth = require_recursion_depth(ctx.headers)
        except ProtocolViolationError as exc:
            raise ProtocolViolationError(f"fork_join: {exc}") from exc

        req = BatchCreateRequest(
            project_id=ctx.meta.project_id,
            channel_id=ctx.meta.channel_id,
            source_agent_id=cmd.source_agent_id,
            parent_agent_turn_id=safe_str(cmd.agent_turn_id),
            parent_turn_epoch=int(cmd.turn_epoch),
            parent_tool_call_id=safe_str(cmd.tool_call_id),
            parent_step_id=safe_str(cmd.step_id),
            lineage_parent_step_id=safe_str(getattr(cmd, "tool_call_parent_step_id", None)) or None,
            parent_after_execution=parent_after_execution,
            tool_suffix=ctx.meta.tool_suffix,
            task_specs=[],
            fail_fast=bool(parsed_args.fail_fast),
            deadline_seconds=effective_deadline,
            structured_output="digest",
            trace_id=trace_id_from_headers(ctx.headers),
            parent_traceparent=ctx.headers.get("traceparent") if isinstance(ctx.headers, dict) else None,
            recursion_depth=parent_depth,
        )

        state_pool = ctx.deps.state_store.pool
        async with UnitOfWork(state_pool).transaction() as tx:
            task_specs = await self._build_task_specs(
                ctx=ctx,
                parsed_args=parsed_args,
                conn=tx.conn,
            )
            req = BatchCreateRequest(
                project_id=req.project_id,
                channel_id=req.channel_id,
                source_agent_id=req.source_agent_id,
                parent_agent_turn_id=req.parent_agent_turn_id,
                parent_turn_epoch=req.parent_turn_epoch,
                parent_tool_call_id=req.parent_tool_call_id,
                parent_step_id=req.parent_step_id,
                lineage_parent_step_id=req.lineage_parent_step_id,
                parent_after_execution=req.parent_after_execution,
                tool_suffix=req.tool_suffix,
                task_specs=task_specs,
                fail_fast=req.fail_fast,
                deadline_seconds=req.deadline_seconds,
                structured_output=req.structured_output,
                trace_id=req.trace_id,
                parent_traceparent=req.parent_traceparent,
                recursion_depth=req.recursion_depth,
            )
            batch_id = await ctx.deps.batch_manager.create_batch(
                req=req,
                conn=tx.conn,
            )

        await ctx.deps.batch_manager.activate_batch(
            req=req,
            batch_id=batch_id,
            task_count=len(task_specs),
        )
        logger.info("Created fork_join batch=%s tasks=%s", batch_id, len(task_specs))
        return True, {"status": "accepted", "batch_id": batch_id, "task_count": len(task_specs)}
