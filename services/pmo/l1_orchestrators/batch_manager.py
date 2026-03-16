"""L1 BatchManager implementation (Issue #42: Batch/Fanout-Join orchestration)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Optional, Literal

import uuid6

from core.cg_context import CGContext
from core.utp_protocol import (
    extract_card_content_text,
    extract_content_fields,
)
from core.time_utils import utc_now_iso
from core.trace import TRACEPARENT_HEADER, build_traceparent
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.l0_engine import AddressIntent, DepthPolicy, JoinIntent, L0Engine, ReportIntent, TurnRef
from infra.stores import BatchStore, ExecutionStore, IdentityStore, ResourceStore, StateStore
from infra.observability.otel import (
    current_traceparent,
    extract_context_from_headers,
    get_tracer,
    link_from_traceparent,
    start_span,
    traced,
)

from services.pmo.agent_lifecycle import CreateAgentSpec, ensure_agent_ready
from infra.tool_executor import ToolResultBuilder, ToolResultContext, fetch_and_parse_tool_result
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from core.utils import safe_str, normalize_label
from .base import BatchCreateRequest
from core.status import (
    BATCH_TASK_FAILURE_SET,
    BATCH_TASK_STATUS_SET,
    BATCH_TASK_TERMINAL_SET,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_TIMEOUT,
)


logger = logging.getLogger("BatchManager")
_TRACER = get_tracer("services.pmo.l1_orchestrators.batch_manager")
_FORK_JOIN_TOOL_NAME = "fork_join"


def _derive_agent_instance_id(profile_box_id: str) -> str:
    return f"{str(profile_box_id).lower()}_{uuid6.uuid7().hex}"


def _hash_payload(obj: Dict[str, Any]) -> str:
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class BatchManager:
    """Stateful orchestration engine for fanout/join batch dispatch."""

    TERMINAL_TASK_STATUSES = BATCH_TASK_TERMINAL_SET
    DEFAULT_RECONCILE_MISSING_EPOCH_AFTER_S = 2.0
    DEFAULT_REVERT_MISSING_EPOCH_AFTER_S = 60.0
    DISPATCH_RETRY_MAX_ATTEMPTS = 5
    DISPATCH_RETRY_BASE_DELAY_SECONDS = 2.0
    DISPATCH_RETRY_MAX_DELAY_SECONDS = 30.0

    def __init__(
        self,
        *,
        resource_store: ResourceStore,
        state_store: StateStore,
        batch_store: BatchStore,
        cardbox: CardBoxClient,
        nats: NATSClient,
        execution_store: ExecutionStore,
        identity_store: IdentityStore,
        watchdog_limit: int = 200,
        dispatch_concurrency: int = 8,
    ) -> None:
        self.resource_store = resource_store
        self.state_store = state_store
        self.batch_store = batch_store
        self.cardbox = cardbox
        self.nats = nats
        self.execution_store = execution_store
        self.identity_store = identity_store
        self.watchdog_limit = watchdog_limit
        self.dispatch_concurrency = max(1, int(dispatch_concurrency))
        self.l0 = L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
            cardbox=self.cardbox,
        )

    @staticmethod
    def _ctx_snapshot(ctx: CGContext) -> Dict[str, Any]:
        snapshot = ctx.to_sys_dict()
        if ctx.headers:
            snapshot["headers"] = dict(ctx.headers)
        return snapshot

    @staticmethod
    def _ctx_from_snapshot(raw: Any) -> Optional[CGContext]:
        if not isinstance(raw, dict):
            return None
        try:
            return CGContext.from_sys_dict(raw)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Batch context snapshot restore failed: %s", exc)
            return None

    @staticmethod
    def _batch_source_ctx(base_ctx: CGContext) -> CGContext:
        return base_ctx.evolve(agent_id="sys.pmo.batch_manager", agent_turn_id="", step_id=None)

    @traced(
        _TRACER,
        "pmo.agent.provision",
        mark_error_on_exception=True,
    )
    async def _provision_batch_agent(
        self,
        *,
        batch_id: str,
        profile_box_id: str,
        create_spec: CreateAgentSpec,
        target_ctx: CGContext,
        conn: Any = None,
    ) -> Any:
        return await ensure_agent_ready(
            resource_store=self.resource_store,
            state_store=self.state_store,
            identity_store=self.identity_store,
            target_ctx=target_ctx,
            spec=create_spec,
            conn=conn,
        )

    @traced(
        _TRACER,
        "pmo.batch.create",
        span_arg="_span",
    )
    async def create_batch(
        self,
        *,
        req: BatchCreateRequest,
        conn: Any,
        _span: Any = None,
    ) -> str:
        parent_ctx = req.parent_ctx
        project_id = parent_ctx.project_id
        channel_id = parent_ctx.channel_id
        source_agent_id = parent_ctx.agent_id
        parent_tool_call_id = safe_str(parent_ctx.tool_call_id)
        parent_step_id = parent_ctx.step_id or ""
        lineage_parent_step_id = req.lineage_parent_step_id
        parent_after_execution = req.parent_after_execution
        tool_suffix = req.tool_suffix
        task_specs = req.task_specs
        fail_fast = req.fail_fast
        deadline_seconds = req.deadline_seconds
        structured_output = req.structured_output
        persisted_parent_ctx = parent_ctx.with_tool_call(parent_tool_call_id)

        if not task_specs:
            raise ValueError("internal batch requires non-empty task_specs")

        # Hard constraint: disallow duplicate explicit agent_id within a batch.
        # (Provisioned agents derive unique ids; reuse mode must not self-content.)
        seen_agent_ids: Dict[str, int] = {}
        for idx, t in enumerate(task_specs):
            provision = bool(t.get("provision", True))
            agent_id = safe_str(t.get("agent_id")) if t.get("agent_id") is not None else ""
            if provision or not agent_id:
                continue
            prev = seen_agent_ids.get(agent_id)
            if prev is not None:
                raise ValueError(
                    f"duplicate agent_id in batch task_specs: {agent_id} "
                    f"(task_specs[{prev}] and task_specs[{idx}])"
                )
            seen_agent_ids[agent_id] = idx

        batch_id = f"batch_{uuid6.uuid7().hex}"
        deadline_at: Optional[str] = None
        if deadline_seconds:
            deadline_at = (datetime.now(UTC) + timedelta(seconds=float(deadline_seconds))).isoformat()

        tool_name = _FORK_JOIN_TOOL_NAME
        batch_metadata: Dict[str, Any] = {
            "internal": True,
            "fail_fast": bool(fail_fast),
            "parent_after_execution": parent_after_execution,
            "tool_suffix": tool_suffix,
            "tool_name": tool_name,
            "parent_ctx": self._ctx_snapshot(persisted_parent_ctx),
        }
        if structured_output:
            batch_metadata["structured_output"] = str(structured_output)
        if lineage_parent_step_id:
            batch_metadata["lineage_parent_step_id"] = str(lineage_parent_step_id)

        tasks_to_insert: List[Dict[str, Any]] = []
        seen_task_indexes: Dict[int, int] = {}
        if _span is not None:
            _span.set_attribute("cg.batch_id", str(batch_id))
        for idx, t in enumerate(task_specs, start=1):
            profile_box_id = t.get("profile_box_id")
            context_box_id = t.get("context_box_id")
            task_args = t.get("task_args") or {}
            provision = bool(t.get("provision", True))
            agent_id = safe_str(t.get("agent_id")) if t.get("agent_id") is not None else ""
            output_box_id = safe_str(t.get("output_box_id")) if t.get("output_box_id") is not None else ""

            if not profile_box_id or not context_box_id:
                raise ValueError("internal batch task missing profile_box_id/context_box_id")
            if not isinstance(task_args, dict):
                task_args = {"value": task_args}
            runtime_config = t.get("runtime_config") or {}
            if not isinstance(runtime_config, dict):
                raise ValueError("internal batch task runtime_config must be an object")

            requested_task_index = task_args.get("task_index")
            if requested_task_index is None:
                stable_task_index = idx - 1
            else:
                stable_task_index = self._parse_task_index(
                    {"task_index": requested_task_index},
                    fallback=-1,
                )
                if stable_task_index < 0:
                    raise ValueError("internal batch task task_index must be a non-negative integer")
            previous_task = seen_task_indexes.get(stable_task_index)
            if previous_task is not None:
                raise ValueError(
                    f"duplicate task_index in batch task_specs: {stable_task_index} "
                    f"(task_specs[{previous_task}] and task_specs[{idx - 1}])"
                )
            seen_task_indexes[stable_task_index] = idx - 1

            label = normalize_label(task_args.get("label"))
            profile_name = (
                safe_str(task_args.get("profile_name")).strip()
                if safe_str(task_args.get("profile_name"))
                else ""
            )
            display_name = label or (
                f"{profile_name} #{idx}" if profile_name else f"{str(profile_box_id)} #{idx}"
            )

            metadata_raw = t.get("metadata")
            if metadata_raw is None:
                metadata_patch: Dict[str, Any] = {}
            elif isinstance(metadata_raw, dict):
                metadata_patch = dict(metadata_raw)
            else:
                raise ValueError("internal batch task metadata must be an object when provided")

            if provision:
                if agent_id:
                    raise ValueError("internal batch task agent_id not allowed when provision=true")
                if not agent_id:
                    agent_id = _derive_agent_instance_id(str(profile_box_id))
                agent_metadata: Dict[str, Any] = {
                    "spawned_by": source_agent_id,
                    "batch_id": batch_id,
                    **({"task_label": label} if label else {}),
                }
                if metadata_patch:
                    agent_metadata.update(metadata_patch)

                create_spec = CreateAgentSpec(
                    profile_box_id=str(profile_box_id),
                    metadata=agent_metadata,
                    display_name=display_name,
                    owner_agent_id=source_agent_id,
                    active_channel_id=channel_id,
                )
                lifecycle_result = await self._provision_batch_agent(
                    batch_id=batch_id,
                    profile_box_id=str(profile_box_id),
                    create_spec=create_spec,
                    target_ctx=parent_ctx.evolve(
                        agent_id=agent_id,
                        parent_agent_id=source_agent_id,
                        parent_step_id=parent_step_id,
                        trace_id=parent_ctx.trace_id,
                    ),
                    conn=conn,
                )
                if lifecycle_result.error_code:
                    raise ValueError(
                        f"internal batch agent init failed: {lifecycle_result.error_code}"
                    )
            else:
                if not agent_id:
                    raise ValueError("internal batch task missing agent_id when provision=false")
                roster = await self.resource_store.fetch_roster(
                    parent_ctx.evolve(agent_id=agent_id, channel_id=channel_id),
                    conn=conn,
                )
                if not roster:
                    raise ValueError(f"internal batch agent not found: {agent_id}")
                roster_profile_box_id = safe_str(roster.get("profile_box_id"))
                if not roster_profile_box_id:
                    raise ValueError(f"internal batch agent profile_box_id missing: {agent_id}")
                if safe_str(profile_box_id) and roster_profile_box_id != safe_str(profile_box_id):
                    raise ValueError(f"internal batch agent profile mismatch: {agent_id}")
                profile_box_id = roster_profile_box_id
                if not display_name:
                    display_name = safe_str(roster.get("display_name")) or None

            payload_hash = _hash_payload(
                {
                    "agent_id": agent_id,
                    "profile_box_id": str(profile_box_id),
                    "context_box_id": str(context_box_id),
                    **({"output_box_id": output_box_id} if output_box_id else {}),
                    "task_args": task_args,
                    "runtime_config": runtime_config,
                }
            )

            # Persist input order explicitly so result mapping never depends on DB row order.
            task_metadata = {
                "task_args": task_args,
                "task_index": stable_task_index,
            }
            if runtime_config:
                task_metadata["runtime_config"] = runtime_config
            tasks_to_insert.append(
                {
                    "batch_task_id": f"bt_{uuid6.uuid7().hex}",
                    "batch_id": batch_id,
                    "project_id": project_id,
                    "agent_id": agent_id,
                    "status": STATUS_PENDING,
                    "attempt_count": 0,
                    "current_agent_turn_id": None,
                    "payload_hash": payload_hash,
                    "profile_box_id": str(profile_box_id),
                    "context_box_id": str(context_box_id),
                    "output_box_id": output_box_id or None,
                    "metadata": task_metadata,
                }
            )

        await self.batch_store.insert_batch(
            batch_id=batch_id,
            parent_ctx=persisted_parent_ctx,
            task_count=len(tasks_to_insert),
            deadline_at=deadline_at,
            metadata=batch_metadata,
            conn=conn,
        )
        await self.batch_store.insert_batch_tasks(tasks_to_insert, conn=conn)

        return batch_id

    async def activate_batch(
        self,
        *,
        req: BatchCreateRequest,
        batch_id: str,
        task_count: int,
        deadline_at: Optional[str] = None,
        tool_name: str = _FORK_JOIN_TOOL_NAME,
    ) -> None:
        parent_ctx = req.parent_ctx
        project_id = parent_ctx.project_id
        source_agent_id = parent_ctx.agent_id
        fail_fast = bool(req.fail_fast)

        if deadline_at is None:
            batch_row = await self.batch_store.fetch_batch(batch_id=batch_id, project_id=project_id)
            if isinstance(batch_row, dict):
                deadline_raw = batch_row.get("deadline_at")
                deadline_at = str(deadline_raw) if deadline_raw is not None else None

        join_source_ctx = parent_ctx.evolve(
            parent_step_id=parent_ctx.step_id,
        ).with_transport(
            trace_id=parent_ctx.trace_id,
            recursion_depth=int(parent_ctx.recursion_depth),
        )

        join_result = await self._join_with_tx(
            phase="request",
            source_ctx=join_source_ctx,
            target_ref=TurnRef(
                project_id=parent_ctx.project_id,
                agent_id=parent_ctx.agent_id,
                agent_turn_id=parent_ctx.require_agent_turn_id,
            ),
            correlation_id=batch_id,
            metadata={
                "batch_id": batch_id,
                "task_count": int(task_count),
                "fail_fast": bool(fail_fast),
                "deadline_at": deadline_at,
                "tool_name": tool_name,
            },
        )
        if join_result.status in ("rejected", "error"):
            logger.warning(
                "join ack failed (phase=request status=%s) parent=%s batch=%s",
                join_result.status,
                source_agent_id,
                batch_id,
            )

        logger.info(
            "Created internal batch=%s for parent=%s tasks=%s",
            batch_id,
            source_agent_id,
            int(task_count),
        )

        await self.dispatch_pending_tasks(limit=int(task_count))

    async def _report_with_doorbell(
        self,
        *,
        source_ctx: CGContext,
        target_ref: TurnRef,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: str,
    ) -> None:
        wakeup_signals = []
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                intent_source_ctx = self._batch_source_ctx(source_ctx)
                result = await self.l0.report_intent(
                    source_ctx=intent_source_ctx,
                    intent=ReportIntent(
                        target=target_ref,
                        message_type=message_type,
                        payload=payload,
                        correlation_id=correlation_id,
                    ),
                    conn=conn,
                    wakeup=True,
                )
                wakeup_signals = list(result.wakeup_signals or ())
        if wakeup_signals:
            await self.l0.publish_wakeup_signals(wakeup_signals)

    async def _enqueue_address_with_doorbell(
        self,
        *,
        source_ctx: CGContext,
        target_ref: TurnRef,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        wakeup_signals = []
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                intent_source_ctx = self._batch_source_ctx(source_ctx)
                result = await self.l0.enqueue_intent(
                    source_ctx=intent_source_ctx,
                    intent=AddressIntent(
                        target=target_ref,
                        message_type=message_type,
                        payload=payload,
                        correlation_id=correlation_id,
                    ),
                    wakeup=True,
                    conn=conn,
                )
                wakeup_signals = list(result.wakeup_signals or ())
        if wakeup_signals:
            await self.l0.publish_wakeup_signals(wakeup_signals)

    async def _join_with_tx(
        self,
        *,
        phase: Literal["request", "response"],
        source_ctx: CGContext,
        target_ref: TurnRef,
        correlation_id: str,
        metadata: Dict[str, Any],
    ) -> Any:
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                intent_source_ctx = self._batch_source_ctx(source_ctx)
                return await self.l0.join_intent(
                    source_ctx=intent_source_ctx,
                    intent=JoinIntent(
                        target=target_ref,
                        phase=phase,
                        correlation_id=correlation_id,
                        metadata=metadata,
                    ),
                    conn=conn,
                )

    async def dispatch_pending_tasks(self, *, limit: int = 200) -> None:
        rows = await self.batch_store.list_pending_tasks(limit=min(limit, self.watchdog_limit))
        if not rows:
            return
        dispatcher = AgentDispatcher(
            resource_store=self.resource_store,
            state_store=self.state_store,
            cardbox=self.cardbox,
            nats=self.nats,
        )

        async def _dispatch_row(row: Dict[str, Any]) -> None:
            project_id = row["project_id"]
            batch_task_id = row["batch_task_id"]
            agent_id = row["agent_id"]
            profile_box_id = row.get("profile_box_id")
            context_box_id = row.get("context_box_id")
            preset_output_box_id = safe_str(row.get("output_box_id")) if row.get("output_box_id") else ""
            task_meta = row.get("task_metadata") or {}
            task_args = task_meta.get("task_args") if isinstance(task_meta, dict) else None
            if not isinstance(task_args, dict):
                task_args = {}
            batch_meta = row.get("batch_metadata") or {}
            parent_ctx = self._ctx_from_snapshot(
                batch_meta.get("parent_ctx") if isinstance(batch_meta, dict) else None,
            )
            if parent_ctx is None:
                logger.warning(
                    "Batch dispatch skipped: missing parent_ctx snapshot task=%s batch=%s",
                    batch_task_id,
                    row.get("batch_id"),
                )
                return
            runtime_config = {}
            if isinstance(task_meta, dict) and isinstance(task_meta.get("runtime_config"), dict):
                runtime_config = dict(task_meta["runtime_config"])
            display_name = normalize_label(task_args.get("label")) or safe_str(task_args.get("profile_name")) or None

            output_box_id = await self.cardbox.ensure_box_id(
                project_id=project_id,
                box_id=preset_output_box_id or None,
            )

            claimed = await self.batch_store.claim_task_dispatched(
                batch_task_id=batch_task_id,
                project_id=project_id,
                agent_turn_id=None,
                output_box_id=output_box_id,
            )
            if not claimed:
                return

            parent_trace_id = safe_str(parent_ctx.trace_id)
            parent_tp = safe_str(parent_ctx.headers.get(TRACEPARENT_HEADER))
            parent_current_tp = current_traceparent() or parent_tp
            child_tp, child_trace_id = build_traceparent(None)
            dispatch_lineage_ctx = parent_ctx.with_trace_transport(
                traceparent=child_tp,
                parent_traceparent=parent_current_tp or None,
                trace_id=child_trace_id,
                default_depth=int(parent_ctx.recursion_depth),
            )
            child_link = link_from_traceparent(
                safe_str(child_tp),
                attributes={
                    "cg.link": "child",
                    "cg.batch_task_id": str(batch_task_id),
                },
            )
            links = [child_link] if child_link else None

            span_context = None
            # If this dispatch is running out-of-band (watchdog), use stored parent traceparent
            # to keep BatchManager spans in the original run trace.
            if parent_tp and current_traceparent() is None:
                ctx = extract_context_from_headers({TRACEPARENT_HEADER: parent_tp})
                if ctx is not None:
                    span_context = ctx

            dispatch_span_attrs = {
                "cg.project_id": str(project_id),
                "cg.batch_id": str(row.get("batch_id")),
                "cg.batch_task_id": str(batch_task_id),
                "cg.agent_id": str(agent_id),
                "cg.child_trace_id": str(child_trace_id),
            }
            with start_span(
                _TRACER,
                "pmo.batch.dispatch_task",
                context=span_context,
                extra_links=links,
                attributes=dispatch_span_attrs,
                record_exception=True,
                set_status_on_exception=True,
                mark_error_on_exception=True,
            ) as span:
                await self.batch_store.patch_task_metadata(
                    batch_task_id=batch_task_id,
                    project_id=project_id,
                    metadata_patch={
                        "child_trace_id": str(child_trace_id),
                        "child_traceparent": str(child_tp),
                        "parent_trace_id": str(parent_trace_id) if parent_trace_id else None,
                        "parent_traceparent": str(parent_current_tp) if parent_current_tp else None,
                        "dispatch_channel_id": parent_ctx.channel_id,
                    },
                )
                dispatch_result = await dispatcher.dispatch(
                    DispatchRequest(
                        source_ctx=self._batch_source_ctx(dispatch_lineage_ctx),
                        target_agent_id=str(agent_id),
                        target_channel_id=parent_ctx.channel_id,
                        lineage_ctx=dispatch_lineage_ctx,
                        correlation_id=batch_task_id,
                        depth_policy=DepthPolicy.DESCEND,
                        profile_box_id=safe_str(profile_box_id),
                        context_box_id=safe_str(context_box_id),
                        output_box_id=output_box_id,
                        display_name=display_name,
                        runtime_config=runtime_config,
                        emit_state_event=True,
                    )
                )

                span.set_attribute("cg.dispatch_status", str(dispatch_result.status))
                if dispatch_result.agent_turn_id:
                    span.set_attribute("cg.child_agent_turn_id", str(dispatch_result.agent_turn_id))

                dispatch_status = str(dispatch_result.status or "")
                dispatch_error_code = safe_str(dispatch_result.error_code) or ""
                prior_attempt = int(row.get("attempt_count") or 0)
                attempt_count = max(1, prior_attempt + 1)
                retry_delay = min(
                    self.DISPATCH_RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt_count - 1)),
                    self.DISPATCH_RETRY_MAX_DELAY_SECONDS,
                )
                # Fail-fast policy by design: any non-internal dispatch error is surfaced
                # to the parent agent as a terminal batch-task failure, and retries are
                # handled at business/orchestration level upstream instead of PMO/L0.
                deterministic_error = (
                    dispatch_status == "rejected"
                    or (dispatch_status == "error" and dispatch_error_code != "internal_error")
                )

                if deterministic_error:
                    error_detail = (
                        f"dispatch_{dispatch_status}:{dispatch_error_code}"
                        if dispatch_error_code
                        else f"dispatch_{dispatch_status}"
                    )
                    failed_batch_id = await self.batch_store.mark_task_dispatch_failed(
                        batch_task_id=batch_task_id,
                        project_id=project_id,
                        agent_turn_id=None,
                        error_detail=error_detail,
                        metadata_patch={
                            "dispatch_status": dispatch_status,
                            "dispatch_error_code": dispatch_error_code or None,
                            "dispatch_failed_at": utc_now_iso(),
                            "dispatch_attempt_count": attempt_count,
                        },
                    )
                    logger.warning(
                        "Batch dispatch deterministic failure task=%s status=%s code=%s attempt=%s",
                        batch_task_id,
                        dispatch_status,
                        dispatch_error_code or "-",
                        attempt_count,
                    )
                    if failed_batch_id:
                        await self._maybe_finalize_batch(batch_id=failed_batch_id, project_id=project_id)
                    return

                transient_retry = dispatch_status in ("busy", "error")
                if transient_retry and attempt_count >= self.DISPATCH_RETRY_MAX_ATTEMPTS:
                    failed_batch_id = await self.batch_store.mark_task_dispatch_failed(
                        batch_task_id=batch_task_id,
                        project_id=project_id,
                        agent_turn_id=None,
                        error_detail="dispatch_retry_exhausted",
                        metadata_patch={
                            "dispatch_status": dispatch_status,
                            "dispatch_error_code": dispatch_error_code or None,
                            "dispatch_failed_at": utc_now_iso(),
                            "dispatch_attempt_count": attempt_count,
                            "dispatch_retry_exhausted": True,
                        },
                    )
                    logger.warning(
                        "Batch dispatch retry exhausted task=%s status=%s code=%s attempts=%s",
                        batch_task_id,
                        dispatch_status,
                        dispatch_error_code or "-",
                        attempt_count,
                    )
                    if failed_batch_id:
                        await self._maybe_finalize_batch(batch_id=failed_batch_id, project_id=project_id)
                    return

                if transient_retry:
                    await self.batch_store.revert_task_to_pending(
                        batch_task_id=batch_task_id,
                        project_id=project_id,
                        agent_turn_id=None,
                        retry_delay_seconds=retry_delay,
                        metadata_patch={
                            "dispatch_status": dispatch_status,
                            "dispatch_error_code": dispatch_error_code or None,
                            "dispatch_retry_scheduled_at": utc_now_iso(),
                            "dispatch_attempt_count": attempt_count,
                            "dispatch_next_retry_delay_seconds": retry_delay,
                        },
                    )
                    logger.info(
                        "Batch dispatch transient failure task=%s status=%s code=%s attempt=%s retry_in=%.1fs",
                        batch_task_id,
                        dispatch_status,
                        dispatch_error_code or "-",
                        attempt_count,
                        retry_delay,
                    )
                    return
                resolved_turn_id = safe_str(dispatch_result.agent_turn_id)
                if not resolved_turn_id:
                    raise RuntimeError(
                        f"batch dispatch accepted without agent_turn_id: task={batch_task_id}"
                    )
                await self.batch_store.set_task_turn_identity(
                    batch_task_id=batch_task_id,
                    project_id=project_id,
                    agent_turn_id=resolved_turn_id,
                    turn_epoch=(
                        int(dispatch_result.turn_epoch)
                        if dispatch_result.turn_epoch is not None
                        else None
                    ),
                )
                if dispatch_result.turn_epoch is None:
                    logger.info(
                        "Batch dispatched task=%s agent=%s turn=%s (lease pending)",
                        batch_task_id,
                        agent_id,
                        resolved_turn_id,
                    )
                    return
                logger.info(
                    "Batch dispatched task=%s agent=%s turn=%s epoch=%s",
                    batch_task_id,
                    agent_id,
                    resolved_turn_id,
                    dispatch_result.turn_epoch,
                )

        # Dispatch tasks concurrently to avoid O(N) ACK wait amplification under internal.fork_join.
        # Keep a modest limit to avoid flooding NATS/DB under large batches.
        sem = asyncio.Semaphore(self.dispatch_concurrency)

        async def _run(row: Dict[str, Any]) -> None:
            async with sem:
                await _dispatch_row(row)

        results = await asyncio.gather(*[_run(r) for r in rows], return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                logger.exception("Batch dispatch task failed: %s", res)

    async def reconcile_dispatched_tasks(
        self,
        *,
        reconcile_after_seconds: float = DEFAULT_RECONCILE_MISSING_EPOCH_AFTER_S,
        revert_after_seconds: float = DEFAULT_REVERT_MISSING_EPOCH_AFTER_S,
        limit: int = 200,
    ) -> None:
        """Observe lease-pending dispatched tasks and nudge downstream execution when needed."""

        async def _resolve_child_runtime(
            *,
            project_id: str,
            agent_id: str,
            agent_turn_id: str,
        ) -> tuple[Optional[str], Optional[int]]:
            try:
                head = await self.state_store.fetch(
                    CGContext(project_id=project_id, agent_id=agent_id),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Batch reconcile state-head lookup failed agent=%s turn=%s: %s",
                    agent_id,
                    agent_turn_id,
                    exc,
                )
                head = None
            if head and safe_str(getattr(head, "active_agent_turn_id", None)) == agent_turn_id:
                try:
                    head_epoch = int(getattr(head, "turn_epoch", None))
                except Exception:  # noqa: BLE001
                    head_epoch = None
                return safe_str(getattr(head, "status", None)) or "active", head_epoch
            try:
                rows = await self.execution_store.list_inbox_by_agent_turn_id(
                    ctx=CGContext(project_id=project_id, agent_id=agent_id),
                    agent_turn_id=agent_turn_id,
                    limit=1,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Batch reconcile inbox lookup failed agent=%s turn=%s: %s",
                    agent_id,
                    agent_turn_id,
                    exc,
                )
                return None, None
            if not rows:
                return None, None
            row = rows[0] or {}
            status = safe_str(row.get("status")) if isinstance(row, dict) else None
            return status, None

        reconcile_after = max(0.0, float(reconcile_after_seconds))
        revert_after = max(reconcile_after, float(revert_after_seconds))
        limit_value = min(int(limit), self.watchdog_limit)

        async def _observe_pending_row(
            row: Dict[str, Any],
            *,
            allow_fail_downstream: bool,
        ) -> None:
            project_id = safe_str(row.get("project_id"))
            batch_task_id = safe_str(row.get("batch_task_id"))
            agent_id = safe_str(row.get("agent_id"))
            agent_turn_id = safe_str(row.get("current_agent_turn_id"))
            if not project_id or not batch_task_id or not agent_id or not agent_turn_id:
                return

            status, resolved_epoch = await _resolve_child_runtime(
                project_id=project_id,
                agent_id=agent_id,
                agent_turn_id=agent_turn_id,
            )
            if resolved_epoch is not None and int(resolved_epoch) > 0:
                return
            if status in ("running", "processing"):
                return
            task_meta_any = row.get("task_metadata")
            task_meta = task_meta_any if isinstance(task_meta_any, dict) else {}
            batch_id = safe_str(row.get("batch_id"))
            metadata_patch: Dict[str, Any] = {}
            first_soft_signal = task_meta.get("dispatch_warning_at") is None
            if first_soft_signal:
                metadata_patch.update(
                    {
                        "dispatch_warning_code": "ack_pending_window_exceeded",
                        "dispatch_warning_at": utc_now_iso(),
                        "dispatch_warning_inbox_status": status or "missing",
                    }
                )

            wakeup_sent = task_meta.get("dispatch_wakeup_sent_at") is not None
            if status == "queued" and not wakeup_sent:
                wakeup_channel_id = safe_str(task_meta.get("dispatch_channel_id")) or "public"
                try:
                    wakeup_result = await self.l0.wakeup(
                        target_ctx=CGContext(
                            project_id=project_id,
                            channel_id=wakeup_channel_id,
                            agent_id=agent_id,
                        ),
                        mode="dispatch_next",
                        reason="batch_reconcile_ack_pending_window_exceeded",
                        metadata={
                            "batch_task_id": batch_task_id,
                            **({"batch_id": batch_id} if batch_id else {}),
                        },
                    )
                    wakeup_status = str(wakeup_result.ack_status)
                    if wakeup_status == "accepted":
                        metadata_patch["dispatch_wakeup_sent_at"] = utc_now_iso()
                        metadata_patch["dispatch_wakeup_ack_status"] = wakeup_status
                    else:
                        metadata_patch["dispatch_wakeup_last_status"] = wakeup_status
                        logger.warning(
                            "Batch reconcile dispatch_next returned status=%s task=%s agent=%s turn=%s",
                            wakeup_status,
                            batch_task_id,
                            agent_id,
                            agent_turn_id,
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Batch reconcile dispatch_next failed task=%s agent=%s turn=%s: %s",
                        batch_task_id,
                        agent_id,
                        agent_turn_id,
                        exc,
                    )

            fail_fast_enabled = row.get("fail_fast") is True
            deadline_raw = row.get("deadline_at")
            deadline_reached = (
                isinstance(deadline_raw, datetime)
                and deadline_raw <= datetime.now(UTC)
            )
            should_fail_downstream = allow_fail_downstream and (fail_fast_enabled or deadline_reached)

            if should_fail_downstream:
                metadata_patch["result_view"] = {
                    "error_code": "downstream_unavailable",
                    "inbox_status": status or "missing",
                }
                failed_batch_id = await self.batch_store.mark_task_terminal_from_turn(
                    project_id=project_id,
                    agent_turn_id=agent_turn_id,
                    new_status=STATUS_FAILED,
                    output_box_id=None,
                    metadata_patch=metadata_patch,
                    error_detail="downstream_unavailable",
                )
                if failed_batch_id:
                    await self._maybe_finalize_batch(batch_id=failed_batch_id, project_id=project_id)
                    logger.warning(
                        "Batch task failed fast as downstream_unavailable task=%s agent=%s turn=%s fail_fast=%s deadline_reached=%s inbox_status=%s",
                        batch_task_id,
                        agent_id,
                        agent_turn_id,
                        fail_fast_enabled,
                        deadline_reached,
                        status or "missing",
                    )
                return

            if metadata_patch:
                await self.batch_store.patch_task_metadata(
                    batch_task_id=batch_task_id,
                    project_id=project_id,
                    metadata_patch=metadata_patch,
                )
                if first_soft_signal:
                    logger.warning(
                        "Batch dispatch pending window exceeded task=%s agent=%s turn=%s inbox_status=%s",
                        batch_task_id,
                        agent_id,
                        agent_turn_id,
                        status or "missing",
                    )

        pending = await self.batch_store.list_stuck_dispatched_missing_epoch(
            older_than_seconds=reconcile_after,
            limit=limit_value,
        )
        for row in pending or []:
            await _observe_pending_row(row, allow_fail_downstream=False)

        stale = await self.batch_store.list_stuck_dispatched_missing_epoch(
            older_than_seconds=revert_after,
            limit=limit_value,
        )
        for row in stale or []:
            await _observe_pending_row(row, allow_fail_downstream=True)

    async def handle_child_task_event(
        self,
        *,
        project_id: str,
        agent_turn_id: str,
        status: str,
        output_box_id: Optional[str],
        deliverable_card_id: Optional[str],
        tool_result_card_id: Optional[str],
        error: Optional[str],
    ) -> None:
        batch_context = await self.batch_store.fetch_task_context_by_turn(
            project_id=project_id,
            agent_turn_id=agent_turn_id,
        )
        batch_meta = batch_context.get("batch_metadata") if isinstance(batch_context, dict) else None
        structured_output = safe_str(batch_meta.get("structured_output")) if isinstance(batch_meta, dict) else ""
        if not structured_output:
            structured_output = "digest"

        result_view: Dict[str, Any] = {}
        if deliverable_card_id:
            result_view["deliverable_card_id"] = str(deliverable_card_id)
            try:
                cards = await self.cardbox.get_cards([str(deliverable_card_id)], project_id=project_id)
                deliverable = cards[0] if cards else None
                content = getattr(deliverable, "content", None) if deliverable else None
                fields_out = extract_content_fields(content)
                if fields_out is not None:
                    result_view["result_fields"] = fields_out
                    digest = " | ".join(
                        [f'{it["name"]}: {it["value"]}' for it in fields_out[:5] if it.get("value")]
                    ).strip()
                    result_view["digest"] = digest
                else:
                    result_view["digest"] = extract_card_content_text(deliverable).strip()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Batch result_view build failed deliverable_card_id=%s: %s", deliverable_card_id, exc)
                result_view = {
                    "deliverable_card_id": str(deliverable_card_id),
                    "error_code": "deliverable_read_failed",
                }
        else:
            result_view = {"error_code": "missing_deliverable_card_id"}

        if tool_result_card_id:
            result_view["tool_result_card_id"] = str(tool_result_card_id)
            if structured_output == "tool_result":
                read_result = await fetch_and_parse_tool_result(
                    cardbox=self.cardbox,
                    project_id=project_id,
                    card_id=tool_result_card_id,
                )
                if read_result.error_code == "card_not_found":
                    result_view["tool_result_error"] = "tool_result_not_found"
                elif read_result.ok and isinstance(read_result.payload, dict):
                    result_view["tool_result_payload"] = read_result.payload
                else:
                    err_detail = read_result.error_message or read_result.error_code or "unknown"
                    logger.warning(
                        "Batch tool_result payload read failed tool_result_card_id=%s err=%s",
                        tool_result_card_id,
                        err_detail,
                    )
                    result_view["tool_result_error"] = "tool_result_read_failed"

        new_status = status if status in BATCH_TASK_STATUS_SET else STATUS_FAILED
        batch_id = await self.batch_store.mark_task_terminal_from_turn(
            project_id=project_id,
            agent_turn_id=agent_turn_id,
            new_status=new_status,
            output_box_id=output_box_id,
            metadata_patch={"result_view": result_view},
            error_detail=error,
        )
        if not batch_id:
            return
        await self._maybe_finalize_batch(batch_id=batch_id, project_id=project_id)

    async def reap_timed_out_batches(self) -> None:
        batches = await self.batch_store.list_expired_running_batches(limit=50)
        for b in batches:
            batch_id = b["batch_id"]
            project_id = b["project_id"]
            claimed = await self.batch_store.claim_batch_terminal(
                batch_id=batch_id,
                project_id=project_id,
                final_status=STATUS_TIMEOUT,
            )
            if not claimed:
                continue
            await self._terminate_children(batch_id=batch_id, project_id=project_id)
            await self.batch_store.abort_remaining_tasks(batch_id=batch_id, project_id=project_id)
            await self._resume_parent(batch_id=batch_id, project_id=project_id, final_status=STATUS_TIMEOUT)

    async def _maybe_finalize_batch(self, *, batch_id: str, project_id: str) -> None:
        batch = await self.batch_store.fetch_batch(batch_id=batch_id, project_id=project_id)
        if not batch or batch.get("status") != STATUS_RUNNING:
            return
        tasks = await self.batch_store.fetch_tasks_for_batch(batch_id=batch_id, project_id=project_id)
        terminal = [t for t in tasks if t.get("status") in self.TERMINAL_TASK_STATUSES]
        fail_fast = bool((batch.get("metadata") or {}).get("fail_fast"))
        any_failed = any(t.get("status") in BATCH_TASK_FAILURE_SET for t in tasks)

        if fail_fast and any_failed:
            claimed = await self.batch_store.claim_batch_terminal(
                batch_id=batch_id,
                project_id=project_id,
                final_status=STATUS_FAILED,
            )
            if claimed:
                await self._terminate_children(batch_id=batch_id, project_id=project_id)
                await self.batch_store.abort_remaining_tasks(batch_id=batch_id, project_id=project_id)
                await self._resume_parent(batch_id=batch_id, project_id=project_id, final_status=STATUS_FAILED)
            return

        if len(terminal) >= int(batch.get("task_count") or 0):
            statuses = {t.get("status") for t in tasks}
            if statuses == {STATUS_SUCCESS}:
                final_status = STATUS_SUCCESS
            elif STATUS_SUCCESS in statuses:
                final_status = STATUS_PARTIAL
            elif STATUS_FAILED in statuses:
                final_status = STATUS_FAILED
            elif STATUS_TIMEOUT in statuses:
                final_status = STATUS_TIMEOUT
            elif STATUS_PARTIAL in statuses:
                final_status = STATUS_PARTIAL
            else:
                final_status = STATUS_FAILED
            claimed = await self.batch_store.claim_batch_terminal(
                batch_id=batch_id,
                project_id=project_id,
                final_status=final_status,
            )
            if claimed:
                await self._resume_parent(batch_id=batch_id, project_id=project_id, final_status=final_status)

    async def _terminate_children(self, *, batch_id: str, project_id: str) -> None:
        """Best-effort stop/cancel all non-terminal children for a batch.

        - Dispatched tasks with known epoch -> send stop signal.
        - Dispatched tasks missing epoch:
          - inbox.status='queued' -> cancel (prevent future dispatch).
          - inbox has turn_epoch -> send stop signal (pending/processing/running path).
        """
        batch = await self.batch_store.fetch_batch(batch_id=batch_id, project_id=project_id)
        if not batch:
            return
        meta = batch.get("metadata") or {}
        parent_ctx = self._ctx_from_snapshot(meta.get("parent_ctx") if isinstance(meta, dict) else None)
        if parent_ctx is None:
            logger.warning(
                "Batch terminate skipped: missing parent_ctx snapshot batch=%s",
                batch_id,
            )
            return
        transport_ctx = parent_ctx.evolve(
            agent_id="sys.pmo.batch_manager",
        ).with_trace_transport(
            base_headers=parent_ctx.headers,
            trace_id=parent_ctx.trace_id,
            default_depth=int(parent_ctx.recursion_depth),
        )

        # 1) Stop children we can address directly (epoch known in batch store).
        turns = await self.batch_store.list_active_turns_for_batch(batch_id=batch_id, project_id=project_id)
        for r in turns:
            agent_turn_id = safe_str(r.get("current_agent_turn_id"))
            if not agent_turn_id:
                continue
            payload = {
                "agent_id": r["agent_id"],
                "agent_turn_id": agent_turn_id,
                "turn_epoch": r["current_turn_epoch"],
                "reason": "batch_terminated",
            }
            await self._enqueue_address_with_doorbell(
                source_ctx=transport_ctx,
                target_ref=TurnRef(
                    project_id=project_id,
                    agent_id=str(r["agent_id"]),
                    agent_turn_id=agent_turn_id,
                    expected_turn_epoch=(
                        int(r["current_turn_epoch"])
                        if r.get("current_turn_epoch") is not None
                        else None
                    ),
                ),
                message_type="stop",
                payload=payload,
            )
            logger.warning(
                "Sent stop to child agent=%s turn=%s epoch=%s batch=%s",
                r["agent_id"],
                agent_turn_id,
                r["current_turn_epoch"],
                batch_id,
            )

        # 2) Best-effort: stop/cancel children missing epoch in batch store (mailbox semantics).
        tasks = await self.batch_store.fetch_tasks_for_batch(batch_id=batch_id, project_id=project_id)
        for t in tasks or []:
            if t.get("status") not in ("dispatched", "pending"):
                continue
            agent_id = safe_str(t.get("agent_id"))
            agent_turn_id = safe_str(t.get("current_agent_turn_id"))
            if not agent_id or not agent_turn_id:
                continue
            if t.get("current_turn_epoch") is not None:
                continue

            cancel_result = await self.l0.cancel(
                ctx=parent_ctx.evolve(
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                ),
                reason="batch_terminated",
                metadata={"batch_id": batch_id},
            )
            if bool((cancel_result.payload or {}).get("canceled")):
                logger.warning(
                    "Canceled queued child agent=%s turn=%s batch=%s",
                    agent_id,
                    agent_turn_id,
                    batch_id,
                )
                continue

            stop_payload = {
                "agent_id": agent_id,
                "agent_turn_id": agent_turn_id,
                "reason": "batch_terminated",
            }
            await self._enqueue_address_with_doorbell(
                source_ctx=transport_ctx,
                target_ref=TurnRef(
                    project_id=project_id,
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                ),
                message_type="stop",
                payload=stop_payload,
            )
            logger.warning(
                "Sent stop to child without stored epoch agent=%s turn=%s batch=%s",
                agent_id,
                agent_turn_id,
                batch_id,
            )

    @staticmethod
    def _resolve_resume_tool_name(meta: Dict[str, Any]) -> str:
        tool_name = safe_str(meta.get("tool_name")) if isinstance(meta, dict) else ""
        if tool_name:
            return tool_name
        return _FORK_JOIN_TOOL_NAME

    @staticmethod
    def _extract_result_summary(result_view: Any) -> str:
        if not isinstance(result_view, dict):
            return ""
        digest = safe_str(result_view.get("digest"))
        if digest:
            return digest
        fields = result_view.get("result_fields")
        if isinstance(fields, list):
            for entry in fields:
                if not isinstance(entry, dict):
                    continue
                if safe_str(entry.get("name")).strip().lower() != "summary":
                    continue
                value = entry.get("value")
                if isinstance(value, str):
                    return value.strip()
                return safe_str(value)
        return ""

    @staticmethod
    def _parse_task_index(task_meta: Dict[str, Any], fallback: int) -> int:
        raw = task_meta.get("task_index")
        if isinstance(raw, int) and raw >= 0:
            return raw
        if isinstance(raw, str):
            try:
                parsed = int(raw)
            except Exception:  # noqa: BLE001
                parsed = -1
            if parsed >= 0:
                return parsed
        return fallback

    @classmethod
    def _build_fork_join_result(cls, *, tasks: List[Dict[str, Any]], final_status: str) -> Dict[str, Any]:
        # Use persisted task_index to guarantee stable mapping to caller input order.
        normalized: List[tuple[int, Dict[str, Any], Dict[str, Any]]] = []
        for fallback_idx, task in enumerate(tasks):
            task_meta = task.get("metadata") if isinstance(task, dict) else {}
            task_meta = task_meta if isinstance(task_meta, dict) else {}
            stable_idx = cls._parse_task_index(task_meta, fallback=fallback_idx)
            normalized.append((stable_idx, task, task_meta))
        normalized.sort(key=lambda item: item[0])

        results: List[Dict[str, Any]] = []
        for task_index, task, task_meta in normalized:
            result_view = task_meta.get("result_view")
            task_status = safe_str(task.get("status")) or STATUS_FAILED

            result_item: Dict[str, Any] = {
                "task_index": task_index,
                "status": task_status,
            }
            summary = cls._extract_result_summary(result_view)
            if summary:
                result_item["summary"] = summary

            output_box_id = safe_str(task.get("output_box_id"))
            if output_box_id:
                result_item["output_box_id"] = output_box_id

            error = safe_str(task.get("error_detail"))
            if not error and isinstance(result_view, dict):
                error = (
                    safe_str(result_view.get("tool_result_error"))
                    or safe_str(result_view.get("error_code"))
                )
            if error and task_status != STATUS_SUCCESS:
                result_item["error"] = error

            results.append(result_item)
        return {"status": final_status, "results": results}

    async def _resume_parent(self, *, batch_id: str, project_id: str, final_status: str) -> None:
        batch = await self.batch_store.fetch_batch(batch_id=batch_id, project_id=project_id)
        tasks = await self.batch_store.fetch_tasks_for_batch(batch_id=batch_id, project_id=project_id)
        if not batch:
            return
        meta = batch.get("metadata") or {}
        parent_ctx = self._ctx_from_snapshot(meta.get("parent_ctx") if isinstance(meta, dict) else None)
        if parent_ctx is None:
            logger.warning(
                "Batch resume skipped: missing parent_ctx snapshot batch=%s",
                batch_id,
            )
            return
        tool_name = self._resolve_resume_tool_name(meta)

        resume_status = STATUS_SUCCESS
        if final_status == STATUS_FAILED:
            resume_status = STATUS_FAILED
        elif final_status == STATUS_TIMEOUT:
            resume_status = STATUS_TIMEOUT
        elif final_status == STATUS_PARTIAL:
            resume_status = STATUS_PARTIAL

        result_obj = self._build_fork_join_result(tasks=tasks, final_status=final_status)

        parent_after = meta.get("parent_after_execution") or "suspend"
        lineage_parent_step_id = meta.get("lineage_parent_step_id") if isinstance(meta, dict) else None
        parent_agent_id = parent_ctx.agent_id
        error_payload = None
        if resume_status != STATUS_SUCCESS:
            error_payload = {
                "code": "batch_failed",
                "message": f"batch finished with status={resume_status}",
                "detail": {"batch_id": batch_id, "status": resume_status},
            }
        result_cg_ctx = parent_ctx.evolve(parent_step_id=parent_ctx.step_id)
        if lineage_parent_step_id is not None:
            result_cg_ctx = result_cg_ctx.evolve(parent_step_id=str(lineage_parent_step_id))

        cmd_data = {
            "after_execution": parent_after,
            "tool_name": tool_name,
        }
        result_context = ToolResultContext(
            ctx=result_cg_ctx,
            cmd_data=cmd_data,
        )
        payload, result_card = ToolResultBuilder(
            result_context,
            author_id="sys.pmo.batch_manager",
            function_name=tool_name,
            error_source="worker",
        ).build(
            status=resume_status,
            result=result_obj,
            error=error_payload,
            after_execution=parent_after,
        )
        await self.cardbox.save_card(result_card)

        transport_ctx = parent_ctx.evolve(
            agent_id="sys.pmo.batch_manager",
        ).with_trace_transport(
            base_headers=parent_ctx.headers,
            trace_id=parent_ctx.trace_id,
            default_depth=int(parent_ctx.recursion_depth),
        )
        join_result = await self._join_with_tx(
            phase="response",
            source_ctx=transport_ctx,
            target_ref=TurnRef(
                project_id=parent_ctx.project_id,
                agent_id=parent_ctx.agent_id,
                agent_turn_id=parent_ctx.require_agent_turn_id,
            ),
            correlation_id=batch_id,
            metadata={
                "batch_id": batch_id,
                "status": final_status,
                "task_count": len(tasks),
                "tool_name": tool_name,
            },
        )
        if join_result.status in ("rejected", "error"):
            logger.warning(
                "join ack failed (phase=response status=%s) parent=%s batch=%s",
                join_result.status,
                parent_agent_id,
                batch_id,
            )

        await self._report_with_doorbell(
            source_ctx=transport_ctx,
            target_ref=TurnRef(
                project_id=parent_ctx.project_id,
                agent_id=parent_ctx.agent_id,
                agent_turn_id=parent_ctx.require_agent_turn_id,
            ),
            message_type="tool_result",
            payload=payload,
            correlation_id=str(parent_ctx.tool_call_id or ""),
        )
        logger.info(
            "Resumed parent agent=%s batch=%s status=%s",
            parent_agent_id,
            batch_id,
            final_status,
        )
