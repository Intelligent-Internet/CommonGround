"""PMO service entrypoint (refactored).

Listens for PMO-targeted tool commands (`cmd.sys.pmo.>`), validates tool
definitions from Postgres, and dispatches to PMO internal (L1) handlers.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.cg_context import CGContext
from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config
from core.errors import BadRequestError, ProtocolViolationError, build_error_result_from_exception
from core.payload_control import sanitize_payload_control_fields
from core.status import STATUS_FAILED, STATUS_SUCCESS, TOOL_RESULT_STATUS_SET
from core.subject import parse_subject, subject_pattern
from core.config_defaults import (
    DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS,
    DEFAULT_PMO_DISPATCHED_RETRY_SECONDS,
    DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS,
    DEFAULT_PMO_ACTIVE_REAP_SECONDS,
    DEFAULT_PMO_PENDING_WAKEUP_SECONDS,
    DEFAULT_PMO_MAX_CONCURRENCY,
    DEFAULT_PMO_QUEUE_MAXSIZE,
    DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS,
    DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
)
from core.utils import safe_str, set_loop_policy
from infra.observability.otel import get_tracer, is_compact_exception, mark_span_error, traced
from infra.service_runtime import ServiceBase
from infra.messaging.ingress import build_nats_ingress_context

from .handover import HandoverPacker
from .internal_handlers.base import InternalHandlerDeps
from .internal_handlers.ask_expert import AskExpertHandler
from .internal_handlers.delegate_async import DelegateAsyncHandler
from .internal_handlers.fork_join import ForkJoinHandler
from .internal_handlers.launch_principal import LaunchPrincipalHandler
from .internal_handlers.provision_agent import ProvisionAgentHandler
from .l0_guard import L0Guard
from .l1_orchestrators import BatchManager
from infra.l0.tool_reports import publish_tool_result_report
from .protocol import PMOToolCommand, SubjectMeta, decode_tool_command, parse_subject_cmd
from .reply import build_and_save_tool_result_reply

set_loop_policy()

# ----------------------------- Logging Setup ----------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PMO")
_TRACER = get_tracer("services.pmo.service")


@dataclass(frozen=True)
class PMOWorkItem:
    kind: str
    subject: str
    data: Dict[str, Any]
    headers: Dict[str, str]
# ----------------------------- Helpers ----------------------------- #
INTERNAL_PREFIX = "internal."


def _parse_internal_function(tool_suffix: str) -> Optional[str]:
    if not tool_suffix.startswith(INTERNAL_PREFIX):
        return None
    rest = tool_suffix[len(INTERNAL_PREFIX) :]
    return rest or None


def _is_fork_join_invalid_args(exc: Exception, *, tool_name: str) -> bool:
    return (
        tool_name == "fork_join"
        and isinstance(exc, BadRequestError)
        and "invalid args" in str(exc)
    )


# ----------------------------- Service ----------------------------- #
class PMOService(ServiceBase):

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config, use_nats=True, use_cardbox=True)
        pmo_cfg = self.ctx.cfg.get("pmo", {}) or {}

        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.state_store = self.register_store(self.ctx.stores.state_store())
        self.batch_store = self.register_store(self.ctx.stores.batch_store())
        self.identity_store = self.register_store(self.ctx.stores.identity_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        requested_batch_dispatch_concurrency = int(pmo_cfg.get("batch_dispatch_concurrency", 8))
        if requested_batch_dispatch_concurrency < 1:
            requested_batch_dispatch_concurrency = 1
        if self.ctx.db_pool_max_size is None:
            batch_dispatch_concurrency = requested_batch_dispatch_concurrency
        else:
            batch_dispatch_concurrency = max(
                1,
                min(requested_batch_dispatch_concurrency, int(self.ctx.db_pool_max_size)),
            )
        self.handover = HandoverPacker(self.cardbox, self.resource_store)
        self.batch_manager = BatchManager(
            resource_store=self.resource_store,
            state_store=self.state_store,
            batch_store=self.batch_store,
            cardbox=self.cardbox,
            nats=self.nats,
            execution_store=self.execution_store,
            identity_store=self.identity_store,
            dispatch_concurrency=batch_dispatch_concurrency,
        )

        self._internal_deps = InternalHandlerDeps(
            handover=self.handover,
            batch_manager=self.batch_manager,
            state_store=self.state_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            execution_store=self.execution_store,
            identity_store=self.identity_store,
        )
        # L1 internal handlers must be explicitly listed here (no auto-discovery).
        fork_join_deadline_raw = pmo_cfg.get(
            "fork_join_default_deadline_seconds",
            DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS,
        )
        self.fork_join_default_deadline_seconds: Optional[float]
        if fork_join_deadline_raw is None:
            self.fork_join_default_deadline_seconds = None
        else:
            try:
                parsed_deadline = float(fork_join_deadline_raw)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "Invalid [pmo].fork_join_default_deadline_seconds=%r; fallback to %s",
                    fork_join_deadline_raw,
                    DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS,
                )
                parsed_deadline = float(DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS)
            if parsed_deadline <= 0:
                logger.warning(
                    "[pmo].fork_join_default_deadline_seconds=%s <= 0; default deadline disabled",
                    parsed_deadline,
                )
                self.fork_join_default_deadline_seconds = None
            else:
                self.fork_join_default_deadline_seconds = parsed_deadline

        fork_join_max_tasks_raw = pmo_cfg.get(
            "fork_join_max_tasks",
            DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
        )
        try:
            parsed_max_tasks = int(fork_join_max_tasks_raw)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Invalid [pmo].fork_join_max_tasks=%r; fallback to %s",
                fork_join_max_tasks_raw,
                DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
            )
            parsed_max_tasks = int(DEFAULT_PMO_FORK_JOIN_MAX_TASKS)
        if parsed_max_tasks <= 0:
            logger.warning(
                "[pmo].fork_join_max_tasks=%s <= 0; fallback to %s",
                parsed_max_tasks,
                DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
            )
            parsed_max_tasks = int(DEFAULT_PMO_FORK_JOIN_MAX_TASKS)
        self.fork_join_max_tasks = parsed_max_tasks

        self._internal_handlers = {
            DelegateAsyncHandler.name: DelegateAsyncHandler(),
            LaunchPrincipalHandler.name: LaunchPrincipalHandler(),
            AskExpertHandler.name: AskExpertHandler(),
            ForkJoinHandler.name: ForkJoinHandler(
                default_deadline_seconds=self.fork_join_default_deadline_seconds,
                max_tasks=self.fork_join_max_tasks,
            ),
            ProvisionAgentHandler.name: ProvisionAgentHandler(),
        }

        self.watchdog_interval_seconds = float(
            pmo_cfg.get("watchdog_interval_seconds", DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS)
        )
        self.dispatched_retry_seconds = float(
            pmo_cfg.get("dispatched_retry_seconds", DEFAULT_PMO_DISPATCHED_RETRY_SECONDS)
        )
        self.dispatched_timeout_seconds = float(
            pmo_cfg.get("dispatched_timeout_seconds", DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS)
        )
        self.active_reap_seconds = float(
            pmo_cfg.get("active_reap_seconds", DEFAULT_PMO_ACTIVE_REAP_SECONDS)
        )
        self.pending_wakeup_seconds = float(
            pmo_cfg.get("pending_wakeup_seconds", DEFAULT_PMO_PENDING_WAKEUP_SECONDS)
        )

        self.queue_maxsize = int(pmo_cfg.get("queue_maxsize", DEFAULT_PMO_QUEUE_MAXSIZE))
        if self.queue_maxsize < 0:
            self.queue_maxsize = 0
        self.worker_concurrency = max(
            1, int(pmo_cfg.get("max_concurrency", DEFAULT_PMO_MAX_CONCURRENCY))
        )
        self.queue: asyncio.Queue[PMOWorkItem] = asyncio.Queue(maxsize=self.queue_maxsize)
        self._worker_tasks: set[asyncio.Task] = set()
        self._watchdog_task: Optional[asyncio.Task] = None

        self.l0_guard = L0Guard(
            state_store=self.state_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            execution_store=self.execution_store,
            dispatched_retry_seconds=self.dispatched_retry_seconds,
            dispatched_timeout_seconds=self.dispatched_timeout_seconds,
            active_reap_seconds=self.active_reap_seconds,
            pending_wakeup_seconds=self.pending_wakeup_seconds,
        )
    async def start(self) -> None:
        await self.open()

        # Listen to COMMANDS: cg.v1r4.*.*.cmd.sys.pmo.>
        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="sys",
            target="pmo",
            suffix=">",
            protocol_version=PROTOCOL_VERSION,
        )
        queue = "pmo_service"
        logger.info(f"🚀 Started. Listening on {subject} (queue={queue})")
        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._enqueue_tool_command,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        # Listen to child task results for BatchManager aggregation.
        subject_task_evt = subject_pattern(
            project_id="*",
            channel_id="*",
            category="evt",
            component="agent",
            target="*",
            suffix="task",
            protocol_version=PROTOCOL_VERSION,
        )
        await self.nats.subscribe_cmd(
            subject_task_evt,
            "pmo_batch_evt",
            self._enqueue_task_event,
            durable_name=f"pmo_batch_evt_task_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        self._start_workers()
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

        # Keep alive
        while True:
            await asyncio.sleep(1)

    async def close(self) -> None:
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None

        if self._worker_tasks:
            tasks = list(self._worker_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._worker_tasks.clear()

        await super().close()

    def _start_workers(self) -> None:
        logger.info(
            "PMO worker pool started (concurrency=%s, queue_maxsize=%s)",
            self.worker_concurrency,
            self.queue.maxsize,
        )
        for idx in range(self.worker_concurrency):
            task = asyncio.create_task(self._worker(idx + 1), name=f"pmo_worker:{idx + 1}")
            self._worker_tasks.add(task)
            task.add_done_callback(self._worker_tasks.discard)

    async def _worker(self, worker_id: int) -> None:
        logger.info("PMO worker %s ready", worker_id)
        while True:
            item = await self.queue.get()
            try:
                await self._handle_work_item(item=item, headers=item.headers, worker_id=worker_id)
            except Exception as exc:  # noqa: BLE001
                if is_compact_exception(exc):
                    logger.error("PMO worker %s error: %s", worker_id, exc)
                else:
                    logger.error("PMO worker %s error: %s", worker_id, exc, exc_info=True)
            finally:
                self.queue.task_done()

    @traced(
        _TRACER,
        "pmo.handle_item",
        headers_arg="headers",
        include_links=True,
        record_exception=True,
        set_status_on_exception=True,
        span_arg="_span",
    )
    async def _handle_work_item(
        self,
        *,
        item: PMOWorkItem,
        headers: Dict[str, str],
        worker_id: int,
        _span: Any = None,
    ) -> None:
        if item.kind == "tool":
            await self._handle_tool_command(item.subject, item.data, headers)
            return
        if item.kind == "task_evt":
            await self._handle_agent_task_event(item.subject, item.data, headers)
            return
        logger.warning("PMO worker %s received unknown item kind=%s", worker_id, item.kind)
        if _span is not None:
            _span.set_attribute("cg.unknown_kind", True)

    async def _enqueue_tool_command(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]) -> None:
        await self.queue.put(PMOWorkItem(kind="tool", subject=subject, data=data or {}, headers=headers or {}))

    async def _enqueue_task_event(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]) -> None:
        await self.queue.put(PMOWorkItem(kind="task_evt", subject=subject, data=data or {}, headers=headers or {}))

    async def _watchdog_loop(self) -> None:
        logger.info(
            "PMO watchdog started interval=%ss retry_dispatched=%ss timeout_dispatched=%ss pending_wakeup=%ss reap_active=%ss",
            self.watchdog_interval_seconds,
            self.dispatched_retry_seconds,
            self.dispatched_timeout_seconds,
            self.pending_wakeup_seconds,
            self.active_reap_seconds,
        )
        while True:
            try:
                await self.l0_guard.retry_stuck_dispatched()
                await self.l0_guard.reap_stuck_dispatched_timeout()
                await self.l0_guard.poke_stale_inbox()
                await self.l0_guard.reap_stuck_active()
                await self.batch_manager.reconcile_dispatched_tasks()
                await self.batch_manager.dispatch_pending_tasks()
                await self.batch_manager.reap_timed_out_batches()
            except Exception as exc:  # noqa: BLE001
                logger.error("Watchdog tick failed: %s", exc, exc_info=True)
            await asyncio.sleep(self.watchdog_interval_seconds)

    @traced(
        _TRACER,
        "pmo.handle_tool_command",
        span_arg="_span",
    )
    async def _handle_tool_command(
        self,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ) -> None:
        span = _span
        meta = parse_subject_cmd(subject)
        if not meta:
            logger.warning("⚠️ Unrecognized subject: %s", subject)
            return

        ingress_ctx = None
        try:
            ingress_ctx, _ = build_nats_ingress_context(
                headers=headers,
                raw_payload=data if isinstance(data, dict) else {},
                subject_parts=meta,
            )
        except ProtocolViolationError as exc:
            logger.error("❌ Invalid ingress context: %s", exc)
            fallback_payload = sanitize_payload_control_fields(data if isinstance(data, dict) else {})
            try:
                ingress_ctx, _ = build_nats_ingress_context(
                    headers=headers,
                    raw_payload=fallback_payload,
                    subject_parts=meta,
                )
            except ProtocolViolationError as fallback_exc:
                self._log_unreplyable_ingress_failure(
                    subject=subject,
                    data=data,
                    headers=headers,
                    exc=exc,
                    fallback_exc=fallback_exc,
                )
                return
            await self._reply_failed_best_effort(
                meta=meta,
                data=data,
                ingress_ctx=ingress_ctx,
                exc=exc,
            )
            return
        try:
            cmd = await decode_tool_command(
                data=dict(data or {}),
                ctx=ingress_ctx,
                cardbox=self.cardbox,
            )
        except Exception as exc:
            logger.error("❌ Failed to decode payload: %s", exc)
            await self._reply_failed_best_effort(
                meta=meta,
                data=data,
                ingress_ctx=ingress_ctx,
                exc=exc,
            )
            return

        ctx = ingress_ctx.with_tool_call(cmd.tool_call_id)

        db_after, handler = await self._resolve_internal_handler(
            meta=meta,
            cmd=cmd,
            ctx=ctx,
        )
        if not handler:
            return

        try:
            defer_resume, result_data = await self._invoke_internal_handler(
                handler=handler,
                deps=self._internal_deps,
                ctx=ctx,
                cmd=cmd,
                parent_after_execution=db_after or cmd.after_execution,
            )
        except Exception as exc:  # noqa: BLE001
            compact_error = is_compact_exception(exc)
            mark_span_error(span, exc, include_exception=not compact_error)
            if _is_fork_join_invalid_args(exc, tool_name=str(cmd.tool_name)):
                logger.warning(
                    "PMO: fork_join invalid args: %s; replied failed tool.result.",
                    str(exc),
                )
            elif isinstance(exc, BadRequestError):
                logger.warning(
                    "PMO: tool=%s bad request: %s; replied failed tool.result.",
                    str(cmd.tool_name),
                    str(exc),
                )
            elif compact_error:
                logger.error("❌ PMO handler failed: %s", exc)
            else:
                logger.error("❌ PMO handler failed: %s", exc, exc_info=True)
            await self._reply_failed_from_exception(
                meta,
                cmd,
                ctx=ctx,
                after_execution=db_after,
                exc=exc,
            )
            return

        if defer_resume:
            logger.info("PMO internal batch accepted: parent suspended until batch completes.")
            return

        await self._reply_resume(
            meta,
            cmd,
            ctx=ctx,
            status=STATUS_SUCCESS,
            result=result_data or {"status": "processed"},
            after_execution=db_after,
        )

    @traced(
        _TRACER,
        "pmo.internal_handler",
        mark_error_on_exception=True,
    )
    async def _invoke_internal_handler(
        self,
        *,
        handler: Any,
        deps: InternalHandlerDeps,
        ctx: CGContext,
        cmd: PMOToolCommand,
        parent_after_execution: str,
    ) -> tuple[bool, Dict[str, Any]]:
        defer_resume, result_data = await handler.handle(
            deps=deps,
            ctx=ctx,
            cmd=cmd,
            parent_after_execution=parent_after_execution,
        )
        return defer_resume, result_data or {}

    def _get_internal_handler(self, func_name: str):
        return self._internal_handlers.get(func_name)

    async def _resolve_internal_handler(
        self,
        *,
        meta: SubjectMeta,
        cmd: PMOToolCommand,
        ctx: CGContext,
    ) -> tuple[Optional[str], Any]:
        internal_func = _parse_internal_function(meta.tool_suffix)
        tool_def = await self.resource_store.fetch_tool_definition_model(meta.project_id, cmd.tool_name)
        if not tool_def:
            logger.warning(f"⚠️ Missing tool definition for {cmd.tool_name}")
            await self._reply_validation_failure(
                meta=meta,
                cmd=cmd,
                ctx=ctx,
                error_code="tool_definition_missing",
                error_message=f"Tool definition missing: {cmd.tool_name}",
                after_execution=None,
            )
            return None, None

        db_after = tool_def.after_execution
        if db_after and cmd.after_execution != db_after:
            await self._reply_validation_failure(
                meta=meta,
                cmd=cmd,
                ctx=ctx,
                error_code="after_execution_mismatch",
                error_message=f"expected {db_after}, got {cmd.after_execution}",
                after_execution=db_after,
            )
            return db_after, None

        if not internal_func:
            await self._reply_validation_failure(
                meta=meta,
                cmd=cmd,
                ctx=ctx,
                error_code="unsupported_pmo_route",
                error_message=(
                    "PMO only supports internal routes: expected suffix "
                    f"'internal.<function>', got '{meta.tool_suffix}'"
                ),
                after_execution=db_after,
            )
            return db_after, None

        handler = self._get_internal_handler(internal_func)
        if not handler:
            await self._reply_validation_failure(
                meta=meta,
                cmd=cmd,
                ctx=ctx,
                error_code="not_implemented",
                error_message=f"PMO internal function not implemented: {internal_func}",
                after_execution=db_after,
            )
            return db_after, None
        return db_after, handler

    async def _reply_validation_failure(
        self,
        *,
        meta: SubjectMeta,
        cmd: PMOToolCommand,
        ctx: CGContext,
        error_code: str,
        error_message: str,
        after_execution: Optional[str],
    ) -> None:
        await self._reply_resume(
            meta,
            cmd,
            ctx=ctx,
            status=STATUS_FAILED,
            result={"error_code": error_code, "error_message": error_message},
            after_execution=after_execution,
        )

    @traced(
        _TRACER,
        "pmo.handle_task_event",
        span_arg="_span",
    )
    async def _handle_agent_task_event(
        self,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ) -> None:
        parts = parse_subject(subject)
        if not parts:
            return
        try:
            ingress_ctx, safe_payload = build_nats_ingress_context(
                headers=headers,
                raw_payload=data if isinstance(data, dict) else {},
                subject_parts=parts,
            )
        except ProtocolViolationError as exc:
            logger.warning("Invalid agent task event context: %s", exc)
            return
        agent_turn_id = ingress_ctx.agent_turn_id
        status = safe_payload.get("status")
        if not agent_turn_id or status not in TOOL_RESULT_STATUS_SET:
            return
        await self.batch_manager.handle_child_task_event(
            project_id=parts.project_id,
            agent_turn_id=str(agent_turn_id),
            status=str(status),
            output_box_id=safe_str(safe_payload.get("output_box_id")),
            deliverable_card_id=safe_str(safe_payload.get("deliverable_card_id")),
            tool_result_card_id=safe_str(safe_payload.get("tool_result_card_id")),
            error=safe_str(safe_payload.get("error")),
        )

    async def _publish_tool_result_payload(
        self,
        *,
        meta: SubjectMeta,
        ctx: CGContext,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        source_ctx = ctx.evolve(agent_id="sys.pmo", agent_turn_id="", step_id=None)
        await publish_tool_result_report(
            nats=self.nats,
            execution_store=self.execution_store,
            cardbox=self.cardbox,
            resource_store=self.resource_store,
            state_store=self.state_store,
            source_ctx=source_ctx,
            target_ctx=ctx,
            payload=payload,
            correlation_id=correlation_id,
        )

    async def _reply_failed_from_exception(
        self,
        meta: SubjectMeta,
        cmd: PMOToolCommand,
        *,
        ctx: CGContext,
        after_execution: Optional[str],
        exc: Exception,
    ) -> None:
        result, error = build_error_result_from_exception(exc, source="worker")
        await self._reply_resume(
            meta,
            cmd,
            ctx=ctx,
            status=STATUS_FAILED,
            result=result,
            error=error,
            after_execution=after_execution,
        )

    async def _reply_failed_best_effort(
        self,
        *,
        meta: SubjectMeta,
        data: Dict[str, Any],
        ingress_ctx: CGContext,
        exc: Exception,
    ) -> None:
        try:
            tool_call_id = ingress_ctx.require_tool_call_id
        except ProtocolViolationError:
            return
        agent_turn_id = safe_str(ingress_ctx.agent_turn_id)
        if not tool_call_id or not agent_turn_id:
            return
        after_execution = safe_str((data or {}).get("after_execution")) or "suspend"
        if after_execution not in ("suspend", "terminate"):
            after_execution = "suspend"
        tool_name = safe_str((data or {}).get("tool_name")) or "unknown"
        tool_call_card_id = safe_str((data or {}).get("tool_call_card_id"))
        result, error = build_error_result_from_exception(exc, source="worker")
        cmd_data = {
            "after_execution": after_execution,
            "tool_name": tool_name,
        }
        reply_ctx = ingress_ctx.with_tool_call(tool_call_id)
        payload, _ = await build_and_save_tool_result_reply(
            cardbox=self.cardbox,
            ctx=reply_ctx,
            tool_call_card_id=tool_call_card_id,
            cmd_data=cmd_data,
            status=STATUS_FAILED,
            result=result,
            error=error,
            function_name=tool_name,
            after_execution=after_execution,
            warn_context="bad_request",
        )
        await self._publish_tool_result_payload(
            meta=meta,
            ctx=reply_ctx,
            payload=payload,
            correlation_id=tool_call_id,
        )

    def _log_unreplyable_ingress_failure(
        self,
        *,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        exc: Exception,
        fallback_exc: Exception,
    ) -> None:
        logger.error(
            "❌ PMO ingress failure is not replyable subject=%s primary=%s fallback=%s payload_keys=%s header_keys=%s",
            subject,
            exc,
            fallback_exc,
            sorted(str(k) for k in (data or {}).keys()) if isinstance(data, dict) else [],
            sorted(str(k) for k in (headers or {}).keys()),
        )


    async def _reply_resume(
        self,
        meta: SubjectMeta,
        cmd: PMOToolCommand,
        ctx: CGContext,
        status: str,
        result: Any,
        after_execution: Optional[str] = None,
        error: Optional[Any] = None,
    ) -> None:
        """Send UTP Resume command back to worker."""
        after_exec = after_execution or cmd.after_execution

        if error is None and status != STATUS_SUCCESS:
            error = result

        cmd_data = {
            "after_execution": after_exec,
            "tool_name": cmd.tool_name,
        }
        payload, _ = await build_and_save_tool_result_reply(
            cardbox=self.cardbox,
            ctx=ctx,
            tool_call_card_id=cmd.tool_call_card_id,
            cmd_data=cmd_data,
            status=status,
            result=result,
            error=error,
            function_name=cmd.tool_name,
            after_execution=after_exec,
            warn_context="resume",
        )
        await self._publish_tool_result_payload(
            meta=meta,
            ctx=ctx,
            payload=payload,
            correlation_id=cmd.tool_call_id,
        )
        logger.info(f"↩️  Replied RESUME to {ctx.agent_id} (status={status})")



async def main() -> None:
    cfg = load_app_config()
    service = PMOService(cfg)
    try:
        await service.start()
    finally:
        await service.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
