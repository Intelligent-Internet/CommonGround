"""Agent Worker loop entrypoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import asyncio
import json
import logging
from typing import Any, Awaitable, Dict, Iterable, Optional


from core.config import PROTOCOL_VERSION, MAX_STEPS, MAX_RECURSION_DEPTH, DEFAULT_TIMEOUT
from core.app_config import load_app_config
from core.config_defaults import (
    DEFAULT_WORKER_MAX_CONCURRENCY,
    DEFAULT_WORKER_QUEUE_MAXSIZE_FACTOR,
    DEFAULT_WORKER_INBOX_FETCH_LIMIT,
    DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS,
    DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS,
    DEFAULT_WORKER_MAX_RESUME_DEFER_RETRIES,
    DEFAULT_WORKER_TARGETS,
)
from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.headers import CG_AGENT_ID
from core.status import STATUS_TIMEOUT
from core.subject import parse_subject, subject_pattern
from core.utils import safe_str, set_loop_policy, safe_target_label
from core.utp_protocol import AgentTaskPayload
from infra.stores.context_hydration import (
    build_replay_context,
    build_state_head_context,
)
from infra.l0_engine import L0Engine, ReportIntent, TurnRef
from infra.llm_gateway import LLMService, LLMWrapper
from infra.mock_llm_handler import mock_chat
from infra.observability.otel import get_tracer, traced
from infra.service_runtime import ServiceBase
from infra.worker_helpers import InboxRowContext, InboxRowResult, consume_inbox_rows
from infra.tool_executor import ToolResultBuilder, ToolResultContext

from .context_loader import ContextAssembler, ResourceLoader
from .models import AgentTurnWorkItem
from .react_step import ReactStepProcessor
from .resume_handler import ResumeHandler
from .stop_handler import StopHandler
from .utp_dispatcher import UTPDispatcher
from .timing import duration_ms, to_iso, utc_now

set_loop_policy()

# ----------------------------- Logging Setup ----------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AgentWorker")
_TRACER = get_tracer("services.agent_worker.loop")

def _build_inbox_timing(
    *,
    enqueued_at: Optional[datetime],
    inbox_created_at: Optional[datetime],
    inbox_processed_at: Optional[datetime],
) -> Dict[str, Any]:
    timing: Dict[str, Any] = {}
    queue_enqueued_at = to_iso(enqueued_at)
    if queue_enqueued_at:
        timing["queue_enqueued_at"] = queue_enqueued_at
    created_at = to_iso(inbox_created_at)
    if created_at:
        timing["inbox_created_at"] = created_at
    processed_at = to_iso(inbox_processed_at)
    if processed_at:
        timing["inbox_processed_at"] = processed_at
    return timing



def _normalize_worker_targets(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [part for part in (seg.strip() for seg in raw.split(",")) if part]
    if isinstance(raw, dict):
        return []
    if isinstance(raw, Iterable):
        targets: list[str] = []
        for item in raw:
            text = str(item).strip()
            if text:
                targets.append(text)
        return targets
    return []


def _coerce_non_negative_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except Exception:
        return int(default)
    return parsed if parsed >= 0 else int(default)


@dataclass(frozen=True)
class AgentWorkerSettings:
    max_steps: int
    max_recursion_depth: int
    max_concurrency: int
    wakeup_concurrency: int
    queue_maxsize: int
    wakeup_queue_maxsize: int
    inbox_fetch_limit: int
    suspend_timeout_seconds: float
    watchdog_interval_seconds: float
    inbox_processing_timeout_seconds: float
    max_resume_defer_retries: int
    worker_targets: list[str]

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "AgentWorkerSettings":
        worker_cfg = cfg.get("worker", {}) if isinstance(cfg, dict) else {}
        max_steps = int(worker_cfg.get("max_steps", MAX_STEPS))
        max_recursion_depth = int(worker_cfg.get("max_recursion_depth", MAX_RECURSION_DEPTH))
        max_concurrency = max(1, int(worker_cfg.get("max_concurrency", DEFAULT_WORKER_MAX_CONCURRENCY)))
        wakeup_concurrency = max(1, int(worker_cfg.get("wakeup_concurrency", max_concurrency)))
        queue_maxsize = int(
            worker_cfg.get(
                "queue_maxsize",
                max_concurrency * DEFAULT_WORKER_QUEUE_MAXSIZE_FACTOR,
            )
        )
        if queue_maxsize < 0:
            queue_maxsize = 0
        wakeup_queue_maxsize = int(worker_cfg.get("wakeup_queue_maxsize", 0))
        if wakeup_queue_maxsize < 0:
            wakeup_queue_maxsize = 0
        inbox_fetch_limit = int(
            worker_cfg.get("inbox_fetch_limit", DEFAULT_WORKER_INBOX_FETCH_LIMIT)
        )
        suspend_timeout_seconds = float(worker_cfg.get("suspend_timeout_seconds", DEFAULT_TIMEOUT))
        watchdog_interval_seconds = float(
            worker_cfg.get(
                "watchdog_interval_seconds",
                DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS,
            )
        )
        inbox_processing_timeout_seconds = float(
            worker_cfg.get(
                "inbox_processing_timeout_seconds",
                DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS,
            )
        )
        max_resume_defer_retries = max(
            0,
            int(
                worker_cfg.get(
                    "max_resume_defer_retries",
                    DEFAULT_WORKER_MAX_RESUME_DEFER_RETRIES,
                )
            ),
        )
        raw_targets = worker_cfg.get("worker_targets")
        worker_targets = _normalize_worker_targets(raw_targets)
        if not worker_targets:
            worker_targets = list(DEFAULT_WORKER_TARGETS)

        return cls(
            max_steps=max_steps,
            max_recursion_depth=max_recursion_depth,
            max_concurrency=max_concurrency,
            wakeup_concurrency=wakeup_concurrency,
            queue_maxsize=queue_maxsize,
            wakeup_queue_maxsize=wakeup_queue_maxsize,
            inbox_fetch_limit=inbox_fetch_limit,
            suspend_timeout_seconds=suspend_timeout_seconds,
            watchdog_interval_seconds=watchdog_interval_seconds,
            inbox_processing_timeout_seconds=inbox_processing_timeout_seconds,
            max_resume_defer_retries=max_resume_defer_retries,
            worker_targets=list(dict.fromkeys(worker_targets)),
        )


@dataclass(frozen=True)
class ObservabilitySettings:
    timing_enabled: bool
    timing_step_event: bool
    timing_task_event: bool
    timing_stream_metadata: bool
    timing_tool_dispatch: bool
    timing_worker_logs: bool
    timing_capture: bool

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "ObservabilitySettings":
        obs_cfg = cfg.get("observability", {}) if isinstance(cfg, dict) else {}
        timing_cfg = obs_cfg.get("timing", {}) if isinstance(obs_cfg, dict) else {}
        timing_enabled = bool(timing_cfg.get("enabled", False))

        def _flag(key: str, default: bool) -> bool:
            if not isinstance(timing_cfg, dict):
                return default
            value = timing_cfg.get(key)
            if value is None:
                return default
            return bool(value)

        timing_step_event = _flag("step_event", timing_enabled)
        timing_task_event = _flag("task_event", timing_enabled)
        timing_stream_metadata = _flag("stream_metadata", timing_enabled)
        timing_tool_dispatch = _flag("tool_dispatch", timing_enabled)
        timing_worker_logs = _flag("worker_logs", timing_enabled)
        timing_capture = timing_enabled or any(
            [
                timing_step_event,
                timing_task_event,
                timing_stream_metadata,
                timing_tool_dispatch,
                timing_worker_logs,
            ]
        )

        return cls(
            timing_enabled=timing_enabled,
            timing_step_event=timing_step_event,
            timing_task_event=timing_task_event,
            timing_stream_metadata=timing_stream_metadata,
            timing_tool_dispatch=timing_tool_dispatch,
            timing_worker_logs=timing_worker_logs,
            timing_capture=timing_capture,
        )

@dataclass(frozen=True, slots=True)
class WakeupKey:
    project_id: str
    agent_id: str


@dataclass(slots=True)
class WakeupSlot:
    """Per-agent wakeup coalescing state.

    Wakeup is a doorbell only; we coalesce repeated doorbells for the same
    (project_id, agent_id) into at most one queued scan plus an optional
    follow-up pass ("dirty").
    """

    subject: str
    dirty: bool = False


class AgentWorker(ServiceBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        settings = AgentWorkerSettings.from_config(self.ctx.cfg)
        observability = ObservabilitySettings.from_config(self.ctx.cfg)
        self.settings = settings
        self.inbox_fetch_limit = settings.inbox_fetch_limit
        self.watchdog_interval_seconds = settings.watchdog_interval_seconds
        self.inbox_processing_timeout_seconds = settings.inbox_processing_timeout_seconds
        self.max_resume_defer_retries = settings.max_resume_defer_retries
        self.worker_targets = settings.worker_targets
        self.observability = observability

        self.state_store = self.register_store(self.ctx.stores.state_store())
        self.step_store = self.register_store(self.ctx.stores.step_store())
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        self.skill_store = self.register_store(self.ctx.stores.skill_store())
        self.l0 = L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
            cardbox=self.cardbox,
        )
        self.resource_loader = ResourceLoader(self.cardbox, self.resource_store)
        self.assembler = ContextAssembler()
        self.llm_wrapper = LLMWrapper(LLMService(), mock_chat=mock_chat)
        self.dispatcher = UTPDispatcher(
            self.cardbox,
            self.nats,
            self.resource_store,
            execution_store=self.execution_store,
            l0_engine=self.l0,
            timing_tool_dispatch=observability.timing_tool_dispatch,
        )
        self.queue: asyncio.Queue[AgentTurnWorkItem] = asyncio.Queue(maxsize=settings.queue_maxsize)
        self.worker_concurrency = settings.max_concurrency
        self.wakeup_concurrency = settings.wakeup_concurrency
        self._wakeup_queue: asyncio.Queue[WakeupKey] = asyncio.Queue(
            maxsize=settings.wakeup_queue_maxsize
        )
        self._wakeup_tasks: set[asyncio.Task] = set()
        self._wakeup_slots: dict[WakeupKey, WakeupSlot] = {}

        self.react_processor = ReactStepProcessor(
            state_store=self.state_store,
            step_store=self.step_store,
            resource_store=self.resource_store,
            resource_loader=self.resource_loader,
            skill_store=self.skill_store,
            cardbox=self.cardbox,
            assembler=self.assembler,
            llm_wrapper=self.llm_wrapper,
            action_handler=self.dispatcher,
            nats=self.nats,
            queue=self.queue,
            execution_store=self.execution_store,
            max_steps=settings.max_steps,
            max_recursion_depth=settings.max_recursion_depth,
            suspend_timeout_seconds=settings.suspend_timeout_seconds,
            timing_capture=observability.timing_capture,
            timing_step_event=observability.timing_step_event,
            timing_task_event=observability.timing_task_event,
            timing_stream_metadata=observability.timing_stream_metadata,
        )
        self.resume_handler = ResumeHandler(
            state_store=self.state_store,
            step_store=self.step_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            queue=self.queue,
            suspend_timeout_seconds=settings.suspend_timeout_seconds,
            timing_capture=observability.timing_capture,
            timing_step_event=observability.timing_step_event,
            judge_cfg=self.ctx.cfg.get("judge", {}) if isinstance(self.ctx.cfg, dict) else {},
            timing_task_event=observability.timing_task_event,
        )
        self.stop_handler = StopHandler(
            state_store=self.state_store,
            step_store=self.step_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            timing_step_event=observability.timing_step_event,
            timing_task_event=observability.timing_task_event,
        )

    def _build_item_timing(
        self,
        *,
        enqueued_at: datetime | None,
        inbox_created_at: datetime | None,
        inbox_processed_at: datetime | None,
    ) -> Dict[str, Any]:
        if not self.observability.timing_capture:
            return {}
        return _build_inbox_timing(
            enqueued_at=enqueued_at,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
        )

    def _new_enqueued_at(self) -> datetime | None:
        return utc_now() if self.observability.timing_capture else None

    async def _queue_work_item(self, item: AgentTurnWorkItem) -> None:
        await self.queue.put(item)

    async def start(self):
        await self._open_resources()
        await self._subscribe_wakeup()
        self._start_wakeup_workers()
        self._start_watchdog()
        await self._enqueue_suspended_reconcile_resumes(limit=200, include_received=True)
        await self.worker_loop()

    async def _open_resources(self) -> None:
        await self.open()

    async def _subscribe_wakeup(self) -> None:
        for target in self.worker_targets:
            subject_wakeup = subject_pattern(
                project_id="*",
                channel_id="*",
                category="cmd",
                component="agent",
                target=target,
                suffix="wakeup",
                protocol_version=PROTOCOL_VERSION,
            )
            safe_target = safe_target_label(target)
            queue_wakeup = f"agent_worker_wakeup_{safe_target}"
            durable = f"agent_worker_{safe_target}_wakeup_{PROTOCOL_VERSION}"
            logger.info("Listening on %s (queue=%s)", subject_wakeup, queue_wakeup)
            await self.nats.subscribe_cmd(
                subject_wakeup,
                queue_wakeup,
                self._enqueue_wakeup,
                durable_name=durable,
                deliver_policy="all",
            )

    def _start_wakeup_workers(self) -> None:
        logger.info(
            "Wakeup worker pool started (concurrency=%s, queue_maxsize=%s)",
            self.wakeup_concurrency,
            self._wakeup_queue.maxsize,
        )
        for idx in range(self.wakeup_concurrency):
            self._spawn_background_task(
                self._wakeup_worker(idx + 1),
                name=f"wakeup_worker:{idx + 1}",
            )

    async def _wakeup_worker(self, worker_id: int) -> None:
        logger.info("Wakeup worker %s ready", worker_id)
        while True:
            key = await self._wakeup_queue.get()
            try:
                slot = self._wakeup_slots.get(key)
                if slot is None:
                    logger.debug(
                        "Wakeup missing slot proj=%s agent=%s",
                        key.project_id,
                        key.agent_id,
                    )
                    continue
                await self._process_wakeup(subject=slot.subject, key=key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Wakeup worker failed worker=%s proj=%s agent=%s: %s",
                    worker_id,
                    key.project_id,
                    key.agent_id,
                    exc,
                    exc_info=True,
                )
            finally:
                self._wakeup_queue.task_done()

            # Follow-up pass if any wakeup arrived while this key was scheduled/inflight.
            slot = self._wakeup_slots.get(key)
            if slot is not None and slot.dirty:
                slot.dirty = False
                try:
                    self._wakeup_queue.put_nowait(key)
                    continue
                except asyncio.QueueFull:
                    logger.warning(
                        "Wakeup queue full; rescan dropped proj=%s agent=%s size=%s",
                        key.project_id,
                        key.agent_id,
                        self._wakeup_queue.qsize(),
                    )
                    self._wakeup_slots.pop(key, None)
                    continue

            # No more work scheduled for this key.
            self._wakeup_slots.pop(key, None)

    def _start_watchdog(self) -> None:
        self._spawn_background_task(self._watchdog_loop(), name="worker_watchdog")

    async def close(self) -> None:
        wakeup_tasks = list(self._wakeup_tasks)
        for task in wakeup_tasks:
            task.cancel()
        if wakeup_tasks:
            await asyncio.gather(*wakeup_tasks, return_exceptions=True)
        await super().close()

    def _spawn_background_task(self, coro: Awaitable[Any], *, name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to spawn background task %s: %s", name, exc)
            return
        self._wakeup_tasks.add(task)
        task.add_done_callback(self._wakeup_tasks.discard)

    async def _enqueue_cmd(
        self,
        data: dict,
        *,
        ingress_ctx: CGContext,
        inbox_id: str | None,
        inbox_retry_count: int = 0,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        raw_data = data if isinstance(data, dict) else {}
        raw_metadata = raw_data.get("metadata")
        raw_runtime_config = raw_data.get("runtime_config")
        task_payload_data = {
            "profile_box_id": raw_data.get("profile_box_id"),
            "context_box_id": raw_data.get("context_box_id"),
            "output_box_id": raw_data.get("output_box_id"),
            "display_name": raw_data.get("display_name"),
            "metadata": raw_metadata if isinstance(raw_metadata, dict) else {},
            "runtime_config": raw_runtime_config if isinstance(raw_runtime_config, dict) else {},
        }
        try:
            payload = AgentTaskPayload(**task_payload_data)
        except Exception as exc:  # noqa: BLE001
            logger.error("Payload decode failed: %s", exc)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            ctx=ingress_ctx,
            profile_box_id=safe_str(payload, "profile_box_id"),
            context_box_id=safe_str(payload, "context_box_id"),
            output_box_id=safe_str(payload, "output_box_id"),
            resume_data=None,
            stop_data=None,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
            inbox_retry_count=_coerce_non_negative_int(inbox_retry_count),
            enqueued_at=enqueued_at,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
            timing=self._build_item_timing(
                enqueued_at=enqueued_at,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            ),
        )
        logger.info(
            "Enqueued turn %s for %s (proj=%s, ch=%s)",
            ingress_ctx.agent_turn_id,
            ingress_ctx.agent_id,
            ingress_ctx.project_id,
            ingress_ctx.channel_id,
        )
        await self._queue_work_item(item)
        return True

    async def _enqueue_resume(
        self,
        data: dict,
        *,
        ingress_ctx: CGContext,
        inbox_id: str | None,
        inbox_retry_count: int = 0,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        try:
            _ = ingress_ctx.require_agent_turn_id
        except ProtocolViolationError:
            logger.warning("Resume missing context for inbox payload: %s", data)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            ctx=ingress_ctx,
            profile_box_id=None,
            context_box_id=None,
            output_box_id=None,
            resume_data=data,
            stop_data=None,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
            inbox_retry_count=_coerce_non_negative_int(inbox_retry_count),
            enqueued_at=enqueued_at,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
            timing=self._build_item_timing(
                enqueued_at=enqueued_at,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            ),
        )
        logger.info(
            "Enqueued RESUME for %s turn=%s epoch=%s",
            ingress_ctx.agent_id,
            ingress_ctx.agent_turn_id,
            ingress_ctx.turn_epoch,
        )
        await self._queue_work_item(item)
        return True

    async def _enqueue_stop(
        self,
        data: dict,
        *,
        ingress_ctx: CGContext,
        inbox_id: str | None,
        inbox_retry_count: int = 0,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        try:
            _ = ingress_ctx.require_agent_turn_id
        except ProtocolViolationError:
            logger.warning("Stop missing context for inbox payload: %s", data)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            ctx=ingress_ctx,
            profile_box_id=None,
            context_box_id=None,
            output_box_id=None,
            resume_data=None,
            stop_data=data,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
            inbox_retry_count=_coerce_non_negative_int(inbox_retry_count),
            enqueued_at=enqueued_at,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
            timing=self._build_item_timing(
                enqueued_at=enqueued_at,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            ),
        )
        logger.info(
            "Enqueued STOP for %s turn=%s epoch=%s",
            ingress_ctx.agent_id,
            ingress_ctx.agent_turn_id,
            ingress_ctx.turn_epoch,
        )
        await self._queue_work_item(item)
        return True

    async def _enqueue_wakeup(self, subject: str, data: dict, headers: dict):
        parts = parse_subject(subject)
        if not parts:
            logger.warning("Invalid wakeup subject: %s", subject)
            return
        agent_id = safe_str((headers or {}).get(CG_AGENT_ID))
        if not agent_id:
            logger.warning("Wakeup missing %s header: %s", CG_AGENT_ID, headers)
            return

        key = WakeupKey(project_id=str(parts.project_id), agent_id=str(agent_id))
        # Keep the latest subject so we preserve channel_id semantics for stop/resume work items.
        slot = self._wakeup_slots.get(key)
        if slot is not None:
            slot.subject = subject
            slot.dirty = True
            return
        self._wakeup_slots[key] = WakeupSlot(subject=subject)
        try:
            self._wakeup_queue.put_nowait(key)
        except asyncio.QueueFull:
            self._wakeup_slots.pop(key, None)
            logger.warning(
                "Wakeup queue full proj=%s agent=%s size=%s",
                key.project_id,
                key.agent_id,
                self._wakeup_queue.qsize(),
            )

    async def _process_wakeup(self, *, subject: str, key: WakeupKey) -> None:
        parts = parse_subject(subject)
        if not parts:
            logger.warning("Invalid wakeup subject: %s", subject)
            return
        agent_id = key.agent_id

        # Greedy drain: a single wakeup can correspond to multiple pending inbox rows (or backlog
        # replay from JetStream). Drain until no more rows are claimable.
        drain_batches = 0
        max_batches = 50
        while True:
            drain_batches += 1
            if drain_batches > max_batches:
                # Avoid monopolizing a wakeup worker forever under extreme sustained churn.
                logger.warning(
                    "Wakeup drain capped proj=%s agent=%s batches=%s limit=%s",
                    parts.project_id,
                    agent_id,
                    drain_batches,
                    max_batches,
                )
                # Ask for a follow-up pass; this preserves liveness even if wakeups are coalesced.
                slot = self._wakeup_slots.get(key)
                if slot is not None:
                    slot.dirty = True
                return

            wakeup_ctx = CGContext(
                project_id=parts.project_id,
                agent_id=agent_id,
                channel_id=parts.channel_id,
            )
            rows = await self._claim_inbox_rows(ctx=wakeup_ctx)
            if not rows:
                return

            logger.debug(
                "Wakeup inbox scan proj=%s agent=%s pending=%s",
                parts.project_id,
                agent_id,
                len(rows),
            )

            async def _handle_row(ctx: InboxRowContext) -> InboxRowResult:
                logger.debug(
                    "Wakeup inbox item id=%s type=%s corr=%s trace=%s depth=%s created_at=%s",
                    ctx.inbox_id,
                    ctx.message_type,
                    ctx.row.get("correlation_id"),
                    ctx.ctx.trace_id,
                    ctx.ctx.recursion_depth,
                    ctx.row.get("created_at"),
                )
                new_status, _ = await self._handle_inbox_row(
                    row_ctx=ctx,
                    leased_turn=False,
                )
                return InboxRowResult(status=new_status)

            async def _status_hook(row: dict, status: str) -> None:
                await self._on_inbox_status_updated(
                    row=row,
                    status=status,
                    channel_id=parts.channel_id,
                )

            await consume_inbox_rows(
                nats=self.nats,
                execution_store=self.execution_store,
                project_id=parts.project_id,
                rows=rows,
                handler=_handle_row,
                logger=logger,
                expected_status="processing",
                status_update_hook=_status_hook,
            )

    async def _claim_inbox_rows(self, *, ctx: CGContext) -> list:
        rows = await self.execution_store.claim_pending_inbox(
            ctx=ctx,
            limit=self.inbox_fetch_limit,
        )
        return rows

    async def _emit_inbox_progress(
        self,
        *,
        ctx: CGContext,
        inbox_id: Optional[str],
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        await self.react_processor.notify_inbox_progress(ctx=ctx)

        subject = ctx.subject("evt", "agent", ctx.agent_id, "inbox_progress")
        payload: Dict[str, Any] = {
            "project_id": ctx.project_id,
            "agent_id": ctx.agent_id,
            "status": status,
        }
        if inbox_id:
            payload["inbox_id"] = inbox_id
        if reason:
            payload["reason"] = reason

        try:
            await self.nats.publish_core(subject, json.dumps(payload, default=str).encode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Inbox progress publish failed proj=%s agent=%s inbox=%s status=%s: %s",
                ctx.project_id,
                ctx.agent_id,
                inbox_id,
                status,
                exc,
            )

    async def _schedule_delayed_wakeup(
        self,
        *,
        target_ctx: CGContext,
        delay_seconds: float,
    ) -> None:
        try:
            resolved_delay = max(0.0, float(delay_seconds or 0.0))
            if resolved_delay > 0.0:
                await asyncio.sleep(resolved_delay)
            await self.l0.wakeup(target_ctx=target_ctx, reason="deferred_wakeup")
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Delayed wakeup failed proj=%s agent=%s delay=%s: %s",
                target_ctx.project_id,
                target_ctx.agent_id,
                delay_seconds,
                exc,
            )

    async def _finalize_inbox_item(
        self,
        *,
        worker_id: int,
        item: AgentTurnWorkItem,
    ) -> None:
        if not item.inbox_id:
            return

        final_action = str(item.inbox_finalize_action or "consume")
        if final_action == "defer" and item.resume_data is not None:
            current_retry = _coerce_non_negative_int(item.inbox_retry_count, default=0)
            next_retry = current_retry + 1
            if next_retry > int(self.max_resume_defer_retries):
                logger.warning(
                    "Worker %s resume retry exhausted inbox=%s retry_count=%s max=%s; emit failed tool.result",
                    worker_id,
                    item.inbox_id,
                    next_retry,
                    self.max_resume_defer_retries,
                )
                await self.resume_handler.fail_retry_exhausted(
                    item,
                    retry_count=next_retry,
                    max_retries=int(self.max_resume_defer_retries),
                )
                final_action = str(item.inbox_finalize_action or "consume")

        if final_action == "defer":
            updated = await self.execution_store.defer_inbox_with_backoff(
                inbox_id=item.inbox_id,
                project_id=item.ctx.project_id,
                delay_seconds=float(item.retry_delay_seconds or 0.0),
                reason=safe_str(item.retry_reason) or "worker_deferred",
                last_error=safe_str(item.retry_reason),
                expected_status="processing",
            )
            if not updated:
                logger.warning(
                    "Worker %s failed to defer inbox id=%s",
                    worker_id,
                    item.inbox_id,
                )
            else:
                await self._emit_inbox_progress(
                    ctx=item.ctx,
                    inbox_id=item.inbox_id,
                    status="deferred",
                    reason=safe_str(item.retry_reason) or "worker_deferred",
                )
                self._spawn_background_task(
                    self._schedule_delayed_wakeup(
                        target_ctx=item.ctx,
                        delay_seconds=float(item.retry_delay_seconds or 0.0),
                    ),
                    name=f"deferred_wakeup:{safe_str(item.ctx.project_id)}:{safe_str(item.ctx.agent_id)}",
                )
            return

        target_status = "error" if final_action == "error" else "consumed"
        progress_reason = "worker_error" if final_action == "error" else "worker_consumed"
        updated = await self.execution_store.update_inbox_status(
            inbox_id=item.inbox_id,
            project_id=item.ctx.project_id,
            status=target_status,
            expected_status="processing",
        )
        if not updated:
            logger.warning(
                "Worker %s failed to mark %s inbox id=%s",
                worker_id,
                target_status,
                item.inbox_id,
            )
            return
        await self._emit_inbox_progress(
            ctx=item.ctx,
            inbox_id=item.inbox_id,
            status=target_status,
            reason=progress_reason,
        )

    async def _on_inbox_status_updated(
        self,
        *,
        row: dict,
        status: str,
        channel_id: str,
    ) -> None:
        if status in {"queued", "pending", "processing"}:
            return

        project_id = safe_str(row.get("project_id"))
        agent_id = safe_str(row.get("agent_id"))
        inbox_id = safe_str(row.get("inbox_id"))
        if not project_id or not agent_id:
            return

        progress_ctx = CGContext(
            project_id=project_id,
            channel_id=channel_id or "public",
            agent_id=agent_id,
        )
        await self._emit_inbox_progress(
            ctx=progress_ctx,
            inbox_id=inbox_id,
            status=status,
            reason="inbox_status_update",
        )

    async def _handle_inbox_row(
        self,
        *,
        row_ctx: InboxRowContext,
        leased_turn: bool,
    ) -> tuple[Optional[str], bool]:
        message_type = row_ctx.message_type
        payload = row_ctx.payload
        inbox_id = row_ctx.inbox_id
        inbox_retry_count = _coerce_non_negative_int((row_ctx.row or {}).get("retry_count"), default=0)
        if message_type == "turn":
            return await self._handle_turn_inbox(
                payload=dict(payload or {}),
                inbox_id=inbox_id,
                inbox_retry_count=inbox_retry_count,
                wakeup_ctx=row_ctx.ctx,
                leased_turn=leased_turn,
                inbox_created_at=row_ctx.inbox_created_at,
                inbox_processed_at=row_ctx.inbox_processed_at,
            )
        if message_type in ("tool_result", "timeout"):
            enqueued = await self._enqueue_resume(
                payload,
                ingress_ctx=row_ctx.ctx,
                inbox_id=inbox_id,
                inbox_retry_count=inbox_retry_count,
                inbox_created_at=row_ctx.inbox_created_at,
                inbox_processed_at=row_ctx.inbox_processed_at,
            )
            return (None if enqueued else "error"), leased_turn
        if message_type == "stop":
            enqueued = await self._enqueue_stop(
                payload,
                ingress_ctx=row_ctx.ctx,
                inbox_id=inbox_id,
                inbox_retry_count=inbox_retry_count,
                inbox_created_at=row_ctx.inbox_created_at,
                inbox_processed_at=row_ctx.inbox_processed_at,
            )
            return (None if enqueued else "error"), leased_turn

        logger.warning(
            "Wakeup inbox unsupported message_type=%s inbox_id=%s",
            message_type,
            inbox_id,
        )
        return "error", leased_turn

    async def _handle_turn_inbox(
        self,
        *,
        payload: dict,
        inbox_id: str,
        inbox_retry_count: int,
        wakeup_ctx: CGContext,
        leased_turn: bool,
        inbox_created_at: datetime | None,
        inbox_processed_at: datetime | None,
    ) -> tuple[Optional[str], bool]:
        recursion_depth = int(wakeup_ctx.recursion_depth)
        agent_turn_id = safe_str(wakeup_ctx.agent_turn_id)
        if not agent_turn_id:
            logger.warning("Wakeup turn missing agent_turn_id inbox_id=%s", inbox_id)
            return "error", leased_turn

        turn_epoch = int(wakeup_ctx.turn_epoch or 0)

        if turn_epoch <= 0:
            logger.warning(
                "Wakeup turn missing/invalid turn_epoch inbox_id=%s turn=%s epoch=%s",
                inbox_id,
                agent_turn_id,
                wakeup_ctx.turn_epoch,
            )
            return "error", leased_turn

        try:
            await self.state_store.update(
                ctx=wakeup_ctx,
                active_recursion_depth=recursion_depth,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Wakeup update active_recursion_depth failed agent=%s turn=%s: %s",
                wakeup_ctx.agent_id,
                agent_turn_id,
                exc,
            )
        enqueued = await self._enqueue_cmd(
            payload,
            ingress_ctx=wakeup_ctx,
            inbox_id=inbox_id,
            inbox_retry_count=inbox_retry_count,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
        )
        return (None if enqueued else "error"), leased_turn

    async def _enqueue_suspended_reconcile_resumes(
        self,
        *,
        limit: int = 200,
        include_received: bool = False,
    ) -> None:
        heads = await self.state_store.list_suspended_heads_with_waiting(limit=limit)
        if not heads:
            return
        statuses = ("waiting", "received") if include_received else ("waiting",)
        for head in heads:
            agent_turn_id = safe_str(getattr(head, "active_agent_turn_id", None))
            if not agent_turn_id:
                continue
            try:
                head_turn_ctx = build_state_head_context(head)
            except ProtocolViolationError as exc:
                logger.warning(
                    "Worker reconcile skip invalid state_head context agent=%s turn=%s err=%s",
                    getattr(head, "agent_id", None),
                    agent_turn_id,
                    exc,
                )
                continue
            wait_rows = await self.state_store.list_turn_waiting_tools(
                ctx=head_turn_ctx,
                statuses=statuses,
            )
            if not wait_rows:
                continue

            tool_call_ids = [
                safe_str(row.get("tool_call_id"))
                for row in wait_rows
                if safe_str(row.get("tool_call_id"))
            ]
            if not tool_call_ids:
                continue

            for row in wait_rows:
                tool_call_id = safe_str(row.get("tool_call_id"))
                if not tool_call_id:
                    continue
                pending_rows = await self.execution_store.claim_pending_inbox_by_correlation(
                    ctx=head_turn_ctx,
                    correlation_id=tool_call_id,
                    limit=20,
                )
                if not pending_rows:
                    continue
                for pending_row in pending_rows:
                    inbox_id = safe_str(pending_row.get("inbox_id"))
                    message_type = safe_str(pending_row.get("message_type"))
                    payload = pending_row.get("payload") or {}
                    if not isinstance(payload, dict):
                        payload = {}
                    if message_type not in ("tool_result", "timeout"):
                        await self.execution_store.update_inbox_status(
                            inbox_id=inbox_id,
                            project_id=head.project_id,
                            status="error",
                            expected_status="processing",
                        )
                        continue
                    try:
                        ingress_ctx, safe_payload = build_replay_context(
                            raw_payload=payload,
                            db_row=pending_row,
                        )
                    except ProtocolViolationError as exc:
                        logger.warning(
                            "Watchdog reconcile invalid inbox context id=%s: %s",
                            inbox_id,
                            exc,
                        )
                        await self.execution_store.update_inbox_status(
                            inbox_id=inbox_id,
                            project_id=head.project_id,
                            status="error",
                            expected_status="processing",
                        )
                        continue
                    enqueued = await self._enqueue_resume(
                        safe_payload,
                        ingress_ctx=ingress_ctx,
                        inbox_id=inbox_id,
                        inbox_retry_count=_coerce_non_negative_int(pending_row.get("retry_count"), default=0),
                        inbox_created_at=pending_row.get("created_at"),
                        inbox_processed_at=pending_row.get("processed_at"),
                    )
                    if not enqueued:
                        await self.execution_store.update_inbox_status(
                            inbox_id=inbox_id,
                            project_id=head.project_id,
                            status="error",
                            expected_status="processing",
                        )

    async def _watchdog_loop(self) -> None:
        logger.info("Worker watchdog started interval=%ss", self.watchdog_interval_seconds)
        while True:
            try:
                await self._requeue_stale_inbox()
                await self._wakeup_due_deferred_inbox(limit=200)
                await self._enqueue_suspended_reconcile_resumes(limit=200, include_received=True)
                await self._inject_timeout_reports()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Worker watchdog tick failed: %s", exc, exc_info=True)
            await asyncio.sleep(self.watchdog_interval_seconds)

    async def _requeue_stale_inbox(self) -> None:
        if self.inbox_processing_timeout_seconds <= 0:
            return
        requeued = await self.execution_store.requeue_stale_inbox(
            timeout_seconds=self.inbox_processing_timeout_seconds
        )
        if requeued:
            logger.info(
                "Requeued %s stale inbox items (timeout=%ss)",
                requeued,
                self.inbox_processing_timeout_seconds,
            )

    async def _wakeup_due_deferred_inbox(self, *, limit: int = 200) -> None:
        rows = await self.execution_store.list_due_deferred_inbox_targets(limit=limit)
        if not rows:
            return
        woke = 0
        for row in rows:
            project_id = safe_str(row.get("project_id"))
            agent_id = safe_str(row.get("agent_id"))
            if not project_id or not agent_id:
                continue
            channel_id = safe_str(row.get("channel_id")) or "public"
            try:
                await self.l0.wakeup(
                    target_ctx=CGContext(
                        project_id=project_id,
                        channel_id=channel_id,
                        agent_id=agent_id,
                    ),
                    reason="watchdog_due_deferred",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Worker watchdog failed deferred wakeup proj=%s agent=%s: %s",
                    project_id,
                    agent_id,
                    exc,
                )
                continue
            woke += 1
        if woke:
            logger.info("Worker watchdog woke deferred inbox targets=%s", woke)

    async def _watchdog_load_tool_call_card(self, *, project_id: str, tool_call_id: str) -> Optional[Any]:
        try:
            cards = await self.cardbox.get_cards_by_tool_call_ids(
                project_id=project_id,
                tool_call_ids=[tool_call_id],
            )
            for card in cards:
                if getattr(card, "type", None) == "tool.call":
                    return card
        except Exception as exc:  # noqa: BLE001
            logger.warning("Worker watchdog: load tool.call failed: %s", exc)
        return None

    async def _watchdog_resolve_after_execution(self, *, project_id: str, tool_name: str) -> str:
        after_exec = "suspend"
        try:
            tool_def = await self.resource_store.fetch_tool_definition(project_id, tool_name)
            after_exec_val = safe_str(tool_def.get("after_execution")) if isinstance(tool_def, dict) else ""
            if after_exec_val not in ("suspend", "terminate"):
                logger.warning(
                    "Worker watchdog: invalid after_execution for timeout report tool=%s value=%s",
                    tool_name,
                    after_exec_val or "<empty>",
                )
                return after_exec
            return after_exec_val
        except Exception as exc:  # noqa: BLE001
            logger.warning("Worker watchdog: load tool definition failed: %s", exc)
            return after_exec

    def _watchdog_build_headers(
        self, *, trace_id: Optional[str], recursion_depth: Any
    ) -> Optional[tuple[Dict[str, str], Optional[str]]]:
        base_ctx = CGContext(project_id="sys.worker", channel_id="public", agent_id="worker.watchdog")
        try:
            default_depth = max(0, int(recursion_depth or 0))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Worker watchdog: invalid recursion_depth=%r err=%s", recursion_depth, exc)
            return None
        try:
            normalized_ctx = base_ctx.with_trace_transport(
                base_headers={},
                trace_id=trace_id,
                default_depth=default_depth,
            )
        except ProtocolViolationError as exc:
            logger.warning("Worker watchdog: invalid trace_id: %s", exc)
            return None
        return dict(normalized_ctx.headers or {}), normalized_ctx.trace_id

    async def _inject_timeout_reports(self) -> None:
        rows = await self.state_store.list_timed_out_waiting_tools(limit=200)
        if not rows:
            return
        cleared_heads: set[tuple[str, str, str, int]] = set()
        for row in rows:
            project_id = safe_str(row.get("project_id"))
            agent_id = safe_str(row.get("agent_id"))
            agent_turn_id = safe_str(row.get("active_agent_turn_id"))
            tool_call_id = safe_str(row.get("tool_call_id"))
            channel_id = safe_str(row.get("active_channel_id")) or "public"
            if not project_id or not agent_id or not agent_turn_id or not tool_call_id:
                continue
            turn_epoch = int(row.get("turn_epoch") or 0)
            if turn_epoch <= 0:
                continue

            tool_call_card = await self._watchdog_load_tool_call_card(
                project_id=project_id,
                tool_call_id=tool_call_id,
            )
            if not tool_call_card:
                logger.warning(
                    "Worker watchdog: tool.call not found tool_call_id=%s agent=%s",
                    tool_call_id,
                    agent_id,
                )
                continue

            tool_name = None
            content = getattr(tool_call_card, "content", None)
            if content is not None:
                tool_name = getattr(content, "tool_name", None)
            tool_name = safe_str(tool_name)
            if not tool_name:
                logger.warning(
                    "Worker watchdog: tool.call missing tool_name tool_call_id=%s agent=%s",
                    tool_call_id,
                    agent_id,
                )
                continue
            after_exec = await self._watchdog_resolve_after_execution(
                project_id=project_id,
                tool_name=tool_name,
            )

            cmd_data = {
                "after_execution": after_exec,
                "tool_name": tool_name,
            }
            tool_call_meta = dict(getattr(tool_call_card, "metadata", {}) or {})
            resolved_step_id = safe_str(tool_call_meta.get("step_id")) or safe_str(row.get("step_id"))
            resolved_parent_step_id = safe_str(tool_call_meta.get("parent_step_id")) or safe_str(
                row.get("parent_step_id")
            )
            trace_id_value = safe_str(row.get("trace_id")) or safe_str(tool_call_meta.get("trace_id"))
            base_turn_ctx = CGContext(
                project_id=project_id,
                channel_id=channel_id,
                agent_id=agent_id,
                agent_turn_id=agent_turn_id,
                turn_epoch=int(turn_epoch),
            )
            result_cg_ctx = base_turn_ctx.evolve(
                step_id=resolved_step_id,
                tool_call_id=tool_call_id,
                trace_id=trace_id_value,
                parent_step_id=resolved_parent_step_id,
            )
            result_context = ToolResultContext(
                ctx=result_cg_ctx,
                cmd_data=cmd_data,
            )
            payload, result_card = ToolResultBuilder(
                result_context,
                author_id="worker.watchdog",
                function_name=tool_name,
                error_source="watchdog",
            ).build(
                status=STATUS_TIMEOUT,
                result=None,
                error={
                    "code": "tool_timeout",
                    "message": "tool timeout (resume_deadline exceeded)",
                },
                after_execution=after_exec,
            )
            await self.cardbox.save_card(result_card)

            headers_result = self._watchdog_build_headers(
                trace_id=trace_id_value or None,
                recursion_depth=row.get("active_recursion_depth"),
            )
            if headers_result is None:
                logger.warning(
                    "Worker watchdog: skip timeout injection due to invalid transport context "
                    "project=%s agent=%s turn=%s tool_call_id=%s",
                    project_id,
                    agent_id,
                    agent_turn_id,
                    tool_call_id,
                )
                continue
            headers, trace_id = headers_result
            watchdog_source_ctx = result_cg_ctx.evolve(
                agent_id="sys.agent_worker.watchdog",
                agent_turn_id="",
                step_id=None,
                tool_call_id=None,
                parent_agent_id=None,
                parent_agent_turn_id=None,
                parent_step_id=resolved_step_id,
            ).with_trace_transport(
                base_headers=headers,
                trace_id=trace_id,
                default_depth=int(base_turn_ctx.recursion_depth),
            )
            async with self.execution_store.pool.connection() as conn:
                async with conn.transaction():
                    report_result = await self.l0.report_intent(
                        source_ctx=watchdog_source_ctx,
                        intent=ReportIntent(
                            target=TurnRef(
                                project_id=project_id,
                                agent_id=agent_id,
                                agent_turn_id=agent_turn_id,
                                expected_turn_epoch=turn_epoch,
                            ),
                            message_type="timeout",
                            payload=payload,
                            correlation_id=tool_call_id,
                        ),
                        conn=conn,
                        wakeup=True,
                    )
            if report_result.status != "accepted":
                logger.warning(
                    "Worker watchdog: timeout report rejected status=%s code=%s agent=%s turn=%s",
                    report_result.status,
                    report_result.error_code,
                    agent_id,
                    agent_turn_id,
                )
                continue
            wakeup_signals = list(report_result.wakeup_signals or ())
            if wakeup_signals:
                await self.l0.publish_wakeup_signals(wakeup_signals)
            wakeup_ctx = (
                wakeup_signals[0].target_ctx
                if wakeup_signals
                else base_turn_ctx.with_trace_transport(
                    base_headers=headers,
                    trace_id=safe_str(report_result.payload.get("trace_id")) or trace_id,
                    default_depth=int(base_turn_ctx.recursion_depth),
                )
            )

            head_key = (project_id, agent_id, agent_turn_id, turn_epoch)
            if head_key in cleared_heads:
                continue
            clear_ctx = base_turn_ctx.with_transport(
                headers=wakeup_ctx.to_nats_headers(include_topology=False),
            )
            updated = await self.state_store.update(
                ctx=clear_ctx,
                expect_status="suspended",
                resume_deadline=None,
            )
            if not updated:
                logger.warning(
                    "Worker watchdog: failed to clear resume_deadline agent=%s turn=%s",
                    agent_id,
                    agent_turn_id,
                )
            else:
                cleared_heads.add(head_key)

    async def worker_loop(self):
        logger.info(
            "Worker loop started (concurrency=%s, queue_maxsize=%s)",
            self.worker_concurrency,
            self.queue.maxsize,
        )
        workers = [
            asyncio.create_task(self._worker(i + 1), name=f"agent_worker:{i + 1}")
            for i in range(self.worker_concurrency)
        ]
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

    async def _worker(self, worker_id: int) -> None:
        logger.info("Worker %s ready", worker_id)
        while True:
            item = await self.queue.get()
            dequeued_at = utc_now()
            queue_wait_ms = None
            inbox_age_ms = None
            if self.observability.timing_capture:
                if item.timing is None:
                    item.timing = {}
                item.timing["worker_dequeued_at"] = to_iso(dequeued_at)
                queue_wait_ms = duration_ms(item.enqueued_at, dequeued_at)
                if queue_wait_ms is not None:
                    item.timing["queue_wait_ms"] = queue_wait_ms
                inbox_age_ms = duration_ms(item.inbox_created_at, dequeued_at)
                if inbox_age_ms is not None:
                    item.timing["inbox_age_ms"] = inbox_age_ms
                inbox_claim_lag_ms = duration_ms(item.inbox_created_at, item.inbox_processed_at)
                if inbox_claim_lag_ms is not None:
                    item.timing["inbox_claim_lag_ms"] = inbox_claim_lag_ms
                post_claim_queue_lag_ms = duration_ms(item.inbox_processed_at, item.enqueued_at)
                if post_claim_queue_lag_ms is not None:
                    item.timing["post_claim_queue_lag_ms"] = post_claim_queue_lag_ms
            handled_ok = False
            try:
                kind = "turn"
                if item.resume_data is not None:
                    kind = "resume"
                elif item.stop_data is not None:
                    kind = "stop"
                await self._handle_work_item(
                    item=item,
                    headers=item.ctx.headers,
                    worker_id=worker_id,
                    kind=kind,
                    queue_wait_ms=queue_wait_ms,
                    inbox_age_ms=inbox_age_ms,
                )
                handled_ok = True
            except Exception as exc:  # noqa: BLE001
                logger.error("Worker %s error processing item: %s", worker_id, exc, exc_info=True)
            finally:
                if handled_ok and item.inbox_id:
                    await self._finalize_inbox_item(worker_id=worker_id, item=item)
                self.queue.task_done()

    @traced(
        _TRACER,
        "agent_worker.handle_item",
        headers_arg="headers",
        include_links=True,
        record_exception=True,
        set_status_on_exception=True,
    )
    async def _handle_work_item(
        self,
        *,
        item: AgentTurnWorkItem,
        headers: Dict[str, str],
        worker_id: int,
        kind: str,
        queue_wait_ms: Optional[float],
        inbox_age_ms: Optional[float],
    ) -> None:
        _ = headers
        if self.observability.timing_worker_logs:
            logger.info(
                "Worker %s processing %s agent=%s turn=%s queue_wait_ms=%s inbox_age_ms=%s inbox_id=%s",
                worker_id,
                kind,
                item.ctx.agent_id,
                item.ctx.agent_turn_id,
                queue_wait_ms,
                inbox_age_ms,
                item.inbox_id,
            )
        else:
            logger.info(
                "Worker %s processing %s agent=%s turn=%s",
                worker_id,
                kind,
                item.ctx.agent_id,
                item.ctx.agent_turn_id,
            )

        if item.resume_data is not None:
            await self.resume_handler.handle(item)
        elif item.stop_data is not None:
            await self.stop_handler.handle(item)
        else:
            await self.react_processor.process_step(item)


async def main():
    cfg = load_app_config()
    worker = AgentWorker(cfg)
    try:
        await worker.start()
    finally:
        await worker.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
