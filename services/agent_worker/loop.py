"""Agent Worker loop entrypoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC
import asyncio
import json
import logging
from typing import Any, Dict, Iterable, Optional


from core.config import PROTOCOL_VERSION, MAX_STEPS, MAX_RECURSION_DEPTH, DEFAULT_TIMEOUT
from core.app_config import load_app_config
from core.config_defaults import (
    DEFAULT_WORKER_MAX_CONCURRENCY,
    DEFAULT_WORKER_QUEUE_MAXSIZE_FACTOR,
    DEFAULT_WORKER_INBOX_FETCH_LIMIT,
    DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS,
    DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS,
    DEFAULT_WORKER_TARGETS,
)
from core.errors import ProtocolViolationError
from core.headers import RECURSION_DEPTH_HEADER, ensure_recursion_depth
from core.status import STATUS_FAILED, STATUS_TIMEOUT, TOOL_RESULT_STATUS_SET
from core.subject import format_subject, parse_subject, subject_pattern
from core.trace import ensure_trace_headers
from core.utils import safe_str, set_loop_policy, safe_target_label
from core.utp_protocol import AgentTaskPayload, extract_tool_result_payload
from infra.agent_routing import resolve_agent_target
from infra.llm_gateway import LLMService, LLMWrapper
from infra.mock_llm_handler import mock_chat
from infra.observability.otel import get_tracer, traced
from infra.primitives import report as report_primitive
from infra.service_runtime import ServiceBase
from infra.worker_helpers import InboxRowContext, InboxRowResult, consume_inbox_rows
from infra.worker_helpers import normalize_headers
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


def _worker_item_span_attrs(args: Dict[str, Any]) -> Dict[str, Any]:
    item = args.get("item")
    if item is None:
        return {}
    attrs: Dict[str, Any] = {
        "cg.project_id": str(getattr(item, "project_id", "")),
        "cg.channel_id": str(getattr(item, "channel_id", "")),
        "cg.agent_id": str(getattr(item, "agent_id", "")),
        "cg.agent_turn_id": str(getattr(item, "agent_turn_id", "")),
        "cg.turn_epoch": int(getattr(item, "turn_epoch", 0) or 0),
        "cg.kind": str(args.get("kind") or "turn"),
    }
    inbox_id = getattr(item, "inbox_id", None)
    if inbox_id:
        attrs["cg.inbox_id"] = str(inbox_id)
    parent_step_id = getattr(item, "parent_step_id", None)
    if parent_step_id:
        attrs["cg.parent_step_id"] = str(parent_step_id)
    trace_id = getattr(item, "trace_id", None)
    if trace_id:
        attrs["cg.trace_id"] = str(trace_id)
    return attrs


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


def _is_watch_heartbeat_payload(*, function_name: Optional[str], payload: Dict[str, Any]) -> bool:
    if function_name not in ("skills.task_watch", "skills.job_watch"):
        return False
    result = payload.get("result")
    if not isinstance(result, dict):
        return False
    if result.get("task_watch") == "running":
        return True
    if result.get("job_watch") == "running":
        return True
    status = result.get("status")
    if isinstance(status, dict):
        if status.get("task_watch") == "running" or status.get("job_watch") == "running":
            return True
    return False


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
        self.worker_targets = settings.worker_targets
        self.observability = observability

        self.state_store = self.register_store(self.ctx.stores.state_store())
        self.step_store = self.register_store(self.ctx.stores.step_store())
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        self.skill_store = self.register_store(self.ctx.stores.skill_store())
        self.resource_loader = ResourceLoader(self.cardbox, self.resource_store)
        self.assembler = ContextAssembler()
        self.llm_wrapper = LLMWrapper(LLMService(), mock_chat=mock_chat)
        self.dispatcher = UTPDispatcher(
            self.cardbox,
            self.nats,
            self.resource_store,
            execution_store=self.execution_store,
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
        self._resume_reconcile_lease_owner = f"agent-worker:{id(self)}"

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
            task = asyncio.create_task(self._wakeup_worker(idx + 1), name=f"wakeup_worker:{idx + 1}")
            self._wakeup_tasks.add(task)
            task.add_done_callback(self._wakeup_tasks.discard)

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
        asyncio.create_task(self._watchdog_loop())

    async def close(self) -> None:
        await super().close()

    async def _enqueue_cmd(
        self,
        subject: str,
        data: dict,
        headers: dict,
        *,
        inbox_id: str | None,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        parts = parse_subject(subject)
        if not parts:
            logger.warning("Invalid subject: %s", subject)
            return False
        try:
            payload = AgentTaskPayload(**data)
        except Exception as exc:  # noqa: BLE001
            logger.error("Payload decode failed: %s", exc)
            return False

        try:
            headers, trace_id, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
                trace_id=safe_str(payload, "trace_id"),
            )
        except ProtocolViolationError as exc:
            logger.warning("Reject cmd (protocol_violation): %s", exc)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            project_id=parts.project_id,
            channel_id=parts.channel_id or payload.channel_id,
            agent_turn_id=payload.agent_turn_id,
            agent_id=payload.agent_id,
            profile_box_id=safe_str(payload, "profile_box_id"),
            context_box_id=safe_str(payload, "context_box_id"),
            output_box_id=safe_str(payload, "output_box_id"),
            turn_epoch=payload.turn_epoch,
            headers=headers,
            parent_step_id=safe_str(payload, "parent_step_id"),
            trace_id=trace_id,
            resume_data=None,
            stop_data=None,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
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
            item.agent_turn_id,
            item.agent_id,
            item.project_id,
            item.channel_id,
        )
        await self._queue_work_item(item)
        return True

    async def _enqueue_resume(
        self,
        subject: str,
        data: dict,
        headers: dict,
        *,
        inbox_id: str | None,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        parts = parse_subject(subject)
        agent_id = data.get("agent_id")
        agent_turn_id = data.get("agent_turn_id")
        turn_epoch = data.get("turn_epoch")

        if not parts or not agent_id or not agent_turn_id or turn_epoch is None:
            logger.warning("Resume missing context: %s %s", subject, data)
            return False


        try:
            headers, trace_id, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
                require_depth=True,
            )
        except ProtocolViolationError as exc:
            logger.warning("Reject resume (protocol_violation): %s", exc)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            project_id=parts.project_id,
            channel_id=parts.channel_id,
            agent_turn_id=str(agent_turn_id),
            agent_id=agent_id,
            profile_box_id=None,
            context_box_id=None,
            output_box_id=None,
            turn_epoch=int(turn_epoch),
            headers=headers,
            parent_step_id=None,
            trace_id=trace_id,
            resume_data=data,
            stop_data=None,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
            enqueued_at=enqueued_at,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
            timing=self._build_item_timing(
                enqueued_at=enqueued_at,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            ),
        )
        logger.info("Enqueued RESUME for %s turn=%s epoch=%s", agent_id, item.agent_turn_id, item.turn_epoch)
        await self._queue_work_item(item)
        return True

    async def _enqueue_stop(
        self,
        subject: str,
        data: dict,
        headers: dict,
        *,
        inbox_id: str | None,
        inbox_created_at: datetime | None = None,
        inbox_processed_at: datetime | None = None,
    ):
        parts = parse_subject(subject)
        agent_id = data.get("agent_id")
        agent_turn_id = data.get("agent_turn_id")
        turn_epoch = data.get("turn_epoch")

        if not parts or not agent_id or not agent_turn_id or turn_epoch is None:
            logger.warning("Stop missing context: %s %s", subject, data)
            return False

        try:
            headers, trace_id, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
                require_depth=True,
            )
        except ProtocolViolationError as exc:
            logger.warning("Reject stop (protocol_violation): %s", exc)
            return False

        enqueued_at = self._new_enqueued_at()
        item = AgentTurnWorkItem(
            project_id=parts.project_id,
            channel_id=parts.channel_id,
            agent_turn_id=str(agent_turn_id),
            agent_id=agent_id,
            profile_box_id=None,
            context_box_id=None,
            output_box_id=None,
            turn_epoch=int(turn_epoch),
            headers=headers,
            parent_step_id=None,
            trace_id=trace_id,
            resume_data=None,
            stop_data=data,
            is_continuation=False,
            inbox_id=inbox_id,
            guard_inbox_id=inbox_id,
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
            agent_id,
            item.agent_turn_id,
            item.turn_epoch,
        )
        await self._queue_work_item(item)
        return True

    async def _enqueue_wakeup(self, subject: str, data: dict, headers: dict):
        parts = parse_subject(subject)
        if not parts:
            logger.warning("Invalid wakeup subject: %s", subject)
            return
        agent_id = data.get("agent_id")
        if not agent_id:
            logger.warning("Wakeup missing agent_id: %s", data)
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

            state = await self.state_store.fetch(parts.project_id, agent_id)
            rows = await self._claim_inbox_rows(parts=parts, agent_id=agent_id, state=state)
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
                    ctx.row.get("trace_id"),
                    ctx.row.get("recursion_depth"),
                    ctx.row.get("created_at"),
                )
                new_status, _ = await self._handle_inbox_row(
                    subject=subject,
                    message_type=ctx.message_type,
                    payload=ctx.payload,
                    parts=parts,
                    inbox_id=ctx.inbox_id,
                    agent_id=agent_id,
                    state=state,
                    wakeup_headers=ctx.headers,
                    trace_id=ctx.trace_id,
                    leased_turn=False,
                    recursion_depth=ctx.recursion_depth,
                    inbox_created_at=ctx.inbox_created_at,
                    inbox_processed_at=ctx.inbox_processed_at,
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
                require_depth=True,
                status_update_hook=_status_hook,
            )

    async def _claim_inbox_rows(self, *, parts: Any, agent_id: str, state: Any) -> list:
        rows = await self.execution_store.claim_pending_inbox(
            project_id=parts.project_id,
            agent_id=agent_id,
            limit=self.inbox_fetch_limit,
        )
        return rows

    async def _emit_inbox_progress(
        self,
        *,
        project_id: str,
        channel_id: str,
        agent_id: str,
        inbox_id: Optional[str],
        status: str,
        reason: Optional[str] = None,
    ) -> None:
        await self.react_processor.notify_inbox_progress(project_id=project_id, agent_id=agent_id)

        subject = format_subject(
            project_id=project_id,
            channel_id=channel_id or "public",
            category="evt",
            component="agent",
            target=agent_id,
            suffix="inbox_progress",
            protocol_version=PROTOCOL_VERSION,
        )
        payload: Dict[str, Any] = {
            "project_id": project_id,
            "agent_id": agent_id,
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
                project_id,
                agent_id,
                inbox_id,
                status,
                exc,
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

        await self._emit_inbox_progress(
            project_id=project_id,
            channel_id=channel_id or "public",
            agent_id=agent_id,
            inbox_id=inbox_id,
            status=status,
            reason="inbox_status_update",
        )

    async def _handle_inbox_row(
        self,
        *,
        subject: str,
        message_type: str,
        payload: dict,
        parts: Any,
        inbox_id: str,
        agent_id: str,
        state: Any,
        wakeup_headers: Dict[str, str],
        trace_id: Optional[str],
        leased_turn: bool,
        recursion_depth: int,
        inbox_created_at: datetime | None,
        inbox_processed_at: datetime | None,
    ) -> tuple[Optional[str], bool]:
        if message_type == "turn":
            return await self._handle_turn_inbox(
                subject=subject,
                payload=payload,
                parts=parts,
                inbox_id=inbox_id,
                agent_id=agent_id,
                state=state,
                wakeup_headers=wakeup_headers,
                trace_id=trace_id,
                leased_turn=leased_turn,
                recursion_depth=recursion_depth,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            )
        if message_type in ("tool_result", "timeout"):
            enqueued = await self._enqueue_resume(
                subject,
                payload,
                wakeup_headers,
                inbox_id=inbox_id,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
            )
            return (None if enqueued else "error"), leased_turn
        if message_type == "stop":
            enqueued = await self._enqueue_stop(
                subject,
                payload,
                wakeup_headers,
                inbox_id=inbox_id,
                inbox_created_at=inbox_created_at,
                inbox_processed_at=inbox_processed_at,
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
        subject: str,
        payload: dict,
        parts: Any,
        inbox_id: str,
        agent_id: str,
        state: Any,
        wakeup_headers: Dict[str, str],
        trace_id: Optional[str],
        leased_turn: bool,
        recursion_depth: int,
        inbox_created_at: datetime | None,
        inbox_processed_at: datetime | None,
    ) -> tuple[Optional[str], bool]:
        agent_turn_id = safe_str(payload.get("agent_turn_id"))
        if not agent_turn_id:
            logger.warning("Wakeup turn missing agent_turn_id inbox_id=%s", inbox_id)
            return "error", leased_turn

        turn_epoch_raw = payload.get("turn_epoch")
        try:
            turn_epoch = int(turn_epoch_raw) if turn_epoch_raw is not None else 0
        except (TypeError, ValueError):
            turn_epoch = 0

        if turn_epoch <= 0:
            logger.warning(
                "Wakeup turn missing/invalid turn_epoch inbox_id=%s turn=%s epoch=%s",
                inbox_id,
                agent_turn_id,
                turn_epoch_raw,
            )
            return "error", leased_turn

        if turn_epoch > 0:
            try:
                await self.state_store.update(
                    project_id=parts.project_id,
                    agent_id=agent_id,
                    expect_turn_epoch=turn_epoch,
                    expect_agent_turn_id=agent_turn_id,
                    active_recursion_depth=recursion_depth,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Wakeup update active_recursion_depth failed agent=%s turn=%s: %s",
                    agent_id,
                    agent_turn_id,
                    exc,
                )
        enqueued = await self._enqueue_cmd(
            subject,
            payload,
            wakeup_headers,
            inbox_id=inbox_id,
            inbox_created_at=inbox_created_at,
            inbox_processed_at=inbox_processed_at,
        )
        return (None if enqueued else "error"), leased_turn

    async def _enqueue_resume_from_ledger_row(self, *, head: Any, row: Dict[str, Any]) -> None:
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        tool_call_id = safe_str(row.get("tool_call_id")) or safe_str(payload.get("tool_call_id"))
        tool_result_card_id = safe_str(row.get("tool_result_card_id")) or safe_str(
            payload.get("tool_result_card_id")
        )
        if not tool_call_id or not tool_result_card_id:
            return
        status = safe_str(payload.get("status"))
        if status not in TOOL_RESULT_STATUS_SET:
            status = STATUS_FAILED
        after_execution = safe_str(payload.get("after_execution"))
        if after_execution not in ("suspend", "terminate"):
            after_execution = "suspend"
        headers, trace_id = self._watchdog_build_headers(
            trace_id=str(getattr(head, "trace_id", "") or ""),
            recursion_depth=getattr(head, "active_recursion_depth", 0),
        )
        item = AgentTurnWorkItem(
            project_id=head.project_id,
            channel_id=head.active_channel_id or "public",
            agent_turn_id=safe_str(getattr(head, "active_agent_turn_id", None)),
            agent_id=head.agent_id,
            profile_box_id=safe_str(head.profile_box_id),
            context_box_id=safe_str(head.context_box_id),
            output_box_id=safe_str(head.output_box_id),
            turn_epoch=int(head.turn_epoch),
            headers=dict(headers),
            parent_step_id=safe_str(getattr(head, "parent_step_id", None)),
            trace_id=trace_id,
            step_count=0,
            resume_data={
                "tool_call_id": tool_call_id,
                "status": status,
                "after_execution": after_execution,
                "tool_result_card_id": tool_result_card_id,
                "step_id": safe_str(payload.get("step_id")),
            },
            is_continuation=True,
            inbox_id=None,
            guard_inbox_id=None,
            enqueued_at=self._new_enqueued_at(),
        )
        await self._queue_work_item(item)

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
            claimed_ledgers = await self.state_store.claim_turn_resume_ledgers(
                project_id=head.project_id,
                agent_id=head.agent_id,
                agent_turn_id=agent_turn_id,
                turn_epoch=int(head.turn_epoch),
                lease_owner=self._resume_reconcile_lease_owner,
                lease_seconds=max(5.0, self.watchdog_interval_seconds * 2.0),
                limit=50,
            )
            if claimed_ledgers:
                for ledger_row in claimed_ledgers:
                    await self._enqueue_resume_from_ledger_row(head=head, row=ledger_row)
                continue

            wait_rows = await self.state_store.list_turn_waiting_tools(
                project_id=head.project_id,
                agent_id=head.agent_id,
                agent_turn_id=agent_turn_id,
                turn_epoch=int(head.turn_epoch),
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

            try:
                cards = await self.cardbox.get_cards_by_tool_call_ids(
                    project_id=head.project_id,
                    tool_call_ids=tool_call_ids,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Startup reconcile: failed loading tool_result cards agent=%s turn=%s: %s",
                    head.agent_id,
                    agent_turn_id,
                    exc,
                )
                continue

            by_call: Dict[str, list[Any]] = {}
            for card in cards or []:
                if getattr(card, "type", None) != "tool.result":
                    continue
                tcid = safe_str(getattr(card, "tool_call_id", None))
                if not tcid:
                    continue
                by_call.setdefault(tcid, []).append(card)
            for tcid, card_list in by_call.items():
                card_list.sort(
                    key=lambda card: getattr(card, "created_at", datetime.max.replace(tzinfo=UTC))
                )

            headers, trace_id = self._watchdog_build_headers(
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                recursion_depth=head.active_recursion_depth,
            )
            parent_step_id = safe_str(getattr(head, "parent_step_id", None))

            for row in wait_rows:
                tool_call_id = safe_str(row.get("tool_call_id"))
                if not tool_call_id:
                    continue
                candidates = by_call.get(tool_call_id) or []
                if not candidates:
                    continue
                chosen_card = candidates[0]
                tool_result_card_id = safe_str(getattr(chosen_card, "card_id", None))
                if not tool_result_card_id:
                    continue

                try:
                    payload = extract_tool_result_payload(chosen_card)
                except Exception:
                    payload = {}
                meta = getattr(chosen_card, "metadata", None)
                function_name = (
                    safe_str(meta.get("function_name"))
                    if isinstance(meta, dict)
                    else None
                )
                if _is_watch_heartbeat_payload(function_name=function_name, payload=payload):
                    continue
                status = payload.get("status")
                if status not in TOOL_RESULT_STATUS_SET:
                    status = STATUS_FAILED
                after_execution = payload.get("after_execution")
                if after_execution not in ("suspend", "terminate"):
                    after_execution = "suspend"

                item = AgentTurnWorkItem(
                    project_id=head.project_id,
                    channel_id=head.active_channel_id or "public",
                    agent_turn_id=agent_turn_id,
                    agent_id=head.agent_id,
                    profile_box_id=safe_str(head.profile_box_id),
                    context_box_id=safe_str(head.context_box_id),
                    output_box_id=safe_str(head.output_box_id),
                    turn_epoch=int(head.turn_epoch),
                    headers=dict(headers),
                    parent_step_id=parent_step_id,
                    trace_id=trace_id,
                    step_count=0,
                    resume_data={
                        "tool_call_id": tool_call_id,
                        "status": status,
                        "after_execution": after_execution,
                        "tool_result_card_id": tool_result_card_id,
                        "step_id": safe_str(row.get("step_id")),
                    },
                    is_continuation=True,
                    inbox_id=None,
                    guard_inbox_id=None,
                    enqueued_at=self._new_enqueued_at(),
                )
                await self.state_store.record_turn_resume_ledger(
                    project_id=head.project_id,
                    agent_id=head.agent_id,
                    agent_turn_id=agent_turn_id,
                    turn_epoch=int(head.turn_epoch),
                    tool_call_id=tool_call_id,
                    tool_result_card_id=tool_result_card_id,
                    payload=item.resume_data or {},
                )
                await self._queue_work_item(item)

    async def _watchdog_loop(self) -> None:
        logger.info("Worker watchdog started interval=%ss", self.watchdog_interval_seconds)
        while True:
            try:
                await self._requeue_stale_inbox()
                requeued_ledgers = await self.state_store.requeue_expired_resume_ledgers(
                    limit=500,
                )
                if requeued_ledgers:
                    logger.info("Requeued %s expired resume ledger leases", requeued_ledgers)
                await self._enqueue_suspended_reconcile_resumes(limit=200, include_received=False)
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
            after_exec_val = tool_def.get("after_execution") if tool_def else None
            if after_exec_val in ("suspend", "terminate"):
                after_exec = after_exec_val
        except Exception as exc:  # noqa: BLE001
            logger.warning("Worker watchdog: load tool definition failed: %s", exc)
        return after_exec

    def _watchdog_build_headers(
        self, *, trace_id: Optional[str], recursion_depth: Any
    ) -> tuple[Dict[str, str], Optional[str]]:
        headers: Dict[str, str] = {}
        resolved_trace_id: Optional[str] = None
        try:
            headers, _, resolved_trace_id = ensure_trace_headers(headers, trace_id=trace_id)
        except ProtocolViolationError as exc:
            logger.warning("Worker watchdog: invalid trace_id: %s", exc)
            headers, _, resolved_trace_id = ensure_trace_headers(headers)
        try:
            headers = ensure_recursion_depth(headers, default_depth=recursion_depth)
        except ProtocolViolationError as exc:
            logger.warning("Worker watchdog: defaulted recursion depth: %s", exc)
            headers[RECURSION_DEPTH_HEADER] = str(recursion_depth)
        return headers, resolved_trace_id

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
            tool_name = str(tool_name) if tool_name else "unknown"
            after_exec = await self._watchdog_resolve_after_execution(
                project_id=project_id,
                tool_name=tool_name,
            )

            cmd_data = {
                "tool_call_id": tool_call_id,
                "agent_turn_id": agent_turn_id,
                "turn_epoch": turn_epoch,
                "agent_id": agent_id,
                "after_execution": after_exec,
                "tool_name": tool_name,
            }
            tool_call_meta = dict(getattr(tool_call_card, "metadata", {}) or {})
            trace_id_value = safe_str(row.get("trace_id"))
            parent_step_id = safe_str(row.get("parent_step_id"))
            step_id = safe_str(row.get("step_id"))
            if trace_id_value and not tool_call_meta.get("trace_id"):
                tool_call_meta["trace_id"] = trace_id_value
            if parent_step_id and not tool_call_meta.get("parent_step_id"):
                tool_call_meta["parent_step_id"] = parent_step_id
            if step_id and not tool_call_meta.get("step_id"):
                tool_call_meta["step_id"] = step_id

            result_context = ToolResultContext.from_cmd_data(
                project_id=project_id,
                cmd_data=cmd_data,
                tool_call_meta=tool_call_meta,
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

            headers, trace_id = self._watchdog_build_headers(
                trace_id=trace_id_value or None,
                recursion_depth=row.get("active_recursion_depth"),
            )
            async with self.execution_store.pool.connection() as conn:
                async with conn.transaction():
                    report_result = await report_primitive(
                        store=self.execution_store,
                        project_id=project_id,
                        channel_id=channel_id,
                        target_agent_id=agent_id,
                        message_type="timeout",
                        payload=payload,
                        correlation_id=tool_call_id,
                        headers=headers,
                        traceparent=headers.get("traceparent"),
                        tracestate=headers.get("tracestate"),
                        trace_id=trace_id,
                        parent_step_id=tool_call_meta.get("step_id"),
                        source_agent_id="worker.watchdog",
                        conn=conn,
                    )
            headers = dict(headers)
            headers["traceparent"] = report_result.traceparent

            target = await resolve_agent_target(
                resource_store=self.resource_store,
                project_id=project_id,
                agent_id=agent_id,
            )
            if not target:
                logger.warning(
                    "Worker watchdog: missing worker_target agent=%s turn=%s",
                    agent_id,
                    agent_turn_id,
                )
                continue
            wakeup_subject = format_subject(
                project_id,
                channel_id,
                "cmd",
                "agent",
                target,
                "wakeup",
            )
            await self.nats.publish_event(
                wakeup_subject,
                {"agent_id": agent_id, "agent_turn_id": agent_turn_id},
                headers=headers,
                retry_count=3,
                retry_delay=1.0,
            )

            head_key = (project_id, agent_id, agent_turn_id, turn_epoch)
            if head_key in cleared_heads:
                continue
            updated = await self.state_store.update(
                project_id=project_id,
                agent_id=agent_id,
                expect_turn_epoch=turn_epoch,
                expect_agent_turn_id=agent_turn_id,
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
                    headers=item.headers,
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
                    updated = await self.execution_store.update_inbox_status(
                        inbox_id=item.inbox_id,
                        project_id=item.project_id,
                        status="consumed",
                        expected_status="processing",
                    )
                    if not updated:
                        logger.warning(
                            "Worker %s failed to consume inbox id=%s",
                            worker_id,
                            item.inbox_id,
                        )
                    else:
                        await self._emit_inbox_progress(
                            project_id=item.project_id,
                            channel_id=item.channel_id,
                            agent_id=item.agent_id,
                            inbox_id=item.inbox_id,
                            status="consumed",
                            reason="worker_consumed",
                        )
                self.queue.task_done()

    @traced(
        _TRACER,
        "agent_worker.handle_item",
        headers_arg="headers",
        include_links=True,
        record_exception=True,
        set_status_on_exception=True,
        attributes_getter=_worker_item_span_attrs,
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
        if self.observability.timing_worker_logs:
            logger.info(
                "Worker %s processing %s agent=%s turn=%s queue_wait_ms=%s inbox_age_ms=%s inbox_id=%s",
                worker_id,
                kind,
                item.agent_id,
                item.agent_turn_id,
                queue_wait_ms,
                inbox_age_ms,
                item.inbox_id,
            )
        else:
            logger.info(
                "Worker %s processing %s agent=%s turn=%s",
                worker_id,
                kind,
                item.agent_id,
                item.agent_turn_id,
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
