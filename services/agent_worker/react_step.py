import asyncio
import json
import logging
import time
import uuid6
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from typing import Any, Dict, List, Optional

from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore, SkillStore
from infra.llm_gateway import LLMWrapper
from core.llm import LLMRequest
from core.errors import ProtocolViolationError
from core.fields_schema import extract_fields_schema
from core.utp_protocol import Card, TextContent
from core.config import PROTOCOL_VERSION, MAX_STEPS, MAX_RECURSION_DEPTH, DEFAULT_TIMEOUT
from core.status import STATUS_FAILED, TOOL_RESULT_STATUS_SET
from core.utils import safe_str
from core.headers import require_recursion_depth
from core.subject import format_subject
from core.utp_protocol import extract_tool_result_payload
from infra.worker_helpers import TurnGuard, publish_idle_wakeup
from infra.event_emitter import emit_agent_state, emit_agent_task
from card_box_core.engine import ContextEngine

from .context_loader import ContextAssembler, ResourceLoader
from .models import ActionOutcome, AgentTurnWorkItem
from .react_context import (
    build_card_metadata,
    build_stream_start_metadata,
    normalize_tool_args,
    sanitize_provider_options,
)
from .react_response import (
    normalize_tool_calls,
    split_submit_result_calls,
)
from .react_outcomes import normalize_action_outcomes
from .react_streaming import (
    build_stream_content_payload,
    build_stream_end_payload,
    build_stream_start_payload,
    build_stream_subject,
    publish_stream_payload,
)
from .react_tool_cards import save_failed_tool_result_card
from .timing import monotonic_ms, to_iso, utc_now
from .tx_utils import call_with_optional_conn, state_store_transaction
from .utp_dispatcher import UTPDispatcher
from .tool_spec import build_tool_specs, filter_allowed_tools
from .builtin_tools import build_submit_result_tool_spec
from .shared import emit_step_event as emit_step_event_helper, resolve_box_id
from .tool_call_policy import apply_step_budget_controls
from .deliverable_utils import append_partial_deliverable
from .helpers.step_recorder import StepRecorder
from .helpers.turn_finisher import TurnFinisher
from infra.observability.otel import get_tracer, traced


logger = logging.getLogger("ReactStep")
_TRACER = get_tracer("services.agent_worker.react_step")


def _normalize_reasoning_content(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if text is None:
                    text = item.get("content")
                if text is not None:
                    parts.append(str(text))
                continue
            text = getattr(item, "text", None)
            if text is None:
                text = getattr(item, "content", None)
            if text is not None:
                parts.append(str(text))
        return "".join(parts) if parts else None
    if isinstance(value, dict):
        text = value.get("text")
        if text is None:
            text = value.get("content")
        if text is not None:
            return str(text)
        return None
    return str(value)


def _llm_span_attrs(args: Dict[str, Any]) -> Dict[str, Any]:
    item = args.get("item")
    profile = args.get("profile")
    step_id = args.get("step_id")
    if item is None:
        return {}
    llm_cfg = getattr(profile, "llm_config", None)
    return {
        "cg.project_id": str(getattr(item, "project_id", "")),
        "cg.agent_id": str(getattr(item, "agent_id", "")),
        "cg.agent_turn_id": str(getattr(item, "agent_turn_id", "")),
        "cg.step_id": str(step_id or ""),
        "cg.llm.stream": bool(getattr(llm_cfg, "stream", False)),
        "cg.llm.model": str(getattr(llm_cfg, "model", "") or ""),
    }


def _tool_span_attrs(args: Dict[str, Any]) -> Dict[str, Any]:
    item = args.get("item")
    tool_call = args.get("tool_call")
    if item is None:
        return {}
    fn = "unknown"
    tool_call_id = "unknown"
    if isinstance(tool_call, dict):
        fn = (tool_call.get("function") or {}).get("name") or "unknown"
        tool_call_id = tool_call.get("id") or "unknown"
    return {
        "cg.project_id": str(getattr(item, "project_id", "")),
        "cg.agent_id": str(getattr(item, "agent_id", "")),
        "cg.agent_turn_id": str(getattr(item, "agent_turn_id", "")),
        "cg.step_id": str(args.get("step_id") or ""),
        "cg.tool.name": str(fn),
        "cg.tool_call_id": str(tool_call_id),
    }


@dataclass
class TurnContext:
    """Per-turn execution context (MUST NOT be stored on self; safe for concurrency)."""

    item: AgentTurnWorkItem
    start_ts: float
    state: Any
    new_card_ids: List[str] = field(default_factory=list)
    depth: Optional[int] = None
    step_id: Optional[str] = None
    profile: Any = None
    tool_suspend_timeouts: Dict[str, float] = field(default_factory=dict)
    result_fields: List[Dict[str, str]] = field(default_factory=list)
    llm_messages: list = field(default_factory=list)
    llm_tools: Any = None
    llm_timing: Dict[str, Any] = field(default_factory=dict)
    resp: Optional[Dict[str, Any]] = None
    thought_card_id: Optional[str] = None


class ReactStepProcessor:
    """Processes a single ReAct step for an agent work item."""

    def __init__(
        self,
        *,
        state_store: StateStore,
        step_store: "StepStore",
        resource_store: ResourceStore,
        resource_loader: ResourceLoader,
        skill_store: SkillStore,
        cardbox: CardBoxClient,
        assembler: ContextAssembler,
        llm_wrapper: LLMWrapper,
        action_handler: UTPDispatcher,
        nats: NATSClient,
        queue: asyncio.Queue[AgentTurnWorkItem],
        execution_store: Optional[ExecutionStore] = None,
        max_steps: int = MAX_STEPS,
        max_recursion_depth: int = MAX_RECURSION_DEPTH,
        suspend_timeout_seconds: float,
        timing_capture: bool = False,
        timing_step_event: bool = False,
        timing_task_event: bool = False,
        timing_stream_metadata: bool = False,
    ) -> None:
        self.state_store = state_store
        self.step_store = step_store
        self.resource_store = resource_store
        self.resource_loader = resource_loader
        self.skill_store = skill_store
        self.cardbox = cardbox
        self.assembler = assembler
        self.llm_wrapper = llm_wrapper
        self.action_handler = action_handler
        self.nats = nats
        self.queue = queue
        self.execution_store = execution_store
        self.max_steps = max_steps
        self.max_recursion_depth = max_recursion_depth
        self.suspend_timeout_seconds = float(suspend_timeout_seconds)
        self.timing_capture = timing_capture
        self.timing_step_event = timing_step_event
        self.timing_task_event = timing_task_event
        self.timing_stream_metadata = timing_stream_metadata
        self.step_recorder = StepRecorder(
            step_store=self.step_store,
            nats=self.nats,
            timing_step_event=self.timing_step_event,
        )
        self.turn_finisher = TurnFinisher(
            state_store=self.state_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            step_recorder=self.step_recorder,
            timing_task_event=self.timing_task_event,
            card_metadata_builder=self._card_metadata,
            logger=logger,
            emit_state_fn=emit_agent_state,
            emit_task_fn=emit_agent_task,
            idle_wakeup_fn=publish_idle_wakeup,
        )
        self._inbox_progress_waiters: Dict[tuple[str, str], set[asyncio.Event]] = {}
        self._inbox_progress_waiters_lock = asyncio.Lock()

    async def _register_inbox_progress_waiter(
        self,
        key: tuple[str, str],
        waiter: asyncio.Event,
    ) -> None:
        async with self._inbox_progress_waiters_lock:
            waiters = self._inbox_progress_waiters.setdefault(key, set())
            waiters.add(waiter)

    async def _unregister_inbox_progress_waiter(
        self,
        key: tuple[str, str],
        waiter: asyncio.Event,
    ) -> None:
        async with self._inbox_progress_waiters_lock:
            waiters = self._inbox_progress_waiters.get(key)
            if not waiters:
                return
            waiters.discard(waiter)
            if not waiters:
                self._inbox_progress_waiters.pop(key, None)

    async def notify_inbox_progress(self, *, project_id: str, agent_id: str) -> None:
        key = (str(project_id), str(agent_id))
        async with self._inbox_progress_waiters_lock:
            waiters = list(self._inbox_progress_waiters.get(key, ()))
        for waiter in waiters:
            waiter.set()

    async def _defer_until_inbox_drains(self, item: AgentTurnWorkItem) -> None:
        subscription = None
        wake_event = asyncio.Event()
        key = (str(item.project_id), str(item.agent_id))
        try:
            max_waits = 5
            wait_cycles = 0
            exclude_inbox_id = item.guard_inbox_id or item.inbox_id
            if not self.execution_store:
                self._enqueue_next_step(item)
                return

            await self._register_inbox_progress_waiter(key, wake_event)
            try:
                subject = format_subject(
                    project_id=item.project_id,
                    channel_id="*",
                    category="evt",
                    component="agent",
                    target=item.agent_id,
                    suffix="inbox_progress",
                    protocol_version=PROTOCOL_VERSION,
                )

                async def _on_inbox_progress(msg) -> None:
                    if msg.subject:
                        wake_event.set()

                subscription = await self.nats.subscribe_core(subject, _on_inbox_progress)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Inbox guard wait event subscription failed agent=%s turn=%s: %s",
                    item.agent_id,
                    item.agent_turn_id,
                    exc,
                )
                subscription = None

            deadline = time.monotonic() + max(1.0, self.suspend_timeout_seconds)
            logger.info(
                "Inbox guard wait start agent=%s turn=%s step=%s",
                item.agent_id,
                item.agent_turn_id,
                item.step_count,
            )
            checks = 0
            while True:
                checks += 1
                try:
                    has_open = await self.execution_store.has_open_inbox(
                        project_id=item.project_id,
                        agent_id=item.agent_id,
                        exclude_inbox_id=exclude_inbox_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Inbox drain check failed: %s", exc)
                    has_open = True
                if not has_open:
                    logger.info(
                        "Inbox guard wait done agent=%s turn=%s checks=%s",
                        item.agent_id,
                        item.agent_turn_id,
                        checks,
                    )
                    self._enqueue_next_step(item)
                    return
                if time.monotonic() >= deadline:
                    wait_cycles += 1
                    if wait_cycles >= max_waits:
                        logger.warning(
                            "Inbox guard wait exceeded agent=%s turn=%s waits=%s; failing turn.",
                            item.agent_id,
                            item.agent_turn_id,
                            wait_cycles,
                        )
                        await self._fail_inbox_guard_wait(
                            item,
                            reason="inbox_guard_wait_exceeded",
                            error_code="inbox_guard_wait_exceeded",
                        )
                        return
                    logger.warning(
                        "Inbox guard wait timeout agent=%s turn=%s checks=%s; continuing wait cycle %s/%s.",
                        item.agent_id,
                        item.agent_turn_id,
                        checks,
                        wait_cycles,
                        max_waits,
                    )
                    deadline = time.monotonic() + max(1.0, self.suspend_timeout_seconds)
                    continue
                if checks == 1 or checks % 5 == 0:
                    logger.info(
                        "Inbox still open; waiting agent=%s turn=%s checks=%s",
                        item.agent_id,
                        item.agent_turn_id,
                        checks,
                    )
                remaining = max(0.05, deadline - time.monotonic())
                if remaining <= 0:
                    continue
                try:
                    await asyncio.wait_for(wake_event.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    continue
                wake_event.clear()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Inbox guard wait crashed agent=%s turn=%s: %s",
                item.agent_id,
                item.agent_turn_id,
                exc,
                exc_info=True,
            )
            try:
                await self._fail_inbox_guard_wait(
                    item,
                    reason="inbox_guard_wait_failed",
                    error_code="inbox_guard_wait_failed",
                )
            except Exception as fail_exc:  # noqa: BLE001
                logger.error(
                    "Inbox guard wait fail handler crashed agent=%s turn=%s: %s",
                    item.agent_id,
                    item.agent_turn_id,
                    fail_exc,
                    exc_info=True,
                )
        finally:
            await self._unregister_inbox_progress_waiter(key, wake_event)
            if subscription:
                try:
                    await subscription.unsubscribe()
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "Inbox guard wait unsubscribe failed agent=%s turn=%s: %s",
                        item.agent_id,
                        item.agent_turn_id,
                        exc,
                    )

    def _enqueue_next_step(self, item: AgentTurnWorkItem) -> None:
        next_item = item.next_step()
        if self.timing_capture:
            enqueued_at = utc_now()
            next_item.enqueued_at = enqueued_at
            next_item.timing = {"queue_enqueued_at": to_iso(enqueued_at)}
        self.queue.put_nowait(next_item)

    async def _enqueue_existing_resume_for_waiting_tools(
        self,
        *,
        item: AgentTurnWorkItem,
        state: Any,
        step_id: str,
        wait_items: List[Dict[str, str]],
    ) -> None:
        lease_owner = (
            f"react-suspend:{item.project_id}:{item.agent_id}:{item.agent_turn_id}:{int(state.turn_epoch)}:{step_id}"
        )
        claimed_rows = await self.state_store.claim_turn_resume_ledgers(
            project_id=item.project_id,
            agent_id=item.agent_id,
            agent_turn_id=item.agent_turn_id,
            turn_epoch=int(state.turn_epoch),
            lease_owner=lease_owner,
            lease_seconds=max(5.0, self.suspend_timeout_seconds),
            limit=100,
        )
        claimed_pairs: set[tuple[str, str]] = set()
        for row in claimed_rows:
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
                continue
            status = safe_str(payload.get("status"))
            if status not in TOOL_RESULT_STATUS_SET:
                status = STATUS_FAILED
            after_execution = safe_str(payload.get("after_execution"))
            if after_execution not in ("suspend", "terminate"):
                after_execution = "suspend"
            claimed_pairs.add((tool_call_id, tool_result_card_id))
            await self.queue.put(
                AgentTurnWorkItem(
                    project_id=item.project_id,
                    channel_id=item.channel_id,
                    agent_turn_id=item.agent_turn_id,
                    agent_id=item.agent_id,
                    profile_box_id=safe_str(state.profile_box_id),
                    context_box_id=safe_str(state.context_box_id),
                    output_box_id=safe_str(state.output_box_id),
                    turn_epoch=state.turn_epoch,
                    headers=dict(item.headers),
                    parent_step_id=item.parent_step_id,
                    trace_id=item.trace_id,
                    step_count=item.step_count,
                    resume_data={
                        "tool_call_id": tool_call_id,
                        "status": status,
                        "after_execution": after_execution,
                        "tool_result_card_id": tool_result_card_id,
                        "step_id": safe_str(payload.get("step_id")) or step_id,
                    },
                    is_continuation=True,
                    inbox_id=None,
                    guard_inbox_id=item.guard_inbox_id or item.inbox_id,
                    enqueued_at=utc_now() if self.timing_capture else None,
                )
            )

        tool_call_ids = [
            safe_str(wait_item.get("tool_call_id"))
            for wait_item in wait_items
            if safe_str(wait_item.get("tool_call_id"))
        ]
        if not tool_call_ids:
            return
        try:
            cards = await self.cardbox.get_cards_by_tool_call_ids(
                project_id=item.project_id,
                tool_call_ids=tool_call_ids,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Suspend reconcile: failed loading tool_result cards turn=%s: %s",
                item.agent_turn_id,
                exc,
            )
            return
        by_call: Dict[str, Any] = {}
        for card in cards or []:
            if getattr(card, "type", None) != "tool.result":
                continue
            tool_call_id = safe_str(getattr(card, "tool_call_id", None))
            if not tool_call_id:
                continue
            current = by_call.get(tool_call_id)
            if current is None or getattr(
                card, "created_at", datetime.max.replace(tzinfo=UTC)
            ) < getattr(current, "created_at", datetime.max.replace(tzinfo=UTC)):
                by_call[tool_call_id] = card

        for wait_item in wait_items:
            tool_call_id = safe_str(wait_item.get("tool_call_id"))
            if not tool_call_id:
                continue
            card = by_call.get(tool_call_id)
            if card is None:
                continue
            card_id = safe_str(getattr(card, "card_id", None))
            if not card_id:
                continue
            if (tool_call_id, card_id) in claimed_pairs:
                continue
            try:
                payload = extract_tool_result_payload(card)
            except Exception:
                payload = {}
            status = payload.get("status")
            if status not in TOOL_RESULT_STATUS_SET:
                status = STATUS_FAILED
            after_execution = payload.get("after_execution")
            if after_execution not in ("suspend", "terminate"):
                after_execution = "suspend"
            await self.state_store.record_turn_resume_ledger(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=int(state.turn_epoch),
                tool_call_id=tool_call_id,
                tool_result_card_id=card_id,
                payload={
                    "tool_call_id": tool_call_id,
                    "status": status,
                    "after_execution": after_execution,
                    "tool_result_card_id": card_id,
                    "step_id": step_id,
                },
            )
            await self.queue.put(
                AgentTurnWorkItem(
                    project_id=item.project_id,
                    channel_id=item.channel_id,
                    agent_turn_id=item.agent_turn_id,
                    agent_id=item.agent_id,
                    profile_box_id=safe_str(state.profile_box_id),
                    context_box_id=safe_str(state.context_box_id),
                    output_box_id=safe_str(state.output_box_id),
                    turn_epoch=state.turn_epoch,
                    headers=dict(item.headers),
                    parent_step_id=item.parent_step_id,
                    trace_id=item.trace_id,
                    step_count=item.step_count,
                    resume_data={
                        "tool_call_id": tool_call_id,
                        "status": status,
                        "after_execution": after_execution,
                        "tool_result_card_id": card_id,
                        "step_id": step_id,
                    },
                    is_continuation=True,
                    inbox_id=None,
                    guard_inbox_id=item.guard_inbox_id or item.inbox_id,
                    enqueued_at=utc_now() if self.timing_capture else None,
                )
            )

    async def _fail_inbox_guard_wait(
        self,
        item: AgentTurnWorkItem,
        *,
        reason: str,
        error_code: str = "inbox_guard_wait_exceeded",
    ) -> None:
        state = await self.state_store.fetch(item.project_id, item.agent_id)
        if not state:
            logger.warning("Inbox guard wait fail skipped: missing state agent=%s", item.agent_id)
            return
        guard = TurnGuard(agent_turn_id=item.agent_turn_id, turn_epoch=item.turn_epoch)
        if not guard.matches(state):
            logger.warning(
                "Inbox guard wait fail skipped: guard mismatch (%s).",
                guard.mismatch_detail(state),
            )
            return

        if getattr(state, "parent_step_id", None):
            item.parent_step_id = safe_str(state.parent_step_id)
        state_trace_id = getattr(state, "trace_id", None)
        if state_trace_id:
            item.trace_id = safe_str(state_trace_id)
        if item.output_box_id is None and state.active_agent_turn_id == item.agent_turn_id:
            item.output_box_id = state.output_box_id
        item.output_box_id = await self.cardbox.ensure_box_id(
            project_id=item.project_id,
            box_id=item.output_box_id,
        )

        step_id = f"step_guard_{uuid6.uuid7().hex}"
        if self.timing_capture:
            item.timing = dict(item.timing or {})
            item.timing.pop("llm", None)
            item.timing["step_id"] = step_id
            item.timing["step_started_at"] = to_iso(utc_now())
        await self.step_recorder.start_step(
            item=item,
            step_id=step_id,
            status="started",
            profile_box_id=getattr(state, "profile_box_id", None),
            context_box_id=getattr(state, "context_box_id", None),
            output_box_id=item.output_box_id,
        )
        await self._fail_turn(
            item,
            step_id,
            reason,
            error_code=error_code,
        )

    @staticmethod
    def _normalize_tool_args(tool_call: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return normalize_tool_args(tool_call)

    @staticmethod
    def _describe_exception(exc: Exception) -> str:
        parts: List[str] = [f"{type(exc).__name__}: {exc}"]
        cause = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
        if cause and cause is not exc:
            parts.append(f"cause={type(cause).__name__}: {cause}")
        last_exc: Optional[BaseException] = None
        if hasattr(exc, "last_attempt"):
            try:
                last_attempt = exc.last_attempt
                if hasattr(last_attempt, "exception"):
                    last_exc = last_attempt.exception()
                elif hasattr(last_attempt, "outcome") and hasattr(last_attempt.outcome, "exception"):
                    last_exc = last_attempt.outcome.exception()
            except Exception:  # noqa: BLE001
                last_exc = None
        if last_exc is None and hasattr(exc, "last_exception"):
            try:
                last_exc = exc.last_exception
            except Exception:  # noqa: BLE001
                last_exc = None
        if last_exc and last_exc is not exc and last_exc is not cause:
            parts.append(f"last_exception={type(last_exc).__name__}: {last_exc}")
        return " | ".join(p for p in parts if p)

    async def _build_available_skills_text(self, project_id: str) -> str:
        skills = await self.skill_store.list_skills(project_id=project_id, limit=200, offset=0)
        entries: List[str] = []
        for skill in skills or []:
            if not skill.get("enabled", True):
                continue
            name = str(skill.get("skill_name") or "").strip()
            desc = str(skill.get("description") or "").strip()
            if not name or not desc:
                continue
            entries.append(
                "  <skill>\n"
                f"    <name>{name}</name>\n"
                f"    <description>{desc}</description>\n"
                f"    <location>skill://{project_id}/{name}</location>\n"
                "  </skill>"
            )
        if not entries:
            return ""
        return "<available_skills>\n" + "\n".join(entries) + "\n</available_skills>"

    async def _build_uploaded_files_text(
        self,
        project_id: str,
        context_card_ids: List[str],
    ) -> str:
        if not context_card_ids:
            return ""
        try:
            cards = await self.cardbox.get_cards(context_card_ids, project_id=project_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load context cards for uploads: %s", exc)
            return ""
        lines: List[str] = []
        seen: set[tuple[str, str, str]] = set()
        for card in cards:
            meta = getattr(card, "metadata", None) or {}
            uploaded = meta.get("uploaded_files")
            if not isinstance(uploaded, list):
                continue
            for entry in uploaded:
                if not isinstance(entry, dict):
                    continue
                artifact_id = safe_str(entry.get("artifact_id") or entry.get("artifactId"))
                filename = safe_str(entry.get("filename"))
                mount_path = safe_str(entry.get("mount_path") or entry.get("mountPath"))
                if not mount_path and filename:
                    mount_path = f"inputs/{filename}"
                key = (artifact_id, mount_path, filename)
                if key in seen:
                    continue
                seen.add(key)
                parts = []
                if filename:
                    parts.append(filename)
                if artifact_id:
                    parts.append(f"artifact_id: {artifact_id}")
                if mount_path:
                    parts.append(f"mount_path: {mount_path}")
                if parts:
                    lines.append("- " + " ".join(parts))
        if not lines:
            return ""
        return "Uploaded Files:\n" + "\n".join(lines)

    @traced(
        _TRACER,
        "agent.tool.execute",
        record_exception=True,
        set_status_on_exception=True,
        attributes_getter=_tool_span_attrs,
    )
    async def _execute_tool_parallel(
        self,
        *,
        tool_call: Dict[str, Any],
        item: AgentTurnWorkItem,
        state_epoch: int,
        step_id: str,
    ) -> ActionOutcome:
        fn = (tool_call.get("function") or {}).get("name") or "unknown"
        logger.info("Executing tool call: %s", fn)
        tool_call_id = tool_call.get("id")
        if not tool_call_id:
            tool_call_id = f"tc_{uuid6.uuid7().hex}"
            tool_call["id"] = tool_call_id

        dispatch_ctx = item.to_cg_context(step_id=step_id)
        return await self.action_handler.execute(
            tool_call=tool_call,
            ctx=dispatch_ctx,
            turn_epoch=state_epoch,
            context_box_id=item.context_box_id,
            inbox_id=item.inbox_id,
            guard_inbox_id=item.guard_inbox_id,
        )

    @traced(
        _TRACER,
        "agent.llm.chat",
        record_exception=True,
        set_status_on_exception=True,
        attributes_getter=_llm_span_attrs,
    )
    async def _chat_llm(
        self,
        *,
        item: AgentTurnWorkItem,
        step_id: str,
        profile: Any,
        request: LLMRequest,
        on_chunk: Any,
    ) -> Dict[str, Any]:
        logger.info("Sending request to LLM...")
        if self.timing_step_event and item.timing:
            await emit_step_event_helper(
                nats=self.nats,
                item=item,
                step_id=step_id,
                phase="executing",
                timing_enabled=self.timing_step_event,
            )
        return await self.llm_wrapper.chat(request, on_chunk=on_chunk)

    async def _guard_fail_turn(
        self,
        item: AgentTurnWorkItem,
        *,
        state: Any,
        reason: str,
        error_code: Optional[str],
        new_card_ids: List[str],
    ) -> None:
        step_id = f"step_{uuid6.uuid7().hex}"
        if self.timing_capture:
            item.timing = dict(item.timing or {})
            item.timing.pop("llm", None)
            item.timing["step_id"] = step_id
            item.timing["step_started_at"] = to_iso(utc_now())
        profile_box_id = item.profile_box_id or state.profile_box_id
        profile_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=profile_box_id,
            project_id=item.project_id,
        )
        context_box_id = item.context_box_id or state.context_box_id
        context_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=context_box_id,
            project_id=item.project_id,
        )

        box_id = item.output_box_id or safe_str(state.output_box_id)
        item.output_box_id = await self.cardbox.ensure_box_id(
            project_id=item.project_id,
            box_id=box_id,
        )

        await self.step_recorder.start_step(
            item=item,
            step_id=step_id,
            status="started",
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=item.output_box_id,
        )
        deliverable_card_id = await self._append_partial_deliverable(
            item,
            step_id,
            reason=reason,
        )
        await self._fail_turn(
            item,
            step_id,
            reason,
            error_code=error_code,
            deliverable_card_id=deliverable_card_id,
            new_card_ids=new_card_ids,
        )

    def _card_metadata(
        self,
        item: AgentTurnWorkItem,
        *,
        step_id: str,
        role: str,
        tool_call_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return build_card_metadata(
            item=item,
            step_id=step_id,
            role=role,
            tool_call_id=tool_call_id,
            extra=extra,
        )

    @staticmethod
    def _sanitize_provider_options(provider_options: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return sanitize_provider_options(provider_options)

    def _build_stream_start_metadata(self, item: AgentTurnWorkItem, profile: "ProfileData") -> Dict[str, Any]:
        return build_stream_start_metadata(item=item, profile=profile)

    async def _fetch_state_guarded(self, item: AgentTurnWorkItem) -> Optional[Any]:
        state = await self.state_store.fetch(item.project_id, item.agent_id)
        if not state:
            logger.warning("No state for %s, drop", item.agent_id)
            return None
        if item.turn_epoch is None:
            logger.warning("Reject turn (missing epoch): cmd(turn=%s)", item.agent_turn_id)
            return None
        guard = TurnGuard(agent_turn_id=item.agent_turn_id, turn_epoch=item.turn_epoch)
        if not guard.matches(state):
            logger.warning(
                "Reject turn (id/epoch mismatch): %s",
                guard.mismatch_detail(state),
            )
            return None

        if item.is_continuation:
            if state.status != "running":
                logger.warning(
                    "Reject continuation (status=%s) turn=%s epoch=%s",
                    state.status,
                    item.agent_turn_id,
                    item.turn_epoch,
                )
                return None
        else:
            if state.status != "dispatched":
                logger.warning(
                    "Reject first-turn (status=%s) turn=%s epoch=%s",
                    state.status,
                    item.agent_turn_id,
                    item.turn_epoch,
                )
                return None
        return state

    async def _guard_depth_and_steps(self, ctx: TurnContext) -> bool:
        try:
            depth = require_recursion_depth(ctx.item.headers)
        except ProtocolViolationError as exc:
            await self._guard_fail_turn(
                ctx.item,
                state=ctx.state,
                reason=str(exc),
                error_code="protocol_violation",
                new_card_ids=ctx.new_card_ids,
            )
            return False
        if depth >= self.max_recursion_depth:
            await self._guard_fail_turn(
                ctx.item,
                state=ctx.state,
                reason=f"recursion_depth_exceeded (depth={depth}, max={self.max_recursion_depth})",
                error_code="recursion_depth_exceeded",
                new_card_ids=ctx.new_card_ids,
            )
            return False

        # Hard guard: enforce max steps across suspend/resume cycles.
        if ctx.item.step_count >= self.max_steps:
            await self._guard_fail_turn(
                ctx.item,
                state=ctx.state,
                reason=f"max_steps_reached (max_steps={self.max_steps})",
                error_code=None,
                new_card_ids=ctx.new_card_ids,
            )
            return False

        ctx.depth = depth
        return True

    async def _start_step_and_enter_running(self, ctx: TurnContext) -> bool:
        step_id = f"step_{uuid6.uuid7().hex}"
        ctx.step_id = step_id
        if self.timing_capture:
            ctx.item.timing = dict(ctx.item.timing or {})
            ctx.item.timing.pop("llm", None)
            ctx.item.timing["step_id"] = step_id
            ctx.item.timing["step_started_at"] = to_iso(utc_now())

        # Capture profile/context box IDs (state as authority when missing)
        profile_box_id = ctx.item.profile_box_id or ctx.state.profile_box_id
        profile_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=profile_box_id,
            project_id=ctx.item.project_id,
        )
        context_box_id = ctx.item.context_box_id or ctx.state.context_box_id
        context_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=context_box_id,
            project_id=ctx.item.project_id,
        )

        # Ensure output box exists
        if ctx.item.output_box_id is None and ctx.state.active_agent_turn_id == ctx.item.agent_turn_id:
            ctx.item.output_box_id = ctx.state.output_box_id

        ctx.item.output_box_id = await self.cardbox.ensure_box_id(
            project_id=ctx.item.project_id,
            box_id=ctx.item.output_box_id,
        )

        await self.step_recorder.start_step(
            item=ctx.item,
            step_id=step_id,
            status="started",
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=ctx.item.output_box_id,
        )

        updated = await self.state_store.update(
            project_id=ctx.item.project_id,
            agent_id=ctx.item.agent_id,
            expect_turn_epoch=ctx.state.turn_epoch,
            expect_agent_turn_id=ctx.item.agent_turn_id,
            new_status="running",
            active_recursion_depth=int(ctx.depth or 0),
            context_box_id=ctx.item.context_box_id,
            output_box_id=ctx.item.output_box_id,
        )
        if not updated:
            fresh_state = await self.state_store.fetch(ctx.item.project_id, ctx.item.agent_id)
            if not (
                fresh_state
                and fresh_state.status == "running"
                and fresh_state.active_agent_turn_id == ctx.item.agent_turn_id
                and fresh_state.turn_epoch == ctx.item.turn_epoch
            ):
                logger.warning("CAS failed when entering running")
                return False
        await self._emit_state_event(
            ctx.item,
            status="running",
            output_box_id=ctx.item.output_box_id,
            metadata={"activity": "thinking"},
        )
        return True

    async def _load_profile_or_fail(self, ctx: TurnContext) -> bool:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before loading profile")
        profile = await self.resource_loader.load_profile(
            ctx.item.project_id, ctx.item.agent_id, ctx.item.profile_box_id
        )
        if not profile:
            await self._fail_turn(
                ctx.item, str(ctx.step_id), "profile_not_found", new_card_ids=ctx.new_card_ids
            )
            return False
        logger.info("Loaded profile box=%s cards_ok=True", ctx.item.profile_box_id)
        ctx.profile = profile
        return True

    async def _load_context_card_ids(self, item: AgentTurnWorkItem) -> List[str]:
        context_ids: List[str] = []
        if item.context_box_id:
            ctx_box = await self.cardbox.get_box(item.context_box_id, project_id=item.project_id)
            if ctx_box:
                context_ids = list(ctx_box.card_ids or [])
                logger.info("Loaded %d cards from context_box=%s", len(context_ids), item.context_box_id)

        if item.output_box_id and item.output_box_id != item.context_box_id:
            logger.info("Loading additional context from output_box=%s", item.output_box_id)
            out_box = await self.cardbox.get_box(item.output_box_id, project_id=item.project_id)
            if out_box and out_box.card_ids:
                existing = set(context_ids)
                added_count = 0
                for cid in out_box.card_ids:
                    if cid not in existing:
                        context_ids.append(cid)
                        existing.add(cid)
                        added_count += 1
                logger.info("Added %d new cards from output_box", added_count)

        logger.info("Final Context Card IDs: %s", context_ids)
        return context_ids

    def _apply_step_budget_controls(
        self,
        *,
        item: AgentTurnWorkItem,
        messages: Any,
        tools: Any,
    ) -> tuple[List[Dict[str, Any]], Any]:
        return apply_step_budget_controls(
            item=item,
            messages=messages,
            tools=tools,
            max_steps=self.max_steps,
            logger=logger,
        )

    async def _prepare_llm_request(self, ctx: TurnContext, *, context_card_ids: List[str]) -> bool:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before preparing LLM request")
        if ctx.profile is None:
            raise RuntimeError("ctx.profile must be set before preparing LLM request")
        item = ctx.item
        step_id = str(ctx.step_id)
        profile = ctx.profile
        tool_defs = await self.resource_store.fetch_tools_for_project_model(item.project_id)
        tool_defs = filter_allowed_tools(
            tool_defs,
            allowed_tools=profile.allowed_tools,
            project_id=item.project_id,
            agent_id=item.agent_id,
        )
        tool_suspend_timeouts: Dict[str, float] = {}
        for tool_def in tool_defs:
            tool_name = getattr(tool_def, "tool_name", None)
            if not tool_name:
                continue
            options = getattr(tool_def, "options", None)
            if not isinstance(options, dict):
                continue
            raw_timeout = options.get("suspend_timeout_seconds")
            if raw_timeout is None:
                raw_timeout = options.get("timeout_seconds")
            if raw_timeout is None:
                continue
            try:
                timeout_value = float(raw_timeout)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid suspend_timeout_seconds for tool=%s: %r",
                    tool_name,
                    raw_timeout,
                )
                continue
            if timeout_value > 0:
                tool_suspend_timeouts[str(tool_name)] = timeout_value
        tool_specs = build_tool_specs(tool_defs)
        ctx.tool_suspend_timeouts = tool_suspend_timeouts

        # Inject builtin tool: submit_result (controlled by profile.allowed_internal_tools).
        result_fields: List[Dict[str, str]] = []
        should_add_submit_result = "submit_result" in (profile.allowed_internal_tools or [])
        if should_add_submit_result:
            if item.context_box_id:
                try:
                    ctx_box = await self.cardbox.get_box(item.context_box_id, project_id=item.project_id)
                    if ctx_box and ctx_box.card_ids:
                        ctx_cards = await self.cardbox.get_cards(list(ctx_box.card_ids), project_id=item.project_id)
                        for c in reversed(ctx_cards):
                            if getattr(c, "type", "") == "task.result_fields":
                                try:
                                    fields_schema = extract_fields_schema(c)
                                except ProtocolViolationError as exc:
                                    logger.warning("task.result_fields invalid: %s", exc)
                                    break
                                result_fields = [
                                    {"name": f.name, "description": f.description}
                                    for f in fields_schema
                                    if f.name.strip()
                                ]
                                break
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to build submit_result tool spec from task.result_fields: %s", exc)
            tool_specs.append(build_submit_result_tool_spec(required_fields=result_fields))
        ctx.result_fields = result_fields

        # Assemble Request Box (formerly virtual_box)
        # ContextAssembler stitches Profile + Context + Tools into a single snapshot.
        skills_text = await self._build_available_skills_text(item.project_id)
        uploaded_files_text = await self._build_uploaded_files_text(item.project_id, context_card_ids)
        system_append_parts = [part for part in (skills_text, uploaded_files_text) if part]
        system_append = "\n\n".join(system_append_parts) if system_append_parts else ""
        request_box = await self.assembler.assemble(
            cardbox=self.cardbox,
            profile=profile,
            project_id=item.project_id,
            context_card_ids=context_card_ids,
            tool_specs=tool_specs,
            system_append=system_append,
        )

        # Use ContextEngine.to_api (inject tool stderr/stdout into tool messages)
        tool_debug: Dict[str, Dict[str, str]] = {}
        if request_box.card_ids:
            try:
                req_cards = await self.cardbox.get_cards(
                    list(request_box.card_ids), project_id=item.project_id
                )
                for c in req_cards:
                    if getattr(c, "type", "") != "tool.result":
                        continue
                    try:
                        payload = extract_tool_result_payload(c)
                    except ProtocolViolationError:
                        continue
                    result = payload.get("result")
                    if not isinstance(result, dict):
                        continue
                    stderr = result.get("stderr")
                    stdout = result.get("stdout")
                    if stderr or stdout:
                        tool_debug[str(getattr(c, "tool_call_id", "") or "")] = {
                            "stderr": str(stderr) if stderr is not None else "",
                            "stdout": str(stdout) if stdout is not None else "",
                        }
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to collect tool stderr/stdout: %s", exc)

        def _truncate_text(text: str, limit: int = 4000) -> str:
            if len(text) <= limit:
                return text
            return text[:limit] + "...(truncated)"

        def _tool_result_modifier(api_request: Dict[str, Any]) -> Dict[str, Any]:
            messages = api_request.get("messages")
            if not isinstance(messages, list) or not tool_debug:
                return api_request
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                if msg.get("role") != "tool":
                    continue
                call_id = msg.get("tool_call_id")
                if not call_id or call_id not in tool_debug:
                    continue
                extra = tool_debug.get(call_id) or {}
                if not extra:
                    continue
                content = msg.get("content")
                data = None
                if isinstance(content, str) and content.strip():
                    try:
                        data = json.loads(content)
                    except Exception:
                        data = None
                elif isinstance(content, dict):
                    data = dict(content)
                if not isinstance(data, dict):
                    continue
                stderr = extra.get("stderr")
                stdout = extra.get("stdout")
                if stderr:
                    data["stderr"] = _truncate_text(stderr)
                if stdout:
                    data["stdout"] = _truncate_text(stdout)
                msg["content"] = json.dumps(data, ensure_ascii=False)
            return api_request

        ctx_engine = ContextEngine(
            trace_id=item.trace_id or item.agent_turn_id,
            tenant_id=item.project_id,
            storage_adapter=self.cardbox.adapter,
        )
        api_req, _ = await ctx_engine.to_api(request_box, modifier=_tool_result_modifier)

        request_box_id = await self.cardbox.save_box(
            list(request_box.card_ids),
            project_id=item.project_id,
        )
        await self.step_recorder.set_step_status(
            item=item,
            step_id=step_id,
            status="llm_ready",
            update_fields={"request_box_id": request_box_id},
        )

        messages = api_req.get("messages", [])
        tools = api_req.get("tools")  # Optional
        messages, tools = self._apply_step_budget_controls(
            item=item,
            messages=messages,
            tools=tools,
        )
        ctx.llm_messages = messages
        ctx.llm_tools = tools

        logger.info(
            "LLM Request prepared: %s messages, %s tools available",
            len(messages),
            len(tools) if tools else 0,
        )

        return True

    async def _call_llm_or_fail(
        self,
        *,
        ctx: TurnContext,
    ) -> bool:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before calling LLM")
        if ctx.profile is None:
            raise RuntimeError("ctx.profile must be set before calling LLM")
        item = ctx.item
        step_id = str(ctx.step_id)
        profile = ctx.profile
        messages = ctx.llm_messages
        tools = ctx.llm_tools

        stream_subject = build_stream_subject(
            project_id=item.project_id,
            channel_id=item.channel_id,
            agent_id=item.agent_id,
        )

        async def _publish_stream(payload) -> None:
            await publish_stream_payload(
                nats=self.nats,
                subject=stream_subject,
                payload=payload,
                logger=logger,
            )

        llm_started_at = utc_now()
        llm_start_ts = time.perf_counter()
        llm_first_token_ts: Optional[float] = None
        llm_timing: Dict[str, Any] = {}
        if self.timing_capture:
            item.timing = dict(item.timing or {})
            llm_timing = item.timing.setdefault("llm", {})
            llm_timing["request_started_at"] = to_iso(llm_started_at)

        if profile.llm_config.stream:
            start_meta = self._build_stream_start_metadata(item, profile)
            if self.timing_stream_metadata:
                start_meta["llm_request_started_at"] = to_iso(llm_started_at)
            start_payload = build_stream_start_payload(
                agent_turn_id=item.agent_turn_id,
                step_id=step_id,
                metadata=start_meta,
            )
            await _publish_stream(start_payload)

        async def on_chunk(text: str) -> None:
            nonlocal llm_first_token_ts
            if llm_first_token_ts is None:
                llm_first_token_ts = time.perf_counter()
                if self.timing_capture:
                    llm_timing["first_token_at"] = to_iso(utc_now())
                    llm_timing["first_token_ms"] = monotonic_ms(llm_start_ts, llm_first_token_ts)
            payload = build_stream_content_payload(
                agent_turn_id=item.agent_turn_id,
                step_id=step_id,
                content=text,
            )
            await _publish_stream(payload)

        tool_choice = profile.llm_config.tool_choice if profile.llm_config else None
        if tool_choice:
            request = LLMRequest(messages=messages, tools=tools, tool_choice=tool_choice, config=profile.llm_config)
        else:
            request = LLMRequest(messages=messages, tools=tools, config=profile.llm_config)

        try:
            resp = await self._chat_llm(
                item=item,
                step_id=step_id,
                profile=profile,
                request=request,
                on_chunk=on_chunk,
            )
        except Exception as exc:  # noqa: BLE001
            llm_failed_at = utc_now()
            if self.timing_capture:
                llm_timing["error_at"] = to_iso(llm_failed_at)
                llm_timing["duration_ms"] = monotonic_ms(llm_start_ts)
            logger.error("LLM call failed: %s", self._describe_exception(exc))
            await self._fail_turn(item, step_id, str(exc), new_card_ids=ctx.new_card_ids)
            return False

        llm_finished_at = utc_now()
        llm_duration_ms = monotonic_ms(llm_start_ts)
        if self.timing_capture:
            llm_timing["response_received_at"] = to_iso(llm_finished_at)
            llm_timing["duration_ms"] = llm_duration_ms
        logger.info(
            "LLM responded: content_len=%s tool_calls=%s duration_ms=%s first_token_ms=%s",
            len(resp.get("content", "") or ""),
            len(resp.get("tool_calls") or []),
            llm_duration_ms,
            llm_timing.get("first_token_ms") if self.timing_capture else None,
        )
        step_metadata: Dict[str, Any] = {}
        if isinstance(resp, dict):
            usage = resp.get("usage")
            if usage is not None:
                step_metadata["llm_usage"] = usage
            response_cost = resp.get("response_cost")
            if response_cost is not None:
                step_metadata["llm_response_cost"] = response_cost
        if profile.llm_config.stream:
            end_payload = build_stream_end_payload(
                agent_turn_id=item.agent_turn_id,
                step_id=step_id,
                usage=resp.get("usage") if isinstance(resp, dict) else None,
                response_cost=resp.get("response_cost") if isinstance(resp, dict) else None,
                include_timing_metadata=self.timing_stream_metadata,
                llm_timing=llm_timing,
            )
            await _publish_stream(end_payload)
        update_fields: Dict[str, Any] = {}
        if step_metadata:
            update_fields["metadata"] = step_metadata
        await self.step_recorder.set_step_status(
            item=item,
            step_id=step_id,
            status="llm_done",
            update_fields=update_fields or None,
        )

        ctx.llm_timing = llm_timing
        ctx.resp = resp
        return True

    async def _persist_thought_card(
        self, ctx: TurnContext
    ) -> List[Dict[str, Any]]:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before persisting thought card")
        if ctx.resp is None:
            raise RuntimeError("ctx.resp must be set before persisting thought card")
        item = ctx.item
        step_id = str(ctx.step_id)
        resp = ctx.resp
        tool_calls = normalize_tool_calls(resp.get("tool_calls") or [])
        if tool_calls:
            resp["tool_calls"] = tool_calls
        seen_tool_call_ids: set[str] = set()
        tool_call_ids: List[str] = []
        for tool_call in tool_calls:
            tool_call_id = safe_str(tool_call.get("id"))
            if not tool_call_id or tool_call_id in seen_tool_call_ids:
                continue
            seen_tool_call_ids.add(tool_call_id)
            tool_call_ids.append(tool_call_id)
        await self.step_recorder.set_step_status(
            item=item,
            step_id=step_id,
            update_fields={"tool_call_ids": tool_call_ids},
        )
        extra_metadata: Optional[Dict[str, Any]] = None
        profile = getattr(ctx, "profile", None)
        llm_config = getattr(profile, "llm_config", None) if profile is not None else None
        save_reasoning_content = getattr(llm_config, "save_reasoning_content", False) if llm_config is not None else False
        persist_reasoning = bool(save_reasoning_content)
        if persist_reasoning:
            reasoning_content = _normalize_reasoning_content(resp.get("reasoning_content"))
            if reasoning_content:
                extra_metadata = {"reasoning_content": reasoning_content}

        thought_card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=item.project_id,
            type="agent.thought",
            content=TextContent(text=resp.get("content", "") or ""),
            created_at=datetime.now(UTC),
            author_id=item.agent_id,
            metadata=self._card_metadata(item, step_id=step_id, role="assistant", extra=extra_metadata),
            tool_calls=resp.get("tool_calls"),
        )
        await self.cardbox.save_card(thought_card)
        ctx.new_card_ids.append(thought_card.card_id)
        ctx.thought_card_id = thought_card.card_id
        item.output_box_id = await self.cardbox.append_to_box(
            item.output_box_id,
            [thought_card.card_id],
            project_id=item.project_id,
        )
        return tool_calls

    async def _handle_tool_calls(
        self, ctx: TurnContext, tool_calls: List[Dict[str, Any]]
    ) -> None:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before handling tool calls")
        if ctx.thought_card_id is None:
            raise RuntimeError("ctx.thought_card_id must be set before handling tool calls")
        item = ctx.item
        state = ctx.state
        step_id = str(ctx.step_id)
        tool_suspend_timeouts = ctx.tool_suspend_timeouts
        new_card_ids = ctx.new_card_ids
        start_ts = ctx.start_ts
        submit_calls, non_submit_calls = split_submit_result_calls(tool_calls)
        if submit_calls and len(tool_calls) != 1:
            # Hard rule: submit_result must be called alone; do not execute any other tools to avoid side effects.
            tc = submit_calls[0]
            tool_call_id = tc.get("id") or f"tc_{uuid6.uuid7().hex}"
            error_card = await save_failed_tool_result_card(
                cardbox=self.cardbox,
                ctx=item.to_cg_context(step_id=step_id),
                turn_epoch=state.turn_epoch,
                tool_call_id=str(tool_call_id),
                function_name="submit_result",
                error="submit_result must be called alone (this turn may only include submit_result).",
            )
            new_card_ids.append(error_card.card_id)
            item.output_box_id = await self.cardbox.append_to_box(
                item.output_box_id, [error_card.card_id], project_id=item.project_id
            )
            if item.step_count + 1 < self.max_steps:
                await self.step_recorder.complete_step(
                    item=item,
                    step_id=step_id,
                    new_card_ids=new_card_ids,
                )
                self._enqueue_next_step(item)
            else:
                await self._fail_turn(item, step_id, "max_steps_reached", new_card_ids=new_card_ids)

            return

        # Pre-mark running BEFORE dispatching the external tool command to avoid
        # a race where RESUME arrives before the state is observable as active.
        if non_submit_calls:
            current_tool = (non_submit_calls[0].get("function") or {}).get("name") or "unknown"
            marked = await self.state_store.update(
                project_id=item.project_id,
                agent_id=item.agent_id,
                expect_turn_epoch=state.turn_epoch,
                expect_agent_turn_id=item.agent_turn_id,
                new_status="running",
            )
            if not marked:
                fresh_state = await self.state_store.fetch(item.project_id, item.agent_id)
                if not (
                    fresh_state
                    and fresh_state.active_agent_turn_id == item.agent_turn_id
                    and fresh_state.turn_epoch == item.turn_epoch
                    and fresh_state.status == "running"
                ):
                    logger.warning(
                        "CAS failed when pre-marking running turn=%s epoch=%s; stop side effects",
                        item.agent_turn_id,
                        item.turn_epoch,
                    )
                    return
            await self._emit_state_event(
                item,
                status="running",
                output_box_id=item.output_box_id,
                metadata={"activity": "executing_tool", "current_tool": current_tool},
            )

        suspend = False
        finished = False
        deliverable_ids: List[str] = []
        blocked_by_inbox = False

        indexed_calls = list(enumerate(tool_calls))
        logger.info(
            "Dispatching tool calls (total=%s, parallel=%s)",
            len(tool_calls),
            len(indexed_calls),
        )

        outcomes_by_index: List[Any] = [None] * len(tool_calls)
        if indexed_calls:
            parallel_outcomes = await asyncio.gather(
                *[
                    self._execute_tool_parallel(
                        tool_call=tc,
                        item=item,
                        state_epoch=state.turn_epoch,
                        step_id=step_id,
                    )
                    for _, tc in indexed_calls
                ],
                return_exceptions=True,
            )
            for (idx, _), outcome in zip(indexed_calls, parallel_outcomes):
                outcomes_by_index[idx] = outcome

        outcomes = outcomes_by_index
        ordered_outcomes, error_count = await normalize_action_outcomes(
            cardbox=self.cardbox,
            ctx=item.to_cg_context(step_id=step_id),
            turn_epoch=state.turn_epoch,
            tool_calls=tool_calls,
            outcomes=outcomes,
            logger=logger,
        )

        suspend_count = sum(1 for outcome in ordered_outcomes if outcome.suspend)
        logger.info(
            "Parallel tool calls finished: total=%s results=%s errors=%s suspends=%s",
            len(tool_calls),
            len(ordered_outcomes),
            error_count,
            suspend_count,
        )
        suspend_wait_items: List[Dict[str, str]] = []
        for tool_call, outcome in zip(tool_calls, ordered_outcomes):
            if not outcome.suspend:
                continue
            tool_call_id = tool_call.get("id")
            if tool_call_id:
                function = tool_call.get("function") or {}
                tool_name = safe_str(function.get("name")) or "unknown"
                suspend_wait_items.append(
                    {
                        "tool_call_id": str(tool_call_id),
                        "tool_name": tool_name,
                        "step_id": str(step_id),
                    }
                )

        for outcome in ordered_outcomes:
            if outcome.cards:
                card_ids = [c.card_id for c in outcome.cards]
                new_card_ids.extend(card_ids)
                item.output_box_id = await self.cardbox.append_to_box(
                    item.output_box_id,
                    card_ids,
                    project_id=item.project_id,
                )
                deliverable_ids.extend([c.card_id for c in outcome.cards if c.type == "task.deliverable"])
                for card in outcome.cards:
                    if getattr(card, "type", "") != "tool.result":
                        continue
                    try:
                        payload = extract_tool_result_payload(card)
                    except ProtocolViolationError:
                        continue
                    error = payload.get("error")
                    if isinstance(error, dict):
                        code = safe_str(error.get("code"))
                        if code in ("inbox_not_empty", "inbox_check_failed"):
                            blocked_by_inbox = True
            suspend = suspend or outcome.suspend
            finished = finished or outcome.mark_complete

        if finished:
            logger.info("Turn finished via tool deliverable, completing.")
            await self._complete_turn(
                item,
                step_id,
                start_ts,
                deliverable_card_id=(deliverable_ids[0] if deliverable_ids else None),
                new_card_ids=new_card_ids,
            )
        elif suspend:
            logger.info(
                "Turn suspended waiting for %s tool callbacks (total=%s).",
                suspend_count,
                len(tool_calls),
            )
            dedup_wait_items: List[Dict[str, str]] = []
            seen_wait_ids: set[str] = set()
            for wait_item in suspend_wait_items:
                tool_call_id = safe_str(wait_item.get("tool_call_id"))
                if not tool_call_id or tool_call_id in seen_wait_ids:
                    continue
                seen_wait_ids.add(tool_call_id)
                dedup_wait_items.append(wait_item)
            waiting_tool_count = len(dedup_wait_items)
            waiting_tool_call_ids = [safe_str(item.get("tool_call_id")) for item in dedup_wait_items]
            suspend_timeout_seconds = self.suspend_timeout_seconds
            if tool_suspend_timeouts and tool_calls:
                for tool_call, outcome in zip(tool_calls, ordered_outcomes):
                    if not outcome.suspend:
                        continue
                    function = tool_call.get("function") or {}
                    tool_name = function.get("name")
                    if tool_name and tool_name in tool_suspend_timeouts:
                        suspend_timeout_seconds = max(
                            suspend_timeout_seconds,
                            tool_suspend_timeouts[tool_name],
                        )
            resume_deadline = (
                datetime.now(UTC) + timedelta(seconds=suspend_timeout_seconds)
                if waiting_tool_count > 0
                else None
            )
            updated = False
            async with state_store_transaction(self.state_store) as tx:
                updated = await call_with_optional_conn(
                    self.state_store.update,
                    project_id=item.project_id,
                    agent_id=item.agent_id,
                    expect_turn_epoch=state.turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    expect_status="running",
                    new_status="suspended",
                    expecting_correlation_id=None,
                    waiting_tool_count=waiting_tool_count,
                    resume_deadline=resume_deadline,
                    conn=tx.conn,
                )
                if updated:
                    await self.state_store.replace_turn_waiting_tools(
                        project_id=item.project_id,
                        agent_id=item.agent_id,
                        agent_turn_id=item.agent_turn_id,
                        turn_epoch=state.turn_epoch,
                        wait_items=dedup_wait_items,
                        conn=tx.conn,
                    )
            if updated:
                await self._emit_state_event(
                    item,
                    status="suspended",
                    output_box_id=item.output_box_id,
                    metadata={
                        "activity": "awaiting_tool_result",
                        "waiting_tool_count": waiting_tool_count,
                        "waiting_tool_call_ids": waiting_tool_call_ids,
                        "resume_deadline": to_iso(resume_deadline),
                    },
                )
            else:
                logger.warning(
                    "CAS failed when marking suspended turn=%s epoch=%s; stop side effects",
                    item.agent_turn_id,
                    item.turn_epoch,
                )
                return
            await self.step_recorder.set_step_status(
                item=item,
                step_id=step_id,
                status="suspended",
                phase="completed",
                new_card_ids=new_card_ids,
            )
            await self._enqueue_existing_resume_for_waiting_tools(
                item=item,
                state=state,
                step_id=str(step_id),
                wait_items=dedup_wait_items,
            )
        elif blocked_by_inbox:
            logger.info("Turn blocked by inbox guard; waiting for inbox drain.")
            await self._emit_state_event(
                item,
                status="running",
                output_box_id=item.output_box_id,
                metadata={"activity": "waiting_for_inbox_drain"},
            )
            await self.step_recorder.complete_step(
                item=item,
                step_id=step_id,
                new_card_ids=new_card_ids,
            )
            if item.step_count + 1 < self.max_steps:
                asyncio.create_task(self._defer_until_inbox_drains(item))
            else:
                deliverable_card_id = await self._append_partial_deliverable(
                    item,
                    step_id,
                    reason="max_steps_reached (inbox_guard_wait)",
                )
                await self._fail_turn(
                    item,
                    step_id,
                    "max_steps_reached",
                    deliverable_card_id=deliverable_card_id,
                    new_card_ids=new_card_ids,
                )
            return
        elif item.step_count + 1 < self.max_steps:
            # We pre-marked running before dispatch; if no tool actually suspended,
            # remain running so the next ReAct step can proceed.
            if non_submit_calls:
                reverted = await self.state_store.update(
                    project_id=item.project_id,
                    agent_id=item.agent_id,
                    expect_turn_epoch=state.turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    new_status="running",
                )
                if not reverted:
                    fresh_state = await self.state_store.fetch(item.project_id, item.agent_id)
                    if not (
                        fresh_state
                        and fresh_state.active_agent_turn_id == item.agent_turn_id
                        and fresh_state.turn_epoch == item.turn_epoch
                        and fresh_state.status == "running"
                    ):
                        logger.warning(
                            "CAS failed when reverting to running turn=%s epoch=%s; stop side effects",
                            item.agent_turn_id,
                            item.turn_epoch,
                        )
                        return
                await self._emit_state_event(
                    item,
                    status="running",
                    output_box_id=item.output_box_id,
                    metadata={"activity": "thinking"},
                )

            logger.info("Re-enqueue continuation turn=%s", item.step_count + 1)
            await self.step_recorder.complete_step(
                item=item,
                step_id=step_id,
                new_card_ids=new_card_ids,
            )
            self._enqueue_next_step(item)
        else:
            await self._fail_turn(item, step_id, "max_steps_reached", new_card_ids=new_card_ids)

    async def _handle_no_tool_calls(
        self, ctx: TurnContext
    ) -> None:
        if not ctx.step_id:
            raise RuntimeError("ctx.step_id must be set before handling no-tool path")
        if ctx.profile is None:
            raise RuntimeError("ctx.profile must be set before handling no-tool path")
        if ctx.resp is None:
            raise RuntimeError("ctx.resp must be set before handling no-tool path")
        item = ctx.item
        step_id = str(ctx.step_id)
        profile = ctx.profile
        resp = ctx.resp
        result_fields = ctx.result_fields
        new_card_ids = ctx.new_card_ids
        start_ts = ctx.start_ts
        must_end_with = [t for t in (profile.must_end_with or []) if safe_str(t).strip()]
        if not must_end_with:
            deliverable_text = (resp.get("content") or "").strip()
            if not deliverable_text:
                deliverable_text = "(no content)"
            deliverable_card = Card(
                card_id=uuid6.uuid7().hex,
                project_id=item.project_id,
                type="task.deliverable",
                content=TextContent(text=deliverable_text),
                created_at=datetime.now(UTC),
                author_id=item.agent_id,
                metadata=self._card_metadata(item, step_id=step_id, role="assistant"),
            )
            await self.cardbox.save_card(deliverable_card)
            new_card_ids.append(deliverable_card.card_id)
            item.output_box_id = await self.cardbox.append_to_box(
                item.output_box_id,
                [deliverable_card.card_id],
                project_id=item.project_id,
            )
            await self._complete_turn(
                item,
                step_id,
                start_ts,
                deliverable_card_id=deliverable_card.card_id,
                new_card_ids=new_card_ids,
            )
            return

        # No tool_calls and must_end_with is set -> require an end tool next turn.
        fields_lines: List[str] = []
        if "submit_result" in must_end_with:
            for f in result_fields or []:
                name = (f.get("name") or "").strip()
                desc = (f.get("description") or "").strip()
                if name:
                    fields_lines.append(f"- {name}: {desc}".rstrip())
        fields_desc = "\n".join(fields_lines) if fields_lines else "- (no required fields provided)"

        tools_desc = ", ".join(must_end_with)
        reminder_text = (
            "You have not called any required end tool yet, so this turn will NOT finish.\n"
            f"In the NEXT turn, call ONE of: {tools_desc}.\n"
            "It MUST be the ONLY tool call in that turn."
        )
        if "submit_result" in must_end_with:
            reminder_text += "\nRequired result fields:\n" + fields_desc
        reminder_card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=item.project_id,
            type="sys.must_end_with_required",
            content=TextContent(text=reminder_text),
            created_at=datetime.now(UTC),
            author_id=item.agent_id,
            metadata=self._card_metadata(item, step_id=step_id, role="system"),
        )
        await self.cardbox.save_card(reminder_card)
        new_card_ids.append(reminder_card.card_id)
        item.output_box_id = await self.cardbox.append_to_box(
            item.output_box_id,
            [reminder_card.card_id],
            project_id=item.project_id,
        )
        await self.step_recorder.complete_step(
            item=item,
            step_id=step_id,
            new_card_ids=new_card_ids,
        )
        if item.step_count + 1 < self.max_steps:
            logger.info("No tool_calls; require must_end_with and re-enqueue continuation.")
            self._enqueue_next_step(item)
        else:
            deliverable_card_id = await self._append_partial_deliverable(
                item,
                step_id,
                reason="max_steps_reached (missing_required_end_tool)",
            )
            await self._fail_turn(
                item,
                step_id,
                "max_steps_reached",
                deliverable_card_id=deliverable_card_id,
                new_card_ids=new_card_ids,
            )

    async def process_step(self, item: AgentTurnWorkItem) -> None:
        start_ts = time.monotonic()
        logger.info(
            "Start processing turn=%s agent=%s turn=%s",
            item.agent_turn_id,
            item.agent_id,
            item.step_count,
        )

        state = await self._fetch_state_guarded(item)
        if not state:
            return

        ctx = TurnContext(item=item, start_ts=start_ts, state=state)

        if not await self._guard_depth_and_steps(ctx):
            return

        if not await self._start_step_and_enter_running(ctx):
            return

        if not await self._load_profile_or_fail(ctx):
            return

        context_ids = await self._load_context_card_ids(item)
        ok = await self._prepare_llm_request(ctx, context_card_ids=context_ids)
        if not ok:
            return

        ok = await self._call_llm_or_fail(ctx=ctx)
        if not ok:
            return

        tool_calls = await self._persist_thought_card(ctx)

        if tool_calls:
            await self._handle_tool_calls(ctx, tool_calls)
            return

        await self._handle_no_tool_calls(ctx)
        return

    async def _emit_state_event(
        self,
        item: AgentTurnWorkItem,
        *,
        status: str,
        output_box_id: Optional[str] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        await emit_agent_state(
            nats=self.nats,
            ctx=item.to_cg_context(),
            turn_epoch=item.turn_epoch,
            status=status,
            output_box_id=output_box_id or item.output_box_id,
            metadata=metadata,
        )

    async def _complete_turn(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        start_ts: float,
        *,
        deliverable_card_id: Optional[str] = None,
        new_card_ids: Optional[List[str]] = None,
    ) -> None:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        stats = {"duration_ms": duration_ms}
        finish_effects = None

        class _TurnCasMiss(Exception):
            pass

        try:
            async with state_store_transaction(self.state_store) as tx:
                finish_effects = await self.turn_finisher.complete_turn(
                    item=item,
                    step_id=step_id,
                    deliverable_card_id=deliverable_card_id,
                    conn=tx.conn,
                    expect_turn_epoch=item.turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    new_card_ids=new_card_ids,
                    stats=stats,
                )
                if finish_effects is None:
                    raise _TurnCasMiss("complete turn: finish_turn_idle CAS failed")
        except _TurnCasMiss:
            logger.warning(
                "Complete turn CAS failed (stale?) turn=%s epoch=%s; skip evt.task",
                item.agent_turn_id,
                item.turn_epoch,
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Complete turn rollback triggered: %s", exc)
            return

        try:
            assert finish_effects is not None
            await self.turn_finisher.publish_finish_effects(finish_effects)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Complete turn post-commit effects failed: %s", exc)

    async def _fail_turn(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        error: str,
        *,
        error_code: Optional[str] = None,
        deliverable_card_id: Optional[str] = None,
        new_card_ids: Optional[List[str]] = None,
    ) -> None:
        finish_effects = None

        class _TurnCasMiss(Exception):
            pass

        try:
            async with state_store_transaction(self.state_store) as tx:
                finish_effects = await self.turn_finisher.fail_turn(
                    item=item,
                    step_id=step_id,
                    reason=error,
                    error_code=error_code,
                    conn=tx.conn,
                    expect_turn_epoch=item.turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    deliverable_card_id=deliverable_card_id,
                    new_card_ids=new_card_ids,
                )
                if finish_effects is None:
                    raise _TurnCasMiss("fail turn: finish_turn_idle CAS failed")
        except _TurnCasMiss:
            logger.warning(
                "Fail turn CAS failed (stale?) turn=%s epoch=%s; skip evt.task",
                item.agent_turn_id,
                item.turn_epoch,
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fail turn rollback triggered: %s", exc)
            return

        try:
            assert finish_effects is not None
            await self.turn_finisher.publish_finish_effects(finish_effects)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Fail turn post-commit effects failed: %s", exc)

    async def _append_partial_deliverable(
        self, item: AgentTurnWorkItem, step_id: str, *, reason: str
    ) -> Optional[str]:
        return await append_partial_deliverable(
            cardbox=self.cardbox,
            item=item,
            step_id=step_id,
            reason=reason,
            card_metadata_builder=self._card_metadata,
            logger=logger,
        )
