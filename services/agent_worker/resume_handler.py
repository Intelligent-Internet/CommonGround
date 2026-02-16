import logging
import uuid6
from asyncio import Queue
from datetime import datetime, UTC, timedelta
from typing import Dict, Optional, List, Any

from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ResourceStore, StateStore, StepStore
from core.errors import ProtocolViolationError, build_error_result_from_exception
from core.utp_protocol import Card, TextContent, JsonContent, ToolCallContent, extract_tool_result_payload
from core.status import STATUS_FAILED, STATUS_SUCCESS, TOOL_RESULT_STATUS_SET
from core.utils import safe_str
from core.trace import ensure_trace_headers
from infra.worker_helpers import TurnGuard, publish_idle_wakeup
from infra.tool_executor import ToolResultBuilder, ToolResultContext
from infra.event_emitter import emit_agent_state, emit_agent_task

from .models import AgentTurnWorkItem
from .resume_context import (
    has_tool_result_card_id,
    is_valid_after_execution,
    normalize_resume_status,
    parse_resume_command,
)
from .resume_decision import ResumeResultInfo, resolve_resume_outcome
from .resume_effects import (
    ensure_output_box_and_append_tool_result,
    ensure_terminated_deliverable,
)
from .helpers.step_recorder import StepRecorder
from .timing import to_iso, utc_now
from .tx_utils import call_with_optional_conn, state_store_transaction
from .shared import resolve_box_id


logger = logging.getLogger("ResumeHandler")


def _card_loader_err(code: str, message: str) -> Dict[str, str]:
    return {"code": code, "message": message}


def _extract_tool_call_id(card: Any) -> str:
    direct = getattr(card, "tool_call_id", None)
    if direct:
        return str(direct)
    content = getattr(card, "content", None)
    if isinstance(content, ToolCallContent):
        content_id = getattr(content, "id", None)
        if content_id:
            return str(content_id)
    meta = getattr(card, "metadata", None)
    if isinstance(meta, dict) and meta.get("tool_call_id"):
        return str(meta.get("tool_call_id"))
    return ""


async def _require_tool_result_card(
    *,
    cardbox: Any,
    project_id: str,
    card_id: str,
    expected_call_id: Optional[str],
) -> tuple[Optional[Card], Optional[Dict[str, str]]]:
    if not str(card_id or "").strip():
        return None, _card_loader_err("missing_card_id", "tool_result_card_id is required")
    try:
        cards = await cardbox.get_cards([str(card_id)], project_id=project_id)
    except Exception as exc:  # noqa: BLE001
        return None, _card_loader_err("card_fetch_failed", f"failed to load card: {exc}")
    card = cards[0] if cards else None
    if not card:
        return None, _card_loader_err("card_not_found", f"tool.result card not found: {card_id}")
    if getattr(card, "type", None) != "tool.result":
        return None, _card_loader_err("invalid_card_type", "card type must be tool.result")
    if expected_call_id:
        actual_call_id = _extract_tool_call_id(card)
        if actual_call_id != str(expected_call_id):
            return None, _card_loader_err(
                "tool_call_id_mismatch",
                f"tool_call_id mismatch: expected={expected_call_id} actual={actual_call_id or '<empty>'}",
            )
    return card, None


def _is_watch_heartbeat(tool_result: Any, payload: Dict[str, Any]) -> bool:
    meta = getattr(tool_result, "metadata", None)
    if not isinstance(meta, dict):
        return False
    tool_name = meta.get("function_name")
    if tool_name not in ("skills.task_watch", "skills.job_watch"):
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


class ResumeHandler:
    """Handle UTP resume commands and transition agent state."""

    def __init__(
        self,
        *,
        state_store: StateStore,
        step_store: StepStore,
        resource_store: ResourceStore,
        cardbox: CardBoxClient,
        nats: NATSClient,
        queue: Queue[AgentTurnWorkItem],
        suspend_timeout_seconds: float,
        timing_capture: bool = False,
        timing_step_event: bool = False,
        judge_cfg: Optional[Dict[str, Any]] = None,
        timing_task_event: bool = False,
    ) -> None:
        self.state_store = state_store
        self.step_store = step_store
        self.resource_store = resource_store
        self.cardbox = cardbox
        self.nats = nats
        self.queue = queue
        self.suspend_timeout_seconds = float(suspend_timeout_seconds)
        self.timing_capture = timing_capture
        self.timing_step_event = timing_step_event
        self.step_recorder = StepRecorder(
            step_store=self.step_store,
            nats=self.nats,
            timing_step_event=self.timing_step_event,
        )
        cfg = judge_cfg or {}
        self.judge_timeout_s = float(cfg.get("timeout_s", 5.0))
        self.judge_max_retries = int(cfg.get("max_retries", 3))
        self.timing_task_event = timing_task_event

    async def _fail_resume(
        self,
        item: AgentTurnWorkItem,
        state: Any,
        *,
        reason: str,
        tool_call_id: Optional[str],
        error: Any = None,
    ) -> None:
        agent_turn_id = state.active_agent_turn_id or item.agent_turn_id
        step_id = f"step_res_{uuid6.uuid7().hex}"
        if self.timing_capture:
            item.timing = dict(item.timing or {})
            item.timing["step_id"] = step_id
            item.timing["step_started_at"] = to_iso(utc_now())
        tool_call_id_value = tool_call_id or "unknown"
        error_payload = error or {"code": "resume_failed", "message": reason}
        error_msg = None
        if isinstance(error_payload, dict):
            error_msg = safe_str(error_payload.get("message")) or safe_str(error_payload.get("code"))
        if error_msg is None:
            error_msg = str(error_payload)

        profile_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.profile_box_id,
            project_id=item.project_id,
        )
        context_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.context_box_id,
            project_id=item.project_id,
        )
        output_box_id_init = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.output_box_id,
            project_id=item.project_id,
        )
        await self.step_recorder.start_step(
            item=item,
            step_id=step_id,
            status="tool_dispatched",
            agent_turn_id=agent_turn_id,
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=output_box_id_init,
        )

        cmd_data = {
            "tool_call_id": tool_call_id_value,
            "agent_turn_id": agent_turn_id,
            "turn_epoch": item.turn_epoch,
            "agent_id": item.agent_id,
            "after_execution": "terminate",
            "tool_name": "resume_handler",
        }
        tool_call_meta = {
            "step_id": step_id,
            **({"trace_id": item.trace_id} if item.trace_id else {}),
            **({"parent_step_id": item.parent_step_id} if item.parent_step_id else {}),
        }
        result_context = ToolResultContext.from_cmd_data(
            project_id=item.project_id,
            cmd_data=cmd_data,
            tool_call_meta=tool_call_meta,
        )
        _, error_card = ToolResultBuilder(
            result_context,
            author_id=item.agent_id,
            function_name="resume_handler",
            error_source="worker",
        ).build(
            status=STATUS_FAILED,
            result=None,
            error=error_payload,
            after_execution="terminate",
        )
        output_box_id = output_box_id_init or ""
        updated = False
        try:
            async with state_store_transaction(self.state_store) as tx:
                output_box_id = await call_with_optional_conn(
                    self.cardbox.ensure_box_id,
                    project_id=item.project_id,
                    box_id=state.output_box_id,
                    conn=tx.conn,
                )
                if output_box_id != safe_str(state.output_box_id):
                    synced = await call_with_optional_conn(
                        self.state_store.update,
                        project_id=item.project_id,
                        agent_id=item.agent_id,
                        expect_turn_epoch=state.turn_epoch,
                        expect_agent_turn_id=state.active_agent_turn_id or item.agent_turn_id,
                        expect_status=state.status,
                        output_box_id=output_box_id,
                        conn=tx.conn,
                    )
                    if not synced:
                        raise RuntimeError("resume fail: output_box sync CAS failed")

                await call_with_optional_conn(self.cardbox.save_card, error_card, conn=tx.conn)
                await call_with_optional_conn(
                    self.cardbox.append_to_box,
                    output_box_id,
                    [error_card.card_id],
                    project_id=item.project_id,
                    conn=tx.conn,
                )

                updated = await call_with_optional_conn(
                    self.state_store.finish_turn_idle,
                    project_id=item.project_id,
                    agent_id=item.agent_id,
                    expect_turn_epoch=state.turn_epoch,
                    expect_agent_turn_id=state.active_agent_turn_id or item.agent_turn_id,
                    expect_status=state.status,
                    last_output_box_id=output_box_id,
                    conn=tx.conn,
                )
                if not updated:
                    raise RuntimeError("resume fail: finish_turn_idle CAS failed")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Resume fail: transactional rollback triggered: %s", exc)
            return

        if not updated:
            logger.warning("Resume fail: CAS failed; skip emit task event")
            return

        await self._emit_state_event(
            item,
            status="idle",
            output_box_id=str(output_box_id) if output_box_id is not None else None,
            metadata={"error": error_msg} if error_msg else None,
        )

        await self.step_recorder.fail_step(
            item=item,
            step_id=step_id,
            error=error_msg,
            new_card_ids=[error_card.card_id],
        )

        task_ctx = item.to_cg_context(agent_turn_id=agent_turn_id, step_id=step_id)
        await emit_agent_task(
            nats=self.nats,
            ctx=task_ctx,
            status=STATUS_FAILED,
            output_box_id=str(output_box_id) if output_box_id is not None else None,
            deliverable_card_id=None,
            tool_result_card_id=error_card.card_id,
            error=error_msg,
            stats={},
            timing=item.timing if self.timing_task_event else None,
            include_llm_meta=self.timing_task_event,
        )
        await self._publish_idle_wakeup(item)

    async def _fail_invalid_resume(
        self,
        item: AgentTurnWorkItem,
        state: Any,
        *,
        reason: str,
        tool_call_id: Optional[str],
        error_code: str,
        error_message: str,
    ) -> None:
        await self._fail_resume(
            item,
            state,
            reason=reason,
            tool_call_id=tool_call_id,
            error={"code": error_code, "message": error_message},
        )

    async def handle(self, item: AgentTurnWorkItem) -> None:
        data: Dict[str, Optional[object]] = item.resume_data or {}
        resume = parse_resume_command(data)
        tool_call_id = resume.tool_call_id
        status = resume.status
        after_exec = resume.after_execution
        tool_result_card_id = resume.tool_result_card_id
        step_id_hint = resume.step_id_hint

        new_card_ids: List[str] = []
        ledger_turn_epoch = int(item.turn_epoch or 0)
        ledger_enabled = bool(
            safe_str(tool_call_id)
            and has_tool_result_card_id(tool_result_card_id)
            and ledger_turn_epoch > 0
        )

        async def _ledger_record_if_needed() -> None:
            if not ledger_enabled:
                return
            await self.state_store.record_turn_resume_ledger(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=ledger_turn_epoch,
                tool_call_id=safe_str(tool_call_id),
                tool_result_card_id=safe_str(tool_result_card_id),
                payload=dict(data or {}),
            )

        async def _ledger_mark_applied_if_needed() -> None:
            if not ledger_enabled:
                return
            await self.state_store.mark_turn_resume_ledger_applied(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=ledger_turn_epoch,
                tool_call_id=safe_str(tool_call_id),
                tool_result_card_id=safe_str(tool_result_card_id),
            )

        async def _ledger_requeue_if_needed(delay_seconds: float, reason: str) -> None:
            if not ledger_enabled:
                return
            await self.state_store.mark_turn_resume_ledger_pending_with_backoff(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=ledger_turn_epoch,
                tool_call_id=safe_str(tool_call_id),
                tool_result_card_id=safe_str(tool_result_card_id),
                delay_seconds=delay_seconds,
                last_error=reason,
            )

        async def _ledger_drop_if_needed(reason: str) -> None:
            if not ledger_enabled:
                return
            await self.state_store.mark_turn_resume_ledger_dropped(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=ledger_turn_epoch,
                tool_call_id=safe_str(tool_call_id),
                tool_result_card_id=safe_str(tool_result_card_id),
                reason=reason,
            )

        await _ledger_record_if_needed()

        logger.info(
            "Handling RESUME: %s status=%s after=%s", tool_call_id, status, after_exec
        )

        state = await self.state_store.fetch(item.project_id, item.agent_id)
        if not state:
            logger.warning(
                "Resume: State not found for %s. Dropping stale resume and consuming inbox.",
                item.agent_id,
            )
            await _ledger_requeue_if_needed(2.0, "state_not_found")
            return
        if getattr(state, "parent_step_id", None):
            item.parent_step_id = safe_str(state.parent_step_id)
        state_trace_id = getattr(state, "trace_id", None)
        if state_trace_id:
            item.trace_id = safe_str(state_trace_id)
            try:
                item.headers, _, _ = ensure_trace_headers(item.headers, trace_id=item.trace_id)
            except ProtocolViolationError as exc:
                logger.warning("Resume: invalid trace_id: %s", exc)

        guard = TurnGuard(agent_turn_id=item.agent_turn_id, turn_epoch=item.turn_epoch)
        if not guard.matches(state):
            function_name = ""
            if tool_result_card_id:
                try:
                    cards = await self.cardbox.get_cards([tool_result_card_id], project_id=item.project_id)
                    card = cards[0] if cards else None
                    meta = getattr(card, "metadata", None) if card else None
                    if isinstance(meta, dict):
                        function_name = safe_str(meta.get("function_name"))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Resume: Guard mismatch fetch tool_result_card failed: %s", exc)
            logger.warning(
                "Resume: Guard failed (%s). Ignoring. agent=%s turn=%s epoch=%s expected_turn=%s expected_epoch=%s "
                "state=%s tool_call_id=%s tool=%s after=%s status=%s source=%s",
                guard.mismatch_detail(state),
                item.agent_id,
                item.agent_turn_id,
                item.turn_epoch,
                getattr(state, "active_agent_turn_id", None),
                getattr(state, "turn_epoch", None),
                getattr(state, "status", None),
                tool_call_id,
                function_name,
                after_exec,
                status,
                resume.source_agent_id,
            )
            await _ledger_drop_if_needed("turn_guard_mismatch")
            return

        if state.status == "running":
            logger.info(
                "Resume: turn=%s still running; consume early resume and rely on suspended reconcile.",
                item.agent_turn_id,
            )
            await _ledger_requeue_if_needed(1.0, "turn_running_not_suspended")
            return
        if state.status != "suspended":
            logger.warning(
                "Resume: Early/invalid state=%s for turn=%s, dropping stale resume and consuming inbox.",
                state.status,
                item.agent_turn_id,
            )
            await _ledger_drop_if_needed(f"invalid_state:{state.status}")
            return

        # Hard-cut v1: RESUME must carry tool_result_card_id and must NOT carry inline result.
        if resume.has_inline_result:
            logger.warning("Resume: payload contains forbidden field 'result'; fail turn.")
            await self._fail_invalid_resume(
                item,
                state,
                reason="resume payload contains forbidden field 'result'",
                tool_call_id=safe_str(tool_call_id),
                error_code="invalid_payload",
                error_message="resume payload contains forbidden field 'result'",
            )
            await _ledger_drop_if_needed("invalid_payload_inline_result")
            return

        if not has_tool_result_card_id(tool_result_card_id):
            logger.warning("Resume: missing tool_result_card_id; fail turn.")
            await self._fail_invalid_resume(
                item,
                state,
                reason="missing tool_result_card_id",
                tool_call_id=safe_str(tool_call_id),
                error_code="missing_tool_result_card",
                error_message="missing tool_result_card_id",
            )
            await _ledger_drop_if_needed("missing_tool_result_card_id")
            return

        normalized_status = normalize_resume_status(status)
        if normalized_status != status:
            logger.warning("Resume: Invalid status '%s', coercing to 'failed'", status)
        status = normalized_status

        if not is_valid_after_execution(after_exec):
            logger.warning("Resume: Invalid after_execution '%s', fail turn.", after_exec)
            await self._fail_invalid_resume(
                item,
                state,
                reason=f"invalid after_execution={after_exec!r}",
                tool_call_id=safe_str(tool_call_id),
                error_code="invalid_after_execution",
                error_message=f"invalid after_execution={after_exec!r}",
            )
            await _ledger_drop_if_needed("invalid_after_execution")
            return

        agent_turn_id = state.active_agent_turn_id or item.agent_turn_id
        step_id = f"step_res_{uuid6.uuid7().hex}"
        if self.timing_capture:
            item.timing = dict(item.timing or {})
            item.timing["step_id"] = step_id
            item.timing["step_started_at"] = to_iso(utc_now())

        expected_call_id = safe_str(tool_call_id)
        tool_result, load_err = await _require_tool_result_card(
            cardbox=self.cardbox,
            project_id=item.project_id,
            card_id=str(tool_result_card_id),
            expected_call_id=expected_call_id,
        )
        if (not tool_result) and load_err and load_err.get("code") in {
            "invalid_card_type",
            "tool_call_id_mismatch",
        }:
            error_code = "invalid_tool_result_type"
            error_message = "tool_result card has invalid type"
            reason = "tool_result_card_id has invalid type"
            if load_err.get("code") == "tool_call_id_mismatch":
                error_code = "tool_call_id_mismatch"
                error_message = "tool_call_id mismatch"
                reason = "tool_call_id mismatch between resume payload and tool.result card"
            await self._fail_resume(
                item,
                state,
                reason=reason,
                tool_call_id=expected_call_id,
                error={"code": error_code, "message": error_message},
            )
            await _ledger_drop_if_needed(error_code)
            return

        if (not tool_result) and load_err and load_err.get("code") == "card_not_found" and tool_call_id and step_id_hint:
            existing_cards = await self.cardbox.get_tool_results_by_step_and_call_ids(
                project_id=item.project_id,
                step_id=str(step_id_hint),
                tool_call_ids=[str(tool_call_id)],
            )
            if existing_cards:
                existing_cards.sort(
                    key=lambda card: getattr(card, "created_at", datetime.max.replace(tzinfo=UTC))
                )
                tool_result = existing_cards[0]
                tool_result_card_id = getattr(tool_result, "card_id", tool_result_card_id)
        if not tool_result:
            if load_err and load_err.get("code") and load_err.get("code") != "card_not_found":
                logger.warning(
                    "Resume: failed loading tool_result card id=%s code=%s msg=%s",
                    tool_result_card_id,
                    load_err.get("code"),
                    load_err.get("message"),
                )
            logger.warning(
                "Resume: tool_result_card_id not found (%s); keep pending and retry later.",
                tool_result_card_id,
            )
            await _ledger_requeue_if_needed(1.5, "tool_result_card_not_found")
            return

        dispatch_step_id = None
        tool_result_meta = getattr(tool_result, "metadata", None)
        if isinstance(tool_result_meta, dict):
            dispatch_step_id = tool_result_meta.get("step_id")
            if item.trace_id and not tool_result_meta.get("trace_id"):
                logger.warning("Resume: tool.result missing trace_id (call_id=%s).", tool_call_id)
            if item.parent_step_id and not tool_result_meta.get("parent_step_id"):
                logger.warning("Resume: tool.result missing parent_step_id (call_id=%s).", tool_call_id)
        if not dispatch_step_id:
            logger.warning("Resume: tool.result missing step_id; fail turn.")
            await self._fail_resume(
                item,
                state,
                reason="tool.result missing step_id",
                tool_call_id=safe_str(tool_call_id),
                error={"code": "missing_step_id", "message": "tool.result missing step_id"},
            )
            await _ledger_drop_if_needed("missing_dispatch_step_id")
            return

        existing_cards = await self.cardbox.get_tool_results_by_step_and_call_ids(
            project_id=item.project_id,
            step_id=str(dispatch_step_id),
            tool_call_ids=[str(tool_call_id)],
        )
        if existing_cards:
            existing_cards.sort(
                key=lambda card: getattr(card, "created_at", datetime.max.replace(tzinfo=UTC))
            )
            primary_card = existing_cards[0]
            primary_id = getattr(primary_card, "card_id", None)
            if primary_id and primary_id != tool_result_card_id:
                tool_result = primary_card
                tool_result_card_id = primary_id
                tool_result_meta = getattr(tool_result, "metadata", None)
                dispatch_step_id = (
                    tool_result_meta.get("step_id")
                    if isinstance(tool_result_meta, dict)
                    else dispatch_step_id
                )

        wait_entry = await self.state_store.get_turn_waiting_tool(
            project_id=item.project_id,
            agent_id=item.agent_id,
            agent_turn_id=item.agent_turn_id,
            turn_epoch=state.turn_epoch,
            tool_call_id=str(tool_call_id),
        )
        if not wait_entry:
            logger.warning(
                "Resume: tool_call_id=%s not in waiting set (turn=%s epoch=%s); consume stale/out-of-order resume.",
                tool_call_id,
                item.agent_turn_id,
                state.turn_epoch,
            )
            await _ledger_drop_if_needed("tool_call_not_waiting")
            return
        wait_status = safe_str(wait_entry.get("wait_status"))
        if wait_status in ("applied", "expired", "dropped"):
            logger.info(
                "Resume: tool_call_id=%s already terminal in waiting set (status=%s); idempotent consume.",
                tool_call_id,
                wait_status,
            )
            await _ledger_mark_applied_if_needed()
            return

        try:
            result_payload = extract_tool_result_payload(tool_result)
        except ProtocolViolationError as exc:
            logger.warning("Resume: %s (tool_result_card_id=%s)", exc, tool_result_card_id)
            result_payload, _ = build_error_result_from_exception(exc, source="worker")
            status = STATUS_FAILED
        is_watch_heartbeat = False
        try:
            is_watch_heartbeat = _is_watch_heartbeat(tool_result, result_payload)
        except Exception:
            is_watch_heartbeat = False

        thought_card = await self.cardbox.get_latest_card_by_step_type(
            project_id=item.project_id,
            step_id=str(dispatch_step_id),
            card_type="agent.thought",
        )
        if not thought_card:
            logger.warning("Resume: agent.thought not found for step_id=%s; fail turn.", dispatch_step_id)
            await self._fail_resume(
                item,
                state,
                reason=f"agent.thought not found for step_id={dispatch_step_id}",
                tool_call_id=safe_str(tool_call_id),
                error={"code": "missing_agent_thought", "message": "agent.thought not found for turn"},
            )
            await _ledger_requeue_if_needed(1.5, "agent_thought_not_found")
            return
        thought_tool_calls = getattr(thought_card, "tool_calls", None) or []
        expected_ids = {
            tc.get("id")
            for tc in thought_tool_calls
            if isinstance(tc, dict) and tc.get("id")
        }
        if not expected_ids:
            logger.warning("Resume: no tool_calls found in thought for step_id=%s; fail turn.", dispatch_step_id)
            await self._fail_resume(
                item,
                state,
                reason=f"no tool_calls found in thought for step_id={dispatch_step_id}",
                tool_call_id=safe_str(tool_call_id),
                error={"code": "missing_tool_calls", "message": "no tool_calls found in thought"},
            )
            await _ledger_drop_if_needed("missing_thought_tool_calls")
            return

        # Record turn row (tool callback)
        profile_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.profile_box_id,
            project_id=item.project_id,
        )
        context_box_id = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.context_box_id,
            project_id=item.project_id,
        )
        output_box_id_init = await resolve_box_id(
            cardbox=self.cardbox,
            box_id=state.output_box_id,
            project_id=item.project_id,
        )
        await self.step_recorder.start_step(
            item=item,
            step_id=step_id,
            status="tool_dispatched",
            agent_turn_id=agent_turn_id,
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=output_box_id_init,
        )

        if is_watch_heartbeat:
            output_box_id = await self.cardbox.ensure_box_id(
                project_id=item.project_id,
                box_id=state.output_box_id,
            )
            if output_box_id != safe_str(state.output_box_id):
                await self.state_store.update(
                    project_id=item.project_id,
                    agent_id=item.agent_id,
                    expect_turn_epoch=state.turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    expect_status=state.status,
                    output_box_id=output_box_id,
                )
        else:
            new_card_ids.append(tool_result_card_id)
            # ensure output box (always append tool.result)
            output_box_id = await ensure_output_box_and_append_tool_result(
                cardbox=self.cardbox,
                state_store=self.state_store,
                project_id=item.project_id,
                agent_id=item.agent_id,
                expect_turn_epoch=state.turn_epoch,
                expect_agent_turn_id=item.agent_turn_id,
                expect_status=state.status,
                current_output_box_id=state.output_box_id,
                tool_result_card_id=tool_result.card_id,
            )
            await self.state_store.mark_turn_waiting_tool_applied(
                project_id=item.project_id,
                agent_id=item.agent_id,
                agent_turn_id=item.agent_turn_id,
                turn_epoch=state.turn_epoch,
                tool_call_id=str(tool_call_id),
                tool_result_card_id=safe_str(tool_result.card_id),
            )

        # Judge retry decision for skills.* failures (optional, bounded).
        judge_decision = None
        try:
            status_val = result_payload.get("status")
            error_val = result_payload.get("error")
            tool_name = None
            meta = getattr(tool_result, "metadata", None)
            if isinstance(meta, dict):
                tool_name = meta.get("function_name")
            if (
                status_val == STATUS_FAILED
                and isinstance(tool_name, str)
                and tool_name.startswith("skills.")
            ):
                # Count previous judge feedbacks for this tool_call_id
                retry_count = 0
                output_box = await self.cardbox.get_box(output_box_id, project_id=item.project_id)
                if output_box and output_box.card_ids:
                    cards = await self.cardbox.get_cards(list(output_box.card_ids), project_id=item.project_id)
                    for c in cards:
                        meta_c = getattr(c, "metadata", None)
                        if not isinstance(meta_c, dict):
                            continue
                        if meta_c.get("type") != "sys.judge_feedback":
                            continue
                        if safe_str(meta_c.get("tool_call_id")) != safe_str(tool_call_id):
                            continue
                        retry_count += 1

                if retry_count < self.judge_max_retries:
                    # Try to fetch tool.call args for context
                    args = {}
                    tool_call_cards = await self.cardbox.get_cards_by_tool_call_ids(
                        project_id=item.project_id,
                        tool_call_ids=[str(tool_call_id)],
                    )
                    if tool_call_cards:
                        tool_call_card = next(
                            (c for c in tool_call_cards if getattr(c, "type", "") == "tool.call"),
                            tool_call_cards[0],
                        )
                        try:
                            args = extract_tool_call_args(tool_call_card)
                        except Exception:
                            args = {}
                    payload = {
                        "tool_name": tool_name,
                        "skill_name": (args.get("skill_name") if isinstance(args, dict) else None),
                        "error": error_val,
                        "result": result_payload.get("result"),
                        "args": args,
                        "attempt": retry_count + 1,
                        "max_attempts": self.judge_max_retries,
                    }
                    subject = subject_pattern(
                        project_id=item.project_id,
                        channel_id=item.channel_id,
                        category="cmd",
                        component="sys",
                        target="judge",
                        suffix="retry",
                        protocol_version=PROTOCOL_VERSION,
                    )
                    resp = await self.nats.request_core(
                        subject,
                        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                        timeout=self.judge_timeout_s,
                    )
                    judge_decision = json.loads(resp.data.decode("utf-8")) if resp and resp.data else None

                if judge_decision and judge_decision.get("action") == "retry":
                    feedback = {
                        "tool_name": tool_name,
                        "tool_call_id": str(tool_call_id),
                        "retry_attempt": retry_count + 1,
                        "reason": judge_decision.get("reason"),
                        "instruction": "Re-load SKILL.md once and retry strictly following its examples and import paths.",
                        "error": error_val,
                    }
                    feedback_card = Card(
                        card_id=uuid6.uuid7().hex,
                        project_id=item.project_id,
                        type="sys.judge_feedback",
                        content=JsonContent(data=feedback),
                        created_at=datetime.now(UTC),
                        author_id="sys.judge",
                        metadata={
                            "agent_turn_id": agent_turn_id,
                            "step_id": step_id,
                            "role": "system",
                            "type": "sys.judge_feedback",
                            "tool_call_id": str(tool_call_id),
                            **({"trace_id": item.trace_id} if item.trace_id else {}),
                            **({"parent_step_id": item.parent_step_id} if item.parent_step_id else {}),
                        },
                    )
                    await self.cardbox.save_card(feedback_card)
                    new_card_ids.append(feedback_card.card_id)
                    output_box_id = await self.cardbox.append_to_box(
                        output_box_id, [feedback_card.card_id], project_id=item.project_id
                    )
                elif judge_decision and judge_decision.get("action") == "fail_fast":
                    await self._fail_resume(
                        item,
                        state,
                        reason="judge_fail_fast",
                        tool_call_id=safe_str(tool_call_id),
                        error={"code": "judge_fail_fast", "message": judge_decision.get("reason")},
                    )
                    await _ledger_drop_if_needed("judge_fail_fast")
                    return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Judge decision failed: %s", exc)

        pending_wait_rows = await self.state_store.list_turn_waiting_tools(
            project_id=item.project_id,
            agent_id=item.agent_id,
            agent_turn_id=item.agent_turn_id,
            turn_epoch=state.turn_epoch,
            statuses=("waiting", "received"),
        )
        pending = {
            safe_str(row.get("tool_call_id"))
            for row in pending_wait_rows
            if safe_str(row.get("tool_call_id"))
        }

        if pending:
            resume_deadline = (
                datetime.now(UTC) + timedelta(seconds=self.suspend_timeout_seconds)
                if pending
                else None
            )
            updated = await self.state_store.update(
                project_id=item.project_id,
                agent_id=item.agent_id,
                expect_turn_epoch=state.turn_epoch,
                expect_agent_turn_id=item.agent_turn_id,
                expect_status=state.status,
                new_status="suspended",
                expecting_correlation_id=None,
                waiting_tool_count=len(pending),
                resume_deadline=resume_deadline,
            )
            if updated:
                await self._emit_state_event(
                    item,
                    status="suspended",
                    output_box_id=str(output_box_id) if output_box_id is not None else None,
                    metadata={
                        "activity": "awaiting_tool_result",
                        "waiting_tool_count": len(pending),
                        "waiting_tool_call_ids": sorted(pending),
                        "resume_deadline": to_iso(resume_deadline),
                    },
                )
            await self.step_recorder.set_step_status(
                item=item,
                step_id=step_id,
                status="suspended",
                phase="completed",
                new_card_ids=new_card_ids,
            )
            logger.info(
                "Resume: Waiting for %s pending tools (step_id=%s)", len(pending), dispatch_step_id
            )
            await _ledger_mark_applied_if_needed()
            return

        result_infos: list[ResumeResultInfo] = []
        actual_cards = await self.cardbox.get_tool_results_by_step_and_call_ids(
            project_id=item.project_id,
            step_id=str(dispatch_step_id),
            tool_call_ids=list(expected_ids),
        )
        for card in actual_cards:
            try:
                payload = extract_tool_result_payload(card)
                status_val = payload.get("status")
                after_exec_val = payload.get("after_execution")
            except ProtocolViolationError as exc:
                logger.warning(
                    "Resume: %s (tool_result_card_id=%s)", exc, getattr(card, "card_id", None)
                )
                payload, _ = build_error_result_from_exception(exc, source="worker")
                status_val = STATUS_FAILED
                after_exec_val = None
            if status_val not in TOOL_RESULT_STATUS_SET:
                status_val = STATUS_FAILED
            result_infos.append(
                ResumeResultInfo(
                    card=card,
                    status=status_val,
                    after_execution=after_exec_val,
                    payload=payload,
                )
            )
        outcome = resolve_resume_outcome(result_infos)
        aggregate_after_exec = outcome.aggregate_after_execution

        if aggregate_after_exec == "suspend":
            next_turn_epoch = int(state.turn_epoch) + 1
            await self.step_recorder.set_step_status(
                item=item,
                step_id=step_id,
                status="suspended",
                phase="completed",
                new_card_ids=new_card_ids,
            )
            updated = await self.state_store.update(
                project_id=item.project_id,
                agent_id=item.agent_id,
                expect_turn_epoch=state.turn_epoch,
                expect_agent_turn_id=item.agent_turn_id,
                expect_status=state.status,
                new_status="running",
                output_box_id=output_box_id,
                expecting_correlation_id=None,
                waiting_tool_count=0,
                resume_deadline=None,
                bump_epoch=True,
            )
            if updated:
                item.turn_epoch = next_turn_epoch
                await self._emit_state_event(
                    item,
                    status="running",
                    output_box_id=str(output_box_id) if output_box_id is not None else None,
                    metadata={"activity": "thinking"},
                )
                step_count = await self.step_store.count_react_steps(
                    project_id=item.project_id, agent_turn_id=agent_turn_id
                )
                next_item = AgentTurnWorkItem(
                    project_id=item.project_id,
                    channel_id=item.channel_id,
                    agent_turn_id=agent_turn_id,
                    agent_id=item.agent_id,
                    profile_box_id=safe_str(state.profile_box_id),
                    context_box_id=safe_str(state.context_box_id),
                    output_box_id=output_box_id,
                    turn_epoch=next_turn_epoch,
                    headers=item.headers,
                    parent_step_id=item.parent_step_id,
                    trace_id=item.trace_id,
                    step_count=step_count,
                    resume_data=None,
                    is_continuation=True,
                    guard_inbox_id=item.guard_inbox_id or item.inbox_id,
                )
                await self._enqueue_next_step(next_item)
            else:
                logger.warning("Resume: CAS failed (state changed?)")
                await _ledger_requeue_if_needed(1.0, "resume_cas_failed_running")
                return
            await _ledger_mark_applied_if_needed()
            return

        elif aggregate_after_exec == "terminate":
            task_status = outcome.task_status
            result_payload = outcome.primary_payload or result_payload

            # Best-effort: ensure a deliverable exists and expose deliverable_card_id in evt.agent.*.task.
            deliverable_card_id: Optional[str] = None
            try:
                deliverable_ctx = item.to_cg_context(agent_turn_id=agent_turn_id, step_id=step_id)
                output_box_id, deliverable_card_id, created_card_id = await ensure_terminated_deliverable(
                    cardbox=self.cardbox,
                    ctx=deliverable_ctx,
                    output_box_id=output_box_id,
                    result_payload=result_payload,
                    tool_call_id=safe_str(tool_call_id),
                    resume_status=status,
                    task_status=task_status,
                )
                if created_card_id:
                    new_card_ids.append(created_card_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Resume terminate: ensure deliverable failed: %s", exc)

            updated = await self.state_store.finish_turn_idle(
                project_id=item.project_id,
                agent_id=item.agent_id,
                expect_turn_epoch=state.turn_epoch,
                expect_agent_turn_id=item.agent_turn_id,
                expect_status=state.status,
                last_output_box_id=output_box_id,
            )
            if not updated:
                logger.warning(
                    "Resume terminate CAS failed (stale?) turn=%s epoch=%s; skip evt.task",
                    item.agent_turn_id,
                    item.turn_epoch,
                )
                await _ledger_requeue_if_needed(1.0, "resume_cas_failed_terminate")
                return

            error_msg = None
            if task_status != STATUS_SUCCESS:
                error_obj = None
                if isinstance(result_payload, dict):
                    error_obj = result_payload.get("error")
                if isinstance(error_obj, dict):
                    error_msg = error_obj.get("message") or error_obj.get("code")
                if error_msg is None and isinstance(result_payload, dict):
                    result_obj = result_payload.get("result")
                    if isinstance(result_obj, dict):
                        error_msg = (
                            result_obj.get("error_message")
                            or result_obj.get("error")
                            or result_obj.get("error_code")
                        )
                error_msg = error_msg or task_status

            await self._emit_state_event(
                item,
                status="idle",
                output_box_id=str(output_box_id) if output_box_id is not None else None,
                metadata={"error": str(error_msg)} if task_status != STATUS_SUCCESS and error_msg else None,
            )

            step_phase = "completed" if task_status == STATUS_SUCCESS else "failed"
            if step_phase == "completed":
                await self.step_recorder.complete_step(
                    item=item,
                    step_id=step_id,
                    new_card_ids=new_card_ids,
                )
            else:
                await self.step_recorder.fail_step(
                    item=item,
                    step_id=step_id,
                    error=str(error_msg or task_status),
                    new_card_ids=new_card_ids,
                )

            task_ctx = item.to_cg_context(agent_turn_id=agent_turn_id, step_id=step_id)
            await emit_agent_task(
                nats=self.nats,
                ctx=task_ctx,
                status=task_status,
                output_box_id=str(output_box_id) if output_box_id is not None else None,
                deliverable_card_id=deliverable_card_id,
                tool_result_card_id=str(tool_result_card_id) if tool_result_card_id else None,
                error=error_msg,
                stats={},
                timing=item.timing if self.timing_task_event else None,
                include_llm_meta=self.timing_task_event,
            )
            await self._publish_idle_wakeup(item)
            await _ledger_mark_applied_if_needed()

    async def _publish_idle_wakeup(self, item: AgentTurnWorkItem) -> None:
        await publish_idle_wakeup(
            nats=self.nats,
            resource_store=self.resource_store,
            state_store=self.state_store,
            project_id=item.project_id,
            channel_id=item.channel_id,
            agent_id=item.agent_id,
            headers=item.headers,
            retry_count=3,
            retry_delay=1.0,
            logger=logger,
            warn_prefix="Idle wakeup",
        )

    async def _enqueue_next_step(self, item: AgentTurnWorkItem) -> None:
        if self.timing_capture:
            enqueued_at = utc_now()
            item.enqueued_at = enqueued_at
            item.timing = {"queue_enqueued_at": to_iso(enqueued_at)}
        await self.queue.put(item)

    async def _emit_state_event(
        self,
        item: AgentTurnWorkItem,
        *,
        status: str,
        output_box_id: Optional[str],
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        ctx = item.to_cg_context()
        await emit_agent_state(
            nats=self.nats,
            ctx=ctx,
            turn_epoch=item.turn_epoch,
            status=status,
            output_box_id=output_box_id,
            metadata=metadata,
        )
