"""UI Worker service entrypoint (UI action -> tool dispatch)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Dict, Optional

import uuid6

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config
from core.config_defaults import (
    DEFAULT_UI_WORKER_TARGET,
    DEFAULT_UI_IDEM_TTL_S,
    DEFAULT_UI_IDEM_WAIT_S,
)
from core.errors import ProtocolViolationError
from core.cg_context import CGContext
from core.status import STATUS_FAILED
from core.subject import format_subject, parse_subject, subject_pattern
from core.utils import safe_str, set_loop_policy, safe_target_label
from core.utp_protocol import extract_tool_result_payload
from infra.idempotency import NatsKvIdempotencyStore, build_idempotency_key
from infra.l0_engine import L0Engine
from infra.observability.otel import get_tracer, set_span_attrs, traced
from infra.service_runtime import ServiceBase
from infra.worker_helpers import (
    InboxRowContext,
    InboxRowResult,
    consume_inbox_rows,
    normalize_headers,
    publish_idle_wakeup,
)
from services.agent_worker.utp_dispatcher import UTPDispatcher
from infra.event_emitter import emit_agent_state

set_loop_policy()

# ----------------------------- Logging Setup ----------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UIWorker")
_TRACER = get_tracer("services.ui_worker.loop")


ACK_STATUS_SET = {"accepted", "busy", "rejected", "done"}


def _subject_span_attrs(args: Dict[str, Any]) -> Dict[str, Any]:
    return {"cg.subject": str(args.get("subject"))}


class UIWorkerService(ServiceBase):
    IDEM_BUCKET = "cg_ui_action_idem_v1"
    IDEM_TTL_S = DEFAULT_UI_IDEM_TTL_S
    IDEM_WAIT_S = DEFAULT_UI_IDEM_WAIT_S

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        ui_cfg = self.ctx.cfg.get("ui_worker", {}) or {}
        ttl_cfg = ui_cfg.get("idempotency_ttl_seconds")
        idem_ttl_s = self.IDEM_TTL_S
        if ttl_cfg is not None:
            try:
                parsed_ttl = int(ttl_cfg)
                if parsed_ttl <= 0:
                    raise ValueError("ttl must be positive")
                if parsed_ttl < self.IDEM_TTL_S:
                    logger.warning(
                        "ui_worker.idempotency_ttl_seconds=%s is shorter than cmd replay window; clamping to %s",
                        parsed_ttl,
                        self.IDEM_TTL_S,
                    )
                    parsed_ttl = self.IDEM_TTL_S
                idem_ttl_s = parsed_ttl
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid ui_worker.idempotency_ttl_seconds=%r; fallback=%s",
                    ttl_cfg,
                    self.IDEM_TTL_S,
                )
        self.idem_ttl_s = idem_ttl_s

        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.state_store = self.register_store(self.ctx.stores.state_store())
        self.step_store = self.register_store(self.ctx.stores.step_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        self.l0 = L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
        )
        self.dispatcher = UTPDispatcher(
            self.cardbox,
            self.nats,
            self.resource_store,
            execution_store=self.execution_store,
        )
        self.idem = NatsKvIdempotencyStore(
            self.nats,
            bucket_name=self.IDEM_BUCKET,
            ttl_s=self.idem_ttl_s,
        )
        # Prefer `worker_target` for new configs; fall back to legacy `target` key for backward compatibility.
        configured_target = safe_str(ui_cfg.get("worker_target") or ui_cfg.get("target"))
        self.worker_target = configured_target or DEFAULT_UI_WORKER_TARGET

    async def start(self) -> None:
        await self.open()

        subject_action = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="sys",
            target="ui",
            suffix="action",
            protocol_version=PROTOCOL_VERSION,
        )
        logger.info("Listening on %s (queue=%s)", subject_action, "ui_worker")
        await self.nats.subscribe_cmd(
            subject_action,
            "ui_worker",
            self._handle_ui_action,
            durable_name=f"ui_worker_ui_action_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        subject_wakeup = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="agent",
            target=self.worker_target,
            suffix="wakeup",
            protocol_version=PROTOCOL_VERSION,
        )
        safe_target = safe_target_label(self.worker_target)
        queue_wakeup = f"ui_worker_wakeup_{safe_target}"
        logger.info("Listening on %s (queue=%s)", subject_wakeup, queue_wakeup)
        await self.nats.subscribe_cmd(
            subject_wakeup,
            queue_wakeup,
            self._enqueue_wakeup,
            durable_name=f"ui_worker_wakeup_{safe_target}_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        while True:
            await asyncio.sleep(1)

    async def close(self) -> None:
        await super().close()

    async def _publish_action_ack(
        self,
        *,
        parts: Any,
        action_id: str,
        agent_id: str,
        status: str,
        headers: Dict[str, str],
        message_id: Optional[str] = None,
        tool_result_card_id: Optional[str] = None,
        error_code: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        subject_ack = format_subject(
            parts.project_id,
            parts.channel_id,
            "evt",
            "sys",
            "ui",
            "action_ack",
        )
        payload: Dict[str, Any] = {
            "action_id": action_id,
            "agent_id": agent_id,
            "status": status,
        }
        if message_id:
            payload["message_id"] = message_id
        if tool_result_card_id:
            payload["tool_result_card_id"] = tool_result_card_id
        if error_code:
            payload["error_code"] = error_code
        if error:
            payload["error"] = error
        await self.nats.publish_event(subject_ack, payload, headers=headers)

    async def _fetch_ui_roster(
        self,
        *,
        parts: Any,
        action_id: str,
        agent_id: str,
        headers: Dict[str, str],
        message_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        roster = await self.resource_store.fetch_roster(parts.project_id, agent_id)
        if not roster:
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="rejected",
                headers=headers,
                message_id=message_id,
                error_code="agent_not_found",
            )
            return None
        if safe_str(roster.get("worker_target")) != self.worker_target:
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="rejected",
                headers=headers,
                message_id=message_id,
                error_code="invalid_worker_target",
            )
            return None
        return roster

    def _resolve_profile_box_id(self, *, state: Any, roster: Dict[str, Any]) -> str:
        profile_box_id = safe_str(state, "profile_box_id") if state else None
        if not profile_box_id:
            profile_box_id = safe_str(roster.get("profile_box_id"))
        if not profile_box_id:
            raise ValueError("profile_box_id missing for ui agent")
        return profile_box_id

    async def _emit_state_event(
        self,
        *,
        project_id: str,
        channel_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        status: str,
        output_box_id: Optional[str],
        headers: Dict[str, str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        ctx = CGContext(
            project_id=project_id,
            channel_id=channel_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            headers=dict(headers or {}),
        )
        await emit_agent_state(
            nats=self.nats,
            turn_epoch=turn_epoch,
            status=status,
            output_box_id=output_box_id,
            ctx=ctx,
            metadata=metadata,
        )

    @traced(
        _TRACER,
        "ui.handle_action_cmd",
        headers_arg="headers",
        span_arg="_span",
        attributes_getter=_subject_span_attrs,
    )
    async def _handle_ui_action(
        self,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ) -> None:
        span = _span
        parts = parse_subject(subject)
        if not parts or parts.suffix != "action":
            logger.warning("Invalid UI action subject: %s", subject)
            return

        payload = data or {}
        try:
            headers, _, _ = normalize_headers(nats=self.nats, headers=headers)
        except ProtocolViolationError as exc:
            logger.warning("UI action invalid trace headers: %s", exc)
            return

        action_id = safe_str(payload.get("action_id"))
        agent_id = safe_str(payload.get("agent_id"))
        tool_name = safe_str(payload.get("tool_name"))
        if span is not None:
            span.set_attribute("cg.project_id", str(parts.project_id))
            span.set_attribute("cg.channel_id", str(parts.channel_id))
            span.set_attribute("cg.action_id", str(action_id))
            span.set_attribute("cg.agent_id", str(agent_id))
            span.set_attribute("cg.tool_name", str(tool_name))
        args = payload.get("args")
        parent_step_id = safe_str(payload.get("parent_step_id"))

        if not action_id or not agent_id or not tool_name:
            if action_id and agent_id:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="missing_required_fields",
                )
            return
        if not isinstance(args, dict):
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="rejected",
                headers=headers,
                error_code="invalid_args",
            )
            return
        state = await self.state_store.fetch(parts.project_id, agent_id)
        default_depth = int(getattr(state, "active_recursion_depth", 0) or 0) if state else 0
        try:
            headers, _, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
                default_depth=default_depth,
            )
        except ProtocolViolationError as exc:
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="rejected",
                headers=headers,
                error_code="protocol_violation",
                error=str(exc),
            )
            return

        message_id = safe_str(payload.get("message_id")) or safe_str(args.get("message_id"))

        if not await self._fetch_ui_roster(
            parts=parts,
            action_id=action_id,
            agent_id=agent_id,
            headers=headers,
            message_id=message_id,
        ):
            return

        if state and safe_str(state.status) != "idle":
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="busy",
                headers=headers,
                message_id=message_id,
                error_code="agent_busy",
            )
            return

        inbox_payload: Dict[str, Any] = {
            "action_id": action_id,
            "agent_id": agent_id,
            "tool_name": tool_name,
            "args": args,
            "parent_step_id": parent_step_id,
        }
        if message_id:
            inbox_payload["message_id"] = message_id
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            inbox_payload["metadata"] = metadata

        idem_identity = message_id or action_id
        idem_key = build_idempotency_key("ui_action", parts.project_id, tool_name, idem_identity)
        # Use action_id for L0 correlation to match tool_call_id in tool_result.
        correlation_id = action_id
        wakeup_signals = []
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                publish_result = await self.l0.enqueue(
                    project_id=parts.project_id,
                    channel_id=parts.channel_id,
                    target_agent_id=agent_id,
                    message_type="ui_action",
                    payload=inbox_payload,
                    enqueue_mode="call",
                    correlation_id=correlation_id,
                    recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
                    parent_step_id=parent_step_id,
                    source_agent_id=None,
                    headers=headers,
                    wakeup=True,
                    conn=conn,
                )
                wakeup_signals = list(publish_result.wakeup_signals or ())
        if wakeup_signals:
            await self.l0.publish_wakeup_signals(wakeup_signals)
        span.set_attribute("cg.l0.ack_status", str(publish_result.ack_status))
        ack_status = publish_result.ack_status
        if ack_status == "busy":
            error_code = publish_result.ack_error_code or "agent_busy"
            await self.idem.set_result(
                idem_key,
                {
                    "status": "busy",
                    "message_id": message_id,
                    "error_code": error_code,
                },
            )
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="busy",
                headers=headers,
                message_id=message_id,
                error_code=error_code,
            )
            return

        if ack_status in ("rejected", "error"):
            error_code = publish_result.ack_error_code
            if ack_status == "error" and not error_code:
                error_code = "l0_error"
            await self.idem.set_result(
                idem_key,
                {
                    "status": "rejected",
                    "message_id": message_id,
                    "error_code": error_code,
                },
            )
            await self._publish_action_ack(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                status="rejected",
                headers=headers,
                message_id=message_id,
                error_code=error_code,
            )
            return

        # ack_status == "accepted": confirmed enqueue/lease outcome.
        await self._publish_action_ack(
            parts=parts,
            action_id=action_id,
            agent_id=agent_id,
            status="accepted",
            headers=headers,
            message_id=message_id,
        )
        return

    @traced(
        _TRACER,
        "ui.process_action",
        headers_arg="headers",
        span_arg="_span",
        record_exception=True,
        set_status_on_exception=True,
    )
    async def _process_ui_action(
        self,
        *,
        parts: Any,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        inbox_id: Optional[str] = None,
        _span: Any = None,
    ) -> bool:
        payload = payload or {}
        try:
            headers, trace_id, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
            )
        except ProtocolViolationError as exc:
            logger.warning("UI action invalid trace headers: %s", exc)
            return True

        action_id = safe_str(payload.get("action_id"))
        agent_id = safe_str(payload.get("agent_id"))
        tool_name = safe_str(payload.get("tool_name"))
        args = payload.get("args")
        parent_step_id = safe_str(payload.get("parent_step_id"))
        agent_turn_id = safe_str(payload.get("agent_turn_id"))
        turn_epoch_raw = payload.get("turn_epoch")
        try:
            turn_epoch = int(turn_epoch_raw) if turn_epoch_raw is not None else 0
        except (TypeError, ValueError):
            turn_epoch = 0

        # Backward compatibility: older clients may omit agent_turn_id.
        # Derive a stable value from action_id so callers can reconcile by action_id.
        if not agent_turn_id and action_id:
            logger.warning(
                "UI action missing agent_turn_id; deriving from action_id action_id=%s agent_id=%s",
                action_id,
                agent_id,
            )
            agent_turn_id = f"turn_{action_id}"

        span_attrs: Dict[str, Any] = {
            "cg.project_id": str(parts.project_id),
            "cg.channel_id": str(parts.channel_id),
            "cg.action_id": str(action_id),
            "cg.agent_id": str(agent_id),
            "cg.tool_name": str(tool_name),
            "cg.agent_turn_id": str(agent_turn_id),
            "cg.turn_epoch": int(turn_epoch),
        }
        if inbox_id:
            span_attrs["cg.inbox_id"] = str(inbox_id)
        span = _span
        if span is not None:
            set_span_attrs(span, span_attrs)

            if not action_id or not agent_id or not tool_name:
                if action_id and agent_id:
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status="rejected",
                        headers=headers,
                        error_code="missing_required_fields",
                    )
                return True
            if not isinstance(args, dict):
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="invalid_args",
                )
                return True

            if not agent_turn_id:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="missing_agent_turn_id",
                )
                return True

            ui_metadata = payload.get("metadata")
            if ui_metadata is not None and not isinstance(ui_metadata, dict):
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="invalid_metadata",
                )
                return True
            if ui_metadata:
                args = dict(args)
                args_meta = args.get("metadata")
                if args_meta is not None and not isinstance(args_meta, dict):
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status="rejected",
                        headers=headers,
                        error_code="invalid_metadata",
                    )
                    return True
                merged_meta = dict(ui_metadata)
                if isinstance(args_meta, dict):
                    merged_meta.update(args_meta)
                args["metadata"] = merged_meta

            state = await self.state_store.fetch(parts.project_id, agent_id)
            default_depth = int(getattr(state, "active_recursion_depth", 0) or 0) if state else 0
            try:
                headers, _, depth = normalize_headers(
                    nats=self.nats,
                    headers=headers,
                    default_depth=default_depth,
                )
            except ProtocolViolationError as exc:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="protocol_violation",
                    error=str(exc),
                )
                return True
            depth = int(depth or 0)

            roster = await self._fetch_ui_roster(
                parts=parts,
                action_id=action_id,
                agent_id=agent_id,
                headers=headers,
            )
            if not roster:
                return True

            tool_def = await self.resource_store.fetch_tool_definition_model(parts.project_id, tool_name)
            if not tool_def:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="tool_definition_missing",
                )
                return True
            if not bool((tool_def.options or {}).get("ui_allowed")):
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="ui_not_allowed",
                )
                return True

            message_id = safe_str(payload.get("message_id")) or safe_str(args.get("message_id"))

            if state and safe_str(state.status) != "idle":
                active_turn_id = safe_str(state, "active_agent_turn_id")
                if active_turn_id and active_turn_id == agent_turn_id:
                    pass
                else:
                    logger.info("UI action rejected (agent busy) agent=%s status=%s", agent_id, state.status)
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status="busy",
                        headers=headers,
                        message_id=message_id,
                        error_code="agent_busy",
                    )
                    return True

            idem_identity = message_id or action_id
            idem_key = build_idempotency_key("ui_action", parts.project_id, tool_name, idem_identity)

            is_leader = await self.idem.try_acquire(idem_key)
            if not is_leader:
                done_payload = await self.idem.wait_result(idem_key, timeout=self.IDEM_WAIT_S)
                if isinstance(done_payload, dict):
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status=str(done_payload.get("status") or "done"),
                        headers=headers,
                        message_id=safe_str(done_payload.get("message_id")) or message_id,
                        tool_result_card_id=safe_str(done_payload.get("tool_result_card_id")),
                        error_code=safe_str(done_payload.get("error_code")),
                        error=safe_str(done_payload.get("error")),
                    )
                    return True
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="busy",
                    headers=headers,
                    message_id=message_id,
                    error_code="idempotency_inflight",
                )
                return True

            output_box_id: Optional[str] = None
            try:
                profile_box_id = self._resolve_profile_box_id(state=state, roster=roster)
                pointers = None
                try:
                    pointers = await self.state_store.fetch_pointers(parts.project_id, agent_id)
                except Exception:
                    pointers = None

                # Prefer active-turn boxes when continuing the same turn; otherwise use pointers.
                context_box_id = None
                if state and safe_str(state, "active_agent_turn_id") == agent_turn_id:
                    context_box_id = safe_str(state, "context_box_id")
                if not context_box_id:
                    context_box_id = safe_str(pointers, "memory_context_box_id") if pointers else None
                if not context_box_id:
                    async def _create_box(conn: Any = None) -> str:
                        return await self.cardbox.save_box([], project_id=parts.project_id, conn=conn)
                    context_box_id = await self.state_store.ensure_memory_context_box_id(
                        project_id=parts.project_id,
                        agent_id=agent_id,
                        ensure=True,
                        create_box=_create_box,
                    )
                output_box_id = None
                if state and safe_str(state, "active_agent_turn_id") == agent_turn_id:
                    output_box_id = safe_str(state, "output_box_id")
                if not output_box_id:
                    output_box_id = safe_str(pointers, "last_output_box_id") if pointers else None
                if not output_box_id and state:
                    # Legacy fallback: older deployments kept output_box_id on idle head rows.
                    output_box_id = safe_str(state, "output_box_id")
                output_box_id = await self.cardbox.ensure_box_id(
                    project_id=parts.project_id,
                    box_id=output_box_id,
                )

                if turn_epoch <= 0:
                    await self.idem.set_result(
                        idem_key,
                        {
                            "status": "busy",
                            "message_id": message_id,
                            "error_code": "agent_busy",
                        },
                    )
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status="busy",
                        headers=headers,
                        message_id=message_id,
                        error_code="agent_busy",
                    )
                    return True
                if not output_box_id:
                    raise RuntimeError("ui action missing turn metadata")
                updated = False
                try:
                    updated = await self.state_store.update(
                        project_id=parts.project_id,
                        agent_id=agent_id,
                        expect_turn_epoch=turn_epoch,
                        expect_agent_turn_id=agent_turn_id,
                        active_recursion_depth=depth,
                        context_box_id=context_box_id,
                        output_box_id=output_box_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "UI update active_recursion_depth failed agent=%s turn=%s: %s",
                        agent_id,
                        agent_turn_id,
                        exc,
                    )
                if not updated:
                    logger.info(
                        "UI action rejected (stale turn) agent=%s turn=%s epoch=%s",
                        agent_id,
                        agent_turn_id,
                        turn_epoch,
                    )
                    await self.idem.set_result(
                        idem_key,
                        {
                            "status": "rejected",
                            "message_id": message_id,
                            "error_code": "stale_turn",
                        },
                    )
                    await self._publish_action_ack(
                        parts=parts,
                        action_id=action_id,
                        agent_id=agent_id,
                        status="rejected",
                        headers=headers,
                        message_id=message_id,
                        error_code="stale_turn",
                    )
                    return True

                step_id = f"step_ui_{uuid6.uuid7().hex}"
                await self.step_store.insert_step(
                    project_id=parts.project_id,
                    agent_id=agent_id,
                    step_id=step_id,
                    agent_turn_id=agent_turn_id,
                    channel_id=parts.channel_id,
                    parent_step_id=parent_step_id or None,
                    trace_id=trace_id,
                    status="tool_dispatched",
                    profile_box_id=profile_box_id,
                    context_box_id=context_box_id,
                    output_box_id=output_box_id,
                )
                tool_call = {
                    "id": action_id,
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": args,
                    },
                }

                dispatch_ctx = CGContext(
                    project_id=parts.project_id,
                    channel_id=parts.channel_id,
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                    trace_id=trace_id,
                    headers=dict(headers or {}),
                    step_id=step_id,
                    parent_step_id=parent_step_id or None,
                )
                outcome = await self.dispatcher.execute(
                    tool_call=tool_call,
                    ctx=dispatch_ctx,
                    turn_epoch=int(turn_epoch),
                    context_box_id=context_box_id,
                )

                if outcome.cards:
                    card_ids = [c.card_id for c in outcome.cards]
                    await self.cardbox.append_to_box(output_box_id, card_ids, project_id=parts.project_id)

                if outcome.suspend:
                    updated = await self.state_store.update(
                        project_id=parts.project_id,
                        agent_id=agent_id,
                        expect_turn_epoch=int(turn_epoch),
                        expect_agent_turn_id=agent_turn_id,
                        new_status="suspended",
                        active_recursion_depth=depth,
                        output_box_id=output_box_id,
                        expecting_correlation_id=action_id,
                    )
                    if updated:
                        await self._emit_state_event(
                            project_id=parts.project_id,
                            channel_id=parts.channel_id,
                            agent_id=agent_id,
                            agent_turn_id=agent_turn_id,
                            turn_epoch=int(turn_epoch),
                            status="suspended",
                            output_box_id=output_box_id,
                            headers=headers,
                            metadata={
                                "activity": "awaiting_tool_result",
                                "current_tool": tool_name,
                                "expecting_correlation_id": action_id,
                            },
                        )
                    return True

                # Dispatch failed immediately; mark idle and ack rejected.
                await self.state_store.finish_turn_idle(
                    project_id=parts.project_id,
                    agent_id=agent_id,
                    expect_turn_epoch=int(turn_epoch),
                    expect_agent_turn_id=agent_turn_id,
                    last_output_box_id=output_box_id,
                )
                await self._emit_state_event(
                    project_id=parts.project_id,
                    channel_id=parts.channel_id,
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                    turn_epoch=int(turn_epoch),
                    status="idle",
                    output_box_id=output_box_id,
                    headers=headers,
                )
                await self.idem.set_result(
                    idem_key,
                    {
                        "status": "rejected",
                        "message_id": message_id,
                        "error_code": "dispatch_failed",
                    },
                )
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    message_id=message_id,
                    tool_result_card_id=None,
                    error_code="dispatch_failed",
                )
                await self._publish_idle_wakeup(parts=parts, agent_id=agent_id, headers=headers)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.error("UI action handling failed: %s", exc, exc_info=True)
                try:
                    await self.idem.delete(idem_key)
                except Exception:
                    pass
                try:
                    current_state = await self.state_store.fetch(parts.project_id, agent_id)
                    if current_state and current_state.status in ("running", "suspended", "dispatched"):
                        same_turn = (
                            current_state.active_agent_turn_id == agent_turn_id
                            and current_state.turn_epoch == turn_epoch
                        )
                        if same_turn:
                            fallback_output_box_id = (
                                output_box_id
                                or safe_str(current_state, "output_box_id")
                            )
                            await self.state_store.finish_turn_idle(
                                project_id=parts.project_id,
                                agent_id=agent_id,
                                expect_turn_epoch=current_state.turn_epoch,
                                expect_agent_turn_id=current_state.active_agent_turn_id,
                                last_output_box_id=fallback_output_box_id,
                            )
                except Exception as reset_exc:  # noqa: BLE001
                    logger.warning("UI action conditional reset failed agent=%s: %s", agent_id, reset_exc)
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    message_id=message_id,
                    tool_result_card_id=None,
                    error_code="internal_error",
                    error=str(exc),
                )
                return True

    @traced(
        _TRACER,
        "ui.handle_tool_result",
        headers_arg="headers",
        span_arg="_span",
        record_exception=True,
        set_status_on_exception=True,
    )
    async def _handle_tool_result(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ) -> None:
        payload = data or {}
        try:
            headers, _, _ = normalize_headers(
                nats=self.nats,
                headers=headers,
            )
        except ProtocolViolationError as exc:
            logger.warning("UI tool_result invalid trace headers: %s", exc)
            return

        action_id = safe_str(payload.get("tool_call_id"))
        agent_id = safe_str(payload.get("agent_id"))
        agent_turn_id = safe_str(payload.get("agent_turn_id"))
        turn_epoch = payload.get("turn_epoch")
        tool_result_card_id = safe_str(payload.get("tool_result_card_id"))
        span_attrs: Dict[str, Any] = {
            "cg.project_id": str(parts.project_id),
            "cg.channel_id": str(parts.channel_id),
            "cg.action_id": str(action_id),
            "cg.agent_id": str(agent_id),
            "cg.agent_turn_id": str(agent_turn_id),
            "cg.tool_result_card_id": str(tool_result_card_id),
            "cg.turn_epoch": int(turn_epoch) if turn_epoch is not None else None,
        }
        span = _span
        if span is not None:
            set_span_attrs(span, span_attrs)

            if not action_id or not agent_id or not agent_turn_id or turn_epoch is None:
                logger.warning("UI tool_result missing required fields: %s", payload)
                return

            if not tool_result_card_id:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="missing_tool_result_card",
                )
                return

            cards = await self.cardbox.get_cards([tool_result_card_id], project_id=parts.project_id)
            tool_result = cards[0] if cards else None
            if not tool_result:
                await self._publish_action_ack(
                    parts=parts,
                    action_id=action_id,
                    agent_id=agent_id,
                    status="rejected",
                    headers=headers,
                    error_code="tool_result_not_found",
                )
                return

        tool_meta = getattr(tool_result, "metadata", {}) or {}
        tool_name = safe_str(tool_meta.get("function_name")) or "unknown"
        dispatch_step_id = safe_str(tool_meta.get("step_id"))
        try:
            result_payload = extract_tool_result_payload(tool_result)
            tool_status = safe_str(result_payload.get("status"))
        except ProtocolViolationError as exc:
            logger.warning("UI tool_result protocol violation: %s", exc)
            result_payload = {
                "status": STATUS_FAILED,
                "after_execution": "terminate",
                "result": None,
                "error": {"code": "protocol_violation", "message": str(exc), "source": "worker"},
            }
            tool_status = STATUS_FAILED
        result_obj = result_payload.get("result")

        ack_status = "done"
        message_id = None
        error_code = None
        error_msg = None
        if isinstance(result_obj, dict):
            status_candidate = safe_str(result_obj.get("status"))
            if status_candidate in ACK_STATUS_SET:
                ack_status = status_candidate
            message_id = safe_str(result_obj.get("message_id"))
            error_code = safe_str(result_obj.get("error_code"))
            err_any = result_obj.get("error")
            if isinstance(err_any, str):
                error_msg = err_any
            elif isinstance(err_any, dict):
                error_code = error_code or safe_str(err_any.get("code"))
                error_msg = error_msg or safe_str(err_any.get("message"))
        if tool_status == STATUS_FAILED and ack_status == "done":
            ack_status = "rejected"
        if error_msg is None:
            err_any = result_payload.get("error")
            if isinstance(err_any, str):
                error_msg = err_any
            elif isinstance(err_any, dict):
                error_code = error_code or safe_str(err_any.get("code"))
                error_msg = error_msg or safe_str(err_any.get("message"))

        state = await self.state_store.fetch(parts.project_id, agent_id)
        output_box_id_src = safe_str(state, "output_box_id") if state else None
        if not output_box_id_src:
            pointers = await self.state_store.fetch_pointers(parts.project_id, agent_id)
            output_box_id_src = safe_str(pointers, "last_output_box_id") if pointers else None
        output_box_id = await self.cardbox.ensure_box_id(
            project_id=parts.project_id,
            box_id=output_box_id_src,
        )

        await self.cardbox.append_to_box(output_box_id, [tool_result_card_id], project_id=parts.project_id)

        if dispatch_step_id:
            await self.step_store.update_step(
                project_id=parts.project_id,
                agent_id=agent_id,
                step_id=dispatch_step_id,
                status="completed",
                ended=True,
                error=error_msg,
            )

        await self.state_store.finish_turn_idle(
            project_id=parts.project_id,
            agent_id=agent_id,
            expect_turn_epoch=int(turn_epoch),
            expect_agent_turn_id=agent_turn_id,
            last_output_box_id=output_box_id,
        )
        await self._emit_state_event(
            project_id=parts.project_id,
            channel_id=parts.channel_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            turn_epoch=int(turn_epoch),
            status="idle",
            output_box_id=output_box_id,
            headers=headers,
        )

        idem_identity = message_id or action_id
        idem_key = build_idempotency_key("ui_action", parts.project_id, tool_name, idem_identity)
        await self.idem.set_result(
            idem_key,
            {
                "status": ack_status,
                "message_id": message_id,
                "error_code": error_code,
                "error": error_msg,
                "output_box_id": output_box_id,
                "agent_turn_id": agent_turn_id,
                "tool_result_card_id": tool_result_card_id,
            },
        )

        await self._publish_action_ack(
            parts=parts,
            action_id=action_id,
            agent_id=agent_id,
            status=ack_status,
            headers=headers,
            message_id=message_id,
            tool_result_card_id=tool_result_card_id,
            error_code=error_code,
            error=error_msg,
        )
        await self._publish_idle_wakeup(parts=parts, agent_id=agent_id, headers=headers)

    async def _publish_idle_wakeup(
        self,
        *,
        parts: Any,
        agent_id: str,
        headers: Dict[str, str],
    ) -> None:
        await publish_idle_wakeup(
            nats=self.nats,
            resource_store=self.resource_store,
            state_store=self.state_store,
            project_id=parts.project_id,
            channel_id=parts.channel_id,
            agent_id=agent_id,
            headers=headers,
            retry_count=0,
            retry_delay=0.0,
            logger=logger,
            warn_prefix="UI idle wakeup",
        )

    @traced(
        _TRACER,
        "ui.handle_wakeup",
        headers_arg="headers",
        span_arg="_span",
        attributes_getter=_subject_span_attrs,
    )
    async def _enqueue_wakeup(
        self,
        subject: str,
        data: Dict[str, Any],
        headers: Dict[str, str],
        _span: Any = None,
    ) -> None:
        span = _span
        parts = parse_subject(subject)
        if not parts:
            logger.warning("Invalid UI wakeup subject: %s", subject)
            return
        agent_id = data.get("agent_id")
        if not agent_id:
            logger.warning("UI wakeup missing agent_id: %s", data)
            return

        rows = await self.execution_store.claim_pending_inbox(
            project_id=parts.project_id,
            agent_id=agent_id,
            limit=20,
        )
        if span is not None:
            span.set_attribute("cg.inbox_rows", int(len(rows or [])))
        if not rows:
            return

        async def _handle_row(ctx: InboxRowContext) -> InboxRowResult:
            if ctx.message_type not in ("tool_result", "ui_action"):
                return InboxRowResult(status="skipped")

            if ctx.message_type == "tool_result":
                await self._handle_tool_result(parts=parts, data=ctx.payload, headers=ctx.headers)
                return InboxRowResult(status="consumed")

            processed = await self._process_ui_action(
                parts=parts,
                payload=ctx.payload,
                headers=ctx.headers,
                inbox_id=ctx.inbox_id,
            )
            return InboxRowResult(status="consumed" if processed else "pending")

        await consume_inbox_rows(
            nats=self.nats,
            execution_store=self.execution_store,
            project_id=parts.project_id,
            rows=rows,
            handler=_handle_row,
            logger=logger,
            expected_status="processing",
            require_depth=True,
        )


async def main() -> None:
    cfg = load_app_config()
    service = UIWorkerService(cfg)
    try:
        await service.start()
    finally:
        await service.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
