import logging
import uuid6
from typing import Dict, Optional

from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ResourceStore, StateStore, StepStore
from core.utils import safe_str
from core.errors import ProtocolViolationError
from core.trace import ensure_trace_headers
from infra.worker_helpers import TurnGuard, publish_idle_wakeup
from infra.event_emitter import emit_agent_state, emit_agent_task

from .helpers.step_recorder import StepRecorder
from .helpers.turn_finisher import TurnFinisher
from .models import AgentTurnWorkItem
from .react_context import build_card_metadata
from .tx_utils import state_store_transaction


logger = logging.getLogger("StopHandler")


class StopHandler:
    """Handle stop inbox messages to cancel an active AgentTurn."""

    def __init__(
        self,
        *,
        state_store: StateStore,
        resource_store: ResourceStore,
        cardbox: CardBoxClient,
        nats: NATSClient,
        step_store: StepStore,
        timing_step_event: bool = False,
        timing_task_event: bool = False,
    ) -> None:
        self.state_store = state_store
        self.step_store = step_store
        self.resource_store = resource_store
        self.cardbox = cardbox
        self.nats = nats
        self.step_recorder = StepRecorder(
            step_store=self.step_store,
            nats=self.nats,
            timing_step_event=timing_step_event,
        )
        self.turn_finisher = TurnFinisher(
            state_store=self.state_store,
            resource_store=self.resource_store,
            cardbox=self.cardbox,
            nats=self.nats,
            step_recorder=self.step_recorder,
            timing_task_event=timing_task_event,
            card_metadata_builder=self._card_metadata,
            logger=logger,
            emit_state_fn=emit_agent_state,
            emit_task_fn=emit_agent_task,
            idle_wakeup_fn=publish_idle_wakeup,
        )

    async def handle(self, item: AgentTurnWorkItem) -> None:
        data: Dict[str, Optional[object]] = item.stop_data or {}
        reason = safe_str(data.get("reason")) or "stopped_by_pmo"

        state = await self.state_store.fetch(item.project_id, item.agent_id)
        if not state:
            logger.warning("Stop: State not found for %s", item.agent_id)
            return
        if getattr(state, "parent_step_id", None):
            item.parent_step_id = safe_str(state.parent_step_id)
        state_trace_id = getattr(state, "trace_id", None)
        if state_trace_id:
            item.trace_id = safe_str(state_trace_id)
            try:
                item.headers, _, _ = ensure_trace_headers(item.headers, trace_id=item.trace_id)
            except ProtocolViolationError as exc:
                logger.warning("Stop: invalid trace_id: %s", exc)

        command_turn_epoch = int(item.turn_epoch or 0)
        guard = TurnGuard(agent_turn_id=item.agent_turn_id, turn_epoch=command_turn_epoch)
        if not guard.matches_same_turn_allow_newer_epoch(state):
            logger.warning(
                "Stop: Guard failed (%s). Ignoring.",
                guard.mismatch_detail(state),
            )
            return
        matched_turn_epoch = int(state.turn_epoch)
        if matched_turn_epoch > command_turn_epoch:
            logger.info(
                "Stop: accepted stale epoch command turn=%s cmd_epoch=%s state_epoch=%s",
                item.agent_turn_id,
                command_turn_epoch,
                matched_turn_epoch,
            )

        item.turn_epoch = matched_turn_epoch
        item.output_box_id = safe_str(state.output_box_id)
        step_id = f"step_stop_{uuid6.uuid7().hex}"
        await self.step_recorder.start_step(
            item=item,
            step_id=step_id,
            status="tool_dispatched",
            profile_box_id=safe_str(getattr(state, "profile_box_id", None)),
            context_box_id=safe_str(getattr(state, "context_box_id", None)),
            output_box_id=safe_str(state.output_box_id),
        )

        finish_effects = None

        class _TurnCasMiss(Exception):
            pass

        try:
            async with state_store_transaction(self.state_store) as tx:
                finish_effects = await self.turn_finisher.fail_turn(
                    item=item,
                    step_id=step_id,
                    reason=reason,
                    error_code="stopped",
                    conn=tx.conn,
                    expect_turn_epoch=matched_turn_epoch,
                    expect_agent_turn_id=item.agent_turn_id,
                    bump_epoch=True,
                    append_error_card=True,
                    error_author_id="system_pmo",
                    fallback_source="agent_worker.stop_handler",
                )
                if finish_effects is None:
                    raise _TurnCasMiss("stop turn: finish_turn_idle CAS failed")
        except _TurnCasMiss:
            logger.warning("Stop: CAS failed, state changed?")
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stop: transactional rollback triggered: %s", exc)
            return

        try:
            assert finish_effects is not None
            await self.turn_finisher.publish_finish_effects(finish_effects)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stop: post-commit effects failed: %s", exc)
            return
        logger.info("Stop: canceled turn=%s epoch=%s agent=%s", item.agent_turn_id, matched_turn_epoch, item.agent_id)

    @staticmethod
    def _card_metadata(
        item: AgentTurnWorkItem,
        *,
        step_id: str,
        role: str,
        extra: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        normalized_extra = dict(extra or {})
        stop_reason = safe_str(normalized_extra.pop("terminal_reason", None))
        if stop_reason:
            normalized_extra["stop_reason"] = stop_reason
        metadata = build_card_metadata(
            item=item,
            step_id=step_id,
            role=role,
            extra=normalized_extra,
        )
        if item.turn_epoch is not None:
            metadata["resume_turn_epoch"] = int(item.turn_epoch)
        return metadata
