from __future__ import annotations

import logging

from CommonGround.contracts import AgentRef, OperationMeta, TurnOutcome, TurnState

from .claim_renewer import ClaimAutoRenewer, ClaimLeaseLostError
from .http_client import HttpAgentClient
from .safe_suspend import try_suspend_after_context_fetch_error
from .types import FinishTurnAction, RetryTurnAction, SuspendTurnAction, TurnHandler, WorkerRunResult

logger = logging.getLogger(__name__)


class _LeaseAwareClient:
    def __init__(self, client: HttpAgentClient, renewer: ClaimAutoRenewer) -> None:
        self._client = client
        self._renewer = renewer

    def __getattr__(self, name: str):
        target = getattr(self._client, name)
        if not callable(target):
            return target

        def _wrapped(*args, **kwargs):
            self._renewer.raise_if_unhealthy()
            return target(*args, **kwargs)

        return _wrapped


class PollingWorker:
    def __init__(self, *, client: HttpAgentClient, agent: AgentRef, handler: TurnHandler, claim_heartbeat_interval_seconds: float = 0.5) -> None:
        self._client = client
        self._agent = agent
        self._handler = handler
        self._claim_heartbeat_interval_seconds = claim_heartbeat_interval_seconds

    def run_once(self) -> WorkerRunResult:
        claim = self._client.claim_turn_handle(self._agent)
        if claim is None:
            return WorkerRunResult(claimed_turn=None, action="idle")
        turn = claim.turn_ref()
        logger.info("claimed turn project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
        if self._client.is_stop_requested(turn):
            self._client.finish_turn(claim, outcome=TurnOutcome.STOPPED, final_payload={"status": "stopped"})
            logger.info("finished stopped turn project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            return WorkerRunResult(claimed_turn=turn, action="stopped")
        heartbeater = ClaimAutoRenewer(self._client, claim=claim, interval_seconds=self._claim_heartbeat_interval_seconds)
        heartbeater.start()
        try:
            context = self._client.fetch_context(turn)
        except Exception as exc:
            logger.exception("context fetch failed project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            suspend_error = try_suspend_after_context_fetch_error(
                self._client,
                claim,
                exc,
                before_suspend=heartbeater.raise_if_unhealthy,
            )
            if isinstance(suspend_error, ClaimLeaseLostError):
                logger.warning("claim lease lost before context-fetch safe-stop project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            heartbeater.stop()
            if suspend_error is not None and not isinstance(suspend_error, ClaimLeaseLostError):
                raise suspend_error
            return WorkerRunResult(claimed_turn=turn, action="safe_stop")
        lease_aware_client = _LeaseAwareClient(self._client, heartbeater)
        try:
            action = self._handler.handle_turn(context, lease_aware_client, claim)
        except ClaimLeaseLostError:
            heartbeater.stop()
            logger.warning("claim lease lost during handler project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            return WorkerRunResult(claimed_turn=turn, action="safe_stop")
        except Exception as exc:
            heartbeater.stop()
            logger.exception("turn handler raised project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            if heartbeater.fatal_error() is not None:
                logger.warning("claim lease lost before safe-stop project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
                return WorkerRunResult(claimed_turn=turn, action="safe_stop")
            self._client.suspend_turn(claim, reason="handler_error", note=str(exc), meta=OperationMeta(reason="handler_error", note=str(exc)))
            return WorkerRunResult(claimed_turn=turn, action="safe_stop")
        heartbeater.stop()
        if heartbeater.fatal_error() is not None:
            logger.warning("claim lease lost after handler project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            return WorkerRunResult(claimed_turn=turn, action="safe_stop")
        snapshot = self._client.get_turn(turn)
        if snapshot.state == TurnState.CLOSED:
            logger.info("turn already closed project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            return WorkerRunResult(claimed_turn=turn, action="already_closed")
        if snapshot.state == TurnState.SUSPENDED:
            logger.info("turn already suspended project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
            return WorkerRunResult(claimed_turn=turn, action="suspended")
        if isinstance(action, FinishTurnAction):
            self._client.finish_turn(claim, outcome=action.outcome, final_payload=action.final_payload, final_record_role=action.final_record_role)
            logger.info("finished turn project=%s turn=%s agent=%s outcome=%s", claim.project_id, claim.turn_id, claim.agent_id, action.outcome.value)
            return WorkerRunResult(claimed_turn=turn, action="finished")
        if isinstance(action, SuspendTurnAction):
            self._client.suspend_turn(claim, reason=action.reason, note=action.note)
            logger.info("suspended turn project=%s turn=%s agent=%s reason=%s", claim.project_id, claim.turn_id, claim.agent_id, action.reason)
            return WorkerRunResult(claimed_turn=turn, action="suspended")
        if isinstance(action, RetryTurnAction):
            self._client.suspend_turn(claim, reason=action.reason, note=action.note)
            logger.info("retry-suspended turn project=%s turn=%s agent=%s reason=%s", claim.project_id, claim.turn_id, claim.agent_id, action.reason)
            return WorkerRunResult(claimed_turn=turn, action="retry")
        self._client.suspend_turn(
            claim,
            reason="handler_no_action",
            note="handler returned no action while claim remained active",
            meta=OperationMeta(reason="handler_no_action", note="handler returned no action while claim remained active"),
        )
        logger.warning("handler returned no action project=%s turn=%s agent=%s", claim.project_id, claim.turn_id, claim.agent_id)
        return WorkerRunResult(claimed_turn=turn, action="safe_stop")
