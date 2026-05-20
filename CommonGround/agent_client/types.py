from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from CommonGround.contracts import ClaimToken, TurnOutcome, TurnRef
from CommonGround.payloads import UNSET_PAYLOAD
from CommonGround.sdk import TurnContext


@dataclass(frozen=True, slots=True)
class ClaimedTurn:
    claim: ClaimToken
    context: TurnContext


class ClaimTurnPartialFailure(RuntimeError):
    def __init__(
        self,
        *,
        claim: ClaimToken,
        context_error: Exception,
        suspend_error: Exception | None = None,
    ) -> None:
        self.claim = claim
        self.context_error = context_error
        self.suspend_error = suspend_error
        super().__init__(str(context_error))


@dataclass(frozen=True, slots=True)
class FinishTurnAction:
    outcome: TurnOutcome
    final_payload: Any = UNSET_PAYLOAD
    final_record_role: str = "deliverable"


@dataclass(frozen=True, slots=True)
class SuspendTurnAction:
    reason: str
    note: str | None = None


@dataclass(frozen=True, slots=True)
class RetryTurnAction:
    reason: str = "retry"
    note: str | None = None


WorkerAction = FinishTurnAction | SuspendTurnAction | RetryTurnAction | None


class TurnHandler(Protocol):
    def handle_turn(self, context: TurnContext, client, claim: ClaimToken) -> WorkerAction:
        ...


@dataclass(frozen=True, slots=True)
class WorkerRunResult:
    claimed_turn: TurnRef | None
    action: Literal["idle", "finished", "stopped", "suspended", "retry", "safe_stop", "already_closed"]
