from __future__ import annotations

from .caller_headers import agent_auth_headers


def __getattr__(name: str):
    if name in {"ClaimAutoRenewer", "ClaimLeaseLostError"}:
        from .claim_renewer import ClaimAutoRenewer, ClaimLeaseLostError

        return {"ClaimAutoRenewer": ClaimAutoRenewer, "ClaimLeaseLostError": ClaimLeaseLostError}[name]
    if name == "HttpAgentClient":
        from .http_client import HttpAgentClient

        return HttpAgentClient
    if name == "PollingWorker":
        from .polling_worker import PollingWorker

        return PollingWorker
    if name in {
        "ClaimTurnPartialFailure",
        "ClaimedTurn",
        "FinishTurnAction",
        "RetryTurnAction",
        "SuspendTurnAction",
        "WorkerRunResult",
    }:
        from .types import ClaimTurnPartialFailure, ClaimedTurn, FinishTurnAction, RetryTurnAction, SuspendTurnAction, WorkerRunResult

        return {
            "ClaimTurnPartialFailure": ClaimTurnPartialFailure,
            "ClaimedTurn": ClaimedTurn,
            "FinishTurnAction": FinishTurnAction,
            "RetryTurnAction": RetryTurnAction,
            "SuspendTurnAction": SuspendTurnAction,
            "WorkerRunResult": WorkerRunResult,
        }[name]
    raise AttributeError(name)

__all__ = [
    "ClaimAutoRenewer",
    "ClaimLeaseLostError",
    "ClaimTurnPartialFailure",
    "ClaimedTurn",
    "FinishTurnAction",
    "HttpAgentClient",
    "PollingWorker",
    "agent_auth_headers",
    "RetryTurnAction",
    "SuspendTurnAction",
    "WorkerRunResult",
]
