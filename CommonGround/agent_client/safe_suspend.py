from __future__ import annotations

from collections.abc import Callable

from CommonGround.contracts.models import ClaimToken, OperationMeta


CONTEXT_FETCH_ERROR_REASON = "context_fetch_error"


def try_suspend_after_context_fetch_error(
    client,
    claim: ClaimToken,
    exc: Exception,
    *,
    before_suspend: Callable[[], None] | None = None,
) -> Exception | None:
    try:
        if before_suspend is not None:
            before_suspend()
        client.suspend_turn(
            claim,
            reason=CONTEXT_FETCH_ERROR_REASON,
            note=str(exc),
            meta=OperationMeta(reason=CONTEXT_FETCH_ERROR_REASON, note=str(exc)),
        )
    except Exception as suspend_exc:
        return suspend_exc
    return None
