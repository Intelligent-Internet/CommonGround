from __future__ import annotations

import logging
import threading

import httpx

from CommonGround.contracts import ClaimToken

from .http_client import HttpAgentClient

logger = logging.getLogger(__name__)


class ClaimLeaseLostError(RuntimeError):
    pass


class ClaimAutoRenewer:
    def __init__(
        self,
        client: HttpAgentClient,
        *,
        claim: ClaimToken,
        interval_seconds: float,
        max_consecutive_failures: int = 3,
    ) -> None:
        self._client = client
        self._claim = claim
        self._interval_seconds = interval_seconds
        self._max_consecutive_failures = max_consecutive_failures
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = False
        self._consecutive_failures = 0
        self._fatal_error: ClaimLeaseLostError | None = None

    def start(self) -> None:
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds * 2))

    def fatal_error(self) -> ClaimLeaseLostError | None:
        return self._fatal_error

    def raise_if_unhealthy(self) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                renewed = self._client.renew_claim(self._claim)
            except Exception as exc:
                self._consecutive_failures += 1
                logger.warning(
                    "claim auto-renew failed project=%s turn=%s agent=%s",
                    self._claim.project_id,
                    self._claim.turn_id,
                    self._claim.agent_id,
                    exc_info=True,
                )
                if self._is_definitive_lease_loss(exc) or self._consecutive_failures >= self._max_consecutive_failures:
                    self._fatal_error = ClaimLeaseLostError(
                        f"claim auto-renew lost lease after {self._consecutive_failures} consecutive failures "
                        f"for {self._claim.project_id}/{self._claim.turn_id}"
                    )
                    return
                continue
            self._consecutive_failures = 0
            self._interval_seconds = max(0.01, float(renewed.recommended_interval_seconds))

    @staticmethod
    def _is_definitive_lease_loss(exc: Exception) -> bool:
        return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403, 409, 410}
