from __future__ import annotations

import logging
import threading
from typing import Callable

logger = logging.getLogger(__name__)


class PresenceHeartbeater:
    def __init__(self, *, heartbeat_fn: Callable[[], None], interval_seconds: float) -> None:
        self._heartbeat_fn = heartbeat_fn
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = False

    def start(self, *, emit_now: bool = True) -> None:
        if emit_now:
            self._heartbeat_fn()
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds * 2))

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._heartbeat_fn()
            except Exception:
                logger.warning("presence heartbeat failed; continuing background loop", exc_info=True)
