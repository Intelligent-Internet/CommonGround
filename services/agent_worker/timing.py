from __future__ import annotations

from typing import Any, Dict, Optional
import time

from core.time_utils import ensure_utc, to_iso, utc_now


def duration_ms(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if start is None or end is None:
        return None
    start_utc = ensure_utc(start)
    end_utc = ensure_utc(end)
    delta = end_utc - start_utc
    return max(0, int(delta.total_seconds() * 1000))


def monotonic_ms(start: float, end: Optional[float] = None) -> int:
    if end is None:
        end = time.perf_counter()
    return max(0, int((end - start) * 1000))


def extract_llm_meta(timing: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    llm = timing.get("llm") if isinstance(timing, dict) else None
    if not isinstance(llm, dict) or not llm:
        return {}
    meta: Dict[str, Any] = {"llm_timing": llm}
    request_started_at = llm.get("request_started_at")
    response_received_at = llm.get("response_received_at")
    error_at = llm.get("error_at")
    if request_started_at:
        meta["llm_request_started_at"] = request_started_at
    if response_received_at:
        meta["llm_response_received_at"] = response_received_at
    if error_at:
        meta["llm_error_at"] = error_at
    return meta
