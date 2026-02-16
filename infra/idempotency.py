from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import KeyWrongLastSequenceError

from infra.nats_client import NATSClient

logger = logging.getLogger(__name__)


_SAFE_SEGMENT_RE = re.compile(r"[^A-Za-z0-9_-]+")


def _safe_key_segment(value: Any) -> str:
    text = "" if value is None else str(value)
    safe = _SAFE_SEGMENT_RE.sub("_", text)
    return safe or "_"


def build_idempotency_key(namespace: str, project_id: str, domain: str, identity: str) -> str:
    safe_namespace = _safe_key_segment(namespace)
    safe_project_id = _safe_key_segment(project_id)
    safe_domain = _safe_key_segment(domain)
    identity_hash = hashlib.sha256(str(identity).encode("utf-8")).hexdigest()
    return f"{safe_namespace}.{safe_project_id}.{safe_domain}.{identity_hash}"


@dataclass(frozen=True)
class IdempotencyOutcome:
    is_leader: bool
    payload: Any


class IdempotentExecutor:
    def __init__(
        self,
        store: "NatsKvIdempotencyStore",
        *,
        timeout_s: float = 60.0,
        retry_attempts: int = 1,
        retry_backoff_s: float = 0.5,
        failure_payload_factory: Optional[Callable[[Exception], Any]] = None,
    ):
        if retry_attempts < 1:
            raise ValueError("retry_attempts must be >= 1")
        self.store = store
        self.timeout_s = timeout_s
        self.retry_attempts = retry_attempts
        self.retry_backoff_s = retry_backoff_s
        self.failure_payload_factory = failure_payload_factory

    async def execute(
        self,
        key: str,
        leader_fn: Callable[[], Awaitable[Any]],
    ) -> IdempotencyOutcome:
        is_leader = await self.store.try_acquire(key)
        if not is_leader:
            payload = await self.store.wait_result(key, timeout=self.timeout_s)
            if payload is None:
                raise TimeoutError(f"Idempotency wait timed out for key={key}")
            return IdempotencyOutcome(is_leader=False, payload=payload)

        payload = await self._run_with_retries(leader_fn)
        await self.store.set_result(key, payload)
        return IdempotencyOutcome(is_leader=True, payload=payload)

    async def _run_with_retries(
        self,
        leader_fn: Callable[[], Awaitable[Any]],
    ) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(self.retry_attempts):
            try:
                return await leader_fn()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt + 1 >= self.retry_attempts:
                    if self.failure_payload_factory:
                        return self.failure_payload_factory(exc)
                    raise
                await asyncio.sleep(self.retry_backoff_s * (2**attempt))
        if last_exc:
            raise last_exc
        raise RuntimeError("Idempotent executor reached an unexpected state")


class NatsKvIdempotencyStore:
    def __init__(
        self,
        nats_client: NATSClient,
        *,
        bucket_name: str = "cg_idem_v1",
        ttl_s: Optional[int] = None,
        payload_max_bytes: Optional[int] = None,
        poll_interval_s: float = 0.5,
    ):
        self.nats = nats_client
        self.bucket_name = bucket_name
        self.ttl_s = ttl_s
        self.payload_max_bytes = payload_max_bytes
        self.poll_interval_s = poll_interval_s
        self.kv = None

    async def _ensure_kv(self):
        if self.kv:
            return
        if self.ttl_s is None:
            self.kv = await self.nats.kv_bucket(self.bucket_name)
            return
        if not self.nats.js:
            raise RuntimeError("NATS JetStream not connected")
        try:
            self.kv = await self.nats.js.key_value(self.bucket_name)
        except Exception:
            logger.info(
                "Idempotency creating KV bucket bucket=%s ttl_s=%s",
                self.bucket_name,
                self.ttl_s,
            )
            self.kv = await self.nats.js.create_key_value(
                bucket=self.bucket_name,
                history=64,
                ttl=int(self.ttl_s),
            )

    def _encode(self, value: Any) -> bytes:
        raw = json.dumps(value, default=str).encode("utf-8")
        if self.payload_max_bytes is not None and len(raw) > self.payload_max_bytes:
            raise ValueError(
                f"payload size {len(raw)} exceeds limit {self.payload_max_bytes} bytes"
            )
        return raw

    async def try_acquire(self, key: str) -> bool:
        await self._ensure_kv()
        payload = {
            "status": "running",
            "timestamp": time.time(),
        }
        try:
            await self.kv.create(key, self._encode(payload))
            return True
        except KeyWrongLastSequenceError:
            return False
        except Exception as exc:  # noqa: BLE001
            logger.warning("Idempotency acquire failed key=%s: %s", key, exc)
            return False

    async def set_result(self, key: str, payload: Any) -> None:
        await self._ensure_kv()
        value = {
            "status": "done",
            "timestamp": time.time(),
            "payload": payload,
        }
        await self.kv.put(key, self._encode(value))

    async def get_entry(self, key: str) -> Optional[dict]:
        await self._ensure_kv()
        try:
            entry = await self.kv.get(key)
        except Exception:
            return None
        if entry and entry.value:
            try:
                return json.loads(entry.value.decode())
            except Exception:
                return None
        return None

    async def delete(self, key: str) -> None:
        await self._ensure_kv()
        try:
            await self.kv.delete(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Idempotency delete failed key=%s: %s", key, exc)

    async def wait_result(self, key: str, timeout: float = 60.0) -> Optional[Any]:
        await self._ensure_kv()
        timeout_s = max(0.0, float(timeout))
        initial = await self.get_entry(key)
        if initial and initial.get("status") == "done":
            return initial.get("payload")
        if timeout_s <= 0:
            return None

        deadline = time.monotonic() + timeout_s
        watcher = None
        try:
            # Event-driven wait: follow updates for this key instead of polling kv.get.
            watcher = await self.kv.watch(
                key,
                include_history=False,
                ignore_deletes=True,
                inactive_threshold=max(30.0, timeout_s + 5.0),
            )
        except Exception as exc:  # noqa: BLE001
            # Fallback path only if watch cannot be created.
            logger.warning("Idempotency watch setup failed key=%s: %s", key, exc)

        if watcher is None:
            while time.monotonic() < deadline:
                entry = await self.get_entry(key)
                if entry and entry.get("status") == "done":
                    return entry.get("payload")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(self.poll_interval_s, max(0.05, remaining)))
            return None

        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                try:
                    update = await watcher.updates(timeout=remaining)
                except NatsTimeoutError:
                    break
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Idempotency watch update failed key=%s: %s", key, exc)
                    break

                # None is only an initialization marker from nats-py watcher.
                if update is None:
                    continue

                try:
                    data = json.loads(update.value.decode()) if getattr(update, "value", None) else {}
                except Exception:
                    data = {}
                if data.get("status") == "done":
                    return data.get("payload")
        finally:
            try:
                await watcher.stop()
            except Exception:
                pass

        final = await self.get_entry(key)
        if final and final.get("status") == "done":
            return final.get("payload")
        return None
