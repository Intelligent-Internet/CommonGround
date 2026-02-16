import os
import ssl
import json
import time
import asyncio
import logging
from dataclasses import dataclass
import nats
from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from nats.errors import TimeoutError
from datetime import datetime, timezone
from typing import Callable, Dict, Any, Optional, Set
from core.config_defaults import DEFAULT_NATS_CERT_DIR, DEFAULT_NATS_SERVERS
from core.subject import PROTOCOL_VERSION, parse_subject, subject_pattern
from core.trace import TRACEPARENT_HEADER, next_traceparent
from infra.observability.otel import (
    get_tracer,
    inject_context_to_headers,
    mark_span_error,
    traced,
)


logger = logging.getLogger("NATSClient")
_TRACER = get_tracer("infra.nats_client")


@dataclass(frozen=True)
class NATSSubscriptionHandle:
    """Handle returned by subscribe_* helper methods for lifecycle control."""

    durable_name: str
    task: asyncio.Task

    def stop(self) -> None:
        self.task.cancel()

    async def wait(self, cancel_ok: bool = True) -> None:
        """
        Wait for the underlying subscription task to finish.

        By default, stop operations (e.g., calling stop()) are treated as successful
        completion to avoid asyncio.CancelledError leaking into callers that only
        catch Exception.
        """
        try:
            await self.task
        except asyncio.CancelledError:
            if not cancel_ok:
                raise
class NATSClient:
    """Lightweight NATS/JetStream wrapper with optional TLS.

    TLS certificate directory and server addresses come from config to support local
    non-TLS testing.
    """

    def __init__(
        self,
        servers: Optional[list[str]] = None,
        cert_dir: Optional[str] = None,
        tls_enabled: Optional[bool] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        cfg = config or {}

        # servers is the single source of NATS endpoint configuration.
        raw_servers = (
            servers
            if isinstance(servers, list)
            else (cfg.get("servers") if isinstance(cfg.get("servers"), list) else None)
        )
        normalized_servers = [
            s.strip() for s in (raw_servers or []) if isinstance(s, str) and s.strip()
        ]
        self.servers = normalized_servers or list(DEFAULT_NATS_SERVERS)

        self.tls_enabled = bool(tls_enabled if tls_enabled is not None else cfg.get("tls_enabled", False))
        self.cert_dir = cert_dir or cfg.get("cert_dir") or DEFAULT_NATS_CERT_DIR

        pull_cfg = cfg.get("pull") if isinstance(cfg.get("pull"), dict) else {}
        self.pull_batch_size = max(1, int(pull_cfg.get("batch_size", 1)))
        self.pull_max_inflight = max(1, int(pull_cfg.get("max_inflight", 1)))
        self.pull_fetch_timeout_seconds = float(pull_cfg.get("fetch_timeout_seconds", 5.0))
        if self.pull_fetch_timeout_seconds <= 0:
            self.pull_fetch_timeout_seconds = 5.0
        self.pull_warmup_timeout_seconds = float(pull_cfg.get("warmup_timeout_seconds", 0.1))
        if self.pull_warmup_timeout_seconds <= 0:
            self.pull_warmup_timeout_seconds = 0.1

        self.nc = NATS()
        self.js = None
        self._tasks: Set[asyncio.Task] = set()
        self._streams_ready = False

    async def _ensure_cg_streams(self) -> None:
        if self._streams_ready:
            return
        if not self.js:
            raise Exception("NATS JetStream not connected")

        cmd_stream = f"cg_cmd_{PROTOCOL_VERSION}"
        evt_stream = f"cg_evt_{PROTOCOL_VERSION}"

        async def _exists(name: str) -> bool:
            try:
                await self.js.stream_info(name)
                return True
            except Exception:
                return False

        # cmd.* retention is 24 hours; evt.* retention is 7 days.
        if not await _exists(cmd_stream):
            try:
                await self.js.add_stream(
                    name=cmd_stream,
                    subjects=[
                        subject_pattern(
                            project_id="*",
                            channel_id="*",
                            category="cmd",
                            component="*",
                            target="*",
                            suffix=">",
                            protocol_version=PROTOCOL_VERSION,
                        )
                    ],
                    max_age=24 * 60 * 60,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to create JetStream stream {cmd_stream!r}. "
                    "If an older overlapping stream was used before, delete it first and retry."
                ) from exc
        if not await _exists(evt_stream):
            try:
                await self.js.add_stream(
                    name=evt_stream,
                    subjects=[
                        subject_pattern(
                            project_id="*",
                            channel_id="*",
                            category="evt",
                            component="*",
                            target="*",
                            suffix=">",
                            protocol_version=PROTOCOL_VERSION,
                        )
                    ],
                    max_age=7 * 24 * 60 * 60,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to create JetStream stream {evt_stream!r}. "
                    "If an older overlapping stream was used before, delete it first and retry."
                ) from exc

        self._streams_ready = True

    async def connect(self):
        ssl_ctx = None
        if self.tls_enabled:
            ca_file = os.path.join(self.cert_dir, "ca.crt")
            client_cert = os.path.join(self.cert_dir, "client.crt")
            client_key = os.path.join(self.cert_dir, "client.key")

            if not os.path.exists(ca_file):
                print(
                    f"[WARN] Certificate directory {self.cert_dir} not found; TLS connection may fail and fallback to plain mode."
                )

            ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            try:
                ssl_ctx.load_verify_locations(ca_file)
                ssl_ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)
                ssl_ctx.check_hostname = False
            except FileNotFoundError:
                print("[NATS] TLS files not found; switching to non-TLS mode in development.")
                ssl_ctx = None

        await self.nc.connect(
            servers=self.servers,
            tls=ssl_ctx,
        )
        self.js = self.nc.jetstream()
        await self._ensure_cg_streams()
        proto = "tls" if ssl_ctx else "plain"
        print(f"[NATS] Connected to {self.servers} (mode={proto})")

    @traced(
        _TRACER,
        "nats.publish",
        headers_arg="headers",
        span_arg="_span",
    )
    async def publish_event(
        self,
        subject: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        *,
        retry_count: int = 0,
        retry_delay: float = 0.0,
        _span: Any = None,
    ):
        if not self.js:
            raise Exception("NATS JetStream is not connected")
        await self._ensure_cg_streams()

        final_headers: Dict[str, str] = dict(headers or {})
        span = _span
        if span is not None:
            span.set_attribute("messaging.system", "nats")
            span.set_attribute("messaging.destination", subject)
            span.set_attribute("messaging.operation", "publish")
            parts = parse_subject(subject)
            if parts:
                span.set_attribute("cg.project_id", str(parts.project_id))
                span.set_attribute("cg.channel_id", str(parts.channel_id))
                span.set_attribute("cg.category", str(parts.category))
                span.set_attribute("cg.component", str(parts.component))
                span.set_attribute("cg.target", str(parts.target))
                span.set_attribute("cg.suffix", str(parts.suffix))

        inject_context_to_headers(final_headers)

        # Fill in missing cg-next header field conventions.
        now_ms = str(int(time.time() * 1000))
        if not final_headers.get(TRACEPARENT_HEADER):
            traceparent, _ = next_traceparent(None)
            final_headers[TRACEPARENT_HEADER] = traceparent
        final_headers.setdefault("CG-Timestamp", now_ms)
        final_headers.setdefault("CG-Version", PROTOCOL_VERSION)

        # Infer message type when the caller does not explicitly pass one.
        inferred_msg_type = None
        if isinstance(payload, dict):
            inferred_msg_type = payload.get("__msg_type__")
        if inferred_msg_type is None and hasattr(payload, "__class__"):
            inferred_msg_type = payload.__class__.__name__
        final_headers.setdefault("CG-Msg-Type", inferred_msg_type or "Payload")
        final_headers.setdefault("CG-Sender", os.getenv("CG_SENDER", "cg-nats-client"))
        if span is not None:
            span.set_attribute("messaging.message_type", str(final_headers.get("CG-Msg-Type")))

        # Guardrail: business payload should not include metadata fields.
        if isinstance(payload, dict):
            leaked_meta = [
                k for k in payload.keys() if isinstance(k, str) and k.lower().startswith("cg-")
            ]
            if leaked_meta:
                print(
                    f"[NATS] Warning: payload contains suspected header fields {leaked_meta}. "
                    "Use headers instead."
                )

        data = json.dumps(payload, default=str).encode("utf-8")
        attempts = max(0, int(retry_count)) + 1
        delay = max(0.0, float(retry_delay))
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                ack = await self.js.publish(subject, data, headers=final_headers)
                return ack
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= attempts:
                    mark_span_error(span, exc)
                    print(
                        f"[NATS] Publish failed after {attempts} retries, still failed for subject={subject}: {exc}"
                    )
                    raise
                print(
                    f"[NATS] Publish failed attempt={attempt}/{attempts}, subject={subject}: {exc} (retry after {delay}s)"
                )
                if delay:
                    await asyncio.sleep(delay)
        if last_exc:
            raise last_exc

    @staticmethod
    def merge_headers(
        upstream: Optional[Dict[str, str]],
        extras: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        merged: Dict[str, str] = dict(upstream or {})
        if extras:
            for key, value in extras.items():
                if value is None:
                    continue
                if not merged.get(key):
                    merged[key] = str(value)
        return merged

    @staticmethod
    def _warn_missing_headers(headers: Dict[str, str], subject: str) -> None:
        if not headers.get(TRACEPARENT_HEADER):
            print(f"[NATS] Warning: missing traceparent header (subject={subject})")

    def _configure_consumer(
        self,
        *,
        subject: str,
        queue_group: str,
        durable_name: Optional[str],
        deliver_policy: Optional[str],
        start_ms: Optional[int],
        ack_wait: int,
        max_deliver: int,
        backoff: Optional[list[float]] = None,
    ) -> tuple[str, ConsumerConfig]:
        if durable_name:
            final_durable = durable_name
        else:
            safe_subject = subject.replace(".", "_").replace("*", "ALL").replace(">", "REST")
            final_durable = f"{queue_group}_{safe_subject}"
            if len(final_durable) > 64:
                import hashlib

                h = hashlib.md5(subject.encode()).hexdigest()[:8]
                final_durable = f"{queue_group}_{h}"

        opt_start_time = None
        if start_ms is not None:
            opt_start_time = (
                datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            resolved_policy = DeliverPolicy.BY_START_TIME
        else:
            if deliver_policy is None:
                resolved_policy = DeliverPolicy.NEW
            elif isinstance(deliver_policy, DeliverPolicy):
                resolved_policy = deliver_policy
            else:
                policy_name = str(deliver_policy).strip().upper()
                if not hasattr(DeliverPolicy, policy_name):
                    allowed = ", ".join([p.name for p in DeliverPolicy])
                    raise ValueError(f"Invalid deliver_policy={deliver_policy!r}. Allowed values: {allowed}")
                resolved_policy = getattr(DeliverPolicy, policy_name)

        backoff_list = [float(x) for x in (backoff or []) if x is not None]
        consumer_cfg = ConsumerConfig(
            durable_name=final_durable,
            deliver_policy=resolved_policy,
            opt_start_time=opt_start_time,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=int(ack_wait),
            max_deliver=int(max_deliver),
            backoff=backoff_list or None,
        )
        return final_durable, consumer_cfg

    async def _run_worker_loop(
        self,
        *,
        sub,
        final_durable: str,
        callback: Callable,
        auto_ack: bool,
        pull_batch_size: Optional[int] = None,
        max_inflight: Optional[int] = None,
        fetch_timeout_seconds: Optional[float] = None,
        warmup_timeout_seconds: Optional[float] = None,
        warmup: bool = False,
        log_alive: bool = False,
    ) -> NATSSubscriptionHandle:
        batch_size = max(1, int(pull_batch_size if pull_batch_size is not None else self.pull_batch_size))
        inflight_limit = max(1, int(max_inflight if max_inflight is not None else self.pull_max_inflight))
        fetch_timeout = float(
            fetch_timeout_seconds if fetch_timeout_seconds is not None else self.pull_fetch_timeout_seconds
        )
        warmup_timeout = float(
            warmup_timeout_seconds if warmup_timeout_seconds is not None else self.pull_warmup_timeout_seconds
        )
        if batch_size > inflight_limit:
            logger.warning(
                "NATS pull batch_size (%s) is greater than max_inflight (%s), durable=%s; this may increase ack_wait pressure.",
                batch_size,
                inflight_limit,
                final_durable,
            )

        async def _handle_msg(msg):
            try:
                data = json.loads(msg.data.decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                if auto_ack:
                    print(f"[NATS] Failed to process message: {exc}", callback)
                    try:
                        await msg.nak()
                    except Exception:
                        pass
                else:
                    print(f"[NATS] Failed to decode message: {exc}")
                    try:
                        await msg.ack()
                    except Exception:
                        pass
                return

            headers = msg.headers or {}
            self._warn_missing_headers(headers, msg.subject)
            
            @traced(
                _TRACER,
                "nats.consume",
                headers_arg="headers",
                span_arg="_span",
            )
            async def _process_msg(*, msg: Any, data: Dict[str, Any], headers: Dict[str, str], _span: Any = None):
                span = _span
                span.set_attribute("messaging.system", "nats")
                span.set_attribute("messaging.destination", msg.subject)
                span.set_attribute("messaging.operation", "process")
                parts = parse_subject(msg.subject)
                if parts:
                    span.set_attribute("cg.project_id", str(parts.project_id))
                    span.set_attribute("cg.channel_id", str(parts.channel_id))
                    span.set_attribute("cg.category", str(parts.category))
                    span.set_attribute("cg.component", str(parts.component))
                    span.set_attribute("cg.target", str(parts.target))
                    span.set_attribute("cg.suffix", str(parts.suffix))
                if auto_ack:
                    try:
                        await callback(msg.subject, data, headers)
                        await msg.ack()
                    except Exception as exc:  # noqa: BLE001
                        mark_span_error(span, exc)
                        print(f"[NATS] Failed to process message: {exc}", callback)
                        try:
                            await msg.nak()
                        except Exception:
                            pass
                else:
                    try:
                        await callback(msg, msg.subject, data, headers)
                    except Exception as exc:  # noqa: BLE001
                        mark_span_error(span, exc)
                        print(f"[NATS] Failed to process message: {exc}", callback)
                        try:
                            await msg.nak()
                        except Exception:
                            pass

            await _process_msg(msg=msg, data=data, headers=headers)

        if warmup:
            try:
                warm_msgs = await sub.fetch(1, timeout=warmup_timeout)
                for msg in warm_msgs:
                    await _handle_msg(msg)
            except TimeoutError:
                pass
            except Exception as exc:  # noqa: BLE001
                print(f"[NATS] Warmup pull failed: {exc}")

        async def worker_loop():
            loop_counter = 0
            inflight: Set[asyncio.Task] = set()
            sem = asyncio.Semaphore(inflight_limit)

            def _done(task: asyncio.Task) -> None:
                inflight.discard(task)
                sem.release()

            try:
                while True:
                    loop_counter += 1
                    try:
                        msgs = await sub.fetch(batch_size, timeout=fetch_timeout)
                        for msg in msgs:
                            await sem.acquire()
                            task = asyncio.create_task(
                                _handle_msg(msg),
                                name=f"nats_msg:{final_durable}",
                            )
                            inflight.add(task)
                            task.add_done_callback(_done)
                    except TimeoutError:
                        if log_alive and loop_counter % 200 == 0:
                            print(f"[NATS] worker_loop running: {final_durable}")
                        continue
                    except Exception as exc:  # noqa: BLE001
                        print(f"[NATS] worker_loop error: {exc}")
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Graceful shutdown: stop pulling messages.
                pass
            finally:
                if inflight:
                    for task in list(inflight):
                        task.cancel()
                    await asyncio.gather(*list(inflight), return_exceptions=True)

        task = asyncio.create_task(worker_loop(), name=f"nats_worker:{final_durable}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return NATSSubscriptionHandle(durable_name=final_durable, task=task)

    async def subscribe_cmd(
        self,
        subject: str,
        queue_group: str,
        callback: Callable,
        durable_name: Optional[str] = None,
        *,
        deliver_policy: Optional[str] = None,
        start_ms: Optional[int] = None,
        pull_batch_size: Optional[int] = None,
        max_inflight: Optional[int] = None,
        fetch_timeout_seconds: Optional[float] = None,
    ) -> NATSSubscriptionHandle:
        if not self.js:
            raise Exception("NATS JetStream is not connected")
        await self._ensure_cg_streams()

        final_durable, consumer_cfg = self._configure_consumer(
            subject=subject,
            queue_group=queue_group,
            durable_name=durable_name,
            deliver_policy=deliver_policy,
            start_ms=start_ms,
            ack_wait=30,
            max_deliver=3,
            backoff=None,
        )

        try:
            # pull subscription
            sub = await self.js.pull_subscribe(
                subject,
                durable=final_durable,
                config=consumer_cfg,
            )
            print(f"[NATS] Established pull subscription: {subject} (Durable: {final_durable})")

            return await self._run_worker_loop(
                sub=sub,
                final_durable=final_durable,
                callback=callback,
                auto_ack=True,
                pull_batch_size=pull_batch_size,
                max_inflight=max_inflight,
                fetch_timeout_seconds=fetch_timeout_seconds,
                warmup=True,
                log_alive=True,
            )

        except Exception as e:
            print(f"[NATS] Subscription failed: {e}")
            raise

    async def subscribe_cmd_with_ack(
        self,
        subject: str,
        queue_group: str,
        callback: Callable,
        durable_name: Optional[str] = None,
        *,
        deliver_policy: Optional[str] = None,
        start_ms: Optional[int] = None,
        ack_wait: int = 30,
        max_deliver: int = 3,
        backoff: Optional[list[float]] = None,
        pull_batch_size: Optional[int] = None,
        max_inflight: Optional[int] = None,
        fetch_timeout_seconds: Optional[float] = None,
    ) -> NATSSubscriptionHandle:
        if not self.js:
            raise Exception("NATS JetStream is not connected")
        await self._ensure_cg_streams()

        final_durable, consumer_cfg = self._configure_consumer(
            subject=subject,
            queue_group=queue_group,
            durable_name=durable_name,
            deliver_policy=deliver_policy,
            start_ms=start_ms,
            ack_wait=ack_wait,
            max_deliver=max_deliver,
            backoff=backoff,
        )

        try:
            sub = await self.js.pull_subscribe(
                subject,
                durable=final_durable,
                config=consumer_cfg,
            )
            print(f"[NATS] Established pull manual-ack subscription: {subject} (Durable: {final_durable})")

            return await self._run_worker_loop(
                sub=sub,
                final_durable=final_durable,
                callback=callback,
                auto_ack=False,
                pull_batch_size=pull_batch_size,
                max_inflight=max_inflight,
                fetch_timeout_seconds=fetch_timeout_seconds,
            )

        except Exception as exc:  # noqa: BLE001
            print(f"[NATS] Subscription failed: {exc}")
            raise


    async def kv_bucket(self, bucket: str):
        """Get the KeyValue bucket instance, auto-create in development when missing."""
        if not self.js:
             raise Exception("NATS JetStream is not connected")
        try:
            return await self.js.key_value(bucket)
        except Exception:
            # Auto-create KV bucket in development.
            print(f"[NATS] Creating KV bucket: {bucket}")
            return await self.js.create_key_value(bucket=bucket, history=64, ttl=0)

    async def close(self):
        if self._tasks:
            for task in list(self._tasks):
                task.cancel()
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        await self.nc.close()

    async def publish_core(self, subject: str, payload: bytes):
        """
        Send a message via Core NATS (single-shot, non-persistent).

        This method sends directly through the NATS core protocol without JetStream
        persistence, streaming, or reliable delivery features. It is suitable for
        lightweight, short-lived messages.

        Args:
            subject (str): NATS subject to publish to.
            payload (bytes): Raw message payload.

        Returns:
            None

        Difference from publish_event:
            - publish_core uses Core NATS (no JetStream features, no persistence, no headers).
            - publish_event uses JetStream (supports persistence, headers, and publish acknowledgements).
        """
        if not self.nc.is_connected:
             raise Exception("NATS is not connected")
        await self.nc.publish(subject, payload)

    async def subscribe_core(self, subject: str, callback: Callable):
        """
        Subscribe using Core NATS for a subject (non-persistent, at-most-once delivery).

        This method creates a subscription using the Core NATS protocol, without JetStream
        persistence, consumer groups, or replay capabilities; each arrived message triggers
        the callback.

        Args:
            subject (str): NATS subject to subscribe to.
            callback (Callable): Async callback with signature (msg) -> Awaitable.

        Returns:
            Subscription: NATS subscription object.

        Difference from subscribe_cmd:
            - subscribe_core uses Core NATS (no JetStream, no queue groups, no persistence).
            - subscribe_cmd uses JetStream (supports queue groups, persistence, and acknowledgements).
        """
        if not self.nc.is_connected:
             raise Exception("NATS is not connected")
        return await self.nc.subscribe(subject, cb=callback)

    async def request_core(self, subject: str, payload: bytes, *, timeout: float = 5.0):
        """Perform request-response with Core NATS."""
        if not self.nc.is_connected:
            raise Exception("NATS is not connected")
        return await self.nc.request(subject, payload, timeout=timeout)
