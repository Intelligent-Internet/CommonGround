from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
import inspect
import logging
import os
import threading
from typing import Any, Callable, Dict, Iterator, Mapping, MutableMapping, Optional, Sequence


logger = logging.getLogger("OTel")


class _NoopSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        _ = (key, value)

    def record_exception(self, exc: BaseException) -> None:
        _ = exc

    def set_status(self, status: Any) -> None:
        _ = status


class _NoopSpanContext:
    def __enter__(self) -> _NoopSpan:
        return _NoopSpan()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        _ = (exc_type, exc, tb)
        return False


class _NoopTracer:
    def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> _NoopSpanContext:
        _ = (name, args, kwargs)
        return _NoopSpanContext()


_NOOP_TRACER = _NoopTracer()

try:
    from opentelemetry import trace
    from opentelemetry.propagate import extract as otel_extract
    from opentelemetry.propagate import inject as otel_inject
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.trace import Link, SpanContext, Status, StatusCode, TraceFlags

    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover - fallback when OTel is absent
    trace = None  # type: ignore[assignment]
    Resource = None  # type: ignore[assignment]
    TracerProvider = None  # type: ignore[assignment]
    BatchSpanProcessor = None  # type: ignore[assignment]
    ParentBased = None  # type: ignore[assignment]
    TraceIdRatioBased = None  # type: ignore[assignment]
    OTLPSpanExporter = None  # type: ignore[assignment]
    otel_extract = None  # type: ignore[assignment]
    otel_inject = None  # type: ignore[assignment]
    Link = None  # type: ignore[assignment]
    SpanContext = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment]
    StatusCode = None  # type: ignore[assignment]
    TraceFlags = None  # type: ignore[assignment]
    _OTEL_AVAILABLE = False


_state_lock = threading.Lock()
_otel_initialized = False
_otel_enabled = False
_otel_provider: Any = None
_otel_ref_count = 0


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        ratio = float(value)
    except Exception:
        return default
    if ratio < 0.0:
        return 0.0
    if ratio > 1.0:
        return 1.0
    return ratio


def _coerce_str(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _resolve_otel_cfg(cfg: Mapping[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(cfg, Mapping):
        return {}
    obs = cfg.get("observability")
    if not isinstance(obs, Mapping):
        return {}
    otel_cfg = obs.get("otel")
    if not isinstance(otel_cfg, Mapping):
        return {}
    return dict(otel_cfg)


def init_otel(
    *,
    cfg: Mapping[str, Any] | None = None,
    service_name: Optional[str] = None,
) -> bool:
    global _otel_initialized
    global _otel_enabled
    global _otel_provider
    global _otel_ref_count

    with _state_lock:
        if _otel_initialized and _otel_enabled:
            _otel_ref_count += 1
            return _otel_enabled
        if _otel_initialized and not _otel_enabled:
            return False

        _otel_initialized = True
        otel_cfg = _resolve_otel_cfg(cfg)
        enabled = _coerce_bool(
            otel_cfg.get("enabled"),
            default=False,
        )
        if not enabled:
            _otel_enabled = False
            return False

        if not _OTEL_AVAILABLE:
            logger.warning("OpenTelemetry is enabled in config but SDK is unavailable")
            _otel_enabled = False
            return False

        resolved_service_name = (
            _coerce_str(service_name)
            or _coerce_str(otel_cfg.get("service_name"))
            or _coerce_str(os.getenv("CG_SERVICE_NAME"))
            or _coerce_str(os.getenv("OTEL_SERVICE_NAME"))
            or "CommonGround"
        )
        service_namespace = (
            _coerce_str(otel_cfg.get("service_namespace"))
            or _coerce_str(os.getenv("OTEL_SERVICE_NAMESPACE"))
            or "commonground"
        )
        service_version = (
            _coerce_str(otel_cfg.get("service_version"))
            or _coerce_str(os.getenv("CG_SERVICE_VERSION"))
            or "0.1.0"
        )
        otlp_endpoint = (
            _coerce_str(otel_cfg.get("otlp_endpoint"))
            or _coerce_str(os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
            or _coerce_str(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
        )
        sampler_ratio = _coerce_float(otel_cfg.get("sampler_ratio"), default=1.0)

        resource = Resource.create(
            {
                "service.name": resolved_service_name,
                "service.namespace": service_namespace,
                "service.version": service_version,
            }
        )
        sampler = ParentBased(TraceIdRatioBased(sampler_ratio))
        provider = TracerProvider(resource=resource, sampler=sampler)

        exporter_kwargs: Dict[str, Any] = {}
        if otlp_endpoint:
            exporter_kwargs["endpoint"] = otlp_endpoint
        exporter = OTLPSpanExporter(**exporter_kwargs)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

        try:
            trace.set_tracer_provider(provider)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OpenTelemetry set_tracer_provider failed: %s", exc)

        _otel_provider = provider
        _otel_enabled = True
        _otel_ref_count = 1
        logger.info(
            "OpenTelemetry enabled service=%s endpoint=%s sampler_ratio=%s",
            resolved_service_name,
            otlp_endpoint or "default",
            sampler_ratio,
        )
        return True


def shutdown_otel() -> None:
    global _otel_ref_count

    with _state_lock:
        if _otel_ref_count > 0:
            _otel_ref_count -= 1
        if _otel_ref_count > 0:
            return
        provider = _otel_provider

    if provider and hasattr(provider, "force_flush"):
        try:
            provider.force_flush()
        except Exception as exc:  # noqa: BLE001
            logger.warning("OpenTelemetry flush failed: %s", exc)


def is_otel_enabled() -> bool:
    return _otel_enabled


def get_tracer(name: str) -> Any:
    if _OTEL_AVAILABLE and trace is not None:
        return trace.get_tracer(name)
    _ = name
    return _NOOP_TRACER


def span_kwargs_from_headers(
    headers: Mapping[str, str] | None,
    *,
    include_links: bool = False,
    extra_links: Sequence[Any] | None = None,
    context: Any = None,
) -> Dict[str, Any]:
    span_kwargs: Dict[str, Any] = {}
    resolved_context = context if context is not None else extract_context_from_headers(headers)
    if resolved_context is not None:
        span_kwargs["context"] = resolved_context
    links: list[Any] = []
    if include_links:
        links.extend(links_from_headers(headers))
    if extra_links:
        links.extend(link for link in extra_links if link is not None)
    if links:
        span_kwargs["links"] = links
    return span_kwargs


def _clean_span_attributes(attributes: Mapping[str, Any] | None) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    if not attributes:
        return cleaned
    for key, value in attributes.items():
        if value is None:
            continue
        cleaned[str(key)] = value
    return cleaned


def set_span_attrs(span: Any, attributes: Mapping[str, Any] | None) -> None:
    if span is None or not attributes:
        return
    for key, value in _clean_span_attributes(attributes).items():
        try:
            span.set_attribute(key, value)
        except Exception:
            continue


@contextmanager
def start_span(
    tracer: Any,
    name: str,
    *,
    headers: Mapping[str, str] | None = None,
    attributes: Mapping[str, Any] | None = None,
    include_links: bool = False,
    extra_links: Sequence[Any] | None = None,
    context: Any = None,
    record_exception: Optional[bool] = None,
    set_status_on_exception: Optional[bool] = None,
    mark_error_on_exception: bool = False,
    **kwargs: Any,
) -> Iterator[Any]:
    span_kwargs: Dict[str, Any] = dict(kwargs)
    extracted_kwargs = span_kwargs_from_headers(
        headers,
        include_links=include_links,
        extra_links=extra_links,
        context=context,
    )
    for key, value in extracted_kwargs.items():
        span_kwargs.setdefault(key, value)
    cleaned_attrs = _clean_span_attributes(attributes)
    if cleaned_attrs:
        span_kwargs["attributes"] = cleaned_attrs
    if record_exception is not None:
        span_kwargs["record_exception"] = bool(record_exception)
    if set_status_on_exception is not None:
        span_kwargs["set_status_on_exception"] = bool(set_status_on_exception)
    with tracer.start_as_current_span(name, **span_kwargs) as span:
        try:
            yield span
        except Exception as exc:
            if mark_error_on_exception:
                mark_span_error(span, exc)
            raise


def traced(
    tracer: Any,
    name: Optional[str] = None,
    *,
    name_getter: Optional[Callable[[Mapping[str, Any]], str]] = None,
    headers_arg: Optional[str] = None,
    include_links: bool = False,
    extra_links: Sequence[Any] | None = None,
    context_arg: Optional[str] = None,
    record_exception: Optional[bool] = None,
    set_status_on_exception: Optional[bool] = None,
    mark_error_on_exception: bool = False,
    span_arg: Optional[str] = None,
    attributes_getter: Optional[Callable[[Mapping[str, Any]], Mapping[str, Any] | None]] = None,
    **span_kwargs: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        signature = inspect.signature(func)
        fixed_name = str(name).strip() if name is not None else ""

        def _resolve_span_options(
            args: tuple[Any, ...],
            kwargs: Dict[str, Any],
        ) -> tuple[str, Any, Any, Any]:
            bound = signature.bind_partial(*args, **kwargs)
            arguments = dict(bound.arguments)
            resolved_name = fixed_name
            if name_getter is not None:
                resolved_name = str(name_getter(arguments)).strip()
            if not resolved_name:
                raise ValueError("traced requires a non-empty span name")
            headers = arguments.get(headers_arg) if headers_arg else None
            context = arguments.get(context_arg) if context_arg else None
            attributes = attributes_getter(arguments) if attributes_getter else None
            return resolved_name, headers, context, attributes

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def _wrapped_async(*args: Any, **kwargs: Any) -> Any:
                resolved_name, headers, context, attributes = _resolve_span_options(args, kwargs)
                with start_span(
                    tracer,
                    resolved_name,
                    headers=headers,
                    attributes=attributes,
                    include_links=include_links,
                    extra_links=extra_links,
                    context=context,
                    record_exception=record_exception,
                    set_status_on_exception=set_status_on_exception,
                    mark_error_on_exception=mark_error_on_exception,
                    **span_kwargs,
                ) as span:
                    if span_arg:
                        kwargs = dict(kwargs)
                        kwargs[span_arg] = span
                    return await func(*args, **kwargs)

            return _wrapped_async

        @wraps(func)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            resolved_name, headers, context, attributes = _resolve_span_options(args, kwargs)
            with start_span(
                tracer,
                resolved_name,
                headers=headers,
                attributes=attributes,
                include_links=include_links,
                extra_links=extra_links,
                context=context,
                record_exception=record_exception,
                set_status_on_exception=set_status_on_exception,
                mark_error_on_exception=mark_error_on_exception,
                **span_kwargs,
            ) as span:
                if span_arg:
                    kwargs = dict(kwargs)
                    kwargs[span_arg] = span
                return func(*args, **kwargs)

        return _wrapped

    return _decorator


def extract_context_from_headers(headers: Mapping[str, str] | None) -> Any:
    if not (_OTEL_AVAILABLE and otel_extract):
        return None
    carrier = {str(k): str(v) for k, v in (headers or {}).items()}
    try:
        return otel_extract(carrier)
    except Exception as exc:  # noqa: BLE001
        logger.debug("OpenTelemetry extract failed: %s", exc)
        return None


def inject_context_to_headers(headers: MutableMapping[str, str]) -> MutableMapping[str, str]:
    if not (_OTEL_AVAILABLE and otel_inject):
        return headers
    try:
        # Do not overwrite an explicit upstream traceparent (we sometimes want to
        # force a new trace for child tasks and still keep the current context
        # for links).
        injected: Dict[str, str] = {}
        otel_inject(injected)
        for k, v in injected.items():
            if not headers.get(k):
                headers[k] = v
    except Exception as exc:  # noqa: BLE001
        logger.debug("OpenTelemetry inject failed: %s", exc)
    return headers


CG_PARENT_TRACEPARENT_HEADER = "CG-Parent-Traceparent"


def link_from_traceparent(traceparent: str, *, attributes: Optional[Dict[str, Any]] = None) -> Any:
    if not (_OTEL_AVAILABLE and Link and SpanContext and TraceFlags):
        return None
    try:
        parts = str(traceparent).strip().lower().split("-")
        if len(parts) != 4:
            return None
        trace_id_hex = parts[1]
        span_id_hex = parts[2]
        flags_hex = parts[3]
        trace_id = int(trace_id_hex, 16)
        span_id = int(span_id_hex, 16)
        flags = int(flags_hex, 16)
        if trace_id == 0 or span_id == 0:
            return None
        ctx = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=True,
            trace_flags=TraceFlags(flags),
            trace_state=None,
        )
        return Link(ctx, attributes=attributes or None)
    except Exception:  # noqa: BLE001
        return None


def links_from_headers(headers: Mapping[str, str] | None) -> list[Any]:
    """Build span links from CG headers (cross-trace relationships)."""
    if not headers:
        return []
    parent_tp = headers.get(CG_PARENT_TRACEPARENT_HEADER)
    if not parent_tp:
        return []
    link = link_from_traceparent(parent_tp, attributes={"cg.link": "parent"})
    return [link] if link else []


def current_traceparent() -> Optional[str]:
    """Best-effort traceparent for the current span context (for linking/debug)."""
    if not (_OTEL_AVAILABLE and trace and otel_inject):
        return None
    try:
        span = trace.get_current_span()
        ctx = span.get_span_context() if span else None
        if not ctx or not getattr(ctx, "is_valid", False):
            return None
    except Exception:  # noqa: BLE001
        return None
    carrier: Dict[str, str] = {}
    try:
        otel_inject(carrier)
    except Exception:  # noqa: BLE001
        return None
    return carrier.get("traceparent")


def _iter_exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        next_exc = current.__cause__ if current.__cause__ is not None else current.__context__
        if next_exc is None or not isinstance(next_exc, BaseException):
            break
        current = next_exc


def compact_error_message(exc: BaseException, *, max_len: int = 512) -> str:
    text = str(exc).strip()
    if "\n" in text:
        text = text.splitlines()[0].strip()
    if not text:
        text = type(exc).__name__
    return text[:max_len]


def is_compact_exception(exc: BaseException) -> bool:
    """Whether this exception should be reported without stack traces."""
    for chained in _iter_exception_chain(exc):
        cls = type(chained)
        if cls.__name__ != "ValidationError":
            continue
        module = str(getattr(cls, "__module__", "") or "")
        if module.startswith("pydantic"):
            return True
    return False


def mark_span_error(
    span: Any,
    exc: BaseException,
    *,
    include_exception: Optional[bool] = None,
) -> None:
    if span is None:
        return
    summary = compact_error_message(exc)
    should_record_exception = (
        bool(include_exception) if include_exception is not None else not is_compact_exception(exc)
    )
    if should_record_exception:
        try:
            span.record_exception(exc)
        except Exception:
            pass
    try:
        span.set_attribute("cg.error.type", str(type(exc).__name__))
        span.set_attribute("cg.error.message", summary)
    except Exception:
        pass
    if _OTEL_AVAILABLE and Status is not None and StatusCode is not None:
        try:
            span.set_status(Status(StatusCode.ERROR, description=summary))
        except Exception:
            pass
