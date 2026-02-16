from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class CGError(Exception):
    """Base application error with a stable error code and HTTP status."""

    message: str
    code: str = "internal_error"
    http_status: int = 500
    detail: Any = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message

    def as_error_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"error_code": self.code, "error_message": self.message}
        if self.detail is not None:
            data["detail"] = self.detail
        return data


class BadRequestError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="bad_request", http_status=400, detail=detail)


class NotFoundError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="not_found", http_status=404, detail=detail)


class ConflictError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="conflict", http_status=409, detail=detail)


class PolicyDeniedError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="auth_failed", http_status=403, detail=detail)


class ProtocolViolationError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="bad_request", http_status=400, detail=detail)


class UpstreamUnavailableError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="upstream_unavailable", http_status=503, detail=detail)


class ToolTimeoutError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="tool_timeout", http_status=504, detail=detail)


class InternalError(CGError):
    def __init__(self, message: str, *, detail: Any = None):
        super().__init__(message=message, code="internal_error", http_status=500, detail=detail)


def classify_exception(exc: Exception, *, default_code: str = "internal_error") -> Tuple[str, str, Any]:
    if isinstance(exc, CGError):
        return exc.code, exc.message, exc.detail
    if isinstance(exc, KeyError):
        return "not_found", str(exc), None
    if isinstance(exc, ValueError):
        return "bad_request", str(exc), None
    return default_code, str(exc), None


def build_error_payload(
    code: str,
    message: str,
    detail: Any = None,
    *,
    source: str = "unknown",
    retryable: Optional[bool] = None,
    http_status: Optional[int] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"code": code, "message": message, "source": source}
    if detail is not None:
        payload["detail"] = detail
    if retryable is not None:
        payload["retryable"] = retryable
    if http_status is not None:
        payload["http_status"] = http_status
    return payload


def normalize_error(
    error: Any,
    *,
    default_code: str = "internal_error",
    source: str = "unknown",
    retryable: Optional[bool] = None,
) -> Dict[str, Any]:
    if error is None:
        return build_error_payload(default_code, "unknown error", source=source, retryable=retryable)

    if isinstance(error, dict):
        if "code" in error and "message" in error:
            payload = dict(error)
        elif "error_code" in error or "error_message" in error:
            payload = {
                "code": error.get("error_code") or default_code,
                "message": error.get("error_message") or "unknown error",
            }
            if "detail" in error:
                payload["detail"] = error.get("detail")
        else:
            payload = {"code": default_code, "message": str(error)}

        payload.setdefault("source", source)
        if retryable is not None and "retryable" not in payload:
            payload["retryable"] = retryable
        return payload

    if isinstance(error, CGError):
        return build_error_payload(
            error.code,
            error.message,
            error.detail,
            source=source,
            retryable=retryable,
            http_status=error.http_status,
        )

    if isinstance(error, Exception):
        code, message, detail = classify_exception(error, default_code=default_code)
        return build_error_payload(
            code,
            message,
            detail,
            source=source,
            retryable=retryable,
            http_status=http_status_from_exception(error),
        )

    return build_error_payload(default_code, str(error), source=source, retryable=retryable)


def build_error_result(code: str, message: str, detail: Any = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    error_payload = build_error_payload(code, message, detail)
    result: Dict[str, Any] = {
        "error_code": code,
        "error_message": message,
        "error": error_payload,
    }
    if detail is not None:
        result["error_detail"] = detail
    return result, error_payload


def build_error_result_from_exception(
    exc: Exception,
    *,
    default_code: str = "internal_error",
    source: str = "unknown",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    error_payload = normalize_error(exc, default_code=default_code, source=source)
    result: Dict[str, Any] = {
        "error_code": error_payload.get("code", default_code),
        "error_message": error_payload.get("message", "unknown error"),
        "error": error_payload,
    }
    if "detail" in error_payload:
        result["error_detail"] = error_payload.get("detail")
    return result, error_payload


def http_status_from_exception(exc: Exception) -> int:
    if isinstance(exc, CGError):
        return exc.http_status
    if isinstance(exc, KeyError):
        return 404
    if isinstance(exc, ValueError):
        return 400
    return 500


def http_detail_from_exception(exc: Exception, *, default_code: str = "internal_error") -> Dict[str, Any]:
    code, message, detail = classify_exception(exc, default_code=default_code)
    data: Dict[str, Any] = {"error_code": code, "error_message": message}
    if detail is not None:
        data["detail"] = detail
    return data
