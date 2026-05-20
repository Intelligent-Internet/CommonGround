from __future__ import annotations

import httpx


def raise_for_status_with_detail(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise httpx.HTTPStatusError(http_status_message(exc), request=exc.request, response=exc.response) from exc


def http_status_message(exc: httpx.HTTPStatusError) -> str:
    message = str(exc)
    response = exc.response
    if response is None:
        return message
    try:
        payload = response.json()
    except ValueError:
        return message
    if not isinstance(payload, dict):
        return message
    error = payload.get("error")
    detail = payload.get("message")
    if isinstance(error, str) and isinstance(detail, str) and error and detail:
        return f"{message}\nServer message: {error}: {detail}"
    if isinstance(detail, str) and detail:
        return f"{message}\nServer message: {detail}"
    return message
