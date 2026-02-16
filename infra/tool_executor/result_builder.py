from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Dict, Optional, Tuple

import uuid6

from core.errors import (
    build_error_result_from_exception,
    normalize_error,
)
from core.status import STATUS_FAILED, STATUS_SUCCESS, TOOL_RESULT_STATUS_SET
from core.utp_protocol import (
    AFTER_EXECUTION_SUSPEND,
    Card as SchemaCard,
    ToolResultContent,
    normalize_after_execution,
)

from .context import ToolResultContext
from .hydration import extract_tool_call_lineage


def _is_json_serializable(value: Any) -> bool:
    try:
        json.dumps(value, ensure_ascii=False)
        return True
    except Exception:
        return False


def _normalize_after_execution(
    after_exec: Any,
    *,
    source: str,
) -> tuple[str, Optional[Dict[str, Any]]]:
    normalized = normalize_after_execution(after_exec, default=None)
    if normalized is not None:
        return normalized, None
    after_value = str(after_exec) if after_exec is not None else ""
    error_payload = normalize_error(
        {
            "code": "invalid_after_execution",
            "message": f"invalid after_execution={after_value!r}",
            "detail": {"after_execution": after_value},
        },
        source=source,
    )
    return AFTER_EXECUTION_SUSPEND, error_payload


def build_tool_result_metadata(
    *,
    agent_turn_id: Any,
    tool_call_id: Any,
    function_name: Any,
    tool_call_meta: Dict[str, Any],
    role: str = "tool",
) -> Dict[str, Any]:
    lineage = extract_tool_call_lineage(tool_call_meta)
    metadata: Dict[str, Any] = {
        "agent_turn_id": str(agent_turn_id),
        "tool_call_id": str(tool_call_id),
        "role": role,
        "function_name": str(function_name),
    }
    if lineage["step_id"]:
        metadata["step_id"] = lineage["step_id"]
    if lineage["trace_id"]:
        metadata["trace_id"] = lineage["trace_id"]
    if lineage["parent_step_id"]:
        metadata["parent_step_id"] = lineage["parent_step_id"]
    return metadata


class ToolResultBuilder:
    def __init__(
        self,
        ctx: ToolResultContext,
        *,
        author_id: str,
        function_name: Optional[str] = None,
        error_source: str = "tool",
    ):
        self.ctx = ctx
        self.author_id = str(author_id)
        self.function_name = function_name
        self.error_source = str(error_source)

    def success(
        self,
        result: Any,
        *,
        function_name: Optional[str] = None,
        after_execution: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], SchemaCard]:
        return self.build(
            status=STATUS_SUCCESS,
            result=result,
            error=None,
            function_name=function_name,
            after_execution=after_execution,
        )

    def fail(
        self,
        exc_or_error: Any,
        *,
        result: Any = None,
        function_name: Optional[str] = None,
        after_execution: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], SchemaCard]:
        if isinstance(exc_or_error, Exception):
            default_result, error_payload = build_error_result_from_exception(
                exc_or_error,
                source=self.error_source,
            )
            resolved_result = default_result if result is None else result
            return self.build(
                status=STATUS_FAILED,
                result=resolved_result,
                error=error_payload,
                function_name=function_name,
                after_execution=after_execution,
            )

        normalized_error = normalize_error(
            exc_or_error,
            source=self.error_source,
        )
        default_result = {
            "error_code": normalized_error.get("code", "internal_error"),
            "error_message": normalized_error.get("message", "unknown error"),
            "error": normalized_error,
        }
        resolved_result = default_result if result is None else result
        return self.build(
            status=STATUS_FAILED,
            result=resolved_result,
            error=normalized_error,
            function_name=function_name,
            after_execution=after_execution,
        )

    def build(
        self,
        *,
        status: Any,
        result: Any = None,
        error: Optional[Dict[str, Any]] = None,
        function_name: Optional[str] = None,
        after_execution: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], SchemaCard]:
        status_value = str(status)
        normalized_error: Optional[Dict[str, Any]] = None

        if status_value not in TOOL_RESULT_STATUS_SET:
            status_value = STATUS_FAILED
            error = error or {
                "code": "invalid_status",
                "message": f"invalid tool status={status!r}",
                "detail": {"status": status},
            }

        if error is not None:
            normalized_error = normalize_error(error, source=self.error_source)
            if status_value == STATUS_SUCCESS:
                status_value = STATUS_FAILED

        if status_value != STATUS_SUCCESS and normalized_error is None:
            if isinstance(result, dict) and ("error_code" in result or "error_message" in result):
                normalized_error = normalize_error(result, source=self.error_source)
            else:
                normalized_error = normalize_error(
                    {"code": "unknown_error", "message": "unknown error"},
                    source=self.error_source,
                )

        if normalized_error is not None and not _is_json_serializable(normalized_error):
            normalized_error = normalize_error(
                {
                    "code": "error_not_serializable",
                    "message": "error payload must be JSON-serializable",
                    "detail": {"original_error": str(normalized_error)},
                },
                source=self.error_source,
            )
            status_value = STATUS_FAILED

        if result is not None and not _is_json_serializable(result):
            detail: Dict[str, Any] = {"type": type(result).__name__}
            if normalized_error is not None:
                detail["original_error"] = normalized_error
            normalized_error = normalize_error(
                {
                    "code": "result_not_serializable",
                    "message": "tool result must be JSON-serializable",
                    "detail": detail,
                },
                source=self.error_source,
            )
            status_value = STATUS_FAILED
            result = None

        if after_execution is not None:
            after_exec, after_exec_error = _normalize_after_execution(
                after_execution,
                source=self.error_source,
            )
            if after_exec_error is not None:
                status_value = STATUS_FAILED
                normalized_error = after_exec_error
            effective_after_execution = after_exec
        else:
            effective_after_execution = self.ctx.effective_after_execution(result)

        resolved_function_name = (
            str(function_name)
            if function_name
            else str(self.function_name or self.ctx.tool_name or "unknown")
        )

        content = ToolResultContent(
            status=status_value,
            after_execution=str(effective_after_execution),
            result=result,
            error=normalized_error,
        )

        result_card = SchemaCard(
            card_id=uuid6.uuid7().hex,
            project_id=str(self.ctx.project_id),
            type="tool.result",
            content=content,
            created_at=datetime.now(UTC),
            author_id=self.author_id,
            metadata=build_tool_result_metadata(
                agent_turn_id=self.ctx.agent_turn_id,
                tool_call_id=self.ctx.tool_call_id,
                function_name=resolved_function_name,
                tool_call_meta=self.ctx.tool_call_meta,
            ),
            tool_call_id=str(self.ctx.tool_call_id),
        )

        payload = {
            "tool_call_id": str(self.ctx.tool_call_id),
            "agent_turn_id": str(self.ctx.agent_turn_id),
            "turn_epoch": int(self.ctx.turn_epoch),
            "after_execution": str(effective_after_execution),
            "status": status_value,
            "tool_result_card_id": result_card.card_id,
            "agent_id": str(self.ctx.agent_id),
            "step_id": self.ctx.step_id,
        }
        return payload, result_card
