from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from core.utp_protocol import Card as SchemaCard
from infra.tool_executor import (
    ToolResultBuilder,
    ToolResultContext,
    extract_tool_call_args,
    extract_tool_call_metadata,
)


def build_tool_result_package(
    *,
    project_id: str,
    cmd_data: Dict[str, Any],
    tool_call_meta: Optional[Dict[str, Any]] = None,
    status: str,
    result: Any = None,
    error: Optional[Dict[str, Any]] = None,
    author_id: str = "tool",
    function_name: Optional[str] = None,
    error_source: str = "tool",
) -> Tuple[Dict[str, Any], SchemaCard]:
    """L2-friendly facade for building `tool.result` payload + card.

    This helper intentionally keeps a compact call shape for L2/service code,
    while delegating all validation/normalization behavior to
    `infra.tool_executor` as the single source of truth.
    """
    ctx = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta or {},
    )
    builder = ToolResultBuilder(
        ctx=ctx,
        author_id=str(author_id),
        function_name=function_name,
        error_source=error_source,
    )
    return builder.build(
        status=status,
        result=result,
        error=error,
    )


__all__ = [
    "build_tool_result_package",
    "extract_tool_call_args",
    "extract_tool_call_metadata",
]
