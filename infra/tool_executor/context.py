from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from core.cg_context import CGContext
from core.errors import BadRequestError
from core.utils import safe_str
from core.utp_protocol import (
    AFTER_EXECUTION_SUSPEND,
    extract_after_execution_override,
    normalize_after_execution,
)


@dataclass
class ToolResultContext:
    ctx: CGContext
    cmd_data: Dict[str, Any] = field(default_factory=dict)
    payload: Any = None
    _after_execution_override: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cmd_data = dict(self.cmd_data or {})

    @classmethod
    def from_cmd_data(
        cls,
        *,
        ctx: CGContext,
        cmd_data: Mapping[str, Any],
        tool_call_meta: Optional[Mapping[str, Any]] = None,
        payload: Any = None,
    ) -> "ToolResultContext":
        resolved_ctx = ctx.with_lineage_meta(tool_call_meta)
        _ = resolved_ctx.require_tool_call_id
        return cls(
            ctx=resolved_ctx,
            cmd_data=dict(cmd_data or {}),
            payload=payload,
        )

    @property
    def headers(self) -> Dict[str, str]:
        return self.ctx.to_nats_headers()

    @property
    def tool_call_meta(self) -> Dict[str, Any]:
        return self.ctx.to_lineage_meta()

    @property
    def tool_name(self) -> str:
        value = getattr(self.payload, "tool_name", None)
        return safe_str(value) or safe_str(self.cmd_data.get("tool_name")) or "unknown"

    @property
    def tool_call_card_id(self) -> Optional[str]:
        value = getattr(self.payload, "tool_call_card_id", None)
        return safe_str(value) or safe_str(self.cmd_data.get("tool_call_card_id"))

    @property
    def base_after_execution(self) -> str:
        value = getattr(self.payload, "after_execution", None)
        if value is None:
            value = self.cmd_data.get("after_execution")
        normalized = normalize_after_execution(value, default=AFTER_EXECUTION_SUSPEND)
        return normalized or AFTER_EXECUTION_SUSPEND

    def override_after_execution(self, value: str) -> None:
        normalized = normalize_after_execution(value, default=None)
        if normalized is None:
            raise BadRequestError(f"invalid after_execution={value!r}")
        self._after_execution_override = normalized

    def effective_after_execution(self, result_payload: Any = None) -> str:
        result_override = extract_after_execution_override(result_payload)
        if result_override is not None:
            return result_override
        if self._after_execution_override is not None:
            return self._after_execution_override
        return self.base_after_execution


@dataclass
class ToolCallEnvelope(ToolResultContext):
    """Canonical tool execution envelope with CGContext as identity source."""

    tool_call_card: Any = None
    args: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.args = dict(self.args or {})
