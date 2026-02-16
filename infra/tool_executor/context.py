from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from core.errors import BadRequestError
from core.utp_protocol import (
    AFTER_EXECUTION_SUSPEND,
    extract_after_execution_override,
    normalize_after_execution,
)


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


@dataclass
class ToolResultContext:
    project_id: str
    cmd_data: Dict[str, Any]
    tool_call_meta: Dict[str, Any] = field(default_factory=dict)
    subject: Optional[str] = None
    parts: Any = None
    headers: Dict[str, str] = field(default_factory=dict)
    payload: Any = None
    raw_data: Dict[str, Any] = field(default_factory=dict)
    _after_execution_override: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cmd_data = dict(self.cmd_data or {})
        self.tool_call_meta = dict(self.tool_call_meta or {})
        self.headers = dict(self.headers or {})
        self.raw_data = dict(self.raw_data or {})

    @classmethod
    def from_cmd_data(
        cls,
        *,
        project_id: str,
        cmd_data: Mapping[str, Any],
        tool_call_meta: Optional[Mapping[str, Any]] = None,
        subject: Optional[str] = None,
        parts: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        payload: Any = None,
        raw_data: Optional[Mapping[str, Any]] = None,
    ) -> "ToolResultContext":
        return cls(
            project_id=str(project_id),
            cmd_data=dict(cmd_data or {}),
            tool_call_meta=dict(tool_call_meta or {}),
            subject=subject,
            parts=parts,
            headers=dict(headers or {}),
            payload=payload,
            raw_data=dict(raw_data or cmd_data or {}),
        )

    @property
    def tool_call_id(self) -> str:
        value = getattr(self.payload, "tool_call_id", None)
        return _safe_str(value) or _safe_str(self.cmd_data.get("tool_call_id")) or ""

    @property
    def agent_turn_id(self) -> str:
        value = getattr(self.payload, "agent_turn_id", None)
        return _safe_str(value) or _safe_str(self.cmd_data.get("agent_turn_id")) or ""

    @property
    def turn_epoch(self) -> int:
        value = getattr(self.payload, "turn_epoch", None)
        if value is None:
            value = self.cmd_data.get("turn_epoch")
        try:
            return int(value)
        except Exception:
            return 0

    @property
    def agent_id(self) -> str:
        value = getattr(self.payload, "agent_id", None)
        return _safe_str(value) or _safe_str(self.cmd_data.get("agent_id")) or ""

    @property
    def tool_name(self) -> str:
        value = getattr(self.payload, "tool_name", None)
        return _safe_str(value) or _safe_str(self.cmd_data.get("tool_name")) or "unknown"

    @property
    def tool_call_card_id(self) -> Optional[str]:
        value = getattr(self.payload, "tool_call_card_id", None)
        return _safe_str(value) or _safe_str(self.cmd_data.get("tool_call_card_id"))

    @property
    def base_after_execution(self) -> str:
        value = getattr(self.payload, "after_execution", None)
        if value is None:
            value = self.cmd_data.get("after_execution")
        normalized = normalize_after_execution(value, default=AFTER_EXECUTION_SUSPEND)
        return normalized or AFTER_EXECUTION_SUSPEND

    @property
    def step_id(self) -> Optional[str]:
        return _safe_str(self.tool_call_meta.get("step_id"))

    @property
    def trace_id(self) -> Optional[str]:
        return _safe_str(self.tool_call_meta.get("trace_id"))

    @property
    def parent_step_id(self) -> Optional[str]:
        return _safe_str(self.tool_call_meta.get("parent_step_id"))

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
class ToolCallContext(ToolResultContext):
    tool_call_card: Any = None
    args: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.args = dict(self.args or {})
