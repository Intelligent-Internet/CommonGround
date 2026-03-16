from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses import replace
from typing import Any, Dict, Mapping, Optional

from core.errors import ProtocolViolationError
from core.headers import (
    CG_AGENT_ID,
    CG_AGENT_TURN_ID,
    CG_CHANNEL_ID,
    CG_PARENT_AGENT_ID,
    CG_PARENT_AGENT_TURN_ID,
    CG_PARENT_TRACEPARENT_HEADER,
    CG_PARENT_STEP_ID,
    CG_PROJECT_ID,
    CG_STEP_ID,
    CG_TOOL_CALL_ID,
    CG_TURN_EPOCH,
    RECURSION_DEPTH_HEADER,
    TRACEPARENT_HEADER,
    TRACESTATE_HEADER,
    ensure_recursion_depth,
    require_recursion_depth,
)
from core.trace import ensure_trace_headers

@dataclass(frozen=True, slots=True)
class CGContext:
    """Cross-layer identity/trace context for a single turn-scoped operation."""

    project_id: str
    agent_id: str
    channel_id: str = "public"
    agent_turn_id: str = ""
    turn_epoch: int = 0
    step_id: Optional[str] = None
    tool_call_id: Optional[str] = None
    trace_id: Optional[str] = None
    recursion_depth: int = 0
    parent_agent_id: Optional[str] = None
    parent_agent_turn_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "project_id", self._normalize_required_str("project_id", self.project_id))
        object.__setattr__(self, "agent_id", self._normalize_required_str("agent_id", self.agent_id))
        object.__setattr__(
            self,
            "channel_id",
            self._normalize_str(self.channel_id) or "public",
        )
        object.__setattr__(self, "agent_turn_id", self._normalize_str(self.agent_turn_id))
        object.__setattr__(self, "turn_epoch", self._normalize_non_negative_int("turn_epoch", self.turn_epoch))
        object.__setattr__(self, "recursion_depth", self._normalize_non_negative_int("recursion_depth", self.recursion_depth))
        object.__setattr__(self, "step_id", self._normalize_optional_str(self.step_id))
        object.__setattr__(self, "tool_call_id", self._normalize_optional_str(self.tool_call_id))
        object.__setattr__(self, "trace_id", self._normalize_optional_trace_id(self.trace_id))
        object.__setattr__(self, "parent_agent_id", self._normalize_optional_str(self.parent_agent_id))
        object.__setattr__(self, "parent_agent_turn_id", self._normalize_optional_str(self.parent_agent_turn_id))
        object.__setattr__(self, "parent_step_id", self._normalize_optional_str(self.parent_step_id))
        object.__setattr__(self, "headers", self._normalize_headers(self.headers))

    @staticmethod
    def _normalize_str(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _normalize_required_str(field_name: str, value: Any) -> str:
        normalized = CGContext._normalize_str(value)
        if not normalized:
            raise ProtocolViolationError(f"CGContext: missing required {field_name}")
        return normalized

    @staticmethod
    def _normalize_optional_str(value: Any) -> Optional[str]:
        normalized = CGContext._normalize_str(value)
        return normalized or None

    @staticmethod
    def _normalize_optional_trace_id(value: Any) -> Optional[str]:
        return CGContext._normalize_optional_str(value)

    @staticmethod
    def _normalize_non_negative_int(field_name: str, value: Any) -> int:
        try:
            normalized = int(value or 0)
        except Exception as exc:  # noqa: BLE001
            raise ProtocolViolationError(f"CGContext: invalid {field_name}") from exc
        if normalized < 0:
            raise ProtocolViolationError(f"CGContext: {field_name} cannot be negative")
        return normalized

    @staticmethod
    def parse_optional_int(value: Any, *, field_name: str) -> Optional[int]:
        text = str(value).strip() if value is not None else ""
        if not text:
            return None
        try:
            return int(text)
        except Exception as exc:  # noqa: BLE001
            raise ProtocolViolationError(f"invalid {field_name}") from exc

    @staticmethod
    def _normalize_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        normalized: Dict[str, str] = {}
        for key, value in dict(headers or {}).items():
            key_str = str(key or "").strip()
            if not key_str or value is None:
                continue
            normalized[key_str] = str(value)
        return normalized

    @staticmethod
    def normalize_transport_headers(
        headers: Optional[Dict[str, str]],
        *,
        trace_id: Optional[str] = None,
        default_depth: int = 0,
    ) -> tuple[Dict[str, str], Optional[str], int]:
        normalized = CGContext._normalize_headers(headers)
        normalized, _, resolved_trace_id = ensure_trace_headers(normalized, trace_id=trace_id)
        normalized = ensure_recursion_depth(normalized, default_depth=int(default_depth))
        depth = require_recursion_depth(normalized)
        return normalized, resolved_trace_id, int(depth)

    @classmethod
    def from_sys_dict(
        cls,
        data: Mapping[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> "CGContext":
        payload = dict(data or {})
        merged_headers = headers if headers is not None else payload.get("headers")
        if not isinstance(merged_headers, dict):
            merged_headers = {}
        return cls(
            project_id=payload.get("project_id"),
            channel_id=payload.get("channel_id"),
            agent_id=payload.get("agent_id"),
            agent_turn_id=payload.get("agent_turn_id"),
            turn_epoch=payload.get("turn_epoch"),
            step_id=payload.get("step_id"),
            tool_call_id=payload.get("tool_call_id"),
            trace_id=payload.get("trace_id"),
            recursion_depth=payload.get("recursion_depth"),
            parent_agent_id=payload.get("parent_agent_id"),
            parent_agent_turn_id=payload.get("parent_agent_turn_id"),
            parent_step_id=payload.get("parent_step_id"),
            headers=merged_headers,
        )

    @classmethod
    def from_raw_fields(
        cls,
        *,
        project_id: str,
        channel_id: str,
        agent_id: str,
        agent_turn_id: str = "",
        turn_epoch: int = 0,
        step_id: Optional[str] = None,
        tool_call_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        recursion_depth: int = 0,
        parent_agent_id: Optional[str] = None,
        parent_agent_turn_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "CGContext":
        return cls(
            project_id=project_id,
            channel_id=channel_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            turn_epoch=int(turn_epoch),
            step_id=step_id,
            tool_call_id=tool_call_id,
            trace_id=trace_id,
            recursion_depth=int(recursion_depth),
            parent_agent_id=parent_agent_id,
            parent_agent_turn_id=parent_agent_turn_id,
            parent_step_id=parent_step_id,
            headers=dict(headers or {}),
        )

    def to_sys_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "channel_id": self.channel_id,
            "agent_id": self.agent_id,
            "agent_turn_id": self.agent_turn_id,
            "turn_epoch": self.turn_epoch,
            "step_id": self.step_id,
            "tool_call_id": self.tool_call_id,
            "trace_id": self.trace_id,
            "recursion_depth": self.recursion_depth,
            "parent_agent_id": self.parent_agent_id,
            "parent_agent_turn_id": self.parent_agent_turn_id,
            "parent_step_id": self.parent_step_id,
        }

    def to_otel_attributes(self) -> Dict[str, Any]:
        attrs: Dict[str, Any] = {
            "cg.project_id": self.project_id,
            "cg.channel_id": self.channel_id,
            "cg.agent_id": self.agent_id,
            "cg.agent_turn_id": self.agent_turn_id,
            "cg.turn_epoch": int(self.turn_epoch),
        }
        if self.step_id:
            attrs["cg.step_id"] = self.step_id
        if self.tool_call_id:
            attrs["cg.tool_call_id"] = self.tool_call_id
        if self.trace_id:
            attrs["cg.trace_id"] = self.trace_id
        if self.parent_step_id:
            attrs["cg.parent_step_id"] = self.parent_step_id
        return attrs

    @property
    def require_agent_turn_id(self) -> str:
        if not self.agent_turn_id:
            raise ProtocolViolationError("CGContext: missing required agent_turn_id")
        return self.agent_turn_id

    @property
    def require_step_id(self) -> str:
        if not self.step_id:
            raise ProtocolViolationError("CGContext: missing required step_id")
        return self.step_id

    @property
    def require_tool_call_id(self) -> str:
        if not self.tool_call_id:
            raise ProtocolViolationError("CGContext: missing required tool_call_id")
        return self.tool_call_id

    @property
    def require_parent_step_id(self) -> str:
        if not self.parent_step_id:
            raise ProtocolViolationError("CGContext: missing required parent_step_id")
        return self.parent_step_id

    def subject(self, category: str, component: str, target: str, suffix: str) -> str:
        from core.subject import format_subject

        return format_subject(
            self.project_id,
            self.channel_id,
            str(category),
            str(component),
            str(target),
            str(suffix),
        )

    def render_subject_template(self, template: str) -> str:
        return (
            str(template or "")
            .replace("{project_id}", self.project_id)
            .replace("{channel_id}", self.channel_id)
            .replace("{agent_id}", self.agent_id)
        )

    def to_lineage_meta(self, *, exclude_step_id: bool = False) -> Dict[str, str]:
        meta: Dict[str, str] = {}
        if self.step_id and not exclude_step_id:
            meta["step_id"] = self.step_id
        if self.trace_id:
            meta["trace_id"] = self.trace_id
        if self.parent_step_id:
            meta["parent_step_id"] = self.parent_step_id
        return meta

    def with_lineage_meta(self, lineage: Optional[Mapping[str, Any]]) -> "CGContext":
        raw = dict(lineage or {})
        updates: Dict[str, Any] = {}
        step_id = self._normalize_optional_str(raw.get("step_id"))
        trace_id = self._normalize_optional_trace_id(raw.get("trace_id"))
        parent_step_id = self._normalize_optional_str(raw.get("parent_step_id"))
        if step_id is not None:
            updates["step_id"] = step_id
        if trace_id is not None:
            updates["trace_id"] = trace_id
        if parent_step_id is not None:
            updates["parent_step_id"] = parent_step_id
        if not updates:
            return self
        return self.evolve(**updates)

    def evolve(self, **kwargs: Any) -> "CGContext":
        return replace(self, **kwargs)

    def with_bumped_epoch(self, new_epoch: int) -> "CGContext":
        return self.evolve(turn_epoch=new_epoch)

    def with_tool_call(self, tool_call_id: Optional[str]) -> "CGContext":
        return self.evolve(tool_call_id=tool_call_id)

    def with_turn(self, agent_turn_id: str, turn_epoch: int) -> "CGContext":
        return self.evolve(agent_turn_id=agent_turn_id, turn_epoch=turn_epoch)

    def with_recursion_depth(self, recursion_depth: int) -> "CGContext":
        return self.evolve(recursion_depth=recursion_depth)

    def with_new_step(self, step_id: str) -> "CGContext":
        return self.evolve(step_id=step_id)

    def with_restored_state(
        self,
        *,
        parent_step_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "CGContext":
        return self.evolve(
            parent_step_id=parent_step_id if parent_step_id is not None else self.parent_step_id,
            trace_id=trace_id if trace_id is not None else self.trace_id,
            headers=headers if headers is not None else self.headers,
        )

    def with_transport(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        recursion_depth: Optional[int] = None,
    ) -> "CGContext":
        return self.evolve(
            headers=headers if headers is not None else self.headers,
            trace_id=trace_id if trace_id is not None else self.trace_id,
            recursion_depth=recursion_depth if recursion_depth is not None else self.recursion_depth,
        )

    def with_normalized_transport(
        self,
        headers: Optional[Dict[str, str]],
        *,
        trace_id: Optional[str] = None,
        default_depth: Optional[int] = None,
    ) -> "CGContext":
        normalized_headers, resolved_trace_id, resolved_depth = self.normalize_transport_headers(
            headers,
            trace_id=trace_id if trace_id is not None else self.trace_id,
            default_depth=self.recursion_depth if default_depth is None else int(default_depth),
        )
        return self.with_transport(
            headers=normalized_headers,
            trace_id=resolved_trace_id,
            recursion_depth=int(resolved_depth),
        )

    def with_trace_transport(
        self,
        *,
        traceparent: Optional[str] = None,
        tracestate: Optional[str] = None,
        parent_traceparent: Optional[str] = None,
        trace_id: Optional[str] = None,
        default_depth: Optional[int] = None,
        base_headers: Optional[Dict[str, str]] = None,
    ) -> "CGContext":
        headers = dict(base_headers if base_headers is not None else self.headers)
        if traceparent is not None:
            normalized_tp = self._normalize_optional_str(traceparent)
            if normalized_tp:
                headers[TRACEPARENT_HEADER] = normalized_tp
            else:
                headers.pop(TRACEPARENT_HEADER, None)
        if tracestate is not None:
            normalized_ts = self._normalize_optional_str(tracestate)
            if normalized_ts:
                headers[TRACESTATE_HEADER] = normalized_ts
            else:
                headers.pop(TRACESTATE_HEADER, None)
        if parent_traceparent is not None:
            normalized_parent_tp = self._normalize_optional_str(parent_traceparent)
            if normalized_parent_tp:
                headers[CG_PARENT_TRACEPARENT_HEADER] = normalized_parent_tp
            else:
                headers.pop(CG_PARENT_TRACEPARENT_HEADER, None)
        return self.with_normalized_transport(
            headers,
            trace_id=trace_id if trace_id is not None else self.trace_id,
            default_depth=self.recursion_depth if default_depth is None else int(default_depth),
        )

    def matches_state(self, state: Any) -> bool:
        if state is None:
            return False
        return (
            getattr(state, "active_agent_turn_id", None) == self.agent_turn_id
            and getattr(state, "turn_epoch", None) == self.turn_epoch
        )

    def matches_same_turn_allow_newer_epoch(self, state: Any) -> bool:
        if state is None:
            return False
        return (
            getattr(state, "active_agent_turn_id", None) == self.agent_turn_id
            and int(getattr(state, "turn_epoch", -1)) >= int(self.turn_epoch)
        )

    def state_mismatch_detail(self, state: Any) -> str:
        return (
            f"cmd(turn={self.agent_turn_id},epoch={self.turn_epoch}) "
            f"state(turn={getattr(state, 'active_agent_turn_id', None)},"
            f"epoch={getattr(state, 'turn_epoch', None)})"
        )

    def to_nats_headers(self, *, include_topology: bool = False) -> Dict[str, str]:
        headers = dict(self.headers)
        headers[CG_PROJECT_ID] = self.project_id
        headers[CG_AGENT_ID] = self.agent_id
        if self.channel_id:
            headers[CG_CHANNEL_ID] = self.channel_id
        else:
            headers.pop(CG_CHANNEL_ID, None)
        if self.agent_turn_id:
            headers[CG_AGENT_TURN_ID] = self.agent_turn_id
        else:
            headers.pop(CG_AGENT_TURN_ID, None)
        headers[CG_TURN_EPOCH] = str(self.turn_epoch)
        if include_topology:
            headers[RECURSION_DEPTH_HEADER] = str(self.recursion_depth)
            if self.step_id:
                headers[CG_STEP_ID] = self.step_id
            else:
                headers.pop(CG_STEP_ID, None)
            if self.tool_call_id:
                headers[CG_TOOL_CALL_ID] = self.tool_call_id
            else:
                headers.pop(CG_TOOL_CALL_ID, None)
            if self.parent_agent_id:
                headers[CG_PARENT_AGENT_ID] = self.parent_agent_id
            else:
                headers.pop(CG_PARENT_AGENT_ID, None)
            if self.parent_agent_turn_id:
                headers[CG_PARENT_AGENT_TURN_ID] = self.parent_agent_turn_id
            else:
                headers.pop(CG_PARENT_AGENT_TURN_ID, None)
            if self.parent_step_id:
                headers[CG_PARENT_STEP_ID] = self.parent_step_id
            else:
                headers.pop(CG_PARENT_STEP_ID, None)
        else:
            # Wakeup/event doorbells should stay transport-light and avoid carrying lineage semantics.
            headers.pop(RECURSION_DEPTH_HEADER, None)
            headers.pop(CG_STEP_ID, None)
            headers.pop(CG_TOOL_CALL_ID, None)
            headers.pop(CG_PARENT_AGENT_ID, None)
            headers.pop(CG_PARENT_AGENT_TURN_ID, None)
            headers.pop(CG_PARENT_STEP_ID, None)
        if TRACESTATE_HEADER in headers and not headers[TRACESTATE_HEADER]:
            headers.pop(TRACESTATE_HEADER, None)
        headers, _, _ = ensure_trace_headers(headers, trace_id=self.trace_id)
        return headers
