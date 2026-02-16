import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict, ValidationError, field_validator

from card_box_core.structures import (
    Card as CoreCard,
    CardBox as CoreBox,
    Content,
    TextContent,
    JsonContent,
    FieldSchema,
    FieldsSchemaContent,
    ToolCallContent,
    ToolResultContent,
    ToolContent,
)
from core.errors import BadRequestError, ProtocolViolationError

AFTER_EXECUTION_SUSPEND = "suspend"
AFTER_EXECUTION_TERMINATE = "terminate"
ALLOWED_AFTER_EXECUTION_VALUES = frozenset(
    (AFTER_EXECUTION_SUSPEND, AFTER_EXECUTION_TERMINATE)
)


def normalize_after_execution(value: Any, *, default: Optional[str] = None) -> Optional[str]:
    if value is None:
        return default
    normalized = str(value).strip()
    if not normalized:
        return default
    if normalized in ALLOWED_AFTER_EXECUTION_VALUES:
        return normalized
    return default


def extract_after_execution_override(result_payload: Any) -> Optional[str]:
    if not isinstance(result_payload, dict):
        return None
    control = result_payload.get("__cg_control")
    if not isinstance(control, dict):
        return None
    return normalize_after_execution(control.get("after_execution"), default=None)


def extract_tool_call_args(card: Any) -> Dict[str, Any]:
    content = getattr(card, "content", None)
    if not isinstance(content, ToolCallContent):
        detail = None
        card_id = getattr(card, "card_id", None)
        if card_id:
            detail = {"card_id": str(card_id)}
        raise ProtocolViolationError("tool.call content must be ToolCallContent", detail=detail)
    return content.arguments


def extract_json_content(card: Any) -> Union[Dict[str, Any], List[Any]]:
    content = getattr(card, "content", None)
    if not isinstance(content, JsonContent):
        detail: Dict[str, Any] = {}
        card_id = getattr(card, "card_id", None)
        card_type = getattr(card, "type", None)
        if card_id:
            detail["card_id"] = str(card_id)
        if card_type:
            detail["card_type"] = str(card_type)
        raise ProtocolViolationError(
            "card content must be JsonContent",
            detail=detail or None,
        )
    return content.data


def extract_json_object(card: Any) -> Dict[str, Any]:
    data = extract_json_content(card)
    if not isinstance(data, dict):
        detail: Dict[str, Any] = {"data_type": type(data).__name__}
        card_id = getattr(card, "card_id", None)
        card_type = getattr(card, "type", None)
        if card_id:
            detail["card_id"] = str(card_id)
        if card_type:
            detail["card_type"] = str(card_type)
        raise ProtocolViolationError(
            "JsonContent data must be an object",
            detail=detail,
        )
    return data


def extract_content_text(content: Any) -> str:
    """Normalize Card content into a plain text representation."""
    if content is None:
        return ""
    if isinstance(content, TextContent):
        return content.text or ""
    if isinstance(content, JsonContent):
        content = content.data
    if isinstance(content, (dict, list)):
        try:
            return json.dumps(content, ensure_ascii=False)
        except Exception:
            return str(content)
    return str(content)


def extract_card_content_text(card: Any) -> str:
    return extract_content_text(getattr(card, "content", None))


def extract_content_fields(content: Any) -> Optional[List[Dict[str, str]]]:
    """Extract normalized [{name, value}] fields from JsonContent/dict payloads."""
    obj: Any = None
    if isinstance(content, JsonContent):
        obj = content.data
    elif isinstance(content, dict):
        obj = content
    if not isinstance(obj, dict):
        return None
    raw_fields = obj.get("fields")
    if not isinstance(raw_fields, list):
        return None

    fields_out: List[Dict[str, str]] = []
    for item in raw_fields:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if not isinstance(name, str) or not name.strip():
            continue
        if isinstance(value, str):
            value_text = value
        else:
            try:
                value_text = json.dumps(value, ensure_ascii=False)
            except Exception:
                value_text = str(value)
        fields_out.append({"name": name.strip(), "value": value_text})
    return fields_out


def extract_tool_result_payload(card: Any) -> Dict[str, Any]:
    content = getattr(card, "content", None)
    if not isinstance(content, ToolResultContent):
        raise ProtocolViolationError("tool.result content must be ToolResultContent")
    result_payload = content.result
    after_exec = resolve_effective_after_execution(result_payload, content.after_execution)
    return {
        "status": content.status,
        "after_execution": after_exec,
        "result": result_payload,
        "error": content.error,
    }


def resolve_effective_after_execution(result_payload: Any, default: Optional[str]) -> Optional[str]:
    override = extract_after_execution_override(result_payload)
    if override is not None:
        return override
    return default


DEFAULT_PROJECT_ID = "public"
DEFAULT_CARD_TYPE = "unknown"
DEFAULT_AUTHOR_ID = "unknown"


def build_metadata(
    *,
    project_id: str,
    card_type: str,
    author_id: str,
    created_at: datetime,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = (extra or {}).copy()
    meta.update(
        {
            "project_id": project_id,
            "type": card_type,
            "author_id": author_id,
            "created_at": created_at.isoformat(),
        }
    )
    return meta


def parse_metadata(
    meta: Optional[Dict[str, Any]],
    *,
    project_id: Optional[str] = None,
) -> tuple[str, str, str, datetime, Dict[str, Any]]:
    normalized_meta = dict(meta or {})
    proj = project_id or normalized_meta.get("project_id") or DEFAULT_PROJECT_ID
    card_type = normalized_meta.get("type", normalized_meta.get("semantic_type", DEFAULT_CARD_TYPE))
    producer = normalized_meta.get("producer")
    producer_id = producer.get("id") if isinstance(producer, dict) else None
    author_id = normalized_meta.get("author_id") or producer_id or DEFAULT_AUTHOR_ID
    created_at_raw = normalized_meta.get("created_at") or normalized_meta.get("timestamp")
    created_at_val = _parse_datetime(created_at_raw) or datetime.utcnow()
    return str(proj), str(card_type), str(author_id), created_at_val, normalized_meta


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


class CardBox(BaseModel):
    """
    DTO: Container for card ID lists used only for NATS transport.
    Do not add business logic here.
    """

    card_ids: List[str] = Field(default_factory=list)


class Card(BaseModel):
    """
    Pydantic facade Card used for message/event serialization, mapped
    one-to-one with card_box_core.Card.

    Semantic type (type) spec v1r2:
    - task.instruction: Task instruction (Role: user)
    - agent.thought: Thought and intent (Role: assistant)
    - tool.call: Tool call request (Role: assistant)
    - tool.result: Tool execution result (Role: tool)
    - task.deliverable: Task deliverable (Role: assistant)
    - sys.profile: Agent static configuration (Role: system)
    - sys.rendered_prompt: Rendered system prompt snapshot (Role: system)
    - sys.tools: Tool definitions snapshot (Role: system)
    - meta.parent_pointer: Parent task pointer (Role: system)
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True, extra="forbid")

    card_id: str
    project_id: str
    type: str
    content: Union[ToolCallContent, ToolResultContent, FieldsSchemaContent, JsonContent, TextContent]
    created_at: datetime
    author_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None
    ttl_seconds: Optional[int] = None


def to_core_card(schema_card: "Card") -> CoreCard:
    core_content = _normalize_content_for_core(schema_card.content)
    meta = build_metadata(
        project_id=schema_card.project_id,
        card_type=schema_card.type,
        author_id=schema_card.author_id,
        created_at=schema_card.created_at,
        extra=schema_card.metadata,
    )
    return CoreCard(
        card_id=schema_card.card_id,
        content=core_content,
        tool_calls=schema_card.tool_calls,
        tool_call_id=schema_card.tool_call_id,
        ttl_seconds=schema_card.ttl_seconds,
        metadata=meta,
    )


def from_core_card(
    core_card: CoreCard,
    *,
    project_id: Optional[str] = None,
    schema_cls: Optional[type] = None,
) -> "Card":
    proj, card_type, author_id, created_at_val, meta = parse_metadata(
        core_card.metadata, project_id=project_id
    )
    parsed_content = _normalize_content_for_schema(core_card.content)
    if schema_cls is None:
        schema_cls = Card
    return schema_cls(
        card_id=getattr(core_card, "card_id"),
        project_id=proj,
        type=card_type,
        content=parsed_content,
        created_at=created_at_val,
        author_id=author_id,
        metadata=meta,
        tool_calls=getattr(core_card, "tool_calls", None),
        tool_call_id=getattr(core_card, "tool_call_id", None),
        ttl_seconds=getattr(core_card, "ttl_seconds", None),
    )


def to_core_box(schema_box: "CardBox") -> CoreBox:
    box = CoreBox()
    box.card_ids = list(schema_box.card_ids)
    return box


def from_core_box(core_box: CoreBox, *, schema_cls: Optional[type] = None) -> "CardBox":
    if schema_cls is None:
        schema_cls = CardBox
    return schema_cls(card_ids=list(getattr(core_box, "card_ids", [])))


def _normalize_content_for_core(content: Any) -> Content:
    if isinstance(
        content,
        (ToolCallContent, ToolResultContent, FieldsSchemaContent, JsonContent, TextContent),
    ):
        return content
    if isinstance(content, str):
        return TextContent(text=content)

    def _json_default(obj: Any) -> str:
        # Preserve structure for UUID/datetime payloads instead of stringifying the whole object.
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        return str(obj)

    try:
        text_payload = json.dumps(content, ensure_ascii=False, default=_json_default)
    except Exception:
        text_payload = str(content)
    return TextContent(text=text_payload)


def _normalize_content_for_schema(raw_content: Any) -> Any:
    if isinstance(raw_content, ToolContent):
        return JsonContent(data={"tools": raw_content.tools})
    if isinstance(
        raw_content,
        (ToolCallContent, ToolResultContent, FieldsSchemaContent, JsonContent, TextContent),
    ):
        return raw_content
    return str(raw_content)


class AgentTaskPayload(BaseModel):
    """Payload for L0 AgentTurn (delivered via Inbox + cmd.agent.*.wakeup)."""

    model_config = ConfigDict(extra="ignore")

    agent_turn_id: str
    agent_id: str
    channel_id: str = "public"
    profile_box_id: Optional[str] = None
    context_box_id: Optional[str] = None
    output_box_id: Optional[str] = None
    turn_epoch: int
    parent_agent_turn_id: Optional[str] = None
    parent_tool_call_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    display_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    runtime_config: Dict[str, Any] = Field(default_factory=dict)


class ToolCommandPayload(BaseModel):
    """Subject: cmd.tool.* payload (tool execution requests)."""

    model_config = ConfigDict(extra="allow")

    tool_call_id: str
    agent_turn_id: str
    turn_epoch: int
    agent_id: str
    tool_call_card_id: Optional[str] = None
    tool_name: Optional[str] = None
    after_execution: Optional[str] = None

    @field_validator("tool_call_id", "agent_turn_id", "agent_id")
    @classmethod
    def _require_non_empty(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value

    @field_validator("tool_call_card_id", "tool_name", "after_execution", mode="before")
    @classmethod
    def _empty_to_none(cls, value: Any) -> Any:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @classmethod
    def validate(
        cls,
        data: Dict[str, Any],
        *,
        forbid_keys: Optional[set[str]] = None,
        require_tool_call_card_id: bool = False,
        require_tool_name: bool = False,
        require_after_execution: bool = False,
        allowed_after_execution: Optional[set[str]] = None,
    ) -> "ToolCommandPayload":
        payload = cls.model_validate(data)
        missing: List[str] = []
        if require_tool_call_card_id and not payload.tool_call_card_id:
            missing.append("tool_call_card_id")
        if require_tool_name and not payload.tool_name:
            missing.append("tool_name")
        if require_after_execution and not payload.after_execution:
            missing.append("after_execution")
        if missing:
            raise BadRequestError(f"missing required fields {missing}")
        if allowed_after_execution and payload.after_execution:
            if payload.after_execution not in allowed_after_execution:
                raise BadRequestError(f"invalid after_execution={payload.after_execution!r}")
        if forbid_keys and isinstance(data, dict):
            for key in forbid_keys:
                if key in data:
                    raise ProtocolViolationError(
                        f"Protocol violation: payload.{key} is forbidden (use tool_call_card_id)."
                    )
        return payload

    @staticmethod
    def missing_fields_from_error(error: ValidationError) -> List[str]:
        required_fields = {"tool_call_id", "agent_turn_id", "turn_epoch", "agent_id"}
        missing: List[str] = []
        for item in error.errors():
            loc = item.get("loc")
            field = loc[0] if isinstance(loc, (list, tuple)) and loc else None
            if field in required_fields:
                missing.append(str(field))
        return missing
