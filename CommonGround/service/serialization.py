from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from cardbox.structures import Card, CardBox

from CommonGround.contracts import (
    AgentRef,
    AgentSnapshot,
    CardBoxRef,
    CauseRef,
    ClaimRenewal,
    ClaimToken,
    HydratedCardBox,
    LedgerEvent,
    LedgerFeedPage,
    OperationMeta,
    ReconcileSummary,
    SemanticRecordPage,
    SemanticRecordRef,
    SemanticRecordSnapshot,
    TURN_KIND_CONVERSATION_V1,
    TraceContext,
    TurnOutcome,
    TurnRef,
    TurnSnapshot,
    WorkMemoryReportSubmissionResult,
)
from CommonGround.sdk import SemanticContextItem, TurnContext


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {item.name: to_jsonable(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return to_jsonable(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    return value


def parse_agent_ref(data: Mapping[str, Any]) -> AgentRef:
    return AgentRef(project_id=data["project_id"], agent_id=data["agent_id"])


def _optional_str(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string when provided")
    return value


def parse_agent_snapshot(data: Mapping[str, Any]) -> AgentSnapshot:
    public_metadata = data["public_metadata"]
    if not isinstance(public_metadata, Mapping):
        raise ValueError("public_metadata must be an object")
    return AgentSnapshot(
        agent=parse_agent_ref(data["agent"]),
        role=data.get("role"),
        description=_optional_str(data, "description"),
        enabled=data["enabled"],
        accepts_work=data["accepts_work"],
        capacity=data["capacity"],
        capabilities=tuple(data["capabilities"]),
        grants=tuple(data["grants"]),
        public_metadata=dict(public_metadata),
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
        last_seen_at=None if data.get("last_seen_at") is None else datetime.fromisoformat(data["last_seen_at"]),
        registered_by_agent_id=data.get("registered_by_agent_id"),
        registration_provenance_kind=data.get("registration_provenance_kind"),
        registration_provenance_ref=data.get("registration_provenance_ref"),
        registration_provenance_payload_hash=data.get("registration_provenance_payload_hash"),
        admitted_spec_hash=data.get("admitted_spec_hash"),
    )


def parse_turn_ref(data: Mapping[str, Any]) -> TurnRef:
    return TurnRef(project_id=data["project_id"], turn_id=data["turn_id"])


def parse_claim_token(data: Mapping[str, Any]) -> ClaimToken:
    return ClaimToken(
        project_id=data["project_id"],
        turn_id=data["turn_id"],
        agent_id=data["agent_id"],
        token=data["token"],
        expires_at=datetime.fromisoformat(data["expires_at"]),
    )


def parse_claim_renewal(data: Mapping[str, Any]) -> ClaimRenewal:
    return ClaimRenewal(
        server_time=datetime.fromisoformat(data["server_time"]),
        expires_at=datetime.fromisoformat(data["expires_at"]),
        recommended_interval_seconds=float(data["recommended_interval_seconds"]),
    )


def parse_cause_ref(data: Mapping[str, Any]) -> CauseRef:
    return CauseRef(kind=data["kind"], id=data["id"])


def parse_operation_meta(data: Mapping[str, Any] | None) -> OperationMeta | None:
    if data is None:
        return None
    return OperationMeta(note=data.get("note"), reason=data.get("reason"), annotations=dict(data.get("annotations", {})))


def parse_trace_context(data: Mapping[str, Any] | None) -> TraceContext | None:
    if data is None:
        return None
    return TraceContext(
        trace_id=data.get("trace_id"),
        parent_trace_id=data.get("parent_trace_id"),
        recursion_depth=data.get("recursion_depth", 0),
    )


def parse_cardbox_ref(data: Mapping[str, Any]) -> CardBoxRef:
    return CardBoxRef(project_id=data["project_id"], cardbox_id=data["cardbox_id"])


def parse_card(data: Mapping[str, Any]) -> Card:
    from cardbox.structures import Card

    return Card.from_dict(dict(data))


def parse_card_box(data: Mapping[str, Any]) -> CardBox:
    from cardbox.structures import CardBox

    return CardBox.model_validate(dict(data))


def parse_hydrated_cardbox(data: Mapping[str, Any]) -> HydratedCardBox:
    return HydratedCardBox(
        ref=parse_cardbox_ref(data["ref"]),
        box=parse_card_box(data["box"]),
        cards=tuple(parse_card(item) for item in data["cards"]),
    )


def parse_turn_snapshot(data: Mapping[str, Any]) -> TurnSnapshot:
    return TurnSnapshot(
        turn=parse_turn_ref(data["turn"]),
        target_agent=parse_agent_ref(data["target_agent"]),
        turn_kind=data.get("turn_kind", TURN_KIND_CONVERSATION_V1),
        cause=parse_cause_ref(data["cause"]),
        state=data["state"],
        outcome=None if data.get("outcome") is None else TurnOutcome(data["outcome"]),
        stop_requested=data["stop_requested"],
        current_claim_agent_id=data.get("current_claim_agent_id"),
        claim_expires_at=None if data.get("claim_expires_at") is None else datetime.fromisoformat(data["claim_expires_at"]),
        spawn_key=data["spawn_key"],
        final_record_role=data.get("final_record_role"),
        final_cardbox_ref=None if data.get("final_cardbox_ref") is None else parse_cardbox_ref(data["final_cardbox_ref"]),
        final_payload=data.get("final_payload"),
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
        closed_at=None if data.get("closed_at") is None else datetime.fromisoformat(data["closed_at"]),
    )


def parse_semantic_record_snapshot(data: Mapping[str, Any]) -> SemanticRecordSnapshot:
    return SemanticRecordSnapshot(
        ref=SemanticRecordRef(project_id=data["ref"]["project_id"], record_id=data["ref"]["record_id"]),
        turn=parse_turn_ref(data["turn"]),
        turn_seq=data["turn_seq"],
        record_role=data["record_role"],
        cardbox_ref=parse_cardbox_ref(data["cardbox_ref"]),
        created_at=datetime.fromisoformat(data["created_at"]),
    )


def parse_semantic_record_page(data: Mapping[str, Any]) -> SemanticRecordPage:
    return SemanticRecordPage(items=tuple(parse_semantic_record_snapshot(item) for item in data["items"]), next_after_turn_seq=data["next_after_turn_seq"])


def parse_ledger_event(data: Mapping[str, Any]) -> LedgerEvent:
    cause = data.get("cause")
    payload_ref = data.get("payload_ref")
    return LedgerEvent(
        ledger_seq=data["ledger_seq"],
        project_id=data["project_id"],
        event_type=data["event_type"],
        subject_kind=data["subject_kind"],
        subject_id=data["subject_id"],
        actor_kind=data["actor_kind"],
        actor_id=data["actor_id"],
        cause=None if cause is None else parse_cause_ref(cause),
        created_at=datetime.fromisoformat(data["created_at"]),
        note=data.get("note"),
        annotations=dict(data.get("annotations", {})),
        payload_ref=None if payload_ref is None else parse_cardbox_ref(payload_ref),
    )


def parse_ledger_feed_page(data: Mapping[str, Any]) -> LedgerFeedPage:
    return LedgerFeedPage(items=tuple(parse_ledger_event(item) for item in data["items"]), next_after_ledger_seq=data["next_after_ledger_seq"])


def parse_reconcile_summary(data: Mapping[str, Any]) -> ReconcileSummary:
    return ReconcileSummary(scanned_count=data["scanned_count"], reconciled_count=data["reconciled_count"])


def parse_work_memory_report_submission_result(data: Mapping[str, Any]) -> WorkMemoryReportSubmissionResult:
    return WorkMemoryReportSubmissionResult(
        status=data["status"],
        turn=parse_turn_ref(data["turn"]),
        record_refs=tuple(SemanticRecordRef(project_id=item["project_id"], record_id=item["record_id"]) for item in data["record_refs"]),
        final_payload=data.get("final_payload"),
    )


def parse_turn_context(data: Mapping[str, Any]) -> TurnContext:
    return TurnContext(
        turn=parse_turn_snapshot(data["turn"]),
        semantic_items=tuple(
            SemanticContextItem(record=parse_semantic_record_snapshot(item["record"]), content=parse_hydrated_cardbox(item["content"]))
            for item in data["semantic_items"]
        ),
    )
