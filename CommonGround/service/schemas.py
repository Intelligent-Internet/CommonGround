from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from CommonGround.contracts import WORK_MEMORY_REPORT_MANIFEST_KIND_V1, normalize_dispatch_anchor


class ServiceModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AgentRefModel(ServiceModel):
    project_id: str
    agent_id: str


class TurnRefModel(ServiceModel):
    project_id: str
    turn_id: str


class ClaimTokenModel(ServiceModel):
    project_id: str
    turn_id: str
    agent_id: str
    token: str
    expires_at: str


class OperationMetaModel(ServiceModel):
    note: str | None = None
    reason: str | None = None
    annotations: dict[str, Any] = Field(default_factory=dict)


class TraceContextModel(ServiceModel):
    trace_id: str | None = None
    parent_trace_id: str | None = None
    recursion_depth: int = 0


class AgentBirthSpecModel(ServiceModel):
    agent_id: str
    role: str
    description: str | None = None
    enabled: bool = True
    accepts_work: bool = True
    capacity: int = 1
    capabilities: tuple[str, ...] = ()
    grants: tuple[str, ...] = ()
    public_metadata: dict[str, Any] = Field(default_factory=dict)


class AgentRegistrationProvenanceModel(ServiceModel):
    kind: str
    external_ref: str
    payload_hash: str | None = None


class RegisterAgentByServiceRequest(ServiceModel):
    spec: AgentBirthSpecModel
    provenance: AgentRegistrationProvenanceModel


class IssueAgentCredentialRequest(ServiceModel):
    expires_at: datetime | None = None
    provenance_kind: str | None = None
    provenance_ref: str | None = None
    provenance_payload_hash: str | None = None


class WorkMemoryReportRecordModel(ServiceModel):
    role: str
    payload: Any
    source_refs: list[dict[str, Any]] = Field(default_factory=list)


class WorkMemoryReportSubmissionRequest(ServiceModel):
    kind: str = WORK_MEMORY_REPORT_MANIFEST_KIND_V1
    request_id: str
    summary: str | None = None
    records: list[WorkMemoryReportRecordModel]
    final_payload: Any | None = None
    declared_project_id: str | None = None
    declared_agent_id: str | None = None


class DrainAgentRequest(ServiceModel):
    agent: AgentRefModel
    requested_by: AgentRefModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class ResumeAgentRequest(ServiceModel):
    agent: AgentRefModel
    requested_by: AgentRefModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class UpdateAgentPresenceRequest(ServiceModel):
    agent: AgentRefModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class UpdateAgentPublicMetadataRequest(ServiceModel):
    agent: AgentRefModel
    public_metadata: dict[str, Any] = Field(default_factory=dict)
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class StopTurnRequest(ServiceModel):
    turn: TurnRefModel
    requested_by: AgentRefModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class DispatchAuthorityModel(ServiceModel):
    mode: str
    request_id: str | None = None
    parent_claim: ClaimTokenModel | None = None

    @model_validator(mode="after")
    def _validate_fields(self) -> "DispatchAuthorityModel":
        if self.mode == "root_request":
            self.request_id = normalize_dispatch_anchor(self.request_id, field_name="request_id")
            if self.parent_claim is not None:
                raise ValueError("root_request authority cannot include parent_claim")
        elif self.mode == "child_derivation":
            if self.parent_claim is None:
                raise ValueError("child_derivation authority requires parent_claim")
            if self.request_id is not None:
                raise ValueError("child_derivation authority cannot include request_id")
        else:
            raise ValueError(f"unsupported dispatch authority mode: {self.mode}")
        return self


class DispatchTurnRequest(ServiceModel):
    requested_by: AgentRefModel
    target_agent: AgentRefModel
    input: Any
    turn_kind: str = "turn.conversation.v1"
    dispatch_key: str
    authority: DispatchAuthorityModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None

    @field_validator("dispatch_key")
    @classmethod
    def _validate_dispatch_key(cls, value: str) -> str:
        return normalize_dispatch_anchor(value, field_name="dispatch_key")


class ClaimTurnRequest(ServiceModel):
    agent: AgentRefModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class RenewClaimRequest(ServiceModel):
    claim: ClaimTokenModel
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class ReconcileExpiredRequest(ServiceModel):
    agent: AgentRefModel | None = None
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class AppendSemanticRecordRequest(ServiceModel):
    claim: ClaimTokenModel
    payload: Any
    role: str = "progress"
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class SuspendTurnRequest(ServiceModel):
    claim: ClaimTokenModel
    reason: str
    note: str | None = None
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class ResumeTurnRequest(ServiceModel):
    requested_by: AgentRefModel
    note: str | None = None
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None


class FinishTurnRequest(ServiceModel):
    claim: ClaimTokenModel
    outcome: str
    final_payload: Any | None = None
    final_record_role: str = "deliverable"
    meta: OperationMetaModel | None = None
    trace: TraceContextModel | None = None
