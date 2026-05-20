from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

from .models import TurnOutcome, TurnState


@dataclass(slots=True)
class AgentRow:
    project_id: str
    agent_id: str
    enabled: bool
    accepts_work: bool
    capacity: int
    capabilities: tuple[str, ...]
    grants: tuple[str, ...]
    public_metadata: Mapping[str, Any]
    created_at: datetime
    updated_at: datetime
    last_seen_at: datetime | None = None
    role: str | None = None
    description: str | None = None
    registered_by_agent_id: str | None = None
    registration_provenance_kind: str | None = None
    registration_provenance_ref: str | None = None
    registration_provenance_payload_hash: str | None = None
    admitted_spec_hash: str | None = None


@dataclass(slots=True)
class AgentCredentialRow:
    credential_id: str
    project_id: str
    agent_id: str
    secret_hash: str = field(repr=False)
    status: str
    issued_by_agent_id: str | None
    provenance_kind: str | None
    provenance_ref: str | None
    provenance_payload_hash: str | None
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    revoked_at: datetime | None = None
    last_used_at: datetime | None = None


@dataclass(slots=True)
class TurnRow:
    project_id: str
    turn_id: str
    project_turn_seq: int
    target_agent_id: str
    turn_kind: str
    cause_kind: str
    cause_id: str
    state: TurnState
    outcome: TurnOutcome | None
    stop_requested: bool
    current_claim_agent_id: str | None
    current_claim_token_hash: str | None
    claim_expires_at: datetime | None
    spawn_key: str
    final_record_role: str | None
    final_cardbox_ref_project_id: str | None
    final_cardbox_ref_cardbox_id: str | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    last_turn_seq: int


@dataclass(slots=True)
class SpawnEnvelopeRow:
    project_id: str
    spawn_key: str
    turn_kind: str
    requested_by_agent_id: str
    cause_fingerprint: str
    bootstrap_bundle_ref_project_id: str
    bootstrap_bundle_ref_cardbox_id: str
    target_agent_id: str
    turn_id: str
    created_at: datetime
