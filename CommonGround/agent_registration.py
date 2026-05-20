from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Mapping


AGENT_REGISTRATION_BIRTH_GRANT = "agent.registration.birth"
AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT = "agent.accepts_work.update.any"


@dataclass(frozen=True, slots=True)
class AgentBirthSpec:
    agent_id: str
    role: str
    capabilities: tuple[str, ...]
    grants: tuple[str, ...]
    description: str | None = None
    enabled: bool = True
    accepts_work: bool = True
    capacity: int = 1
    public_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentRegistrationProvenance:
    kind: str
    external_ref: str
    payload_hash: str | None = None


def agent_birth_spec_hash(spec: AgentBirthSpec) -> str:
    return hashlib.sha256(encode_agent_birth_spec(canonical_agent_birth_spec(spec)).encode("ascii")).hexdigest()


def canonical_agent_birth_spec(spec: AgentBirthSpec) -> AgentBirthSpec:
    return AgentBirthSpec(
        agent_id=spec.agent_id,
        role=spec.role,
        description=spec.description,
        enabled=spec.enabled,
        accepts_work=spec.accepts_work,
        capacity=spec.capacity,
        capabilities=tuple(sorted(set(spec.capabilities))),
        grants=tuple(sorted(set(spec.grants))),
        public_metadata=dict(spec.public_metadata),
    )


def encode_agent_birth_spec(spec: AgentBirthSpec) -> str:
    return json.dumps(
        {
            "agent_id": spec.agent_id,
            "role": spec.role,
            "description": spec.description,
            "enabled": spec.enabled,
            "accepts_work": spec.accepts_work,
            "capacity": spec.capacity,
            "capabilities": list(spec.capabilities),
            "grants": list(spec.grants),
            "public_metadata": dict(spec.public_metadata),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
