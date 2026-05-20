from __future__ import annotations

from typing import Any, Mapping

from CommonGround.agent_registration import (
    AGENT_REGISTRATION_BIRTH_GRANT,
    AgentBirthSpec,
    AgentRegistrationProvenance,
    agent_birth_spec_hash,
    canonical_agent_birth_spec,
)
from CommonGround.contracts import (
    AgentRef,
    AgentSnapshot,
    ConflictError,
    OperationMeta,
    TraceContext,
    TruthRepositoryPort,
)
from CommonGround.provision_roles import normalize_public_metadata


class TopologyKernel:
    def __init__(self, truth: TruthRepositoryPort) -> None:
        self._truth = truth

    def register_agent(
        self,
        agent: AgentRef,
        *,
        role: str | None = None,
        description: str | None = None,
        capabilities: tuple[str, ...] = (),
        accepts_work: bool = True,
        grants: tuple[str, ...] = (),
        enabled: bool = True,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> AgentRef:
        del meta, trace
        return self._truth.register_agent_primitive(
            agent=agent,
            role=role,
            description=description,
            capabilities=capabilities,
            accepts_work=accepts_work,
            grants=grants,
            enabled=enabled,
        )

    def register_agent_by_service(
        self,
        *,
        registered_by: AgentRef,
        spec: AgentBirthSpec,
        provenance: AgentRegistrationProvenance,
    ) -> AgentSnapshot:
        caller_row = self._truth.require_agent_row(registered_by)
        if AGENT_REGISTRATION_BIRTH_GRANT not in caller_row.grants:
            raise ConflictError(f"caller missing grant: {AGENT_REGISTRATION_BIRTH_GRANT}")
        _validate_agent_birth_spec(spec)
        _validate_registration_provenance(provenance)
        public_metadata = normalize_public_metadata(
            _normalize_json_object(dict(spec.public_metadata), field_name="public_metadata")
        )
        admitted_spec = canonical_agent_birth_spec(
            AgentBirthSpec(
                agent_id=spec.agent_id,
                role=spec.role,
                description=spec.description,
                enabled=spec.enabled,
                accepts_work=spec.accepts_work,
                capacity=spec.capacity,
                capabilities=spec.capabilities,
                grants=spec.grants,
                public_metadata=public_metadata,
            )
        )
        target = AgentRef(project_id=registered_by.project_id, agent_id=spec.agent_id)
        row = self._truth.insert_agent_birth_primitive(
            agent=target,
            role=admitted_spec.role,
            description=admitted_spec.description,
            capabilities=admitted_spec.capabilities,
            accepts_work=admitted_spec.accepts_work,
            grants=admitted_spec.grants,
            enabled=admitted_spec.enabled,
            capacity=admitted_spec.capacity,
            public_metadata=admitted_spec.public_metadata,
            registered_by=registered_by,
            required_grant=AGENT_REGISTRATION_BIRTH_GRANT,
            registration_provenance_kind=provenance.kind,
            registration_provenance_ref=provenance.external_ref,
            registration_provenance_payload_hash=provenance.payload_hash,
            admitted_spec_hash=agent_birth_spec_hash(admitted_spec),
        )
        return self._truth.agent_snapshot(row)

    def set_agent_accepts_work(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef,
        accepts_work: bool,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        self._truth.set_agent_accepts_work_primitive(
            agent=agent,
            requested_by=requested_by,
            accepts_work=accepts_work,
            meta=meta,
        )

    def drain_agent(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.set_agent_accepts_work(
            agent,
            requested_by=requested_by,
            accepts_work=False,
            meta=meta,
            trace=trace,
        )

    def resume_agent(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.set_agent_accepts_work(
            agent,
            requested_by=requested_by,
            accepts_work=True,
            meta=meta,
            trace=trace,
        )

    def update_agent_presence(
        self,
        agent: AgentRef,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        self._truth.update_agent_presence_primitive(agent=agent, meta=meta)

    def update_agent_public_metadata(
        self,
        agent: AgentRef,
        public_metadata: Mapping[str, Any],
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        if not isinstance(public_metadata, Mapping):
            raise ConflictError("public_metadata must be an object")
        self._truth.update_agent_public_metadata_primitive(
            agent=agent,
            public_metadata=normalize_public_metadata(
                _normalize_json_object(public_metadata, field_name="public_metadata")
            ),
            meta=meta,
        )

    def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
        row = self._truth.get_agent_row(agent)
        return None if row is None else self._truth.agent_snapshot(row)


def _normalize_json_object(value: Mapping[str, Any], *, field_name: str) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, inner in value.items():
        if not isinstance(key, str):
            raise ConflictError(f"{field_name} keys must be strings")
        normalized[key] = _normalize_json_value(inner, field_name=f"{field_name}.{key}")
    return normalized


def _normalize_json_value(value: Any, *, field_name: str) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return _normalize_json_object(value, field_name=field_name)
    if isinstance(value, list):
        return [_normalize_json_value(item, field_name=f"{field_name}[]") for item in value]
    raise ConflictError(f"{field_name} must be JSON-compatible")


def _validate_agent_birth_spec(spec: AgentBirthSpec) -> None:
    if not spec.agent_id:
        raise ConflictError("agent birth spec agent_id must be non-empty")
    if not spec.role:
        raise ConflictError("agent birth spec role must be non-empty")
    if spec.capacity <= 0:
        raise ConflictError("agent birth spec capacity must be positive")
    if any(not capability for capability in spec.capabilities):
        raise ConflictError("agent birth spec capabilities must contain non-empty strings")
    if any(not grant for grant in spec.grants):
        raise ConflictError("agent birth spec grants must contain non-empty strings")


def _validate_registration_provenance(provenance: AgentRegistrationProvenance) -> None:
    if not provenance.kind:
        raise ConflictError("registration provenance kind must be non-empty")
    if not provenance.external_ref:
        raise ConflictError("registration provenance external_ref must be non-empty")
    if provenance.payload_hash is not None and not provenance.payload_hash:
        raise ConflictError("registration provenance payload_hash must be non-empty when provided")
