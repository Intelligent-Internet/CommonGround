from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.contracts import AgentRef, OperationMeta, TraceContext, TurnRef
from CommonGround.sdk import KernelSDK
from CommonGround.kernel import LifecycleKernel, TopologyKernel


@dataclass(slots=True)
class ManagementAdapter:
    topology: TopologyKernel
    lifecycle: LifecycleKernel
    sdk: KernelSDK

    def register_agent_by_service(
        self,
        *,
        registered_by: AgentRef,
        spec: AgentBirthSpec,
        provenance: AgentRegistrationProvenance,
    ):
        return self.topology.register_agent_by_service(
            registered_by=registered_by,
            spec=spec,
            provenance=provenance,
        )

    def drain_agent(
        self,
        *,
        agent: AgentRef,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.topology.drain_agent(
            agent,
            requested_by=requested_by,
            meta=meta,
            trace=trace,
        )

    def resume_agent(
        self,
        *,
        agent: AgentRef,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.topology.resume_agent(
            agent,
            requested_by=requested_by,
            meta=meta,
            trace=trace,
        )

    def update_agent_presence(
        self,
        *,
        agent: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.topology.update_agent_presence(
            agent,
            meta=meta,
            trace=trace,
        )

    def update_agent_public_metadata(
        self,
        *,
        agent: AgentRef,
        public_metadata: Mapping[str, Any],
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.topology.update_agent_public_metadata(
            agent,
            public_metadata,
            meta=meta,
            trace=trace,
        )

    def request_stop(
        self,
        *,
        project_id: str,
        turn_id: str,
        requested_by: AgentRef,
        reason: str = "operator_stop",
        note: str | None = None,
        annotations: Mapping[str, Any] | None = None,
        trace: TraceContext | None = None,
    ) -> TurnRef:
        turn = TurnRef(project_id=project_id, turn_id=turn_id)
        self.lifecycle.request_stop_turn(
            turn,
            requested_by=requested_by,
            meta=OperationMeta(note=note, reason=reason, annotations=annotations or {}),
            trace=trace,
        )
        return turn
