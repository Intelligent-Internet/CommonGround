from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from CommonGround.contracts import AgentRef, DispatchAuthority, DispatchAuthorityMode, OperationMeta, TURN_KIND_CONVERSATION_V1, TraceContext, TurnRef
from CommonGround.sdk import KernelSDK


@dataclass(slots=True)
class DispatchIngressAdapter:
    sdk: KernelSDK

    def dispatch(
        self,
        *,
        requested_by: AgentRef,
        target_agent: AgentRef,
        request_id: str,
        input_payload: Any,
        turn_kind: str = TURN_KIND_CONVERSATION_V1,
        dispatch_key: str | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> TurnRef:
        return self.sdk.dispatch(
            requested_by=requested_by,
            target_agent=target_agent,
            input_payload=input_payload,
            authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
            dispatch_key=dispatch_key or request_id,
            turn_kind=turn_kind,
            meta=meta,
            trace=trace,
        )
