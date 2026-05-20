from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from CommonGround.contracts import AgentRef, ClaimRenewal, ClaimToken, DispatchAuthority, DispatchAuthorityMode, LedgerFeedPage, OperationMeta, ReconcileSummary, SemanticRecordSpec, TURN_KIND_CONVERSATION_V1, TraceContext, TurnOutcome, TurnRef
from CommonGround.payloads import UNSET_PAYLOAD
from CommonGround.provision_launch import (
    PROVISION_LAUNCH_STARTED_ROLE,
    parse_provision_launch_started,
)
from CommonGround.sdk import KernelOpContext, KernelSDK, TurnContext


def _merge_meta(
    meta: OperationMeta | None,
    *,
    reason: str | None = None,
    note: str | None = None,
) -> OperationMeta | None:
    if meta is None and reason is None and note is None:
        return None
    return OperationMeta(
        note=note if note is not None else (meta.note if meta is not None else None),
        reason=reason if reason is not None else (meta.reason if meta is not None else None),
        annotations={} if meta is None else dict(meta.annotations),
    )


@dataclass(slots=True)
class ExternalAgentAdapter:
    agent: AgentRef
    sdk: KernelSDK

    def claim_turn(
        self,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ClaimToken | None:
        return self.sdk.lifecycle.claim_turn(self.agent, meta=meta, trace=trace)

    def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100) -> TurnContext:
        return self.sdk.fetch_context(turn, after_turn_seq=after_turn_seq, limit=limit)

    def renew_claim(
        self,
        claim: ClaimToken,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ClaimRenewal:
        return self.sdk.lifecycle.renew_claim(claim, meta=meta, trace=trace)

    def get_turn(self, turn: TurnRef):
        return self.sdk.lifecycle.get_turn(turn)

    def append_record(
        self,
        claim: ClaimToken,
        payload: Any,
        *,
        role: str = "progress",
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ):
        if role == PROVISION_LAUNCH_STARTED_ROLE:
            parse_provision_launch_started(payload)
        ref = self.sdk.cardbox.create_payload_box(
            claim.project_id,
            payload,
            metadata={"cg_role": role},
        )
        self.sdk.semantic.append_semantic_record(
            claim,
            SemanticRecordSpec(record_role=role, cardbox_ref=ref),
            meta=meta,
            trace=trace,
        )
        return ref

    def dispatch(
        self,
        claim: ClaimToken,
        *,
        target_agent: AgentRef,
        input_payload: Any,
        dispatch_key: str,
        turn_kind: str = TURN_KIND_CONVERSATION_V1,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> TurnRef:
        return self.sdk.dispatch(
            requested_by=self.agent,
            target_agent=target_agent,
            input_payload=input_payload,
            authority=DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim),
            dispatch_key=dispatch_key,
            turn_kind=turn_kind,
            meta=meta,
            trace=trace,
        )

    def suspend_current(
        self,
        claim: ClaimToken,
        *,
        reason: str,
        note: str | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.sdk.lifecycle.suspend_turn(
            claim,
            meta=_merge_meta(meta, reason=reason, note=note),
            trace=trace,
        )

    def resume_turn(
        self,
        turn: TurnRef,
        *,
        note: str | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.sdk.lifecycle.resume_turn(
            turn,
            requested_by=self.agent,
            meta=_merge_meta(meta, note=note),
            trace=trace,
        )

    def finish_current(
        self,
        claim: ClaimToken,
        *,
        outcome: TurnOutcome,
        final_payload: Any = UNSET_PAYLOAD,
        final_record_role: str = "deliverable",
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self.sdk.record_and_finish(
            context=KernelOpContext(claim=claim, requested_by=self.agent, meta=meta, trace=trace),
            outcome=outcome,
            final_payload=final_payload,
            record_role=final_record_role,
        )

    def is_stop_requested(self, turn: TurnRef) -> bool:
        snapshot = self.sdk.lifecycle.get_turn(turn)
        return False if snapshot is None else snapshot.stop_requested

    def fetch_turn_feed(
        self,
        turn: TurnRef,
        *,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> LedgerFeedPage:
        return self.sdk.fetch_turn_feed(turn, after_ledger_seq=after_ledger_seq, limit=limit)

    def fetch_agent_feed(
        self,
        *,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> LedgerFeedPage:
        return self.sdk.fetch_agent_feed(self.agent, after_ledger_seq=after_ledger_seq, limit=limit)

    def reconcile_once(
        self,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ReconcileSummary:
        return self.sdk.lifecycle.reconcile_expired_claim(self.agent, meta=meta, trace=trace)
