from __future__ import annotations

from typing import Any, Mapping

import httpx

from CommonGround.agent_client.caller_headers import agent_auth_headers
from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.contracts import AgentRef, AgentSnapshot, ClaimRenewal, ClaimToken, DispatchAuthority, DispatchAuthorityMode, OperationMeta, ReconcileSummary, TURN_KIND_CONVERSATION_V1, TraceContext, TurnOutcome, TurnRef, TurnSnapshot, WorkMemoryReportSubmissionResult
from CommonGround.http_errors import raise_for_status_with_detail
from CommonGround.payloads import UNSET_PAYLOAD
from CommonGround.agent_client.safe_suspend import try_suspend_after_context_fetch_error
from CommonGround.agent_client.types import ClaimTurnPartialFailure, ClaimedTurn
from CommonGround.sdk import TurnContext
from CommonGround.service.serialization import (
    parse_agent_snapshot,
    parse_claim_renewal,
    parse_claim_token,
    parse_ledger_feed_page,
    parse_reconcile_summary,
    parse_turn_context,
    parse_turn_ref,
    parse_turn_snapshot,
    parse_work_memory_report_submission_result,
    to_jsonable,
)


class HttpAgentClient:
    def __init__(
        self,
        *,
        base_url: str = "http://127.0.0.1:8000",
        client: Any | None = None,
        timeout: float = 5.0,
        auth_token: str | None = None,
        agent: AgentRef | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = client or httpx.Client(base_url=self._base_url, timeout=timeout)
        self._owns_client = client is None
        self._default_headers: dict[str, str] = {}
        if agent is not None:
            if not auth_token:
                raise ValueError("auth_token is required when agent is provided")
            self._default_headers.update(agent_auth_headers(agent, auth_token))
        elif auth_token:
            self._default_headers["Authorization"] = f"Bearer {auth_token}"
        if headers:
            self._default_headers.update(dict(headers))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def register_agent_by_service(
        self,
        *,
        project_id: str,
        spec: AgentBirthSpec,
        provenance: AgentRegistrationProvenance,
    ) -> AgentSnapshot:
        return parse_agent_snapshot(
            self._post(
                f"/v3r1/projects/{project_id}/agents:register",
                {
                    "spec": {
                        "agent_id": spec.agent_id,
                        "role": spec.role,
                        "description": spec.description,
                        "enabled": spec.enabled,
                        "accepts_work": spec.accepts_work,
                        "capacity": spec.capacity,
                        "capabilities": spec.capabilities,
                        "grants": spec.grants,
                        "public_metadata": dict(spec.public_metadata),
                    },
                    "provenance": {
                        "kind": provenance.kind,
                        "external_ref": provenance.external_ref,
                        "payload_hash": provenance.payload_hash,
                    },
                },
            )
        )

    def issue_agent_credential(
        self,
        agent: AgentRef,
        *,
        expires_at: str | None = None,
        provenance_kind: str | None = None,
        provenance_ref: str | None = None,
        provenance_payload_hash: str | None = None,
    ) -> dict[str, Any]:
        return self._post(
            f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/credentials:issue",
            {
                "expires_at": expires_at,
                "provenance_kind": provenance_kind,
                "provenance_ref": provenance_ref,
                "provenance_payload_hash": provenance_payload_hash,
            },
        )

    def revoke_agent_credential(self, agent: AgentRef, credential_id: str) -> dict[str, Any]:
        return self._post(
            f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/credentials/{credential_id}:revoke",
            {},
        )

    def list_agent_credentials(self, agent: AgentRef) -> dict[str, Any]:
        return self._get(
            f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/credentials",
            params={},
        )

    def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
        try:
            data = self._get(f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}", params={})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        return parse_agent_snapshot(data)

    def submit_work_memory_report(
        self,
        actor: AgentRef,
        manifest: Mapping[str, Any],
        *,
        meta: OperationMeta | None = None,
    ) -> WorkMemoryReportSubmissionResult:
        if meta is not None:
            raise ValueError("work-memory report HTTP submission does not accept operation metadata; use a trusted in-process API for ledger meta")
        if "meta" in manifest:
            raise ValueError("work-memory manifest must not include meta")
        payload = dict(manifest)
        return parse_work_memory_report_submission_result(
            self._post(
                f"/v3r1/projects/{actor.project_id}/agents/{actor.agent_id}/work-memory-reports",
                payload,
            )
        )

    def drain_agent(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        requested_by = agent if requested_by is None else requested_by
        self._post(
            f"/v3r1/projects/{agent.project_id}/management/agents/{agent.agent_id}:drain",
            {
                "agent": agent,
                "requested_by": requested_by,
                "meta": meta,
                "trace": trace,
            },
        )

    def resume_agent(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        requested_by = agent if requested_by is None else requested_by
        self._post(
            f"/v3r1/projects/{agent.project_id}/management/agents/{agent.agent_id}:resume",
            {
                "agent": agent,
                "requested_by": requested_by,
                "meta": meta,
                "trace": trace,
            },
        )

    def heartbeat_agent_presence(
        self,
        agent: AgentRef,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self._post(
            f"/v3r1/projects/{agent.project_id}/management/agents/{agent.agent_id}:heartbeat-presence",
            {
                "agent": agent,
                "meta": meta,
                "trace": trace,
            },
        )

    def update_agent_public_metadata(
        self,
        agent: AgentRef,
        *,
        public_metadata: Mapping[str, Any],
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        self._put(
            f"/v3r1/projects/{agent.project_id}/management/agents/{agent.agent_id}/public-metadata",
            {
                "agent": agent,
                "public_metadata": dict(public_metadata),
                "meta": meta,
                "trace": trace,
            },
        )

    def request_stop_turn(
        self,
        turn: TurnRef,
        *,
        requested_by: AgentRef,
        reason: str = "operator_stop",
        note: str | None = None,
        meta: OperationMeta | None = None,
    ) -> TurnRef:
        merged = OperationMeta(
            note=note if note is not None else (None if meta is None else meta.note),
            reason=reason if reason is not None else (None if meta is None else meta.reason),
            annotations={} if meta is None else dict(meta.annotations),
        )
        return parse_turn_ref(
            self._post(
                f"/v3r1/projects/{turn.project_id}/management/turns/{turn.turn_id}:stop",
                {"turn": turn, "requested_by": requested_by, "meta": merged},
            )
        )

    def dispatch(
        self,
        *,
        requested_by: AgentRef,
        target_agent: AgentRef,
        input_payload: Any,
        authority: DispatchAuthority,
        dispatch_key: str,
        turn_kind: str = TURN_KIND_CONVERSATION_V1,
        meta: OperationMeta | None = None,
    ) -> TurnRef:
        authority_payload: dict[str, Any] = {"mode": authority.mode.value}
        if authority.mode == DispatchAuthorityMode.ROOT_REQUEST:
            authority_payload["request_id"] = authority.request_id
        elif authority.mode == DispatchAuthorityMode.CHILD_DERIVATION:
            authority_payload["parent_claim"] = authority.parent_claim
        return parse_turn_ref(
            self._post(
                f"/v3r1/projects/{requested_by.project_id}/turns:dispatch",
                {
                    "requested_by": requested_by,
                    "target_agent": target_agent,
                    "input": input_payload,
                    "turn_kind": turn_kind,
                    "dispatch_key": dispatch_key,
                    "authority": authority_payload,
                    "meta": meta,
                },
            )
        )

    def claim_turn_handle(
        self,
        agent: AgentRef,
        *,
        meta: OperationMeta | None = None,
    ) -> ClaimToken | None:
        data = self._post(
            f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/claims:claim",
            {
                "agent": agent,
                "meta": meta,
            },
        )
        if data is None:
            return None
        return parse_claim_token(data)

    def claim_turn(
        self,
        agent: AgentRef,
        *,
        meta: OperationMeta | None = None,
        context_after_turn_seq: int = 0,
        context_limit: int = 100,
    ) -> ClaimedTurn | None:
        claim = self.claim_turn_handle(agent, meta=meta)
        if claim is None:
            return None
        try:
            context = self.fetch_context(
                claim.turn_ref(),
                after_turn_seq=context_after_turn_seq,
                limit=context_limit,
            )
        except Exception as exc:
            suspend_error = try_suspend_after_context_fetch_error(self, claim, exc)
            raise ClaimTurnPartialFailure(
                claim=claim,
                context_error=exc,
                suspend_error=suspend_error,
            ) from exc
        return ClaimedTurn(claim=claim, context=context)

    def renew_claim(self, claim: ClaimToken, *, meta: OperationMeta | None = None) -> ClaimRenewal:
        return parse_claim_renewal(self._post(f"/v3r1/projects/{claim.project_id}/claims:renew", {"claim": claim, "meta": meta}))

    def reconcile_expired_claim(
        self,
        agent: AgentRef | None = None,
        *,
        project_id: str | None = None,
        meta: OperationMeta | None = None,
    ) -> ReconcileSummary:
        resolved_project_id = agent.project_id if agent is not None else project_id
        if resolved_project_id is None:
            raise ValueError("project_id is required when reconciling without an explicit agent")
        return parse_reconcile_summary(
            self._post(f"/v3r1/projects/{resolved_project_id}/claims:reconcile-expired", {"agent": agent, "meta": meta})
        )

    def get_turn(self, turn: TurnRef) -> TurnSnapshot:
        return parse_turn_snapshot(self._get(f"/v3r1/projects/{turn.project_id}/turns/{turn.turn_id}", params={}))

    def is_stop_requested(self, turn: TurnRef) -> bool:
        return self.get_turn(turn).stop_requested

    def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100) -> TurnContext:
        return parse_turn_context(
            self._get(
                f"/v3r1/projects/{turn.project_id}/turns/{turn.turn_id}/context",
                params={"after_turn_seq": after_turn_seq, "limit": limit},
            )
        )

    def fetch_turn_feed(self, turn: TurnRef, *, after_ledger_seq: int = 0, limit: int = 100):
        return parse_ledger_feed_page(
            self._get(
                f"/v3r1/projects/{turn.project_id}/turns/{turn.turn_id}/feed",
                params={"after_ledger_seq": after_ledger_seq, "limit": limit},
            )
        )

    def fetch_agent_feed(self, agent: AgentRef, *, after_ledger_seq: int = 0, limit: int = 100):
        return parse_ledger_feed_page(
            self._get(
                f"/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/feed",
                params={
                    "after_ledger_seq": after_ledger_seq,
                    "limit": limit,
                },
            )
        )

    def append_record(self, claim: ClaimToken, payload: Any, *, role: str = "progress", meta: OperationMeta | None = None):
        return self._post(f"/v3r1/projects/{claim.project_id}/turns/{claim.turn_id}/semantic-records", {"claim": claim, "payload": payload, "role": role, "meta": meta})

    def suspend_turn(self, claim: ClaimToken, *, reason: str, note: str | None = None, meta: OperationMeta | None = None) -> None:
        self._post(f"/v3r1/projects/{claim.project_id}/turns/{claim.turn_id}:suspend", {"claim": claim, "reason": reason, "note": note, "meta": meta})

    def resume_turn(self, requested_by: AgentRef, turn: TurnRef, *, note: str | None = None, meta: OperationMeta | None = None) -> None:
        self._post(f"/v3r1/projects/{turn.project_id}/turns/{turn.turn_id}:resume", {"requested_by": requested_by, "note": note, "meta": meta})

    def finish_turn(self, claim: ClaimToken, *, outcome: TurnOutcome, final_payload: Any = UNSET_PAYLOAD, final_record_role: str = "deliverable", meta: OperationMeta | None = None) -> None:
        payload = {"claim": claim, "outcome": outcome.value, "final_record_role": final_record_role, "meta": meta}
        if final_payload is not UNSET_PAYLOAD:
            payload["final_payload"] = final_payload
        self._post(f"/v3r1/projects/{claim.project_id}/turns/{claim.turn_id}:finish", payload)

    def _post(self, path: str, payload: Any):
        response = self._client.post(path, json=to_jsonable(payload), headers=self._default_headers or None)
        raise_for_status_with_detail(response)
        if not response.content or response.text == "null":
            return None
        return response.json()

    def _put(self, path: str, payload: Any):
        response = self._client.put(path, json=to_jsonable(payload), headers=self._default_headers or None)
        raise_for_status_with_detail(response)
        if not response.content or response.text == "null":
            return None
        return response.json()

    def _get(self, path: str, *, params: dict[str, Any]):
        response = self._client.get(path, params=params, headers=self._default_headers or None)
        raise_for_status_with_detail(response)
        return response.json()
