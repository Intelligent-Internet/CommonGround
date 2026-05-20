from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from CommonGround.agent_client import FinishTurnAction, HttpAgentClient, SuspendTurnAction
from CommonGround.contracts import (
    AgentRef,
    ClaimToken,
    DispatchAuthority,
    DispatchAuthorityMode,
    TURN_KIND_CONVERSATION_V1,
    TurnOutcome,
    TurnRef,
)
from CommonGround.sdk import TurnContext

from .context_mapping import extract_bootstrap_payload
from ..runtime.feed_utils import fetch_turn_feed_items

USER_REQUEST_KIND_V1 = "cg.user_request.v1"
FORWARDED_USER_REQUEST_KIND_V1 = "cg.forwarded_user_request.v1"


@dataclass(frozen=True, slots=True)
class SelfRootIngressBinding:
    root_turn: TurnRef
    self_agent: AgentRef
    external_thread: Mapping[str, Any] | None
    request_id: str
    dispatch_key: str

    def as_persistable(self) -> dict[str, Any]:
        return {
            "root_turn": {"project_id": self.root_turn.project_id, "turn_id": self.root_turn.turn_id},
            "self_agent": {"project_id": self.self_agent.project_id, "agent_id": self.self_agent.agent_id},
            "external_thread": self.external_thread,
            "request_id": self.request_id,
            "dispatch_key": self.dispatch_key,
        }


def build_user_request_payload(
    *,
    target_agent_id: str,
    message: Any,
    external_thread: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": USER_REQUEST_KIND_V1,
        "target_agent_id": target_agent_id,
        "message": message,
    }
    if external_thread is not None:
        payload["external_thread"] = dict(external_thread)
    return payload


def validate_user_request_payload(payload: Any, *, project_id: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("root payload must be an object")
    if payload.get("kind") != USER_REQUEST_KIND_V1:
        raise ValueError(f"root payload kind must be {USER_REQUEST_KIND_V1!r}")
    target_agent_id = payload.get("target_agent_id")
    if not isinstance(target_agent_id, str) or not target_agent_id.strip():
        raise ValueError("target_agent_id must be a non-empty string")
    target_project_id = payload.get("target_project_id")
    if target_project_id is not None and target_project_id != project_id:
        raise ValueError("target_project_id must match the root turn project")
    target_agent_ref = payload.get("target_agent_ref")
    if target_agent_ref is not None:
        if not isinstance(target_agent_ref, dict):
            raise ValueError("target_agent_ref must be an object when present")
        ref_project_id = target_agent_ref.get("project_id")
        ref_agent_id = target_agent_ref.get("agent_id")
        if ref_project_id is not None and ref_project_id != project_id:
            raise ValueError("target_agent_ref.project_id must match the root turn project")
        if ref_agent_id is not None and ref_agent_id != target_agent_id:
            raise ValueError("target_agent_ref.agent_id must match target_agent_id")
    if "message" not in payload:
        raise ValueError("message is required")
    external_thread = payload.get("external_thread")
    if external_thread is not None and not isinstance(external_thread, dict):
        raise ValueError("external_thread must be an object when present")
    return payload


class SelfRootIngress:
    def __init__(
        self,
        *,
        client: HttpAgentClient,
        self_agent: AgentRef,
        turn_kind: str = TURN_KIND_CONVERSATION_V1,
    ) -> None:
        self._client = client
        self._self_agent = self_agent
        self._turn_kind = turn_kind

    def dispatch_user_message(
        self,
        *,
        target_agent_id: str,
        message: Any,
        request_id: str,
        dispatch_key: str | None = None,
        external_thread: Mapping[str, Any] | None = None,
    ) -> SelfRootIngressBinding:
        resolved_dispatch_key = dispatch_key or request_id
        payload = build_user_request_payload(
            target_agent_id=target_agent_id,
            message=message,
            external_thread=external_thread,
        )
        root_turn = self._client.dispatch(
            requested_by=self._self_agent,
            target_agent=self._self_agent,
            input_payload=payload,
            authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
            dispatch_key=resolved_dispatch_key,
            turn_kind=self._turn_kind,
        )
        return SelfRootIngressBinding(
            root_turn=root_turn,
            self_agent=self._self_agent,
            external_thread=None if external_thread is None else dict(external_thread),
            request_id=request_id,
            dispatch_key=resolved_dispatch_key,
        )


class SelfRootFrontsideHandler:
    def __init__(self, *, child_turn_kind: str = TURN_KIND_CONVERSATION_V1) -> None:
        self._child_turn_kind = child_turn_kind

    def handle_turn(self, context: TurnContext, client: HttpAgentClient, claim: ClaimToken):
        try:
            payload = validate_user_request_payload(
                extract_bootstrap_payload(context),
                project_id=claim.project_id,
            )
        except ValueError as exc:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={"error": "invalid_user_request", "message": str(exc)},
            )

        target_agent = AgentRef(project_id=claim.project_id, agent_id=payload["target_agent_id"].strip())
        if target_agent == claim.agent_ref():
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "error": "invalid_target_agent",
                    "message": "target_agent_id must not be the self frontside agent",
                },
            )
        parent_turn = claim.turn_ref()
        child_turn_id = _latest_child_turn_id(client, parent_turn, turn_kind=self._child_turn_kind)
        if child_turn_id is None:
            target_snapshot = client.get_agent(target_agent)
            if target_snapshot is None:
                return FinishTurnAction(
                    outcome=TurnOutcome.FAILED,
                    final_payload={"error": "target_agent_not_found", "target_agent_id": target_agent.agent_id},
                )
            if not getattr(target_snapshot, "enabled", True) or not getattr(target_snapshot, "accepts_work", True):
                return FinishTurnAction(
                    outcome=TurnOutcome.FAILED,
                    final_payload={
                        "error": "target_agent_unavailable",
                        "target_agent_id": target_agent.agent_id,
                        "enabled": bool(getattr(target_snapshot, "enabled", False)),
                        "accepts_work": bool(getattr(target_snapshot, "accepts_work", False)),
                    },
                )
            child_turn = client.dispatch(
                requested_by=claim.agent_ref(),
                target_agent=target_agent,
                input_payload=_build_forwarded_payload(payload=payload, parent_claim=claim, target_agent=target_agent),
                authority=DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim),
                dispatch_key=f"{claim.turn_id}:method2:child:1",
                turn_kind=self._child_turn_kind,
            )
            return SuspendTurnAction(reason="await_child", note=f"awaiting child {child_turn.turn_id}")

        child_ref = TurnRef(project_id=claim.project_id, turn_id=child_turn_id)
        child_snapshot = client.get_turn(child_ref)
        if child_snapshot.outcome is None:
            return SuspendTurnAction(reason="await_child", note=f"child {child_turn_id} still active")
        if child_snapshot.outcome != TurnOutcome.SUCCEEDED:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload=_build_parent_final_payload(
                    claim=claim,
                    payload=payload,
                    target_agent=target_agent,
                    child_ref=child_ref,
                    child_result=child_snapshot.final_payload,
                    child_outcome=child_snapshot.outcome,
                    error="child_failed",
                ),
            )

        return FinishTurnAction(
            outcome=TurnOutcome.SUCCEEDED,
            final_payload=_build_parent_final_payload(
                claim=claim,
                payload=payload,
                target_agent=target_agent,
                child_ref=child_ref,
                child_result=child_snapshot.final_payload,
                child_outcome=child_snapshot.outcome,
            ),
        )


def _build_forwarded_payload(
    *,
    payload: Mapping[str, Any],
    parent_claim: ClaimToken,
    target_agent: AgentRef,
) -> dict[str, Any]:
    forwarded = {
        "kind": FORWARDED_USER_REQUEST_KIND_V1,
        "message": payload["message"],
        "external_thread": payload.get("external_thread"),
        "root_turn": {"project_id": parent_claim.project_id, "turn_id": parent_claim.turn_id},
        "source_agent": {"project_id": parent_claim.project_id, "agent_id": parent_claim.agent_id},
        "target_agent": {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id},
    }
    return {key: value for key, value in forwarded.items() if value is not None}


def _build_parent_final_payload(
    *,
    claim: ClaimToken,
    payload: Mapping[str, Any],
    target_agent: AgentRef,
    child_ref: TurnRef,
    child_result: Any,
    child_outcome: TurnOutcome,
    error: str | None = None,
) -> dict[str, Any]:
    final_payload = {
        "root": {"project_id": claim.project_id, "turn_id": claim.turn_id, "agent_id": claim.agent_id},
        "child": {"project_id": child_ref.project_id, "turn_id": child_ref.turn_id, "outcome": child_outcome.value},
        "target": {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id},
        "external_thread": payload.get("external_thread"),
        "child_result": child_result,
    }
    if error is not None:
        final_payload["error"] = error
    return final_payload


def _latest_child_turn_id(client: HttpAgentClient, parent_turn: TurnRef, *, turn_kind: str) -> str | None:
    child_ids: list[str] = []
    for event in fetch_turn_feed_items(client, parent_turn):
        if event.event_type != "turn.spawned" or event.subject_id == parent_turn.turn_id:
            continue
        child_ref = TurnRef(project_id=parent_turn.project_id, turn_id=event.subject_id)
        if client.get_turn(child_ref).turn_kind == turn_kind:
            child_ids.append(event.subject_id)
    return None if not child_ids else child_ids[-1]
