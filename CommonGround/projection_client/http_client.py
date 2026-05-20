from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

import httpx

from CommonGround.agent_client import agent_auth_headers
from CommonGround.contracts import AgentRef
from CommonGround.contracts import TurnOutcome, TurnState
from CommonGround.http_errors import raise_for_status_with_detail

from .types import (
    ProjectedAgentDirectoryPage,
    ProjectedAgentEntry,
    ProjectedFeedEvent,
    ProjectedProjectFeedPage,
    ProjectedTurnOfferEntry,
    ProjectedTurnOfferEntryPage,
    ProjectedTurnEntry,
    ProjectedTurnEntryPage,
    ProjectedTurnLineage,
    ProjectionDiagnostic,
)


class ProjectionHttpClient:
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

    def list_agents(
        self,
        *,
        project_id: str,
        agent_id: str | None = None,
        enabled_only: bool | None = None,
        accepts_work_only: bool | None = None,
        role: str | None = None,
        capability: str | None = None,
        limit: int = 100,
    ) -> ProjectedAgentDirectoryPage:
        return _parse_agent_directory_page(
            self._get(
                f"/v3r1/projects/{project_id}/projection/agents",
                params={
                    "agent_id": agent_id,
                    "enabled_only": enabled_only,
                    "accepts_work_only": accepts_work_only,
                    "role": role,
                    "capability": capability,
                    "limit": limit,
                },
            )
        )

    def list_turn_offers(
        self,
        *,
        project_id: str,
        turn_kind: str | None = None,
        agent_id: str | None = None,
        enabled_only: bool | None = None,
        accepts_work_only: bool | None = None,
        limit: int = 100,
    ) -> ProjectedTurnOfferEntryPage:
        return _parse_turn_offer_entry_page(
            self._get(
                f"/v3r1/projects/{project_id}/projection/turn-offers",
                params={
                    "turn_kind": turn_kind,
                    "agent_id": agent_id,
                    "enabled_only": enabled_only,
                    "accepts_work_only": accepts_work_only,
                    "limit": limit,
                },
            )
        )

    def list_turns(
        self,
        *,
        project_id: str,
        target_agent_id: str | None = None,
        turn_kind: str | None = None,
        state: str | None = None,
        outcome: str | None = None,
        stop_requested_only: bool | None = None,
        limit: int = 100,
    ) -> ProjectedTurnEntryPage:
        return _parse_turn_entry_page(
            self._get(
                f"/v3r1/projects/{project_id}/projection/turns",
                params={
                    "target_agent_id": target_agent_id,
                    "turn_kind": turn_kind,
                    "state": state,
                    "outcome": outcome,
                    "stop_requested_only": stop_requested_only,
                    "limit": limit,
                },
            )
        )

    def get_turn_lineage(
        self,
        *,
        project_id: str,
        turn_id: str,
        limit: int = 100,
    ) -> ProjectedTurnLineage:
        return _parse_turn_lineage(
            self._get(
                f"/v3r1/projects/{project_id}/projection/turns/{turn_id}/lineage",
                params={"limit": limit},
            )
        )

    def fetch_project_feed(
        self,
        *,
        project_id: str,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> ProjectedProjectFeedPage:
        return _parse_project_feed_page(
            self._get(
                f"/v3r1/projects/{project_id}/projection/feed",
                params={"after_ledger_seq": after_ledger_seq, "limit": limit},
            )
        )

    def _get(self, path: str, *, params: dict[str, Any]):
        clean_params = {key: value for key, value in params.items() if value is not None}
        response = self._client.get(path, params=clean_params, headers=self._default_headers or None)
        raise_for_status_with_detail(response)
        return response.json()


def _parse_projection_diagnostics(data: Any) -> tuple[ProjectionDiagnostic, ...]:
    if not isinstance(data, list):
        return ()
    return tuple(
        ProjectionDiagnostic(code=item["code"], message=item["message"], subject_id=item.get("subject_id"))
        for item in data
        if isinstance(item, Mapping)
    )


def _parse_agent_entry(data: Mapping[str, Any]) -> ProjectedAgentEntry:
    return ProjectedAgentEntry(
        agent_id=data["agent_id"],
        role=data.get("role"),
        description=data.get("description"),
        enabled=data["enabled"],
        accepts_work=data["accepts_work"],
        capabilities=tuple(data.get("capabilities", ())),
        public_metadata=dict(data.get("public_metadata", {})),
        last_seen_at=None if data.get("last_seen_at") is None else datetime.fromisoformat(data["last_seen_at"]),
        grants=None if data.get("grants") is None else tuple(data["grants"]),
    )


def _parse_agent_directory_page(data: Mapping[str, Any]) -> ProjectedAgentDirectoryPage:
    return ProjectedAgentDirectoryPage(
        project_id=data["project_id"],
        items=tuple(_parse_agent_entry(item) for item in data["items"]),
        limit=data["limit"],
        diagnostics=_parse_projection_diagnostics(data.get("diagnostics")),
    )


def _parse_turn_offer_entry(data: Mapping[str, Any]) -> ProjectedTurnOfferEntry:
    return ProjectedTurnOfferEntry(
        agent_id=data["agent_id"],
        agent_label=data.get("agent_label"),
        agent_description=data.get("agent_description"),
        turn_kind=data["turn_kind"],
        purpose=data.get("purpose"),
        calling=dict(data.get("calling", {})),
        input_contract=dict(data.get("input_contract", {})),
        variants=dict(data.get("variants", {})),
        enabled=data["enabled"],
        accepts_work=data["accepts_work"],
        metadata_source=data["metadata_source"],
    )


def _parse_turn_offer_entry_page(data: Mapping[str, Any]) -> ProjectedTurnOfferEntryPage:
    return ProjectedTurnOfferEntryPage(
        project_id=data["project_id"],
        items=tuple(_parse_turn_offer_entry(item) for item in data["items"]),
        limit=data["limit"],
        diagnostics=_parse_projection_diagnostics(data.get("diagnostics")),
    )


def _parse_turn_entry(data: Mapping[str, Any]) -> ProjectedTurnEntry:
    return ProjectedTurnEntry(
        turn_id=data["turn_id"],
        project_turn_seq=data["project_turn_seq"],
        target_agent_id=data["target_agent_id"],
        turn_kind=data["turn_kind"],
        state=TurnState(data["state"]),
        outcome=None if data.get("outcome") is None else TurnOutcome(data["outcome"]),
        stop_requested=data["stop_requested"],
        cause_kind=data["cause_kind"],
        cause_id=data["cause_id"],
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
        closed_at=None if data.get("closed_at") is None else datetime.fromisoformat(data["closed_at"]),
    )


def _parse_turn_entry_page(data: Mapping[str, Any]) -> ProjectedTurnEntryPage:
    return ProjectedTurnEntryPage(
        project_id=data["project_id"],
        items=tuple(_parse_turn_entry(item) for item in data["items"]),
        limit=data["limit"],
        diagnostics=_parse_projection_diagnostics(data.get("diagnostics")),
    )


def _parse_turn_lineage(data: Mapping[str, Any]) -> ProjectedTurnLineage:
    return ProjectedTurnLineage(
        project_id=data["project_id"],
        turn_id=data["turn_id"],
        parent=_parse_turn_entry(data["parent"]),
        direct_children=tuple(_parse_turn_entry(item) for item in data["direct_children"]),
        limit=data["limit"],
        diagnostics=_parse_projection_diagnostics(data.get("diagnostics")),
    )


def _parse_feed_event(data: Mapping[str, Any]) -> ProjectedFeedEvent:
    payload_ref = data.get("payload_ref")
    return ProjectedFeedEvent(
        ledger_seq=data["ledger_seq"],
        event_type=data["event_type"],
        subject_kind=data["subject_kind"],
        subject_id=data["subject_id"],
        actor_kind=data["actor_kind"],
        actor_id=data["actor_id"],
        cause_kind=data.get("cause_kind"),
        cause_id=data.get("cause_id"),
        created_at=datetime.fromisoformat(data["created_at"]),
        note=data.get("note"),
        annotations=dict(data.get("annotations", {})),
        payload_ref=None if payload_ref is None else dict(payload_ref),
    )


def _parse_project_feed_page(data: Mapping[str, Any]) -> ProjectedProjectFeedPage:
    return ProjectedProjectFeedPage(
        project_id=data["project_id"],
        items=tuple(_parse_feed_event(item) for item in data["items"]),
        limit=data["limit"],
        next_after_ledger_seq=data["next_after_ledger_seq"],
        diagnostics=_parse_projection_diagnostics(data.get("diagnostics")),
    )
