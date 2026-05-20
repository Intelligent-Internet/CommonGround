from __future__ import annotations

from fastapi import APIRouter, Query, Request

from CommonGround.service.read_policy import ReadSurfaceKind, authorize_read
from CommonGround.service.serialization import to_jsonable

from .agents import list_agent_directory
from .feed import fetch_project_feed
from .filters import AgentDirectoryFilters, TurnEntryFilters, TurnOfferFilters
from .lineage import get_turn_lineage
from .offers import list_turn_offer_entries
from .turns import list_turn_entries


router = APIRouter()


def _deps(request: Request):
    return request.app.state.service_deps


def _authorize_projection(
    request: Request,
    *,
    project_id: str,
    resource_family: str,
    resource_id: str | None = None,
):
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.PROJECTION,
        resource_family=resource_family,
        resource_id=resource_id,
    )


@router.get("/v3r1/projects/{project_id}/projection/agents")
def projection_agents(
    project_id: str,
    request: Request,
    agent_id: str | None = Query(None),
    enabled_only: bool | None = Query(None),
    accepts_work_only: bool | None = Query(None),
    role: str | None = Query(None),
    capability: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    _authorize_projection(request, project_id=project_id, resource_family="agent_directory")
    page = list_agent_directory(
        _deps(request).projection_source,
        project_id=project_id,
        filters=AgentDirectoryFilters(
            agent_id=agent_id,
            role=role,
            capability=capability,
            enabled_only=enabled_only,
            accepts_work_only=accepts_work_only,
            limit=limit,
        ),
    )
    return to_jsonable(page)


@router.get("/v3r1/projects/{project_id}/projection/turn-offers")
def projection_turn_offers(
    project_id: str,
    request: Request,
    turn_kind: str | None = Query(None),
    agent_id: str | None = Query(None),
    enabled_only: bool | None = Query(None),
    accepts_work_only: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    _authorize_projection(request, project_id=project_id, resource_family="turn_offer_entries")
    page = list_turn_offer_entries(
        _deps(request).projection_source,
        project_id=project_id,
        filters=TurnOfferFilters(
            turn_kind=turn_kind,
            agent_id=agent_id,
            enabled_only=enabled_only,
            accepts_work_only=accepts_work_only,
            limit=limit,
        ),
    )
    return to_jsonable(page)


@router.get("/v3r1/projects/{project_id}/projection/turns")
def projection_turns(
    project_id: str,
    request: Request,
    target_agent_id: str | None = Query(None),
    turn_kind: str | None = Query(None),
    state: str | None = Query(None),
    outcome: str | None = Query(None),
    stop_requested_only: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    _authorize_projection(request, project_id=project_id, resource_family="turn_entries")
    page = list_turn_entries(
        _deps(request).projection_source,
        project_id=project_id,
        filters=TurnEntryFilters(
            target_agent_id=target_agent_id,
            turn_kind=turn_kind,
            state=state,
            outcome=outcome,
            stop_requested_only=stop_requested_only,
            limit=limit,
        ),
    )
    return to_jsonable(page)


@router.get("/v3r1/projects/{project_id}/projection/turns/{turn_id}/lineage")
def projection_turn_lineage(
    project_id: str,
    turn_id: str,
    request: Request,
    limit: int = Query(100, ge=1, le=500),
):
    _authorize_projection(request, project_id=project_id, resource_family="turn_lineage", resource_id=turn_id)
    page = get_turn_lineage(
        _deps(request).projection_source,
        project_id=project_id,
        turn_id=turn_id,
        limit=limit,
    )
    return to_jsonable(page)


@router.get("/v3r1/projects/{project_id}/projection/feed")
def projection_feed(
    project_id: str,
    request: Request,
    after_ledger_seq: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
):
    _authorize_projection(request, project_id=project_id, resource_family="project_feed")
    page = fetch_project_feed(
        _deps(request).projection_source,
        project_id=project_id,
        after_ledger_seq=after_ledger_seq,
        limit=limit,
    )
    return to_jsonable(page)
