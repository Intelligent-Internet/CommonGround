from __future__ import annotations

from .filters import TurnEntryFilters
from .models import ProjectedTurnEntry, ProjectedTurnEntryPage
from .source import ProjectionSource


def list_turn_entries(
    source: ProjectionSource,
    *,
    project_id: str,
    filters: TurnEntryFilters,
) -> ProjectedTurnEntryPage:
    rows = source.list_turn_rows(
        project_id=project_id,
        target_agent_id=filters.target_agent_id,
        turn_kind=filters.turn_kind,
        state=filters.state,
        outcome=filters.outcome,
        stop_requested_only=filters.stop_requested_only,
        limit=filters.limit,
    )
    return ProjectedTurnEntryPage(
        project_id=project_id,
        items=tuple(project_turn_entry_from_row(row) for row in rows),
        limit=filters.limit,
    )


def project_turn_entry_from_row(row) -> ProjectedTurnEntry:
    return ProjectedTurnEntry(
        turn_id=row.turn_id,
        project_turn_seq=row.project_turn_seq,
        target_agent_id=row.target_agent_id,
        turn_kind=row.turn_kind,
        state=row.state,
        outcome=row.outcome,
        stop_requested=row.stop_requested,
        cause_kind=row.cause_kind,
        cause_id=row.cause_id,
        created_at=row.created_at,
        updated_at=row.updated_at,
        closed_at=row.closed_at,
    )
