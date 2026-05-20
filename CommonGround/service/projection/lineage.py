from __future__ import annotations

from CommonGround.contracts import NotFoundError

from .models import ProjectedTurnLineage
from .source import ProjectionSource
from .turns import project_turn_entry_from_row


def get_turn_lineage(
    source: ProjectionSource,
    *,
    project_id: str,
    turn_id: str,
    limit: int = 100,
) -> ProjectedTurnLineage:
    parent_row = source.get_turn_row(project_id=project_id, turn_id=turn_id)
    if parent_row is None:
        raise NotFoundError(f"turn not found: {turn_id}")
    children = source.list_child_turn_rows(project_id=project_id, parent_turn_id=turn_id, limit=limit)
    return ProjectedTurnLineage(
        project_id=project_id,
        turn_id=turn_id,
        parent=project_turn_entry_from_row(parent_row),
        direct_children=tuple(project_turn_entry_from_row(row) for row in children),
        limit=limit,
    )
