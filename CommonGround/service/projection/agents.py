from __future__ import annotations

from .filters import AgentDirectoryFilters
from .models import ProjectedAgentDirectoryPage, ProjectedAgentEntry
from .source import ProjectionSource


def list_agent_directory(
    source: ProjectionSource,
    *,
    project_id: str,
    filters: AgentDirectoryFilters,
) -> ProjectedAgentDirectoryPage:
    rows = source.list_agent_rows(
        project_id=project_id,
        agent_id=filters.agent_id,
        role=filters.role,
        capability=filters.capability,
        enabled_only=filters.enabled_only,
        accepts_work_only=filters.accepts_work_only,
        limit=filters.limit,
    )
    return ProjectedAgentDirectoryPage(
        project_id=project_id,
        items=tuple(
            ProjectedAgentEntry(
                agent_id=row.agent_id,
                role=row.role,
                description=row.description,
                enabled=row.enabled,
                accepts_work=row.accepts_work,
                capabilities=row.capabilities,
                public_metadata=dict(row.public_metadata),
                last_seen_at=row.last_seen_at,
                grants=row.grants or None,
            )
            for row in rows
        ),
        limit=filters.limit,
    )
