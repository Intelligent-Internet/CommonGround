from __future__ import annotations

from .models import ProjectedFeedEvent, ProjectedProjectFeedPage
from .source import ProjectionSource


def fetch_project_feed(
    source: ProjectionSource,
    *,
    project_id: str,
    after_ledger_seq: int = 0,
    limit: int = 100,
) -> ProjectedProjectFeedPage:
    page = source.fetch_project_feed(project_id=project_id, after_ledger_seq=after_ledger_seq, limit=limit)
    return ProjectedProjectFeedPage(
        project_id=project_id,
        items=tuple(_project_feed_event(item) for item in page.items),
        limit=limit,
        next_after_ledger_seq=page.next_after_ledger_seq,
    )


def _project_feed_event(event) -> ProjectedFeedEvent:
    return ProjectedFeedEvent(
        ledger_seq=event.ledger_seq,
        event_type=event.event_type,
        subject_kind=event.subject_kind,
        subject_id=event.subject_id,
        actor_kind=event.actor_kind,
        actor_id=event.actor_id,
        cause_kind=None if event.cause is None else event.cause.kind,
        cause_id=None if event.cause is None else event.cause.id,
        created_at=event.created_at,
        note=event.note,
        annotations=dict(event.annotations),
        payload_ref=None
        if event.payload_ref is None
        else {
            "project_id": event.payload_ref.project_id,
            "cardbox_id": event.payload_ref.cardbox_id,
        },
    )
