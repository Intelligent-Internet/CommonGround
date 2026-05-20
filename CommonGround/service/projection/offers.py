from __future__ import annotations

from typing import Mapping

from CommonGround.contracts import ConflictError
from CommonGround.turn_offers import extract_turn_offers

from .filters import TurnOfferFilters
from .models import ProjectionDiagnostic, ProjectedTurnOfferEntry, ProjectedTurnOfferEntryPage
from .source import ProjectionSource


_METADATA_SOURCE = "agent.public_metadata.turn_offers"


def list_turn_offer_entries(
    source: ProjectionSource,
    *,
    project_id: str,
    filters: TurnOfferFilters,
) -> ProjectedTurnOfferEntryPage:
    rows = source.list_agent_rows(
        project_id=project_id,
        agent_id=filters.agent_id,
        enabled_only=filters.enabled_only,
        accepts_work_only=filters.accepts_work_only,
        limit=None,
    )
    items: list[ProjectedTurnOfferEntry] = []
    diagnostics: list[ProjectionDiagnostic] = []
    for row in rows:
        try:
            offers = extract_turn_offers(row.public_metadata)
        except ConflictError as exc:
            diagnostics.append(ProjectionDiagnostic(code="invalid_public_metadata", message=str(exc), subject_id=row.agent_id))
            continue
        agent_label = _agent_label(row.public_metadata)
        for offer in offers:
            turn_kind = _required_str(offer, "turn_kind")
            if filters.turn_kind is not None and turn_kind != filters.turn_kind:
                continue
            if turn_kind not in row.capabilities:
                diagnostics.append(
                    ProjectionDiagnostic(
                        code="turn_offer_missing_capability",
                        message=f"turn_offers turn_kind not declared in agent capabilities: {turn_kind}",
                        subject_id=row.agent_id,
                    )
                )
                continue
            items.append(
                ProjectedTurnOfferEntry(
                    agent_id=row.agent_id,
                    agent_label=agent_label,
                    agent_description=row.description,
                    turn_kind=turn_kind,
                    purpose=_optional_str(offer, "purpose"),
                    calling=_required_mapping_as_dict(offer, "calling"),
                    input_contract=_required_mapping_as_dict(offer, "input_contract"),
                    variants=_mapping_as_dict(offer.get("variants")),
                    enabled=row.enabled,
                    accepts_work=row.accepts_work,
                    metadata_source=_METADATA_SOURCE,
                )
            )
    items.sort(key=lambda item: (item.turn_kind, item.agent_id))
    return ProjectedTurnOfferEntryPage(
        project_id=project_id,
        items=tuple(items[: filters.limit]),
        limit=filters.limit,
        diagnostics=tuple(diagnostics),
    )


def _agent_label(public_metadata: Mapping[str, object]) -> str | None:
    ui = public_metadata.get("ui")
    if not isinstance(ui, Mapping):
        return None
    label = ui.get("label")
    return label if isinstance(label, str) and label else None


def _optional_str(value: Mapping[str, object], key: str) -> str | None:
    inner = value.get(key)
    return inner if isinstance(inner, str) else None


def _required_str(value: Mapping[str, object], key: str) -> str:
    inner = value.get(key)
    if not isinstance(inner, str) or not inner:
        raise ConflictError(f"turn_offers[].{key} must be a non-empty string")
    return inner


def _required_mapping_as_dict(value: Mapping[str, object], key: str) -> dict[str, object]:
    inner = value.get(key)
    if not isinstance(inner, Mapping):
        raise ConflictError(f"turn_offers[].{key} must be an object")
    return dict(inner)


def _mapping_as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    return dict(value)
