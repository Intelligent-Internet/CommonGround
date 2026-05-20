from __future__ import annotations

from dataclasses import dataclass

from CommonGround.contracts import (
    AgentRef,
    ClaimToken,
    HydratedCardBox,
    OperationMeta,
    SemanticRecordSnapshot,
    TraceContext,
    TurnSnapshot,
)


@dataclass(frozen=True, slots=True)
class SemanticContextItem:
    record: SemanticRecordSnapshot
    content: HydratedCardBox


@dataclass(frozen=True, slots=True)
class TurnContext:
    turn: TurnSnapshot
    semantic_items: tuple[SemanticContextItem, ...]


@dataclass(frozen=True, slots=True)
class KernelOpContext:
    claim: ClaimToken | None = None
    requested_by: AgentRef | None = None
    meta: OperationMeta | None = None
    trace: TraceContext | None = None
