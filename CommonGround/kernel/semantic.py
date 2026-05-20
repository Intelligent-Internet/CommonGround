from __future__ import annotations

from CommonGround.contracts import (
    CardBoxPort,
    ClaimToken,
    ConflictError,
    NotFoundError,
    OperationMeta,
    SemanticRecordPage,
    SemanticRecordRef,
    SemanticRecordSpec,
    TraceContext,
    TurnRef,
    TruthRepositoryPort,
)


class SemanticKernel:
    def __init__(self, truth: TruthRepositoryPort, cardbox: CardBoxPort) -> None:
        self._truth = truth
        self._cardbox = cardbox

    def append_semantic_record(
        self,
        claim: ClaimToken,
        record: SemanticRecordSpec,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> SemanticRecordRef:
        del trace
        if record.cardbox_ref.project_id != claim.project_id:
            raise ConflictError("semantic record cardbox project must match turn project")
        if not self._cardbox.box_exists(record.cardbox_ref):
            raise NotFoundError(f"cardbox ref not found: {record.cardbox_ref.cardbox_id}")
        return self._truth.append_semantic_record_primitive(claim=claim, record=record, meta=meta)

    def list_semantic_records(
        self,
        turn: TurnRef,
        after_turn_seq: int = 0,
        limit: int = 100,
    ) -> SemanticRecordPage:
        items = [item for item in self._truth.get_semantic_records(turn) if item.turn_seq > after_turn_seq]
        items = tuple(items[:limit])
        next_after = after_turn_seq if not items else items[-1].turn_seq
        return SemanticRecordPage(items=items, next_after_turn_seq=next_after)
