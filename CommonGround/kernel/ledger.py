from __future__ import annotations

from CommonGround.contracts import LedgerFeedPage, LedgerFeedScope, TruthRepositoryPort


class LedgerKernel:
    def __init__(self, truth: TruthRepositoryPort) -> None:
        self._truth = truth

    def fetch_ledger_feed(
        self,
        project_id: str,
        after_ledger_seq: int,
        scope: LedgerFeedScope | None = None,
        limit: int = 100,
    ) -> LedgerFeedPage:
        return self._truth.fetch_ledger_feed(
            project_id=project_id,
            after_ledger_seq=after_ledger_seq,
            scope=scope,
            limit=limit,
        )
