from __future__ import annotations

from typing import Protocol, runtime_checkable

from CommonGround.contracts import AgentRow, LedgerFeedPage, TurnRow


@runtime_checkable
class ProjectionSource(Protocol):
    def list_agent_rows(
        self,
        *,
        project_id: str,
        agent_id: str | None = None,
        role: str | None = None,
        capability: str | None = None,
        enabled_only: bool | None = None,
        accepts_work_only: bool | None = None,
        limit: int | None = 100,
    ) -> tuple[AgentRow, ...]:
        ...

    def get_turn_row(self, *, project_id: str, turn_id: str) -> TurnRow | None:
        ...

    def list_turn_rows(
        self,
        *,
        project_id: str,
        target_agent_id: str | None = None,
        turn_kind: str | None = None,
        state: str | None = None,
        outcome: str | None = None,
        stop_requested_only: bool | None = None,
        limit: int = 100,
    ) -> tuple[TurnRow, ...]:
        ...

    def list_child_turn_rows(
        self,
        *,
        project_id: str,
        parent_turn_id: str,
        limit: int = 100,
    ) -> tuple[TurnRow, ...]:
        ...

    def fetch_project_feed(
        self,
        *,
        project_id: str,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> LedgerFeedPage:
        ...
