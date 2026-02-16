from __future__ import annotations

from core.config_defaults import DEFAULT_PMO_FORK_JOIN_MAX_TASKS
from services.pmo.internal_handlers.fork_join import ForkJoinHandler
from services.pmo.service import PMOService


def test_pmo_service_passes_fork_join_max_tasks_to_handler() -> None:
    service = PMOService(config={"pmo": {"fork_join_max_tasks": 3}})
    handler = service._internal_handlers[ForkJoinHandler.name]
    assert isinstance(handler, ForkJoinHandler)
    assert service.fork_join_max_tasks == 3
    assert handler.max_tasks == 3


def test_pmo_service_falls_back_when_fork_join_max_tasks_invalid() -> None:
    service = PMOService(config={"pmo": {"fork_join_max_tasks": 0}})
    handler = service._internal_handlers[ForkJoinHandler.name]
    assert isinstance(handler, ForkJoinHandler)
    assert service.fork_join_max_tasks == DEFAULT_PMO_FORK_JOIN_MAX_TASKS
    assert handler.max_tasks == DEFAULT_PMO_FORK_JOIN_MAX_TASKS
