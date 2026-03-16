from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol, Tuple

from core.cg_context import CGContext


InternalHandlerResult = Tuple[bool, Dict[str, Any]]


@dataclass(frozen=True, slots=True)
class InternalHandlerDeps:
    handover: Any
    batch_manager: Any
    state_store: Any
    resource_store: Any
    cardbox: Any
    nats: Any
    execution_store: Any = None
    identity_store: Any = None


class InternalHandler(Protocol):
    name: str

    async def handle(
        self,
        *,
        deps: InternalHandlerDeps,
        ctx: CGContext,
        cmd: Any,
        parent_after_execution: str,
    ) -> InternalHandlerResult: ...
