from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.cg_context import CGContext
from core.errors import BadRequestError, NotFoundError, ProtocolViolationError
from core.subject import parse_subject
from core.utp_protocol import extract_tool_call_args
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient


@dataclass
class SubjectMeta:
    project_id: str
    channel_id: str
    component: str
    target: str
    tool_suffix: str


def parse_subject_cmd(subject: str) -> Optional[SubjectMeta]:
    """Parse cmd subjects: cg.<ver>.<proj>.<chan>.cmd.sys.pmo.<tool>."""
    parts = parse_subject(subject)
    if not parts:
        return None
    if parts.category != "cmd" or parts.component != "sys" or parts.target != "pmo":
        return None
    return SubjectMeta(
        project_id=parts.project_id,
        channel_id=parts.channel_id,
        component=parts.component,
        target=parts.target,
        tool_suffix=parts.suffix,
    )


class PMOToolCommand:
    """Lightweight command DTO."""

    def __init__(self, data: Dict[str, Any], ctx: CGContext):
        self.tool_name: str = data.get("tool_name") or "unknown"
        if "tool_call_id" in data:
            raise ProtocolViolationError("protocol violation: payload.tool_call_id is forbidden (use CG-Tool-Call-Id)")
        try:
            self.tool_call_id = ctx.require_tool_call_id
        except ProtocolViolationError as exc:
            raise BadRequestError("missing required header: CG-Tool-Call-Id") from exc
        self.after_execution: str = data.get("after_execution") or ""
        if self.after_execution not in ("suspend", "terminate"):
            raise BadRequestError("invalid required field: after_execution")

        self.tool_call_card_id: Optional[str] = safe_str(data.get("tool_call_card_id"))
        if not self.tool_call_card_id:
            raise BadRequestError("missing required field: tool_call_card_id")
        self.tool_call_parent_step_id: Optional[str] = None

        if "args" in data:
            raise ProtocolViolationError("protocol violation: payload.args is forbidden (use tool_call_card_id)")

        self.arguments: Dict[str, Any] = {}

    async def hydrate_arguments(self, *, cardbox: CardBoxClient, project_id: str) -> None:
        cards = await cardbox.get_cards([str(self.tool_call_card_id)], project_id=project_id)
        tool_call = cards[0] if cards else None
        if not tool_call:
            raise NotFoundError(f"tool_call_card_id not found: {self.tool_call_card_id}")
        metadata = getattr(tool_call, "metadata", {}) or {}
        if isinstance(metadata, dict):
            parent_step_id = safe_str(metadata.get("parent_step_id"))
            self.tool_call_parent_step_id = parent_step_id or None
        self.arguments = extract_tool_call_args(tool_call)


async def decode_tool_command(
    *,
    data: Dict[str, Any],
    ctx: CGContext,
    cardbox: CardBoxClient,
) -> PMOToolCommand:
    cmd = PMOToolCommand(data or {}, ctx)
    await cmd.hydrate_arguments(cardbox=cardbox, project_id=ctx.project_id)
    return cmd
