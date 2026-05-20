from __future__ import annotations

import asyncio
from typing import Protocol

from CommonGround.contracts import ClaimToken, TURN_KIND_CONVERSATION_V1, TurnOutcome
from CommonGround.agent_client import FinishTurnAction, HttpAgentClient
from CommonGround.sdk import TurnContext

from .context_mapping import build_turn_session_key, render_leaf_prompt


class _DirectLoop(Protocol):
    async def process_direct(self, content: str, session_key: str = "cli:direct", channel: str = "cli", chat_id: str = "direct", on_progress=None, on_stream=None, on_stream_end=None):
        ...


class LeafTurnHandler:
    def __init__(self, *, loop: _DirectLoop, emit_progress: bool = False) -> None:
        self._loop = loop
        self._emit_progress = emit_progress

    def handle_turn(self, context: TurnContext, client: HttpAgentClient, claim: ClaimToken) -> FinishTurnAction:
        if context.turn.turn_kind != TURN_KIND_CONVERSATION_V1:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "error": "unexpected_turn_kind",
                    "expected": TURN_KIND_CONVERSATION_V1,
                    "actual": context.turn.turn_kind,
                },
            )
        prompt = render_leaf_prompt(context)

        async def _run():
            async def _progress(text: str, *, tool_hint: bool = False, **_: object) -> None:
                if not self._emit_progress:
                    return
                text = text.strip()
                if not text:
                    return
                payload = {"progress": text}
                if tool_hint:
                    payload["tool_hint"] = True
                client.append_record(claim, payload, role="progress")

            return await self._loop.process_direct(
                prompt,
                session_key=build_turn_session_key(agent_id=claim.agent_id, turn_id=claim.turn_id),
                channel="cg",
                chat_id=claim.turn_id,
                on_progress=_progress,
            )

        response = asyncio.run(_run())
        content = "" if response is None else ((response.content or "").strip())
        return FinishTurnAction(
            outcome=TurnOutcome.SUCCEEDED,
            final_payload={
                "content": content,
                "agent_id": claim.agent_id,
                "turn_id": claim.turn_id,
            },
        )
