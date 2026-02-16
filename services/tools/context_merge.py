"""
Context Merge Tool Service (L2 Data Flow)

Listens on: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.context_merge.*
Behavior:
    - Reads multiple Context Boxes.
    - Merges or Summarizes content server-side.
    - Writes result to a new Output Box.
    - Returns pointer (box_id) only.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple

import uuid6

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config, config_to_dict
from core.errors import BadRequestError, ProtocolViolationError
from core.llm import LLMConfig, LLMRequest
from core.subject import subject_pattern
from core.utils import set_loop_policy
from core.utp_protocol import Card as SchemaCard, TextContent, extract_card_content_text
from infra.agent_routing import resolve_agent_target
from infra.llm_gateway import LLMService
from infra.service_runtime import ServiceBase
from infra.tool_executor import ToolCallContext
from services.tools.tool_runner import (
    ToolPayloadValidation,
    ToolRunner,
    build_tool_idempotency,
)

set_loop_policy()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ContextMergeTool")


class ContextMergeTool(ServiceBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        idempotency = build_tool_idempotency(self.nats)
        self.idempotency_store = idempotency.store
        self.idempotent_executor = idempotency.executor
        self.tool_runner = ToolRunner(
            target="context_merge",
            source_agent_id="tool.context_merge",
            nats=self.nats,
            cardbox=self.cardbox,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            idempotent_executor=self.idempotent_executor,
            payload_validation=ToolPayloadValidation(
                forbid_keys={"args"},
                require_tool_call_card_id=True,
            ),
            result_author_id="sys.tool.context_merge",
            logger=logger,
            service_name="ContextMerge",
        )
        self.llm = LLMService()

        # Default LLM Config for summarization
        # In a real app, this should come from config.toml or tool args
        self.default_llm_config = LLMConfig(
            model="gpt-4o",  # Powerful model for summarization
            temperature=1.0,
            max_tokens=4000,
            stream=False,  # No need to stream for background job
        )

    async def start(self):
        await self.open()

        # Listen on wildcard project/channel
        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="tool",
            target="context_merge",
            suffix="*",
            protocol_version=PROTOCOL_VERSION,
        )
        queue = "tool_context_merge"
        logger.info("🚀 Started. Listening on %s (queue=%s)", subject, queue)

        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._handle_cmd,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        # Keep alive without periodic wakeups.
        await asyncio.Event().wait()

    @staticmethod
    def _build_concat_card_text(*, content_text: str, source_index: int) -> str:
        # Keep source attribution human-readable without leaking internal card IDs.
        normalized_index = max(1, int(source_index))
        return f"--- Source {normalized_index} ---\n{content_text}"

    async def _execute_logic(
        self,
        ctx: ToolCallContext,
    ) -> Dict[str, Any]:
        parts = ctx.parts
        payload = ctx.payload
        tool_call_id = payload.tool_call_id
        agent_turn_id = payload.agent_turn_id
        turn_epoch = payload.turn_epoch
        agent_id = payload.agent_id
        after_exec = payload.after_execution or "suspend"

        target_agent_id = str(agent_id)
        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=parts.project_id,
            agent_id=target_agent_id,
        )
        if not target:
            raise ProtocolViolationError(
                "missing worker_target for agent",
                detail={
                    "project_id": parts.project_id,
                    "agent_id": target_agent_id,
                    "tool_call_id": tool_call_id,
                },
            )

        tool_call_meta = ctx.tool_call_meta
        args = ctx.args
        box_ids = args.get("box_ids")
        mode = args.get("mode", "concat")  # concat | summarize
        instruction = args.get("instruction", "Summarize the following content.")

        if not box_ids or not isinstance(box_ids, list):
            raise BadRequestError("Missing or invalid 'box_ids'")
        if any(not isinstance(bid, (str, int)) or not str(bid) for bid in box_ids):
            raise BadRequestError("Invalid 'box_ids': expected list of non-empty strings")

        logger.info(
            "📥 Processing MERGE: mode=%s boxes=%s from=%s turn=%s call=%s epoch=%s after=%s",
            mode,
            len(box_ids),
            agent_id,
            agent_turn_id,
            tool_call_id,
            turn_epoch,
            after_exec,
        )

        # 1. Fetch all cards from all boxes
        all_cards: List[SchemaCard] = []
        for box_id in box_ids:
            box = await self.cardbox.get_box(box_id, project_id=parts.project_id)
            if box and box.card_ids:
                cards = await self.cardbox.get_cards(box.card_ids, project_id=parts.project_id)
                all_cards.extend(cards)

        # 2. Process
        output_card_ids: List[str] = []

        if mode == "concat":
            for source_index, c in enumerate(all_cards, start=1):
                content_str = extract_card_content_text(c)
                if content_str:
                    new_card = SchemaCard(
                        card_id=uuid6.uuid7().hex,
                        project_id=parts.project_id,
                        type="task.context",
                        content=TextContent(
                            text=self._build_concat_card_text(
                                content_text=content_str,
                                source_index=source_index,
                            )
                        ),
                        created_at=datetime.now(UTC),
                        author_id=agent_id,
                        metadata={"role": "user"},
                    )
                    await self.cardbox.save_card(new_card)
                    output_card_ids.append(new_card.card_id)
        elif mode == "summarize":
            full_text = "\n\n".join(
                [extract_card_content_text(c) for c in all_cards if extract_card_content_text(c)]
            )
            summary_text = await self._call_llm_summarize(full_text, instruction)
            new_card = SchemaCard(
                card_id=uuid6.uuid7().hex,
                project_id=parts.project_id,
                type="task.context",
                content=TextContent(text=f"# Summary\n\n{summary_text}"),
                created_at=datetime.now(UTC),
                author_id="sys.tool.context_merge",
                metadata={"role": "user"},
            )
            await self.cardbox.save_card(new_card)
            output_card_ids.append(new_card.card_id)
        else:
            raise BadRequestError(f"Unknown mode: {mode}")

        new_box_id = await self.cardbox.save_box(output_card_ids, project_id=parts.project_id)
        return {
            "output_box_id": str(new_box_id),
            "mode": mode,
            "source_count": len(box_ids),
            "card_count": len(output_card_ids),
        }

    async def _handle_cmd(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]):
        async def _execute(ctx: ToolCallContext) -> Dict[str, Any]:
            return await self._execute_logic(ctx)

        run_result = await self.tool_runner.run(
            subject=subject,
            data=data,
            headers=headers,
            execute=_execute,
            tool_name_fallback="context_merge",
        )
        if not run_result.handled:
            return

        tool_call_id = str(data.get("tool_call_id") or "")
        if run_result.is_leader:
            logger.info("ContextMerge tool_result sent (leader) tool_call_id=%s", tool_call_id)
        elif run_result.is_leader is False:
            logger.info("ContextMerge tool_result re-sent (follower) tool_call_id=%s", tool_call_id)

    async def _call_llm_summarize(self, text: str, instruction: str) -> str:
        if not text.strip():
            return "No content to summarize."

        # Truncate to avoid context window explosion (simple safety)
        # Assuming char count approx token * 4
        limit = 50000
        if len(text) > limit:
            text = text[:limit] + "\n...[Truncated]..."

        messages = [
            {"role": "system", "content": "You are a helpful assistant that summarizes context."},
            {"role": "user", "content": f"{instruction}\n\n---\n{text}"},
        ]

        req = LLMRequest(
            messages=messages,
            config=self.default_llm_config,
        )

        try:
            resp = await self.llm.completion(req)
            return resp.get("content", "")
        except Exception as exc:  # noqa: BLE001
            logger.error("LLM Call Failed: %s", exc)
            return f"Summarization failed: {exc}"


async def main():
    cfg = config_to_dict(load_app_config())
    service = ContextMergeTool(cfg)
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Allow Ctrl+C exit without a stacktrace.
        pass
