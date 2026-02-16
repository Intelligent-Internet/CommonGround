"""
Emit Marker tool service

Listen to subject: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.emit_marker.*
Behavior:
  - Read tool.call cards and extract marker payload.
  - Write ack card to tool.result.
  - Write Inbox first, then publish cmd.agent.{target}.wakeup to wake up the worker.
  - Publish evt.sys.ui.marker for UI usage.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config, config_to_dict
from core.errors import (
    BadRequestError,
)
from core.subject import format_subject, subject_pattern
from core.time_utils import to_iso, utc_now
from core.trace import trace_id_from_headers
from core.utils import set_loop_policy
from infra.service_runtime import ServiceBase
from infra.tool_executor import ToolCallContext
from services.tools.tool_runner import (
    ToolPayloadValidation,
    ToolRunner,
    build_tool_idempotency,
)

set_loop_policy()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("EmitMarkerTool")


class EmitMarkerToolService(ServiceBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        idempotency = build_tool_idempotency(self.nats)
        self.idempotency_store = idempotency.store
        self.idempotent_executor = idempotency.executor
        self.tool_runner = ToolRunner(
            target="emit_marker",
            source_agent_id="tool.emit_marker",
            nats=self.nats,
            cardbox=self.cardbox,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            idempotent_executor=self.idempotent_executor,
            payload_validation=ToolPayloadValidation(
                forbid_keys={"args"},
                require_tool_call_card_id=True,
            ),
            result_author_id="tool_emit_marker",
            logger=logger,
            service_name="EmitMarker",
        )

    async def start(self):
        await self.open()

        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="tool",
            target="emit_marker",
            suffix="*",
            protocol_version=PROTOCOL_VERSION,
        )
        queue = "tool_emit_marker"
        logger.info("Listening: %s (queue=%s)", subject, queue)
        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._handle_cmd,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )

        await asyncio.Event().wait()

    async def _execute_logic(
        self,
        ctx: ToolCallContext,
    ) -> Dict[str, Any]:
        parts = ctx.parts
        headers = ctx.headers
        payload = ctx.payload
        tool_call_id = payload.tool_call_id
        tool_call_card_id = ctx.tool_call_card_id
        agent_id = payload.agent_id

        tool_call_meta = ctx.tool_call_meta
        args = ctx.args
        marker_type = args.get("marker_type")
        title = args.get("title")
        markdown_content = args.get("markdown_content")
        if not marker_type or not title or not markdown_content:
            raise BadRequestError("emit_marker requires title, markdown_content, and marker_type")

        event_payload = {
            "marker_type": marker_type,
            "tool_call_card_id": str(tool_call_card_id),
            "source_agent_id": str(agent_id),
            "trace_id": tool_call_meta.get("trace_id") or trace_id_from_headers(headers),
            "ts": to_iso(utc_now()),
        }
        event_subject = format_subject(
            parts.project_id,
            parts.channel_id,
            "evt",
            "sys",
            "ui",
            "marker",
        )
        try:
            await self.nats.publish_event(
                event_subject,
                event_payload,
                headers=headers,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("emit_marker event publish failed: %s", exc, exc_info=True)
        return {
            "message": "marker_emitted",
            "llm_instruction": "A recommendation has been generated; ask the user to review it and decide whether to adopt it.",
        }

    async def _handle_cmd(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]):
        async def _execute(ctx: ToolCallContext) -> Dict[str, Any]:
            return await self._execute_logic(ctx)

        run_result = await self.tool_runner.run(
            subject=subject,
            data=data,
            headers=headers,
            execute=_execute,
            tool_name_fallback="emit_marker",
        )
        if not run_result.handled:
            return

        tool_call_id = str(data.get("tool_call_id") or "")
        if run_result.is_leader:
            logger.info("Marker emitted: tool_call_id=%s", tool_call_id)
        elif run_result.is_leader is False:
            logger.info("Marker reused (follower): tool_call_id=%s", tool_call_id)


async def main():
    cfg = config_to_dict(load_app_config())
    service = EmitMarkerToolService(cfg)
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Exit gracefully on Ctrl+C without printing a traceback.
        pass
