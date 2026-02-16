"""
Deterministic Word Count Tool Service (UTP)

Listens on: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.word_count.*
Behavior:
  - mode=map: tokenize text and return deterministic counts
  - mode=reduce: merge multiple map outputs deterministically
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from typing import Any, Dict, List, Tuple

from core.app_config import config_to_dict, load_app_config
from core.config import PROTOCOL_VERSION
from core.errors import BadRequestError, ProtocolViolationError
from core.subject import subject_pattern
from core.utils import safe_str, set_loop_policy
from core.utp_protocol import ALLOWED_AFTER_EXECUTION_VALUES, Card
from infra.agent_routing import resolve_agent_target
from infra.service_runtime import ServiceBase
from infra.tool_executor import ToolCallContext
from services.tools.tool_runner import (
    ToolPayloadValidation,
    ToolRunner,
    build_tool_idempotency,
)

set_loop_policy()

logger = logging.getLogger("WordCountTool")

_TOKEN_TRIM_RE = re.compile(r"^[^\w]+|[^\w]+$")


def _normalize_top_k(value: Any, *, default: int = 50, limit: int = 2000) -> int:
    if value is None:
        return int(default)
    if isinstance(value, bool):
        raise BadRequestError("top_k must be a positive integer")
    try:
        parsed = int(value)
    except Exception as exc:  # noqa: BLE001
        raise BadRequestError("top_k must be a positive integer") from exc
    if parsed <= 0:
        raise BadRequestError("top_k must be a positive integer")
    return min(parsed, int(limit))


def _normalize_mode(value: Any) -> str:
    mode = safe_str(value).strip().lower()
    if not mode:
        mode = "map"
    if mode not in {"map", "reduce"}:
        raise BadRequestError("mode must be one of: map, reduce")
    return mode


def _tokenize(text: str) -> List[str]:
    tokens: List[str] = []
    for raw in str(text or "").split():
        lowered = raw.lower().strip()
        normalized = _TOKEN_TRIM_RE.sub("", lowered)
        if normalized:
            tokens.append(normalized)
    return tokens


def _sorted_items(counter: Counter[str], *, top_k: int) -> List[Tuple[str, int]]:
    ranked = sorted(counter.items(), key=lambda item: (-int(item[1]), item[0]))
    return ranked[: int(top_k)]


def map_word_count(*, text: str, top_k: int) -> Dict[str, Any]:
    tokens = _tokenize(text)
    counter: Counter[str] = Counter(tokens)
    ranked_items = _sorted_items(counter, top_k=top_k)
    counts_list = [{"word": word, "count": int(count)} for word, count in ranked_items]
    counts_map = {word: int(count) for word, count in ranked_items}
    return {
        "mode": "map",
        "top_k": int(top_k),
        "total_tokens": len(tokens),
        "unique_tokens": len(counter),
        "counts": counts_list,
        "counts_map": counts_map,
    }


def _extract_counts_map(item: Any) -> Dict[str, int]:
    if not isinstance(item, dict):
        return {}
    counts_map_raw = item.get("counts_map")
    if isinstance(counts_map_raw, dict):
        out: Dict[str, int] = {}
        for word, count_any in counts_map_raw.items():
            key = safe_str(word).strip().lower()
            if not key:
                continue
            try:
                count = int(count_any)
            except Exception:  # noqa: BLE001
                continue
            if count > 0:
                out[key] = out.get(key, 0) + count
        return out

    counts_raw = item.get("counts")
    if isinstance(counts_raw, list):
        out = {}
        for entry in counts_raw:
            if not isinstance(entry, dict):
                continue
            key = safe_str(entry.get("word")).strip().lower()
            if not key:
                continue
            try:
                count = int(entry.get("count"))
            except Exception:  # noqa: BLE001
                continue
            if count > 0:
                out[key] = out.get(key, 0) + count
        return out
    return {}


def reduce_word_count(*, items: List[Any], top_k: int) -> Dict[str, Any]:
    if not isinstance(items, list):
        raise BadRequestError("items must be an array")
    merged: Counter[str] = Counter()
    for entry in items:
        merged.update(_extract_counts_map(entry))
    ranked_items = _sorted_items(merged, top_k=top_k)
    counts_list = [{"word": word, "count": int(count)} for word, count in ranked_items]
    counts_map = {word: int(count) for word, count in ranked_items}
    return {
        "mode": "reduce",
        "top_k": int(top_k),
        "input_items": len(items),
        "unique_tokens": len(merged),
        "counts": counts_list,
        "counts_map": counts_map,
    }


class WordCountToolService(ServiceBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg, use_nats=True, use_cardbox=True)
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        idempotency = build_tool_idempotency(self.nats)
        self.idempotency_store = idempotency.store
        self.idempotent_executor = idempotency.executor
        self.tool_runner = ToolRunner(
            target="word_count",
            source_agent_id="tool.word_count",
            nats=self.nats,
            cardbox=self.cardbox,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            idempotent_executor=self.idempotent_executor,
            payload_validation=ToolPayloadValidation(
                forbid_keys={"args"},
                require_tool_call_card_id=True,
                require_after_execution=True,
                allowed_after_execution=ALLOWED_AFTER_EXECUTION_VALUES,
            ),
            result_author_id="sys.tool.word_count",
            logger=logger,
            service_name="WordCount",
        )

    async def start(self) -> None:
        await self.open()
        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="tool",
            target="word_count",
            suffix="*",
            protocol_version=PROTOCOL_VERSION,
        )
        queue = "tool_word_count"
        logger.info("WordCount started. Listening on %s (queue=%s)", subject, queue)
        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._handle_cmd,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )
        await asyncio.Event().wait()

    async def _execute_logic(self, ctx: ToolCallContext) -> Dict[str, Any]:
        parts = ctx.parts
        payload = ctx.payload
        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=parts.project_id,
            agent_id=str(payload.agent_id),
        )
        if not target:
            raise ProtocolViolationError(
                "missing worker_target for agent",
                detail={
                    "project_id": parts.project_id,
                    "agent_id": str(payload.agent_id),
                    "tool_call_id": str(payload.tool_call_id),
                },
            )
        args = ctx.args
        mode = _normalize_mode(args.get("mode"))
        top_k = _normalize_top_k(args.get("top_k"), default=50, limit=2000)
        if mode == "map":
            text = args.get("text")
            if not isinstance(text, str):
                raise BadRequestError("mode=map requires string field 'text'")
            return map_word_count(text=text, top_k=top_k)
        items = args.get("items")
        return reduce_word_count(items=items, top_k=top_k)

    async def _handle_cmd(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]) -> None:
        async def _execute(ctx: ToolCallContext) -> Dict[str, Any]:
            return await self._execute_logic(ctx)

        run_result = await self.tool_runner.run(
            subject=subject,
            data=data,
            headers=headers,
            execute=_execute,
            tool_name_fallback="word_count",
        )
        if not run_result.handled:
            return

        tool_call_id = safe_str(data.get("tool_call_id"))
        if run_result.is_leader:
            logger.info("WordCount tool_result sent (leader) tool_call_id=%s", tool_call_id)
        elif run_result.is_leader is False:
            logger.info("WordCount tool_result re-sent (follower) tool_call_id=%s", tool_call_id)


async def main() -> None:
    cfg = config_to_dict(load_app_config())
    service = WordCountToolService(cfg)
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
