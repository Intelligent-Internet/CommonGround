"""
Mock Web Search Tool Service (UTP)

Listens on: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.search.*
Behavior: reads args from tool.call card, generates 3 plausible search results, writes tool.result,
          then reports to Inbox and emits cmd.agent.{worker_target}.wakeup.
By default it attempts to call an LLM (Gemini) to generate results; falls back to a static set on failure.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config, config_to_dict
from core.errors import (
    ProtocolViolationError,
)
from core.llm import LLMConfig, LLMRequest
from core.subject import subject_pattern
from core.utils import set_loop_policy
from core.utp_protocol import (
    ALLOWED_AFTER_EXECUTION_VALUES,
)
from infra.agent_routing import resolve_agent_target
from infra.cardbox_client import CardBoxClient
from infra.llm_gateway import LLMService
from infra.nats_client import NATSClient
from infra.service_runtime import ServiceBase
from infra.tool_executor import ToolCallContext
from services.tools.tool_runner import (
    ToolPayloadValidation,
    ToolRunner,
    build_tool_idempotency,
)

set_loop_policy()


logger = logging.getLogger("SearchMock")


DEFAULT_MOCK_RESULTS = [
    {
        "title": "NATS - Connective Technology for Adaptive Systems",
        "url": "https://nats.io/what-is-nats",
        "snippet": "NATS is a connective technology that powers modern distributed systems with a lightweight pub/sub protocol.",
    },
    {
        "title": "NATS Documentation",
        "url": "https://docs.nats.io/",
        "snippet": "Official docs covering NATS server, clients, JetStream, security, and deployment guides.",
    },
    {
        "title": "Getting Started with NATS",
        "url": "https://docs.nats.io/running-a-nats-service/installation",
        "snippet": "Step-by-step instructions for installing and running the NATS server on your machine.",
    },
]


class MockSearchService(ServiceBase):
    def __init__(
        self,
        cfg: Dict[str, Any],
        *,
        nats: Optional[NATSClient] = None,
        cardbox: Optional[CardBoxClient] = None,
        llm: Optional[LLMService] = None,
    ):
        self._external_nats = nats is not None
        self._external_cardbox = cardbox is not None
        super().__init__(cfg, use_nats=not self._external_nats, use_cardbox=not self._external_cardbox)
        self.cfg = cfg or {}
        self.nats = nats or self.ctx.nats
        self.cardbox = cardbox or self.ctx.cardbox
        self.resource_store = self.register_store(self.ctx.stores.resource_store())
        self.execution_store = self.register_store(self.ctx.stores.execution_store())
        self.llm = llm or LLMService()
        idempotency = build_tool_idempotency(self.nats)
        self.idempotency_store = idempotency.store
        self.idempotent_executor = idempotency.executor
        self.tool_runner = ToolRunner(
            target="search",
            source_agent_id="tool.search",
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
            result_author_id="tool_search_mock",
            logger=logger,
            service_name="SearchMock",
        )
        
        # Debug flag (disabled by default). Use MOCK_SEARCH_DEBUG=1 when needed.
        self._debug_llm = self._resolve_debug_flag()

    def _resolve_debug_flag(self) -> bool:
        tools_cfg = self.cfg.get("tools") if isinstance(self.cfg, dict) else None
        mock_cfg = tools_cfg.get("mock_search") if isinstance(tools_cfg, dict) else None
        cfg_debug = mock_cfg.get("debug") if isinstance(mock_cfg, dict) else None
        env_debug = os.getenv("MOCK_SEARCH_DEBUG")
        if env_debug is not None:
            return env_debug.strip().lower() in {"1", "true", "yes", "on"}
        if cfg_debug is not None:
            return bool(cfg_debug)
        return False

    def _dbg(self, msg: str) -> None:
        if self._debug_llm:
            print(f"[SearchMock][LLM] {msg}")

    async def start(self):
        await self.open()
        if self._external_nats:
            await self.nats.connect()
        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="tool",
            target="search",
            suffix="*",
            protocol_version=PROTOCOL_VERSION,
        )
        queue = "tool_search_mock"
        print(f"[SearchMock] Listening on {subject} (queue={queue})")
        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._handle_cmd,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )
        await asyncio.Event().wait()

    @staticmethod
    def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
        """Best-effort: extract and parse a JSON object from arbitrary text."""
        if not isinstance(text, str):
            return None
        raw = text.strip()
        if not raw:
            return None

        def _normalize_text(s: str) -> str:
            return (
                s.replace("\ufeff", "")
                .replace("“", '"')
                .replace("”", '"')
                .replace("‘", "'")
                .replace("’", "'")
            ).strip()

        def _loads_relaxed(s: str) -> Optional[Dict[str, Any]]:
            s = _normalize_text(s)
            try:
                obj = json.loads(s)
                return obj if isinstance(obj, dict) else None
            except Exception:
                pass
            fixed = s
            for _ in range(3):
                newer = re.sub(r",(\s*[}\]])", r"\1", fixed)
                if newer == fixed:
                    break
                fixed = newer
            try:
                obj = json.loads(fixed)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None

        direct = _loads_relaxed(raw)
        if direct is not None:
            return direct

        if "```" in raw:
            blocks: list[tuple[str, str]] = []
            i = 0
            while True:
                start = raw.find("```", i)
                if start < 0:
                    break
                end = raw.find("```", start + 3)
                if end < 0:
                    break
                block = raw[start + 3 : end]
                block = block.lstrip("\n")
                first_line, _, rest = block.partition("\n")
                lang = first_line.strip().lower()
                content = rest if lang in {"json", "javascript", "js"} else block
                blocks.append((lang, content))
                i = end + 3

            for lang, content in sorted(blocks, key=lambda x: (x[0] != "json",)):
                parsed = _loads_relaxed(content)
                if parsed is not None:
                    return parsed

        candidates: list[Dict[str, Any]] = []
        in_str = False
        escape = False
        depth = 0
        start_idx: Optional[int] = None

        for idx, ch in enumerate(raw):
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == '"':
                    in_str = False
                continue

            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                if depth == 0:
                    start_idx = idx
                depth += 1
                continue
            if ch == "}":
                if depth > 0:
                    depth -= 1
                    if depth == 0 and start_idx is not None:
                        snippet = raw[start_idx : idx + 1]
                        parsed = _loads_relaxed(snippet)
                        if parsed is not None:
                            candidates.append(parsed)
                        start_idx = None
                continue

        if not candidates:
            return None
        for obj in candidates:
            if isinstance(obj.get("results"), list):
                return obj
        return candidates[0]

    async def _llm_generate_results(self, *, query: str) -> Optional[list[dict[str, str]]]:
        tools_cfg = self.cfg.get("tools") if isinstance(self.cfg, dict) else None
        mock_cfg = tools_cfg.get("mock_search") if isinstance(tools_cfg, dict) else None
        cfg_use_llm = mock_cfg.get("use_llm") if isinstance(mock_cfg, dict) else None
        env_use_llm = os.getenv("MOCK_SEARCH_USE_LLM")

        if env_use_llm is not None:
            use_llm = env_use_llm.strip().lower() not in {"0", "false", "no"}
        elif cfg_use_llm is not None:
            use_llm = bool(cfg_use_llm)
        else:
            use_llm = True
        if not use_llm:
            self._dbg("disabled (use_llm=false); skip generation")
            return None

        cfg_model = mock_cfg.get("llm_model") if isinstance(mock_cfg, dict) else None
        cfg_provider = mock_cfg.get("llm_provider") if isinstance(mock_cfg, dict) else None
        model = (
            (os.getenv("MOCK_SEARCH_LLM_MODEL") or "").strip()
            or (str(cfg_model).strip() if cfg_model else "")
            or "gemini/gemini-2.5-flash"
        )
        provider = (
            (os.getenv("MOCK_SEARCH_LLM_PROVIDER") or "").strip()
            or (str(cfg_provider).strip() if cfg_provider else "")
            or "gemini"
        )
        self._dbg(f"calling model={model!r} provider={provider!r} query={query!r}")

        system = (
            "You are a web search engine simulator for developer testing.\n"
            "Given a query, generate EXACTLY 3 search results.\n"
            "Return ONLY valid JSON (no markdown, no commentary) in this shape:\n"
            '{ "query": "...", "results": [ { "title": "...", "url": "...", "snippet": "..." } ] }\n'
            "Constraints:\n"
            "- results must be an array of length 3\n"
            "- title/url/snippet must be strings\n"
            "- url must start with https://\n"
            "- Make results plausible and relevant; you may use well-known domains.\n"
            "Do NOT wrap output in markdown fences like ```json.\n"
        )
        user = f"query: {query}"
        
        cfg = LLMConfig(
            model=model,
            provider_options={"custom_llm_provider": provider},
            stream=False,
            temperature=1.0,
            max_tokens=600,
        )
        req = LLMRequest(messages=[{"role": "system", "content": system}, {"role": "user", "content": user}], config=cfg)
        resp = await self.llm.completion(req)
        content = resp.get("content") if isinstance(resp, dict) else None
        if content is None:
            return None
        
        obj = self._extract_json_object(content or "")
        if not obj:
            return None
        results_any = obj.get("results")
        if not isinstance(results_any, list) or len(results_any) != 3:
            return None

        normalized: list[dict[str, str]] = []
        for r in results_any:
            if not isinstance(r, dict):
                return None
            title = r.get("title")
            url = r.get("url")
            snippet = r.get("snippet")
            if not all(isinstance(x, str) for x in (title, url, snippet)):
                return None
            if not url.startswith("https://"):
                return None
            normalized.append({"title": title, "url": url, "snippet": snippet})
        return normalized

    async def _build_results(self, *, query: str) -> list[dict[str, str]]:
        tools_cfg = self.cfg.get("tools") if isinstance(self.cfg, dict) else None
        mock_cfg = tools_cfg.get("mock_search") if isinstance(tools_cfg, dict) else None
        cfg_use_llm = mock_cfg.get("use_llm") if isinstance(mock_cfg, dict) else None
        env_use_llm = os.getenv("MOCK_SEARCH_USE_LLM")

        if env_use_llm is not None:
            use_llm = env_use_llm.strip().lower() not in {"0", "false", "no"}
        elif cfg_use_llm is not None:
            use_llm = bool(cfg_use_llm)
        else:
            use_llm = True

        if not use_llm:
            return list(DEFAULT_MOCK_RESULTS)

        try:
            results = await self._llm_generate_results(query=query)
            if results:
                self._dbg("using LLM-generated results")
                return results
        except Exception as exc:  # noqa: BLE001
            self._dbg(f"LLM generation raised {type(exc).__name__}: {exc}")

        self._dbg("LLM generation invalid; using empty results")
        return []

    async def _execute_search_logic(
        self,
        ctx: ToolCallContext,
    ) -> Dict[str, Any]:
        parts = ctx.parts
        payload = ctx.payload

        tool_call_id = payload.tool_call_id
        agent_turn_id = payload.agent_turn_id
        turn_epoch = payload.turn_epoch
        agent_id = payload.agent_id

        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=parts.project_id,
            agent_id=str(agent_id),
        )
        if not target:
            raise ProtocolViolationError(
                "missing worker_target for agent",
                detail={
                    "project_id": parts.project_id,
                    "agent_id": str(agent_id),
                    "tool_call_id": str(tool_call_id),
                },
            )
        args = ctx.args
        query = args.get("query", "")
        if not isinstance(query, str):
            query = str(query)

        print(
            f"[SearchMock] CMD recv turn={agent_turn_id} epoch={turn_epoch} tool_call_id={tool_call_id} query='{query}'"
        )
        results = await self._build_results(query=query)

        return {"query": query, "results": results}

    async def _handle_cmd(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]):
        async def _execute(ctx: ToolCallContext) -> Dict[str, Any]:
            print(f"[SearchMock] 🔒 Leader acquired tool_call_id={ctx.payload.tool_call_id}")
            return await self._execute_search_logic(ctx)

        run_result = await self.tool_runner.run(
            subject=subject,
            data=data,
            headers=headers,
            execute=_execute,
            tool_name_fallback="web_search",
        )
        if not run_result.handled:
            print(f"[SearchMock] invalid subject: {subject}")
            return

        tool_call_id = str(data.get("tool_call_id") or "")
        if run_result.is_leader:
            print(f"[SearchMock] ↩️ tool_result sent (leader) tool_call_id={tool_call_id}")
        elif run_result.is_leader is False:
            print(f"[SearchMock] ↩️ tool_result re-sent (follower) tool_call_id={tool_call_id}")


async def main():
    cfg = config_to_dict(load_app_config())
    service = MockSearchService(cfg)
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
