"""Judge service for tool failure retry decisions."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from core.app_config import load_app_config, config_to_dict
from core.config import PROTOCOL_VERSION
from core.subject import subject_pattern
from core.utils import set_loop_policy
from infra.llm_gateway import LLMService, LLMWrapper
from infra.mock_llm_handler import mock_chat
from infra.nats_client import NATSClient
from core.llm import LLMRequest

set_loop_policy()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("JudgeService")


def _build_prompt(payload: Dict[str, Any]) -> list[Dict[str, str]]:
    tool_name = payload.get("tool_name") or ""
    error = payload.get("error") or {}
    args = payload.get("args") or {}
    attempt = payload.get("attempt") or 0
    max_attempts = payload.get("max_attempts") or 0
    skill_name = payload.get("skill_name") or ""

    system = (
        "You are a tool failure judge. Decide whether to retry a tool call or fail fast. "
        "Return ONLY strict JSON with keys: action (retry|fail_fast), reason, hint. "
        "Prefer fail_fast for unrecoverable errors or if attempts are exhausted."
    )
    user = {
        "tool_name": tool_name,
        "skill_name": skill_name,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "error": error,
        "args": args,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
    ]


def _parse_decision(text: str) -> Dict[str, Any]:
    try:
        data = json.loads(text)
    except Exception:
        return {"action": "fail_fast", "reason": "invalid_json", "hint": ""}
    action = str(data.get("action") or "").strip().lower()
    if action not in {"retry", "fail_fast"}:
        action = "fail_fast"
    return {
        "action": action,
        "reason": str(data.get("reason") or ""),
        "hint": str(data.get("hint") or ""),
    }


class JudgeService:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self.nats = NATSClient(config=self.cfg.get("nats", {}))
        self.llm = LLMWrapper(LLMService(), mock_chat=mock_chat)
        judge_cfg = self.cfg.get("judge", {}) if isinstance(self.cfg, dict) else {}
        self.model = str(judge_cfg.get("model") or "gemini/gemini-2.5-flash")
        self.temperature = float(judge_cfg.get("temperature", 0.0))
        self.timeout_s = float(judge_cfg.get("timeout_s", 8.0))

    async def start(self) -> None:
        await self.nats.connect()
        subject = subject_pattern(
            project_id="*",
            channel_id="*",
            category="cmd",
            component="sys",
            target="judge",
            suffix="retry",
            protocol_version=PROTOCOL_VERSION,
        )
        logger.info("Listening on %s", subject)
        await self.nats.subscribe_core(subject, self._handle_request)
        while True:
            await asyncio.sleep(1)

    async def _handle_request(self, msg) -> None:
        try:
            payload = json.loads(msg.data.decode("utf-8")) if msg.data else {}
        except Exception:
            payload = {}
        prompt = _build_prompt(payload)
        request = LLMRequest(
            messages=prompt,
            tools=None,
            config={
                "model": self.model,
                "temperature": self.temperature,
                "stream": False,
            },
        )
        decision: Dict[str, Any]
        try:
            resp = await asyncio.wait_for(self.llm.chat(request), timeout=self.timeout_s)
            text = (resp.get("content") or "").strip()
            decision = _parse_decision(text)
        except Exception as exc:  # noqa: BLE001
            decision = {"action": "fail_fast", "reason": f"judge_error: {exc}", "hint": ""}

        if msg.reply:
            await self.nats.publish_core(msg.reply, json.dumps(decision).encode("utf-8"))


async def main() -> None:
    cfg = config_to_dict(load_app_config())
    service = JudgeService(cfg)
    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
