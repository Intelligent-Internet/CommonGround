from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from core.subject import format_subject
from infra.nats_client import NATSClient


async def subscribe_task_queue(
    nats: NATSClient,
    *,
    subject: Optional[str] = None,
    project_id: Optional[str] = None,
    channel_id: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> asyncio.Queue:
    if not subject:
        if not (project_id and channel_id and agent_id):
            raise ValueError("subject or (project_id, channel_id, agent_id) required")
        subject = format_subject(project_id, channel_id, "evt", "agent", agent_id, "task")

    queue: asyncio.Queue = asyncio.Queue()

    async def _on_task(msg) -> None:
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        await queue.put(data)

    await nats.subscribe_core(subject, _on_task)
    return queue


async def wait_for_task_event(
    queue: asyncio.Queue,
    *,
    agent_turn_id: str,
    timeout_s: float,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("timeout waiting for evt.agent.*.task")
        payload = await asyncio.wait_for(queue.get(), timeout=remaining)
        if payload.get("agent_turn_id") == agent_turn_id:
            return payload
