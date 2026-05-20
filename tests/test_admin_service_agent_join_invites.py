from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
import time

from Integrations.admin_service import agent_join_invites
from Integrations.admin_service.agent_join_invites import AgentJoinInviteStore


def test_agent_join_invite_schema_ensure_is_thread_safe(monkeypatch) -> None:
    store = AgentJoinInviteStore("postgresql://unused")
    start = Barrier(8)
    calls = 0
    calls_lock = Lock()

    def fake_ensure_schema(*args, **kwargs):
        del args, kwargs
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.02)

    monkeypatch.setattr(agent_join_invites, "ensure_agent_join_invite_schema", fake_ensure_schema)

    def ensure_once() -> None:
        start.wait(timeout=5)
        store._ensure_schema_once()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(ensure_once) for _ in range(8)]
        for future in futures:
            future.result(timeout=5)

    assert calls == 1
