from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from scripts.utils.agent import ensure_agent


class _StubResourceStore:
    def __init__(self) -> None:
        self.fetch_calls: List[Any] = []

    async def fetch_roster(self, ctx: Any) -> Dict[str, Any] | None:
        self.fetch_calls.append(ctx)
        if len(self.fetch_calls) == 1:
            return None
        return {"profile_box_id": "profile_box_1"}

    async def find_profile_by_name(self, project_id: str, name: str) -> Dict[str, Any] | None:
        assert project_id == "proj_test"
        assert name == "worker"
        return {"profile_box_id": "profile_box_1"}

    async def list_profiles(self, project_id: str, *, limit: int = 500, offset: int = 0) -> List[Dict[str, Any]]:
        _ = (project_id, limit, offset)
        return []


@pytest.mark.asyncio
async def test_ensure_agent_uses_ctx_only_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Dict[str, Any]] = []

    async def _fake_ensure_agent_ready(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return SimpleNamespace(error_code=None)

    monkeypatch.setattr("scripts.utils.agent.ensure_agent_ready", _fake_ensure_agent_ready)

    resource_store = _StubResourceStore()
    roster, profile_box_id = await ensure_agent(
        resource_store,
        state_store=object(),
        project_id="proj_test",
        channel_id="public",
        agent_id="agent_worker_1",
        ensure_agent=True,
        profile_box_id=None,
        profile_name="worker",
        display_name="Worker 1",
        owner_agent_id=None,
        worker_target="worker_generic",
        tags=["bench"],
        metadata_source="test",
    )

    assert profile_box_id == "profile_box_1"
    assert roster["profile_box_id"] == "profile_box_1"
    assert len(resource_store.fetch_calls) == 2
    assert resource_store.fetch_calls[0].project_id == "proj_test"
    assert resource_store.fetch_calls[0].channel_id == "public"
    assert resource_store.fetch_calls[0].agent_id == "agent_worker_1"
    assert calls[0]["target_ctx"].project_id == "proj_test"
    assert calls[0]["target_ctx"].channel_id == "public"
    assert calls[0]["target_ctx"].agent_id == "agent_worker_1"
