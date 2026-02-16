import pytest

from infra.agent_routing import resolve_agent_target


class DummyResourceStore:
    def __init__(self, row=None, exc: Exception | None = None):
        self.row = row
        self.exc = exc
        self.calls = []

    async def fetch_roster(self, project_id, agent_id, *, conn=None):
        self.calls.append((project_id, agent_id, conn))
        if self.exc:
            raise self.exc
        return self.row


@pytest.mark.asyncio
async def test_resolve_agent_target_prefers_worker_target():
    store = DummyResourceStore(row={"worker_target": " ui_worker "})
    target = await resolve_agent_target(
        resource_store=store,
        project_id="proj",
        agent_id="agent",
        default_target="worker_generic",
    )
    assert target == "ui_worker"


@pytest.mark.asyncio
async def test_resolve_agent_target_falls_back_on_missing_worker_target():
    store = DummyResourceStore(row={"worker_target": "   "})
    target = await resolve_agent_target(
        resource_store=store,
        project_id="proj",
        agent_id="agent",
        default_target="worker_generic",
    )
    assert target == "worker_generic"


@pytest.mark.asyncio
async def test_resolve_agent_target_falls_back_on_errors():
    store = DummyResourceStore(exc=RuntimeError("db down"))
    target = await resolve_agent_target(
        resource_store=store,
        project_id="proj",
        agent_id="agent",
        default_target="worker_generic",
    )
    assert target == "worker_generic"


@pytest.mark.asyncio
async def test_resolve_agent_target_passes_conn_when_provided():
    store = DummyResourceStore(row={"worker_target": "worker_generic"})
    marker_conn = object()
    target = await resolve_agent_target(
        resource_store=store,
        project_id="proj",
        agent_id="agent",
        conn=marker_conn,
    )
    assert target == "worker_generic"
    assert store.calls == [("proj", "agent", marker_conn)]
