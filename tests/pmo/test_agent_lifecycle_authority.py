from __future__ import annotations

import pytest

from core.cg_context import CGContext
from services.pmo.agent_lifecycle import CreateAgentSpec, DerivedAgentSpec, ensure_agent_ready


class _DummyResourceStore:
    def __init__(self, *, roster: dict, profiles: dict[str, dict], profiles_by_name: dict[str, dict] | None = None) -> None:
        self.roster = dict(roster)
        self.profiles = dict(profiles)
        self.profiles_by_name = dict(profiles_by_name or {})
        self.upsert_calls: list[dict] = []

    async def fetch_roster(self, ctx: CGContext, conn=None):  # noqa: ANN001
        _ = ctx, conn
        return dict(self.roster)

    async def fetch_profile(self, project_id: str, profile_box_id: str, conn=None):  # noqa: ANN001
        _ = project_id, conn
        return self.profiles.get(profile_box_id)

    async def find_profile_by_name(self, project_id: str, profile_name: str, conn=None):  # noqa: ANN001
        _ = project_id, conn
        return self.profiles_by_name.get(profile_name)

    async def upsert_project_agent(self, **kwargs):  # noqa: ANN001
        self.upsert_calls.append(dict(kwargs))


class _DummyStateStore:
    def __init__(self) -> None:
        self.init_calls: list[dict] = []

    async def init_if_absent(self, **kwargs):  # noqa: ANN001
        self.init_calls.append(dict(kwargs))


class _DummyIdentityStore:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def insert_identity_edge(self, **kwargs):  # noqa: ANN001
        self.calls.append(dict(kwargs))


def _target_ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_existing",
        parent_agent_id="agent_parent",
        parent_step_id="step_parent",
    )


def _existing_roster() -> dict:
    return {
        "profile_box_id": "profile_existing",
        "worker_target": "worker.existing",
        "tags": ["stable", "owned"],
        "display_name": "Existing Agent",
        "owner_agent_id": "owner_existing",
        "metadata": {"persisted": True},
    }


def _profiles() -> dict[str, dict]:
    return {
        "profile_existing": {
            "profile_box_id": "profile_existing",
            "name": "delegatee",
            "worker_target": "worker.from_profile",
            "tags": ["profile", "defaults"],
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec_kwargs", "error_code"),
    [
        ({"owner_agent_id": "owner_new"}, "existing_agent_owner_mutation_forbidden"),
        ({"worker_target": "worker.new"}, "existing_agent_worker_target_mutation_forbidden"),
        ({"tags": ["fresh"]}, "existing_agent_tags_mutation_forbidden"),
        ({"profile_box_id": "profile_other"}, "existing_agent_profile_mutation_forbidden"),
    ],
)
async def test_ensure_agent_ready_rejects_existing_create_identity_mutation(
    spec_kwargs: dict,
    error_code: str,
) -> None:
    resource_store = _DummyResourceStore(roster=_existing_roster(), profiles=_profiles())
    state_store = _DummyStateStore()

    result = await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=None,
        target_ctx=_target_ctx(),
        spec=CreateAgentSpec(metadata={}, **spec_kwargs),
    )

    assert result.roster_updated is False
    assert result.error_code == error_code
    assert resource_store.upsert_calls == []
    assert state_store.init_calls == []


@pytest.mark.asyncio
async def test_ensure_agent_ready_preserves_existing_runtime_fields_on_metadata_patch() -> None:
    resource_store = _DummyResourceStore(roster=_existing_roster(), profiles=_profiles())
    state_store = _DummyStateStore()

    result = await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=None,
        target_ctx=_target_ctx(),
        spec=CreateAgentSpec(
            profile_box_id="profile_existing",
            metadata={"patched": True},
            display_name="Renamed Agent",
        ),
    )

    assert result.roster_updated is True
    assert result.error_code is None
    assert len(resource_store.upsert_calls) == 1
    upsert = resource_store.upsert_calls[0]
    assert upsert["profile_box_id"] == "profile_existing"
    assert upsert["worker_target"] == "worker.existing"
    assert upsert["tags"] == ["stable", "owned"]
    assert upsert["owner_agent_id"] == "owner_existing"
    assert upsert["display_name"] == "Renamed Agent"
    assert upsert["metadata"]["persisted"] is True
    assert upsert["metadata"]["patched"] is True
    assert upsert["metadata"]["profile_name"] == "delegatee"
    assert len(state_store.init_calls) == 1


@pytest.mark.asyncio
async def test_ensure_agent_ready_rejects_existing_derived_worker_target_mutation() -> None:
    resource_store = _DummyResourceStore(
        roster=_existing_roster(),
        profiles=_profiles(),
        profiles_by_name={"delegatee": {"profile_box_id": "profile_existing", "name": "delegatee"}},
    )
    state_store = _DummyStateStore()

    result = await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=None,
        target_ctx=_target_ctx(),
        spec=DerivedAgentSpec(
            derived_from="agent_parent",
            profile_name="delegatee",
            worker_target="worker.new",
            metadata={},
        ),
    )

    assert result.roster_updated is False
    assert result.error_code == "existing_agent_worker_target_mutation_forbidden"
    assert resource_store.upsert_calls == []
    assert state_store.init_calls == []


@pytest.mark.asyncio
async def test_ensure_agent_ready_reuses_target_ctx_for_identity_edge() -> None:
    resource_store = _DummyResourceStore(roster={}, profiles=_profiles())
    state_store = _DummyStateStore()
    identity_store = _DummyIdentityStore()
    target_ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_new",
        trace_id="A" * 32,
        parent_agent_id="agent_parent",
        parent_agent_turn_id="turn_parent",
        parent_step_id="step_parent",
        step_id="step_current",
        tool_call_id="call_current",
    )

    result = await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=identity_store,
        target_ctx=target_ctx,
        spec=CreateAgentSpec(
            profile_box_id="profile_existing",
            metadata={},
        ),
    )

    assert result.error_code is None
    assert result.roster_updated is True
    assert len(identity_store.calls) == 1
    edge_ctx = identity_store.calls[0]["ctx"]
    assert edge_ctx.project_id == "proj_1"
    assert edge_ctx.channel_id == "public"
    assert edge_ctx.agent_id == "agent_new"
    assert edge_ctx.parent_agent_id == "agent_parent"
    assert edge_ctx.parent_agent_turn_id == "turn_parent"
    assert edge_ctx.parent_step_id == "step_parent"
    assert edge_ctx.step_id == "step_current"
    assert edge_ctx.tool_call_id == "call_current"
    assert edge_ctx.trace_id == ("a" * 32)
