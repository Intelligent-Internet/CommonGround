from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from services.pmo.internal_handlers.provision_agent import ProvisionAgentHandler
import services.pmo.internal_handlers.provision_agent as provision_agent_module


def _ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_parent",
        agent_turn_id="turn_parent",
        turn_epoch=1,
        step_id="step_parent",
    )


class _DummyResourceStore:
    def __init__(self) -> None:
        self.profile_by_name = {
            "delegatee": {"profile_box_id": "profile_delegatee", "name": "delegatee"},
        }
        self.profile_by_box = {
            "profile_delegatee": {"profile_box_id": "profile_delegatee", "name": "delegatee"},
            "profile_clone": {"profile_box_id": "profile_clone", "name": "clone_profile"},
        }
        self.rosters = {
            "agent_source": {"profile_box_id": "profile_clone"},
            "agent_existing": {"profile_box_id": "profile_delegatee"},
        }

    async def find_profile_by_name(self, project_id: str, profile_name: str):
        _ = project_id
        return self.profile_by_name.get(profile_name)

    async def fetch_profile(self, project_id: str, profile_box_id: str, conn=None):
        _ = project_id, conn
        return self.profile_by_box.get(profile_box_id)

    async def fetch_roster(self, ctx: CGContext, conn=None):
        _ = conn
        return self.rosters.get(ctx.agent_id)


@pytest.mark.asyncio
async def test_provision_agent_create_uses_delegation_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    async def _fake_guard(*, deps, ctx, target_profile_name, conn=None):  # noqa: ANN001
        _ = deps, ctx, conn
        captured["target_profile_name"] = target_profile_name
        return {"profile_box_id": "profile_delegatee"}

    async def _fake_ensure_agent_ready(**kwargs):  # noqa: ANN001
        return SimpleNamespace(roster_updated=True, error_code=None)

    monkeypatch.setattr(provision_agent_module, "assert_can_delegate_to_profile_name", _fake_guard)
    monkeypatch.setattr(provision_agent_module, "ensure_agent_ready", _fake_ensure_agent_ready)

    handler = ProvisionAgentHandler()
    deps = SimpleNamespace(
        resource_store=_DummyResourceStore(),
        state_store=object(),
        identity_store=object(),
    )
    defer_resume, payload = await handler.handle(
        deps=deps,
        ctx=_ctx(),
        cmd=SimpleNamespace(
            tool_name="provision_agent",
            arguments={
                "action": "create",
                "agent_id": "agent_child",
                "profile_name": "delegatee",
            },
        ),
        parent_after_execution="terminate",
    )

    assert defer_resume is False
    assert payload["status"] == "accepted"
    assert captured["target_profile_name"] == "delegatee"


@pytest.mark.asyncio
async def test_provision_agent_derive_resolves_source_profile_for_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    async def _fake_guard(*, deps, ctx, target_profile_name, conn=None):  # noqa: ANN001
        _ = deps, ctx, conn
        captured["target_profile_name"] = target_profile_name
        return {"profile_box_id": "profile_clone"}

    async def _fake_ensure_agent_ready(**kwargs):  # noqa: ANN001
        return SimpleNamespace(roster_updated=True, error_code=None)

    monkeypatch.setattr(provision_agent_module, "assert_can_delegate_to_profile_name", _fake_guard)
    monkeypatch.setattr(provision_agent_module, "ensure_agent_ready", _fake_ensure_agent_ready)

    handler = ProvisionAgentHandler()
    deps = SimpleNamespace(
        resource_store=_DummyResourceStore(),
        state_store=object(),
        identity_store=object(),
    )
    defer_resume, payload = await handler.handle(
        deps=deps,
        ctx=_ctx(),
        cmd=SimpleNamespace(
            tool_name="provision_agent",
            arguments={
                "action": "derive",
                "agent_id": "agent_child",
                "derived_from": "agent_source",
            },
        ),
        parent_after_execution="terminate",
    )

    assert defer_resume is False
    assert payload["status"] == "accepted"
    assert captured["target_profile_name"] == "clone_profile"


@pytest.mark.asyncio
async def test_provision_agent_rejects_when_delegation_guard_denies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_guard(*, deps, ctx, target_profile_name, conn=None):  # noqa: ANN001
        _ = deps, ctx, target_profile_name, conn
        raise RuntimeError("denied")

    monkeypatch.setattr(provision_agent_module, "assert_can_delegate_to_profile_name", _fake_guard)

    handler = ProvisionAgentHandler()
    deps = SimpleNamespace(
        resource_store=_DummyResourceStore(),
        state_store=object(),
        identity_store=object(),
    )
    defer_resume, payload = await handler.handle(
        deps=deps,
        ctx=_ctx(),
        cmd=SimpleNamespace(
            tool_name="provision_agent",
            arguments={
                "action": "create",
                "agent_id": "agent_child",
                "profile_name": "delegatee",
            },
        ),
        parent_after_execution="terminate",
    )

    assert defer_resume is False
    assert payload == {"status": "rejected", "error_code": "delegation_denied"}
