from __future__ import annotations

from datetime import UTC, datetime, timedelta
import threading
from types import SimpleNamespace

import httpx
import pytest

from CommonGround.agent_client import SuspendTurnAction, agent_auth_headers
from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT
from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.contracts import (
    AGENT_CREDENTIAL_STATUS_ACTIVE,
    AGENT_CREDENTIAL_STATUS_REVOKED,
    AgentRef,
    ClaimToken,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TurnOutcome,
    TurnRef,
    TurnState,
)
from CommonGround.provision_launch import PROVISION_LAUNCH_STARTED_KIND_V1, PROVISION_LAUNCH_STARTED_ROLE
from Integrations.nanobot.adapter.context_mapping import (
    EXECUTION_PLAN_PHASE,
    WORK_ORDER_KIND_V1,
    build_child_work_order,
    render_leaf_prompt,
)
from Integrations.nanobot.adapter.leaf_handler import LeafTurnHandler
from Integrations.nanobot.adapter.self_root_frontside import (
    FORWARDED_USER_REQUEST_KIND_V1,
    USER_REQUEST_KIND_V1,
    SelfRootFrontsideHandler,
    SelfRootIngress,
    build_user_request_payload,
    validate_user_request_payload,
)
from Integrations.nanobot.substrate.process_substrate import ProcessOpsSubstrate
from Integrations.nanobot.adapter.provision_handler import ProvisionAgentSpawnHandler, ProvisionLaunchRequest, ProvisionLaunchResult
from Integrations.nanobot.provision_lifecycle import (
    DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
    PROVISION_LIFECYCLE_METADATA_KEY,
    build_ephemeral_lifecycle_metadata,
    cleanup_ephemeral_provision_agents,
)
from Integrations.nanobot.provision_role_policy import published_available_roles
from Integrations.nanobot.runtime.presence import PresenceHeartbeater
from Integrations.nanobot.runtime.leaf_worker_runner import run_leaf_worker_forever
from Integrations.nanobot.runtime.provisioner_runner import _resume_ready_provision_turns, run_provisioner_worker_forever
from Integrations.nanobot.adapter.registration_spec import AgentRegistrationSpec, REGISTRATION_SPEC_ENV, encode_registration_spec
from Integrations.nanobot.adapter.supervisor_handler import SupervisorTurnHandler
from Integrations.nanobot.runtime.self_root_frontside_main import load_self_root_frontside_env, run_from_env as run_self_root_frontside_from_env
from Integrations.nanobot.runtime.self_root_frontside_runner import run_self_root_frontside_worker_forever
from Integrations.nanobot.runtime.supervisor_runner import _resume_ready_parent_turns, run_supervisor_worker_forever
from Integrations.nanobot.turn_offer_metadata import conversation_turn_offer, provision_turn_offer

LEAF_ROLE = "nanobot.leaf.conversation.v1"
TEST_AGENT_CREDENTIAL_TOKEN = "test-token"


def _agent_headers(agent: AgentRef) -> dict[str, str]:
    return agent_auth_headers(agent, TEST_AGENT_CREDENTIAL_TOKEN)


def _semantic_item(role, payload):
    return SimpleNamespace(
        record=SimpleNamespace(record_role=role),
        content=SimpleNamespace(payload=lambda: payload),
    )


def _context(
    payload,
    *,
    turn_id: str = "T-1",
    agent_id: str = "nanobot_b",
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
    parent_turn_id: str | None = None,
    progress_payloads=(),
):
    semantic_items = [_semantic_item("bootstrap", payload)]
    semantic_items.extend(_semantic_item("progress", progress_payload) for progress_payload in progress_payloads)
    return SimpleNamespace(
        turn=SimpleNamespace(
            turn=SimpleNamespace(turn_id=turn_id),
            target_agent=SimpleNamespace(agent_id=agent_id),
            turn_kind=turn_kind,
            cause=SimpleNamespace(kind="turn" if parent_turn_id is not None else "external_request", id=parent_turn_id or "request-1"),
        ),
        semantic_items=tuple(semantic_items),
    )


def _provision_context_for_task(task_id: str, leaf_agent: AgentRef, *, parent_turn_id: str = "T-1"):
    return SimpleNamespace(
        semantic_items=(
            _semantic_item(
                "bootstrap",
                {
                    "parent_turn_id": parent_turn_id,
                    "lifecycle": {"source_turn_id": parent_turn_id},
                    "work_order": {"task_id": task_id, "parent_turn_id": parent_turn_id},
                },
            ),
            _semantic_item(
                "deliverable",
                {
                    "task_id": task_id,
                    "new_agent_ref": {"project_id": leaf_agent.project_id, "agent_id": leaf_agent.agent_id},
                },
            ),
        )
    )


def _child_context_for_task(task_id: str, payload, *, parent_turn_id: str = "T-1"):
    return SimpleNamespace(
        semantic_items=(
            _semantic_item(
                "bootstrap",
                {
                    "kind": WORK_ORDER_KIND_V1,
                    "task_id": task_id,
                    "provenance": {"task_id": task_id, "parent_turn_id": parent_turn_id},
                },
            ),
            _semantic_item("deliverable", payload),
        )
    )


def _feed_page(items=(), *, next_after_ledger_seq: int = 0):
    return SimpleNamespace(items=tuple(items), next_after_ledger_seq=next_after_ledger_seq)


def _provisioner_snapshot(agent):
    return SimpleNamespace(
        agent=agent,
        capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
        grants=(AGENT_CREDENTIAL_ISSUE_ANY_GRANT,),
        accepts_work=True,
    )


def _leaf_snapshot(agent, *, capabilities=(TURN_KIND_CONVERSATION_V1,), grants=(), accepts_work=True, role=LEAF_ROLE):
    return SimpleNamespace(
        agent=agent,
        role=role,
        capabilities=capabilities,
        grants=grants,
        accepts_work=accepts_work,
    )


def _assert_leaf_birth_request(
    *,
    project_id: str,
    spec: AgentBirthSpec,
    provenance: AgentRegistrationProvenance,
    claim: ClaimToken,
    lifecycle_source_turn_id: str | None = None,
) -> None:
    assert project_id == claim.project_id
    assert spec.agent_id == f"nanobot_leaf_{claim.turn_id}"
    assert spec.role == LEAF_ROLE
    assert spec.capabilities == (TURN_KIND_CONVERSATION_V1,)
    assert spec.grants == ()
    assert spec.accepts_work is True
    assert spec.public_metadata == {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=claim.agent_ref(),
            source_turn_id=lifecycle_source_turn_id or claim.turn_id,
            ttl_seconds=DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
        )
    }
    assert provenance == AgentRegistrationProvenance(kind="nanobot.provision_turn.v1", external_ref=claim.turn_id)


def test_render_leaf_prompt_for_child_turn_includes_parent_metadata():
    prompt = render_leaf_prompt(_context({"task": "write summary"}, parent_turn_id="parent-1"))
    assert "parent_turn_id: parent-1" in prompt
    assert '"task": "write summary"' in prompt


def test_build_child_work_order_from_child_task_string():
    payload = build_child_work_order(
        root_payload={"task": "supervise child", "child_task": "Write the child result."},
        parent_turn_id="parent-1",
    )

    assert payload["kind"] == WORK_ORDER_KIND_V1
    assert payload["objective"] == "Write the child result."
    assert payload["input"] == {}
    assert payload["expected_output"] == {"type": "text", "style": "concise"}
    assert payload["delegation_policy"] == {"may_delegate": False}
    assert payload["provenance"] == {"parent_turn_id": "parent-1"}
    assert payload["context"]["parent_request"]["task"] == "supervise child"


def test_build_child_work_order_from_child_task_object():
    payload = build_child_work_order(
        root_payload={
            "child_task": {
                "objective": "Summarize the child input.",
                "input": {"topic": "typed payload"},
                "expected_output": {"type": "json"},
                "delegation_policy": {"may_delegate": True},
            }
        },
        parent_turn_id="parent-1",
        target_agent=AgentRef(project_id="demo", agent_id="nanobot_leaf_001"),
    )

    assert payload["objective"] == "Summarize the child input."
    assert payload["input"] == {"topic": "typed payload"}
    assert payload["expected_output"] == {"type": "json"}
    assert payload["delegation_policy"] == {"may_delegate": True}
    assert payload["provenance"] == {
        "parent_turn_id": "parent-1",
        "target_agent_ref": {"project_id": "demo", "agent_id": "nanobot_leaf_001"},
    }


def test_build_child_work_order_fallback_does_not_use_parent_task_as_objective():
    payload = build_child_work_order(
        root_payload={"task": "Delegate a child task to nanobot_b, then finish after the child result is available."},
        parent_turn_id="parent-1",
    )

    assert payload["kind"] == WORK_ORDER_KIND_V1
    assert payload["objective"] == "Return a concise confirmation that this dynamically provisioned leaf completed the delegated child turn."
    assert payload["objective"] != payload["context"]["parent_request"]["task"]


def test_render_leaf_prompt_for_work_order_separates_objective_from_parent_context():
    work_order = build_child_work_order(
        root_payload={"task": "Delegate a child task to nanobot_b, then finish after the child result is available."},
        parent_turn_id="parent-1",
    )
    prompt = render_leaf_prompt(_context(work_order, parent_turn_id="parent-1"))

    assert "Objective:\nReturn a concise confirmation" in prompt
    assert "Do not create or delegate new subtasks in this turn." in prompt
    assert "Parent request context (not the objective):" in prompt
    assert prompt.index("Objective:\nReturn a concise confirmation") < prompt.index("Parent request context (not the objective):")


def test_leaf_handler_finishes_with_nanobot_content():
    captured = {}

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            captured["content"] = content
            captured["kwargs"] = kwargs
            return SimpleNamespace(content="leaf completed")

    handler = LeafTurnHandler(loop=FakeLoop())
    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_b",
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = handler.handle_turn(_context("do the work"), SimpleNamespace(), claim)
    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["content"] == "leaf completed"
    assert captured["kwargs"]["session_key"] == "cg:nanobot_b:T-1"


def test_leaf_handler_accepts_tool_hint_progress_callback():
    progress_calls = []

    class FakeClient:
        def append_record(self, claim, payload, *, role):
            progress_calls.append((claim.turn_id, payload, role))

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            await kwargs["on_progress"]("thinking")
            await kwargs["on_progress"]("tool: read_file(...)", tool_hint=True)
            return SimpleNamespace(content="leaf completed")

    handler = LeafTurnHandler(loop=FakeLoop(), emit_progress=True)
    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_b",
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = handler.handle_turn(_context("do the work"), FakeClient(), claim)
    assert action.outcome == TurnOutcome.SUCCEEDED
    assert progress_calls == [
        ("T-1", {"progress": "thinking"}, "progress"),
        ("T-1", {"progress": "tool: read_file(...)", "tool_hint": True}, "progress"),
    ]


def test_leaf_handler_rejects_unexpected_turn_kind():
    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            raise AssertionError("leaf handler should not run NanoBot for unexpected turn kind")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_b",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = LeafTurnHandler(loop=FakeLoop())
    action = handler.handle_turn(_context("do the work", turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1), SimpleNamespace(), claim)
    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "unexpected_turn_kind"


def test_self_root_payload_build_and_validate():
    payload = build_user_request_payload(
        target_agent_id="nanobot_target",
        message="hello target",
        external_thread={"provider": "slack", "thread_id": "C1:123"},
    )

    assert payload == {
        "kind": USER_REQUEST_KIND_V1,
        "target_agent_id": "nanobot_target",
        "message": "hello target",
        "external_thread": {"provider": "slack", "thread_id": "C1:123"},
    }
    assert validate_user_request_payload(payload, project_id="demo") is payload


def test_self_root_payload_validation_rejects_missing_target_agent_id():
    with pytest.raises(ValueError, match="target_agent_id"):
        validate_user_request_payload({"kind": USER_REQUEST_KIND_V1, "message": "hello"}, project_id="demo")


def test_self_root_payload_validation_rejects_cross_project_target():
    payload = build_user_request_payload(target_agent_id="nanobot_target", message="hello")
    payload["target_project_id"] = "other"

    with pytest.raises(ValueError, match="target_project_id"):
        validate_user_request_payload(payload, project_id="demo")


def test_self_root_ingress_dispatches_root_to_self_agent():
    dispatch_calls = []
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")

    class FakeClient:
        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return TurnRef(project_id="demo", turn_id="T-root")

    ingress = SelfRootIngress(client=FakeClient(), self_agent=self_agent)
    binding = ingress.dispatch_user_message(
        target_agent_id="nanobot_target",
        message={"text": "hello"},
        external_thread={"provider": "web", "thread_id": "thread-1"},
        request_id="external-1",
    )

    assert binding.root_turn == TurnRef(project_id="demo", turn_id="T-root")
    assert binding.as_persistable()["external_thread"] == {"provider": "web", "thread_id": "thread-1"}
    assert dispatch_calls[0]["requested_by"] == self_agent
    assert dispatch_calls[0]["target_agent"] == self_agent
    assert dispatch_calls[0]["dispatch_key"] == "external-1"
    assert dispatch_calls[0]["authority"].request_id == "external-1"
    assert dispatch_calls[0]["input_payload"]["target_agent_id"] == "nanobot_target"


def test_self_root_handler_dispatches_child_then_suspends():
    dispatch_calls = []
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")
    target_agent = AgentRef(project_id="demo", agent_id="nanobot_target")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def get_agent(self, agent):
            assert agent == target_agent
            return _leaf_snapshot(agent)

        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return TurnRef(project_id="demo", turn_id="T-child")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(
                target_agent_id=target_agent.agent_id,
                message="hello target",
                external_thread={"provider": "slack", "thread_id": "C1:123"},
            ),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_child"
    assert dispatch_calls[0]["requested_by"] == self_agent
    assert dispatch_calls[0]["target_agent"] == target_agent
    assert dispatch_calls[0]["authority"].parent_claim == claim
    assert dispatch_calls[0]["dispatch_key"] == "T-root:method2:child:1"
    assert dispatch_calls[0]["input_payload"] == {
        "kind": FORWARDED_USER_REQUEST_KIND_V1,
        "message": "hello target",
        "external_thread": {"provider": "slack", "thread_id": "C1:123"},
        "root_turn": {"project_id": "demo", "turn_id": "T-root"},
        "source_agent": {"project_id": "demo", "agent_id": "frontside_self"},
        "target_agent": {"project_id": "demo", "agent_id": "nanobot_target"},
    }


def test_self_root_handler_keeps_suspended_while_child_pending():
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")
    target_agent = AgentRef(project_id="demo", agent_id="nanobot_target")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page((SimpleNamespace(event_type="turn.spawned", subject_id="T-child"),))

        def get_agent(self, agent):
            return _leaf_snapshot(agent)

        def get_turn(self, turn):
            assert turn == TurnRef(project_id="demo", turn_id="T-child")
            return SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=None, final_payload=None)

        def dispatch(self, **kwargs):
            raise AssertionError("handler should not dispatch a duplicate child")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(target_agent_id=target_agent.agent_id, message="hello"),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_child"
    assert "still active" in action.note


def test_self_root_handler_finishes_after_child_succeeded():
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")
    target_agent = AgentRef(project_id="demo", agent_id="nanobot_target")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-child"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-child"),
                )
            )

        def get_agent(self, agent):
            return _leaf_snapshot(agent)

        def get_turn(self, turn):
            assert turn == TurnRef(project_id="demo", turn_id="T-child")
            return SimpleNamespace(
                turn_kind=TURN_KIND_CONVERSATION_V1,
                outcome=TurnOutcome.SUCCEEDED,
                final_payload={"content": "child final"},
            )

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(
                target_agent_id=target_agent.agent_id,
                message="hello",
                external_thread={"provider": "web", "thread_id": "thread-1"},
            ),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload == {
        "root": {"project_id": "demo", "turn_id": "T-root", "agent_id": "frontside_self"},
        "child": {"project_id": "demo", "turn_id": "T-child", "outcome": "succeeded"},
        "target": {"project_id": "demo", "agent_id": "nanobot_target"},
        "external_thread": {"provider": "web", "thread_id": "thread-1"},
        "child_result": {"content": "child final"},
    }


def test_self_root_handler_finishes_existing_child_even_if_target_drained_after_dispatch():
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")
    target_agent = AgentRef(project_id="demo", agent_id="nanobot_target")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-child"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-child"),
                )
            )

        def get_agent(self, agent):
            raise AssertionError("target availability is only required before a new child dispatch")

        def get_turn(self, turn):
            assert turn == TurnRef(project_id="demo", turn_id="T-child")
            return SimpleNamespace(
                turn_kind=TURN_KIND_CONVERSATION_V1,
                outcome=TurnOutcome.SUCCEEDED,
                final_payload={"content": "child final"},
            )

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(target_agent_id=target_agent.agent_id, message="hello"),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["target"] == {"project_id": "demo", "agent_id": "nanobot_target"}


def test_self_root_handler_fails_when_target_agent_missing():
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def get_agent(self, agent):
            return None

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(target_agent_id="missing_agent", message="hello"),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload == {"error": "target_agent_not_found", "target_agent_id": "missing_agent"}


def test_self_root_handler_fails_when_target_agent_not_accepting_work():
    self_agent = AgentRef(project_id="demo", agent_id="frontside_self")
    target_agent = AgentRef(project_id="demo", agent_id="nanobot_target")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def get_agent(self, agent):
            assert agent == target_agent
            return SimpleNamespace(agent=agent, enabled=True, accepts_work=False)

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-root",
        agent_id=self_agent.agent_id,
        token="token",
        expires_at=SimpleNamespace(),
    )
    action = SelfRootFrontsideHandler().handle_turn(
        _context(
            build_user_request_payload(target_agent_id=target_agent.agent_id, message="hello"),
            turn_id=claim.turn_id,
            agent_id=self_agent.agent_id,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload == {
        "error": "target_agent_unavailable",
        "target_agent_id": target_agent.agent_id,
        "enabled": True,
        "accepts_work": False,
    }


def test_supervisor_directly_answers_simple_root_turn_without_leaf_provision():
    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            raise AssertionError("direct root turn should not inspect child feed")

        def dispatch(self, **kwargs):
            raise AssertionError("direct root turn should not dispatch children")

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            assert "directly completing" in content
            assert "who am I?" in content
            return SimpleNamespace(content="You are Demo User.")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(
        loop=FakeLoop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha"),
    )
    action = handler.handle_turn(
        _context(
            {
                "task": "who am I?",
                "conversation_context": [{"role": "user", "content": "my name is Demo User."}],
            }
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload == {
        "content": "You are Demo User.",
        "agent_id": "nanobot_a",
        "turn_id": "T-1",
        "execution_mode": "direct",
    }


def test_supervisor_dispatches_provision_child_then_suspends():
    dispatch_calls = []
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return SimpleNamespace(turn_id="T-2")

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            raise AssertionError("supervisor should not run model before leaf child finishes")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(
        loop=FakeLoop(),
        provisioner_agent=provisioner,
    )
    action = handler.handle_turn(_context({"task": "delegate", "child_task": "Return child confirmation."}), FakeClient(), claim)

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_leaf_provision"
    assert dispatch_calls[0]["target_agent"] == provisioner
    assert dispatch_calls[0]["turn_kind"] == TURN_KIND_PROVISION_AGENT_SPAWN_V1
    assert dispatch_calls[0]["dispatch_key"] == "T-1:provision:leaf:task_1"
    assert dispatch_calls[0]["input_payload"]["agent"] == {"role": "nanobot.leaf.conversation.v1"}
    assert dispatch_calls[0]["input_payload"]["lifecycle"] == {"source_turn_id": "T-1"}
    assert dispatch_calls[0]["input_payload"]["work_order"]["task_id"] == "task_1"


def test_supervisor_dispatches_conversation_child_after_provision_result():
    dispatch_calls = []
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_agent = AgentRef(project_id="demo", agent_id="nanobot_leaf_001")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page((SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),))

        def get_turn(self, turn):
            assert turn.turn_id == "T-2"
            return SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED)

        def fetch_context(self, turn):
            assert turn.turn_id == "T-2"
            return _provision_context_for_task("task_1", leaf_agent)

        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return SimpleNamespace(turn_id="T-3")

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            raise AssertionError("supervisor should not run model before conversation child finishes")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(
        _context({"task": "delegate", "child_task": {"objective": "Return child confirmation."}}),
        FakeClient(),
        claim,
    )

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_child"
    assert dispatch_calls[0]["target_agent"] == leaf_agent
    assert dispatch_calls[0]["turn_kind"] == TURN_KIND_CONVERSATION_V1
    assert dispatch_calls[0]["dispatch_key"] == "T-1:leaf:conversation:task_1"
    assert dispatch_calls[0]["input_payload"]["kind"] == WORK_ORDER_KIND_V1
    assert dispatch_calls[0]["input_payload"]["objective"] == "Return child confirmation."
    assert dispatch_calls[0]["input_payload"]["provenance"] == {
        "task_id": "task_1",
        "parent_turn_id": "T-1",
        "target_agent_ref": {"project_id": "demo", "agent_id": "nanobot_leaf_001"},
    }


def test_supervisor_finishes_after_dynamic_leaf_child_result():
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_agent = AgentRef(project_id="demo", agent_id="nanobot_leaf_001")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                )
            )

        def get_turn(self, turn):
            snapshots = {
                "T-2": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
                "T-3": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
            }
            return snapshots[turn.turn_id]

        def fetch_context(self, turn):
            if turn.turn_id == "T-2":
                return _provision_context_for_task("task_1", leaf_agent)
            assert turn.turn_id == "T-3"
            return _child_context_for_task("task_1", {"content": "dynamic child result"})

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            assert "dynamic child result" in content
            return SimpleNamespace(content="dynamic supervisor final")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(_context({"task": "delegate", "child_task": "Return child confirmation."}), FakeClient(), claim)

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["provision_turn_id"] == "T-2"
    assert action.final_payload["leaf_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_001"}
    assert action.final_payload["child_turn_id"] == "T-3"
    assert action.final_payload["content"] == "dynamic supervisor final"


def test_supervisor_plans_auto_orchestration_and_dispatches_multiple_provisions():
    append_calls = []
    dispatch_calls = []
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def append_record(self, claim, payload, *, role):
            append_calls.append((role, payload))

        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return SimpleNamespace(turn_id=f"T-{len(dispatch_calls) + 1}")

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            assert "Use between 1 and 4 child tasks" in content
            return SimpleNamespace(
                content=(
                    '{"child_tasks":['
                    '{"task_id":"concept","objective":"Research concept packaging.","input":{"topic":"concept"}},'
                    '{"task_id":"platform","objective":"Compare platforms.","input":{"topic":"platform"}}'
                    "]}"
                )
            )

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(
        _context({"task": "research", "orchestration": {"mode": "auto", "max_child_tasks": 4}}),
        FakeClient(),
        claim,
    )

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_leaf_provision"
    assert [call["dispatch_key"] for call in dispatch_calls] == [
        "T-1:provision:leaf:concept",
        "T-1:provision:leaf:platform",
    ]
    assert dispatch_calls[0]["input_payload"]["work_order"]["task_id"] == "concept"
    assert dispatch_calls[1]["input_payload"]["work_order"]["task_id"] == "platform"
    assert append_calls[0][0] == "progress"
    assert append_calls[0][1]["phase"] == EXECUTION_PLAN_PHASE
    assert [item["task_id"] for item in append_calls[0][1]["work_orders"]] == ["concept", "platform"]


def test_supervisor_dispatches_multiple_children_after_multiple_provisions():
    dispatch_calls = []
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_agents = (
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"),
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-3"),
    )
    work_orders = (
        {"kind": WORK_ORDER_KIND_V1, "task_id": "concept", "objective": "Research concept packaging."},
        {"kind": WORK_ORDER_KIND_V1, "task_id": "platform", "objective": "Compare platforms."},
    )

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                )
            )

        def get_turn(self, turn):
            return SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED)

        def fetch_context(self, turn):
            index = 0 if turn.turn_id == "T-2" else 1
            leaf_agent = leaf_agents[index]
            task_id = "concept" if turn.turn_id == "T-2" else "platform"
            return _provision_context_for_task(task_id, leaf_agent)

        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return SimpleNamespace(turn_id=f"T-{len(dispatch_calls) + 4}")

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            raise AssertionError("supervisor should not synthesize before children finish")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(
        _context(
            {"task": "research"},
            progress_payloads=({"phase": EXECUTION_PLAN_PHASE, "work_orders": list(work_orders)},),
        ),
        FakeClient(),
        claim,
    )

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_child"
    assert [call["target_agent"] for call in dispatch_calls] == list(leaf_agents)
    assert [call["dispatch_key"] for call in dispatch_calls] == [
        "T-1:leaf:conversation:concept",
        "T-1:leaf:conversation:platform",
    ]
    assert [call["input_payload"]["task_id"] for call in dispatch_calls] == ["concept", "platform"]
    assert dispatch_calls[1]["input_payload"]["provenance"]["target_agent_ref"] == {
        "project_id": "demo",
        "agent_id": "nanobot_leaf_T-3",
    }


def test_supervisor_finishes_after_multiple_child_results():
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_agents = (
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"),
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-3"),
    )
    work_orders = (
        {"kind": WORK_ORDER_KIND_V1, "task_id": "concept", "objective": "Research concept packaging."},
        {"kind": WORK_ORDER_KIND_V1, "task_id": "platform", "objective": "Compare platforms."},
    )

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-4"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-4"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-5"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-5"),
                )
            )

        def get_turn(self, turn):
            kinds = {
                "T-2": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                "T-3": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                "T-4": TURN_KIND_CONVERSATION_V1,
                "T-5": TURN_KIND_CONVERSATION_V1,
            }
            return SimpleNamespace(turn_kind=kinds[turn.turn_id], outcome=TurnOutcome.SUCCEEDED)

        def fetch_context(self, turn):
            if turn.turn_id in {"T-2", "T-3"}:
                leaf_agent = leaf_agents[0 if turn.turn_id == "T-2" else 1]
                task_id = "concept" if turn.turn_id == "T-2" else "platform"
                return _provision_context_for_task(task_id, leaf_agent)
            payload = {"content": "concept result"} if turn.turn_id == "T-4" else {"content": "platform result"}
            task_id = "concept" if turn.turn_id == "T-4" else "platform"
            return _child_context_for_task(task_id, payload)

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            assert "concept result" in content
            assert "platform result" in content
            return SimpleNamespace(content="multi-agent synthesis")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(
        _context(
            {"task": "research"},
            progress_payloads=({"phase": EXECUTION_PLAN_PHASE, "work_orders": list(work_orders)},),
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["content"] == "multi-agent synthesis"
    assert action.final_payload["provision_turn_ids"] == ["T-2", "T-3"]
    assert action.final_payload["child_turn_ids"] == ["T-4", "T-5"]
    assert [item["task_id"] for item in action.final_payload["child_results"]] == ["concept", "platform"]


def test_supervisor_partially_synthesizes_after_child_claim_timeout():
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_agents = (
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"),
        AgentRef(project_id="demo", agent_id="nanobot_leaf_T-3"),
    )
    work_orders = (
        {"kind": WORK_ORDER_KIND_V1, "task_id": "concept", "objective": "Research concept packaging."},
        {"kind": WORK_ORDER_KIND_V1, "task_id": "platform", "objective": "Compare platforms."},
    )
    expired_at = datetime.now(UTC) - timedelta(seconds=5)
    stop_requests = []

    class FakeClient:
        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-4"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-4"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-5"),
                )
            )

        def get_turn(self, turn):
            snapshots = {
                "T-2": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
                "T-3": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
                "T-4": SimpleNamespace(
                    turn_kind=TURN_KIND_CONVERSATION_V1,
                    outcome=TurnOutcome.SUCCEEDED,
                    claim_expires_at=None,
                ),
                "T-5": SimpleNamespace(
                    turn_kind=TURN_KIND_CONVERSATION_V1,
                    outcome=None,
                    claim_expires_at=expired_at,
                ),
            }
            return snapshots[turn.turn_id]

        def fetch_context(self, turn):
            if turn.turn_id in {"T-2", "T-3"}:
                leaf_agent = leaf_agents[0 if turn.turn_id == "T-2" else 1]
                task_id = "concept" if turn.turn_id == "T-2" else "platform"
                return _provision_context_for_task(task_id, leaf_agent)
            if turn.turn_id == "T-4":
                return _child_context_for_task("concept", {"content": "concept result"})
            assert turn.turn_id == "T-5"
            return _child_context_for_task("platform", {"content": "platform result"})

        def request_stop_turn(self, turn, *, requested_by, reason="operator_stop", note=None, meta=None):
            stop_requests.append(
                {
                    "turn": turn,
                    "requested_by": requested_by,
                    "reason": reason,
                    "note": note,
                    "meta": meta,
                }
            )
            return turn

    class FakeLoop:
        async def process_direct(self, content: str, **kwargs):
            assert "concept result" in content
            assert "Timed out child turns" in content
            assert "Compare platforms." in content
            return SimpleNamespace(content="partial synthesis")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-1",
        agent_id="nanobot_a",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = SupervisorTurnHandler(loop=FakeLoop(), provisioner_agent=provisioner)
    action = handler.handle_turn(
        _context(
            {"task": "research"},
            progress_payloads=({"phase": EXECUTION_PLAN_PHASE, "work_orders": list(work_orders)},),
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["content"] == "partial synthesis"
    assert action.final_payload["partial"] is True
    assert action.final_payload["timed_out_child_turn_ids"] == ["T-5"]
    assert action.final_payload["stop_requested_child_turn_ids"] == ["T-5"]
    assert action.final_payload["child_stop_errors"] == []
    assert [item["task_id"] for item in action.final_payload["child_results"]] == ["concept"]
    assert action.final_payload["timed_out_child_results"][0]["task_id"] == "platform"
    assert len(stop_requests) == 1
    stop_request = stop_requests[0]
    assert stop_request["turn"] == TurnRef(project_id="demo", turn_id="T-5")
    assert stop_request["requested_by"] == AgentRef(project_id="demo", agent_id="nanobot_a")
    assert stop_request["reason"] == "parent_partial_child_timeout"
    assert stop_request["note"] == "parent T-1 completed partial synthesis without this timed-out child"
    assert stop_request["meta"].reason == "parent_partial_child_timeout"
    assert stop_request["meta"].annotations["nanobot_orchestration"]["parent_turn_id"] == "T-1"


def test_supervisor_runner_recovers_parent_after_restart_and_latest_child_finished():
    resume_calls = []
    parent_turn = TurnRef(project_id="demo", turn_id="T-1")
    supervisor = AgentRef(project_id="demo", agent_id="nanobot_a")

    class FakeClient:
        def __init__(self, feed_items):
            self._feed_items = feed_items

        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(
                        project_id=parent_turn.project_id,
                        event_type="turn.suspended",
                        subject_kind="turn",
                        subject_id=parent_turn.turn_id,
                    ),
                )
            )

        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.SUSPENDED)

        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(self._feed_items)

        def resume_turn(self, agent, turn, *, note):
            resume_calls.append((agent, turn, note))

    watched_turns = set()
    _resume_ready_parent_turns(
        FakeClient(
            (
                SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
            )
        ),
        agent=supervisor,
        watched_turns=watched_turns,
    )

    assert resume_calls == []
    assert watched_turns == {parent_turn}

    _resume_ready_parent_turns(
        FakeClient(
            (
                SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
            )
        ),
        agent=supervisor,
        watched_turns=watched_turns,
    )

    assert resume_calls == [(supervisor, parent_turn, "children_finished")]
    assert watched_turns == set()


def test_supervisor_runner_resumes_parent_when_conversation_child_claim_expired():
    resume_calls = []
    parent_turn = TurnRef(project_id="demo", turn_id="T-1")
    supervisor = AgentRef(project_id="demo", agent_id="nanobot_a")
    expired_at = datetime.now(UTC) - timedelta(seconds=5)

    class FakeClient:
        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(
                        project_id=parent_turn.project_id,
                        event_type="turn.suspended",
                        subject_kind="turn",
                        subject_id=parent_turn.turn_id,
                    ),
                )
            )

        def get_turn(self, turn):
            snapshots = {
                "T-1": SimpleNamespace(state=TurnState.SUSPENDED),
                "T-2": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
                "T-3": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
                "T-4": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=None, claim_expires_at=expired_at),
            }
            return snapshots[turn.turn_id]

        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                    SimpleNamespace(event_type="turn.spawned", subject_id="T-4"),
                )
            )

        def resume_turn(self, agent, turn, *, note):
            resume_calls.append((agent, turn, note))

    watched_turns = set()
    _resume_ready_parent_turns(FakeClient(), agent=supervisor, watched_turns=watched_turns)

    assert resume_calls == [(supervisor, parent_turn, "children_finished_or_expired")]
    assert watched_turns == set()


def test_supervisor_runner_waits_for_all_spawned_children_before_resume():
    resume_calls = []
    parent_turn = TurnRef(project_id="demo", turn_id="T-1")
    supervisor = AgentRef(project_id="demo", agent_id="nanobot_a")

    class FakeClient:
        def __init__(self, feed_items):
            self._feed_items = feed_items

        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(
                        project_id=parent_turn.project_id,
                        event_type="turn.suspended",
                        subject_kind="turn",
                        subject_id=parent_turn.turn_id,
                    ),
                )
            )

        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.SUSPENDED)

        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            return _feed_page(self._feed_items)

        def resume_turn(self, agent, turn, *, note):
            resume_calls.append((agent, turn, note))

    watched_turns = set()
    _resume_ready_parent_turns(
        FakeClient(
            (
                SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
            )
        ),
        agent=supervisor,
        watched_turns=watched_turns,
    )

    assert resume_calls == []
    assert watched_turns == {parent_turn}

    _resume_ready_parent_turns(
        FakeClient(
            (
                SimpleNamespace(event_type="turn.spawned", subject_id="T-2"),
                SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-3"),
                SimpleNamespace(event_type="turn.finished", subject_id="T-2"),
            )
        ),
        agent=supervisor,
        watched_turns=watched_turns,
    )

    assert resume_calls == [(supervisor, parent_turn, "children_finished")]
    assert watched_turns == set()


def test_supervisor_runner_paginates_feed_recovery_after_restart():
    resume_calls = []
    parent_turn = TurnRef(project_id="demo", turn_id="T-1")
    supervisor = AgentRef(project_id="demo", agent_id="nanobot_a")

    class FakeClient:
        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            if after_ledger_seq == 0:
                return _feed_page(
                    (SimpleNamespace(project_id="demo", event_type="turn.progress_appended", subject_kind="turn", subject_id="old-turn"),),
                    next_after_ledger_seq=1,
                )
            if after_ledger_seq == 1:
                return _feed_page(
                    (
                        SimpleNamespace(
                            project_id=parent_turn.project_id,
                            event_type="turn.suspended",
                            subject_kind="turn",
                            subject_id=parent_turn.turn_id,
                        ),
                    ),
                    next_after_ledger_seq=2,
                )
            return _feed_page(next_after_ledger_seq=after_ledger_seq)

        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.SUSPENDED)

        def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
            if after_ledger_seq == 0:
                return _feed_page(
                    (SimpleNamespace(event_type="turn.spawned", subject_id="T-3"),),
                    next_after_ledger_seq=10,
                )
            if after_ledger_seq == 10:
                return _feed_page(
                    (SimpleNamespace(event_type="turn.finished", subject_id="T-3"),),
                    next_after_ledger_seq=11,
                )
            return _feed_page(next_after_ledger_seq=after_ledger_seq)

        def resume_turn(self, agent, turn, *, note):
            resume_calls.append((agent, turn, note))

    watched_turns = set()
    next_after = _resume_ready_parent_turns(FakeClient(), agent=supervisor, watched_turns=watched_turns)

    assert next_after == 2
    assert resume_calls == [(supervisor, parent_turn, "children_finished")]
    assert watched_turns == set()


def test_provision_handler_launches_leaf_after_service_authorized_birth():
    append_calls = []

    class FakeClient:
        def __init__(self):
            self.leaf_get_agent_calls = 0
            self.issue_calls = []

        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            assert agent.project_id == "demo"
            assert agent.agent_id == "nanobot_leaf_T-2"
            self.leaf_get_agent_calls += 1
            if self.leaf_get_agent_calls == 1:
                return None
            return _leaf_snapshot(agent)

        def register_agent_by_service(
            self,
            *,
            project_id,
            spec,
            provenance,
        ):
            _assert_leaf_birth_request(project_id=project_id, spec=spec, provenance=provenance, claim=claim)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id))

        def issue_agent_credential(
            self,
            agent,
            *,
            expires_at=None,
            provenance_kind=None,
            provenance_ref=None,
            provenance_payload_hash=None,
        ):
            self.issue_calls.append(
                {
                    "agent": agent,
                    "expires_at": expires_at,
                    "provenance_kind": provenance_kind,
                    "provenance_ref": provenance_ref,
                    "provenance_payload_hash": provenance_payload_hash,
                }
            )
            return {
                "token": "cgac_leaf-cred.secret",
                "credential": {
                    "agent_id": agent.agent_id,
                },
            }

    class FakeSubstrate:
        def __init__(self):
            self.requests = []

        def start_leaf_worker(self, request):
            self.requests.append(request)
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    substrate = FakeSubstrate()
    client = FakeClient()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        client,
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["new_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"}
    assert action.final_payload["registration_mode"] == "service_authorized_birth"
    launch_request = substrate.requests[0]
    assert launch_request.assigned_agent_id == "nanobot_leaf_T-2"
    assert launch_request.description == "NanoBot leaf worker for conversation turns."
    assert launch_request.capabilities == (TURN_KIND_CONVERSATION_V1,)
    assert launch_request.grants == ()
    assert launch_request.accepts_work is True
    assert launch_request.env["CG_AGENT_CREDENTIAL_TOKEN"] == "cgac_leaf-cred.secret"
    assert launch_request.env["CG_PROVISION_TURN_ID"] == "T-2"
    assert launch_request.env["CG_AGENT_ID"] == "nanobot_leaf_T-2"
    assert client.issue_calls == [
        {
            "agent": AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"),
            "expires_at": None,
            "provenance_kind": "nanobot.provision_turn.v1",
            "provenance_ref": "T-2",
            "provenance_payload_hash": None,
        }
    ]
    assert launch_request.env[REGISTRATION_SPEC_ENV] == encode_registration_spec(
        AgentRegistrationSpec(
            role="nanobot.leaf.conversation.v1",
            capabilities=(TURN_KIND_CONVERSATION_V1,),
            grants=(),
            description="NanoBot leaf worker for conversation turns.",
            accepts_work=True,
        )
    )
    assert any(payload.get("phase") == "registration_birth" for _, _, payload in append_calls)
    observed_payload = next(payload for _, _, payload in append_calls if payload.get("phase") == "registration_observed")
    assert observed_payload["registration_provenance"] == {
        "registration_mode": "service_authorized_birth",
        "provision_turn_id": "T-2",
    }
    assert observed_payload["requested_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert observed_payload["observed_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert observed_payload["requested_grants"] == []
    assert observed_payload["observed_grants"] == []
    assert observed_payload["requested_accepts_work"] is True
    assert observed_payload["observed_accepts_work"] is True
    assert action.final_payload["registration_provenance"] == observed_payload["registration_provenance"]
    assert action.final_payload["requested_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert action.final_payload["observed_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert action.final_payload["requested_grants"] == []
    assert action.final_payload["observed_grants"] == []
    assert action.final_payload["requested_accepts_work"] is True
    assert action.final_payload["observed_accepts_work"] is True


def test_provision_handler_uses_requested_lifecycle_source_turn_for_orchestrated_leaf():
    registration_specs = []

    class FakeClient:
        def append_record(self, claim, payload, *, role):
            pass

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return None

        def register_agent_by_service(self, *, project_id, spec, provenance):
            registration_specs.append(spec)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id))

        def issue_agent_credential(self, agent, **kwargs):
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-26",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {"role": "nanobot.leaf.conversation.v1"},
                "lifecycle": {"source_turn_id": "T-25"},
            },
            turn_id="T-26",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
            parent_turn_id="T-25",
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert registration_specs[0].public_metadata == {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=claim.agent_ref(),
            source_turn_id="T-25",
            ttl_seconds=DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
        )
    }


def test_provision_handler_rejects_unsupported_dispatch_child_bootstrap_wrapper():
    class FakeClient:
        def __init__(self):
            self.leaf_get_agent_calls = 0

        def append_record(self, claim, payload, *, role):
            pass

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            self.leaf_get_agent_calls += 1
            if self.leaf_get_agent_calls == 1:
                return None
            return SimpleNamespace(agent=agent, capabilities=(TURN_KIND_CONVERSATION_V1,), grants=(), accepts_work=True)

    class FakeSubstrate:
        def __init__(self):
            self.requests = []

        def start_leaf_worker(self, request):
            self.requests.append(request)
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    substrate = FakeSubstrate()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "parent_turn_id": "parent-1",
                "input": {
                    "agent": {
                        "role": "nanobot.leaf.conversation.v1",
                    },
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert substrate.requests == []
    assert action.final_payload == {
        "error": "invalid_bootstrap",
        "message": "agent is required",
    }


def test_provision_handler_ignores_historical_admission_plan_record_for_birth() -> None:
    registration_calls = []

    class FakeClient:
        def append_record(self, claim, payload, *, role):
            pass

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return None

        def register_agent_by_service(self, *, project_id, spec, provenance):
            registration_calls.append((project_id, spec, provenance))
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id))

        def issue_agent_credential(self, agent, **kwargs):
            del kwargs
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:456")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    historical_role = "_".join(("registration", "admission", "plan"))
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        SimpleNamespace(
            turn=SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1),
            semantic_items=(
                _semantic_item(
                    historical_role,
                    {
                        "assigned_agent_id": "malicious_leaf",
                        "role": "malicious.role",
                        "allowed_capabilities": ["malicious.capability"],
                        "allowed_grants": ["malicious.grant"],
                        "allowed_accepts_work": False,
                    },
                ),
                _semantic_item("bootstrap", {"agent": {"role": LEAF_ROLE}}),
            ),
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert len(registration_calls) == 1
    _, spec, provenance = registration_calls[0]
    assert spec.agent_id == "nanobot_leaf_T-2"
    assert spec.role == LEAF_ROLE
    assert spec.capabilities == (TURN_KIND_CONVERSATION_V1,)
    assert spec.grants == ()
    assert spec.accepts_work is True
    assert provenance == AgentRegistrationProvenance(kind="nanobot.provision_turn.v1", external_ref="T-2")


def test_provision_handler_rejects_observed_agent_missing_requested_capability():
    append_calls = []

    class FakeClient:
        def __init__(self):
            self.leaf_get_agent_calls = 0

        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            self.leaf_get_agent_calls += 1
            if self.leaf_get_agent_calls == 1:
                return None
            return _leaf_snapshot(agent, capabilities=())

        def register_agent_by_service(
            self,
            *,
            project_id,
            spec,
            provenance,
        ):
            _assert_leaf_birth_request(project_id=project_id, spec=spec, provenance=provenance, claim=claim)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id), capabilities=())

        def issue_agent_credential(self, agent, **kwargs):
            del agent, kwargs
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    mismatch_payload = next(payload for _, _, payload in append_calls if payload.get("phase") == "registration_scope_mismatch")
    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "registration_scope_mismatch"
    assert mismatch_payload["missing_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert action.final_payload["requested_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert action.final_payload["observed_capabilities"] == []


def test_provision_handler_rejects_observed_agent_with_extra_grant():
    append_calls = []

    class FakeClient:
        def __init__(self):
            self.leaf_get_agent_calls = 0

        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            self.leaf_get_agent_calls += 1
            if self.leaf_get_agent_calls == 1:
                return None
            return _leaf_snapshot(agent, grants=("filesystem.write",))

        def register_agent_by_service(
            self,
            *,
            project_id,
            spec,
            provenance,
        ):
            _assert_leaf_birth_request(project_id=project_id, spec=spec, provenance=provenance, claim=claim)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id), grants=("filesystem.write",))

        def issue_agent_credential(self, agent, **kwargs):
            del agent, kwargs
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    mismatch_payload = next(payload for _, _, payload in append_calls if payload.get("phase") == "registration_scope_mismatch")
    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "registration_scope_mismatch"
    assert mismatch_payload["extra_grants"] == ["filesystem.write"]
    assert action.final_payload["requested_grants"] == []
    assert action.final_payload["observed_grants"] == ["filesystem.write"]


def test_provision_handler_rejects_observed_agent_with_accepts_work_mismatch():
    append_calls = []

    class FakeClient:
        def __init__(self):
            self.leaf_get_agent_calls = 0

        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            self.leaf_get_agent_calls += 1
            if self.leaf_get_agent_calls == 1:
                return None
            return _leaf_snapshot(agent, accepts_work=False)

        def register_agent_by_service(
            self,
            *,
            project_id,
            spec,
            provenance,
        ):
            _assert_leaf_birth_request(project_id=project_id, spec=spec, provenance=provenance, claim=claim)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id), accepts_work=False)

        def issue_agent_credential(self, agent, **kwargs):
            del agent, kwargs
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:123")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    mismatch_payload = next(payload for _, _, payload in append_calls if payload.get("phase") == "registration_scope_mismatch")
    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "registration_scope_mismatch"
    assert mismatch_payload["accepts_work_mismatch"] is True
    assert action.final_payload["requested_accepts_work"] is True
    assert action.final_payload["observed_accepts_work"] is False


def test_provision_handler_suspends_when_launch_has_not_registered():
    class FakeClient:
        def append_record(self, claim, payload, *, role):
            pass

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return None

        def register_agent_by_service(
            self,
            *,
            project_id,
            spec,
            provenance,
        ):
            _assert_leaf_birth_request(project_id=project_id, spec=spec, provenance=provenance, claim=claim)
            return _leaf_snapshot(AgentRef(project_id=project_id, agent_id=spec.agent_id))

        def issue_agent_credential(self, agent, **kwargs):
            del agent, kwargs
            return {"token": "cgac_leaf-cred.secret"}

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            return ProvisionLaunchResult(started=True, handle="pid:123", note="waiting")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["new_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"}


def test_provision_handler_rejects_unsupported_bootstrap_fields():
    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(
        substrate=SimpleNamespace(start_leaf_worker=lambda request: ProvisionLaunchResult(started=True, handle="pid:123")),
        base_url="http://cg.test",
    )
    action = handler.handle_turn(
        _context(
            {
                "agent": {"role": "nanobot.leaf.conversation.v1"},
                "requested_grants": ["filesystem.write"],
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        SimpleNamespace(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_bootstrap"
    assert action.final_payload["message"] == "unsupported bootstrap field is not allowed: requested_grants"


def test_provision_handler_rejects_unregistered_provisioner() -> None:
    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(
        substrate=SimpleNamespace(start_leaf_worker=lambda request: ProvisionLaunchResult(started=True, handle="pid:123")),
        base_url="http://cg.test",
    )
    action = handler.handle_turn(
        _context(
            {"agent": {"role": "nanobot.leaf.conversation.v1"}},
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        SimpleNamespace(get_agent=lambda agent: None),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "provisioner_not_registered"


def test_provision_handler_rejects_unknown_role() -> None:
    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(
        substrate=SimpleNamespace(start_leaf_worker=lambda request: ProvisionLaunchResult(started=True, handle="pid:123")),
        base_url="http://cg.test",
    )

    class FakeClient:
        def get_agent(self, agent):
            return SimpleNamespace(
                agent=agent,
                capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
                grants=(),
                accepts_work=True,
            )

    action = handler.handle_turn(
        _context(
            {"agent": {"role": "nanobot.leaf.unknown.v1"}},
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_bootstrap"


def test_provision_handler_rejects_preexisting_agent_before_launch():
    class FakeClient:
        def append_record(self, claim, payload, *, role):
            raise AssertionError("preexisting agent should fail before appending launch records")

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return SimpleNamespace(agent=agent, capabilities=(TURN_KIND_CONVERSATION_V1,), grants=(), accepts_work=True)

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            raise AssertionError("preexisting agent should fail before launch")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "agent_already_registered"
    assert action.final_payload["existing_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"}


def test_provision_handler_observes_own_prior_service_birth_without_launch_marker():
    append_calls = []
    own_birth = _leaf_snapshot(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"))
    own_birth.registration_provenance_kind = "nanobot.provision_turn.v1"
    own_birth.registration_provenance_ref = "T-2"

    class FakeClient:
        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return own_birth

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            raise AssertionError("own prior birth should be observed instead of relaunched")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["new_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"}
    assert any(payload.get("phase") == "registration_observed" for _, _, payload in append_calls)


def test_provision_handler_can_finish_after_prior_launch_observes_agent():
    append_calls = []

    class FakeClient:
        def append_record(self, claim, payload, *, role):
            append_calls.append((claim.turn_id, role, payload))

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return SimpleNamespace(agent=agent, capabilities=(TURN_KIND_CONVERSATION_V1,), grants=(), accepts_work=True)

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            raise AssertionError("resume after launch should not start a second worker")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(
        _context(
            {
                "agent": {
                    "role": "nanobot.leaf.conversation.v1",
                },
            },
            turn_id="T-2",
            agent_id="nanobot_provisioner",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
            progress_payloads=({"phase": "launch_result", "started": True},),
        ),
        FakeClient(),
        claim,
    )

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["new_agent_ref"] == {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"}
    assert any(payload.get("phase") == "registration_observed" for _, _, payload in append_calls)


def test_provision_handler_does_not_relaunch_after_launch_started_marker():
    class FakeClient:
        def append_record(self, claim, payload, *, role):
            raise AssertionError("recovery path should not append new launch records")

        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return None

    class FakeSubstrate:
        def start_leaf_worker(self, request):
            raise AssertionError("launch_started recovery should not start a second worker")

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    context = SimpleNamespace(
        turn=SimpleNamespace(
            turn=SimpleNamespace(turn_id="T-2"),
            target_agent=SimpleNamespace(agent_id="nanobot_provisioner"),
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        semantic_items=(
            _semantic_item("bootstrap", {"agent": {"role": LEAF_ROLE}}),
            _semantic_item(
                PROVISION_LAUNCH_STARTED_ROLE,
                {
                    "kind": PROVISION_LAUNCH_STARTED_KIND_V1,
                    "assigned_agent_id": "nanobot_leaf_T-2",
                    "role": LEAF_ROLE,
                },
            ),
        ),
    )
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")
    action = handler.handle_turn(context, FakeClient(), claim)

    assert isinstance(action, SuspendTurnAction)
    assert action.reason == "await_agent_registration"


def test_provision_handler_rejects_mismatched_launch_started_marker():
    class FakeClient:
        def get_agent(self, agent):
            if agent.agent_id == "nanobot_provisioner":
                return _provisioner_snapshot(agent)
            return None

    claim = ClaimToken(
        project_id="demo",
        turn_id="T-2",
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )
    context = SimpleNamespace(
        turn=SimpleNamespace(
            turn=SimpleNamespace(turn_id="T-2"),
            target_agent=SimpleNamespace(agent_id="nanobot_provisioner"),
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        ),
        semantic_items=(
            _semantic_item("bootstrap", {"agent": {"role": LEAF_ROLE}}),
            _semantic_item(
                PROVISION_LAUNCH_STARTED_ROLE,
                {
                    "kind": PROVISION_LAUNCH_STARTED_KIND_V1,
                    "assigned_agent_id": "nanobot_leaf_other",
                    "role": LEAF_ROLE,
                },
            ),
        ),
    )
    handler = ProvisionAgentSpawnHandler(
        substrate=SimpleNamespace(start_leaf_worker=lambda request: ProvisionLaunchResult(started=True, handle="pid:123")),
        base_url="http://cg.test",
    )
    action = handler.handle_turn(context, FakeClient(), claim)

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_bootstrap"


def test_process_substrate_starts_leaf_worker_with_provision_env():
    popen_calls = []

    class FakeProcess:
        pid = 4321

    def fake_popen(args, *, env, cwd, start_new_session):
        popen_calls.append(
            {
                "args": args,
                "env": env,
                "cwd": cwd,
                "start_new_session": start_new_session,
            }
        )
        return FakeProcess()

    request = ProvisionLaunchRequest(
        project_id="demo",
        assigned_agent_id="nanobot_leaf_T-2",
        role="nanobot.leaf.conversation.v1",
        description="NanoBot leaf worker for conversation turns.",
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        grants=(),
        accepts_work=True,
        repo_root="/repo/nanobot",
        config_path="/repo/config.json",
        workspace="/tmp/leaf-001",
        substrate="process",
        provision_turn_id="T-2",
        lifecycle_source_turn_id="T-2",
        registration_mode="service_authorized_birth",
        env={
            "CG_BASE_URL": "http://cg.test",
            "CG_PROJECT_ID": "demo",
            "CG_AGENT_ID": "nanobot_leaf_T-2",
            "CG_AGENT_CREDENTIAL_TOKEN": "cgac_leaf-cred.secret",
            REGISTRATION_SPEC_ENV: encode_registration_spec(
                AgentRegistrationSpec(
                    role="nanobot.leaf.conversation.v1",
                    capabilities=(TURN_KIND_CONVERSATION_V1,),
                    grants=(),
                    description="NanoBot leaf worker for conversation turns.",
                    accepts_work=True,
                )
            ),
            "CG_PROVISION_TURN_ID": "T-2",
            "CG_CLAIM_TOKEN": "must-not-leak",
            "NANOBOT_REPO_ROOT": "/repo/nanobot",
            "NANOBOT_CONFIG_PATH": "/repo/config.json",
            "NANOBOT_WORKSPACE": "/tmp/leaf-001",
        },
    )
    substrate = ProcessOpsSubstrate(
        python_executable="/venv/bin/python",
        cwd="/repo/cg",
        base_env={"PATH": "/bin", "CG_PROVISIONER_CLAIM_TOKEN": "provisioner-token"},
        popen_factory=fake_popen,
    )

    result = substrate.start_leaf_worker(request)

    assert result.started is True
    assert result.handle == "pid:4321"
    assert popen_calls == [
        {
            "args": ["/venv/bin/python", "-m", "Integrations.nanobot.runtime.leaf_worker_main"],
            "env": {
                "PATH": "/bin",
                "CG_BASE_URL": "http://cg.test",
                "CG_PROJECT_ID": "demo",
                "CG_AGENT_ID": "nanobot_leaf_T-2",
                "CG_AGENT_CREDENTIAL_TOKEN": "cgac_leaf-cred.secret",
                REGISTRATION_SPEC_ENV: encode_registration_spec(
                    AgentRegistrationSpec(
                        role="nanobot.leaf.conversation.v1",
                        capabilities=(TURN_KIND_CONVERSATION_V1,),
                        grants=(),
                        description="NanoBot leaf worker for conversation turns.",
                        accepts_work=True,
                    )
                ),
                "CG_PROVISION_TURN_ID": "T-2",
                "NANOBOT_REPO_ROOT": "/repo/nanobot",
                "NANOBOT_CONFIG_PATH": "/repo/config.json",
                "NANOBOT_WORKSPACE": "/tmp/leaf-001",
            },
            "cwd": "/repo/cg",
            "start_new_session": True,
        }
    ]
    assert "CG_CLAIM_TOKEN" not in popen_calls[0]["env"]
    assert "CG_PROVISIONER_CLAIM_TOKEN" not in popen_calls[0]["env"]


def test_leaf_worker_runner_heartbeats_presence_without_local_reconcile_by_default(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            return SimpleNamespace(agent=lookup_agent, public_metadata={}, accepts_work=True)

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def close(self):
            events.append(("close", self.base_url))

    class FakeProjectionClient:
        def __init__(self, *, base_url, headers=None):
            events.append(("projection_init", base_url, headers))
            self.base_url = base_url

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            events.append(("projection_list_turns", project_id, target_agent_id, state, limit))
            return SimpleNamespace(items=())

        def close(self):
            events.append(("projection_close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, handler.__class__.__name__))

        def run_once(self):
            events.append(("run_once",))
            raise RuntimeError("stop-loop")

    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.ProjectionHttpClient", FakeProjectionClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    with pytest.raises(RuntimeError, match="stop-loop"):
        run_leaf_worker_forever(
            base_url="http://cg.test",
            agent=agent,
            presence_interval_seconds=60,
            credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
        )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        ("projection_init", "http://cg.test", _agent_headers(agent)),
        ("get_agent", agent),
        ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}),
        ("worker", "http://cg.test", agent, "LeafTurnHandler"),
        ("presence", agent),
        ("get_agent", agent),
        ("run_once",),
        ("projection_close", "http://cg.test"),
        ("close", "http://cg.test"),
    ]


def test_presence_heartbeater_continues_during_wait_window():
    heartbeats = []
    reached = threading.Event()

    def _heartbeat():
        heartbeats.append(datetime.now(UTC))
        if len(heartbeats) >= 2:
            reached.set()

    heartbeater = PresenceHeartbeater(
        heartbeat_fn=_heartbeat,
        interval_seconds=0.01,
    )
    heartbeater.start()
    try:
        assert reached.wait(timeout=1.0)
    finally:
        heartbeater.stop()

    assert len(heartbeats) >= 2


def test_presence_heartbeater_stop_does_not_mask_emit_now_failure():
    heartbeater = PresenceHeartbeater(
        heartbeat_fn=lambda: (_ for _ in ()).throw(ValueError("emit-now failed")),
        interval_seconds=0.01,
    )

    with pytest.raises(ValueError, match="emit-now failed"):
        heartbeater.start()

    heartbeater.stop()


def test_presence_heartbeater_continues_after_transient_failure():
    call_count = 0
    reached = threading.Event()

    def _heartbeat():
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("transient presence failure")
        if call_count >= 3:
            reached.set()

    heartbeater = PresenceHeartbeater(
        heartbeat_fn=_heartbeat,
        interval_seconds=0.01,
    )
    heartbeater.start()
    try:
        assert reached.wait(timeout=1.0)
    finally:
        heartbeater.stop()

    assert call_count >= 3


def test_leaf_worker_runner_presence_continues_while_work_runs(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            return SimpleNamespace(agent=lookup_agent, public_metadata={}, accepts_work=True)

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def close(self):
            events.append(("close", self.base_url))

    class FakeProjectionClient:
        def __init__(self, *, base_url, headers=None):
            events.append(("projection_init", base_url, headers))
            self.base_url = base_url

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            events.append(("projection_list_turns", project_id, target_agent_id, state, limit))
            return SimpleNamespace(items=())

        def close(self):
            events.append(("projection_close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, handler.__class__.__name__))

        def run_once(self):
            import time

            events.append(("run_once_start",))
            time.sleep(0.05)
            events.append(("run_once_end",))
            raise RuntimeError("stop-loop")

    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.ProjectionHttpClient", FakeProjectionClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    with pytest.raises(RuntimeError, match="stop-loop"):
        run_leaf_worker_forever(
            base_url="http://cg.test",
            agent=agent,
            presence_interval_seconds=0.01,
            reconcile_interval_seconds=60,
            credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
        )

    presence_events = [event for event in events if event[0] == "presence"]
    assert ("init", "http://cg.test", _agent_headers(agent)) in events
    assert ("projection_init", "http://cg.test", _agent_headers(agent)) in events
    assert ("get_agent", agent) in events
    assert ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}) in events
    assert len(presence_events) >= 2
    assert ("run_once_start",) in events
    assert ("run_once_end",) in events
    assert ("projection_close", "http://cg.test") in events


def test_leaf_worker_runner_stops_cleanly_after_drain_without_claim_retry(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf")
    get_agent_calls = 0

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            nonlocal get_agent_calls
            get_agent_calls += 1
            events.append(("get_agent", lookup_agent, get_agent_calls))
            accepts_work = get_agent_calls == 1
            return SimpleNamespace(agent=lookup_agent, public_metadata={}, accepts_work=accepts_work)

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def close(self):
            events.append(("close", self.base_url))

    class FakeProjectionClient:
        def __init__(self, *, base_url, headers=None):
            events.append(("projection_init", base_url, headers))
            self.base_url = base_url

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            events.append(("projection_list_turns", project_id, target_agent_id, state, limit))
            return SimpleNamespace(items=())

        def close(self):
            events.append(("projection_close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, handler.__class__.__name__))

        def run_once(self):
            events.append(("run_once",))
            raise AssertionError("run_once should not be reached after retirement drain is observed")

    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.ProjectionHttpClient", FakeProjectionClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    run_leaf_worker_forever(
        base_url="http://cg.test",
        agent=agent,
        presence_interval_seconds=60,
        credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
    )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        ("projection_init", "http://cg.test", _agent_headers(agent)),
        ("get_agent", agent, 1),
        ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}),
        ("worker", "http://cg.test", agent, "LeafTurnHandler"),
        ("presence", agent),
        ("get_agent", agent, 2),
        ("projection_list_turns", "demo", "nanobot_leaf", TurnState.QUEUED.value, 1),
        ("projection_list_turns", "demo", "nanobot_leaf", TurnState.RUNNING.value, 1),
        ("projection_list_turns", "demo", "nanobot_leaf", TurnState.SUSPENDED.value, 1),
        ("projection_close", "http://cg.test"),
        ("close", "http://cg.test"),
    ]


def test_leaf_worker_runner_stops_cleanly_after_revoked_credential(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            return SimpleNamespace(agent=lookup_agent, public_metadata={}, accepts_work=True)

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def close(self):
            events.append(("close", self.base_url))

    class FakeProjectionClient:
        def __init__(self, *, base_url, headers=None):
            events.append(("projection_init", base_url, headers))
            self.base_url = base_url

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            events.append(("projection_list_turns", project_id, target_agent_id, state, limit))
            return SimpleNamespace(items=())

        def close(self):
            events.append(("projection_close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, handler.__class__.__name__))

        def run_once(self):
            events.append(("run_once",))
            request = httpx.Request("POST", f"http://cg.test/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/claims:claim")
            response = httpx.Response(
                403,
                request=request,
                json={"error": "ForbiddenError", "message": "agent credential status is not active: revoked"},
            )
            raise httpx.HTTPStatusError("revoked", request=request, response=response)

    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.ProjectionHttpClient", FakeProjectionClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    run_leaf_worker_forever(
        base_url="http://cg.test",
        agent=agent,
        presence_interval_seconds=60,
        credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
    )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        ("projection_init", "http://cg.test", _agent_headers(agent)),
        ("get_agent", agent),
        ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}),
        ("worker", "http://cg.test", agent, "LeafTurnHandler"),
        ("presence", agent),
        ("get_agent", agent),
        ("run_once",),
        ("projection_close", "http://cg.test"),
        ("close", "http://cg.test"),
    ]


def test_leaf_worker_runner_keeps_non_terminal_auth_errors_visible(monkeypatch):
    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers

        def get_agent(self, lookup_agent):
            return SimpleNamespace(agent=lookup_agent, public_metadata={}, accepts_work=True)

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            return None

        def heartbeat_agent_presence(self, presence_agent):
            return None

        def close(self):
            return None

    class FakeProjectionClient:
        def __init__(self, *, base_url, headers=None):
            del base_url, headers

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            del project_id, target_agent_id, state, limit
            return SimpleNamespace(items=())

        def close(self):
            return None

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            del client, agent, handler

        def run_once(self):
            request = httpx.Request("POST", f"http://cg.test/v3r1/projects/{agent.project_id}/agents/{agent.agent_id}/claims:claim")
            response = httpx.Response(
                403,
                request=request,
                json={"error": "ForbiddenError", "message": "agent credential identity does not match claimed identity"},
            )
            raise httpx.HTTPStatusError("identity mismatch", request=request, response=response)

    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.ProjectionHttpClient", FakeProjectionClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.leaf_worker_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    with pytest.raises(httpx.HTTPStatusError, match="identity mismatch"):
        run_leaf_worker_forever(
            base_url="http://cg.test",
            agent=agent,
            presence_interval_seconds=60,
            credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
        )


def test_supervisor_runner_heartbeats_presence_without_local_reconcile_by_default(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_supervisor")
    provisioner = AgentRef(project_id="demo", agent_id="nanobot_provisioner")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            return SimpleNamespace(agent=lookup_agent, public_metadata={})

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            events.append(("fetch_agent_feed", feed_agent, after_ledger_seq))
            return _feed_page()

        def close(self):
            events.append(("close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, isinstance(handler, SupervisorTurnHandler), handler._provisioner_agent))

        def run_once(self):
            events.append(("run_once",))
            raise RuntimeError("stop-loop")

    monkeypatch.setattr("Integrations.nanobot.runtime.supervisor_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.supervisor_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.supervisor_runner.build_agent_loop", lambda **kwargs: SimpleNamespace())

    with pytest.raises(RuntimeError, match="stop-loop"):
        run_supervisor_worker_forever(
            base_url="http://cg.test",
            agent=agent,
            provisioner_agent=provisioner,
            idle_sleep_seconds=0,
            presence_interval_seconds=60,
            credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
        )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        ("get_agent", agent),
        ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}),
        ("worker", "http://cg.test", agent, True, provisioner),
        ("presence", agent),
        ("fetch_agent_feed", agent, 0),
        ("run_once",),
        ("close", "http://cg.test"),
    ]


def test_self_root_frontside_runner_resumes_suspended_parent_before_next_claim(monkeypatch):
    events = []
    agent = AgentRef(project_id="demo", agent_id="agent_a")
    parent_turn = TurnRef(project_id="demo", turn_id="T-1")
    run_count = 0

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            return SimpleNamespace(agent=lookup_agent, public_metadata={})

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def close(self):
            events.append(("close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(("worker", client.base_url, agent, isinstance(handler, SelfRootFrontsideHandler)))

        def run_once(self):
            nonlocal run_count
            run_count += 1
            events.append(("run_once", run_count))
            if run_count == 1:
                return SimpleNamespace(claimed_turn=parent_turn, action="suspended")
            raise RuntimeError("stop-loop")

    def fake_resume_ready_parent_turns(client, *, agent, watched_turns, agent_feed_after_ledger_seq=0):
        events.append(("resume_ready", agent, tuple(sorted(turn.turn_id for turn in watched_turns)), agent_feed_after_ledger_seq))
        return agent_feed_after_ledger_seq + 1

    monkeypatch.setattr("Integrations.nanobot.runtime.self_root_frontside_runner.HttpAgentClient", FakeClient)
    monkeypatch.setattr("Integrations.nanobot.runtime.self_root_frontside_runner.PollingWorker", FakeWorker)
    monkeypatch.setattr("Integrations.nanobot.runtime.self_root_frontside_runner._resume_ready_parent_turns", fake_resume_ready_parent_turns)

    with pytest.raises(RuntimeError, match="stop-loop"):
        run_self_root_frontside_worker_forever(
            base_url="http://cg.test",
            agent=agent,
            idle_sleep_seconds=0,
            presence_interval_seconds=60,
            credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
        )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        ("get_agent", agent),
        ("metadata", agent, {"turn_offers": [conversation_turn_offer()]}),
        ("worker", "http://cg.test", agent, True),
        ("presence", agent),
        ("resume_ready", agent, (), 0),
        ("run_once", 1),
        ("resume_ready", agent, ("T-1",), 1),
        ("run_once", 2),
        ("close", "http://cg.test"),
    ]


def test_self_root_frontside_main_loads_env_and_runs_worker():
    calls = []

    env = {
        "CG_BASE_URL": "http://cg.test",
        "CG_PROJECT_ID": "demo",
        "CG_AGENT_ID": "agent_a",
        "CG_AGENT_CREDENTIAL_TOKEN": TEST_AGENT_CREDENTIAL_TOKEN,
    }

    run_self_root_frontside_from_env(env, runner=lambda **kwargs: calls.append(kwargs))

    assert load_self_root_frontside_env(env).agent_ref() == AgentRef(project_id="demo", agent_id="agent_a")
    assert calls == [
        {
            "base_url": "http://cg.test",
            "agent": AgentRef(project_id="demo", agent_id="agent_a"),
            "credential_token": TEST_AGENT_CREDENTIAL_TOKEN,
        }
    ]


def test_self_root_frontside_main_requires_agent_id():
    with pytest.raises(ValueError, match="CG_AGENT_ID"):
        load_self_root_frontside_env(
            {
                "CG_PROJECT_ID": "demo",
                "CG_AGENT_CREDENTIAL_TOKEN": TEST_AGENT_CREDENTIAL_TOKEN,
            }
        )


def test_provisioner_runner_builds_worker_with_provision_handler():
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    substrate = SimpleNamespace()

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def get_agent(self, lookup_agent):
            return SimpleNamespace(agent=lookup_agent, public_metadata={"ui": {"label": "Provisioner"}})

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            return _feed_page()

        def close(self):
            events.append(("close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            events.append(
                (
                    "worker",
                    client.base_url,
                    agent,
                    isinstance(handler, ProvisionAgentSpawnHandler),
                    handler._substrate,
                )
            )

        def run_once(self):
            events.append(("run_once",))
            return SimpleNamespace(claimed_turn=None, action="idle")

    run_provisioner_worker_forever(
        base_url="http://cg.test",
        agent=agent,
        substrate=substrate,
        idle_sleep_seconds=0,
        max_iterations=1,
        presence_interval_seconds=60,
        client_factory=FakeClient,
        worker_factory=FakeWorker,
        credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
    )

    assert events == [
        ("init", "http://cg.test", _agent_headers(agent)),
        (
            "metadata",
            agent,
            {
                "ui": {"label": "Provisioner"},
                "turn_offers": [provision_turn_offer(roles=published_available_roles())],
            },
        ),
        ("worker", "http://cg.test", agent, True, substrate),
        ("presence", agent),
        ("run_once",),
        ("close", "http://cg.test"),
    ]


def test_provision_lifecycle_cleanup_drains_and_revokes_closed_ephemeral_agent() -> None:
    events = []
    owner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_id = "nanobot_leaf_T-2"
    closed_at = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
    metadata = {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=owner,
            source_turn_id="T-2",
        )
    }

    class FakeClient:
        def get_turn(self, turn):
            events.append(("get_turn", turn))
            return SimpleNamespace(state=TurnState.CLOSED, closed_at=closed_at)

        def drain_agent(self, drained_agent, *, requested_by=None, meta=None):
            events.append(("drain", drained_agent, requested_by, meta.reason, meta.annotations["provision_lifecycle"]["source_turn_id"]))

        def list_agent_credentials(self, credential_agent):
            events.append(("list_credentials", credential_agent))
            return {
                "credentials": [
                    {"credential_id": "cred_active", "status": AGENT_CREDENTIAL_STATUS_ACTIVE},
                    {"credential_id": "cred_revoked", "status": AGENT_CREDENTIAL_STATUS_REVOKED},
                ]
            }

        def revoke_agent_credential(self, credential_agent, credential_id):
            events.append(("revoke", credential_agent, credential_id))
            return {}

    class FakeProjectionClient:
        def list_agents(self, *, project_id, limit=100):
            events.append(("list_agents", project_id, limit))
            return SimpleNamespace(
                items=(
                    SimpleNamespace(
                        agent_id=leaf_id,
                        accepts_work=True,
                        public_metadata=metadata,
                    ),
                )
            )

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            events.append(("list_turns", project_id, target_agent_id, state, limit))
            return SimpleNamespace(items=())

    summary = cleanup_ephemeral_provision_agents(
        FakeClient(),
        FakeProjectionClient(),
        owner_agent=owner,
        now=closed_at + timedelta(seconds=10),
    )

    target = AgentRef(project_id="demo", agent_id=leaf_id)
    assert summary.scanned_agents == 1
    assert summary.eligible_agents == 1
    assert summary.drained_agents == 1
    assert summary.grace_period_agents == 1
    assert summary.revoked_credentials == 0
    assert ("drain", target, owner, "provision_lifecycle_drain", "T-2") in events
    assert ("revoke", target, "cred_active") not in events
    assert ("revoke", target, "cred_revoked") not in events


def test_provision_lifecycle_cleanup_revokes_closed_ephemeral_agent_after_grace_pass() -> None:
    events = []
    owner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    leaf_id = "nanobot_leaf_T-2"
    closed_at = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
    metadata = {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=owner,
            source_turn_id="T-2",
        )
    }

    class FakeClient:
        def get_turn(self, turn):
            events.append(("get_turn", turn))
            return SimpleNamespace(state=TurnState.CLOSED, closed_at=closed_at)

        def drain_agent(self, drained_agent, *, requested_by=None, meta=None):
            events.append(("drain", drained_agent, requested_by, meta.reason))

        def list_agent_credentials(self, credential_agent):
            events.append(("list_credentials", credential_agent))
            return {"credentials": [{"credential_id": "cred_active", "status": AGENT_CREDENTIAL_STATUS_ACTIVE}]}

        def revoke_agent_credential(self, credential_agent, credential_id):
            events.append(("revoke", credential_agent, credential_id))
            return {}

    class FakeProjectionClient:
        def list_agents(self, *, project_id, limit=100):
            return SimpleNamespace(items=(SimpleNamespace(agent_id=leaf_id, accepts_work=False, public_metadata=metadata),))

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            return SimpleNamespace(items=())

    summary = cleanup_ephemeral_provision_agents(
        FakeClient(),
        FakeProjectionClient(),
        owner_agent=owner,
        now=closed_at + timedelta(seconds=40),
    )

    target = AgentRef(project_id="demo", agent_id=leaf_id)
    assert summary.drained_agents == 0
    assert summary.grace_period_agents == 0
    assert summary.revoked_credentials == 1
    assert ("revoke", target, "cred_active") in events


def test_provision_lifecycle_cleanup_waits_for_open_turns_before_ttl() -> None:
    events = []
    owner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    closed_at = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
    metadata = {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=owner,
            source_turn_id="T-2",
            ttl_seconds=60,
        )
    }

    class FakeClient:
        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.CLOSED, closed_at=closed_at)

        def drain_agent(self, drained_agent, *, requested_by=None, meta=None):
            events.append(("drain", drained_agent, requested_by))

        def list_agent_credentials(self, credential_agent):
            events.append(("list_credentials", credential_agent))
            return {"credentials": [{"credential_id": "cred_active", "status": AGENT_CREDENTIAL_STATUS_ACTIVE}]}

        def revoke_agent_credential(self, credential_agent, credential_id):
            events.append(("revoke", credential_agent, credential_id))
            return {}

    class FakeProjectionClient:
        def list_agents(self, *, project_id, limit=100):
            return SimpleNamespace(items=(SimpleNamespace(agent_id="nanobot_leaf_T-2", accepts_work=True, public_metadata=metadata),))

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            if state == TurnState.RUNNING.value:
                return SimpleNamespace(items=(SimpleNamespace(turn_id="T-open", state=TurnState.RUNNING),))
            return SimpleNamespace(items=())

    summary = cleanup_ephemeral_provision_agents(
        FakeClient(),
        FakeProjectionClient(),
        owner_agent=owner,
        now=closed_at + timedelta(seconds=30),
    )

    assert summary.drained_agents == 0
    assert summary.grace_period_agents == 0
    assert summary.waiting_agents == 1
    assert summary.revoked_credentials == 0
    assert not any(event[0] == "drain" for event in events)
    assert not any(event[0] == "revoke" for event in events)


def test_provision_lifecycle_cleanup_revokes_after_ttl_with_open_turns() -> None:
    events = []
    owner = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    closed_at = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
    metadata = {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=owner,
            source_turn_id="T-2",
            ttl_seconds=60,
        )
    }

    class FakeClient:
        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.CLOSED, closed_at=closed_at)

        def drain_agent(self, drained_agent, *, requested_by=None, meta=None):
            events.append(("drain", drained_agent, requested_by))

        def list_agent_credentials(self, credential_agent):
            events.append(("list_credentials", credential_agent))
            return {"credentials": [{"credential_id": "cred_active", "status": AGENT_CREDENTIAL_STATUS_ACTIVE}]}

        def revoke_agent_credential(self, credential_agent, credential_id):
            events.append(("revoke", credential_agent, credential_id))
            return {}

    class FakeProjectionClient:
        def list_agents(self, *, project_id, limit=100):
            return SimpleNamespace(items=(SimpleNamespace(agent_id="nanobot_leaf_T-2", accepts_work=False, public_metadata=metadata),))

        def list_turns(self, *, project_id, target_agent_id=None, state=None, limit=100):
            if state == TurnState.SUSPENDED.value:
                return SimpleNamespace(items=(SimpleNamespace(turn_id="T-open", state=TurnState.SUSPENDED),))
            return SimpleNamespace(items=())

    summary = cleanup_ephemeral_provision_agents(
        FakeClient(),
        FakeProjectionClient(),
        owner_agent=owner,
        now=closed_at + timedelta(seconds=90),
    )

    assert summary.timed_out_agents == 1
    assert summary.waiting_agents == 0
    assert summary.revoked_credentials == 1
    assert ("revoke", AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2"), "cred_active") in events
    assert not any(event[0] == "drain" for event in events)


def test_provisioner_runner_recovers_suspended_provision_after_restart():
    events = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    provision_turn = TurnRef(project_id="demo", turn_id="T-2")

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            events.append(("init", base_url, headers))

        def update_agent_public_metadata(self, update_agent, *, public_metadata, meta=None, trace=None):
            events.append(("metadata", update_agent, public_metadata))

        def heartbeat_agent_presence(self, presence_agent):
            events.append(("presence", presence_agent))

        def reconcile_expired_claim(self, reconcile_agent):
            events.append(("reconcile", reconcile_agent))

        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            events.append(("fetch_agent_feed", feed_agent, after_ledger_seq))
            if after_ledger_seq == 0:
                return _feed_page(
                    (SimpleNamespace(project_id="demo", event_type="turn.progress_appended", subject_kind="turn", subject_id="old-turn"),),
                    next_after_ledger_seq=1,
                )
            if after_ledger_seq == 1:
                return _feed_page(
                    (
                        SimpleNamespace(
                            project_id=provision_turn.project_id,
                            event_type="turn.suspended",
                            subject_kind="turn",
                            subject_id=provision_turn.turn_id,
                        ),
                    ),
                    next_after_ledger_seq=2,
                )
            return _feed_page(next_after_ledger_seq=after_ledger_seq)

        def get_turn(self, turn):
            events.append(("get_turn", turn))
            return SimpleNamespace(state=TurnState.SUSPENDED)

        def fetch_context(self, turn):
            events.append(("fetch_context", turn))
            return _context(
                {
                    "agent": {
                        "role": "nanobot.leaf.conversation.v1",
                    },
                },
                turn_id=turn.turn_id,
                agent_id=agent.agent_id,
                turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                progress_payloads=(
                    {
                        "phase": "registration_birth",
                        "new_agent_ref": {"project_id": "demo", "agent_id": "nanobot_leaf_T-2"},
                    },
                ),
            )

        def get_agent(self, lookup_agent):
            events.append(("get_agent", lookup_agent))
            if lookup_agent.agent_id == "nanobot_provisioner_alpha":
                return SimpleNamespace(
                    agent=lookup_agent,
                    capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
                    grants=(),
                    accepts_work=True,
                    public_metadata={"ui": {"label": "Provisioner"}},
                )
            return SimpleNamespace(agent=lookup_agent, public_metadata={})

        def resume_turn(self, resume_agent, turn, *, note):
            events.append(("resume_turn", resume_agent, turn, note))

        def close(self):
            events.append(("close", self.base_url))

    class FakeWorker:
        def __init__(self, *, client, agent, handler):
            self._runs = 0

        def run_once(self):
            events.append(("run_once", "idle"))
            return SimpleNamespace(claimed_turn=None, action="idle")

    run_provisioner_worker_forever(
        base_url="http://cg.test",
        agent=agent,
        substrate=SimpleNamespace(),
        idle_sleep_seconds=0,
        max_iterations=1,
        presence_interval_seconds=60,
        client_factory=FakeClient,
        worker_factory=FakeWorker,
        credential_token=TEST_AGENT_CREDENTIAL_TOKEN,
    )

    assert ("fetch_agent_feed", agent, 1) in events
    assert ("init", "http://cg.test", _agent_headers(agent)) in events
    assert ("get_agent", AgentRef(project_id="demo", agent_id="nanobot_leaf_T-2")) in events
    assert ("resume_turn", agent, provision_turn, "agent_registered") in events
    assert (
        "metadata",
        agent,
        {
            "ui": {"label": "Provisioner"},
            "turn_offers": [provision_turn_offer(roles=published_available_roles())],
        },
    ) in events
    assert events[-1] == ("close", "http://cg.test")


def test_provisioner_runner_derives_assigned_agent_id_without_progress_record() -> None:
    resume_calls = []
    agent = AgentRef(project_id="demo", agent_id="nanobot_provisioner_alpha")
    provision_turn = TurnRef(project_id="demo", turn_id="T-2")

    class FakeClient:
        def fetch_agent_feed(self, feed_agent, *, after_ledger_seq=0, limit=500):
            return _feed_page(
                (
                    SimpleNamespace(
                        project_id=provision_turn.project_id,
                        event_type="turn.suspended",
                        subject_kind="turn",
                        subject_id=provision_turn.turn_id,
                    ),
                ),
                next_after_ledger_seq=1,
            )

        def get_turn(self, turn):
            return SimpleNamespace(state=TurnState.SUSPENDED)

        def fetch_context(self, turn):
            return _context(
                {"agent": {"role": LEAF_ROLE}},
                turn_id=turn.turn_id,
                agent_id=agent.agent_id,
                turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
            )

        def get_agent(self, lookup_agent):
            return SimpleNamespace(agent=lookup_agent)

        def resume_turn(self, resume_agent, turn, *, note):
            resume_calls.append((resume_agent, turn, note))

    watched_turns = set()
    next_after = _resume_ready_provision_turns(
        FakeClient(),
        agent=agent,
        watched_turns=watched_turns,
        agent_feed_after_ledger_seq=0,
    )

    assert next_after == 1
    assert resume_calls == [(agent, provision_turn, "agent_registered")]
