from __future__ import annotations

from types import SimpleNamespace

from CommonGround.agent_registration import AgentRegistrationProvenance
from CommonGround.contracts import AgentRef, ClaimToken, TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1, TurnOutcome
from Integrations.nanobot.adapter.provision_handler import ProvisionAgentSpawnHandler, ProvisionLaunchResult
from Integrations.nanobot.provision_lifecycle import (
    DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
    PROVISION_LIFECYCLE_METADATA_KEY,
    build_ephemeral_lifecycle_metadata,
)


LEAF_ROLE = "nanobot.leaf.conversation.v1"


def _semantic_item(role, payload):
    return SimpleNamespace(
        record=SimpleNamespace(record_role=role),
        content=SimpleNamespace(payload=lambda: payload),
    )


def _context(payload, *, turn_id: str = "T-provision", parent_turn_id: str | None = None):
    return SimpleNamespace(
        turn=SimpleNamespace(
            turn=SimpleNamespace(turn_id=turn_id),
            target_agent=SimpleNamespace(agent_id="nanobot_provisioner"),
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
            cause=SimpleNamespace(kind="turn" if parent_turn_id is not None else "external_request", id=parent_turn_id or "request-1"),
        ),
        semantic_items=(_semantic_item("bootstrap", payload),),
    )


def _claim(turn_id: str = "T-provision") -> ClaimToken:
    return ClaimToken(
        project_id="demo",
        turn_id=turn_id,
        agent_id="nanobot_provisioner",
        token="token",
        expires_at=SimpleNamespace(),
    )


def _provisioner_snapshot(agent):
    return SimpleNamespace(agent=agent, capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,), grants=(), accepts_work=True)


def _leaf_snapshot(agent, *, capabilities=(TURN_KIND_CONVERSATION_V1,), grants=(), accepts_work=True):
    return SimpleNamespace(agent=agent, role=LEAF_ROLE, capabilities=capabilities, grants=grants, accepts_work=accepts_work)


class FakeClient:
    def __init__(
        self,
        *,
        registered_capabilities=(TURN_KIND_CONVERSATION_V1,),
        issue_response=None,
        revoke_error: Exception | None = None,
        append_error_phase: str | None = None,
    ):
        self.registered_capabilities = registered_capabilities
        self.issue_response = issue_response or {
            "token": "cgac_leaf-cred.secret",
            "credential": {"credential_id": "cred_leaf"},
        }
        self.revoke_error = revoke_error
        self.append_error_phase = append_error_phase
        self.append_calls = []
        self.issue_calls = []
        self.revoke_calls = []
        self.registration_specs = []

    def append_record(self, claim, payload, *, role):
        if self.append_error_phase is not None and isinstance(payload, dict) and payload.get("phase") == self.append_error_phase:
            raise RuntimeError(f"{self.append_error_phase} append failed")
        self.append_calls.append((claim.turn_id, role, payload))

    def get_agent(self, agent):
        if agent.agent_id == "nanobot_provisioner":
            return _provisioner_snapshot(agent)
        return None

    def register_agent_by_service(self, *, project_id, spec, provenance):
        self.registration_specs.append((project_id, spec, provenance))
        return _leaf_snapshot(
            AgentRef(project_id=project_id, agent_id=spec.agent_id),
            capabilities=self.registered_capabilities,
        )

    def issue_agent_credential(self, agent, **kwargs):
        self.issue_calls.append((agent, kwargs))
        return self.issue_response

    def revoke_agent_credential(self, agent, credential_id):
        self.revoke_calls.append((agent, credential_id))
        if self.revoke_error is not None:
            raise self.revoke_error
        return {"credential": {"credential_id": credential_id, "status": "revoked"}}


class FakeSubstrate:
    def __init__(self, result: ProvisionLaunchResult | None = None, error: Exception | None = None):
        self.result = result or ProvisionLaunchResult(started=True, handle="pid:123")
        self.error = error
        self.requests = []

    def start_leaf_worker(self, request):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.result


def _bootstrap(*, source_turn_id: str = "T-parent", task_id: str | None = None):
    payload = {
        "agent": {"role": LEAF_ROLE},
        "lifecycle": {"source_turn_id": source_turn_id},
    }
    if task_id is not None:
        payload["work_order"] = {"task_id": task_id, "objective": "Research the topic."}
    return payload


def test_provision_lifecycle_source_turn_matches_parent_and_reports_task_id():
    claim = _claim()
    client = FakeClient()
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(task_id="concept"), parent_turn_id="T-parent"), client, claim)

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["task_id"] == "concept"
    _, spec, provenance = client.registration_specs[0]
    assert provenance == AgentRegistrationProvenance(kind="nanobot.provision_turn.v1", external_ref=claim.turn_id)
    assert spec.public_metadata == {
        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
            owner_agent=claim.agent_ref(),
            source_turn_id="T-parent",
            ttl_seconds=DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
        )
    }


def test_provision_rejects_mismatched_lifecycle_source_turn():
    client = FakeClient()
    substrate = FakeSubstrate()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(source_turn_id="T-other"), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_lifecycle_source_turn"
    assert "T-parent" in action.final_payload["message"]
    assert client.registration_specs == []
    assert client.issue_calls == []
    assert substrate.requests == []


def test_provision_rejects_explicit_external_lifecycle_source_turn():
    client = FakeClient()
    substrate = FakeSubstrate()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(source_turn_id="T-other")), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_lifecycle_source_turn"
    assert "parent-caused provision turns" in action.final_payload["message"]
    assert client.registration_specs == []
    assert client.issue_calls == []
    assert substrate.requests == []


def test_provision_scope_mismatch_does_not_issue_credential():
    client = FakeClient(registered_capabilities=())
    handler = ProvisionAgentSpawnHandler(substrate=FakeSubstrate(), base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "registration_scope_mismatch"
    assert action.final_payload["missing_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert client.issue_calls == []
    assert client.revoke_calls == []


def test_provision_launch_failed_revokes_issued_credential():
    client = FakeClient()
    handler = ProvisionAgentSpawnHandler(
        substrate=FakeSubstrate(ProvisionLaunchResult(started=False, handle=None, note="spawn failed")),
        base_url="http://cg.test",
    )

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "launch_failed"
    assert client.revoke_calls == [(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-provision"), "cred_leaf")]
    assert action.final_payload["credential_cleanup"]["credential_id"] == "cred_leaf"
    assert action.final_payload["credential_cleanup"]["result"]["credential"]["status"] == "revoked"
    assert any(payload.get("phase") == "credential_cleanup" for _, _, payload in client.append_calls)


def test_provision_prelaunch_progress_append_failure_revokes_issued_credential():
    client = FakeClient(append_error_phase="launch_plan")
    substrate = FakeSubstrate()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "prelaunch_progress_record_failed"
    assert "launch_plan append failed" in action.final_payload["message"]
    assert substrate.requests == []
    assert client.revoke_calls == [(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-provision"), "cred_leaf")]
    assert action.final_payload["credential_cleanup"]["reason"] == "prelaunch_progress_record_failed"


def test_provision_launch_start_exception_revokes_issued_credential():
    client = FakeClient()
    substrate = FakeSubstrate(error=RuntimeError("process exec failed"))
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "launch_start_failed"
    assert "process exec failed" in action.final_payload["message"]
    assert client.revoke_calls == [(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-provision"), "cred_leaf")]
    assert action.final_payload["credential_cleanup"]["reason"] == "launch_start_failed"


def test_provision_launch_result_record_failure_revokes_when_launch_failed():
    client = FakeClient(append_error_phase="launch_result")
    handler = ProvisionAgentSpawnHandler(
        substrate=FakeSubstrate(ProvisionLaunchResult(started=False, handle=None, note="spawn failed")),
        base_url="http://cg.test",
    )

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "launch_result_record_failed"
    assert "launch_result append failed" in action.final_payload["message"]
    assert client.revoke_calls == [(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-provision"), "cred_leaf")]
    assert action.final_payload["credential_cleanup"]["reason"] == "launch_result_record_failed"


def test_provision_missing_plaintext_token_revokes_issued_credential_id():
    client = FakeClient(issue_response={"credential": {"credential_id": "cred_without_token"}})
    substrate = FakeSubstrate()
    handler = ProvisionAgentSpawnHandler(substrate=substrate, base_url="http://cg.test")

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "credential_issue_failed"
    assert "plaintext token" in action.final_payload["message"]
    assert substrate.requests == []
    assert client.revoke_calls == [(AgentRef(project_id="demo", agent_id="nanobot_leaf_T-provision"), "cred_without_token")]
    assert action.final_payload["credential_cleanup"]["credential_id"] == "cred_without_token"


def test_provision_revoke_failure_is_reported_when_launch_fails():
    client = FakeClient(revoke_error=RuntimeError("revoke unavailable"))
    handler = ProvisionAgentSpawnHandler(
        substrate=FakeSubstrate(ProvisionLaunchResult(started=False, handle=None, note="spawn failed")),
        base_url="http://cg.test",
    )

    action = handler.handle_turn(_context(_bootstrap(), parent_turn_id="T-parent"), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "launch_failed"
    assert action.final_payload["credential_cleanup"]["credential_id"] == "cred_leaf"
    assert action.final_payload["credential_cleanup_error"] == "revoke unavailable"
    cleanup_payload = next(payload for _, _, payload in client.append_calls if payload.get("phase") == "credential_cleanup")
    assert cleanup_payload["credential_cleanup_error"] == "revoke unavailable"
