from __future__ import annotations

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient

from CommonGround.agent_registration import (
    AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT,
    AGENT_REGISTRATION_BIRTH_GRANT,
    AgentBirthSpec,
    AgentRegistrationProvenance,
    agent_birth_spec_hash,
)
from CommonGround.contracts import (
    AgentRef,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
    WORK_MEMORY_REPORT_MANIFEST_ROLE,
    WORK_MEMORY_REPORT_RESULT_ROLE,
    WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
    OperationMeta,
    TurnOutcome,
    TurnState,
    WorkMemoryReportRecordWriteSpec,
    WorkMemoryReportWriteSpec,
)
from CommonGround.agent_client import FinishTurnAction, HttpAgentClient, PollingWorker
from CommonGround.service import ServiceConfig, create_service_app
from CommonGround.service.serialization import to_jsonable
from tests.auth_support import agent_headers, missing_credential_headers


PROJECT_ID = "demo"
FRONTSIDE = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
NANOBOT = AgentRef(project_id=PROJECT_ID, agent_id="nanobot")
OPERATOR = AgentRef(project_id=PROJECT_ID, agent_id="operator")
OTHER_PROJECT_NANOBOT = AgentRef(project_id="other", agent_id="nanobot")
LEAF_ROLE = "nanobot.leaf.conversation.v1"

def _seed_agent(
    kernel_app,
    agent: AgentRef,
    *,
    capabilities: tuple[str, ...] = (),
    accepts_work: bool = True,
    grants: tuple[str, ...] = (),
    enabled: bool = True,
) -> AgentRef:
    return kernel_app.topology.register_agent(
        agent,
        capabilities=capabilities,
        accepts_work=accepts_work,
        grants=grants,
        enabled=enabled,
    )


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def _client(test_client, agent: AgentRef) -> HttpAgentClient:
    return HttpAgentClient(client=test_client, headers=_headers(agent))


def _disable_agent(pg_dsn: str, agent: AgentRef) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cg_agents
            set enabled = false, updated_at = now()
            where project_id = %s and agent_id = %s
            """,
            (agent.project_id, agent.agent_id),
        )
        conn.commit()


def _provision_request_payload(*, role: str = "nanobot.leaf.conversation.v1") -> dict:
    return {"task": "provision", "agent": {"role": role}}


def _provisioner_capabilities() -> tuple[str, ...]:
    return (TURN_KIND_PROVISION_AGENT_SPAWN_V1,)


def _dispatch_root(
    client: HttpAgentClient,
    *,
    requested_by: AgentRef,
    target_agent: AgentRef,
    request_payload,
    request_id: str | None = None,
    dispatch_key: str | None = None,
    spawn_key: str | None = None,
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
):
    resolved_request_id = request_id or dispatch_key or spawn_key
    resolved_dispatch_key = dispatch_key or spawn_key or request_id
    assert resolved_request_id is not None
    assert resolved_dispatch_key is not None
    return client.dispatch(
        requested_by=requested_by,
        target_agent=target_agent,
        input_payload=request_payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=resolved_request_id),
        dispatch_key=resolved_dispatch_key,
        turn_kind=turn_kind,
    )


def test_http_dispatch_separates_caller_meta_from_system_ledger_annotations(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    client = _client(test_client, FRONTSIDE)

    turn = client.dispatch(
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        input_payload={"task": "meta boundary"},
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="meta-boundary"),
        dispatch_key="meta-boundary",
        turn_kind=TURN_KIND_CONVERSATION_V1,
        meta=OperationMeta(
            note="caller note",
            reason="caller reason",
            annotations={
                "requested_by": "spoofed-agent",
                "turn_kind": "spoofed-kind",
                "spawn_entrypoint": "spoofed-entrypoint",
                "system": "spoofed-system-namespace",
                "caller": "spoofed-caller-namespace",
                "custom": "caller-value",
            },
        ),
    )

    feed = client.fetch_turn_feed(turn)
    spawned = next(item for item in feed.items if item.event_type == "turn.spawned")
    assert spawned.note is None
    assert spawned.annotations["requested_by"] == FRONTSIDE.agent_id
    assert spawned.annotations["turn_kind"] == TURN_KIND_CONVERSATION_V1
    assert spawned.annotations["spawn_entrypoint"] == "dispatch.root_request"
    assert "custom" not in spawned.annotations
    assert spawned.annotations["system"] == {
        "requested_by": FRONTSIDE.agent_id,
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "spawn_entrypoint": "dispatch.root_request",
    }
    assert spawned.annotations["caller"]["note"] == "caller note"
    assert spawned.annotations["caller"]["reason"] == "caller reason"
    assert spawned.annotations["caller"]["annotations"] == {
        "requested_by": "spoofed-agent",
        "turn_kind": "spoofed-kind",
        "spawn_entrypoint": "spoofed-entrypoint",
        "system": "spoofed-system-namespace",
        "caller": "spoofed-caller-namespace",
        "custom": "caller-value",
    }
    assert spawned.annotations["caller"]["reserved_annotation_keys"] == [
        "caller",
        "requested_by",
        "spawn_entrypoint",
        "system",
        "turn_kind",
    ]


@pytest.mark.parametrize(
    ("body_patch", "message"),
    [
        ({"authority": {"mode": "root_request", "request_id": "   "}}, "request_id must be non-empty after trimming"),
        ({"dispatch_key": "x" * 129}, "dispatch_key must be at most 128 characters"),
        ({"dispatch_key": "bad key"}, "dispatch_key must start with a letter or digit"),
    ],
)
def test_http_dispatch_rejects_invalid_id_anchors(test_client, body_patch, message) -> None:
    body = {
        "requested_by": {"project_id": PROJECT_ID, "agent_id": FRONTSIDE.agent_id},
        "target_agent": {"project_id": PROJECT_ID, "agent_id": NANOBOT.agent_id},
        "input": {"task": "hello"},
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "dispatch_key": "dispatch-1",
        "authority": {"mode": "root_request", "request_id": "req-1"},
    }
    body.update(body_patch)

    response = test_client.post(f"/v3r1/projects/{PROJECT_ID}/turns:dispatch", json=body)

    assert response.status_code == 422
    assert message in response.text


def test_http_service_authorized_registration_birth_end_to_end(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "description": "External BYOA agent",
                "enabled": True,
                "accepts_work": True,
                "capacity": 2,
                "capabilities": [TURN_KIND_CONVERSATION_V1],
                "grants": ["turn.dispatch.any"],
                "public_metadata": {"ui": {"label": "BYOA Agent"}},
            },
            "provenance": {
                "kind": "test.registration.v1",
                "external_ref": "invite-001",
                "payload_hash": "sha256:registration-fixture",
            },
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["agent"] == {"project_id": PROJECT_ID, "agent_id": "byoa-agent-001"}
    assert body["role"] == "external.agent.v1"
    assert body["description"] == "External BYOA agent"
    assert body["capacity"] == 2
    assert body["capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert body["grants"] == ["turn.dispatch.any"]
    assert body["public_metadata"] == {"ui": {"label": "BYOA Agent"}}
    assert body["registered_by_agent_id"] == registrar.agent_id
    assert body["registration_provenance_kind"] == "test.registration.v1"
    assert body["registration_provenance_ref"] == "invite-001"
    assert body["registration_provenance_payload_hash"] == "sha256:registration-fixture"
    assert body["admitted_spec_hash"]

    snapshot = kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="byoa-agent-001"))
    assert snapshot is not None
    assert snapshot.registered_by_agent_id == registrar.agent_id


def test_http_client_service_authorized_registration_birth(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)
    client = HttpAgentClient(client=test_client, headers=_headers(registrar))

    snapshot = client.register_agent_by_service(
        project_id=PROJECT_ID,
        spec=AgentBirthSpec(
            agent_id="client-byoa-agent",
            role="external.agent.v1",
            capabilities=(TURN_KIND_CONVERSATION_V1,),
            grants=(),
            description="Client registered BYOA agent",
        ),
        provenance=AgentRegistrationProvenance(kind="test.registration.v1", external_ref="client-invite-001"),
    )

    assert snapshot.agent == AgentRef(project_id=PROJECT_ID, agent_id="client-byoa-agent")
    assert snapshot.registered_by_agent_id == registrar.agent_id
    assert snapshot.registration_provenance_ref == "client-invite-001"


def test_http_service_authorized_registration_canonicalizes_grants_and_capabilities(test_client, kernel_app) -> None:
    assert agent_birth_spec_hash(
        AgentBirthSpec(
            agent_id="same-agent",
            role="external.agent.v1",
            capabilities=("z.capability", TURN_KIND_CONVERSATION_V1),
            grants=("turn.dispatch.any", "agent.registration.birth"),
        )
    ) == agent_birth_spec_hash(
        AgentBirthSpec(
            agent_id="same-agent",
            role="external.agent.v1",
            capabilities=(TURN_KIND_CONVERSATION_V1, "z.capability", "z.capability"),
            grants=("agent.registration.birth", "turn.dispatch.any", "turn.dispatch.any"),
        )
    )

    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": "canonical-agent",
                "role": "external.agent.v1",
                "capabilities": ["z.capability", TURN_KIND_CONVERSATION_V1, "z.capability"],
                "grants": ["turn.dispatch.any", "agent.registration.birth", "turn.dispatch.any"],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["capabilities"] == [TURN_KIND_CONVERSATION_V1, "z.capability"]
    assert body["grants"] == ["agent.registration.birth", "turn.dispatch.any"]

    response_equivalent = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": "canonical-agent-equivalent",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1, "z.capability"],
                "grants": ["agent.registration.birth", "turn.dispatch.any"],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-002"},
        },
    )
    assert response_equivalent.status_code == 200, response_equivalent.text
    assert response_equivalent.json()["admitted_spec_hash"] != body["admitted_spec_hash"]


def test_http_service_authorized_registration_requires_grant(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "caller missing grant: agent.registration.birth"


def test_http_service_authorized_registration_rejects_unregistered_caller(test_client) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=missing_credential_headers(registrar),
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 401
    assert response.json()["message"] == "agent credential not found"


def test_http_service_authorized_registration_rejects_cross_project_caller(test_client, kernel_app) -> None:
    caller = AgentRef(project_id="other", agent_id="registration-service")
    _seed_agent(kernel_app, caller)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(caller),
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "caller project must match path project"


def test_http_service_authorized_registration_rejects_body_authority_fields(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "registered_by": {"project_id": PROJECT_ID, "agent_id": "someone-else"},
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("spec_patch", "message"),
    [
        ({"capacity": 0}, "agent birth spec capacity must be positive"),
        ({"capabilities": [""]}, "agent birth spec capabilities must contain non-empty strings"),
        ({"grants": [""]}, "agent birth spec grants must contain non-empty strings"),
    ],
)
def test_http_service_authorized_registration_rejects_invalid_spec(test_client, kernel_app, spec_patch, message) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)

    spec = {
        "agent_id": "byoa-agent-001",
        "role": "external.agent.v1",
        "capabilities": [TURN_KIND_CONVERSATION_V1],
    }
    spec.update(spec_patch)
    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": spec,
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == message
    assert kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="byoa-agent-001")) is None


def test_http_service_authorized_registration_rejects_empty_payload_hash(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001", "payload_hash": ""},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "registration provenance payload_hash must be non-empty when provided"


def test_http_service_authorized_registration_does_not_require_credential_config(kernel_app, test_pg_dsn) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )
    with TestClient(app) as client:
        response = client.post(
            f"/v3r1/projects/{PROJECT_ID}/agents:register",
            headers=_headers(registrar),
            json={
                "spec": {
                    "agent_id": "byoa-agent-001",
                    "role": "external.agent.v1",
                    "capabilities": [TURN_KIND_CONVERSATION_V1],
                },
                "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
            },
        )

    assert response.status_code == 200, response.text


def test_http_service_authorized_registration_is_birth_only(test_client, kernel_app) -> None:
    registrar = AgentRef(project_id=PROJECT_ID, agent_id="registration-service")
    existing = AgentRef(project_id=PROJECT_ID, agent_id="existing-agent")
    _seed_agent(kernel_app, registrar, grants=(AGENT_REGISTRATION_BIRTH_GRANT,), accepts_work=False)
    _seed_agent(kernel_app, existing, capabilities=(TURN_KIND_CONVERSATION_V1,))

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(registrar),
        json={
            "spec": {
                "agent_id": existing.agent_id,
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "agent already exists: existing-agent"


def test_http_service_authorized_registration_requires_agent_credential(test_client) -> None:
    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        json={
            "spec": {
                "agent_id": "byoa-agent-001",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-001"},
        },
    )

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"


def test_topology_register_agent_does_not_override_existing_description(kernel_app) -> None:
    agent = AgentRef(project_id=PROJECT_ID, agent_id="seeded")

    kernel_app.topology.register_agent(
        agent,
        description="Initial description",
        capabilities=(TURN_KIND_CONVERSATION_V1,),
    )
    kernel_app.topology.register_agent(
        agent,
        description="Overridden description",
        capabilities=(TURN_KIND_CONVERSATION_V1,),
    )

    snapshot = kernel_app.topology.get_agent(agent)

    assert snapshot is not None
    assert snapshot.description == "Initial description"


def test_http_get_agent_returns_empty_public_metadata_by_default(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    client = _client(test_client, NANOBOT)

    snapshot = client.get_agent(NANOBOT)

    assert snapshot is not None
    assert snapshot.public_metadata == {}


def _work_memory_manifest(request_id: str = "wm-001") -> dict:
    return {
        "kind": "agent_work_memory_report_manifest.v1",
        "request_id": request_id,
        "summary": "Deployment debugging lessons",
        "records": [
            {
                "role": "local_experience_summary",
                "payload": {"summary": "Regenerate config before worker restart."},
                "source_refs": [{"kind": "runtime_local_trace", "uri": "external://trace/deploy-1"}],
            }
        ],
        "final_payload": {
            "kind": "agent_work_memory_report_result.v1",
            "summary": "Report submitted.",
        },
    }


def test_http_work_memory_report_submission_creates_born_closed_turn_without_claim_or_capability(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter")
    _seed_agent(kernel_app, reporter, capabilities=(), accepts_work=False)
    client = HttpAgentClient(client=test_client, headers=_headers(reporter))

    result = client.submit_work_memory_report(reporter, _work_memory_manifest())

    assert result.status == "submitted"
    assert len(result.record_refs) == 1
    assert result.final_payload == {"kind": "agent_work_memory_report_result.v1", "summary": "Report submitted."}
    snapshot = client.get_turn(result.turn)
    assert snapshot.turn == result.turn
    assert snapshot.target_agent == reporter
    assert snapshot.turn_kind == TURN_KIND_WORK_MEMORY_REPORT_V1
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.SUCCEEDED
    assert snapshot.current_claim_agent_id is None
    assert snapshot.claim_expires_at is None
    assert snapshot.final_record_role == WORK_MEMORY_REPORT_RESULT_ROLE
    assert snapshot.final_payload == result.final_payload

    context = client.fetch_context(result.turn)
    assert [item.record.record_role for item in context.semantic_items] == [
        WORK_MEMORY_REPORT_MANIFEST_ROLE,
        "local_experience_summary",
    ]
    assert context.semantic_items[1].content.payload() == {
        "kind": "work_memory_report_record.v1",
        "role": "local_experience_summary",
        "payload": {"summary": "Regenerate config before worker restart."},
        "source_refs": [{"kind": "runtime_local_trace", "uri": "external://trace/deploy-1"}],
    }
    feed = client.fetch_turn_feed(result.turn)
    progress_events = [item for item in feed.items if item.event_type == "turn.progress_appended"]
    assert any(item.annotations.get("record_role") == "local_experience_summary" for item in progress_events)
    assert all(item.annotations.get("bootstrap") is not True for item in progress_events)
    assert any(item.annotations.get("submission_kind") == WORK_MEMORY_REPORT_SUBMISSION_KIND_V1 for item in progress_events)


def test_http_work_memory_report_submission_rejects_top_level_meta_without_writes(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-meta-rejected")
    _seed_agent(kernel_app, reporter, capabilities=(), accepts_work=False)
    before_turns = kernel_app.debug.list_turn_rows()
    before_events = kernel_app.debug.ledger_events
    manifest = _work_memory_manifest("wm-meta-rejected")
    manifest["meta"] = {"note": "spoofed note", "annotations": {"audit": "spoofed"}}

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{reporter.agent_id}/work-memory-reports",
        headers=_headers(reporter),
        json=manifest,
    )

    assert response.status_code == 422
    assert "meta" in response.text
    assert kernel_app.debug.list_turn_rows() == before_turns
    assert kernel_app.debug.ledger_events == before_events


def test_http_client_work_memory_report_rejects_meta_before_http_body() -> None:
    class FailingClient:
        def post(self, *args, **kwargs):
            raise AssertionError("HTTP request should not be sent")

    client = HttpAgentClient(client=FailingClient())
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-client-meta")
    manifest = _work_memory_manifest("wm-client-meta")
    manifest["meta"] = {"note": "prompt-facing metadata"}

    with pytest.raises(ValueError, match="work-memory manifest must not include meta"):
        client.submit_work_memory_report(reporter, manifest)

    with pytest.raises(ValueError, match="does not accept operation metadata"):
        client.submit_work_memory_report(
            reporter,
            _work_memory_manifest("wm-client-operation-meta"),
            meta=OperationMeta(note="trusted note"),
        )


def test_http_work_memory_report_submission_is_idempotent(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-idempotent")
    _seed_agent(kernel_app, reporter)
    client = HttpAgentClient(client=test_client, headers=_headers(reporter))

    first = client.submit_work_memory_report(reporter, _work_memory_manifest("wm-idempotent"))
    second = client.submit_work_memory_report(reporter, _work_memory_manifest("wm-idempotent"))

    assert first.status == "submitted"
    assert second.status == "already_submitted"
    assert second.turn == first.turn
    assert second.record_refs == first.record_refs


def test_http_work_memory_report_submission_rejects_same_request_id_conflict(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-conflict")
    _seed_agent(kernel_app, reporter)
    client = HttpAgentClient(client=test_client, headers=_headers(reporter))

    client.submit_work_memory_report(reporter, _work_memory_manifest("wm-conflict"))
    changed = _work_memory_manifest("wm-conflict")
    changed["records"][0]["payload"] = {"summary": "Different local fact."}

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{reporter.agent_id}/work-memory-reports",
        headers=_headers(reporter),
        json=changed,
    )
    assert response.status_code == 409
    assert "cardbox write_key conflict" in response.json()["message"]


def test_truth_work_memory_report_idempotency_rejects_changed_low_level_records(kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-truth-conflict")
    _seed_agent(kernel_app, reporter)
    manifest_ref = kernel_app.cardbox.create_payload_box(
        PROJECT_ID,
        {"kind": "agent_work_memory_report_manifest.v1", "request_id": "wm-truth-conflict"},
        write_key="wm-truth-conflict:manifest",
    )
    first_record_ref = kernel_app.cardbox.create_payload_box(
        PROJECT_ID,
        {"kind": "work_memory_report_record.v1", "payload": {"summary": "first"}},
        write_key="wm-truth-conflict:record:first",
    )
    second_record_ref = kernel_app.cardbox.create_payload_box(
        PROJECT_ID,
        {"kind": "work_memory_report_record.v1", "payload": {"summary": "second"}},
        write_key="wm-truth-conflict:record:second",
    )

    kernel_app.debug.truth.submit_work_memory_report_primitive(
        spec=WorkMemoryReportWriteSpec(
            actor=reporter,
            request_id="wm-truth-conflict",
            manifest_ref=manifest_ref,
            records=(WorkMemoryReportRecordWriteSpec(record_role="local_experience_summary", cardbox_ref=first_record_ref),),
        ),
        meta=None,
    )

    with pytest.raises(ConflictError, match="record 1 payload differs"):
        kernel_app.debug.truth.submit_work_memory_report_primitive(
            spec=WorkMemoryReportWriteSpec(
                actor=reporter,
                request_id="wm-truth-conflict",
                manifest_ref=manifest_ref,
                records=(
                    WorkMemoryReportRecordWriteSpec(
                        record_role="local_experience_summary",
                        cardbox_ref=second_record_ref,
                    ),
                ),
            ),
            meta=None,
        )


def test_http_work_memory_report_submission_rejects_impersonation_and_manifest_actor_mismatch(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporter-auth")
    other = AgentRef(project_id=PROJECT_ID, agent_id="other-agent")
    _seed_agent(kernel_app, reporter)
    _seed_agent(kernel_app, other)

    impersonation = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{reporter.agent_id}/work-memory-reports",
        headers=_headers(other),
        json=_work_memory_manifest("wm-impersonation"),
    )
    assert impersonation.status_code == 403
    assert impersonation.json()["message"] == "authenticated caller identity does not match request body"

    manifest = _work_memory_manifest("wm-declared-mismatch")
    manifest["declared_agent_id"] = other.agent_id
    declared_mismatch = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{reporter.agent_id}/work-memory-reports",
        headers=_headers(reporter),
        json=manifest,
    )
    assert declared_mismatch.status_code == 409
    assert declared_mismatch.json()["message"] == "manifest declared_agent_id must match trusted actor"


def test_http_work_memory_report_submission_rejects_disabled_agent_and_top_level_authority_material(test_client, kernel_app, test_pg_dsn: str) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="disabled-reporter")
    _seed_agent(kernel_app, reporter)
    disabled_headers = _headers(reporter)
    _disable_agent(test_pg_dsn, reporter)

    disabled = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{reporter.agent_id}/work-memory-reports",
        headers=disabled_headers,
        json=_work_memory_manifest("wm-disabled"),
    )
    assert disabled.status_code == 403
    assert disabled.json()["message"] == "authenticated agent is disabled"

    enabled = AgentRef(project_id=PROJECT_ID, agent_id="enabled-reporter")
    _seed_agent(kernel_app, enabled)
    forbidden = _work_memory_manifest("wm-forbidden")
    forbidden["claim"] = {"token": "raw"}
    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{enabled.agent_id}/work-memory-reports",
        headers=_headers(enabled),
        json=forbidden,
    )
    assert response.status_code == 422


def test_http_work_memory_report_submission_rejects_unregistered_actor(test_client) -> None:
    unknown = AgentRef(project_id=PROJECT_ID, agent_id="unknown-reporter")

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{unknown.agent_id}/work-memory-reports",
        headers=missing_credential_headers(unknown),
        json=_work_memory_manifest("wm-unknown"),
    )

    assert response.status_code == 401
    assert response.json()["message"] == "agent credential not found"


def test_http_work_memory_report_can_be_consumed_by_same_project_agent(test_client, kernel_app) -> None:
    reporter = AgentRef(project_id=PROJECT_ID, agent_id="reporting-agent")
    consumer = AgentRef(project_id=PROJECT_ID, agent_id="consumer-agent")
    _seed_agent(kernel_app, reporter, accepts_work=False)
    _seed_agent(kernel_app, consumer, capabilities=(TURN_KIND_CONVERSATION_V1,))
    reporter_client = HttpAgentClient(client=test_client, headers=_headers(reporter))
    consumer_client = HttpAgentClient(client=test_client, headers=_headers(consumer))

    report = reporter_client.submit_work_memory_report(reporter, _work_memory_manifest("wm-consume"))

    report_context = consumer_client.fetch_context(report.turn)
    assert [item.record.record_role for item in report_context.semantic_items] == [
        WORK_MEMORY_REPORT_MANIFEST_ROLE,
        "local_experience_summary",
    ]

    report_refs = [{"project_id": ref.project_id, "record_id": ref.record_id} for ref in report.record_refs]
    consumer_turn = _dispatch_root(
        consumer_client,
        requested_by=consumer,
        target_agent=consumer,
        request_payload={
            "task": "use shared work memory",
            "source_turn": {"project_id": report.turn.project_id, "turn_id": report.turn.turn_id},
            "source_records": report_refs,
        },
        request_id="wm-consumer-turn",
    )
    claimed = consumer_client.claim_turn(consumer)
    assert claimed is not None
    assert claimed.claim.turn_ref() == consumer_turn
    assert claimed.context.semantic_items[0].content.payload()["source_records"] == report_refs

    consumer_client.finish_turn(
        claimed.claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={
            "kind": "consumer_work_memory_use.v1",
            "used_turn": {"project_id": report.turn.project_id, "turn_id": report.turn.turn_id},
            "used_records": report_refs,
        },
    )

    snapshot = consumer_client.get_turn(consumer_turn)
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.final_payload["used_records"] == report_refs


def test_http_update_agent_public_metadata_rejects_unsupported_available_roles(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    client = _client(test_client, NANOBOT)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.update_agent_public_metadata(
            NANOBOT,
            public_metadata={
                "provision": {
                    "available_roles": [
                        {"role": "nanobot.leaf.conversation.v1", "description": "Unsupported leaf"}
                    ]
                }
            },
        )

    assert exc_info.value.response.status_code == 409
    assert "public_metadata.provision.available_roles is unsupported" in str(exc_info.value)


def test_http_update_agent_public_metadata_is_full_replace(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1))
    client = _client(test_client, NANOBOT)

    client.update_agent_public_metadata(
        NANOBOT,
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Conversation",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": []},
                    "variants": {},
                }
            ],
            "ui": {"label": "NanoBot"},
        },
    )
    client.update_agent_public_metadata(
        NANOBOT,
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                    "purpose": "Provision",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": ["agent.role"]},
                    "variants": {"roles": [{"role": "nanobot.leaf.reviewer.v1", "description": None}]},
                }
            ]
        },
    )

    snapshot = client.get_agent(NANOBOT)

    assert snapshot is not None
    assert snapshot.public_metadata == {
        "turn_offers": [
            {
                "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                "purpose": "Provision",
                "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                "input_contract": {"required_fields": ["agent.role"]},
                "variants": {"roles": [{"role": "nanobot.leaf.reviewer.v1", "description": None}]},
                "notes": None,
            }
        ]
    }


def test_http_update_agent_public_metadata_rejects_missing_agent(test_client) -> None:
    client = HttpAgentClient(client=test_client, headers=missing_credential_headers(NANOBOT))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.update_agent_public_metadata(
            NANOBOT,
            public_metadata={
                "turn_offers": [
                    {
                        "turn_kind": TURN_KIND_CONVERSATION_V1,
                        "purpose": "Conversation",
                        "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                        "input_contract": {"required_fields": []},
                        "variants": {},
                    }
                ]
            },
        )

    assert exc_info.value.response.status_code == 401


def test_http_update_agent_public_metadata_requires_object_payload(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))

    response = test_client.put(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}/public-metadata",
        json={
            "agent": {"project_id": PROJECT_ID, "agent_id": NANOBOT.agent_id},
            "public_metadata": ["not-an-object"],
        },
    )

    assert response.status_code == 422


def test_topology_update_agent_public_metadata_rejects_non_json_nested_values(kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))

    with pytest.raises(ConflictError):
        kernel_app.topology.update_agent_public_metadata(
            NANOBOT,
            {
                "turn_offers": [
                    {
                        "turn_kind": TURN_KIND_CONVERSATION_V1,
                        "purpose": "Conversation",
                        "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                        "input_contract": {"required_fields": []},
                        "variants": {"bad": object()},
                    }
                ]
            },
        )


def test_http_update_agent_public_metadata_accepts_turn_offers_with_richer_role_variants(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,))
    client = _client(test_client, NANOBOT)

    client.update_agent_public_metadata(
        NANOBOT,
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                    "purpose": "Provision workers",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": ["agent.role"]},
                    "variants": {
                        "roles": [
                            {
                                "role": "nanobot.leaf.conversation.v1",
                                "description": "Leaf agent for delegated conversation turns.",
                                "tags": ["conversation", "leaf"],
                            }
                        ]
                    },
                }
            ]
        },
    )

    snapshot = client.get_agent(NANOBOT)

    assert snapshot is not None
    roles = snapshot.public_metadata["turn_offers"][0]["variants"]["roles"]
    assert roles == [
        {
            "role": "nanobot.leaf.conversation.v1",
            "description": "Leaf agent for delegated conversation turns.",
            "tags": ["conversation", "leaf"],
        }
    ]


def test_http_update_agent_public_metadata_accepts_turn_offers(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1))
    client = _client(test_client, NANOBOT)

    client.update_agent_public_metadata(
        NANOBOT,
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Handle a general conversation turn.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": [{"mode": "root_request"}, {"mode": "child_derivation"}],
                    },
                    "input_contract": {"required_fields": [], "example_payload": {"task": "hello"}},
                    "variants": {},
                },
                {
                    "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                    "purpose": "Provision a new agent.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": [{"mode": "root_request"}, {"mode": "child_derivation"}],
                    },
                    "input_contract": {
                        "required_fields": ["agent.role"],
                        "example_payload": {"agent": {"role": LEAF_ROLE}},
                    },
                    "variants": {"roles": [{"role": LEAF_ROLE, "description": "Leaf"}]},
                },
            ]
        },
    )

    snapshot = client.get_agent(NANOBOT)

    assert snapshot is not None
    assert snapshot.public_metadata["turn_offers"][0]["turn_kind"] == TURN_KIND_CONVERSATION_V1
    assert snapshot.public_metadata["turn_offers"][1]["turn_kind"] == TURN_KIND_PROVISION_AGENT_SPAWN_V1


def test_http_update_agent_public_metadata_rejects_invalid_turn_offer_operation(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    client = _client(test_client, NANOBOT)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.update_agent_public_metadata(
            NANOBOT,
            public_metadata={
                "turn_offers": [
                    {
                        "turn_kind": TURN_KIND_CONVERSATION_V1,
                        "purpose": "Bad offer",
                        "calling": {
                            "operation": "submit",
                            "authority_modes": [{"mode": "root_request"}],
                        },
                        "input_contract": {},
                    }
                ]
            },
        )

    assert exc_info.value.response.status_code == 409
    assert "turn_offers[].calling.operation must be 'dispatch'" in str(exc_info.value)


def test_http_agent_flow_end_to_end(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    nanobot_snapshot = nanobot_client.get_agent(NANOBOT)
    assert nanobot_snapshot is not None
    assert nanobot_snapshot.agent == NANOBOT
    assert nanobot_snapshot.accepts_work is True

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "write progress"},
        request_id="http-req-1",
    )

    claimed = nanobot_client.claim_turn(NANOBOT)
    assert claimed is not None
    assert claimed.claim.turn_ref() == turn
    assert claimed.context.semantic_items[0].content.payload() == {"task": "write progress"}

    nanobot_client.renew_claim(claimed.claim)
    nanobot_client.append_record(claimed.claim, {"phase": "running"})
    nanobot_client.finish_turn(
        claimed.claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={"result": "ok"},
    )

    snapshot = nanobot_client.get_turn(turn)
    assert snapshot.turn_kind == TURN_KIND_CONVERSATION_V1
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.SUCCEEDED

    turn_feed = nanobot_client.fetch_turn_feed(turn)
    agent_feed = nanobot_client.fetch_agent_feed(NANOBOT)
    assert [event.event_type for event in turn_feed.items][-2:] == ["turn.progress_appended", "turn.finished"]
    assert [event.event_type for event in agent_feed.items][-2:] == ["turn.progress_appended", "turn.finished"]


def test_http_claim_route_returns_handle_only(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "claim-only"},
        request_id="http-claim-handle-only",
    )

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{NANOBOT.agent_id}/claims:claim",
        headers=_headers(NANOBOT),
        json={"agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id}},
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"project_id", "turn_id", "agent_id", "token", "expires_at"}
    assert body["turn_id"] == turn.turn_id


def test_http_renew_claim_returns_timing_metadata(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "renew"},
        request_id="http-renew-claim",
    )
    claimed = nanobot_client.claim_turn(NANOBOT)
    assert claimed is not None

    before = nanobot_client.get_turn(claimed.claim.turn_ref())
    kernel_app.advance_time(seconds=5)
    renewed = nanobot_client.renew_claim(claimed.claim)
    after = nanobot_client.get_turn(claimed.claim.turn_ref())

    assert renewed.server_time <= renewed.expires_at
    assert renewed.recommended_interval_seconds > 0
    assert before.claim_expires_at is not None
    assert after.claim_expires_at == renewed.expires_at
    assert after.claim_expires_at > before.claim_expires_at


def test_http_suspend_body_reason_and_note_take_precedence_over_meta(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "suspend precedence"},
        request_id="http-suspend-meta-precedence",
    )
    claimed = nanobot_client.claim_turn(NANOBOT)
    assert claimed is not None

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns/{turn.turn_id}:suspend",
        headers=_headers(NANOBOT),
        json={
            "claim": to_jsonable(claimed.claim),
            "reason": "body_reason",
            "note": "body note",
            "meta": {
                "reason": "meta_reason",
                "note": "meta note",
                "annotations": {"record_role": "spoofed-role", "custom": "caller-value"},
            },
        },
    )

    assert response.status_code == 200
    feed = nanobot_client.fetch_turn_feed(turn)
    suspended = next(event for event in feed.items if event.event_type == "turn.suspended")
    assert suspended.note is None
    assert suspended.annotations["caller"]["reason"] == "body_reason"
    assert suspended.annotations["caller"]["note"] == "body note"
    assert suspended.annotations["caller"]["annotations"] == {
        "record_role": "spoofed-role",
        "custom": "caller-value",
    }
    assert suspended.annotations["caller"]["reserved_annotation_keys"] == ["record_role"]
    assert "record_role" not in suspended.annotations


def test_http_stop_request_flow(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    _seed_agent(kernel_app, OPERATOR, grants=("turn.stop.any",))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)
    operator_client = _client(test_client, OPERATOR)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "stop me"},
        request_id="http-stop-1",
    )
    claimed = nanobot_client.claim_turn(NANOBOT)
    assert claimed is not None

    operator_client.request_stop_turn(turn, requested_by=OPERATOR, reason="operator_stop", note="manual stop")
    stopped_snapshot = nanobot_client.get_turn(turn)
    assert stopped_snapshot.stop_requested is True

    nanobot_client.finish_turn(claimed.claim, outcome=TurnOutcome.STOPPED)

    closed_snapshot = nanobot_client.get_turn(turn)
    assert closed_snapshot.state == TurnState.CLOSED
    assert closed_snapshot.outcome == TurnOutcome.STOPPED

    feed = nanobot_client.fetch_turn_feed(turn)
    assert any(event.event_type == "turn.stop_requested" for event in feed.items)
    assert feed.items[-1].event_type == "turn.finished"


def test_http_drain_and_resume_agent_toggle_accepts_work(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=True)
    client = _client(test_client, NANOBOT)

    client.drain_agent(NANOBOT)

    snapshot = client.get_agent(NANOBOT)
    assert snapshot is not None
    assert snapshot.accepts_work is False
    assert snapshot.last_seen_at is None

    client.resume_agent(NANOBOT)

    snapshot = client.get_agent(NANOBOT)
    assert snapshot is not None
    assert snapshot.accepts_work is True
    assert snapshot.last_seen_at is None


def test_http_update_agent_presence_only_updates_last_seen(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=False)
    client = _client(test_client, NANOBOT)

    before = client.get_agent(NANOBOT)
    assert before is not None
    assert before.last_seen_at is None

    client.heartbeat_agent_presence(NANOBOT)

    after = client.get_agent(NANOBOT)
    assert after is not None
    assert after.accepts_work is False
    assert after.last_seen_at is not None
    assert after.updated_at == before.updated_at


def test_http_update_agent_presence_does_not_change_claim_eligibility(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, accepts_work=False, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "presence-only"},
        request_id="http-presence-eligibility",
    )
    nanobot_client.heartbeat_agent_presence(NANOBOT)

    assert nanobot_client.claim_turn(NANOBOT) is None


def test_http_drain_blocks_new_claims_until_resume(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, accepts_work=True, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "drain-me"},
        request_id="http-drain-claim-gating",
    )

    nanobot_client.drain_agent(NANOBOT)
    assert nanobot_client.claim_turn(NANOBOT) is None

    nanobot_client.resume_agent(NANOBOT)
    claimed = nanobot_client.claim_turn(NANOBOT)

    assert claimed is not None
    assert claimed.claim.turn_ref() == turn


def test_http_drain_does_not_fence_active_claim(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, accepts_work=True, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "keep-running"},
        request_id="http-drain-active-claim",
    )
    claimed = nanobot_client.claim_turn(NANOBOT)
    assert claimed is not None

    nanobot_client.drain_agent(NANOBOT)

    nanobot_client.append_record(claimed.claim, {"status": "still-running"})
    nanobot_client.finish_turn(claimed.claim, outcome=TurnOutcome.SUCCEEDED)

    turn_snapshot = nanobot_client.get_turn(turn)
    assert turn_snapshot.state == TurnState.CLOSED
    assert turn_snapshot.outcome == TurnOutcome.SUCCEEDED

    agent_snapshot = nanobot_client.get_agent(NANOBOT)
    assert agent_snapshot is not None
    assert agent_snapshot.accepts_work is False


def test_http_drain_and_resume_emit_agent_feed_events(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=True)
    client = _client(test_client, NANOBOT)

    client.drain_agent(NANOBOT)
    client.resume_agent(NANOBOT)

    feed = client.fetch_agent_feed(NANOBOT)
    assert [event.event_type for event in feed.items][-2:] == ["agent.drained", "agent.resumed"]
    assert [event.actor_id for event in feed.items][-2:] == [NANOBOT.agent_id, NANOBOT.agent_id]


def test_http_drain_rejects_unauthorized_override(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=True)
    _seed_agent(kernel_app, OPERATOR, accepts_work=True)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}:drain",
        headers=_headers(OPERATOR),
        json={
            "agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "requested_by": {"project_id": OPERATOR.project_id, "agent_id": OPERATOR.agent_id},
        },
    )

    assert response.status_code == 409


def test_http_drain_and_resume_allow_granted_override(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=True)
    _seed_agent(kernel_app, OPERATOR, accepts_work=True, grants=(AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT,))
    client = _client(test_client, NANOBOT)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}:drain",
        headers=_headers(OPERATOR),
        json={
            "agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "requested_by": {"project_id": OPERATOR.project_id, "agent_id": OPERATOR.agent_id},
            "meta": {"reason": "operator_drain", "note": "maintenance"},
        },
    )
    assert response.status_code == 200

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}:resume",
        headers=_headers(OPERATOR),
        json={
            "agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "requested_by": {"project_id": OPERATOR.project_id, "agent_id": OPERATOR.agent_id},
            "meta": {"reason": "operator_resume", "note": "back_online"},
        },
    )
    assert response.status_code == 200

    feed = client.fetch_agent_feed(NANOBOT)
    assert [event.event_type for event in feed.items][-2:] == ["agent.drained", "agent.resumed"]
    assert [event.actor_id for event in feed.items][-2:] == [OPERATOR.agent_id, OPERATOR.agent_id]
    assert [event.note for event in feed.items][-2:] == [None, None]
    assert feed.items[-2].annotations["accepts_work"] is False
    assert feed.items[-1].annotations["accepts_work"] is True
    assert feed.items[-2].annotations["caller"]["note"] == "maintenance"
    assert feed.items[-2].annotations["caller"]["reason"] == "operator_drain"
    assert feed.items[-1].annotations["caller"]["note"] == "back_online"
    assert feed.items[-1].annotations["caller"]["reason"] == "operator_resume"


def test_http_drain_route_rejects_accepts_work_field(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}:drain",
        json={
            "agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "requested_by": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "accepts_work": True,
        },
    )

    assert response.status_code == 422


def test_http_resume_route_rejects_last_seen_field(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, NANOBOT, accepts_work=False)

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/management/agents/{NANOBOT.agent_id}:resume",
        json={
            "agent": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "requested_by": {"project_id": NANOBOT.project_id, "agent_id": NANOBOT.agent_id},
            "last_seen_at": "2026-04-10T15:00:00+00:00",
        },
    )

    assert response.status_code == 422


def test_http_dispatch_accepts_explicit_turn_kind(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=_provisioner_capabilities())
    client = _client(test_client, FRONTSIDE)

    turn = _dispatch_root(client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "provision"},
        request_id="http-provision-kind",
        turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    )

    snapshot = client.get_turn(turn)
    assert snapshot.turn_kind == TURN_KIND_PROVISION_AGENT_SPAWN_V1


def test_http_dispatch_rejects_missing_target_capability(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT)
    client = _client(test_client, FRONTSIDE)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        _dispatch_root(client,
            requested_by=FRONTSIDE,
            target_agent=NANOBOT,
            request_payload={"task": "provision"},
            request_id="http-missing-capability",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        )
    assert exc_info.value.response.status_code == 409


def test_http_stop_rejects_cross_project_same_agent_id(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    _seed_agent(kernel_app, OTHER_PROJECT_NANOBOT)
    frontside_client = _client(test_client, FRONTSIDE)
    other_client = _client(test_client, OTHER_PROJECT_NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "cross project stop"},
        request_id="http-stop-cross-project",
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        other_client.request_stop_turn(turn, requested_by=OTHER_PROJECT_NANOBOT)
    assert exc_info.value.response.status_code == 409


class _FinishHandler:
    def handle_turn(self, context, client, claim):
        assert context.turn.turn == claim.turn_ref()
        return FinishTurnAction(outcome=TurnOutcome.SUCCEEDED, final_payload={"result": "worker-finished"})


class _ExplodingHandler:
    def handle_turn(self, context, client, claim):
        raise RuntimeError("boom")


def test_polling_worker_finishes_turn(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "worker finish"},
        request_id="polling-finish-1",
    )

    worker = PollingWorker(client=nanobot_client, agent=NANOBOT, handler=_FinishHandler())
    result = worker.run_once()

    assert result.claimed_turn == turn
    assert result.action == "finished"
    snapshot = nanobot_client.get_turn(turn)
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.SUCCEEDED


def test_polling_worker_finishes_stopped_turn_before_handler(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "worker stop"},
        request_id="polling-stop-1",
    )
    nanobot_client.request_stop_turn(turn, requested_by=NANOBOT, reason="agent_stop")

    worker = PollingWorker(client=nanobot_client, agent=NANOBOT, handler=_FinishHandler())
    result = worker.run_once()

    assert result.claimed_turn == turn
    assert result.action == "stopped"
    snapshot = nanobot_client.get_turn(turn)
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.STOPPED


def test_polling_worker_suspends_on_handler_error(test_client, kernel_app) -> None:
    _seed_agent(kernel_app, FRONTSIDE)
    _seed_agent(kernel_app, NANOBOT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    frontside_client = _client(test_client, FRONTSIDE)
    nanobot_client = _client(test_client, NANOBOT)

    turn = _dispatch_root(frontside_client,
        requested_by=FRONTSIDE,
        target_agent=NANOBOT,
        request_payload={"task": "worker explode"},
        request_id="polling-error-1",
    )

    worker = PollingWorker(client=nanobot_client, agent=NANOBOT, handler=_ExplodingHandler())
    result = worker.run_once()

    assert result.claimed_turn == turn
    assert result.action == "safe_stop"
    snapshot = nanobot_client.get_turn(turn)
    assert snapshot.state == TurnState.SUSPENDED
