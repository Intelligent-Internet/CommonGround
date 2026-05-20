from __future__ import annotations

from datetime import UTC, datetime, timedelta

from CommonGround.contracts import AgentRef, ClaimToken, TURN_KIND_CONVERSATION_V1
from CommonGround.service.serialization import to_jsonable
from CommonGround.service.write_guard import ServiceWriteGuard, WriteAuthorizationRequest, WriteSurfaceKind

from tests.auth_support import agent_headers
from tests.projection_support import dispatch_root_turn, register_agent


PROJECT_ID = "write-route-policy"
CALLER = AgentRef(project_id=PROJECT_ID, agent_id="caller")
WORKER = AgentRef(project_id=PROJECT_ID, agent_id="worker")


class RecordingWriteGuard(ServiceWriteGuard):
    def __init__(self) -> None:
        self.requests: list[WriteAuthorizationRequest] = []

    def authorize(self, request: WriteAuthorizationRequest) -> None:
        self.requests.append(request)
        super().authorize(request)


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def _fake_claim(turn_id: str = "missing-turn") -> ClaimToken:
    return ClaimToken(
        project_id=PROJECT_ID,
        turn_id=turn_id,
        agent_id=WORKER.agent_id,
        token="fake-token",
        expires_at=datetime.now(UTC) + timedelta(seconds=30),
    )


def test_v3r1_write_routes_map_to_service_write_guard(kernel_app, service_app, test_client) -> None:
    register_agent(kernel_app, CALLER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    register_agent(kernel_app, WORKER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    turn = dispatch_root_turn(kernel_app, requested_by=CALLER, target_agent=WORKER, request_id="write-route-policy")
    real_claim = kernel_app.lifecycle.claim_turn(WORKER)
    assert real_claim is not None
    fake_claim = _fake_claim()
    guard = RecordingWriteGuard()
    service_app.state.service_deps.write_guard = guard

    calls = [
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
            _headers(CALLER),
            {
                "requested_by": CALLER,
                "target_agent": WORKER,
                "input": {"task": "dispatch"},
                "turn_kind": TURN_KIND_CONVERSATION_V1,
                "dispatch_key": "write-route-policy-dispatch",
                "authority": {"mode": "root_request", "request_id": "write-route-policy-dispatch"},
            },
        ),
        ("post", f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/claims:claim", _headers(WORKER), {"agent": WORKER}),
        ("post", f"/v3r1/projects/{PROJECT_ID}/claims:renew", _headers(WORKER), {"claim": real_claim}),
        ("post", f"/v3r1/projects/{PROJECT_ID}/claims:reconcile-expired", _headers(WORKER), {}),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/turns/{fake_claim.turn_id}/semantic-records",
            _headers(WORKER),
            {"claim": fake_claim, "payload": {"progress": "mapped"}, "role": "progress"},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/turns/{fake_claim.turn_id}:suspend",
            _headers(WORKER),
            {"claim": fake_claim, "reason": "mapped"},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/turns/{turn.turn_id}:resume",
            _headers(CALLER),
            {"requested_by": CALLER, "note": "mapped"},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/turns/{fake_claim.turn_id}:finish",
            _headers(WORKER),
            {"claim": fake_claim, "outcome": "succeeded", "final_record_role": "deliverable"},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/management/agents/{WORKER.agent_id}:drain",
            _headers(WORKER),
            {"agent": WORKER, "requested_by": WORKER},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/management/agents/{WORKER.agent_id}:resume",
            _headers(WORKER),
            {"agent": WORKER, "requested_by": WORKER},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/management/agents/{WORKER.agent_id}:heartbeat-presence",
            _headers(WORKER),
            {"agent": WORKER},
        ),
        (
            "put",
            f"/v3r1/projects/{PROJECT_ID}/management/agents/{WORKER.agent_id}/public-metadata",
            _headers(WORKER),
            {"agent": WORKER, "public_metadata": {}},
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/agents:register",
            _headers(WORKER),
            {
                "spec": {
                    "agent_id": "new-service-agent",
                    "role": "external.agent.v1",
                    "capabilities": [TURN_KIND_CONVERSATION_V1],
                },
                "provenance": {"kind": "test.registration.v1", "external_ref": "fixture"},
            },
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/work-memory-reports",
            _headers(WORKER),
            {
                "kind": "agent_work_memory_report_manifest.v1",
                "request_id": "write-route-policy-report",
                "records": [{"role": "local_experience_summary", "payload": {"summary": "mapped"}}],
            },
        ),
        (
            "post",
            f"/v3r1/projects/{PROJECT_ID}/management/turns/{turn.turn_id}:stop",
            _headers(CALLER),
            {"turn": turn, "requested_by": CALLER},
        ),
    ]

    for method, route, headers, payload in calls:
        before_count = len(guard.requests)
        response = getattr(test_client, method)(route, headers=headers, json=to_jsonable(payload))
        assert response.status_code != 401, route
        assert response.status_code != 403, route
        assert len(guard.requests) == before_count + 1, route

    assert [
        (request.surface_kind, request.resource_family, request.operation)
        for request in guard.requests
    ] == [
        (WriteSurfaceKind.TURN_BIRTH, "turn", "dispatch"),
        (WriteSurfaceKind.CLAIM_ACQUIRE, "claim", "claim"),
        (WriteSurfaceKind.CLAIM_RENEWAL, "claim", "renew"),
        (WriteSurfaceKind.CLAIM_RECONCILE, "claim", "reconcile_expired"),
        (WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION, "semantic_record", "append"),
        (WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION, "turn_lifecycle", "suspend"),
        (WriteSurfaceKind.TURN_RESUME, "turn_lifecycle", "resume"),
        (WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION, "turn_lifecycle", "finish"),
        (WriteSurfaceKind.AGENT_OPERATIONAL_STATE, "agent_operational_state", "drain"),
        (WriteSurfaceKind.AGENT_OPERATIONAL_STATE, "agent_operational_state", "resume"),
        (WriteSurfaceKind.AGENT_PRESENCE, "agent_presence", "heartbeat_presence"),
        (WriteSurfaceKind.AGENT_PUBLIC_METADATA, "agent_public_metadata", "put"),
        (WriteSurfaceKind.AGENT_REGISTRATION_BIRTH, "agent_registration", "birth"),
        (WriteSurfaceKind.WORK_MEMORY_REPORT_SUBMISSION, "work_memory_report", "submit"),
        (WriteSurfaceKind.TURN_STOP_REQUEST, "turn_stop", "stop"),
    ]
