from __future__ import annotations

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.service.read_policy import ReadAuthorizationRequest, ReadSurfaceKind, ServiceReadPolicy

from tests.auth_support import agent_headers
from tests.projection_support import dispatch_root_turn, register_agent


PROJECT_ID = "read-route-policy"
CALLER = AgentRef(project_id=PROJECT_ID, agent_id="caller")
WORKER = AgentRef(project_id=PROJECT_ID, agent_id="worker")


class RecordingReadPolicy(ServiceReadPolicy):
    def __init__(self) -> None:
        self.requests: list[ReadAuthorizationRequest] = []

    def authorize(self, request: ReadAuthorizationRequest) -> None:
        self.requests.append(request)
        super().authorize(request)


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def test_v3r1_read_routes_map_to_service_read_policy(kernel_app, service_app, test_client) -> None:
    register_agent(kernel_app, CALLER)
    register_agent(
        kernel_app,
        WORKER,
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Test worker",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": []},
                    "variants": {},
                }
            ]
        },
    )
    turn = dispatch_root_turn(kernel_app, requested_by=CALLER, target_agent=WORKER, request_id="read-route-policy")
    policy = RecordingReadPolicy()
    service_app.state.service_deps.read_policy = policy

    routes = [
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}",
        f"/v3r1/projects/{PROJECT_ID}/turns/{turn.turn_id}",
        f"/v3r1/projects/{PROJECT_ID}/turns/{turn.turn_id}/context",
        f"/v3r1/projects/{PROJECT_ID}/turns/{turn.turn_id}/feed",
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/feed",
        f"/v3r1/projects/{PROJECT_ID}/projection/agents",
        f"/v3r1/projects/{PROJECT_ID}/projection/turn-offers",
        f"/v3r1/projects/{PROJECT_ID}/projection/turns",
        f"/v3r1/projects/{PROJECT_ID}/projection/turns/{turn.turn_id}/lineage",
        f"/v3r1/projects/{PROJECT_ID}/projection/feed",
    ]
    for route in routes:
        response = test_client.get(route, headers=_headers(CALLER))
        assert response.status_code == 200, route

    assert [
        (request.surface_kind, request.resource_family, request.resource_id)
        for request in policy.requests
    ] == [
        (ReadSurfaceKind.TRUTH_SNAPSHOT, "agent_snapshot", WORKER.agent_id),
        (ReadSurfaceKind.TRUTH_SNAPSHOT, "turn_snapshot", turn.turn_id),
        (ReadSurfaceKind.TURN_INSPECT, "turn_context", turn.turn_id),
        (ReadSurfaceKind.TURN_INSPECT, "turn_feed", turn.turn_id),
        (ReadSurfaceKind.PROJECTION, "agent_feed", WORKER.agent_id),
        (ReadSurfaceKind.PROJECTION, "agent_directory", None),
        (ReadSurfaceKind.PROJECTION, "turn_offer_entries", None),
        (ReadSurfaceKind.PROJECTION, "turn_entries", None),
        (ReadSurfaceKind.PROJECTION, "turn_lineage", turn.turn_id),
        (ReadSurfaceKind.PROJECTION, "project_feed", None),
    ]
