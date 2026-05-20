from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest

from CommonGround.contracts import AgentRef, ConflictError, TURN_KIND_CONVERSATION_V1
from Integrations.admin_service import (
    ADMIN_SERVICE_AGENT_ID,
    ADMIN_SERVICE_GRANTS,
    ADMIN_SERVICE_ROLE,
    CREATOR_USER_AUTHORITY_KIND,
    AdminServiceProjectBootstrap,
    ProjectCreationBootstrapRequest,
    ProjectCreatorAuthority,
    bootstrap_admin_service_project,
    bootstrap_project_admin_service_agent,
    create_project,
)
from tests.auth_support import agent_headers


PROJECT_ID = "admin-bootstrap"
OTHER_PROJECT_ID = "other-admin-bootstrap"


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def _request(project_id: str = PROJECT_ID, *, external_ref: str = "creator-001") -> ProjectCreationBootstrapRequest:
    return ProjectCreationBootstrapRequest(
        project_id=project_id,
        creator_authority=ProjectCreatorAuthority(
            kind="test.creator.v1",
            external_ref=external_ref,
            display_name="Test Creator",
        ),
    )


def test_create_project_seeds_project_scoped_admin_service(kernel_app) -> None:
    result = create_project(kernel_app, project_id=PROJECT_ID, creator_user_id="creator-user-001")

    assert result.created_admin_service is True
    assert result.creator_authority.kind == CREATOR_USER_AUTHORITY_KIND
    assert result.creator_authority.external_ref == "creator-user-001"
    assert result.admin_service_ref == AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)
    assert result.admin_service.role == ADMIN_SERVICE_ROLE
    assert result.admin_service.accepts_work is False
    assert result.admin_service.capabilities == ()
    assert result.admin_service.grants == ADMIN_SERVICE_GRANTS
    assert result.admin_service.public_metadata == {
        "service": {"kind": "admin_service", "version": "v1"},
    }


def test_create_project_rejects_malformed_existing_admin_service_without_rewrite(kernel_app) -> None:
    admin_service = AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)
    kernel_app.topology.register_agent(
        admin_service,
        role="custom.admin.service",
        capabilities=("custom.capability",),
        accepts_work=True,
        grants=(),
        enabled=False,
    )
    kernel_app.topology.update_agent_public_metadata(admin_service, {"custom": {"owner": "existing"}})
    before = kernel_app.topology.get_agent(admin_service)

    with pytest.raises(
        ConflictError,
        match="existing admin-service does not match bootstrap spec: .*role.*accepts_work.*grants.*public_metadata",
    ):
        AdminServiceProjectBootstrap(kernel_app).create_project(_request(external_ref="creator-002"))

    after = kernel_app.topology.get_agent(admin_service)

    assert after == before
    assert after is not None
    assert after.role == "custom.admin.service"
    assert after.accepts_work is True
    assert after.grants == ()
    assert after.public_metadata == {"custom": {"owner": "existing"}}


def test_repeated_bootstrap_returns_existing_admin_service(kernel_app) -> None:
    first = bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    second = bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")

    assert first.created_admin_service is True
    assert second.created_admin_service is False
    assert second.admin_service == first.admin_service
    assert second.admin_service_ref == first.admin_service_ref


def test_bootstrapped_admin_service_can_register_agent(test_client, kernel_app) -> None:
    result = bootstrap_admin_service_project(kernel_app, _request())

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(result.admin_service_ref),
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
    body = response.json()
    assert body["agent"] == {"project_id": PROJECT_ID, "agent_id": "byoa-agent-001"}
    assert body["registered_by_agent_id"] == ADMIN_SERVICE_AGENT_ID


def test_other_project_admin_service_cannot_register_into_this_project(test_client, kernel_app) -> None:
    bootstrap_admin_service_project(kernel_app, _request(PROJECT_ID, external_ref="creator-001"))
    other = bootstrap_admin_service_project(kernel_app, _request(OTHER_PROJECT_ID, external_ref="creator-002"))

    response = test_client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents:register",
        headers=_headers(other.admin_service_ref),
        json={
            "spec": {
                "agent_id": "cross-project-agent",
                "role": "external.agent.v1",
                "capabilities": [TURN_KIND_CONVERSATION_V1],
            },
            "provenance": {"kind": "test.registration.v1", "external_ref": "invite-002"},
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "caller project must match path project"
    assert kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="cross-project-agent")) is None


def test_creator_authority_does_not_enter_kernel_snapshot_or_metadata(kernel_app) -> None:
    request = _request(external_ref="creator-sensitive-ref")
    result = bootstrap_admin_service_project(kernel_app, request)

    snapshot = result.admin_service
    assert snapshot.public_metadata == {"service": {"kind": "admin_service", "version": "v1"}}
    assert "creator" not in str(snapshot.public_metadata).lower()
    assert snapshot.registered_by_agent_id is None
    assert snapshot.registration_provenance_kind is None
    assert snapshot.registration_provenance_ref is None
    assert snapshot.registration_provenance_payload_hash is None
    assert "creator_authority" not in {field.name for field in fields(type(snapshot))}


def test_creator_authority_immutability_is_reflected_in_code_and_docs() -> None:
    authority = ProjectCreatorAuthority(kind="test.creator.v1", external_ref="creator-001")

    with pytest.raises(FrozenInstanceError):
        authority.external_ref = "creator-002"

    assert "Immutable product-layer creator authority" in (ProjectCreatorAuthority.__doc__ or "")
    doc = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "en"
        / "guides"
        / "open-source-quickstart.md"
    ).read_text(encoding="utf-8")
    assert "creator authority" in doc.lower()
    assert "must not enter CommonGround Kernel truth" in doc
