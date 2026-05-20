from __future__ import annotations

import pytest

from CommonGround.contracts import ForbiddenError
from CommonGround.service.auth import CallerIdentity
from CommonGround.service.read_policy import ReadAudience, ReadAuthorizationRequest, ReadSurfaceKind, ServiceReadPolicy


def test_service_read_policy_allows_same_project_project_shared() -> None:
    policy = ServiceReadPolicy()

    policy.authorize(
        ReadAuthorizationRequest(
            project_id="demo",
            surface_kind=ReadSurfaceKind.PROJECTION,
            resource_family="agent_directory",
            audience=ReadAudience.PROJECT_SHARED,
            caller=CallerIdentity(project_id="demo", agent_id="worker", credential_id="cred_test"),
        )
    )


def test_service_read_policy_rejects_cross_project_project_shared() -> None:
    policy = ServiceReadPolicy()

    with pytest.raises(ForbiddenError, match="authenticated caller project does not match read project"):
        policy.authorize(
            ReadAuthorizationRequest(
                project_id="demo",
                surface_kind=ReadSurfaceKind.PROJECTION,
                resource_family="agent_directory",
                audience=ReadAudience.PROJECT_SHARED,
                caller=CallerIdentity(project_id="other", agent_id="worker", credential_id="cred_test"),
            )
        )


def test_service_read_policy_records_resource_mapping() -> None:
    request = ReadAuthorizationRequest(
        project_id="demo",
        surface_kind=ReadSurfaceKind.TURN_INSPECT,
        resource_family="turn_feed",
        resource_id="T-1",
        audience=ReadAudience.PROJECT_SHARED,
        caller=CallerIdentity(project_id="demo", agent_id="worker", credential_id="cred_test"),
    )

    assert request.surface_kind is ReadSurfaceKind.TURN_INSPECT
    assert request.resource_family == "turn_feed"
    assert request.resource_id == "T-1"
