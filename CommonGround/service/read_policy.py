from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from fastapi import Request

from CommonGround.contracts import ForbiddenError

from .auth import CallerIdentity, require_caller_identity


class ReadSurfaceKind(StrEnum):
    TRUTH_SNAPSHOT = "truth_snapshot_read"
    TURN_INSPECT = "turn_inspect_read"
    PROJECTION = "projection_read"


class ReadAudience(StrEnum):
    PROJECT_SHARED = "project_shared"


@dataclass(frozen=True, slots=True)
class ReadAuthorizationRequest:
    project_id: str
    surface_kind: ReadSurfaceKind
    resource_family: str
    audience: ReadAudience
    caller: CallerIdentity
    resource_id: str | None = None


class ServiceReadPolicy:
    def authorize(self, request: ReadAuthorizationRequest) -> None:
        if request.caller.project_id != request.project_id:
            raise ForbiddenError("authenticated caller project does not match read project")
        if request.audience is ReadAudience.PROJECT_SHARED:
            return
        raise ForbiddenError(f"read audience not allowed: {request.audience.value}")


def authorize_read(
    request: Request,
    *,
    project_id: str,
    surface_kind: ReadSurfaceKind,
    resource_family: str,
    resource_id: str | None = None,
    audience: ReadAudience = ReadAudience.PROJECT_SHARED,
) -> ReadAuthorizationRequest:
    caller = require_caller_identity(request)
    read_request = ReadAuthorizationRequest(
        project_id=project_id,
        surface_kind=surface_kind,
        resource_family=resource_family,
        audience=audience,
        caller=caller,
        resource_id=resource_id,
    )
    request.app.state.service_deps.read_policy.authorize(read_request)
    return read_request
