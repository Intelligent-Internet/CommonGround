from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from CommonGround.agent_client import HttpAgentClient, agent_auth_headers
from CommonGround.agent_registration import (
    AgentRegistrationProvenance,
    agent_birth_spec_hash,
    canonical_agent_birth_spec,
)
from CommonGround.contracts import AgentRef, AgentSnapshot, ConflictError

from .byoa_workflow import (
    ByoaRegistrationRequest,
    ByoaWorkflowStore,
    agent_birth_spec_from_admitted_spec,
)
from .project_bootstrap import ADMIN_SERVICE_AGENT_ID


class ByoaRegistrationProcessor:
    """Execute approved BYOA registration requests through the CG service route."""

    def __init__(
        self,
        store: ByoaWorkflowStore,
        agent_client_factory_or_client: Any | None = None,
        *,
        base_url: str = "http://127.0.0.1:8000",
        client: Any | None = None,
        admin_service_agent_id: str = ADMIN_SERVICE_AGENT_ID,
        admin_service_token: str | None = None,
    ) -> None:
        self._store = store
        self._base_url = base_url
        self._admin_service_agent_id = admin_service_agent_id
        self._admin_service_token = admin_service_token
        self._agent_client_factory: Callable[..., Any] | None = None
        self._agent_client: Any | None = None
        self._http_client = client

        if agent_client_factory_or_client is not None:
            if _looks_like_agent_client(agent_client_factory_or_client):
                if client is not None:
                    raise ValueError("pass either a configured agent client or client, not both")
                self._agent_client = agent_client_factory_or_client
            elif callable(agent_client_factory_or_client):
                self._agent_client_factory = agent_client_factory_or_client
            else:
                if client is not None:
                    raise ValueError("pass either a raw HTTP client or client, not both")
                self._http_client = agent_client_factory_or_client

    @property
    def has_preconfigured_agent_client(self) -> bool:
        return self._agent_client is not None

    @property
    def admin_service_token(self) -> str | None:
        return self._admin_service_token

    def process_registration(self, request_id: str) -> ByoaRegistrationRequest:
        row = self._store.mark_registering(
            request_id,
            actor_id=self._admin_service_agent_id,
            details={"processor": "byoa_registration"},
        )
        return self._execute_registration(row)

    def process_next_registration(self) -> ByoaRegistrationRequest | None:
        row = self._store.claim_next_approved_for_registration(
            actor_id=self._admin_service_agent_id,
            details={"processor": "byoa_registration"},
        )
        if row is None:
            return None
        return self._execute_registration(row)

    def issue_agent_credential_for_registration(
        self,
        row: ByoaRegistrationRequest,
        *,
        provenance_kind: str,
        provenance_ref: str | None,
        provenance_payload_hash: str | None,
    ) -> Mapping[str, Any]:
        if row.registered_agent_id is None:
            raise ConflictError("BYOA registration must be registered before credential issue")
        client_handle: _AgentClientHandle | None = None
        try:
            client_handle = self._make_agent_client(row)
            return client_handle.client.issue_agent_credential(
                AgentRef(project_id=row.project_id, agent_id=row.registered_agent_id),
                provenance_kind=provenance_kind,
                provenance_ref=provenance_ref,
                provenance_payload_hash=provenance_payload_hash,
            )
        finally:
            if client_handle is not None:
                client_handle.close()

    def _execute_registration(self, row: ByoaRegistrationRequest) -> ByoaRegistrationRequest:
        client_handle: _AgentClientHandle | None = None
        try:
            spec, provenance = _registration_inputs(row)
            client_handle = self._make_agent_client(row)
            try:
                snapshot = client_handle.client.register_agent_by_service(
                    project_id=row.project_id,
                    spec=spec,
                    provenance=provenance,
                )
            except httpx.HTTPStatusError as exc:
                if _is_existing_agent_conflict(exc):
                    return self._reconcile_existing_agent(row, client_handle.client, registration_error=exc)
                return self._mark_failed_from_http_error(row, exc)
            return self._complete_registration(row, snapshot, reconcile_existing=False)
        except _RegistrationFailure as exc:
            return self._mark_failed(row, exc.code, exc.message, details=exc.details)
        except httpx.HTTPStatusError as exc:
            return self._mark_failed_from_http_error(row, exc)
        except httpx.HTTPError as exc:
            return self._mark_failed(
                row,
                "cg_http_error",
                str(exc),
                details={"error_type": exc.__class__.__name__},
            )
        except (ConflictError, ValueError) as exc:
            return self._mark_failed(
                row,
                "registration_processor_error",
                str(exc),
                details={"error_type": exc.__class__.__name__},
            )
        finally:
            if client_handle is not None:
                client_handle.close()

    def _make_agent_client(self, row: ByoaRegistrationRequest) -> "_AgentClientHandle":
        if self._agent_client is not None:
            return _AgentClientHandle(client=self._agent_client, should_close=False)
        if not self._admin_service_token:
            raise ValueError("ByoaRegistrationProcessor requires admin_service_token or a preconfigured agent client")
        headers = agent_auth_headers(
            AgentRef(project_id=row.project_id, agent_id=self._admin_service_agent_id),
            self._admin_service_token,
        )
        if self._agent_client_factory is not None:
            return _AgentClientHandle(
                client=self._agent_client_factory(
                    base_url=self._base_url,
                    client=self._http_client,
                    headers=headers,
                ),
                should_close=False,
            )
        return _AgentClientHandle(
            client=HttpAgentClient(
                base_url=self._base_url,
                client=self._http_client,
                headers=headers,
            ),
            should_close=True,
        )

    def _reconcile_existing_agent(
        self,
        row: ByoaRegistrationRequest,
        client: Any,
        *,
        registration_error: httpx.HTTPStatusError,
    ) -> ByoaRegistrationRequest:
        snapshot = client.get_agent(AgentRef(project_id=row.project_id, agent_id=row.requested_agent_id))
        if snapshot is None:
            return self._mark_failed(
                row,
                "existing_agent_not_found",
                f"CG reported existing agent, but GET returned no agent: {row.requested_agent_id}",
                details={
                    "reconcile_existing": True,
                    "registration_error": _http_error_details(registration_error),
                },
            )
        return self._complete_registration(
            row,
            snapshot,
            reconcile_existing=True,
            registration_error=registration_error,
        )

    def _complete_registration(
        self,
        row: ByoaRegistrationRequest,
        snapshot: AgentSnapshot,
        *,
        reconcile_existing: bool,
        registration_error: httpx.HTTPStatusError | None = None,
    ) -> ByoaRegistrationRequest:
        mismatches = _snapshot_mismatches(
            row,
            snapshot,
            admin_service_agent_id=self._admin_service_agent_id,
        )
        if mismatches:
            mismatch_fields = sorted(mismatches)
            message = f"existing agent mismatch: {', '.join(mismatch_fields)}"
            if not reconcile_existing:
                message = f"registered agent snapshot mismatch: {', '.join(mismatch_fields)}"
            return self._store.mark_conflict_requires_review(
                row.request_id,
                actor_id=self._admin_service_agent_id,
                error_code="existing_agent_mismatch" if reconcile_existing else "registered_snapshot_mismatch",
                error_message=message,
                details={
                    "reconcile_existing": reconcile_existing,
                    "mismatch_fields": mismatch_fields,
                    "mismatches": mismatches,
                    "registration_error": None
                    if registration_error is None
                    else _http_error_details(registration_error),
                },
            )

        return self._store.mark_registered(
            row.request_id,
            actor_id=self._admin_service_agent_id,
            registered_agent_id=snapshot.agent.agent_id,
            details={
                "registered_agent_id": snapshot.agent.agent_id,
                "admitted_spec_hash": row.admitted_spec_hash,
                "provenance_kind": row.provenance_kind,
                "provenance_external_ref": row.provenance_external_ref,
                "provenance_payload_hash": row.provenance_payload_hash,
                "reconcile_existing": reconcile_existing,
            },
        )

    def _mark_failed_from_http_error(
        self,
        row: ByoaRegistrationRequest,
        exc: httpx.HTTPStatusError,
    ) -> ByoaRegistrationRequest:
        return self._mark_failed(
            row,
            _http_error_code(exc),
            _http_error_message(exc),
            details=_http_error_details(exc),
        )

    def _mark_failed(
        self,
        row: ByoaRegistrationRequest,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        return self._store.mark_failed(
            row.request_id,
            actor_id=self._admin_service_agent_id,
            error_code=code,
            error_message=message,
            details={"error_code": code, "message": message, **dict(details or {})},
        )


@dataclass(frozen=True, slots=True)
class _AgentClientHandle:
    client: Any
    should_close: bool

    def close(self) -> None:
        if self.should_close and hasattr(self.client, "close"):
            self.client.close()


@dataclass(frozen=True, slots=True)
class _RegistrationFailure(Exception):
    code: str
    message: str
    details: Mapping[str, Any]


def _registration_inputs(row: ByoaRegistrationRequest):
    missing = [
        field_name
        for field_name, value in (
            ("admitted_spec", row.admitted_spec),
            ("admitted_spec_hash", row.admitted_spec_hash),
            ("provenance_kind", row.provenance_kind),
            ("provenance_external_ref", row.provenance_external_ref),
        )
        if value is None or value == ""
    ]
    if missing:
        raise _RegistrationFailure(
            "missing_registration_inputs",
            f"BYOA request missing registration input(s): {', '.join(missing)}",
            {"missing_fields": missing},
        )
    if not isinstance(row.admitted_spec, Mapping):
        raise _RegistrationFailure(
            "invalid_admitted_spec",
            "BYOA admitted_spec must be an object",
            {"field": "admitted_spec"},
        )

    spec = canonical_agent_birth_spec(agent_birth_spec_from_admitted_spec(row.admitted_spec))
    if spec.agent_id != row.requested_agent_id:
        raise _RegistrationFailure(
            "admitted_spec_agent_mismatch",
            "BYOA admitted_spec.agent_id must match requested_agent_id",
            {
                "expected_agent_id": row.requested_agent_id,
                "actual_agent_id": spec.agent_id,
            },
        )
    expected_hash = agent_birth_spec_hash(spec)
    if expected_hash != row.admitted_spec_hash:
        raise _RegistrationFailure(
            "admitted_spec_hash_mismatch",
            "BYOA admitted_spec_hash does not match admitted_spec",
            {
                "expected_admitted_spec_hash": expected_hash,
                "actual_admitted_spec_hash": row.admitted_spec_hash,
            },
        )

    assert row.provenance_kind is not None
    assert row.provenance_external_ref is not None
    return spec, AgentRegistrationProvenance(
        kind=row.provenance_kind,
        external_ref=row.provenance_external_ref,
        payload_hash=row.provenance_payload_hash,
    )


def _snapshot_mismatches(
    row: ByoaRegistrationRequest,
    snapshot: AgentSnapshot,
    *,
    admin_service_agent_id: str,
) -> dict[str, dict[str, Any]]:
    checks = {
        "project_id": (row.project_id, snapshot.agent.project_id),
        "agent_id": (row.requested_agent_id, snapshot.agent.agent_id),
        "registered_by_agent_id": (admin_service_agent_id, snapshot.registered_by_agent_id),
        "registration_provenance_kind": (row.provenance_kind, snapshot.registration_provenance_kind),
        "registration_provenance_ref": (row.provenance_external_ref, snapshot.registration_provenance_ref),
        "registration_provenance_payload_hash": (
            row.provenance_payload_hash,
            snapshot.registration_provenance_payload_hash,
        ),
        "admitted_spec_hash": (row.admitted_spec_hash, snapshot.admitted_spec_hash),
    }
    return {
        field: {"expected": expected, "actual": actual}
        for field, (expected, actual) in checks.items()
        if expected != actual
    }


def _looks_like_agent_client(value: Any) -> bool:
    return hasattr(value, "register_agent_by_service") and hasattr(value, "get_agent")


def _is_existing_agent_conflict(exc: httpx.HTTPStatusError) -> bool:
    response = exc.response
    if response is None or response.status_code != 409:
        return False
    message = _http_response_message(response).lower()
    return "agent already exists" in message or ("agent" in message and "already exists" in message)


def _http_error_code(exc: httpx.HTTPStatusError) -> str:
    response = exc.response
    if response is None:
        return "cg_http_error"
    return f"cg_http_{response.status_code}"


def _http_error_message(exc: httpx.HTTPStatusError) -> str:
    message = _http_response_message(exc.response)
    return message or str(exc)


def _http_error_details(exc: httpx.HTTPStatusError) -> dict[str, Any]:
    response = exc.response
    if response is None:
        return {"error_type": exc.__class__.__name__, "message": str(exc)}
    payload = _response_json_object(response)
    return {
        "error_type": exc.__class__.__name__,
        "status_code": response.status_code,
        "response_error": payload.get("error"),
        "response_message": payload.get("message"),
    }


def _http_response_message(response: httpx.Response | None) -> str:
    if response is None:
        return ""
    payload = _response_json_object(response)
    message = payload.get("message")
    return message if isinstance(message, str) else ""


def _response_json_object(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


__all__ = ["ByoaRegistrationProcessor"]
