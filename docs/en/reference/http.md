# CommonGround HTTP Reference

This reference describes the current HTTP surface implemented by `CommonGround/service/routes.py`, `CommonGround/service/projection/router.py`, and the Python HTTP clients. It is a minimal integration reference, not an OpenAPI replacement.

## Base URL And Version Prefix

The default local service URL is:

```text
http://127.0.0.1:8000
```

Project-scoped service routes currently use the implementation prefix:

```text
/v3r1/projects/{project_id}
```

Treat `/v3r1` as the current preview service route prefix. This reference and the service implementation define the active HTTP contract for v3r1.

Health routes are not project-scoped:

- `GET /healthz`
- `GET /readyz`

## Authentication

Agent-authenticated requests use three headers:

```http
X-CG-Project-Id: cg-demo
X-CG-Agent-Id: worker
Authorization: Bearer <agent_credential_token>
```

`X-CG-Project-Id` and `X-CG-Agent-Id` declare the caller identity. `Authorization` proves that identity with an Agent credential token. The service rejects missing, mismatched, expired, inactive, or invalid credentials.

The Python clients set these headers when constructed with both `agent=AgentRef(...)` and `auth_token=...`, or when the CLI resolves an authenticated profile.

## Response And Error Shape

Successful HTTP responses return the resource payload directly. Unlike the CLI, the HTTP service does not wrap success responses in `{ "ok": true, "result": ... }`.

Examples:

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123"
}
```

Some mutation routes return:

```json
{
  "ok": true
}
```

Some claim acquisition routes can return JSON `null` when no Turn is available.

Service-level handled errors use:

```json
{
  "error": "ConflictError",
  "message": "human-readable detail"
}
```

Common status mappings:

- `401`: `UnauthorizedError`
- `403`: `ForbiddenError`
- `404`: `NotFoundError`
- `409`: `ConflictError` or `FencingError`
- `422`: `InvariantError` or request validation failure
- `400`: other `KernelError`

FastAPI validation errors may use FastAPI's default validation response shape.

## Shared Request Models

Agent references:

```json
{
  "project_id": "cg-demo",
  "agent_id": "worker"
}
```

Turn references:

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123"
}
```

Claim tokens:

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123",
  "agent_id": "worker",
  "token": "<claim_token>",
  "expires_at": "2026-05-14T00:00:00Z"
}
```

Optional operation metadata:

```json
{
  "note": "operator note",
  "reason": "operator_stop",
  "annotations": {}
}
```

Optional trace context:

```json
{
  "trace_id": "trace-001",
  "parent_trace_id": null,
  "recursion_depth": 0
}
```

## Route Categories

### Health

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Lightweight liveness check. |
| `GET` | `/readyz` | Readiness check with backend and claim-reaper settings. |

### Admin Service Admission

These routes belong to the product-layer Admin Service surface, not the Kernel `/v3r1` authority surface.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/admin/v1/projects/{project_id}/agent-credential-tokens:request` | Request profile bootstrap and one-time AgentCredential material through Admin Service policy. |
| `POST` | `/admin/v1/projects/{project_id}/agent-join-invites` | Create a scoped Agent join invite. Requires Admin Service bearer auth. |
| `POST` | `/admin/v1/agent-joins:redeem` | Redeem a join code for profile metadata and one-time AgentCredential material. Does not require Admin Service bearer auth. |
| `POST` | `/admin/v1/byoa/join:redeem` | Compatibility alias for `/admin/v1/agent-joins:redeem`. |

Join invite create response includes the join code once:

```json
{
  "invite": {
    "invite_id": "aginv_...",
    "project_id": "cg-demo",
    "agent_id": "worker-1",
    "profile_kind": "byoa.conversation_worker.v1",
    "runtime_kind": "manual.shell.v1",
    "expires_at": "2026-05-16T00:00:00+00:00",
    "single_use": true
  },
  "join_code": "cgjoin_..."
}
```

Join redeem response includes profile metadata and a one-time `agent_credential_token`. The token is not included inside the profile object.

### Agent Registration And Credentials

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/agents:register` | Register an Agent through a trusted registration service actor. |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials:issue` | Issue an Agent credential token. Requires credential-issue grant. |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials/{credential_id}:revoke` | Revoke an Agent credential. |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials` | List Agent credentials for the caller's Agent or for credential admins. |

Register Agent request:

```json
{
  "spec": {
    "agent_id": "worker",
    "role": "worker",
    "description": "Example worker",
    "enabled": true,
    "accepts_work": true,
    "capacity": 1,
    "capabilities": [],
    "grants": [],
    "public_metadata": {}
  },
  "provenance": {
    "kind": "local_setup",
    "external_ref": "seed",
    "payload_hash": null
  }
}
```

Credential issue response includes both a credential summary and a one-time token:

```json
{
  "credential": {
    "credential_id": "cred-123",
    "project_id": "cg-demo",
    "agent_id": "worker",
    "status": "active"
  },
  "token": "<agent_credential_token>"
}
```

### Agents And Management

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}` | Fetch an Agent truth snapshot. |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/work-memory-reports` | Submit a work-memory report manifest as the Agent. |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:drain` | Stop admitting new Turns for an Agent. |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:resume` | Resume admitting new Turns for an Agent. |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:heartbeat-presence` | Update Agent presence. |
| `PUT` | `/v3r1/projects/{project_id}/management/agents/{agent_id}/public-metadata` | Replace Agent public metadata. |

Drain/resume body:

```json
{
  "agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "requested_by": {"project_id": "cg-demo", "agent_id": "operator"},
  "meta": null,
  "trace": null
}
```

Work-memory report body:

```json
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "report-001",
  "summary": "What changed",
  "records": [
    {
      "role": "observation",
      "payload": {"text": "Observed fact"},
      "source_refs": []
    }
  ],
  "final_payload": null,
  "declared_project_id": "cg-demo",
  "declared_agent_id": "reporter"
}
```

The work-memory manifest must not include `meta`. Ledger operation metadata is trusted service-side context, not prompt-facing manifest content.

### Turns And Dispatch

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/turns:dispatch` | Dispatch a root or child Turn. |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}` | Fetch a Turn snapshot. |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}/context` | Fetch semantic context with `after_turn_seq` and `limit`. |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}/feed` | Fetch a Turn ledger feed with `after_ledger_seq` and `limit`. |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}/feed` | Fetch an Agent ledger feed with `after_ledger_seq` and `limit`. |
| `POST` | `/v3r1/projects/{project_id}/management/turns/{turn_id}:stop` | Request stop for a Turn. |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:resume` | Resume a suspended Turn. |

Root dispatch body:

```json
{
  "requested_by": {"project_id": "cg-demo", "agent_id": "requester"},
  "target_agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "input": {"prompt": "Summarize the repo status"},
  "turn_kind": "turn.conversation.v1",
  "dispatch_key": "req-001",
  "authority": {
    "mode": "root_request",
    "request_id": "req-001"
  },
  "meta": null,
  "trace": null
}
```

Child dispatch uses:

```json
{
  "authority": {
    "mode": "child_derivation",
    "parent_claim": {
      "project_id": "cg-demo",
      "turn_id": "parent-turn",
      "agent_id": "worker",
      "token": "<claim_token>",
      "expires_at": "2026-05-14T00:00:00Z"
    }
  }
}
```

### Claim-Fenced Worker Operations

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/claims:claim` | Claim the next available Turn for an Agent. |
| `POST` | `/v3r1/projects/{project_id}/claims:renew` | Renew an active claim. |
| `POST` | `/v3r1/projects/{project_id}/claims:reconcile-expired` | Reconcile expired claims. |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}/semantic-records` | Append a semantic record under an active claim. |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:suspend` | Suspend the claimed Turn. |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:finish` | Finish the claimed Turn. |

Claim body:

```json
{
  "agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "meta": null,
  "trace": null
}
```

Append semantic record body:

```json
{
  "claim": {
    "project_id": "cg-demo",
    "turn_id": "turn-123",
    "agent_id": "worker",
    "token": "<claim_token>",
    "expires_at": "2026-05-14T00:00:00Z"
  },
  "payload": {"status": "working"},
  "role": "progress",
  "meta": null,
  "trace": null
}
```

Finish body:

```json
{
  "claim": {
    "project_id": "cg-demo",
    "turn_id": "turn-123",
    "agent_id": "worker",
    "token": "<claim_token>",
    "expires_at": "2026-05-14T00:00:00Z"
  },
  "outcome": "succeeded",
  "final_payload": {"result": "done"},
  "final_record_role": "deliverable",
  "meta": null,
  "trace": null
}
```

### Projection Routes

Projection routes are read-only project observation surfaces.

| Method | Path | Query | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v3r1/projects/{project_id}/projection/agents` | `agent_id`, `enabled_only`, `accepts_work_only`, `role`, `capability`, `limit` | List projected Agent directory entries. |
| `GET` | `/v3r1/projects/{project_id}/projection/turn-offers` | `turn_kind`, `agent_id`, `enabled_only`, `accepts_work_only`, `limit` | List canonical Turn offers. |
| `GET` | `/v3r1/projects/{project_id}/projection/turns` | `target_agent_id`, `turn_kind`, `state`, `outcome`, `stop_requested_only`, `limit` | List projected Turns. |
| `GET` | `/v3r1/projects/{project_id}/projection/turns/{turn_id}/lineage` | `limit` | Fetch Turn lineage. |
| `GET` | `/v3r1/projects/{project_id}/projection/feed` | `after_ledger_seq`, `limit` | Fetch project feed. |

Projection page responses use:

```json
{
  "project_id": "cg-demo",
  "items": [],
  "limit": 100,
  "diagnostics": []
}
```

## CLI vs Integration API

Use `cg` for local setup, operator workflows, debugging, and scripted demos. The CLI adds config resolution, profile bootstrap, token-file handling, and one-shot JSON envelopes.

Use HTTP or Python clients for runtime integrations:

- `HttpAgentClient` maps to the Agent, Turn, claim, dispatch, credential, and work-memory routes.
- `ProjectionHttpClient` maps to projection routes only.

The HTTP API is still service-mediated. It enforces Agent credentials, read policy, write guard checks, claim fencing, grants, and project/path consistency before calling kernel operations.
