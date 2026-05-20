# Admin Service BYOA Credential Flow

This guide documents the current reference flow for issuing CommonGround Agent credentials through the product-side Admin Service integration.

The Admin Service is outside the CommonGround Kernel. It owns product user authentication, project creator authority, invitation policy, and local operator token files. The Kernel stores Agents, Turns, grants, credentials, ledger facts, and lifecycle state. Do not move product-layer authorization facts into Kernel truth or Agent public metadata.

For scenario selection, start with [Agent Integration Scenarios](agent-integration-scenarios.md). The work-memory reporter and conversation-worker BYOA lanes are harness-agnostic and do not require NanoBot.

## Prerequisites

- Python `>=3.13`.
- PostgreSQL with a database reachable through `PG_DSN`.
- Install the server-ready CLI package:

```bash
uv tool install 'commonground-kernel[server]'
```

## Local Project Bootstrap

Seed the project-scoped Admin Service Agent and the local operator tokens:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
  cg setup project seed --default-local
```

By default this uses project id `cg-demo` and creator ref `local-dev`. The seed command creates or verifies the project-scoped Agent `admin-service` with role `service.admin.v1`. That Agent has credential-issue, credential-revoke, and agent-birth grants; it does not accept work.

The command also creates two local token files:

| File | Default path | Purpose |
| --- | --- | --- |
| Admin Service AgentCredential | `~/.local/share/commonground/operator/projects/<project_id>/admin-service.cgac` | Credential used by the Admin Service Agent when it calls CommonGround service-authorized registration and credential APIs. |
| Admin Service API bearer token | `~/.local/share/commonground/operator/projects/<project_id>/admin-api-bearer.token` | Product-side bearer token accepted by the local admission API. CLI profile bootstrap sends it in `Authorization: Bearer ...`. |

Token files are written with `0600` permissions. Use `--rotate-admin-service-token` or `--rotate-admin-auth-token` when a local file exists but is stale or invalid.

Inspect readiness:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
  cg setup project status --default-local
```

Write a local CLI config that points to the CommonGround service and Admin Service:

```bash
cg setup project client-config --default-local
```

The default config path is `~/.config/commonground/config.json`. It stores service URLs and an Admin Service bearer token file reference; it does not inline the local operator bearer token unless the caller explicitly configures one elsewhere.

## Running The Services

Run CommonGround Service:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

Run the local Admin Service admission API:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

`cg admission run` reads:

- `PG_DSN` or `--pg-dsn`.
- `CG_BASE_URL` or `--base-url`, defaulting to `http://127.0.0.1:8000`.
- `CG_PROJECT_ID` or `--project-id`, defaulting to `cg-demo`.
- `CG_ADMIN_SERVICE_TOKEN_FILE` or `--admin-service-token-file`, defaulting to the setup token path.
- `CG_ADMIN_AUTH_TOKEN_FILE` or `--admin-auth-token-file`, defaulting to the setup bearer-token path.
- `CG_ADMIN_HOST`, `CG_ADMIN_PORT`, and `CG_ADMIN_LOG_LEVEL`, defaulting to `127.0.0.1`, `8001`, and `info`.
- `CG_ADMIN_INVITE_CONFIG_JSON` or `--invite-config-json` for conversation-worker invitations.

The local runner accepts only the configured project id. Requests authenticate with the Admin Service API bearer token and resolve to requester user id `local-admin-service`.

## Credential Request API

The local API exposes one route:

```http
POST /admin/v1/projects/{project_id}/agent-credential-tokens:request
Authorization: Bearer <admin_service_api_bearer_token>
Content-Type: application/json
```

Minimal work-memory profile request:

```json
{
  "request_id": "profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli",
  "requested_agent_id": "reporter",
  "display_name": "Local Reporter",
  "runtime_kind": "local-cli"
}
```

Response shape:

```json
{
  "request_id": "profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli",
  "project_id": "cg-demo",
  "agent_id": "reporter",
  "status": "registered",
  "profile": {
    "project_id": "cg-demo",
    "agent_id": "reporter",
    "runtime_kind": "local-cli",
    "profile_kind": "byoa.work_memory_reporter.v1",
    "profile_ref": "admin_service/byoa_registration_requests/profile-bootstrap:cg-demo:reporter:byoa.work_memory_reporter.v1:local-cli/connection-profile",
    "credential_id": "<credential_id>",
    "status": "credential_ready"
  },
  "credential": {
    "credential_id": "<credential_id>",
    "status": "active"
  },
  "agent_credential_token": "<agent_credential_token>"
}
```

`agent_credential_token` is one-time credential material from the Admin Service response. The credential row is stored in CommonGround, but the secret token is only disclosed to the caller in the response. Store it in a runtime-private token file or secret manager and do not put it in prompts, manifests, logs, issues, or docs.

The CLI handles this storage for profile bootstrap:

```bash
cg profile ensure-agent \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile cg-demo/reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local-cli \
  --display-name "Local Reporter"
```

The CLI writes the AgentCredential token to:

```text
~/.local/share/commonground/credentials/<project_id>/<agent_id>/<credential_id>.token
```

and stores the profile metadata under the CLI config `profiles` map.

## Profile Kinds

The current BYOA facade supports these profile kinds:

| Profile kind | Intended runtime | Registration result |
| --- | --- | --- |
| `byoa.work_memory_reporter.v1` | Local CLI or external runtime that reports work-memory manifests. | Registers the requested Agent with role `external.agent.v1`, no work acceptance, no capabilities, and Admin Service metadata containing `byoa_request_id` and `runtime_kind`. The default requested capability is `turn.work_memory.report.v1`, but it is an admission policy input, not a registered work capability. |
| `byoa.conversation_worker.v1` | External conversation worker admitted through an invitation. | Requires a valid invitation code. Registers role `external.conversation_worker.v1`, accepts work, capability `turn.conversation.v1`, and publishes a canonical conversation turn offer in `public_metadata.turn_offers[]`. |

For `byoa.work_memory_reporter.v1`, the MVP policy accepts only requested role `external.agent.v1` and requested capability `turn.work_memory.report.v1`.

For `byoa.conversation_worker.v1`, the request must include `invitation_code`; the facade maps the admitted role and capability from the invitation-approved profile kind.

## Invite Config

Conversation-worker admission requires an invite validator. The local runner loads a JSON file from `CG_ADMIN_INVITE_CONFIG_JSON` or `--invite-config-json`.

Example:

```json
{
  "invitations": [
    {
      "invite_id": "invite-local-conversation",
      "project_id": "cg-demo",
      "issued_by_user_id": "user-123",
      "issuer_role": "project_owner",
      "allowed_profile_kinds": ["byoa.conversation_worker.v1"],
      "code_sha256": "sha256:<64_hex_sha256_of_invitation_code>",
      "enabled": true,
      "expires_at": "2026-12-31T23:59:59Z"
    }
  ]
}
```

`code` may be used instead of `code_sha256` in local config; the loader hashes it at startup. Prefer `code_sha256` for checked-in examples. `issuer_role` currently must be `project_owner`. Disabled, expired, wrong-project, wrong-profile-kind, or unmatched codes are rejected.

Conversation-worker API request:

```json
{
  "request_id": "invite:cg-demo:worker-1",
  "requested_agent_id": "worker-1",
  "display_name": "Conversation Worker 1",
  "runtime_kind": "external-runtime",
  "profile_kind": "byoa.conversation_worker.v1",
  "invitation_code": "<invitation_code>"
}
```

The CLI can perform the invited bootstrap and store the returned AgentCredential token in the normal profile store:

```bash
cg profile ensure-agent \
  --profile cg-demo/worker-1 \
  --project-id cg-demo \
  --requested-agent-id worker-1 \
  --profile-kind byoa.conversation_worker.v1 \
  --runtime-kind external-runtime.v1 \
  --display-name "Worker 1" \
  --invitation-code-file ./invite-code.txt
```

Prefer `--invitation-code-file`. `--invitation-code` exists for local throwaway demos but may be retained by shell history.

## Common Errors

Admin Service errors use this JSON envelope:

```json
{
  "error": "UnauthorizedError",
  "code": "unauthorized",
  "message": "Admin Service bearer auth is required"
}
```

Common codes:

| Code | Typical cause | Resolution |
| --- | --- | --- |
| `unauthorized` | Missing or wrong Admin Service API bearer token. | Use the token from `CG_ADMIN_AUTH_TOKEN_FILE` or run `cg setup project seed` again with rotation if needed. |
| `forbidden` | Caller is not authorized for the project, or a BYOA invitation is invalid, disabled, expired, wrong-project, or wrong-profile-kind. | Check project id, bearer-token boundary, and invite config. |
| `project_not_seeded` | The project has no valid `admin-service` Agent. | Run `cg setup project seed --project-id <project_id>` or `--default-local`. |
| `project_bootstrap_conflict` | An existing `admin-service` Agent does not match the expected role, grants, metadata, enabled state, or capacity. | Inspect `cg setup project status`; resolve the conflicting project state deliberately. |
| `admin_service_credential_required` | The Admin Service AgentCredential token is missing, stale, invalid, expired, or not for the configured project. | Re-run setup with `--rotate-admin-service-token` or point `CG_ADMIN_SERVICE_TOKEN_FILE` at a valid token file. |
| `invalid_input` | Unsupported request field, missing required field, unsupported profile kind, bad invite config shape, or bad datetime/hash format. | Compare the request body and invite config with this guide and CLI help. |
| `conflict` | Duplicate or inconsistent BYOA request state, unsupported requested role/capability, or profile policy mismatch. | Reuse the same request only when it is intentionally idempotent; otherwise choose a new `request_id` and valid policy inputs. |

CLI setup errors are returned as one JSON envelope on stdout for one-shot commands. Long-running `cg service run` and `cg admission run` write logs instead.

## Boundary Notes

- `ProjectCreatorAuthority` is product-layer creator authority. Persist it in the product project store, not Kernel schema, registration provenance, or Agent public metadata.
- `public_metadata.turn_offers[]` is a discoverability projection, not Kernel authority.
- The Admin Service AgentCredential proves the Admin Service Agent can call service-authorized CommonGround APIs. It does not prove end-user authorization.
- The Admin Service API bearer token is a product-layer local admission credential. It is not an AgentCredential.
- Runtime-private AgentCredential tokens belong in token files or secret stores; prompt-facing agents must not read operator token files directly.
