# CommonGround CLI Reference

This reference describes the current `cg` command surface implemented by `CommonGround/cli.py`. It is a minimal open-source reference for users and integration authors. Use `cg <command> --help` as the most precise local argument reference.

## Option Surfaces

`cg` no longer has root-level shared auth flags. Put connection and auth flags on the command that actually uses them:

- **operator** commands, such as `cg setup project *`, `cg local run`, `cg service run`, and `cg admission run`, expose only local process, database, setup-artifact, host, port, and token-file artifact options.
- **join** command `cg agent join` accepts a server URL, join code, `--base-url`, `--admin-base-url`, and `--config`. It redeems a join code and writes local config/profile/token files; it does not require an existing profile, AgentCredential, or Admin bearer token.
- **admin** commands `cg admission invite create` and `cg profile ensure-agent` accept `--admin-base-url`, `--admin-auth-token`, `--admin-auth-token-file`, and `--config`. `cg profile ensure-agent` also accepts `--base-url` and `--profile` because it writes or refreshes a local destination profile.
- **cg_service** commands accept `--base-url`, `--profile`, `--auth-token`, `--auth-token-file`, and `--config`. Read/projection commands that do not have their own actor field may also accept `--caller-project-id` and `--caller-agent-id`.

Admin Service bearer flags are not CommonGround Service auth, and AgentCredential flags are not Admin Service auth. Use `cg <command> --help` for the exact option surface.

For service commands that already name the actor, the CLI infers caller identity:

- `cg dispatch` uses `--requested-by`.
- `cg turn resume` uses `--requested-by`.
- `cg agent drain` and `cg agent resume` use `--requested-by`, or `--agent-id` when omitted.
- `cg provision spawn` uses `--requested-by`.
- `cg report work-memory` uses `--agent-id`.
- `cg worker once` and `cg worker loop` use `--profile`, or `--project-id` with `--agent-id`.
- `cg worker claim next` and `cg worker claim run` use `--agent-id`.
- `cg smoke pair` uses `--from`.
- Claim-file worker commands infer identity from the claim file where possible.

Claim-file worker commands still need a matching AgentCredential; the claim file identifies the claim and actor, but it is not a standalone bearer token.

## Output Envelope

One-shot CLI commands write one JSON envelope to stdout.

Successful commands:

```json
{
  "ok": true,
  "result": {}
}
```

Handled failures:

```json
{
  "ok": false,
  "error": {
    "code": "invalid_arguments",
    "message": "human-readable detail",
    "status": 2
  }
}
```

`cg local run`, `cg service run`, and `cg admission run` are long-running local operator entrypoints. They do not emit the one-shot JSON envelope.

## Local Setup

Seed a local project namespace:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project seed --default-local
```

Inspect local project readiness:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project status --default-local
```

This checks setup artifacts and bootstrap readiness. It is not a live CommonGround service health check.

Write local CLI client config:

Single-port local bundle:

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

Separated services:

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8001
```

Run the CommonGround Service:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg service run
```

Run the single-port local first-run bundle:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

Use `127.0.0.1` for same-machine local testing. Use `0.0.0.0` only when another machine must reach this server, and protect the host and network accordingly.

`cg local run` serves `/v3r1` and `/admin/v1` from one uvicorn process and one port. This combines the local runtime shape only; `/v3r1` still uses AgentCredential and claim fencing, while `/admin/v1` still owns product-layer admission and join policy.

Run the reference Admin Service admission API:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg admission run --project-id cg-demo
```

`cg local run`, `cg service run`, and `cg admission run` require the server-ready CLI package:

```bash
uv tool install 'commonground-kernel[server]'
```

If those dependencies are missing, the CLI reports `missing_extra`.

## Agent Onboarding

Create a scoped join invite through the Admin Service:

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id worker-1 \
  --join-base-url http://10.0.0.10:8000
```

By default this creates a single-use, 24-hour conversation-worker invite with profile kind `byoa.conversation_worker.v1` and runtime kind `manual.shell.v1`. The result includes a copyable canonical command:

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

Redeem the join code on the Agent machine:

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

`cg agent join` writes `base_url`, `admin_base_url`, a local profile, and an AgentCredential token file with `0600` permissions. It does not require or print the Admin Service bearer token, and its receipt does not print the AgentCredential secret.

## Profiles

Profiles let the CLI request and store an Agent credential through the Admin Service.

```bash
cg profile ensure-agent \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Reporter"
```

A profile command needs Admin Service bearer auth through `--admin-auth-token`, `--admin-auth-token-file`, or config. The resulting Agent credential token is stored in a local token file and referenced from the CLI config.

For an invited conversation-worker profile, pass the invitation code through a file:

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

`--invitation-code` is also available for local throwaway demos, but `--invitation-code-file` is preferred because it avoids shell history exposure.

For new conversation-worker onboarding, prefer `cg admission invite create` plus `cg agent join`. `cg profile ensure-agent` remains useful for work-memory reporter profiles and lower-level Admin Service testing.

## Dispatch

Dispatch a root Turn:

```bash
cg dispatch \
  --project-id cg-demo \
  --requested-by requester \
  --target-agent worker \
  --turn-kind turn.conversation.v1 \
  --request-id req-001 \
  --payload-json '{"prompt":"Summarize the repo status"}'
```

`--request-id` and `--dispatch-key` are birth-time causality and idempotency anchors. At least one is required. If only one is provided, the CLI mirrors it to the other.

Payload input modes:

- `--payload-json '<json>'`
- `--payload-file payload.json`
- `--payload-stdin`

## Turns

Fetch a Turn snapshot:

```bash
cg turn get --project-id cg-demo --turn-id <turn_id>
```

Fetch semantic context:

```bash
cg turn context \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --after-turn-seq 0 \
  --limit 100
```

Wait for a Turn to close:

```bash
cg turn wait \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --timeout-seconds 60 \
  --poll-interval-ms 500
```

Resume a suspended Turn:

```bash
cg turn resume \
  --project-id cg-demo \
  --turn-id <turn_id> \
  --requested-by requester
```

## Agents

Fetch an Agent snapshot:

```bash
cg agent get --project-id cg-demo --agent-id worker
```

Drain an Agent so it stops accepting new Turns:

```bash
cg agent drain \
  --project-id cg-demo \
  --agent-id worker \
  --requested-by operator
```

Resume accepting new Turns:

```bash
cg agent resume \
  --project-id cg-demo \
  --agent-id worker \
  --requested-by operator
```

## Project Observation

List Agents through the projection API:

```bash
cg project agent list \
  --project-id cg-demo \
  --accepts-work-only \
  --limit 100
```

List canonical Turn offers:

```bash
cg project offer list \
  --project-id cg-demo \
  --turn-kind turn.conversation.v1
```

Get a single Turn offer:

```bash
cg project offer get \
  --project-id cg-demo \
  --turn-kind turn.conversation.v1 \
  --agent-id worker
```

List Turns:

```bash
cg project turn list \
  --project-id cg-demo \
  --target-agent-id worker \
  --state closed \
  --limit 100
```

Get Turn lineage:

```bash
cg project turn lineage \
  --project-id cg-demo \
  --turn-id <turn_id>
```

Fetch the project feed:

```bash
cg project feed \
  --project-id cg-demo \
  --after-ledger-seq 0 \
  --limit 100
```

## Worker Lifecycle

Run the generic shell worker adapter once:

```bash
cg worker once \
  --profile cg-demo/worker \
  --command ./worker-bin --flag
```

Run the same adapter in a loop:

```bash
cg worker loop \
  --profile cg-demo/worker \
  --command ./worker-bin --flag
```

`cg worker once` and `cg worker loop` claim work, write context to `CG_CONTEXT_FILE`, renew the claim while the child command runs, and finish or suspend the Turn from `CG_FINAL_FILE` or `CG_SUSPEND_FILE`. These adapter paths do not place the active claim token in stdout or the child environment.

Claim the next available Turn:

```bash
cg worker claim next \
  --project-id cg-demo \
  --agent-id worker \
  --claim-out-file claim.json
```

Claim a Turn, keep the lease renewed, and run a child command:

```bash
cg worker claim run \
  --project-id cg-demo \
  --agent-id worker \
  --claim-out-file claim.json \
  --context-out-file context.json \
  -- ./worker-bin --flag
```

Renew a claim:

```bash
cg worker claim renew --claim-file claim.json
```

The lower-level `cg worker claim *` commands are different from `cg worker once/loop`: the claim file and `cg worker claim run` child environment contain active claim authority. Do not log, paste, commit, or pass those claim artifacts to untrusted child processes.

Append a semantic record under the active claim:

```bash
cg worker claim append \
  --claim-file claim.json \
  --payload-file progress.json \
  --role progress
```

Dispatch a child Turn from a parent claim:

```bash
cg worker claim dispatch-child \
  --claim-file claim.json \
  --requested-by worker \
  --target-agent reviewer \
  --payload-file child-payload.json \
  --dispatch-key child-001
```

Suspend or finish the active Turn:

```bash
cg worker claim suspend \
  --claim-file claim.json \
  --reason blocked \
  --note "Waiting for external input"

cg worker claim finish \
  --claim-file claim.json \
  --outcome succeeded \
  --payload-file final.json \
  --final-record-role deliverable
```

## Smoke

Run a pair collaboration smoke test:

```bash
cg smoke pair \
  --from cg-demo/requester \
  --to worker
```

The command checks target offer discovery, dispatches a `turn.conversation.v1`, waits for terminal state, fetches context, and returns the terminal payload.

## Provisioning

Dispatch a provision request to a provisioner Agent:

```bash
cg provision spawn \
  --project-id cg-demo \
  --requested-by requester \
  --provisioner-agent provisioner \
  --role researcher \
  --request-id provision-001
```

This uses Turn kind `turn.provision.agent.spawn.v1`. Role discoverability is projected from canonical Turn offers, not from kernel authority.

## Work-Memory Reports

Submit a local work-memory manifest:

```bash
cg report work-memory \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --agent-id reporter \
  --manifest-file work-memory.json \
  --request-id report-001
```

Bootstrap or refresh the reporting Agent profile first with the Admin Service:

```bash
cg profile ensure-agent \
  --profile cg-demo/reporter \
  --project-id cg-demo \
  --requested-agent-id reporter \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Reporter"
```

The manifest must be a JSON object and must not contain `meta`.

## CLI vs Integration API

Use the CLI for local operation, debugging, scripted demos, and operator workflows. It provides stable command-level envelopes and profile bootstrap conveniences.

Use the Python clients or HTTP API for runtime integrations:

- `CommonGround.agent_client.http_client.HttpAgentClient` covers Agent, Turn, claim, dispatch, credential, and work-memory operations.
- `CommonGround.projection_client.http_client.ProjectionHttpClient` covers read-only project projection routes.

The CLI is not the kernel authority. It is a client over the same HTTP service and Admin Service surfaces used by integrations.
