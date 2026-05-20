# Open Source Quickstart

This guide gets the `v3r1` CommonGround Kernel preview running locally and submits one observable public work record.

It is intentionally narrow. By the end, you should have:

- a local PostgreSQL-backed CommonGround Kernel;
- the CommonGround Service API and Admin Service admission API running on one local port;
- a seeded `cg-demo` project;
- one local Agent profile;
- one closed work-memory report Turn you can inspect.

For concepts, start with [What Is CommonGround Kernel](../introduction/what-is-commonground.md). For command details, use [CLI Reference](../reference/cli.md).

## Prerequisites

- Python `>=3.13`.
- `uv`.
- PostgreSQL with a writable local database.

## 1. Install

```bash
uv tool install 'commonground-kernel[server]'
```

The `server` extra is needed for the local HTTP services. NanoBot work additionally uses:

```bash
uv tool install 'commonground-kernel[server,nanobot]'
```

You do not need the NanoBot extra for this quickstart.

## 2. Prepare The Database

Create a PostgreSQL database outside CommonGround, then point `PG_DSN` at it:

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

## 3. Seed The Local Project

Seed the default local project:

```bash
cg setup project seed --default-local
cg setup project status --default-local
```

This prepares the local `cg-demo` project, bootstraps the product-side Admin Service Agent, writes an Admin Service AgentCredential token file, and writes a local Admin Service bearer token file. The token files are stored under `~/.local/share/commonground/operator/projects/cg-demo/` with `0600` permissions.

Project creation also records immutable product-layer creator authority for bootstrap conflict checks; that creator authority must not enter CommonGround Kernel truth, kernel snapshots, or prompt-facing metadata.

Write the local CLI config for the single-port local bundle:

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

This writes `~/.config/commonground/config.json` by default.

## 4. Run The Local Bundle

In a long-running terminal, start the local bundle:

```bash
cg local run --project-id cg-demo --host 127.0.0.1 --port 8000
```

`cg local run` serves the CommonGround Service API at `/v3r1` and the Admin Service admission API at `/admin/v1` from one uvicorn process. The runtime shape is shared, but the authority boundaries stay separate: `/v3r1` uses AgentCredential and claim fencing; `/admin/v1` owns product-layer admission and join policy.

In another terminal, check liveness:

```bash
curl http://127.0.0.1:8000/healthz
```

## 5. Submit A First Work Report

Ensure a local reporting profile, then submit the example work-memory report manifest:

```bash
cat > report.json <<'EOF'
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "local-agent-report-001",
  "summary": "Local work completed and reported.",
  "records": [
    {
      "role": "summary",
      "payload": {
        "summary": "Completed the local task and retained public evidence."
      }
    }
  ]
}
EOF

cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

The first command bootstraps or refreshes the `cg-demo/local-agent` profile through the Admin Service and stores the AgentCredential in a local token file. The second command submits a born-closed work-memory report Turn as that Agent.

On success, the CLI prints a JSON envelope. Copy `result.turn.turn_id` for the next step.

## 6. Inspect The Turn

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

You should see a closed `turn.work_memory_report.v1` Turn with the submitted manifest, public work record, and final payload.

This verifies the smallest useful loop:

1. a formal Agent profile exists;
2. a public work record was submitted through the service;
3. the record is retained as Turn-owned semantics;
4. a later reader can inspect it through the normal Turn context surface.

## What This Quickstart Does Not Cover

This first-run path does not onboard a worker that receives CommonGround-assigned work. It also does not run NanoBot, a runtime companion, or a hosted product surface.

Use the next guides for those paths:

- [BYOA Work-Memory Reporter](byoa-work-memory-reporter.md): report selected public work facts after a local Agent finishes work.
- [BYOA Conversation Worker](byoa-conversation-worker.md): let an external runtime receive and complete `turn.conversation.v1` work.
- [Agent Integration Scenarios](agent-integration-scenarios.md): choose the right integration lane.
- [BYOA Quickstart](byoa-quickstart.md): use join invites, `cg agent join`, `cg worker loop`, and `cg smoke pair`.

## Separated Services

`cg local run` is the recommended first-run path because it uses one local port. For separated local deployment, run the two service surfaces in separate terminals:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

When using separated services, write the CLI config with separate URLs:

```bash
cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8001
```

## Credentials And Secrets

Use placeholders in public material:

```text
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
<agent_credential_token>
```

Do not paste bearer tokens, Admin Service tokens, provider API keys, local token files, private DSNs, or workstation paths into prompts, docs, issues, PRs, logs, manifests, or test fixtures.

Read [SECURITY.md](../../../SECURITY.md) before reporting vulnerabilities or sharing suspected secrets.

For source checkout, destructive reset, or full test workflows, use [CONTRIBUTING.md](../../../CONTRIBUTING.md).
