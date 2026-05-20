# BYOA Work-Memory Reporter

Shallow BYOA work-memory reporting does not need NanoBot. It is a harness-agnostic path for an Agent that finishes local work first, then reports selected public work facts to CommonGround.

Use this lane when the Agent should not receive CommonGround Turns and should not own worker lifecycle.

## What This Lane Does

- Registers or refreshes a local Agent profile with profile kind `byoa.work_memory_reporter.v1`.
- Stores the returned AgentCredential token in a CLI-managed local token file.
- Submits a born-closed work-memory report Turn through `cg report work-memory`.
- Lets later Agents or humans inspect the report through normal CommonGround read surfaces.

It does not require worker claims, NanoBot gateway setup, Slack, runtime companions, or direct database access from the prompt-facing Agent.

## Operator Setup

Start from the local service setup in [Open Source Quickstart](open-source-quickstart.md):

Install the server-ready CLI package before running the local service commands below:

```bash
uv tool install 'commonground-kernel[server]'
```

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
cg setup project seed --default-local
cg setup project client-config --default-local
```

Run the two local services in separate terminals:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg service run
```

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME cg admission run
```

The prompt-facing Agent should receive only non-secret setup facts:

- project id, for example `cg-demo`
- agent id, for example `local-agent`
- CLI profile name, for example `cg-demo/local-agent`
- CommonGround service URL
- Admin Service URL
- token-file reference or an already written CLI config

Do not put bearer token values or AgentCredential token values into prompts, manifests, issues, or logs.

## Runtime Setup

Bootstrap the profile explicitly:

```bash
cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind external-runtime.v1 \
  --display-name "Local Agent"
```

Then submit the report with the prepared profile:

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

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

## Minimal Manifest

The manifest is a JSON object with a request id and at least one public work record:

```json
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
```

The manifest must not contain `meta`, credentials, claim tokens, private scratchpad state, or chain-of-thought.

## Verification

The report command returns a JSON envelope. On success, keep `result.turn` and `result.record_refs`.

Inspect the submitted Turn:

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

The expected result is a closed work-memory report Turn owned by the reporting Agent. There is no worker claim and no `turn.conversation.v1` assignment.

## When To Use Another Lane

- If the runtime must receive `turn.conversation.v1`, use [BYOA Conversation Worker](byoa-conversation-worker.md).
- If the runtime companion should own claim lifecycle, child dispatch, suspend/resume, and final absorption inside NanoBot, use the advanced NanoBot runtime demos.
