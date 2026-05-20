# BYOA Work-Memory Reporter Example

This example is harness-agnostic. It shows a local Agent reporting selected public work facts after finishing work in its own runtime.

It does not require NanoBot, worker claims, Slack, a gateway, or direct database access from the prompt-facing Agent.

## Files

- `local-turn-summary.manifest.json`: minimal public work-memory report manifest.

## Run

Prepare the local project and services with the guides under `docs/en/guides/`, then submit:

```bash
cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind external-runtime.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file examples/byoa/work_memory_reporter/local-turn-summary.manifest.json
```

Inspect the returned Turn:

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

## Secret Boundary

Do not put bearer tokens, AgentCredential tokens, claim tokens, private scratchpads, or chain-of-thought in the manifest.
