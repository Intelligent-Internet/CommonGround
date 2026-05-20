---
name: cg-work-memory
description: Use when a local agent needs to submit a concise public work-memory report for its own completed local task through `cg report work-memory`.
metadata: {"nanobot":{"requires":{"bins":["cg"]}}}
---

# CG Work Memory Skill

Use this skill with the base `cg` skill. The base `cg` skill defines the CLI safety contract; this skill defines the work-memory reporting workflow and manifest content policy.

If the base `cg` skill is unavailable, stop and fix the local skill setup. Do not replace it with direct CommonGround HTTP calls or raw `cg worker` lifecycle commands.

This skill assumes the target project has already been prepared by an operator. For local open-source demos, use the default project `cg-demo` unless the user explicitly provides another seeded project/profile. If the CLI returns `project_not_seeded`, `project_bootstrap_conflict`, `admin_service_credential_required`, or `profile_auth_required`, stop and report the setup error instead of running `cg setup`, reading `PG_DSN`, or calling Admin Service / CommonGround HTTP directly.

If the CLI config/profile is missing, ask the user/operator for the non-secret client connection facts instead of reading config or token files yourself: project id, agent id/profile, CommonGround service URL, Admin Service URL, and either an already prepared Admin Service bearer token file path or confirmation that CLI config has been written. Never ask for a bearer token value in chat.

## Purpose

Submit selected public work facts from a local agent run as a CommonGround work-memory report.

Use this skill when:

- A local task has completed and produced a useful public summary.
- The task produced validation evidence, artifact references, or failure notes worth future inspection.
- Another same-project agent, operator, or reviewer may need to audit or reference the work later.

Do not use this skill for:

- Token-level reasoning or chain-of-thought capture.
- Private scratchpad, local runtime state, or native memory import.
- Credential, claim token, registration authority, grant, or admission material.
- Managed continuation or worker lifecycle ownership.

## Allowed Command

Only use these base `cg` skill commands:

```bash
cg profile ensure-agent --profile <project-id>/<agent-id> --project-id <project-id> --requested-agent-id <agent-id> --profile-kind byoa.work_memory_reporter.v1 --runtime-kind <runtime-kind> --display-name <display-name>
cg report work-memory --profile <project-id>/<agent-id> --project-id <project-id> --agent-id <agent-id> --manifest-file <manifest.json>
```

Do not call raw `cg worker` lifecycle commands or CommonGround HTTP APIs directly.
Do not call `cg setup ...`, `cg kernel ...`, direct database commands, or any raw token/bootstrap helper from this prompt-level workflow.

## Manifest Rules

Write the manifest to a JSON file before calling `cg report work-memory`.

The manifest must use:

- `kind: agent_work_memory_report_manifest.v1`
- `request_id`
- `summary`
- `records`
- optional `final_payload`

The manifest must not include:

- top-level `meta`
- `claim`, `claim_token`, or claim handles
- credential or registration tokens
- grants, admission authority, or birth authority material
- raw private traces or chain-of-thought

Recommended record roles:

- `local_turn_summary`
- `execution_summary`
- `validation_evidence`
- `artifact_ref`
- `failure_record`

Recommended payload profile:

```json
{
  "kind": "cg.turn_record.v1",
  "schema_version": "1",
  "type": "execution_summary",
  "summary": "Completed the local task and captured the reusable result.",
  "details": {},
  "refs": [],
  "producer": {
    "runtime": "local-agent",
    "component": "cg-work-memory"
  }
}
```

Payload fields are public work facts for inspect, audit, reference, and shared observation. They do not create authorization, contract, policy, routing, native-memory, or other machine-authoritative effects.

## Workflow

1. Summarize the completed local work in public, non-sensitive language.
2. Create a manifest JSON file with at least one record.
3. Ensure the local reporting profile with `cg profile ensure-agent` when the profile is not already prepared.
4. Run `cg report work-memory` with the prepared profile.
5. Parse only the JSON envelope from stdout.
6. If `ok` is true, keep the returned `turn` and `record_refs` for future reference.
7. If `ok` is false, inspect `error.code` and `error.message`; do not parse stderr as protocol data.
8. For missing client config/profile/auth, ask for the missing client connection facts; do not ask for token contents and do not repair operator setup.

Example:

```bash
cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind codex.local.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file examples/skills/cg-work-memory/examples/local-turn-summary.manifest.json
```

Read the submitted report:

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id T-123
```
