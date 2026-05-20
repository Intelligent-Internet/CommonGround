# CG Work Memory Skill

This directory contains a runtime-neutral example skill for submitting local public work summaries to CommonGround through `cg report work-memory`.

It composes the base `cg` skill:

- `examples/skills/cg/` defines the safe CLI contract.
- `examples/skills/cg-work-memory/` defines the work-memory reporting workflow.

## What This Demo Shows

A local agent can report selected public work facts without becoming a CommonGround worker lifecycle owner.

The report is submitted as a born-closed CommonGround work-memory report Turn. It can later be inspected or referenced through normal CommonGround read surfaces such as `cg turn context`.

This demo does not use:

- raw `cg worker` lifecycle commands
- `cg setup` / `cg kernel` operator commands
- direct database or `PG_DSN` setup
- claim tokens
- direct CommonGround HTTP calls
- companion-managed continuation
- native memory import

## Install

Install or copy both skill directories into the runtime's skill directory:

```text
skills/
  cg/
  cg-work-memory/
```

The local environment must provide:

- `cg` on `PATH`
- CLI access to CommonGround Service and Admin Service
- Admin Service bearer auth in CLI config, for example `admin_auth.token_file`
- an operator-seeded project, normally `cg-demo` for local open-source demos

The prompt-level agent should not receive or print Agent credential tokens. The CLI stores per-Agent credential files under its managed profile store.

If the CLI returns `project_not_seeded`, `project_bootstrap_conflict`, `admin_service_credential_required`, or `profile_auth_required`, stop and ask the operator to repair setup. The skill should not call `cg setup`, read `PG_DSN`, or call CommonGround/Admin Service HTTP directly.

## Minimal Flow

Ensure the local reporting profile, then submit a report:

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

The command returns a JSON envelope on stdout. On success, keep:

- `result.turn`
- `result.record_refs`

Read the report context:

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn-id>
```

## Example Manifest

See:

- `examples/local-turn-summary.manifest.json`

The example intentionally uses runtime-neutral wording. Runtime-specific integrations can reuse the same shape while changing the `request_id`, summaries, refs, and producer metadata.
