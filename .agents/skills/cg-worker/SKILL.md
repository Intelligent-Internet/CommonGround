---
name: cg-worker
description: Use when deterministic runtime code needs to execute CommonGround worker lifecycle steps through the local `cg worker` command namespace, including claim acquisition, renewal, progress append, terminal finish, child dispatch, and suspend flows.
metadata:
  short-description: Use CommonGround worker lifecycle via `cg worker`
---

# CG Worker CLI

Use `cg worker` for deterministic worker and companion lifecycle operations. Do not use `curl` for these flows when `cg worker` covers the operation.

## Rules

- Use `cg worker claim next` to acquire work.
- When automation needs a reusable claim token file, pass `--claim-out-file`.
- `--claim-file` accepts either a bare claim JSON object or a full CLI envelope containing `result.claim`.
- Use `cg worker claim renew` while the claim remains active.
- Use `cg worker claim append` for progress records.
- Use `cg worker claim finish` for terminal close.
- Use `cg worker claim suspend` when the turn must pause for approval, orchestration, or external input.
- Use `cg worker claim dispatch-child` to dispatch a child turn from an active parent claim.

## Commands

Claim work and persist the claim token:

```bash
cg worker claim next \
  --project-id demo \
  --agent-id worker \
  --claim-out-file /tmp/cg-claim.json
```

Renew and progress:

```bash
cg worker claim renew --claim-file /tmp/cg-claim.json
cg worker claim append --claim-file /tmp/cg-claim.json --payload-file /tmp/progress.json --role progress
```

Finish, suspend, and child dispatch:

```bash
cg worker claim finish --claim-file /tmp/cg-claim.json --outcome succeeded --payload-file /tmp/final.json
cg worker claim suspend --claim-file /tmp/cg-claim.json --reason await_child --note "waiting on child"

cg worker claim dispatch-child \
  --claim-file /tmp/cg-claim.json \
  --requested-by worker \
  --target-agent codex \
  --payload-file /tmp/child-bootstrap.json \
  --dispatch-key child-123
```
