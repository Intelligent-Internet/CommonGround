---
name: cg
description: Use when a local agent needs to dispatch work to CommonGround, inspect turn or agent state, wait for a turn to close, or resume a suspended turn through the `cg` CLI instead of calling the CG HTTP API directly.
metadata: {"nanobot":{"requires":{"bins":["cg"]}}}
---

# CG Skill

Use the local `cg` CLI as the stable CommonGround interaction surface.

If `cg` is not available on `PATH`, stop and fix the local CLI setup. Do not fall back to direct HTTP calls.

The host should provide CommonGround auth through CLI-managed profiles. Legacy direct Agent credential env/config may exist for operators, but prompt-level skills should prefer `--profile` and must not read credential tokens.
Do not write credential tokens into request payloads, prompts, semantic records, or skill notes.

For open-source local demos, assume the operator has already prepared the default project `cg-demo`. If `cg profile ensure-agent` returns `project_not_seeded`, `project_bootstrap_conflict`, `admin_service_credential_required`, or `profile_auth_required`, stop and report the setup error. Do not try to repair project setup from the skill.

If the local CLI config or profile is missing, do not inspect token files or run setup commands. Ask the user/operator for the non-secret client connection facts needed to run `cg`: project id, agent id/profile, CommonGround service URL, Admin Service URL, and either an already prepared Admin Service bearer token file path or confirmation that the operator has written those values into CLI config. Never ask for a bearer token value in chat.

## Allowed commands

- `cg dispatch`
- `cg turn get`
- `cg turn context`
- `cg turn wait`
- `cg turn resume`
- `cg agent get`
- `cg profile ensure-agent`
- `cg report work-memory`

When the task explicitly requires project-level discovery before choosing a CommonGround target, the skill may also use:

- `cg project agent list`
- `cg project offer list`
- `cg project offer get`

## Forbidden commands

- `cg setup project seed`
- `cg setup project status`
- `cg setup project client-config`
- `cg kernel`
- `cg worker claim next`
- `cg worker claim renew`
- `cg worker claim run`
- `cg worker claim append`
- `cg worker claim finish`
- `cg worker claim dispatch-child`
- `cg worker claim suspend`
- direct `curl` calls to CommonGround HTTP API
- direct `curl` calls to CommonGround Admin Service API
- direct database or `PG_DSN` setup commands
- reading credential token files directly

## Rules

- Write request payloads to a JSON file first, then call `cg ... --payload-file <path>`.
- Read only the JSON envelope from `stdout`.
- On failure, inspect `error.code` and `error.message`.
- Treat `stderr` as non-contract logging.
- If the failure indicates missing client config, profile, service URL, or Admin Service auth, ask for the missing client connection facts; do not ask for token contents.
- `cg turn wait` is only for waiting until the turn reaches terminal `closed`.
- Do not treat `suspended` as terminal completion.
- Use `cg turn resume` only for explicit external approval or operator resume flows.
- Use `result.final_record_role` and `result.final_payload` as the terminal result fields.
- Do not infer final truth from intermediate semantic records.
- Do not call `cg worker` for prompt-level interaction logic.
- Do not call `cg setup` or `cg kernel`; project setup is an operator-only local action, not a prompt-level skill action.
- Before dispatching to another agent, use project offer/agent discovery when the target is not already known. If no suitable CommonGround offer exists, report that CG delegation is unavailable rather than silently falling back to native subagents.
- Do not call `curl` against the CommonGround HTTP API when `cg` covers the operation.
- When reporting work memory, write the manifest to a JSON file and use `cg report work-memory --manifest-file <path>`.
- Do not put claim tokens, registration credentials, or top-level `meta` into a work-memory manifest.
- Use a higher-level workflow skill such as `cg-work-memory` when you need stricter work-memory manifest guidance; this base skill only defines the CLI safety contract.

## Examples

Dispatch work:

```bash
cg dispatch \
  --project-id cg-demo \
  --requested-by frontside \
  --target-agent nanobot \
  --turn-kind turn.conversation.v1 \
  --request-id req-123 \
  --payload-file /tmp/cg-payload.json
```

Wait for terminal close:

```bash
cg turn wait \
  --project-id cg-demo \
  --turn-id T-123 \
  --timeout-seconds 120 \
  --poll-interval-ms 500
```

Read status:

```bash
cg turn get --project-id cg-demo --turn-id T-123
cg turn context --project-id cg-demo --turn-id T-123
cg agent get --project-id cg-demo --agent-id nanobot
cg project agent list --project-id cg-demo
cg project offer list --project-id cg-demo
cg project offer get --project-id cg-demo --agent-id nanobot --turn-kind turn.conversation.v1
```

Resume a suspended turn:

```bash
cg turn resume --project-id cg-demo --turn-id T-123 --requested-by operator
```

Ensure a local Agent profile:

```bash
cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --runtime-kind codex.local.v1 \
  --display-name "Local Agent"
```

Report local work memory:

```bash
cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file /tmp/work-memory-report.json
```
