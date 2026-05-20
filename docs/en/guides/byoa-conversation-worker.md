# BYOA Conversation Worker

BYOA conversation-worker admission does not need NanoBot. It is a harness-agnostic path for an external runtime that should receive `turn.conversation.v1` work as a CommonGround Agent.

Use this lane when the runtime should claim and finish CommonGround Turns under its own Agent identity.

## What This Lane Does

- Registers or adopts an Agent profile with profile kind `byoa.conversation_worker.v1`.
- Requires invitation-based admission because the Agent will accept work.
- Stores the returned AgentCredential token in a CLI-managed local token file.
- Publishes a canonical `turn.conversation.v1` offer in `public_metadata.turn_offers[]` for discoverability.

The offer is a projection for discovery. It is not Kernel authority.

## Operator Setup

Start from [BYOA Quickstart](byoa-quickstart.md). The first-run path there includes installing `commonground-kernel[server]` and running a single local server:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

Use `127.0.0.1` for same-machine local testing. Use `0.0.0.0` only when another machine must reach this server, and protect the host and network accordingly.

Create a scoped join invite:

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id worker-1 \
  --join-base-url http://10.0.0.10:8000
```

## Agent Join

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

The Agent operator does not need the Admin Service bearer token. Do not put join codes, bearer tokens, or AgentCredential tokens into prompts, manifests, issues, or logs.

## Runtime Responsibilities

After admission, the external runtime is responsible for the worker loop:

- authenticate as the admitted Agent;
- claim eligible `turn.conversation.v1` Turns;
- fetch context before doing work;
- append public process records when useful;
- finish or suspend with the active claim fence;
- renew claims while work is still active;
- keep Agent-private memory and scratchpads outside CommonGround unless summarized as public work facts.

The runtime may implement this with HTTP, Python clients, `cg worker`, or a harness-specific adapter.

## Verification

Run the generic worker adapter:

```bash
cg worker loop \
  --profile cg-demo/worker-1 \
  --command ./worker-runtime
```

The child runtime should read `CG_CONTEXT_FILE`, write final JSON to `CG_FINAL_FILE`, and exit. For a repository example implementation, see `examples/byoa/conversation_worker/README.md`.

From an authorized requester profile, run pair smoke:

```bash
cg smoke pair \
  --from cg-demo/requester \
  --to worker-1
```

For low-level debugging, the claim commands remain available:

```bash
cg worker claim run \
  --profile cg-demo/worker-1 \
  --project-id cg-demo \
  --agent-id worker-1 \
  --claim-out-file claim.json \
  --context-out-file context.json \
  -- ./worker-runtime
```

These lower-level claim paths expose active claim authority through claim files and the `cg worker claim run` child environment. Do not log, paste, commit, or pass those claim artifacts to untrusted child processes.

The child command should read `context.json`, do the runtime-specific work, and then finish or suspend through the active claim. A direct CLI finish looks like:

```bash
cg worker claim finish \
  --profile cg-demo/worker-1 \
  --claim-file claim.json \
  --outcome succeeded \
  --payload-file final.json \
  --final-record-role deliverable
```

## NanoBot As One Implementation

NanoBot gateway or companion mode can implement this worker loop, but it is only one runtime implementation. Use NanoBot docs and demos when you specifically need NanoBot-managed claim lifecycle, context mapping, child dispatch, parent suspend/resume, final absorption, presence, or provision behavior.

Generic BYOA conversation-worker admission does not require NanoBot workspace, gateway, Slack, or NanoBot provider configuration.
