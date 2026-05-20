# BYOA Conversation Worker Example

This example is harness-agnostic. It shows the thin path for admitting an external runtime that can receive `turn.conversation.v1` work.

It does not require NanoBot. A NanoBot gateway can be one worker implementation, but the admission and worker responsibilities are CommonGround concepts.

## Files

- `invite-config.example.json`: sample local Admin Service invitation config shape for low-level tests.
- `root_request.json`: small smoke payload for a conversation Turn.
- `final.json`: small terminal payload for a CLI finish smoke test.
- `worker_runtime.py`: minimal child runtime for `cg worker once` or `cg worker loop`.

## Bootstrap

Run the single-port local bundle:

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

On the worker machine, redeem the printed command:

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

## Smoke Flow

Dispatch from an authorized requester profile:

```bash
cg dispatch \
  --profile cg-demo/requester \
  --project-id cg-demo \
  --requested-by requester \
  --target-agent worker-1 \
  --turn-kind turn.conversation.v1 \
  --request-id worker-smoke-001 \
  --payload-file examples/byoa/conversation_worker/root_request.json
```

Run a generic shell worker adapter:

```bash
cg worker once \
  --profile cg-demo/worker-1 \
  --command python examples/byoa/conversation_worker/worker_runtime.py
```

The child command reads `CG_CONTEXT_FILE` and writes final JSON to `CG_FINAL_FILE`. The included `worker_runtime.py` is intentionally small so the worker contract is visible without reading NanoBot code. For a manual low-level finish smoke, the claim commands are still available:

```bash
cg worker claim finish \
  --profile cg-demo/worker-1 \
  --claim-file claim.json \
  --outcome succeeded \
  --payload-file examples/byoa/conversation_worker/final.json \
  --final-record-role deliverable
```

Real runtimes should append useful public process records and use suspend when blocked. The generic `cg worker once` and `cg worker loop` adapters handle claim, context, renew, finish, suspend, and failure receipts without exposing the claim token to the child process.
