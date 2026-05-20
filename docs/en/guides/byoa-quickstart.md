# BYOA Quickstart

BYOA is a usage scenario: you bring an external Agent or runtime into CommonGround. The canonical CLI commands are generic CommonGround onboarding commands, not a long-term `cg byoa ...` namespace.

First-run constraints:

- Agent operators need one server URL and one join code.
- Agent operators do not need the Admin Service bearer token.
- The local first-run path does not require two ports.
- NanoBot is not required.
- The generic path uses `cg local run`, `cg admission invite create`, `cg agent join`, `cg worker loop`, and `cg smoke pair`.

Install the server-ready CLI package before the local service commands below:

```bash
uv tool install 'commonground-kernel[server]'
```

## Server

Seed the local project and token files:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg setup project seed --default-local
```

Run the single-port local bundle:

```bash
PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME \
cg local run --project-id cg-demo --host 0.0.0.0 --port 8000
```

Use `127.0.0.1` for same-machine local testing. Use `0.0.0.0` only when another machine must reach this server, and protect the host and network accordingly.

This runs the CommonGround Service API at `/v3r1` and the Admin Service admission API at `/admin/v1` in one uvicorn process. The runtime shape is shared, but the authority boundaries stay separate: `/v3r1` uses AgentCredential and claim fencing; `/admin/v1` owns product-layer admission and join policy.

In another operator shell, create a scoped join invite:

```bash
cg admission invite create \
  --project-id cg-demo \
  --agent-id agent-a \
  --join-base-url http://10.0.0.10:8000
```

The result includes a copyable command:

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

## Agent Machine

Redeem the join code:

```bash
cg agent join http://10.0.0.10:8000 cgjoin_abc123
```

The CLI writes `base_url`, `admin_base_url`, a local profile such as `cg-demo/agent-a`, and an AgentCredential token file with `0600` permissions. The receipt does not print the AgentCredential secret.

Run the generic shell worker adapter:

```bash
cg worker loop \
  --profile cg-demo/agent-a \
  --command ./worker-runtime
```

The adapter claims eligible Turns, writes context to `CG_CONTEXT_FILE`, keeps the claim renewed while the child command runs, and finishes or suspends the Turn from child output files. The child command writes final JSON to `CG_FINAL_FILE` or suspend JSON to `CG_SUSPEND_FILE`. In this higher-level adapter path, the active claim token is not passed through environment variables or stdout.

The lower-level `cg worker claim *` commands are different: claim files and `cg worker claim run` child environments carry active claim authority and must not be logged, pasted, committed, or passed to untrusted child processes.

If you want a repository example worker command, see `examples/byoa/conversation_worker/README.md`.

## Pair Smoke

After two Agents have joined, verify collaboration:

```bash
cg smoke pair \
  --from cg-demo/agent-a \
  --to agent-b
```

The smoke command checks target offer discovery, dispatches a `turn.conversation.v1`, waits for terminal state, fetches context, and returns the terminal payload.

## Separated Services

Use `cg service run` and `cg admission run` when service and admission surfaces run as distinct local processes or ports.

BYOA remains the guide and product scenario name. The CLI surfaces above are generic so first-party Agents, test workers, custom services, external runtimes, and non-NanoBot integrations can share the same onboarding path.
