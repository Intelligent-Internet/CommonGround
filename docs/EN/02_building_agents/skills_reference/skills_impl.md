# Skills + E2B Implementation Notes

This document describes the architecture, implementation details, and test demo for skills and E2B.

## Architecture

```mermaid
flowchart LR
  subgraph Client
    U[User / Demo Script]
  end

  subgraph API
    A[services.api]
  end

  subgraph Agent
    W[services.agent_worker]
    L[LLM Gateway]
  end

  subgraph Tools
    S[services.tools.skills_service]
  end

  subgraph Infra
    N[NATS / JetStream]
    CB[CardBox DB]
    KV[Idempotency KV]
    GCS[GCS Artifact Storage]
    E2B[E2B Sandbox API]
  end

  U -->|HTTP| A
  A -->|commands| N
  W -->|subscribe| N
  W --> L
  L --> W
  W -->|tool.call publish: cg.<ver>.<project>.<channel>.cmd.tool.skills.*| N
  N --> S

  S -->|skills.load/skills.activate/skill loading| E2B
  S -->|skills.run_cmd/skills.run_cmd_async/skills.run_service| E2B

  S -->|artifacts read/write| GCS
  N -->|wakeup callback| W

  S -->|tool_result card write / report_primitive| CB
  W -->|read card (context/tool_result)| CB

  S --> KV
  S --> KV
```

## Call Sequence

```mermaid
sequenceDiagram
  autonumber
  participant U as User/Demo Script
  participant API as services.api
  participant N as NATS/JetStream
  participant AW as agent_worker
  participant LLM as LLM Gateway
  participant SK as skills_service
  participant KV as Idempotency KV
  participant E2B as E2B Sandbox
  participant GCS as GCS Artifacts
  participant CB as CardBox DB

  U->>API: Start conversation/task request (no /dispatch; exact endpoint is caller-defined)
  API->>N: publish wakeup
  N->>AW: deliver wakeup
  AW->>LLM: prompt + context
  LLM-->>AW: tool_calls (skills.activate/skills.load/skills.run_cmd)
  AW->>N: publish tool call

  N->>SK: handle skills.activate/skills.load
  SK->>KV: try_acquire
  SK->>GCS: read skill.zip/upload artifacts
  SK->>E2B: prepare sandbox and execute command
  SK->>CB: write tool_result card
  SK->>N: publish tool_result
  N->>AW: cmd.agent.{target}.wakeup

  AW->>LLM: consume tool_result (tool.call/card result)
  LLM-->>AW: tool_calls (skills.run_cmd)
  AW->>N: publish tool call

  N->>SK: handle skills.run_cmd
  SK->>KV: try_acquire
  SK->>E2B: run bash
  SK->>E2B: collect/upload output files
  SK->>GCS: upload artifact (out.gif)
  SK->>CB: write tool_result card
  SK->>N: publish tool_result
  N->>AW: cmd.agent.{target}.wakeup

  AW->>LLM: tool_result
  LLM-->>AW: tool_calls (skills.task_status/task_watch | skills.job_status/job_watch)
  AW->>N: publish status/watch call
  N->>SK: handle task/job status queries
  SK->>N: publish task/job result
  N->>AW: cmd.agent.{target}.wakeup

  LLM-->>AW: submit_result
  AW->>CB: write final card
```

## Implementation Details

### Hydrating Skills into Prompt
- `agent_worker` injects the **available skill list** into the system prompt as `<available_skills>`, sourced from `resource.skills`. Each item includes only `name/description/location` and does not automatically inject `SKILL.md` or source files.
- Tools callable by LLM come from `resource.tools`, then are filtered by `profile.allowed_tools` and injected as standard OpenAI tool specs.
- Internal tool `submit_result` is controlled by `profile.allowed_internal_tools`; the `tool.call` + `tool.result` loop can be completed via `submit_result`.

### Registration Flow (Current Implementation)
- On project initialization, `infra/project_bootstrap.seed_tools` pre-registers these skill tools in `resource.tools`:
  `skills.load`, `skills.activate`, `skills.run_cmd`, `skills.run_cmd_async`, `skills.run_service`, `skills.start_job`, `skills.job_status`, `skills.job_cancel`, `skills.job_watch`, `skills.task_status`, `skills.task_cancel`, `skills.task_watch`.
- `POST /projects/{project_id}/skills:upload` (`services/api/main.py`) runs `SkillService.upload_skill_zip`, parses `SKILL.md`, uploads the zip to GCS, creates a version, and sets the active version.
- External tools are registered to `resource.tools` through `POST /projects/{project_id}/tools`, but names are checked against reserved built-in skill names and `cmd.sys.*` target subjects.
- Only `skills.*` tools go through `services.tools.skills_service`; their subscription subject is `cg.<ver>.*.*.cmd.tool.skills.*` with queue group `tool_skills`.

### Sandbox Reuse and Reclamation
- By default, each call creates a new sandbox (`tools.e2b.reuse_mode=none`).
- When reuse is enabled, sandboxes are reused by `project_id + agent_id` and dependencies/files are retained.
- Expired sandboxes are cleaned regularly by the reaper service.
- **Self-hosted E2B compatibility:** `Sandbox.connect` is used first. If `/connect` is missing (404), fallback logic is applied:
  - call `get_info`, and if `state=paused` then invoke `/sandboxes/{id}/resume`
  - call `get_info` again, then build the client directly with `envdAccessToken + domain`
  - this fallback is validated to work on servers without `/connect`

Configuration example:
```toml
[tools.e2b]
reuse_mode = "project_agent"
idle_ttl_sec = 1800
hard_ttl_sec = 21600
lock_timeout_sec = 600
reaper_interval_sec = 60
reaper_batch_size = 50
```

Start the reaper service:
```bash
uv run -m services.tools.sandbox_reaper
```

### Self-hosted E2B API Compatibility Probe
Use this to determine whether the server supports `/connect`:
```bash
uv run -m scripts.e2b_connect_probe --template pycowsay
```
Expected:
- `200/201` from `/connect` means the server supports the new API.
- `404` (no matching operation) means fallback is required or server routes should be completed.

### Troubleshoot: `execution_edges` Index Too Long
When `tool_call_id` is long, `state.execution_edges` may hit a B-tree index-size error:
`index row size ... exceeds btree version 4 maximum ...`
Solution (added to `scripts/setup/init_db.sql`):
```sql
DROP INDEX IF EXISTS idx_execution_edges_correlation;
DROP INDEX IF EXISTS idx_execution_edges_unique;
CREATE INDEX idx_execution_edges_correlation
  ON state.execution_edges USING hash (correlation_id);
CREATE UNIQUE INDEX idx_execution_edges_unique
  ON state.execution_edges(project_id, md5(correlation_id), primitive, edge_phase);
```
And ensure inserts use:
`ON CONFLICT ON CONSTRAINT idx_execution_edges_unique DO NOTHING`.

### Troubleshoot: `agent_inbox` Index Too Long
When `correlation_id` is long, `state.agent_inbox` may hit a B-tree index-size error:
`index row size ... exceeds btree version 4 maximum ...`
Solution (added to `scripts/setup/init_db.sql`):
```sql
DROP INDEX IF EXISTS state.idx_agent_inbox_correlation;
DROP INDEX IF EXISTS state.idx_agent_inbox_unique;
CREATE INDEX idx_agent_inbox_correlation
  ON state.agent_inbox USING hash (correlation_id);
CREATE UNIQUE INDEX idx_agent_inbox_unique
  ON state.agent_inbox(project_id, message_type, md5(correlation_id));
```

### Skill Execution and Dependency Installation
- For jobs with `requirements.txt` or long-running work, prefer:
  - `skills.run_cmd_async`: returns `task_id` immediately, then query with `skills.task_status`/`skills.task_watch`.
  - `skills.start_job`: fits multi-step serial workflows (e.g., install deps then run script), with `job_status`/`job_watch` and `job_cancel`.
- `skills.run_cmd` is synchronous and returns stdout, stderr, exit code, and artifacts after command completion.
- `skills.load` ensures skill contents are injected into the sandbox for the current session (local and remote behavior differ).
- `skills.run_service` requires `session_id` for long-lived services; only available when `skills.mode=local`.

- Example use of `skills.run_cmd` (short task):
  - skill files are injected into `skill/`
  - `workdir` is fixed to `skill` in the `run_cmd` flow
  - you can run `pip install -r requirements.txt && python ...` in one command

### Output File Rules
- If `workdir` is set, output paths are resolved relative to that directory.
- Example: `workdir="skill"` + `outputs.path="out.gif"` reads `skill/out.gif`.

### Port Exposure (Local Mode)
`skills.run_service` supports exposing ports via `expose_ports`; the LLM still starts services on TCP ports:

Example:
```json
{
  "skill_name": "web-dev-agent",
  "session_id": "demo",
  "command": "npm run dev -- --port 3000",
  "expose_ports": [3000]
}
```

Behavior:
- sidecar auto-starts in sandbox: `TCP:127.0.0.1:3000 -> .srt/ports/3000.sock`
- host auto-forwards: `.srt/ports/3000.sock -> 127.0.0.1:<host_port>`
- `tool.result` returns `exposed_endpoints`, including `url` and `host_port`
- `task_status` returns `exposed_endpoints_active`, which becomes `false` once forwarder is reclaimed
  - this field is eventually consistent; forwarder crash may take time before flipping to `false`

Constraints:
- Only supported when `skills.mode=local`
- requires `socat` installed on the system
- `[srt.network].allow_unix_sockets` in `config.toml` must allow `.srt/ports/*.sock`
- if sandbox-runtime supports it, set `[srt.network].allow_all_unix_sockets = true` to allow creating UDS
- `host_port` prefers the original port; on conflict it is auto-randomized
- port mapping is not exposed publicly and binds only to `127.0.0.1`

### Forwarder / Sandbox Reaper
To avoid stale forwarder and sandbox processes after restart/crash, `skills_service` performs automatic reclamation:

Behavior:
- runs one reaper pass at startup and then loops in background (default 60s)
- reads task heads with `starting/running` status from `state.skill_task_heads`
- reaps only when `socat` forwarder cmdline and start time match, then sends `SIGTERM`
- for local mode (`provider=srt`) sandbox processes, it similarly validates by `pid + start_time + settings_path` before reaping

Safety checks:
- Linux: read `/proc/<pid>/cmdline` and `start_time` from `/proc/<pid>/stat`
- macOS: uses `ps -o command` and `ps -o lstart`

Configuration:
- `skills.reaper_interval_sec` (default: 60)

## End-to-End Testing (Self-Validation)

### 1) Start services
```bash
docker-compose up -d nats postgres
uv run -m services.api.main
uv run -m services.tools.skills_service
uv run -m services.agent_worker.loop
uv run -m services.pmo.service
uv run -m services.ui_worker.loop
```

### 2) Initialize Project + Upload Profile + Upload Skill
```bash
PROJECT_ID=public \
SKILL_DIR=examples/skills/slack-gif-creator \
SKILL_NAME=slack-gif-creator \
uv run examples/flows/skills_e2b_bootstrap.py
```

### 3) Trigger via Frontend
In UI, enter:
```
help me generate a gif, a colorful loading circle
```

## Troubleshooting

- tool.result not returned: ensure tool service is running and NATS subject matches.
- skill upload fails: check `[gcs]` config and credentials.
- agent did not call tools: check LLM key and profile allowed tools.
- idempotency stuck: reset NATS KV:
```bash
uv run scripts/setup/reset_nats_streams.py
```

Then restart the tool service.

## Note
