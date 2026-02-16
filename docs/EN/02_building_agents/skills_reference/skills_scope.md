# Skills Scope

This document defines the scope, operation modes, and integration approach for current Skill capabilities. Refer to `docs/EN/02_building_agents/skills_reference/skills_impl.md` and `docs/EN/02_building_agents/skills_reference/skills_background_job_spec.md` for implementation details.

## Goals
- Provide reusable "skill package" execution capabilities to standardize tool calls and execution environments.
- Support synchronous, asynchronous, and multi-step execution to avoid watchdog timeouts for long-running tasks.
- Support two operation modes: local isolated execution and remote sandbox execution.

## Non-goals
- This document does not cover specific skill content design or templates (defined separately by the `SKILL.md` specification).
- This document does not cover productized UI flows for skill upload (currently manual upload).

## Runtime Modes

### 1) Local Mode (Validated)
- Runtime: `anthropic-experimental/sandbox-runtime`
- Sandbox isolation: `bubblewrap` on Linux, `sandbox-exec` on macOS
- Suitable scenarios: local development/debugging, offline environments, low-latency execution
- Current status: used for local development and debugging path, depends on local `srt` execution environment.

### 2) Remote Mode (Preview)
- Runtime: E2B SDK
- Suitable scenarios: multi-tenant isolation, elastic scaling, distributed execution
- Current status: used for isolated/remote-isolated execution; supports E2B sandbox reuse and TTL-based reclamation.

> Note: the mode is controlled by `skills.mode` configuration (see `services/tools/skills_service.py`).

## Components and Dependencies
- Skills service: `services/tools/skills_service.py`
- Agent profile: `examples/profiles/skill_orchestrator_agent.yaml`
- Skill store and API: `services/api/main.py`
- Reference implementation docs: `docs/EN/02_building_agents/skills_reference/skills_impl.md`

## Key Capability Scope

### 1) Skill Discovery and Loading
- `agent_worker` only injects the available skill list (`name`/`description`) and filters first by `profile.allowed_tools`; `skills.*` entries not listed in `allowed_tools` are not injected into the LLM tool list.
- Calling `skills.load` explicitly is required to materialize content as `tool.result` and place it into context.

### 1.1) Permission and Scope Boundaries
- Skill tool registration is stored in `resource.tools` by `project_id` dimension (`cg.{protocol}.{project_id}...`).
- The `skills` tool chain uses subject-based scoping: `cg.{protocol_version}.{project_id}.{channel_id}.cmd.tool.skills.<tool_name>`, so cross-project calls are not allowed (`skills_service` resolves `project_id` directly from the subject at the service entrypoint).
- `UI Worker` invocation capability is controlled by `tool options.ui_allowed`; for `skills.*`, this is not set in `project_bootstrap`, so UI action is not exposed by default (only available through agent/LLM tool whitelist `allowed_tools`).
- Before a synchronous command is executed, tool definition must exist (validation of `tool.call`, `tool_call_card_id`, and `tool_name` routing).

### 2) Skill Execution
- Short tasks: `skills.run_cmd`
- Long tasks: `skills.run_cmd_async`
- Persistent service: `skills.run_service` (available in local mode `local`; `expose_ports` can be configured)
- Multi-step sequencing: `skills.start_job`
- Status query: `skills.task_status` / `skills.job_status`
- Observation and cancellation: `skills.task_watch` / `skills.job_watch` / `skills.task_cancel` / `skills.job_cancel`
- Activation control: `skills.activate` (returns local path directly in local mode; remote mode can trigger ZIP delivery inside sandbox)

### 3) Output and File Rules
- `workdir` affects output file path resolution.
- Artifacts can be returned via tool results to CardBox / GCS (see implementation docs).

### 4) Lifecycle (Task/Job)
- Task (`run_cmd_async` / `run_service` background execution) lifecycle:
  - `queued -> starting -> running -> succeeded|failed|canceled`
- Job (`start_job`) lifecycle:
  - `queued -> running -> (succeeded|failed|canceled)`
- Task and job status are persisted to `state.skill_task_heads` / `state.skill_job_heads`; `run_cmd` does not generate `task_id/job_id` because it is synchronous.
- `task_watch` / `job_watch` are polling/heartbeat-style observability and do not change the observed object state; repeated watch is idempotent by `dedupe_key`.
- `*_cancel` sets status to `canceled` and attempts to cancel the currently running underlying process/task (`_cancel_task` / task-level cancellation).

## Integration and Startup Requirements
- Start `skills_service`:
  - `uv run -m services.tools.skills_service`
- Agent uses `examples/profiles/skill_orchestrator_agent.yaml`
- Ensure NATS / Postgres / CardBox / GCS are available (per `docs/EN/02_building_agents/skills_reference/skills_impl.md`).

## Skill Upload (Current Approach)
Currently it is **manually uploaded**:
- API entry: `services/api/main.py` (`/projects/{project_id}/skills:upload`)
- Reference script: `examples/flows/skills_e2b_bootstrap.py`

## Version and Compatibility
- Switching runtime mode requires updating `skills.mode` in `config.toml`.

## Risks and Constraints (Current Stage)
- `run_service` depends on `skills.mode=local`; it is rejected when `mode!=local`.
- Remote mode requires ongoing stability and compatibility validation.
- Task/job status and snapshots are persisted, but the real execution handle remains in process memory; service restart may lose in-memory handles. The `reaper` will reclaim leftover sessions/forwarders.
- `run_service + expose_ports` depends on `socat` and SRT settings (`allow_unix_sockets/allow_all_unix_sockets/allow_write`); if the environment is not compliant, behavior will degrade or fail.
- For long tasks, prefer asynchronous/job execution to avoid watchdog timeouts.

## TODO / Planned Extensions
- Stability validation and pre-release verification of remote mode
- UI flow for skill upload/management
- Exposing reaper configuration items (scan limits/rate limiting, etc.)
