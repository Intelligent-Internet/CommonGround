# Background Task + Job Workflow (Current Implementation Spec)

## Goal
- Offload long commands from direct `skills.run_cmd`/tool timeout paths.
- Build ordered, ordered-and-recoverable workflow composition with shared session state.
- Keep `Task` as primitive, `Job` as ordered step workflow.

## Terms
- Task: one background command execution (`task_id`) managed by `skills.run_cmd_async`/`skills.run_service`.
- Job: ordered multi-step workflow (`job_id`) managed by `skills.start_job`.
- Watch: heartbeat/continuation helper for agent runtime; should be treated as observability, not a source of state transitions.

## APIs

### 1) `skills.run_cmd_async(skill_name, command, session_id?, workdir?, env?, inputs?, outputs?, timeout_sec?)`
Input:
- `skill_name: string` (required)
- `command: string` (required)
- `session_id?: string`
- `workdir?: string` (default: `"skill"`)
- `env?: object<string,string>`
- `inputs?: array<object>`
- `outputs?: array<object>`
- `timeout_sec?: number`

Output:
- `task_id: string`
- `provider: "srt" | "e2b"` (depends on runtime mode)
- `mode: "batch"`
- `status: "queued"`
- `session_id: string | null`
- `workdir: string`
- `skill_name: string`
- If `mode=service` (see below), additional forwarded endpoint fields are present.

Notes:
- In current code, this simply delegates to `_handle_run_background_command(..., mode="batch")`.
- `run_background_task` is not a registered skill.

### 1b) `skills.run_service(...)` (service mode task)
- Inputs extend `skills.run_cmd_async`, plus:
  - `session_id: string` required
  - `workdir?: string`
  - `env?: object<string,string>`
  - `ready_pattern?: string` (schema only, not used in `_handle_run_background_command`)
  - `startup_timeout_sec?: number` (schema only, not used in `_handle_run_background_command`)
- Output:
  - Same structure as `skills.run_cmd_async`, but `mode: "service"`.
- Local mode only. In non-local mode it returns an error.

### 2) `skills.task_status(provider, ...)`
Input (per registration/schema in `infra/project_bootstrap.py`):
- `provider: "bg" | "cmd"` (required by schema)
- `task_id?: string` (required by runtime when `provider="bg"`)
- `tail_bytes?: number` (default 8192)
- `query_command?: string` (required by runtime when `provider="cmd"`)
- `workdir?: string` (for provider=`cmd`)
- `session_id?: string`
- `status_field?: string`
- `progress_field?: string`
- `events_field?: string`
- `done_states?: array<string>` (default: `["done", "success", "completed"]`)
- `error_states?: array<string>` (default: `["failed", "error", "errored"]`)
- `timeout_sec?: number`

Output:
- For `provider="bg"`: task snapshot from internal `_bg_tasks` / persisted payload:
  - `task_id, provider, status, mode, startup_ready, started_at, finished_at, exit_code`
  - `last_output_at, stdout_tail, stderr_tail, stdout_truncated, stderr_truncated`
  - `created_artifacts, output_errors, session_id, error_message`
  - `exposed_endpoints, exposed_endpoints_active`
  - `orphaned, orphaned_reason`
- For `provider="cmd"`: state extraction output:
  - `task_id, provider, status_field, state, done, error, progress, events, raw`

Output/status caveat:
- `_handle_task_watch` currently stops loop on `done` or `error`, which is populated only for `provider="cmd"`.

### 3) `skills.task_cancel(task_id)`
Input:
- `task_id: string` (required)

Output:
- `task_id: string`
- `status: "succeeded" | "failed" | "canceled"`
- `canceled: boolean` (false if already terminal before cancel)

### 4) `skills.start_job(steps, skill_name, session_id, workdir?, env?, max_concurrency?, max_wall_time_sec?)`
Input:
- `skill_name: string` (required)
- `session_id: string` (required)
- `steps: [{name?, command, timeout_sec?, env?}]` (at least one valid command)
- `workdir?: string`
- `env?: object` (job-level, step env merges on top)
- `max_concurrency?: number` (currently ignored; runtime always sets `1`)
- `max_wall_time_sec?: number` (default 1800)

Output:
- `job_id: string`
- `status: "queued"`
- `session_id: string`

### 5) `skills.job_status(job_id, tail_bytes=8192)`
Output:
- `job_id`
- `status: "queued" | "running" | "succeeded" | "failed" | "canceled"`
- `current_step_index: number` (`-1` before first step starts)
- `steps: Array<{name?, status, task_id?, exit_code?, stdout_tail?, stderr_tail?, provider?}>`
- `started_at? / finished_at?`
- `error_message?`

### 6) `skills.job_watch(job_id, tail_bytes=8192, poll_interval_sec=2, heartbeat_sec=20, timeout_sec=600, dedupe_key?)`
- Calls `job_status` on a poll loop.
- Stops when `status in ("succeeded", "failed", "canceled")`.
- Heartbeats emit `{job_watch:"running", status: ...}`.
- On timeout, returns failure error.
- Returns final job snapshot and timing when done.

### 7) `skills.job_cancel(job_id, reason?)`
- Marks job `canceled`, sets `error_message=reason`, marks current step `canceled` if active, attempts task cancellation.
- Output:
  - `job_id: string`
  - `status: "canceled"`
  - `reason: string`

## State Machines

### Task
- `queued -> starting -> running -> succeeded | failed | canceled`
- `succeeded` iff `exit_code == 0`
- `failed` for `exit_code != 0` or execution exceptions
- cancellation sets `canceled`, `exit_code=-1`

### Job
- `queued -> running -> succeeded | failed | canceled`
- step executes in strict sequence under a session/workdir lock
- step execution status sequence: `queued -> starting -> running -> terminal`
- if any step terminal state is `failed`/`canceled`, job terminal is `failed` (current implementation)
- if wall timer exceeds `max_wall_time_sec`, job set to `failed` and current task canceled (`job_timeout`/`step_canceled`)
- if all steps terminal `succeeded`, job becomes `succeeded`

## Storage

### DB-backed persisted heads
- `state.skill_task_heads` and `state.skill_job_heads` store JSON payload snapshots with status.
- Store class: `infra/stores/skill_task_store.py`.

### In-memory runtime
- `_bg_tasks: Dict[str, Dict[str, Any>]` and `_jobs: Dict[str, Dict[str, Any>]` in `SkillsToolService`.
- On lookup miss and store enabled, snapshots are loaded from DB.

### Task payload fields (runtime snapshot)
- `task_id, project_id, provider, status, mode, startup_ready`
- `created_at, started_at, finished_at, last_updated_at, last_output_at`
- `command, workdir, session_id, skill_name, env`
- `stdout, stderr, stdout_truncated, stderr_truncated`
- `exit_code, error_message, created_artifacts, output_errors`
- `exposed_endpoints, exposed_endpoints_active, forwarder_*`

### Job payload fields (runtime snapshot)
- `job_id, project_id, status, session_id, skill_name`
- `workdir, env, max_wall_time_sec, current_step_index`
- `steps: [{name?, command, timeout_sec?, env?, task_id?, status?, exit_code?}]`
- `created_at, started_at, finished_at, last_updated_at, error_message`

## Runner behavior
- `start_job` creates and persists job with `status=queued`, `current_step_index=-1`, then spawns `_run_job`.
- `_run_job` acquires session lock and sets `status=running`.
- For each step:
  - set `current_step_index = i` and step `status=starting`
  - launch background task via `run_cmd_async` equivalent with env merged (`job.env` + `step.env`)
  - set step `status=running` and poll `_get_bg_task_snapshot` until terminal
  - on terminal non-succeeded: set job `failed` and stop
- After all steps: set job `succeeded`.

## Notes
- `skills.task_status` and `skills.job_status` are read-only snapshot reads.
- `skills.task_watch` and `skills.job_watch` are intended polling helpers and do not mutate job/task state.
- `tail_bytes` in snapshot reads trims output to the requested byte count; no hard code-side cap was found in current implementation.
