# Code Sandbox & Skills

This document promotes the previously REFERENCE-located Skills/E2B content into an L2 usage guide.

## 1. Components and Entry Points

- Tool service: `services/tools/skills_service.py`
- Execution entry points:
  - Local: `infra/srt_executor.py`
  - Remote (E2B): `infra/e2b_executor.py`
- Remote reuse and state: `infra/sandbox_registry.py`, `infra/stores/sandbox_store.py`
- Expired instance reclamation: `services/tools/sandbox_reaper.py`

Start the service:
```bash
uv run -m services.tools.skills_service
```

## 2. Configuration and execution mode

```toml
[skills]
mode = "remote" # remote | local
cache_root = "/var/lib/skills-cache"
local_root = "/tmp/skills-local"
reaper_interval_sec = 60
timeout_sec_default = 60
timeout_sec_max = 120
stdout_max_bytes = 204800
stderr_max_bytes = 204800
input_max_bytes = 20971520
output_max_bytes = 20971520
total_input_max_bytes = 52428800
total_output_max_bytes = 52428800

[tools.e2b]
reuse_mode = "none" # none | off | project_agent
idle_ttl_sec = 1800
hard_ttl_sec = 21600
lock_timeout_sec = 600
timeout_sec_default = 60
timeout_sec_max = 120
stdout_max_bytes = 204800
stderr_max_bytes = 204800
input_max_bytes = 20971520
output_max_bytes = 20971520
total_input_max_bytes = 52428800
total_output_max_bytes = 52428800
```

- `local`: suitable for local development validation. Executor `SrtExecutor` runs the local `srt` command.
- `remote`: suitable for production and isolated execution. Executor `E2BExecutor` uses the E2B sandbox.

> Note: The original example used `timeout_sec_default = 30`, which does not match the current implementation default. The current default is `60` (same for local and remote).

## 3. Command execution implementation (aligned with code)

### Unified dispatch path

- `skills.run_cmd`, `skills.run_cmd_async`, and `skills.task_status` (provider=cmd) call `self.executor.execute(...)` in `services/tools/skills_service.py`.
- The current Skill tool path always submits shell command strings, with `language` set to `bash`.

### Local execution chain

- `SrtExecutor._prepare_run` creates the execution directory:
  - `base_dir = work_root / project_id / run_id`
  - `workdir` defaults to `.` and is constrained by `_safe_relpath`.
- Writes command content to `__srt_run.sh` with permission `0o700`.
- Execution command is:
  ```bash
  srt --settings <settings_path> bash <script_path>
  ```
  (run inside the execution directory)
- Local execution supports only `bash` (`SrtExecutor.execute`).

### Remote execution chain

- `E2BExecutor.execute`:
  - Allowed language set: `python`, `bash`, `javascript`, `typescript`.
  - Actual skill command calls are still submitted via `language="bash"`.
- Bash commands are executed through `Sandbox.run(..., language="bash")`.
- Non-bash commands attempt `Sandbox.run_code`.
- For `skills.activate` and `run_cmd`, code packages are uploaded into the sandbox and default to `/root/{workdir}` (for example `/root/skill`).

## 4. Isolation strategy

### Local isolation (SRT)

- `SrtExecutor` applies `_safe_relpath` and prefix checks to `inputs.mount_path`, `extra_files.path`, `outputs.path`, and `workdir`.
- All reads and writes happen under the current execution directory tree (`base_dir`) to prevent out-of-bounds path read/write.
- SRT settings are sent via `srt_cfg`, including network and filesystem allow/deny rules (see `[srt]` config block).
- This is a directory-level and SRT sandbox configuration combined isolation model, not pure process isolation.

### Remote isolation (E2B)

- `SandboxRegistry` manages instance records and reuse state at the `(project_id, agent_id)` level.
- When reuse is enabled it will:
  - Check for an active instance and evaluate expiration
  - Lock at `agent` execution granularity (`lock_timeout_sec`)
  - Reuse idle instances when hit; otherwise create a new instance and register it in the database
- Lifecycle is controlled by `idle_ttl_sec` (idle timeout) and `hard_ttl_sec` (hard cutoff).
- `services/tools/sandbox_reaper.py` periodically deletes expired records and `kill`s corresponding sandboxes.

## 5. Profile allowlist tools

Refer to `examples/profiles/skill_orchestrator_agent.yaml`:

```yaml
allowed_tools:
  - "skills.load"
  - "skills.activate"
  - "skills.run_cmd"
  - "skills.run_cmd_async"
  - "skills.start_job"
  - "skills.task_status"
  - "skills.task_watch"
```

It is recommended to separate profiles that allow executable code from normal profiles.

## 6. Minimal workflow

1. Upload/register skill.
2. Agent calls `skills.load` / `skills.activate`.
3. Use `skills.run_cmd` for short tasks and `skills.start_job` for long tasks.
4. Track execution with `task_status` / `task_watch`.
5. Return artifacts through artifact upload (`created_artifacts`).

## 7. Security recommendations

- Prefer `skills.mode=remote` in production.
- Strictly narrow `allowed_tools`.
- Configure execution timeouts and input/output size limits.
- Use async interfaces for long tasks to avoid blocking the main execution path.

## 8. References

- `docs/EN/02_building_agents/skills_reference/skills_impl.md`
- `docs/EN/02_building_agents/skills_reference/skills_scope.md`
- `docs/EN/02_building_agents/skills_reference/skills_background_job_spec.md`
