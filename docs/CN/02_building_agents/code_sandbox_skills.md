# Code Sandbox & Skills

本文把原先藏在 `REFERENCE` 的 Skills/E2B 内容提升为 L2 使用指南。

## 1. 组件与入口

- 工具服务：`services/tools/skills_service.py`
- 执行入口：
  - 本地：`infra/srt_executor.py`
  - 远程（E2B）：`infra/e2b_executor.py`
- 远程复用与状态：`infra/sandbox_registry.py`、`infra/stores/sandbox_store.py`
- 过期实例回收：`services/tools/sandbox_reaper.py`

启动服务：
```bash
uv run -m services.tools.skills_service
```

## 2. 配置与执行模式

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

- `local`：适合本地开发验证。执行器 `SrtExecutor` 使用本机 `srt` 命令。
- `remote`：适合生产与隔离执行。执行器 `E2BExecutor` 使用 E2B sandbox。

> 注意：原文示例中 `timeout_sec_default = 30` 与当前实现默认值不一致；当前默认值为 `60`（本地与远程均一致）。

## 3. 命令执行实现（与代码一致）

### 统一调度路径

- `skills.run_cmd`、`skills.run_cmd_async`、`skills.task_status`（provider=cmd）在 `services/tools/skills_service.py` 中调用 `self.executor.execute(...)`。
- 当前 Skill 工具路径固定以 shell 命令字符串提交，`language` 为 `bash`。

### 本地（local）执行链路

- `SrtExecutor._prepare_run` 创建执行目录：
  - `base_dir = work_root / project_id / run_id`
  - `workdir` 默认 `.`，并使用 `_safe_relpath` 约束。
- 将命令内容写入 `__srt_run.sh`，权限 `0o700`。
- 执行命令为：
  ```bash
  srt --settings <settings_path> bash <script_path>
  ```
  （在执行目录下运行）
- 本地仅支持 `bash`（`SrtExecutor.execute`）。

### 远程（remote）执行链路

- `E2BExecutor.execute`:
  - 允许语言集合：`python`、`bash`、`javascript`、`typescript`。
  - 实际技能命令调用仍通过 `language="bash"` 提交。
- Bash 命令实际通过 `Sandbox.run(..., language="bash")` 执行。
- 非 bash 时会尝试 `Sandbox.run_code`。
- `skills.activate` 及 `run_cmd` 的代码包上传到沙箱后，默认落在 `/root/{workdir}`（例如 `/root/skill`）目录。

## 4. 隔离策略

### 本地隔离（SRT）

- `SrtExecutor` 为 `inputs.mount_path`、`extra_files.path`、`outputs.path`、`workdir` 执行 `_safe_relpath` 和前缀限制。
- 所有读写都发生在本次执行目录树（`base_dir`）下，防止越界路径写入/读取。
- SRT 设置通过 `srt_cfg` 下发，包含网络与文件系统 allow/deny 规则（见 `[srt]` 配置块）。
- 这是“目录级 + SRT 沙箱配置”联合隔离，而不是纯进程隔离。

### 远程隔离（E2B）

- `SandboxRegistry` 以 `(project_id, agent_id)` 维度管理实例记录与复用状态。
- 启用复用时会：
  - 检查是否存在活跃实例并做过期判断；
  - 加锁 `agent` 执行粒度（`lock_timeout_sec`）；
  - 命中空闲实例时复用，否则创建新实例并注册到数据库。
- 生命周期由 `idle_ttl_sec`（空闲）和 `hard_ttl_sec`（硬截止）控制。
- `services/tools/sandbox_reaper.py` 周期性删除过期记录并 `kill` 对应沙箱。

## 5. Profile 放行工具

参考 `examples/profiles/skill_orchestrator_agent.yaml`：

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

建议将“可执行代码”的 profile 与普通 profile 分离。

## 6. 最小工作流

1. 上传/注册 skill。
2. Agent 调 `skills.load` / `skills.activate`。
3. 短任务用 `skills.run_cmd`，长任务用 `skills.start_job`。
4. 通过 `task_status`/`task_watch` 追踪。
5. 产物通过 artifact 回传（`created_artifacts`）。

## 7. 安全建议

- 生产优先 `skills.mode=remote`。
- 严格收敛 `allowed_tools`。
- 配置执行超时与输入/输出上限。
- 对长任务用异步接口，避免卡住主执行链路。

## 8. 参考资料

- `docs/CN/02_building_agents/skills_reference/skills_impl.md`
- `docs/CN/02_building_agents/skills_reference/skills_scope.md`
- `docs/CN/02_building_agents/skills_reference/skills_background_job_spec.md`
