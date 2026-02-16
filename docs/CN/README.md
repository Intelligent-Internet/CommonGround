# CommonGround 文档库

本仓库文档按使用者任务路径组织，面向开源用户分为 `01~05` 目录。

## 新入口（建议优先）

```text
docs/CN/
├── 01_getting_started/         # 面向所有用户 (Tutorials)
│   ├── quick_start.md
│   ├── architecture_intro.md
│   └── docker_quickstart_demo_principal_fullflow_api.md
├── 02_building_agents/         # 面向应用开发者 L2 (How-To)
│   ├── defining_profiles.md
│   ├── creating_tools.md
│   ├── code_sandbox_skills.md
│   └── external_integration.md
├── 03_kernel_l1/          # 面向进阶/内核开发者 L1 (Explanation)
│   ├── agent_worker.md
│   ├── pmo_orchestration.md
│   ├── batch_manager.md
│   ├── timing_observability.md
│   └── ui_action_flow.md
├── 04_protocol_l0/             # 面向核心架构师 L0 (Reference & Explanation)
│   ├── state_machine.md
│   ├── nats_protocol.md
│   ├── utp_tool_protocol.md
│   ├── cardbox_storage.md
│   ├── watchdog_safety_net.md
│   └── traceability.md
├── 05_operations/              # 面向运维排障
│   ├── performance_tuning.md
│   └── observability.md
└── ../REFERENCE/               # 参考字典（位于 docs 根目录）
```


## 阅读顺序

1. 初次接触：`01_getting_started/quick_start.md`
2. 应用开发：`02_building_agents/*`
3. 内核机制：`03_kernel_l1/*`
4. 协议约束：`04_protocol_l0/*`
5. 运维排障：`05_operations/*`

## Legacy 目录

历史文档与迁移说明可按需从历史提交中回溯；当前仓库以 `01_getting_started/`～`05_operations/` 作为默认入口。

## 说明

- `docs/REFERENCE/nats_subjects.generated.md` 由 `uv run -m scripts.admin.generate_nats_subjects_doc` 自动生成。
- `docs/REFERENCE` 中除数据字典外的历史实现说明（如 skills 细节）将逐步并入 `02_building_agents/` 对应专题。
