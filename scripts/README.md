# Scripts 指南

`scripts/` 仅保留运维、初始化和压测脚本。面向开发者学习的在线示例已迁移到 `examples/quickstarts/`。

## 目录

- `scripts/setup/`: 数据库初始化、环境 seed、NATS stream 重置、SQL 工具
- `scripts/admin/`: 诊断、观测与文档生成
- `scripts/benchmarks/`: 性能基准脚本
- `scripts/utils/`: 脚本公共工具模块（内部依赖）

## Setup

### reset_db
```bash
uv run -m scripts.setup.reset_db
```

### seed
```bash
uv run -m scripts.setup.seed --project proj_demo_01
```

### reset_nats_streams
```bash
uv run -m scripts.setup.reset_nats_streams
```

### SQL
- `scripts/setup/init_projects.sql`
- `scripts/setup/init_db.sql`
- `scripts/setup/init_pmo_db.sql`
- `scripts/setup/register_jina_tools.sql`

## Admin

### inspect_roster
```bash
uv run -m scripts.admin.inspect_roster --project proj_demo_01 --tag associate
```

### inspect_turn
```bash
uv run -m scripts.admin.inspect_turn --project proj_demo_01 --channel public --agent agent_01
```

### cleanup_inbox
```bash
uv run -m scripts.admin.cleanup_inbox --days 7 --execute
```

### report_project_graph
```bash
uv run -m scripts.admin.report_project_graph --project proj_demo_01 --out /tmp/proj_demo_01.report.json
```

### generate_nats_subjects_doc
```bash
uv run -m scripts.admin.generate_nats_subjects_doc
```

## Benchmarks

### bench_fork_join_batch
```bash
uv run -m scripts.benchmarks.bench_fork_join_batch --project proj_bench_001 --channel public
```

## Quickstarts

公开 demo 已迁移到 `examples/quickstarts/`：
- `demo_simple_principal_stream.py`
- `demo_principal_fullflow.py`
- `demo_fork_join_word_count.py`
- `demo_ui_action.py`
