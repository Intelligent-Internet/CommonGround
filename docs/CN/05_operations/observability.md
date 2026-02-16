# OpenTelemetry + 运维观测（Tracing / Logging / Metrics / Scripts）

本页与当前实现一致：本仓库主要观测能力以 **OTel 追踪 + 日志 + NATS 时序事件（timing）** 为主；尚未提供内置 Prometheus/Scrape 指标导出。

W3C Trace Context 由 `traceparent` 贯穿 NATS 消息头。开启 OTel 后，相关服务会使用当前上下文自动 join 到同一条 trace。

## 追踪（Tracing）

### 1) 启动 OTel/Jaeger 或 OTel/Tempo 环境

```bash
cd CommonGround
docker compose up -d nats postgres

# 推荐：Jaeger + Collector
docker compose -f observability/otel/docker-compose.jaeger.yml up -d

# 或：Tempo + Collector + Grafana
docker compose -f observability/otel/docker-compose.tempo.yml up -d
```

### 2) 为本地服务开启 OTel

```bash
export CG__observability__otel__enabled=true
export CG__observability__otel__otlp_endpoint=http://localhost:4318/v1/traces
export CG__observability__otel__sampler_ratio=1.0
```

### 3) 启动服务

```bash
cd CommonGround
uv run -m services.pmo.service
uv run -m services.agent_worker.loop
```

需要跟踪 UI/工具链路时再启动对应服务，例如 `services.ui_worker.loop`、`services.tools.openapi_service`。

### 4) 生成 trace

```bash
cd CommonGround
uv run examples/quickstarts/demo_simple_principal_stream.py --question "用一句话解释 traceparent 对 OTel 的意义。"
```

脚本会输出 `trace_id` 和 `traceparent`；可在 Jaeger (`http://localhost:16686`) 或 Grafana（Tempo 方案）中按服务名/trace id 查询。

服务名默认来自各服务运行类名（例如 `PMOService`、`AgentWorker`、`UIWorkerService`、`OpenAPIToolService`）。

## 日志（Logging）

- 运行时服务均使用 Python `logging` 输出到标准输出，不依赖专有日志采集器。
- 大多数服务固定 `INFO` 级别（如 `services.pmo.service`、`services.agent_worker.loop`、`services.ui_worker.loop`、`services.tools.openapi_service`）。
- `SkillsToolService` 与 `sandbox_reaper` 支持 `CG_LOG_LEVEL` 覆盖日志级别。
- AgentWorker 支持通过配置输出额外 timing 维度到日志，需同时开启：

```bash
export CG__observability__timing__enabled=true
export CG__observability__timing__worker_logs=true
```

打开后日志会额外输出 queue 等时间信息（用于链路抖动排查）。

## 指标（Metrics）

- 当前仓库没有内置 Prometheus/StatsD 指标导出接口，也没有 `/metrics` HTTP endpoint。
- 观察“指标化”信息的方式是通过 NATS 事件和数据库字段中的 timing 元数据：
  - `evt.agent.*.step` 中可携带 `metadata.timing`（受 `observability.timing.*` 控制）。
  - `evt.agent.*.task` 中可携带 `stats` / `timing`。
  - 工具分发可记录 tool dispatch 延时。
- 运行时控制项（`config.toml`）位于 `[observability.timing]`：
  - `enabled`
  - `step_event`
  - `task_event`
  - `stream_metadata`
  - `tool_dispatch`
  - `worker_logs`

## 脚本（Scripts）

### 1) 生成项目级观测报告（Postgres + OTel）

```bash
cd CommonGround
uv run -m scripts.admin.report_project_graph --project proj_mvp_001 --out /tmp/proj_mvp_001.report.json
```

按时间窗口导出：

```bash
uv run -m scripts.admin.report_project_graph \
  --project proj_mvp_001 \
  --since 2026-02-01T00:00:00Z \
  --until 2026-02-06T23:59:59Z \
  --out /tmp/proj_mvp_001.report.json
```

### 2) Tempo 下观察入口

- Jaeger UI：`http://localhost:16686`
- Grafana（Tempo）：`http://localhost:3000`（匿名 admin 已开启，数据源预置为 Tempo）
