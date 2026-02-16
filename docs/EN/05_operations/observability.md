# OpenTelemetry and Operations Observability (Tracing / Logging / Metrics / Scripts)

This page is synchronized with the current implementation. The repository's primary observability capabilities are based on **OTel tracing + logging + NATS timing events**. Built-in Prometheus/scrape metric export is not provided.

W3C Trace Context is propagated through `traceparent` in NATS message headers. Once OTel is enabled, participating services automatically join the same trace using the current context.

## Tracing

### 1) Start OTel/Jaeger or OTel/Tempo

```bash
cd CommonGround
docker compose up -d nats postgres

# recommended: Jaeger + Collector
docker compose -f observability/otel/docker-compose.jaeger.yml up -d

# or: Tempo + Collector + Grafana
docker compose -f observability/otel/docker-compose.tempo.yml up -d
```

### 2) Enable OTel for local services

```bash
export CG__observability__otel__enabled=true
export CG__observability__otel__otlp_endpoint=http://localhost:4318/v1/traces
export CG__observability__otel__sampler_ratio=1.0
```

### 3) Start services

```bash
cd CommonGround
uv run -m services.pmo.service
uv run -m services.agent_worker.loop
```

Start additional services only when tracing UI/tooling paths, such as `services.ui_worker.loop` and `services.tools.openapi_service`.

### 4) Generate a trace

```bash
cd CommonGround
uv run examples/quickstarts/demo_simple_principal_stream.py --question "Explain the meaning of traceparent for OTel in one sentence."
```

The script prints `trace_id` and `traceparent`. You can search by service name or trace ID in Jaeger (`http://localhost:16686`) or Grafana (Tempo setup).

Service names default to each service's runner class name, for example `PMOService`, `AgentWorker`, `UIWorkerService`, and `OpenAPIToolService`.

## Logging

- Runtime services use Python `logging` and write to standard output, and do not depend on proprietary log shippers.
- Most services use a fixed `INFO` level by default (`services.pmo.service`, `services.agent_worker.loop`, `services.ui_worker.loop`, `services.tools.openapi_service`).
- `SkillsToolService` and `sandbox_reaper` support overriding the log level with `CG_LOG_LEVEL`.
- AgentWorker can output additional timing dimensions to logs when configured:

```bash
export CG__observability__timing__enabled=true
export CG__observability__timing__worker_logs=true
```

With this enabled, logs include extra queue and timing information for diagnosing trace jitter.

## Metrics

- The repository has no built-in Prometheus/StatsD metric export endpoint and no `/metrics` HTTP endpoint.
- Metrics-like visibility is provided via timing metadata in NATS events and DB fields:
  - `evt.agent.*.step` can carry `metadata.timing` (controlled by `observability.timing.*`).
  - `evt.agent.*.task` can carry `stats` / `timing`.
  - Tool dispatch can record tool dispatch latency.
- Runtime controls for this are in `config.toml` under `[observability.timing]`:
  - `enabled`
  - `step_event`
  - `task_event`
  - `stream_metadata`
  - `tool_dispatch`
  - `worker_logs`

## Scripts

### 1) Generate project observability report (Postgres + OTel)

```bash
cd CommonGround
uv run -m scripts.admin.report_project_graph --project proj_mvp_001 --out /tmp/proj_mvp_001.report.json
```

Export with time range:

```bash
uv run -m scripts.admin.report_project_graph \
  --project proj_mvp_001 \
  --since 2026-02-01T00:00:00Z \
  --until 2026-02-06T23:59:59Z \
  --out /tmp/proj_mvp_001.report.json
```

### 2) Tempo entry points

- Jaeger UI: `http://localhost:16686`
- Grafana (Tempo): `http://localhost:3000` (anonymous admin is enabled, data source preconfigured to Tempo)
