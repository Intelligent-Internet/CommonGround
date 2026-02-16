# OTel Stack (Collector + Jaeger/Tempo)

Fastest path:

```bash
cd CommonGround
docker compose up -d nats postgres
docker compose -f observability/otel/docker-compose.jaeger.yml up -d
```

Enable tracing (local run):

```bash
export CG__observability__otel__enabled=true
export CG__observability__otel__otlp_endpoint=http://localhost:4318/v1/traces
export CG__observability__otel__sampler_ratio=1.0
export OTEL_PROPAGATORS=tracecontext,baggage
```

Start services:

```bash
uv run -m services.pmo.service
uv run -m services.agent_worker.loop
```

Generate traces:

```bash
uv run examples/quickstarts/demo_simple_principal_stream.py --question "hello"
```

UI:
- Jaeger: `http://localhost:16686`
- Tempo: `docker compose -f observability/otel/docker-compose.tempo.yml up -d` then Grafana `http://localhost:3000`

More details: `docs/05_operations/observability.md`
