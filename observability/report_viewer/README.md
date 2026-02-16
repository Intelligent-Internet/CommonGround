# Project Report Viewer (Web)

A lightweight, zero-build web UI to explore `scripts/admin/report_project_graph.py` JSON output:
- agent creation graph (agent <-> agent)
- batch fanout graph (agent -> batch -> agent)
- tool call graph (agent -> tool_call)
- LLM/tool durations (from Jaeger-derived fields in the report)
- raw Card contents (embedded in the JSON)

## Run

```bash
cd CommonGround/observability/report_viewer
python -m http.server 8010
```

Open:
- `http://localhost:8010`

Then:
- Drop the generated `*.report.json` into the page, or click **Load JSON**.

## Notes

- This viewer reads the JSON entirely in the browser; nothing is uploaded.
- Large reports can be heavy. Use `--since/--until` when generating the report if needed.
