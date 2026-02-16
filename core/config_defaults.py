from __future__ import annotations

from typing import Any, Dict


DEFAULT_PROTOCOL_VERSION = "v1r3"

DEFAULT_NATS_SERVERS = ["nats://localhost:4222"]
DEFAULT_NATS_TLS_ENABLED = False
DEFAULT_NATS_CERT_DIR = "./nats-js-test"

DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"
DEFAULT_CARDBOX_MIN_SIZE = 1
DEFAULT_CARDBOX_MAX_SIZE = 10
DEFAULT_PSYCO_PG_MIN_SIZE = 1
DEFAULT_PSYCO_PG_MAX_SIZE = 10
DEFAULT_TENANT_ID = "public"

DEFAULT_WORKER_MAX_STEPS = 8
DEFAULT_WORKER_DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_WORKER_MAX_RECURSION_DEPTH = 20
DEFAULT_WORKER_MAX_CONCURRENCY = 1
DEFAULT_WORKER_INBOX_FETCH_LIMIT = 20
DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS = 5.0
DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS = 300.0
DEFAULT_WORKER_QUEUE_MAXSIZE_FACTOR = 4
DEFAULT_WORKER_TARGETS = ["worker_generic"]

DEFAULT_PMO_MAX_CONCURRENCY = 1
DEFAULT_PMO_QUEUE_MAXSIZE = 0
DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS = 5.0
DEFAULT_PMO_DISPATCHED_RETRY_SECONDS = 10.0
DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS = 30.0
DEFAULT_PMO_ACTIVE_REAP_SECONDS = 300.0
DEFAULT_PMO_PENDING_WAKEUP_SECONDS = 5.0
DEFAULT_PMO_PENDING_WAKEUP_SKIP_SECONDS = 300.0
DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS = 900.0
DEFAULT_PMO_FORK_JOIN_MAX_TASKS = 8

DEFAULT_API_LISTEN_HOST = "127.0.0.1"
DEFAULT_API_PORT = 8099

DEFAULT_UI_WORKER_TARGET = "ui_worker"
DEFAULT_UI_IDEM_TTL_S = 24 * 60 * 60
DEFAULT_UI_IDEM_WAIT_S = 5.0

DEFAULT_OBS_TIMING_ENABLED = False
DEFAULT_OBS_TIMING_STEP_EVENT = False
DEFAULT_OBS_TIMING_TASK_EVENT = False
DEFAULT_OBS_TIMING_STREAM_METADATA = False
DEFAULT_OBS_TIMING_TOOL_DISPATCH = False
DEFAULT_OBS_TIMING_WORKER_LOGS = False
DEFAULT_OBS_OTEL_ENABLED = False
DEFAULT_OBS_OTEL_SERVICE_NAMESPACE = "commonground"
DEFAULT_OBS_OTEL_SERVICE_NAME = "CommonGround"
DEFAULT_OBS_OTEL_SERVICE_VERSION = "0.1.0"
DEFAULT_OBS_OTEL_OTLP_ENDPOINT = ""
DEFAULT_OBS_OTEL_SAMPLER_RATIO = 1.0


def default_config() -> Dict[str, Any]:
    return {
        "protocol": {"version": DEFAULT_PROTOCOL_VERSION},
        "nats": {
            "servers": list(DEFAULT_NATS_SERVERS),
            "tls_enabled": DEFAULT_NATS_TLS_ENABLED,
            "cert_dir": DEFAULT_NATS_CERT_DIR,
        },
        "cardbox": {
            "postgres_dsn": DEFAULT_CARDBOX_DSN,
            "postgres_min_size": DEFAULT_CARDBOX_MIN_SIZE,
            "postgres_max_size": DEFAULT_CARDBOX_MAX_SIZE,
            "psycopg_min_size": DEFAULT_PSYCO_PG_MIN_SIZE,
            "psycopg_max_size": DEFAULT_PSYCO_PG_MAX_SIZE,
            "tenant_id": DEFAULT_TENANT_ID,
        },
        "worker": {
            "max_steps": DEFAULT_WORKER_MAX_STEPS,
            "default_timeout_seconds": DEFAULT_WORKER_DEFAULT_TIMEOUT_SECONDS,
            "max_recursion_depth": DEFAULT_WORKER_MAX_RECURSION_DEPTH,
            "max_concurrency": DEFAULT_WORKER_MAX_CONCURRENCY,
            "inbox_fetch_limit": DEFAULT_WORKER_INBOX_FETCH_LIMIT,
            "watchdog_interval_seconds": DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS,
            "inbox_processing_timeout_seconds": DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS,
            "worker_targets": list(DEFAULT_WORKER_TARGETS),
        },
        "pmo": {
            "max_concurrency": DEFAULT_PMO_MAX_CONCURRENCY,
            "queue_maxsize": DEFAULT_PMO_QUEUE_MAXSIZE,
            "watchdog_interval_seconds": DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS,
            "dispatched_retry_seconds": DEFAULT_PMO_DISPATCHED_RETRY_SECONDS,
            "dispatched_timeout_seconds": DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS,
            "active_reap_seconds": DEFAULT_PMO_ACTIVE_REAP_SECONDS,
            "pending_wakeup_seconds": DEFAULT_PMO_PENDING_WAKEUP_SECONDS,
            "pending_wakeup_skip_seconds": DEFAULT_PMO_PENDING_WAKEUP_SKIP_SECONDS,
            "fork_join_default_deadline_seconds": DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS,
            "fork_join_max_tasks": DEFAULT_PMO_FORK_JOIN_MAX_TASKS,
        },
        "api": {
            "listen_host": DEFAULT_API_LISTEN_HOST,
            "port": DEFAULT_API_PORT,
        },
        "ui_worker": {
            "worker_target": DEFAULT_UI_WORKER_TARGET,
            "idempotency_ttl_seconds": DEFAULT_UI_IDEM_TTL_S,
        },
        "observability": {
            "timing": {
                "enabled": DEFAULT_OBS_TIMING_ENABLED,
                "step_event": DEFAULT_OBS_TIMING_STEP_EVENT,
                "task_event": DEFAULT_OBS_TIMING_TASK_EVENT,
                "stream_metadata": DEFAULT_OBS_TIMING_STREAM_METADATA,
                "tool_dispatch": DEFAULT_OBS_TIMING_TOOL_DISPATCH,
                "worker_logs": DEFAULT_OBS_TIMING_WORKER_LOGS,
            },
            "otel": {
                "enabled": DEFAULT_OBS_OTEL_ENABLED,
                "service_namespace": DEFAULT_OBS_OTEL_SERVICE_NAMESPACE,
                "service_name": DEFAULT_OBS_OTEL_SERVICE_NAME,
                "service_version": DEFAULT_OBS_OTEL_SERVICE_VERSION,
                "otlp_endpoint": DEFAULT_OBS_OTEL_OTLP_ENDPOINT,
                "sampler_ratio": DEFAULT_OBS_OTEL_SAMPLER_RATIO,
            },
        },
    }
