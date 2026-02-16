from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.config_defaults import (
    DEFAULT_API_LISTEN_HOST,
    DEFAULT_API_PORT,
    DEFAULT_CARDBOX_DSN,
    DEFAULT_CARDBOX_MAX_SIZE,
    DEFAULT_CARDBOX_MIN_SIZE,
    DEFAULT_NATS_CERT_DIR,
    DEFAULT_NATS_SERVERS,
    DEFAULT_NATS_TLS_ENABLED,
    DEFAULT_PMO_ACTIVE_REAP_SECONDS,
    DEFAULT_PMO_DISPATCHED_RETRY_SECONDS,
    DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS,
    DEFAULT_PMO_MAX_CONCURRENCY,
    DEFAULT_PMO_PENDING_WAKEUP_SECONDS,
    DEFAULT_PMO_PENDING_WAKEUP_SKIP_SECONDS,
    DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS,
    DEFAULT_PMO_QUEUE_MAXSIZE,
    DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS,
    DEFAULT_PROTOCOL_VERSION,
    DEFAULT_PSYCO_PG_MAX_SIZE,
    DEFAULT_PSYCO_PG_MIN_SIZE,
    DEFAULT_TENANT_ID,
    DEFAULT_UI_WORKER_TARGET,
    DEFAULT_WORKER_DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_WORKER_INBOX_FETCH_LIMIT,
    DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS,
    DEFAULT_WORKER_MAX_CONCURRENCY,
    DEFAULT_WORKER_MAX_RECURSION_DEPTH,
    DEFAULT_WORKER_MAX_STEPS,
    DEFAULT_WORKER_TARGETS,
    DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS,
    DEFAULT_OBS_TIMING_ENABLED,
    DEFAULT_OBS_TIMING_STEP_EVENT,
    DEFAULT_OBS_TIMING_TASK_EVENT,
    DEFAULT_OBS_TIMING_STREAM_METADATA,
    DEFAULT_OBS_TIMING_TOOL_DISPATCH,
    DEFAULT_OBS_TIMING_WORKER_LOGS,
    DEFAULT_OBS_OTEL_ENABLED,
    DEFAULT_OBS_OTEL_SERVICE_NAMESPACE,
    DEFAULT_OBS_OTEL_SERVICE_NAME,
    DEFAULT_OBS_OTEL_SERVICE_VERSION,
    DEFAULT_OBS_OTEL_OTLP_ENDPOINT,
    DEFAULT_OBS_OTEL_SAMPLER_RATIO,
)
from core.config_loader import (
    apply_defaults,
    apply_env_overrides,
    apply_legacy_env_overrides,
    _load_raw_config,
)
from core.config_defaults import default_config


class ProtocolConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    version: str = DEFAULT_PROTOCOL_VERSION


class NatsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    servers: list[str] = Field(default_factory=lambda: list(DEFAULT_NATS_SERVERS))
    tls_enabled: bool = DEFAULT_NATS_TLS_ENABLED
    cert_dir: str = DEFAULT_NATS_CERT_DIR


class CardboxConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    postgres_dsn: str = DEFAULT_CARDBOX_DSN
    postgres_min_size: int = DEFAULT_CARDBOX_MIN_SIZE
    postgres_max_size: int = DEFAULT_CARDBOX_MAX_SIZE
    psycopg_min_size: int = DEFAULT_PSYCO_PG_MIN_SIZE
    psycopg_max_size: int = DEFAULT_PSYCO_PG_MAX_SIZE
    tenant_id: str = DEFAULT_TENANT_ID


class WorkerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    max_steps: int = DEFAULT_WORKER_MAX_STEPS
    default_timeout_seconds: float = DEFAULT_WORKER_DEFAULT_TIMEOUT_SECONDS
    max_recursion_depth: int = DEFAULT_WORKER_MAX_RECURSION_DEPTH
    max_concurrency: int = DEFAULT_WORKER_MAX_CONCURRENCY
    inbox_fetch_limit: int = DEFAULT_WORKER_INBOX_FETCH_LIMIT
    watchdog_interval_seconds: float = DEFAULT_WORKER_WATCHDOG_INTERVAL_SECONDS
    inbox_processing_timeout_seconds: float = DEFAULT_WORKER_INBOX_PROCESSING_TIMEOUT_SECONDS
    worker_targets: list[str] = Field(default_factory=lambda: list(DEFAULT_WORKER_TARGETS))


class PmoConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    max_concurrency: int = DEFAULT_PMO_MAX_CONCURRENCY
    queue_maxsize: int = DEFAULT_PMO_QUEUE_MAXSIZE
    watchdog_interval_seconds: float = DEFAULT_PMO_WATCHDOG_INTERVAL_SECONDS
    dispatched_retry_seconds: float = DEFAULT_PMO_DISPATCHED_RETRY_SECONDS
    dispatched_timeout_seconds: float = DEFAULT_PMO_DISPATCHED_TIMEOUT_SECONDS
    active_reap_seconds: float = DEFAULT_PMO_ACTIVE_REAP_SECONDS
    pending_wakeup_seconds: float = DEFAULT_PMO_PENDING_WAKEUP_SECONDS
    pending_wakeup_skip_seconds: float = DEFAULT_PMO_PENDING_WAKEUP_SKIP_SECONDS
    fork_join_default_deadline_seconds: float = DEFAULT_PMO_FORK_JOIN_DEFAULT_DEADLINE_SECONDS


class ApiConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    listen_host: str = DEFAULT_API_LISTEN_HOST
    port: int = DEFAULT_API_PORT


class UiWorkerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    worker_target: str = DEFAULT_UI_WORKER_TARGET


class ObservabilityTimingConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    enabled: bool = DEFAULT_OBS_TIMING_ENABLED
    step_event: bool = DEFAULT_OBS_TIMING_STEP_EVENT
    task_event: bool = DEFAULT_OBS_TIMING_TASK_EVENT
    stream_metadata: bool = DEFAULT_OBS_TIMING_STREAM_METADATA
    tool_dispatch: bool = DEFAULT_OBS_TIMING_TOOL_DISPATCH
    worker_logs: bool = DEFAULT_OBS_TIMING_WORKER_LOGS


class ObservabilityOTelConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    enabled: bool = DEFAULT_OBS_OTEL_ENABLED
    service_namespace: str = DEFAULT_OBS_OTEL_SERVICE_NAMESPACE
    service_name: str = DEFAULT_OBS_OTEL_SERVICE_NAME
    service_version: str = DEFAULT_OBS_OTEL_SERVICE_VERSION
    otlp_endpoint: str = DEFAULT_OBS_OTEL_OTLP_ENDPOINT
    sampler_ratio: float = DEFAULT_OBS_OTEL_SAMPLER_RATIO


class ObservabilityConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    timing: ObservabilityTimingConfig = Field(default_factory=ObservabilityTimingConfig)
    otel: ObservabilityOTelConfig = Field(default_factory=ObservabilityOTelConfig)


class JudgeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str = "gemini/gemini-2.5-flash"
    temperature: float = 0.0
    timeout_s: float = 8.0
    max_retries: int = 3


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    protocol: ProtocolConfig = Field(default_factory=ProtocolConfig)
    nats: NatsConfig = Field(default_factory=NatsConfig)
    cardbox: CardboxConfig = Field(default_factory=CardboxConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    pmo: PmoConfig = Field(default_factory=PmoConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    ui_worker: UiWorkerConfig = Field(default_factory=UiWorkerConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    judge: JudgeConfig = Field(default_factory=JudgeConfig)


def load_app_config(path: Optional[Path] = None) -> AppConfig:
    raw = _load_raw_config(path=path)
    return AppConfig.model_validate(raw)


def normalize_config(config: Optional[AppConfig | Dict[str, Any]]) -> AppConfig:
    if config is None:
        return load_app_config()
    if isinstance(config, AppConfig):
        return config
    if isinstance(config, dict):
        raw = apply_legacy_env_overrides(dict(config))
        raw = apply_env_overrides(raw)
        raw = apply_defaults(raw, default_config())
        return AppConfig.model_validate(raw)
    raise TypeError("config must be AppConfig, dict, or None")


def config_to_dict(config: Optional[AppConfig | Dict[str, Any]]) -> Dict[str, Any]:
    return normalize_config(config).model_dump(mode="python")
