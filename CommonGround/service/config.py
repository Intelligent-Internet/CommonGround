from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "info"
    service_name: str = "commonground-v3-service"
    claim_timeout_seconds: int = 30
    claim_reaper_interval_seconds: float = 0.0
    pg_dsn: str | None = None
    agent_project_header: str = "X-CG-Project-Id"
    agent_id_header: str = "X-CG-Agent-Id"

    @property
    def backend_kind(self) -> str:
        return "postgres"

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        return cls(
            host=os.environ.get("CG_HOST", "127.0.0.1"),
            port=int(os.environ.get("CG_PORT", "8000")),
            log_level=os.environ.get("CG_LOG_LEVEL", "info"),
            service_name=os.environ.get("CG_SERVICE_NAME", "commonground-v3-service"),
            claim_timeout_seconds=int(os.environ.get("CG_CLAIM_TIMEOUT_SECONDS", "30")),
            claim_reaper_interval_seconds=float(os.environ.get("CG_CLAIM_REAPER_INTERVAL_SECONDS", "0")),
            pg_dsn=os.environ.get("PG_DSN"),
            agent_project_header=os.environ.get("CG_AGENT_PROJECT_HEADER", "X-CG-Project-Id"),
            agent_id_header=os.environ.get("CG_AGENT_ID_HEADER", "X-CG-Agent-Id"),
        )
