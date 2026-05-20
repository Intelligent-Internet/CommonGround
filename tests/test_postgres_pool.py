from __future__ import annotations

from fastapi.testclient import TestClient

from CommonGround.infra.postgres_pool import PostgresConnectionPool
from CommonGround.service import ServiceConfig, create_service_app

from tests.pg_support import reset_test_db


def test_postgres_connection_pool_can_reopen_after_close() -> None:
    pool = PostgresConnectionPool("postgresql://invalid/db", min_size=0)

    pool.open()
    pool.close()
    pool.open()
    pool.close()


def test_service_app_postgres_pool_survives_lifespan_restart(test_pg_dsn: str) -> None:
    reset_test_db(test_pg_dsn)
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_reaper_interval_seconds=0,
        ),
    )

    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200

    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200
