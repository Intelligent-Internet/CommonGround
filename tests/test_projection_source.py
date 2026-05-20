from __future__ import annotations

from CommonGround.service import ServiceConfig, create_service_app
from CommonGround.service.projection import PostgresProjectionSource
from CommonGround.service.read_policy import ServiceReadPolicy


def test_projection_source_exposes_pg_dsn(test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)

    assert source.pg_dsn == test_pg_dsn


def test_create_service_app_includes_projection_deps(kernel_app, test_pg_dsn: str) -> None:
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )

    deps = app.state.service_deps
    assert isinstance(deps.projection_source, PostgresProjectionSource)
    assert deps.projection_source.pg_dsn == test_pg_dsn
    assert isinstance(deps.read_policy, ServiceReadPolicy)
