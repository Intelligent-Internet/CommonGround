from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import sys
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from CommonGround.app import build_kernel_app
from CommonGround.contracts import ManualClock
from CommonGround.service import ServiceConfig, create_service_app


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.pg_support import (
    build_test_pg_dsn,
    derive_test_db_name,
    drop_test_database,
    ensure_test_database_exists,
    get_test_base_dsn,
    reset_test_db,
)
from tests.auth_support import set_current_test_pg_dsn


CLI_RUNTIME_ENV_KEYS = (
    "CG_BASE_URL",
    "CG_AGENT_CREDENTIAL_TOKEN",
    "CG_AGENT_CREDENTIAL_TOKEN_FILE",
    "CG_ADMIN_BASE_URL",
    "CG_ADMIN_AUTH_TOKEN",
    "CG_ADMIN_AUTH_TOKEN_FILE",
    "CG_CALLER_PROJECT_ID",
    "CG_CALLER_AGENT_ID",
)


@pytest.fixture(scope="session")
def test_worker_id() -> str:
    return os.environ.get("PYTEST_XDIST_WORKER", "gw0")


@pytest.fixture(scope="session")
def test_run_id() -> str:
    return os.environ.get("PYTEST_XDIST_TESTRUNUID") or os.environ.get("TEST_RUN_ID") or uuid4().hex[:8]


@pytest.fixture(scope="session")
def test_pg_dsn(test_run_id: str, test_worker_id: str):
    base_dsn = get_test_base_dsn()
    db_name = derive_test_db_name(base_dsn=base_dsn, run_id=test_run_id, worker_id=test_worker_id)
    pg_dsn = build_test_pg_dsn(base_dsn=base_dsn, db_name=db_name)
    ensure_test_database_exists(pg_dsn)
    yield pg_dsn
    drop_test_database(pg_dsn)


@pytest.fixture()
def clock() -> ManualClock:
    return ManualClock(current=datetime(2026, 4, 9, tzinfo=UTC))


@pytest.fixture()
def kernel_app(clock: ManualClock, test_pg_dsn: str):
    set_current_test_pg_dsn(test_pg_dsn)
    reset_test_db(test_pg_dsn)
    return build_kernel_app(
        pg_dsn=test_pg_dsn,
        claim_timeout_seconds=30,
        clock=clock,
    )


@pytest.fixture()
def service_app(kernel_app, test_pg_dsn: str):
    return create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )


@pytest.fixture()
def test_client(service_app):
    with TestClient(service_app) as client:
        yield client


@pytest.fixture()
def isolated_cli_runtime(monkeypatch, tmp_path):
    config_path = tmp_path / "commonground-cli-config.json"
    config_path.write_text("{}\n", encoding="utf-8")
    for key in CLI_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", "test-token")
    monkeypatch.setenv("CG_CONFIG_PATH", str(config_path))
    monkeypatch.setattr("CommonGround.cli.load_local_env", lambda: None)
    yield
