from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from contextlib import redirect_stdout
from pathlib import Path

from Integrations.admin_service.admission_runner import LocalAdmissionSettings, create_local_admission_app


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_packaging_splits_server_and_nanobot_dependencies() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]

    base_dependencies = project["dependencies"]
    extras = project["optional-dependencies"]

    assert "nanobot-ai" not in base_dependencies
    assert not any(dependency.startswith("psycopg") for dependency in base_dependencies)
    assert not any(dependency.startswith("fastapi") for dependency in base_dependencies)
    assert not any(dependency.startswith("uvicorn") for dependency in base_dependencies)
    assert "nanobot-ai" in extras["nanobot"]
    assert "nanobot-ai" not in extras["server"]
    assert any(dependency.startswith("psycopg") for dependency in extras["server"])
    assert any(dependency.startswith("fastapi") for dependency in extras["server"])
    assert any(dependency.startswith("uvicorn") for dependency in extras["server"])


def test_cli_import_does_not_load_server_setup_or_nanobot_runtime() -> None:
    code = """
import json
import sys
import CommonGround.cli

names = [
    "psycopg",
    "psycopg_pool",
    "cardbox",
    "cardbox.adapters.async_storage",
    "CommonGround.agent_client.http_client",
    "CommonGround.agent_client.types",
    "CommonGround.app.kernel",
    "CommonGround.kernel",
    "CommonGround.sdk.composites",
    "CommonGround.service.serialization",
    "CommonGround.service.http",
    "CommonGround.infra.postgres",
    "CommonGround.infra.repositories",
    "CommonGround.infra.postgres_pool",
    "Integrations.admin_service.project_setup",
    "Integrations.nanobot.runtime.leaf_worker_runner",
    "Integrations.nanobot.runtime.provisioner_runner",
]
print(json.dumps({name: name in sys.modules for name in names}, sort_keys=True))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    loaded = json.loads(result.stdout)
    assert loaded == {
        "CommonGround.service.serialization": False,
        "CommonGround.agent_client.http_client": False,
        "CommonGround.agent_client.types": False,
        "CommonGround.app.kernel": False,
        "CommonGround.kernel": False,
        "CommonGround.sdk.composites": False,
        "CommonGround.infra.postgres_pool": False,
        "CommonGround.infra.repositories": False,
        "CommonGround.infra.postgres": False,
        "CommonGround.service.http": False,
        "Integrations.admin_service.project_setup": False,
        "Integrations.nanobot.runtime.leaf_worker_runner": False,
        "Integrations.nanobot.runtime.provisioner_runner": False,
        "cardbox": False,
        "cardbox.adapters.async_storage": False,
        "psycopg": False,
        "psycopg_pool": False,
    }


def test_cli_help_does_not_resolve_default_clients_or_server_dependencies() -> None:
    code = """
import io
import json
import sys
from contextlib import redirect_stdout
import CommonGround.cli

try:
    with redirect_stdout(io.StringIO()):
        CommonGround.cli.main(["--help"], stdout=io.StringIO(), stderr=io.StringIO())
except SystemExit:
    pass

names = [
    "psycopg",
    "psycopg_pool",
    "cardbox",
    "cardbox.adapters.async_storage",
    "CommonGround.agent_client.http_client",
    "CommonGround.service.serialization",
    "CommonGround.service.http",
    "CommonGround.infra.postgres",
    "CommonGround.infra.repositories",
    "CommonGround.infra.postgres_pool",
    "Integrations.admin_service.project_setup",
    "Integrations.nanobot.runtime.leaf_worker_runner",
    "Integrations.nanobot.runtime.provisioner_runner",
]
print(json.dumps({name: name in sys.modules for name in names}, sort_keys=True))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    loaded = json.loads(result.stdout)
    assert loaded == {
        "CommonGround.agent_client.http_client": False,
        "CommonGround.infra.postgres": False,
        "CommonGround.infra.postgres_pool": False,
        "CommonGround.infra.repositories": False,
        "CommonGround.service.http": False,
        "CommonGround.service.serialization": False,
        "Integrations.admin_service.project_setup": False,
        "Integrations.nanobot.runtime.leaf_worker_runner": False,
        "Integrations.nanobot.runtime.provisioner_runner": False,
        "cardbox": False,
        "cardbox.adapters.async_storage": False,
        "psycopg": False,
        "psycopg_pool": False,
    }


def test_local_admission_app_builds_without_echoing_tokens(tmp_path, monkeypatch) -> None:
    admin_service_token_file = tmp_path / "admin-service.cgac"
    admin_auth_token_file = tmp_path / "admin-api.token"
    admin_service_token_file.write_text("cgac_cred.admin_secret\n", encoding="utf-8")
    admin_auth_token_file.write_text("admin-api-secret\n", encoding="utf-8")
    admin_service_token_file.chmod(0o600)
    admin_auth_token_file.chmod(0o600)

    class FakeFacade:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    monkeypatch.setattr("Integrations.admin_service.admission_runner.AdminServiceByoaFacade", FakeFacade)
    settings = LocalAdmissionSettings(
        pg_dsn="postgresql://postgres:secret@127.0.0.1/db",
        base_url="http://cg.example",
        project_id="demo",
        admin_service_token_file=admin_service_token_file,
        admin_auth_token_file=admin_auth_token_file,
    )

    app = create_local_admission_app(settings)

    assert app.title == "CommonGround Admin Service Admission API"
    assert "admin-api-secret" not in repr(app)
    assert "cgac_cred.admin_secret" not in repr(app)
