from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.error import URLError
from urllib.request import urlopen

import psycopg

from CommonGround.agent_credentials import AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.agent_registration import AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT, AGENT_REGISTRATION_BIRTH_GRANT
from CommonGround.app import build_kernel_app
from CommonGround.agent_client import HttpAgentClient, agent_auth_headers
from CommonGround.contracts import (
    AgentRef,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
)
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.projection_client.http_client import ProjectionHttpClient

DEFAULT_PERSONAL_PROVIDER = "gemini"
DEFAULT_PERSONAL_MODEL = "gemini-flash-latest"


@dataclass(frozen=True, slots=True)
class ResolvedProviderSettings:
    provider: str
    model: str
    api_key: str
    api_base: str | None


def _build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Run the multi-hop agent handoff CommonGround companion E2E.")
    parser.add_argument("--pg-dsn", default=os.environ.get("PG_DSN"), help="PostgreSQL DSN")
    parser.add_argument("--project-id", default=os.environ.get("CG_PROJECT_ID", "cg-demo"))
    parser.add_argument("--base-url", default=os.environ.get("CG_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--tmp-root", default="/tmp/cg-multihop-agent-handoff-e2e")
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--nanobot-repo", default=None, help="Optional NanoBot source checkout. Defaults to this repo with the nanobot extra installed.")
    parser.add_argument(
        "--payload-file",
        default=str(
            repo_root
            / "examples"
            / "nanobot"
            / "multihop_agent_handoff_demo"
            / "request_samples"
            / "root_request_payload.json"
        ),
    )
    parser.add_argument(
        "--personal-provider",
        default=None,
        help=(
            "Personal-agent provider. Defaults to ~/.nanobot/config.json "
            "agents.defaults/provider resolution, then falls back to gemini."
        ),
    )
    parser.add_argument(
        "--personal-model",
        default=None,
        help=(
            "Personal-agent model. Defaults to ~/.nanobot/config.json "
            "agents.defaults.model, then falls back to gemini-flash-latest."
        ),
    )
    parser.add_argument("--personal-provider-api-key", default=None)
    parser.add_argument("--personal-provider-api-base", default=None)
    parser.add_argument("--expert-provider", default=None)
    parser.add_argument("--expert-model", default=None)
    parser.add_argument("--expert-provider-api-key", default=None)
    parser.add_argument("--expert-provider-api-base", default=None)
    parser.add_argument(
        "--demo-repo-url",
        default="file:///tmp/demo-site.git",
        help="Local demo repo URL to bootstrap for the single-hop coding scenario.",
    )
    return parser


def load_args() -> argparse.Namespace:
    parser = _build_parser()
    args = parser.parse_args()
    if not args.pg_dsn:
        parser.error("--pg-dsn or PG_DSN is required")
    return args


def _load_nanobot_config(config_path: Path | None = None):
    from nanobot.config.loader import load_config, resolve_config_env_vars

    return resolve_config_env_vars(load_config(config_path))


def _load_nanobot_config_payload(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or Path.home() / ".nanobot" / "config.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _provider_alias(name: str) -> str:
    head, *tail = name.split("_")
    return head + "".join(part.title() for part in tail)


def _runtime_provider_api_base(provider: str, api_base: str | None) -> str | None:
    if not api_base:
        return None
    normalized = api_base.rstrip("/")
    if provider == "azure_openai" and normalized.endswith("/openai/v1"):
        return normalized[: -len("/openai/v1")]
    return api_base


def _load_raw_provider_settings(payload: dict[str, Any], provider: str) -> tuple[str, str | None]:
    providers = payload.get("providers")
    if not isinstance(providers, dict):
        return "", None
    for key in (provider, _provider_alias(provider)):
        candidate = providers.get(key)
        if isinstance(candidate, dict):
            api_key = candidate.get("apiKey")
            api_base = candidate.get("apiBase")
            return str(api_key or ""), str(api_base) if api_base else None
    return "", None


def _resolve_provider_settings(
    *,
    provider_arg: str | None,
    model_arg: str | None,
    api_key_arg: str | None,
    api_base_arg: str | None,
    config_path: Path | None = None,
) -> ResolvedProviderSettings:
    config = _load_nanobot_config(config_path)
    raw_payload = _load_nanobot_config_payload(config_path)
    resolved = config.model_copy(deep=True)
    candidate_model = model_arg or resolved.agents.defaults.model or DEFAULT_PERSONAL_MODEL
    if provider_arg:
        resolved.agents.defaults.provider = provider_arg
    if model_arg:
        resolved.agents.defaults.model = model_arg
    candidate_provider = resolved.get_provider_name(candidate_model)
    if not candidate_provider:
        candidate_provider = provider_arg or DEFAULT_PERSONAL_PROVIDER
        if model_arg is None:
            candidate_model = DEFAULT_PERSONAL_MODEL
        resolved.agents.defaults.provider = candidate_provider
        resolved.agents.defaults.model = candidate_model
    resolved_api_key = resolved.get_api_key(candidate_model) or ""
    resolved_api_base = resolved.get_api_base(candidate_model)
    raw_api_key, raw_api_base = _load_raw_provider_settings(raw_payload, candidate_provider)
    return ResolvedProviderSettings(
        provider=candidate_provider,
        model=candidate_model,
        api_key=api_key_arg or resolved_api_key or raw_api_key,
        api_base=api_base_arg or resolved_api_base or raw_api_base,
    )


def _ensure_postgres_ready(pg_dsn: str) -> None:
    try:
        conn = psycopg.connect(pg_dsn)
    except psycopg.OperationalError as exc:
        raise SystemExit(
            "PostgreSQL is not reachable for the multi-hop demo runner. "
            "Start the local database first or pass a working --pg-dsn."
        ) from exc
    conn.close()


def _ensure_provider_ready(*, provider: str, model: str, api_key: str) -> None:
    from nanobot.providers.registry import find_by_name

    if api_key:
        return
    spec = find_by_name(provider)
    if spec is not None and (spec.is_oauth or spec.is_local or provider == "custom"):
        return
    raise SystemExit(
        f"Provider '{provider}' for model '{model}' is not configured with an API key. "
        "Pass --*-provider-api-key or configure ~/.nanobot/config.json first."
    )


def _wait_ready(url: str, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2.0) as response:
                if response.status == 200:
                    return
        except URLError as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"service did not become ready: {url} last_error={last_error}")


def _spawn(command: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> subprocess.Popen[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _terminate(process: subprocess.Popen[str] | None, timeout: float = 10.0) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5.0)


def _reset_db(repo_root: Path, pg_dsn: str) -> None:
    env = os.environ.copy()
    env["PG_DSN"] = pg_dsn
    subprocess.run(["uv", "run", "-m", "scripts.setup.reset_db"], cwd=repo_root, env=env, check=True)


def _register_demo_topology(pg_dsn: str, project_id: str) -> None:
    app = build_kernel_app(pg_dsn=pg_dsn)
    app.topology.register_agent(AgentRef(project_id, "frontside"), accepts_work=False)
    app.topology.register_agent(
        AgentRef(project_id, "nanobot_provisioner_alpha"),
        capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
        grants=(
            AGENT_REGISTRATION_BIRTH_GRANT,
            AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT,
            AGENT_CREDENTIAL_REVOKE_ANY_GRANT,
        ),
    )
    app.topology.register_agent(AgentRef(project_id, "nanobot_a"), capabilities=(TURN_KIND_CONVERSATION_V1,))
    app.topology.register_agent(AgentRef(project_id, "nanobot_b"), capabilities=(TURN_KIND_CONVERSATION_V1,))
    app.topology.register_agent(AgentRef(project_id, "codex_c"), capabilities=(TURN_KIND_CONVERSATION_V1, "coding"))


def _issue_demo_agent_tokens(pg_dsn: str, project_id: str, agent_ids: list[str]) -> dict[str, str]:
    store = PostgresAgentCredentialStore(pg_dsn)
    return {
        agent_id: store.issue_agent_credential(
            AgentRef(project_id, agent_id),
            provenance_kind="multihop_agent_handoff_e2e",
            provenance_ref=agent_id,
        ).token
        for agent_id in agent_ids
    }


def _build_client(*, base_url: str, auth_token: str | None, agent: AgentRef) -> HttpAgentClient:
    if not auth_token:
        raise ValueError("auth_token is required to build an Agent credential client")
    return HttpAgentClient(
        base_url=base_url,
        headers=agent_auth_headers(agent, auth_token),
    )


def _copy_workspace(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "sessions", ignore_errors=True)
    shutil.rmtree(dst / ".nanobot_cg_companion", ignore_errors=True)


def _write_runtime_config(
    src: Path,
    dst: Path,
    *,
    workspace: Path,
    path_append: Path,
    provider: str,
    model: str,
    api_key: str,
    api_base: str | None,
) -> None:
    payload = json.loads(src.read_text(encoding="utf-8"))
    payload.setdefault("agents", {}).setdefault("defaults", {})["workspace"] = str(workspace)
    exec_config = payload.get("tools", {}).get("exec") if isinstance(payload.get("tools"), dict) else None
    if isinstance(exec_config, dict):
        exec_config["pathAppend"] = str(path_append)
    payload["agents"]["defaults"]["provider"] = provider
    payload["agents"]["defaults"]["model"] = model
    provider_config = payload.setdefault("providers", {}).setdefault(provider, {})
    if api_key:
        provider_config["apiKey"] = api_key
    runtime_api_base = _runtime_provider_api_base(provider, api_base)
    if runtime_api_base:
        provider_config["apiBase"] = runtime_api_base
    dst.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_json(argv: list[str], *, cwd: Path, env: dict[str, str]) -> dict[str, Any]:
    completed = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, check=True)
    return json.loads(completed.stdout)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _bootstrap_demo_repo(repo_url: str, *, tmp_root: Path) -> str:
    parsed = urlparse(repo_url)
    if parsed.scheme != "file":
        return repo_url
    bare_repo_path = Path(parsed.path)
    source_repo_path = tmp_root / "demo_repo_src"
    if bare_repo_path.exists():
        shutil.rmtree(bare_repo_path)
    if source_repo_path.exists():
        shutil.rmtree(source_repo_path)
    bare_repo_path.parent.mkdir(parents=True, exist_ok=True)
    source_repo_path.mkdir(parents=True, exist_ok=True)

    (source_repo_path / "index.html").write_text(
        """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Demo Site</title>
  </head>
  <body>
    <main>
      <h1>Build with confidence</h1>
      <p>Launch updates without slowing down your team.</p>
    </main>
  </body>
</html>
""",
        encoding="utf-8",
    )
    (source_repo_path / "package.json").write_text(
        json.dumps(
            {
                "name": "demo-site",
                "private": True,
                "version": "1.0.0",
                "scripts": {"test": "python3 tests/smoke_test.py"},
            },
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    tests_dir = source_repo_path / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / "smoke_test.py").write_text(
        """from pathlib import Path
import re

html = Path("index.html").read_text(encoding="utf-8")
assert re.search(r"<h1>.*</h1>", html), "missing headline"
assert re.search(r"<p>.*</p>", html), "missing body copy"
print("smoke ok")
""",
        encoding="utf-8",
    )

    subprocess.run(["git", "init", "--initial-branch=main"], cwd=source_repo_path, check=True)
    subprocess.run(["git", "config", "user.name", "CG Demo"], cwd=source_repo_path, check=True)
    subprocess.run(["git", "config", "user.email", "cg-demo@example.com"], cwd=source_repo_path, check=True)
    subprocess.run(["git", "add", "."], cwd=source_repo_path, check=True)
    subprocess.run(["git", "commit", "-m", "Initial demo site"], cwd=source_repo_path, check=True)
    subprocess.run(["git", "clone", "--bare", str(source_repo_path), str(bare_repo_path)], check=True)
    return f"file://{bare_repo_path}"


def _replace_payload_placeholders(payload: Any, *, replacements: dict[str, str]) -> Any:
    if isinstance(payload, str):
        return replacements.get(payload, payload)
    if isinstance(payload, list):
        return [_replace_payload_placeholders(item, replacements=replacements) for item in payload]
    if isinstance(payload, dict):
        return {
            key: _replace_payload_placeholders(value, replacements=replacements)
            for key, value in payload.items()
        }
    return payload


def _prepare_payload_file(payload_file: Path, *, demo_repo_url: str, tmp_root: Path) -> Path:
    payload = json.loads(payload_file.read_text(encoding="utf-8"))
    payload = _replace_payload_placeholders(
        payload,
        replacements={"${DEMO_REPO_URL}": demo_repo_url},
    )
    prepared = tmp_root / f"prepared_{payload_file.name}"
    _write_json(prepared, payload)
    return prepared


def _service_bind_env(base_url: str) -> dict[str, str]:
    parsed = urlparse(base_url)
    host = parsed.hostname or "127.0.0.1"
    if parsed.port is not None:
        port = parsed.port
    elif parsed.scheme == "https":
        port = 443
    else:
        port = 80
    return {"CG_HOST": host, "CG_PORT": str(port)}


def _write_demo_context(*, workspace_b: Path, demo_repo_url: str) -> None:
    _write_json(
        workspace_b / "demo_context.json",
        {
            "repo": {
                "clone_url": demo_repo_url,
                "base_branch": "main",
            },
            "notes": {
                "source": "runner_seeded_fixture_context",
                "scope": "peer_personal_agent_repo_lookup_only",
            },
        },
    )


def _extract_lines(path: Path, patterns: list[str]) -> list[str]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    lines: list[str] = []
    for line in text.splitlines():
        if any(pattern in line for pattern in patterns):
            lines.append(line)
    return lines


def _first_match_index(lines: list[str], pattern: str) -> int | None:
    for index, line in enumerate(lines):
        if pattern in line:
            return index
    return None


def _discovery_before_first_dispatch(log_path: Path) -> bool | None:
    if not log_path.exists():
        return None
    lines = log_path.read_text(encoding="utf-8").splitlines()
    dispatch_index = _first_match_index(lines, "cg_dispatch_child")
    if dispatch_index is None:
        return None
    list_agents_index = _first_match_index(lines, "cg_list_agents")
    list_turn_offers_index = _first_match_index(lines, "cg_list_turn_offers")
    if list_agents_index is None or list_turn_offers_index is None:
        return None
    return list_agents_index < dispatch_index and list_turn_offers_index < dispatch_index


def _collect_evidence(*, service_log: Path, log_a: Path, log_b: Path, log_c: Path) -> dict[str, list[str]]:
    return {
        "nanobot_a": _extract_lines(
            log_a,
            [
                "You are executing a CommonGround turn.",
                "You are resuming a suspended CommonGround parent turn.",
                "Tool call:",
                "Response to cg:cg_companion:",
            ],
        ),
        "nanobot_b": _extract_lines(
            log_b,
            [
                "You are executing a CommonGround work order.",
                "Response to cg:cg_companion:",
            ],
        ),
        "codex_c": _extract_lines(
            log_c,
            [
                "You are executing a CommonGround work order.",
                "Tool call:",
                "Response to cg:cg_companion:",
            ],
        ),
        "service": _extract_lines(
            service_log,
            [
                "/turns%3Adispatch HTTP/1.1\" 200",
                "%3Asuspend HTTP/1.1\" 200",
                "%3Aresume HTTP/1.1\" 200",
                "%3Afinish HTTP/1.1\" 200",
            ],
        ),
    }


def _collect_workflow_evidence(*, log_a: Path) -> dict[str, list[str]]:
    return {
        "nanobot_a_selection_flow": _extract_lines(
            log_a,
            [
                "Tool call: cg_list_agents",
                "Tool call: cg_list_turn_offers",
                "Tool call: cg_get_agent",
                "Tool call: cg_dispatch_child",
            ],
        ),
    }


def _collect_workflow_checks(*, log_a: Path) -> dict[str, bool | None]:
    return {
        "nanobot_a_discovery_before_first_child_dispatch": _discovery_before_first_dispatch(log_a),
    }


def _build_summary(
    *,
    project_id: str,
    parent_turn_id: str,
    wait: dict[str, Any],
    lineage: dict[str, Any],
    child_turns: dict[str, dict[str, Any]],
    service_log: Path,
    log_a: Path,
    log_b: Path,
    log_c: Path,
    demo_repo_url: str,
    directory_snapshot: dict[str, Any],
) -> dict[str, Any]:
    return {
        "project_id": project_id,
        "parent_turn_id": parent_turn_id,
        "parent": wait["result"],
        "lineage": lineage["result"],
        "children": child_turns,
        "logs": {
            "service": str(service_log),
            "nanobot_a": str(log_a),
            "nanobot_b": str(log_b),
            "codex_c": str(log_c),
        },
        "demo_repo_url": demo_repo_url,
        "directory_snapshot": directory_snapshot,
        "evidence": _collect_evidence(
            service_log=service_log,
            log_a=log_a,
            log_b=log_b,
            log_c=log_c,
        ),
        "workflow_evidence": _collect_workflow_evidence(log_a=log_a),
        "workflow_checks": _collect_workflow_checks(log_a=log_a),
    }


def _wait_agents_registered(*, base_url: str, frontside_token: str, project_id: str, agent_ids: list[str], timeout_seconds: float = 20.0) -> None:
    client = _build_client(base_url=base_url, auth_token=frontside_token, agent=AgentRef(project_id, "frontside"))
    try:
        deadline = time.time() + timeout_seconds
        pending = set(agent_ids)
        while pending and time.time() < deadline:
            resolved = {agent_id for agent_id in pending if client.get_agent(AgentRef(project_id, agent_id)) is not None}
            pending -= resolved
            if pending:
                time.sleep(0.25)
        if pending:
            raise RuntimeError(f"agents did not register before timeout: {sorted(pending)}")
    finally:
        client.close()


def _demo_public_metadata_by_agent() -> dict[str, dict[str, Any]]:
    authority_modes = [{"mode": "root_request"}, {"mode": "child_derivation"}]
    return {
        "nanobot_a": {
            "ui": {"label": "nanobot_a"},
            "expertise": {
                "kind": "personal_orchestrator",
                "summary": "Personal agent that clarifies goals, resolves missing context, and dispatches the next hop through CommonGround.",
            },
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Handle root user requests, coordinate follow-up child turns, and finish the requester-visible result.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": authority_modes,
                    },
                    "input_contract": {
                        "required_fields": [],
                        "example_payload": {"objective": "Coordinate a delegated task using the provided context and constraints."},
                    },
                    "variants": {"agent_kind": "personal", "task_types": ["orchestration"]},
                }
            ],
        },
        "nanobot_b": {
            "ui": {"label": "nanobot_b"},
            "expertise": {
                "kind": "personal_peer",
                "summary": "Peer personal agent that answers narrow delegated clarification questions or supplies missing context.",
            },
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Answer delegated clarification or information-gathering requests from another personal agent.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": authority_modes,
                    },
                    "input_contract": {
                        "required_fields": [],
                        "example_payload": {"objective": "Answer a delegated clarification request using the available context."},
                    },
                    "variants": {"agent_kind": "personal", "task_types": ["information_gathering"]},
                    "notes": "The peer helper may use fixture-local context when it exists, but the delegated task still arrives through the broad work-order envelope.",
                }
            ],
        },
        "codex_c": {
            "ui": {"label": "codex_c"},
            "expertise": {
                "kind": "expert",
                "summary": "Example expert agent for delegated coding work in this fixture project; similar demos could substitute another expert agent.",
            },
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Handle delegated expert work orders when the parent agent has enough context to hand off the task.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": authority_modes,
                    },
                    "input_contract": {
                        "required_fields": [],
                        "example_payload": {
                            "objective": "Complete the delegated task using the provided context, constraints, and available resources."
                        },
                    },
                    "variants": {"agent_kind": "expert", "domains": ["coding"], "execution_modes": ["local_only"]},
                    "notes": "This offer uses the same broad work-order envelope as other agents. Domain-specific details stay in the payload and are interpreted by the expert agent.",
                },
                {
                    "turn_kind": "coding",
                    "purpose": "Fixture-specific discoverability label for delegated coding work in this project.",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": authority_modes,
                    },
                    "input_contract": {
                        "required_fields": [],
                        "example_payload": {
                            "objective": "Update a repository using the provided task details and constraints."
                        },
                    },
                    "variants": {
                        "agent_kind": "expert",
                        "domains": ["coding"],
                        "execution_modes": ["local_only"],
                        "advisory_only": True,
                    },
                    "notes": "The `coding` turn kind is a fixture label for discovery. It does not introduce a platform-level special contract; the delegated task still uses the broad work-order envelope.",
                },
            ],
        },
    }


def _seed_agent_public_metadata(*, base_url: str, agent_tokens: dict[str, str], project_id: str) -> None:
    metadata_by_agent = _demo_public_metadata_by_agent()
    for agent_id, public_metadata in metadata_by_agent.items():
        agent = AgentRef(project_id, agent_id)
        client = _build_client(base_url=base_url, auth_token=agent_tokens[agent_id], agent=agent)
        try:
            client.update_agent_public_metadata(agent, public_metadata=public_metadata)
        finally:
            client.close()


def _snapshot_project_directory(*, base_url: str, frontside_token: str, project_id: str) -> dict[str, Any]:
    client = ProjectionHttpClient(
        base_url=base_url,
        auth_token=frontside_token,
        agent=AgentRef(project_id, "frontside"),
    )
    try:
        agents = client.list_agents(project_id=project_id, enabled_only=True, accepts_work_only=True, limit=50)
        offers = client.list_turn_offers(project_id=project_id, enabled_only=True, accepts_work_only=True, limit=50)
    finally:
        client.close()
    return {
        "agents": [
            {
                "agent_id": item.agent_id,
                "accepts_work": item.accepts_work,
                "enabled": item.enabled,
                "capabilities": list(item.capabilities),
            }
            for item in agents.items
        ],
        "turn_offers": [
            {
                "agent_id": item.agent_id,
                "turn_kind": item.turn_kind,
                "enabled": item.enabled,
                "accepts_work": item.accepts_work,
            }
            for item in offers.items
        ],
    }


def _child_turn_ids(lineage: dict[str, Any]) -> list[str]:
    children = lineage.get("result", {}).get("direct_children", [])
    if not isinstance(children, list):
        return []
    ids: list[str] = []
    for item in children:
        if isinstance(item, dict):
            turn_id = item.get("turn_id")
            if isinstance(turn_id, str):
                ids.append(turn_id)
    return ids


def _extract_context_payload(content: dict[str, Any]) -> Any:
    if "data" in content:
        return content["data"]
    cards = content.get("cards")
    if not isinstance(cards, list) or not cards:
        return content
    card = cards[0]
    if not isinstance(card, dict):
        return content
    inner = card.get("content")
    if not isinstance(inner, dict):
        return content
    if inner.get("__type__") == "JsonContent":
        return inner.get("data")
    if inner.get("__type__") == "TextContent":
        return inner.get("text")
    return inner


def _semantic_records_from_context(context_envelope: dict[str, Any]) -> list[dict[str, Any]]:
    result = context_envelope.get("result", {})
    items = result.get("semantic_items", [])
    if not isinstance(items, list):
        return []
    records: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        record = item.get("record")
        content = item.get("content")
        if not isinstance(record, dict) or not isinstance(content, dict):
            continue
        ref = record.get("ref") if isinstance(record.get("ref"), dict) else {}
        records.append(
            {
                "record_id": ref.get("record_id"),
                "turn_seq": record.get("turn_seq"),
                "record_role": record.get("record_role"),
                "payload": _extract_context_payload(content),
            }
        )
    return records


def main() -> int:
    args = load_args()
    repo_root = Path(__file__).resolve().parents[2]
    nanobot_cwd = Path(args.nanobot_repo).resolve() if args.nanobot_repo else repo_root
    tmp_root = Path(args.tmp_root).resolve()
    tmp_root.mkdir(parents=True, exist_ok=True)
    personal_settings = _resolve_provider_settings(
        provider_arg=args.personal_provider,
        model_arg=args.personal_model,
        api_key_arg=args.personal_provider_api_key,
        api_base_arg=args.personal_provider_api_base,
    )
    expert_settings = _resolve_provider_settings(
        provider_arg=args.expert_provider or personal_settings.provider,
        model_arg=args.expert_model or personal_settings.model,
        api_key_arg=args.expert_provider_api_key,
        api_base_arg=args.expert_provider_api_base,
    )
    _ensure_postgres_ready(args.pg_dsn)
    _ensure_provider_ready(
        provider=personal_settings.provider,
        model=personal_settings.model,
        api_key=personal_settings.api_key,
    )
    _ensure_provider_ready(
        provider=expert_settings.provider,
        model=expert_settings.model,
        api_key=expert_settings.api_key,
    )
    bootstrapped_repo_url = _bootstrap_demo_repo(args.demo_repo_url, tmp_root=tmp_root)
    prepared_payload_file = _prepare_payload_file(
        Path(args.payload_file).resolve(),
        demo_repo_url=bootstrapped_repo_url,
        tmp_root=tmp_root,
    )

    demo_root = repo_root / "examples" / "nanobot" / "multihop_agent_handoff_demo"
    workspace_a = tmp_root / "workspace_a"
    workspace_b = tmp_root / "workspace_b"
    workspace_c = tmp_root / "workspace_c"
    _copy_workspace(demo_root / "workspace_a", workspace_a)
    _copy_workspace(demo_root / "workspace_b", workspace_b)
    _copy_workspace(demo_root / "workspace_c", workspace_c)
    _write_demo_context(workspace_b=workspace_b, demo_repo_url=bootstrapped_repo_url)

    config_a = tmp_root / "nanobot_a.config.json"
    config_b = tmp_root / "nanobot_b.config.json"
    config_c = tmp_root / "codex_c.config.json"
    _write_runtime_config(
        demo_root / "nanobot_a.config.json",
        config_a,
        workspace=workspace_a,
        path_append=repo_root / ".venv" / "bin",
        provider=personal_settings.provider,
        model=personal_settings.model,
        api_key=personal_settings.api_key,
        api_base=personal_settings.api_base,
    )
    _write_runtime_config(
        demo_root / "nanobot_b.config.json",
        config_b,
        workspace=workspace_b,
        path_append=repo_root / ".venv" / "bin",
        provider=personal_settings.provider,
        model=personal_settings.model,
        api_key=personal_settings.api_key,
        api_base=personal_settings.api_base,
    )
    _write_runtime_config(
        demo_root / "codex_c.config.json",
        config_c,
        workspace=workspace_c,
        path_append=repo_root / ".venv" / "bin",
        provider=expert_settings.provider,
        model=expert_settings.model,
        api_key=expert_settings.api_key,
        api_base=expert_settings.api_base,
    )

    _reset_db(repo_root, args.pg_dsn)
    service_env = os.environ.copy()
    service_env.update(
        {
            "PG_DSN": args.pg_dsn,
            "CG_BASE_URL": args.base_url,
            "CG_PROJECT_ID": args.project_id,
        }
    )
    service_env.update(_service_bind_env(args.base_url))

    processes: list[subprocess.Popen[str]] = []
    try:
        service_log = tmp_root / "service.log"
        service = _spawn(["uv", "run", "cg", "service", "run"], cwd=repo_root, env=service_env, log_path=service_log)
        processes.append(service)
        _wait_ready(f"{args.base_url}/readyz", timeout_seconds=15.0)

        _register_demo_topology(args.pg_dsn, args.project_id)
        agent_tokens = _issue_demo_agent_tokens(
            args.pg_dsn,
            args.project_id,
            ["frontside", "nanobot_provisioner_alpha", "nanobot_a", "nanobot_b", "codex_c"],
        )

        env_b = os.environ.copy()
        env_b.update(
            {
                "CG_BASE_URL": args.base_url,
                "CG_PROJECT_ID": args.project_id,
                "CG_AGENT_CREDENTIAL_TOKEN": agent_tokens["nanobot_b"],
            }
        )
        log_b = tmp_root / "nanobot_b.log"
        proc_b = _spawn(
            ["uv", "run", "nanobot", "gateway", "--port", "18802", "--config", str(config_b), "--workspace", str(workspace_b)],
            cwd=nanobot_cwd,
            env=env_b,
            log_path=log_b,
        )
        processes.append(proc_b)

        env_c = os.environ.copy()
        env_c.update(
            {
                "CG_BASE_URL": args.base_url,
                "CG_PROJECT_ID": args.project_id,
                "CG_AGENT_CREDENTIAL_TOKEN": agent_tokens["codex_c"],
            }
        )
        log_c = tmp_root / "codex_c.log"
        proc_c = _spawn(
            ["uv", "run", "nanobot", "gateway", "--port", "18803", "--config", str(config_c), "--workspace", str(workspace_c)],
            cwd=nanobot_cwd,
            env=env_c,
            log_path=log_c,
        )
        processes.append(proc_c)

        env_a = os.environ.copy()
        env_a.update(
            {
                "CG_BASE_URL": args.base_url,
                "CG_PROJECT_ID": args.project_id,
                "CG_AGENT_CREDENTIAL_TOKEN": agent_tokens["nanobot_a"],
            }
        )
        log_a = tmp_root / "nanobot_a.log"
        proc_a = _spawn(
            ["uv", "run", "nanobot", "gateway", "--port", "18801", "--config", str(config_a), "--workspace", str(workspace_a)],
            cwd=nanobot_cwd,
            env=env_a,
            log_path=log_a,
        )
        processes.append(proc_a)

        time.sleep(4.0)
        _wait_agents_registered(
            base_url=args.base_url,
            frontside_token=agent_tokens["frontside"],
            project_id=args.project_id,
            agent_ids=["nanobot_a", "nanobot_b", "codex_c"],
        )
        _seed_agent_public_metadata(
            base_url=args.base_url,
            agent_tokens=agent_tokens,
            project_id=args.project_id,
        )
        directory_snapshot = _snapshot_project_directory(
            base_url=args.base_url,
            frontside_token=agent_tokens["frontside"],
            project_id=args.project_id,
        )

        cg_env = os.environ.copy()
        cg_env.update(
            {
                "CG_BASE_URL": args.base_url,
                "CG_AGENT_CREDENTIAL_TOKEN": agent_tokens["frontside"],
                "CG_CALLER_PROJECT_ID": args.project_id,
                "CG_CALLER_AGENT_ID": "frontside",
            }
        )
        dispatch = _run_json(
            [
                str(repo_root / ".venv" / "bin" / "cg"),
                "dispatch",
                "--project-id",
                args.project_id,
                "--requested-by",
                "frontside",
                "--target-agent",
                "nanobot_a",
                "--turn-kind",
                "turn.conversation.v1",
                "--request-id",
                "multihop-agent-handoff-e2e",
                "--dispatch-key",
                "multihop-agent-handoff-e2e",
                "--payload-file",
                str(prepared_payload_file),
            ],
            cwd=repo_root,
            env=cg_env,
        )
        parent_turn_id = dispatch["result"]["turn_id"]
        wait = _run_json(
            [
                str(repo_root / ".venv" / "bin" / "cg"),
                "turn",
                "wait",
                "--project-id",
                args.project_id,
                "--turn-id",
                parent_turn_id,
                "--timeout-seconds",
                str(args.timeout_seconds),
                "--poll-interval-ms",
                "500",
            ],
            cwd=repo_root,
            env=cg_env,
        )
        lineage = _run_json(
            [
                str(repo_root / ".venv" / "bin" / "cg"),
                "project",
                "turn",
                "lineage",
                "--project-id",
                args.project_id,
                "--turn-id",
                parent_turn_id,
                "--limit",
                "100",
            ],
            cwd=repo_root,
            env=cg_env,
        )
        child_turns: dict[str, dict[str, Any]] = {}
        child_contexts: dict[str, dict[str, Any]] = {}
        for child_turn_id in _child_turn_ids(lineage):
            child_turns[child_turn_id] = _run_json(
                [
                    str(repo_root / ".venv" / "bin" / "cg"),
                    "turn",
                    "get",
                    "--project-id",
                    args.project_id,
                    "--turn-id",
                    child_turn_id,
                ],
                cwd=repo_root,
                env=cg_env,
            )
            child_contexts[child_turn_id] = _run_json(
                [
                    str(repo_root / ".venv" / "bin" / "cg"),
                    "turn",
                    "context",
                    "--project-id",
                    args.project_id,
                    "--turn-id",
                    child_turn_id,
                    "--limit",
                    "100",
                ],
                cwd=repo_root,
                env=cg_env,
            )
        parent_context = _run_json(
            [
                str(repo_root / ".venv" / "bin" / "cg"),
                "turn",
                "context",
                "--project-id",
                args.project_id,
                "--turn-id",
                parent_turn_id,
                "--limit",
                "100",
            ],
            cwd=repo_root,
            env=cg_env,
        )

        summary = _build_summary(
            project_id=args.project_id,
            parent_turn_id=parent_turn_id,
            wait=wait,
            lineage=lineage,
            child_turns=child_turns,
            service_log=service_log,
            log_a=log_a,
            log_b=log_b,
            log_c=log_c,
            demo_repo_url=bootstrapped_repo_url,
            directory_snapshot=directory_snapshot,
        )
        summary["turn_records"] = {
            "parent": _semantic_records_from_context(parent_context),
            "children": {
                turn_id: _semantic_records_from_context(context)
                for turn_id, context in child_contexts.items()
            },
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        for process in reversed(processes):
            _terminate(process)


if __name__ == "__main__":
    raise SystemExit(main())
