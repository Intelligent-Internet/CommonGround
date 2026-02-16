import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import httpx
import pytest
import toml
import uuid6
import yaml

from core.config import PROTOCOL_VERSION
from core.headers import ensure_recursion_depth
from core.time_utils import to_iso, utc_now
from core.trace import ensure_trace_headers
from infra.nats_client import NATSClient
from tests.integration._llm_defaults import apply_default_llm_config

ROOT = Path(__file__).resolve().parents[2]


def _load_cfg() -> Dict[str, Any]:
    cfg_path = ROOT / "config.toml"
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


def _api_base_url(cfg: Dict[str, Any]) -> str:
    api_cfg = cfg.get("api", {}) if isinstance(cfg, dict) else {}
    host = str(api_cfg.get("listen_host") or "127.0.0.1")
    port = int(api_cfg.get("port") or 8099)
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    return f"http://{host}:{port}"


async def _ensure_project(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    title: str,
    owner_id: str,
    bootstrap: bool,
) -> None:
    payload = {
        "project_id": project_id,
        "title": title,
        "owner_id": owner_id,
        "bootstrap": bootstrap,
    }
    resp = await client.post("/projects", json=payload)
    if resp.status_code == 409:
        return
    resp.raise_for_status()


async def _upload_profile(client: httpx.AsyncClient, *, project_id: str, path: Path) -> str:
    raw_text = path.read_text(encoding="utf-8")
    profile_data = yaml.safe_load(raw_text)
    if isinstance(profile_data, dict):
        profile_data = apply_default_llm_config(profile_data)
        raw_text = yaml.safe_dump(profile_data, allow_unicode=True)
    files = {"file": (path.name, raw_text, "text/yaml")}
    resp = await client.post(f"/projects/{project_id}/profiles", files=files)
    resp.raise_for_status()
    payload = resp.json()
    return str(payload.get("name") or "")


async def _upsert_agent(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    channel_id: str,
    agent_id: str,
    profile_name: str,
    worker_target: str,
    tags: list[str],
    display_name: str,
    owner_agent_id: str,
    metadata: Dict[str, Any],
) -> None:
    payload = {
        "agent_id": agent_id,
        "profile_name": profile_name,
        "worker_target": worker_target,
        "tags": tags,
        "display_name": display_name,
        "owner_agent_id": owner_agent_id,
        "metadata": metadata,
        "init_state": True,
        "channel_id": channel_id,
    }
    resp = await client.post(f"/projects/{project_id}/agents", json=payload)
    resp.raise_for_status()


async def _subscribe_event(
    nats: NATSClient,
    *,
    subject: str,
    queue_name: str,
    event_queue: asyncio.Queue,
) -> None:
    async def _on_event(_subject: str, data: Dict[str, Any], headers: Dict[str, str]) -> None:
        await event_queue.put((data, headers))

    await nats.subscribe_cmd(
        subject,
        queue_name,
        _on_event,
        durable_name=queue_name,
        deliver_policy="new",
    )


async def _wait_for_event(
    event_queue: asyncio.Queue,
    *,
    timeout_s: float,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    return await asyncio.wait_for(event_queue.get(), timeout=timeout_s)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ui_action_flow():
    cfg = _load_cfg()
    api_url = str(os.environ.get("API_URL") or _api_base_url(cfg))

    ui_profile_path = ROOT / "examples" / "profiles" / "ui.yaml"
    chat_profile_path = ROOT / "tests" / "fixtures" / "profiles" / "mock_chat_assistant.yaml"
    if not ui_profile_path.exists() or not chat_profile_path.exists():
        pytest.skip("profile yaml missing for integration test")

    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=5.0) as client:
            resp = await client.get("/health")
            resp.raise_for_status()
    except Exception:
        pytest.skip("API not reachable; start services.api before running integration tests")

    project_id = f"proj_ui_chat_it_{uuid6.uuid7().hex[:8]}"
    channel_id = "public"
    owner_id = "user_demo"
    ui_agent_id = "ui_user_it"
    chat_agent_id = "chat_agent_it"

    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        await _ensure_project(
            client,
            project_id=project_id,
            title="UI Action Integration",
            owner_id=owner_id,
            bootstrap=True,
        )
        ui_profile_name = await _upload_profile(
            client, project_id=project_id, path=ui_profile_path
        )
        chat_profile_name = await _upload_profile(
            client, project_id=project_id, path=chat_profile_path
        )

        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=ui_agent_id,
            profile_name=ui_profile_name,
            worker_target="ui_worker",
            tags=["ui"],
            display_name="UI Session",
            owner_agent_id=owner_id,
            metadata={"is_ui_agent": True},
        )
        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=chat_agent_id,
            profile_name=chat_profile_name,
            worker_target="worker_generic",
            tags=["partner"],
            display_name="Chat Agent",
            owner_agent_id=owner_id,
            metadata={},
        )

    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}
    nats = NATSClient(config=nats_cfg)
    await nats.connect()

    ack_queue: asyncio.Queue = asyncio.Queue()
    task_queue: asyncio.Queue = asyncio.Queue()
    suffix = uuid6.uuid7().hex[:8]

    subject_ack = f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.sys.ui.action_ack"
    subject_task = f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.agent.{chat_agent_id}.task"

    await _subscribe_event(
        nats,
        subject=subject_ack,
        queue_name=f"test_ui_chat_ack_{suffix}",
        event_queue=ack_queue,
    )
    await _subscribe_event(
        nats,
        subject=subject_task,
        queue_name=f"test_ui_chat_task_{suffix}",
        event_queue=task_queue,
    )

    action_id = uuid6.uuid7().hex
    subject_action = f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.ui.action"
    payload = {
        "action_id": action_id,
        "agent_id": ui_agent_id,
        "tool_name": "delegate_async",
        "args": {
            "target_strategy": "reuse",
            "target_ref": chat_agent_id,
            "instruction": "hello ui integration",
        },
        "metadata": {
            "source": "test_ui_action_flow",
            "client_ts": to_iso(utc_now()),
        },
    }
    headers, _, _ = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
    headers = ensure_recursion_depth(headers, default_depth=0)

    try:
        await nats.publish_event(subject_action, payload, headers=headers)
        ack_payload, _ = await _wait_for_event(ack_queue, timeout_s=30.0)
        ack_status = str(ack_payload.get("status") or "")
        assert ack_status in ("accepted", "pending", "done")

        task_payload, _ = await _wait_for_event(task_queue, timeout_s=30.0)
        assert task_payload.get("status") == "success"
        deliverable_id = str(task_payload.get("deliverable_card_id") or "")
        assert deliverable_id

        async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
            resp = await client.get(f"/projects/{project_id}/cards/{deliverable_id}")
            resp.raise_for_status()
            content = resp.json().get("content") or {}
            fields = (content.get("data") or {}).get("fields") or []
            assert fields and fields[0].get("value") == "ok"
    finally:
        await nats.close()
