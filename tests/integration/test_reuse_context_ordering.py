from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import httpx
import pytest
import toml
import uuid6
import yaml

from core.app_config import config_to_dict, load_app_config
from core.config import PROTOCOL_VERSION
from core.headers import CG_AGENT_ID, CG_AGENT_TURN_ID, CG_STEP_ID, CG_TOOL_CALL_ID, CG_TURN_EPOCH, ensure_recursion_depth
from core.trace import ensure_trace_headers
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from core.utp_protocol import Card, ToolCallContent
from scripts.utils.nats import subscribe_task_queue
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
    resp = await client.post(
        "/projects",
        json={
            "project_id": project_id,
            "title": title,
            "owner_id": owner_id,
            "bootstrap": bootstrap,
        },
    )
    if resp.status_code == 409:
        return
    resp.raise_for_status()


async def _upload_profile(client: httpx.AsyncClient, *, project_id: str, path: Path) -> str:
    raw_text = path.read_text(encoding="utf-8")
    profile_data = yaml.safe_load(raw_text)
    if isinstance(profile_data, dict):
        profile_data = apply_default_llm_config(profile_data)
        raw_text = yaml.safe_dump(profile_data, allow_unicode=True)
    resp = await client.post(
        f"/projects/{project_id}/profiles",
        files={"file": (path.name, raw_text, "text/yaml")},
    )
    resp.raise_for_status()
    payload = resp.json()
    return str(payload.get("name") or "")


def _write_delegate_caller_profile(*, target_profile_name: str) -> Path:
    profile = {
        "name": "Reuse_Test_Caller",
        "worker_target": "worker_generic",
        "tags": ["principal"],
        "description": "Caller profile for reuse ordering integration.",
        "system_prompt_template": "You are only used as a caller identity in integration tests.",
        "allowed_tools": [],
        "allowed_internal_tools": [],
        "must_end_with": [],
        "delegation_policy": {
            "target_profiles": [str(target_profile_name)],
            "target_tags": [],
        },
        "llm_config": {
            "model": "mock-direct",
        },
    }
    fd, path_str = tempfile.mkstemp(prefix="reuse_test_caller_", suffix=".yaml")
    os.close(fd)
    path = Path(path_str)
    path.write_text(yaml.safe_dump(profile, allow_unicode=True), encoding="utf-8")
    return path


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
    resp = await client.post(
        f"/projects/{project_id}/agents",
        json={
            "agent_id": agent_id,
            "profile_name": profile_name,
            "worker_target": worker_target,
            "tags": tags,
            "display_name": display_name,
            "owner_agent_id": owner_agent_id,
            "metadata": metadata,
            "init_state": True,
            "channel_id": channel_id,
        },
    )
    resp.raise_for_status()


async def _wait_for_next_task_event(queue: asyncio.Queue, *, timeout_s: float) -> Dict[str, Any]:
    return await asyncio.wait_for(queue.get(), timeout=timeout_s)


async def _publish_pmo_internal_tool(
    *,
    cardbox: CardBoxClient,
    nats: NATSClient,
    project_id: str,
    channel_id: str,
    caller_agent_id: str,
    tool_name: str,
    tool_args: Dict[str, Any],
    after_execution: str,
) -> str:
    tool_call_id = f"call_{uuid6.uuid7().hex}"
    step_id = f"step_{uuid6.uuid7().hex}"
    agent_turn_id = f"turn_{uuid6.uuid7().hex}"
    trace_id = str(uuid6.uuid7())
    subject = f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.pmo.internal.{tool_name}"
    tool_call_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="tool.call",
        content=ToolCallContent(
            tool_name=tool_name,
            arguments=tool_args,
            status="called",
            target_subject=subject,
        ),
        created_at=datetime.now(UTC),
        author_id=caller_agent_id,
        metadata={
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "role": "assistant",
            "trace_id": trace_id,
        },
        tool_call_id=tool_call_id,
    )
    await cardbox.save_card(tool_call_card)
    headers, _, _ = ensure_trace_headers({}, trace_id=trace_id)
    headers = ensure_recursion_depth(headers, default_depth=0)
    headers[CG_AGENT_ID] = caller_agent_id
    headers[CG_AGENT_TURN_ID] = agent_turn_id
    headers[CG_TURN_EPOCH] = "1"
    headers[CG_STEP_ID] = step_id
    headers[CG_TOOL_CALL_ID] = tool_call_id
    await nats.publish_event(
        subject,
        {
            "tool_name": tool_name,
            "after_execution": after_execution,
            "tool_call_card_id": tool_call_card.card_id,
        },
        headers=headers,
    )
    return tool_call_id


def _extract_deliverable_output_value(card: Any) -> str:
    content = getattr(card, "content", None)
    data = getattr(content, "data", None) if content is not None else None
    if not isinstance(data, dict):
        return ""
    fields = data.get("fields") or []
    if not isinstance(fields, list):
        return ""
    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("name") or "").strip() == "output":
            return str(field.get("value") or "")
    return ""


def _runtime_defaults() -> Tuple[Dict[str, Any], str]:
    cfg = config_to_dict(load_app_config())
    api_url = str(os.environ.get("API_URL") or _api_base_url(_load_cfg()))
    return cfg, api_url


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delegate_async_reuse_preserves_latest_user_instruction_order() -> None:
    cfg, api_url = _runtime_defaults()
    cardbox_cfg = cfg.get("cardbox") if isinstance(cfg, dict) else {}
    dsn = str((cardbox_cfg or {}).get("postgres_dsn") or "")
    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}
    _ = dsn

    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=5.0) as client:
            resp = await client.get("/health")
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"API not reachable for reuse ordering integration: {exc}", pytrace=False)

    project_id = f"proj_reuse_delegate_{uuid6.uuid7().hex[-8:]}"
    channel_id = "public"
    owner_id = "owner_reuse_test"
    caller_agent_id = "principal_reuse_it"
    target_agent_id = "chat_reuse_it"
    target_profile_path = ROOT / "tests" / "fixtures" / "profiles" / "mock_simple_assistant.yaml"
    caller_profile_path: Path | None = None

    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        await _ensure_project(
            client,
            project_id=project_id,
            title="Reuse Delegate Ordering",
            owner_id=owner_id,
            bootstrap=True,
        )
        target_profile_name = await _upload_profile(client, project_id=project_id, path=target_profile_path)
        caller_profile_path = _write_delegate_caller_profile(target_profile_name=target_profile_name)
        caller_profile_name = await _upload_profile(client, project_id=project_id, path=caller_profile_path)
        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=caller_agent_id,
            profile_name=caller_profile_name,
            worker_target="worker_generic",
            tags=["caller"],
            display_name="Caller",
            owner_agent_id=owner_id,
            metadata={},
        )
        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=target_agent_id,
            profile_name=target_profile_name,
            worker_target="worker_generic",
            tags=["target"],
            display_name="Target",
            owner_agent_id=owner_id,
            metadata={},
        )

    cardbox = CardBoxClient(config=cardbox_cfg)
    nats = NATSClient(config=nats_cfg)
    await cardbox.init()
    await nats.connect()
    task_queue = await subscribe_task_queue(
        nats,
        project_id=project_id,
        channel_id=channel_id,
        agent_id=target_agent_id,
    )

    try:
        await _publish_pmo_internal_tool(
            cardbox=cardbox,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=caller_agent_id,
            tool_name="delegate_async",
            tool_args={
                "target_strategy": "reuse",
                "target_ref": target_agent_id,
                "instruction": "1 + 1",
            },
            after_execution="suspend",
        )
        first_task = await _wait_for_next_task_event(task_queue, timeout_s=25.0)
        first_deliverable_id = str(first_task.get("deliverable_card_id") or "")
        first_card = (await cardbox.get_cards([first_deliverable_id], project_id=project_id))[0]
        assert _extract_deliverable_output_value(first_card) == "2"
        first_output_box_id = str(first_task.get("output_box_id") or "")
        assert first_output_box_id

        await _publish_pmo_internal_tool(
            cardbox=cardbox,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=caller_agent_id,
            tool_name="delegate_async",
            tool_args={
                "target_strategy": "reuse",
                "target_ref": target_agent_id,
                "instruction": "2 + 3",
            },
            after_execution="suspend",
        )
        second_task = await _wait_for_next_task_event(task_queue, timeout_s=25.0)
        second_deliverable_id = str(second_task.get("deliverable_card_id") or "")
        second_card = (await cardbox.get_cards([second_deliverable_id], project_id=project_id))[0]
        assert _extract_deliverable_output_value(second_card) == "5"
        assert str(second_task.get("output_box_id") or "") == first_output_box_id
    finally:
        if caller_profile_path is not None:
            caller_profile_path.unlink(missing_ok=True)
        await cardbox.close()
        await nats.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fork_join_reuse_preserves_latest_user_instruction_order() -> None:
    cfg, api_url = _runtime_defaults()
    cardbox_cfg = cfg.get("cardbox") if isinstance(cfg, dict) else {}
    dsn = str((cardbox_cfg or {}).get("postgres_dsn") or "")
    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}
    _ = dsn

    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=5.0) as client:
            resp = await client.get("/health")
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"API not reachable for fork_join reuse integration: {exc}", pytrace=False)

    project_id = f"proj_reuse_fork_{uuid6.uuid7().hex[-8:]}"
    channel_id = "public"
    owner_id = "owner_fork_test"
    caller_agent_id = "principal_fork_it"
    target_agent_id = "chat_fork_it"
    target_profile_path = ROOT / "tests" / "fixtures" / "profiles" / "mock_simple_assistant.yaml"
    caller_profile_path: Path | None = None

    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        await _ensure_project(
            client,
            project_id=project_id,
            title="Reuse Fork Ordering",
            owner_id=owner_id,
            bootstrap=True,
        )
        target_profile_name = await _upload_profile(client, project_id=project_id, path=target_profile_path)
        caller_profile_path = _write_delegate_caller_profile(target_profile_name=target_profile_name)
        caller_profile_name = await _upload_profile(client, project_id=project_id, path=caller_profile_path)
        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=caller_agent_id,
            profile_name=caller_profile_name,
            worker_target="worker_generic",
            tags=["caller"],
            display_name="Caller",
            owner_agent_id=owner_id,
            metadata={},
        )
        await _upsert_agent(
            client,
            project_id=project_id,
            channel_id=channel_id,
            agent_id=target_agent_id,
            profile_name=target_profile_name,
            worker_target="worker_generic",
            tags=["target"],
            display_name="Target",
            owner_agent_id=owner_id,
            metadata={},
        )

    cardbox = CardBoxClient(config=cardbox_cfg)
    nats = NATSClient(config=nats_cfg)
    await cardbox.init()
    await nats.connect()
    task_queue = await subscribe_task_queue(
        nats,
        project_id=project_id,
        channel_id=channel_id,
        agent_id=target_agent_id,
    )

    try:
        await _publish_pmo_internal_tool(
            cardbox=cardbox,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=caller_agent_id,
            tool_name="delegate_async",
            tool_args={
                "target_strategy": "reuse",
                "target_ref": target_agent_id,
                "instruction": "1 + 1",
            },
            after_execution="suspend",
        )
        seed_task = await _wait_for_next_task_event(task_queue, timeout_s=25.0)
        seed_deliverable_id = str(seed_task.get("deliverable_card_id") or "")
        seed_card = (await cardbox.get_cards([seed_deliverable_id], project_id=project_id))[0]
        assert _extract_deliverable_output_value(seed_card) == "2"

        await _publish_pmo_internal_tool(
            cardbox=cardbox,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=caller_agent_id,
            tool_name="fork_join",
            tool_args={
                "tasks": [
                    {
                        "target_strategy": "reuse",
                        "target_ref": target_agent_id,
                        "instruction": "2 + 3",
                    }
                ]
            },
            after_execution="suspend",
        )
        child_task = await _wait_for_next_task_event(task_queue, timeout_s=25.0)
        child_deliverable_id = str(child_task.get("deliverable_card_id") or "")
        child_card = (await cardbox.get_cards([child_deliverable_id], project_id=project_id))[0]
        assert _extract_deliverable_output_value(child_card) == "5"
    finally:
        if caller_profile_path is not None:
            caller_profile_path.unlink(missing_ok=True)
        await cardbox.close()
        await nats.close()
