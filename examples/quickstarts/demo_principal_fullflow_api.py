"""
Demo: API-only full workflow launcher (no direct Postgres access).

It relies on:
1) API endpoint `/projects/{project_id}/god/pmo/{tool_name}:call`
2) Polling API state/box data for completion (no NATS dependency)

Usage:
  uv run -m examples.quickstarts.demo_principal_fullflow_api \
    --project proj_mvp_001 --channel public \
    "help me to do a research on k8s"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx
import uuid6

from scripts.utils.config import api_base_url, load_toml_config

REPO_ROOT = Path(__file__).resolve().parents[2]


def _launcher_profile_yaml(*, principal_profile_name: str) -> str:
    return f"""name: Demo_Launcher
worker_target: worker_generic
tags: [partner]
description: demo-only launcher profile for API fullflow demo
delegation_policy:
  target_tags: [principal]
  target_profiles: [{principal_profile_name}]
allowed_tools: [provision_agent, delegate_async]
"""


async def _upload_yaml(api: httpx.AsyncClient, endpoint: str, path: Path) -> None:
    with path.open("rb") as f:
        files = {"file": (path.name, f, "application/x-yaml")}
        resp = await api.post(endpoint, files=files)
    resp.raise_for_status()


async def _upload_yaml_text(api: httpx.AsyncClient, endpoint: str, filename: str, text: str) -> None:
    files = {"file": (filename, text.encode("utf-8"), "application/x-yaml")}
    resp = await api.post(endpoint, files=files)
    resp.raise_for_status()


async def _ensure_project(api: httpx.AsyncClient, project_id: str) -> None:
    r = await api.get(f"/projects/{project_id}")
    if r.status_code == 404:
        cr = await api.post(
            "/projects",
            json={
                "project_id": project_id,
                "title": "Demo Project",
                "owner_id": "demo_user",
                "bootstrap": True,
            },
        )
        cr.raise_for_status()
    elif r.status_code >= 400:
        r.raise_for_status()
    br = await api.post(f"/projects/{project_id}/bootstrap")
    br.raise_for_status()


async def _ensure_resources(api: httpx.AsyncClient, project_id: str, principal_profile_name: str) -> None:
    tool_yaml = REPO_ROOT / "examples" / "tools" / "web_search_tool.yaml"
    principal_yaml = REPO_ROOT / "examples" / "profiles" / "principal_planner_fullflow.yaml"
    associate_yaml = REPO_ROOT / "examples" / "profiles" / "associate_search.yaml"

    await _upload_yaml(api, f"/projects/{project_id}/tools", tool_yaml)
    await _upload_yaml(api, f"/projects/{project_id}/profiles", principal_yaml)
    await _upload_yaml(api, f"/projects/{project_id}/profiles", associate_yaml)
    await _upload_yaml_text(
        api,
        f"/projects/{project_id}/profiles",
        "demo_launcher.yaml",
        _launcher_profile_yaml(principal_profile_name=principal_profile_name),
    )


async def _call_pmo(api: httpx.AsyncClient, *, project_id: str, tool_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = await api.post(f"/projects/{project_id}/god/pmo/{tool_name}:call", json=payload)
    resp.raise_for_status()
    data = resp.json() or {}
    return data if isinstance(data, dict) else {}


async def _fetch_state_head(
    api: httpx.AsyncClient, *, project_id: str, agent_id: str
) -> Optional[Dict[str, Any]]:
    resp = await api.get(f"/projects/{project_id}/state/agents", params={"limit": 500})
    resp.raise_for_status()
    rows = resp.json() or []
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, dict) and str(row.get("agent_id") or "") == agent_id:
            return row
    return None


def _extract_deliverable_text(card: Dict[str, Any]) -> str:
    content = card.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str) and text.strip():
            return text
        data = content.get("data")
        if isinstance(data, dict):
            fields = data.get("fields")
            if isinstance(fields, list):
                out = []
                for field in fields:
                    if isinstance(field, dict):
                        name = str(field.get("name") or "")
                        value = str(field.get("value") or "")
                        if name and value:
                            out.append(f"{name}:\n{value}")
                if out:
                    return "\n\n".join(out)
        return json.dumps(content, ensure_ascii=False, indent=2)
    return json.dumps(content, ensure_ascii=False, indent=2)


async def _poll_deliverable(
    api: httpx.AsyncClient,
    *,
    project_id: str,
    agent_id: str,
    agent_turn_id: str,
    timeout_seconds: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        head = await _fetch_state_head(api, project_id=project_id, agent_id=agent_id)
        if head:
            status = str(head.get("status") or "")
            output_box_id = str(head.get("output_box_id") or "")
            print(
                f"[demo] polling: status={status} turn={head.get('active_agent_turn_id')} "
                f"output_box_id={output_box_id or '-'}"
            )
            if output_box_id:
                cards_resp = await api.get(f"/projects/{project_id}/boxes/{output_box_id}/cards", params={"limit": 500})
                if cards_resp.status_code < 400:
                    cards = cards_resp.json() or []
                    if isinstance(cards, list):
                        for card in reversed(cards):
                            if not isinstance(card, dict):
                                continue
                            if str(card.get("type") or "") == "task.deliverable":
                                return head, card
        await asyncio.sleep(2.0)
    return None, None


async def main() -> None:
    parser = argparse.ArgumentParser(description="API-only principal fullflow demo")
    parser.add_argument("--project", default="proj_mvp_001")
    parser.add_argument("--channel", default="public")
    parser.add_argument("--profile-name", default="Principal_Planner_FullFlow")
    parser.add_argument("--api-url", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("instruction")
    args = parser.parse_args()

    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    api_url = str(args.api_url or api_base_url(cfg))

    async with httpx.AsyncClient(base_url=api_url, timeout=60.0) as api:
        await _ensure_project(api, str(args.project))
        await _ensure_resources(api, str(args.project), str(args.profile_name))

        launcher_agent_id = f"demo_launcher_{uuid6.uuid7().hex[:12]}"
        upsert_resp = await api.post(
            f"/projects/{args.project}/agents",
            json={
                "agent_id": launcher_agent_id,
                "profile_name": "Demo_Launcher",
                "worker_target": "worker_generic",
                "tags": ["partner"],
                "display_name": launcher_agent_id,
                "metadata": {"demo": "principal_fullflow_api"},
                "init_state": True,
                "channel_id": str(args.channel),
            },
        )
        upsert_resp.raise_for_status()

        principal_agent_id = f"principal_{uuid6.uuid7().hex[:12]}"
        provision = await _call_pmo(
            api,
            project_id=str(args.project),
            tool_name="provision_agent",
            payload={
                "caller_agent_id": launcher_agent_id,
                "channel_id": str(args.channel),
                "args": {
                    "action": "create",
                    "agent_id": principal_agent_id,
                    "profile_name": str(args.profile_name),
                    "display_name": f"Principal ({args.profile_name})",
                },
                "after_execution": "suspend",
                "timeout_seconds": 120,
            },
        )
        print("[demo] provision_agent response:", json.dumps(provision, ensure_ascii=False, indent=2))
        if ((provision.get("tool_result") or {}).get("status")) != "success":
            raise RuntimeError(f"provision_agent failed: {json.dumps(provision, ensure_ascii=False)}")

        delegate = await _call_pmo(
            api,
            project_id=str(args.project),
            tool_name="delegate_async",
            payload={
                "caller_agent_id": launcher_agent_id,
                "channel_id": str(args.channel),
                "args": {
                    "target_strategy": "reuse",
                    "target_ref": principal_agent_id,
                    "instruction": str(args.instruction),
                },
                "after_execution": "suspend",
                "timeout_seconds": 180,
            },
        )
        print("[demo] delegate_async response:", json.dumps(delegate, ensure_ascii=False, indent=2))
        tool_result = delegate.get("tool_result") or {}
        if tool_result.get("status") != "success":
            raise RuntimeError(f"delegate_async failed: {json.dumps(delegate, ensure_ascii=False)}")
        result_obj = tool_result.get("result") or {}
        child_agent_turn_id = str(result_obj.get("agent_turn_id") or "")
        print(f"[demo] launched principal agent_id={principal_agent_id} agent_turn_id={child_agent_turn_id}")
        head, deliverable_card = await _poll_deliverable(
            api,
            project_id=str(args.project),
            agent_id=principal_agent_id,
            agent_turn_id=child_agent_turn_id,
            timeout_seconds=int(args.timeout_seconds),
        )
        if deliverable_card:
            print("\n\n[demo] ✅ deliverable:")
            print(_extract_deliverable_text(deliverable_card))
            return
        if head:
            print(f"\n[demo] timeout: last head={json.dumps(head, ensure_ascii=False, indent=2)}")
        else:
            print("\n[demo] timeout: no state head found for principal agent")


if __name__ == "__main__":
    asyncio.run(main())
