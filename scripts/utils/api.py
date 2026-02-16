from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import httpx


async def create_project(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    title: str,
    owner_id: str,
    bootstrap: Optional[bool] = None,
) -> None:
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "title": title,
        "owner_id": owner_id,
    }
    if bootstrap is not None:
        payload["bootstrap"] = bootstrap
    resp = await client.post("/projects", json=payload)
    if resp.status_code == 409:
        return
    if resp.status_code >= 400:
        raise RuntimeError(f"create_project failed: {resp.status_code} {resp.text}")


async def bootstrap_project(client: httpx.AsyncClient, *, project_id: str) -> None:
    resp = await client.post(f"/projects/{project_id}/bootstrap")
    if resp.status_code == 404:
        raise RuntimeError(f"project not found via api: {project_id}")
    if resp.status_code >= 400:
        raise RuntimeError(f"bootstrap failed: {resp.status_code} {resp.text}")


async def upload_yaml_path(
    client: httpx.AsyncClient,
    *,
    endpoint: str,
    yaml_path: Path,
    content_type: str = "application/x-yaml",
) -> Dict[str, Any]:
    if not yaml_path.exists():
        raise RuntimeError(f"yaml file not found: {yaml_path}")
    with yaml_path.open("rb") as f:
        files = {"file": (yaml_path.name, f, content_type)}
        resp = await client.post(endpoint, files=files)
    if resp.status_code >= 400:
        raise RuntimeError(f"upload failed: {resp.status_code} {resp.text}")
    data = resp.json() or {}
    return data if isinstance(data, dict) else {}


async def upload_yaml_text(
    client: httpx.AsyncClient,
    *,
    endpoint: str,
    filename: str,
    yaml_text: str,
    content_type: str = "application/x-yaml",
) -> Dict[str, Any]:
    files = {"file": (filename, yaml_text.encode("utf-8"), content_type)}
    resp = await client.post(endpoint, files=files)
    if resp.status_code >= 400:
        raise RuntimeError(f"upload failed: {resp.status_code} {resp.text}")
    data = resp.json() or {}
    return data if isinstance(data, dict) else {}


async def upload_profile_yaml(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    yaml_path: Path,
) -> str:
    payload = await upload_yaml_path(
        client,
        endpoint=f"/projects/{project_id}/profiles",
        yaml_path=yaml_path,
        content_type="text/yaml",
    )
    name = payload.get("name")
    if isinstance(name, str) and name:
        return name
    return yaml_path.stem


async def upload_profile_yaml_text(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    filename: str,
    yaml_text: str,
) -> str:
    payload = await upload_yaml_text(
        client,
        endpoint=f"/projects/{project_id}/profiles",
        filename=filename,
        yaml_text=yaml_text,
        content_type="text/yaml",
    )
    name = payload.get("name")
    if isinstance(name, str) and name:
        return name
    return Path(filename).stem


async def upsert_agent(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    channel_id: str,
    agent_id: str,
    profile_name: str,
    worker_target: Optional[str] = None,
    tags: Optional[list[str]] = None,
    display_name: Optional[str] = None,
    owner_agent_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    init_state: bool = True,
) -> Dict[str, Any]:
    payload = {
        "agent_id": agent_id,
        "profile_name": profile_name,
        "init_state": init_state,
        "channel_id": channel_id,
    }
    if worker_target is not None:
        payload["worker_target"] = worker_target
    if tags is not None:
        payload["tags"] = tags
    if display_name is not None:
        payload["display_name"] = display_name
    if owner_agent_id is not None:
        payload["owner_agent_id"] = owner_agent_id
    if metadata is not None:
        payload["metadata"] = metadata
    resp = await client.post(f"/projects/{project_id}/agents", json=payload)
    if resp.status_code >= 400:
        raise RuntimeError(f"agent upsert failed: {resp.status_code} {resp.text}")
    data = resp.json() or {}
    return data if isinstance(data, dict) else {}
