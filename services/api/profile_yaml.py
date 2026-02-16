from __future__ import annotations

from typing import Any, Dict

from services.api.profile_models import ProfileYaml
from core.errors import BadRequestError


def normalize_profile_yaml(data: Any) -> ProfileYaml:
    if not isinstance(data, dict):
        raise BadRequestError("YAML root must be a mapping")

    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise BadRequestError("profile name is required")

    worker_target = data.get("worker_target")
    if not isinstance(worker_target, str) or not worker_target.strip():
        raise BadRequestError("profile worker_target is required")
    worker_target = worker_target.strip()

    tags_raw = data.get("tags")
    if tags_raw is None:
        tags = []
    elif isinstance(tags_raw, list):
        tags = [str(item).strip() for item in tags_raw if str(item).strip()]
    elif isinstance(tags_raw, str):
        tags = [part.strip() for part in tags_raw.split(",") if part.strip()]
    else:
        raise BadRequestError("profile tags must be a list of strings")

    system_prompt_template = data.get("system_prompt_template")
    if system_prompt_template is None:
        system_prompt_template = data.get("system_prompt") or ""

    normalized: Dict[str, Any] = dict(data)
    normalized["name"] = name.strip()
    normalized["worker_target"] = worker_target
    normalized["tags"] = tags
    normalized["system_prompt_template"] = str(system_prompt_template or "")

    return ProfileYaml.model_validate(normalized)
