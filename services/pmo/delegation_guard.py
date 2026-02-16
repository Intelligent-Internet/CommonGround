from __future__ import annotations

from typing import Any, Dict

from core.delegation_policy import describe_policy, extract_delegation_policy, is_delegation_allowed
from core.utp_protocol import extract_json_object
from core.utils import safe_str
from core.errors import BadRequestError, NotFoundError, PolicyDeniedError


async def _load_sys_profile_content(
    *,
    deps: Any,
    project_id: str,
    profile_box_id: str,
    conn: Any = None,
) -> Dict[str, Any]:
    box = await deps.cardbox.get_box(str(profile_box_id), project_id=project_id, conn=conn)
    if not box or not getattr(box, "card_ids", None):
        return {}
    cards = await deps.cardbox.get_cards(list(box.card_ids), project_id=project_id, conn=conn)
    if not cards:
        return {}
    sys_card = next((c for c in cards if getattr(c, "type", "") == "sys.profile"), None)
    if not sys_card:
        raise BadRequestError("profile box missing sys.profile card")
    return extract_json_object(sys_card)


async def assert_can_delegate_to_profile_name(
    *,
    deps: Any,
    project_id: str,
    caller_agent_id: str,
    target_profile_name: str,
    conn: Any = None,
) -> Dict[str, Any]:
    """
    Hard permission check for delegating/spawning sub-agents by profile name.

    Returns the resolved target profile cache row (profile_box_id/name/worker_target/tags) on success.
    Raises ValueError on denial or bad config.
    """

    roster = await deps.resource_store.fetch_roster(project_id, caller_agent_id, conn=conn)
    if not roster:
        raise NotFoundError(
            f"caller agent_id not registered in roster: {caller_agent_id} (project_id={project_id})"
        )
    caller_profile_box_id = safe_str(roster.get("profile_box_id"))
    if not caller_profile_box_id:
        raise BadRequestError("caller roster missing profile_box_id")

    caller_profile_content = await _load_sys_profile_content(
        deps=deps,
        project_id=project_id,
        profile_box_id=caller_profile_box_id,
        conn=conn,
    )
    policy = extract_delegation_policy(caller_profile_content)
    if not policy:
        raise PolicyDeniedError("delegation denied: caller profile missing delegation_policy")

    target_name = safe_str(target_profile_name).strip()
    if not target_name:
        raise BadRequestError("delegation denied: target profile_name is required")

    found = await deps.resource_store.find_profile_by_name(project_id, target_name, conn=conn)
    if not found:
        raise NotFoundError(f"delegation denied: target profile not found: {target_name!r}")

    target_tags = found.get("tags") if isinstance(found, dict) else None
    tags = target_tags if isinstance(target_tags, list) else []
    tags = [safe_str(tag) for tag in tags if safe_str(tag)]
    if not is_delegation_allowed(policy, target_profile_name=target_name, target_profile_tags=tags):
        raise PolicyDeniedError(
            "delegation denied by policy: "
            f"target={target_name!r} tags={tags!r} policy={describe_policy(policy)}"
        )

    return found
