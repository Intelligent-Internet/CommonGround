from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.errors import ProtocolViolationError


class DelegationPolicy(BaseModel):
    """
    Delegation policy for spawning sub-agents by profile.

    Semantics:
    - target_tags: optional tag filter. If empty, tags are not used as a restriction
      when target_profiles is provided (explicit allowlist).
    - target_profiles:
        - None: allow ALL profiles (subject to target_tags filter, if provided)
        - []: allow NONE profiles
        - ["NameA", ...]: allow these profile names (and optionally filter by tags)
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    target_tags: List[str] = Field(default_factory=list)
    target_profiles: Optional[List[str]] = None

    @field_validator("target_profiles", mode="before")
    @classmethod
    def _normalize_target_profiles(cls, v: Any) -> Any:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("delegation_policy.target_profiles must be null or a list of strings")
        normalized: List[str] = []
        for item in v:
            if not isinstance(item, str) or not item.strip():
                continue
            normalized.append(item.strip())
        return normalized

    @field_validator("target_tags", mode="before")
    @classmethod
    def _normalize_target_tags(cls, v: Any) -> Any:
        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError("delegation_policy.target_tags must be a list of strings")
        normalized: List[str] = []
        for item in v:
            if not isinstance(item, str) or not item.strip():
                continue
            normalized.append(item.strip())
        return normalized


def extract_delegation_policy(profile_content: Any) -> Optional[DelegationPolicy]:
    """
    Read delegation policy from sys.profile content.

    Preferred field:
      - delegation_policy: {target_tags, target_profiles}
    """

    if not isinstance(profile_content, dict):
        raise ProtocolViolationError("sys.profile content must be a JSON object")

    raw_policy = profile_content.get("delegation_policy")
    if isinstance(raw_policy, dict):
        try:
            return DelegationPolicy.model_validate(raw_policy)
        except Exception:
            return None

    return None


def is_delegation_allowed(
    policy: Optional[DelegationPolicy],
    *,
    target_profile_name: str,
    target_profile_tags: Optional[List[str]],
) -> bool:
    """
    Decide if delegation to (profile_name, tags) is allowed.

    Notes:
    - If policy is missing, deny by default (hard-permission mode).
    - If policy.target_profiles is an explicit list, membership is required.
    - If policy.target_profiles is None, allow all profiles subject to tag filter (if any).
    - Tag filter applies only if policy.target_tags is non-empty.
    """

    if not policy:
        return False

    name = (target_profile_name or "").strip()
    if not name:
        return False

    tags = [t for t in (target_profile_tags or []) if isinstance(t, str) and t.strip()]
    tag_set = set(tags)

    if policy.target_profiles is None:
        if policy.target_tags and not tag_set.intersection(set(policy.target_tags)):
            return False
        return True

    # Explicit allowlist mode.
    if name not in set(policy.target_profiles or []):
        return False
    if policy.target_tags and not tag_set.intersection(set(policy.target_tags)):
        return False
    return True


def describe_policy(policy: Optional[DelegationPolicy]) -> Dict[str, Any]:
    if not policy:
        return {"target_tags": [], "target_profiles": []}
    return {
        "target_tags": list(policy.target_tags or []),
        "target_profiles": None if policy.target_profiles is None else list(policy.target_profiles or []),
    }
