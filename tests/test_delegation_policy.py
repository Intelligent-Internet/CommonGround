from __future__ import annotations

from core.delegation_policy import extract_delegation_policy, is_delegation_allowed


def test_extract_policy_from_delegation_policy() -> None:
    content = {
        "name": "Caller",
        "delegation_policy": {"target_tags": ["associate"], "target_profiles": None},
    }
    policy = extract_delegation_policy(content)
    assert policy is not None
    assert policy.target_tags == ["associate"]
    assert policy.target_profiles is None


def test_is_delegation_allowed_denies_without_policy() -> None:
    assert is_delegation_allowed(None, target_profile_name="X", target_profile_tags=["associate"]) is False


def test_is_delegation_allowed_allow_all_with_tag_filter() -> None:
    policy = extract_delegation_policy({"delegation_policy": {"target_tags": ["principal"], "target_profiles": None}})
    assert policy is not None
    assert is_delegation_allowed(policy, target_profile_name="AnyPrincipal", target_profile_tags=["principal"]) is True
    assert is_delegation_allowed(policy, target_profile_name="AnyAssociate", target_profile_tags=["associate"]) is False


def test_is_delegation_allowed_explicit_allowlist_without_tag_filter() -> None:
    policy = extract_delegation_policy({"delegation_policy": {"target_tags": [], "target_profiles": ["Foo"]}})
    assert policy is not None
    assert is_delegation_allowed(policy, target_profile_name="Foo", target_profile_tags=["principal"]) is True
    assert is_delegation_allowed(policy, target_profile_name="Bar", target_profile_tags=["principal"]) is False
