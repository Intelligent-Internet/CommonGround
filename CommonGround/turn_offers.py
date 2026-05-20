from __future__ import annotations

from typing import Any, Mapping

from CommonGround.contracts import ConflictError


_ALLOWED_AUTHORITY_MODES = frozenset({"root_request", "child_derivation"})


def normalize_turn_offers(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise ConflictError("turn_offers must be a list")
    offers: list[dict[str, Any]] = []
    seen_turn_kinds: set[str] = set()
    for item in value:
        normalized = normalize_turn_offer(item)
        turn_kind = normalized["turn_kind"]
        if turn_kind in seen_turn_kinds:
            raise ConflictError(f"turn_offers contains duplicate turn_kind: {turn_kind}")
        seen_turn_kinds.add(turn_kind)
        offers.append(normalized)
    return tuple(offers)


def normalize_turn_offer(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ConflictError("turn_offers items must be objects")
    normalized = {str(key): inner for key, inner in value.items()}
    turn_kind = normalized.get("turn_kind")
    if not isinstance(turn_kind, str) or not turn_kind:
        raise ConflictError("turn_offers[].turn_kind must be a non-empty string")
    purpose = normalized.get("purpose")
    if purpose is not None and not isinstance(purpose, str):
        raise ConflictError("turn_offers[].purpose must be a string or null")
    calling = normalized.get("calling")
    if not isinstance(calling, Mapping):
        raise ConflictError("turn_offers[].calling must be an object")
    input_contract = normalized.get("input_contract")
    if not isinstance(input_contract, Mapping):
        raise ConflictError("turn_offers[].input_contract must be an object")
    variants = normalized.get("variants", {})
    if not isinstance(variants, Mapping):
        raise ConflictError("turn_offers[].variants must be an object")
    notes = normalized.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise ConflictError("turn_offers[].notes must be a string or null")
    normalized["turn_kind"] = turn_kind
    normalized["purpose"] = purpose
    normalized["calling"] = _normalize_calling(calling)
    normalized["input_contract"] = dict(input_contract)
    normalized["variants"] = dict(variants)
    normalized["notes"] = notes
    return normalized


def normalize_public_turn_offers(public_metadata: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(public_metadata)
    turn_offers = normalized.get("turn_offers")
    if turn_offers is None:
        return normalized
    normalized["turn_offers"] = [dict(item) for item in normalize_turn_offers(turn_offers)]
    return normalized


def extract_turn_offers(public_metadata: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    value = public_metadata.get("turn_offers")
    if value is None:
        return ()
    return tuple(normalize_turn_offers(value))


def find_turn_offer(public_metadata: Mapping[str, object], *, turn_kind: str) -> dict[str, object] | None:
    for offer in extract_turn_offers(public_metadata):
        if offer["turn_kind"] == turn_kind:
            return offer
    return None


def upsert_turn_offer(public_metadata: Mapping[str, Any], offer: Mapping[str, Any]) -> dict[str, Any]:
    normalized_offer = normalize_turn_offer(offer)
    existing_offers = list(normalize_turn_offers(public_metadata.get("turn_offers", [])))
    replaced = False
    merged: list[dict[str, Any]] = []
    for existing in existing_offers:
        if existing["turn_kind"] == normalized_offer["turn_kind"]:
            merged.append(dict(normalized_offer))
            replaced = True
        else:
            merged.append(dict(existing))
    if not replaced:
        merged.append(dict(normalized_offer))
    result = dict(public_metadata)
    result["turn_offers"] = merged
    return result


def _normalize_calling(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {str(key): inner for key, inner in value.items()}
    operation = normalized.get("operation")
    if operation != "dispatch":
        raise ConflictError("turn_offers[].calling.operation must be 'dispatch'")
    authority_modes = normalized.get("authority_modes")
    normalized["operation"] = "dispatch"
    normalized["authority_modes"] = [dict(item) for item in normalize_authority_modes(authority_modes)]
    return normalized


def normalize_authority_modes(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise ConflictError("turn_offers[].calling.authority_modes must be a list")
    items: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ConflictError("turn_offers[].calling.authority_modes items must be objects")
        normalized = {str(key): inner for key, inner in item.items()}
        mode = normalized.get("mode")
        if not isinstance(mode, str) or mode not in _ALLOWED_AUTHORITY_MODES:
            raise ConflictError("turn_offers[].calling.authority_modes[].mode must be one of: root_request, child_derivation")
        normalized["mode"] = mode
        items.append(normalized)
    return tuple(items)
