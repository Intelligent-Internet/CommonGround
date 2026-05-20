from __future__ import annotations

from typing import Mapping

from CommonGround.contracts import ConflictError
from CommonGround.turn_offers import normalize_public_turn_offers


def normalize_public_metadata(public_metadata: Mapping[str, object]) -> dict[str, object]:
    normalized = normalize_public_turn_offers(public_metadata)
    provision_value = normalized.get("provision")
    if provision_value is None:
        return normalized
    if not isinstance(provision_value, Mapping):
        raise ConflictError("public_metadata.provision must be an object")
    if "available_roles" in provision_value:
        raise ConflictError(
            "public_metadata.provision.available_roles is unsupported; "
            "use public_metadata.turn_offers[].variants.roles"
        )
    normalized["provision"] = dict(provision_value)
    return normalized
