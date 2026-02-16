"""PMO service context handoff packaging logic."""

from __future__ import annotations

import uuid6
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Tuple

from infra.cardbox_client import CardBoxClient
from infra.stores import ResourceStore
from core.utp_protocol import Card, FieldsSchemaContent, JsonContent, TextContent
from core.errors import BadRequestError, NotFoundError
from core.utils import safe_str


def _build_parent_pointer_card(project_id: str, parent_agent: str) -> Card:
    return Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="meta.parent_pointer",
        content=JsonContent(data={"parent_agent_id": parent_agent}),
        created_at=datetime.now(UTC),
        author_id=parent_agent,
        metadata={"role": "system"},
    )


class HandoverPacker:
    """Pack cards / boxes for downstream Agent based on handoff rules."""

    def __init__(self, cardbox: CardBoxClient, resource_store: ResourceStore) -> None:
        self.cardbox = cardbox
        self.resource_store = resource_store

    async def pack_context(
        self,
        *,
        project_id: str,
        tool_suffix: str,
        source_agent_id: str,
        arguments: Dict[str, Any],
        handover: Optional[Dict[str, Any]],
        conn: Any = None,
    ) -> Tuple[str, str, List[str]]:
        """Create a context box and return (box_id, target_profile_box_id, card_ids)."""
        target_profile_ref: Optional[str] = None
        card_ids: List[str] = []

        if handover:
            target_cfg = handover.get("target_profile_config") or {}

            target_name = safe_str(arguments.get("profile_name"))
            if target_name:
                target_name = target_name.strip()
            if not target_name:
                target_name = safe_str(target_cfg.get("profile_name"))
                if target_name:
                    target_name = target_name.strip()
            if not target_name:
                raise BadRequestError(
                    "Handover failed: profile_name missing in arguments/target_profile_config"
                )

            found = await self.resource_store.find_profile_by_name(project_id, target_name, conn=conn)
            if not found:
                raise NotFoundError(
                    f"Handover failed: Profile '{target_name}' not found in project '{project_id}'"
                )
            target_profile_ref = found["profile_box_id"]

            ctx_cfg = handover.get("context_packing_config") or {}
            pack_args = ctx_cfg.get("pack_arguments") or []
            for rule in pack_args:
                if not isinstance(rule, dict):
                    continue
                arg_key = rule.get("arg_key")
                card_type = rule.get("as_card_type", "task.instruction")
                card_meta = (rule.get("card_metadata") or {}).copy()
                card_meta.setdefault("role", "user")
                if arg_key and arg_key in arguments:
                    content_val = arguments.get(arg_key)
                    if card_type == "task.result_fields":
                        if not isinstance(content_val, list):
                            raise BadRequestError(
                                "task.result_fields must be a list of field objects"
                            )
                        content = FieldsSchemaContent(fields=content_val)
                    elif isinstance(content_val, (dict, list)):
                        content = JsonContent(data=content_val)
                    else:
                        content = TextContent(text=str(content_val))
                    card = Card(
                        card_id=uuid6.uuid7().hex,
                        project_id=project_id,
                        type=card_type,
                        content=content,
                        created_at=datetime.now(UTC),
                        author_id=tool_suffix,
                        metadata=card_meta,
                    )
                    await self.cardbox.save_card(card, conn=conn)
                    card_ids.append(card.card_id)

            inherit_cfg = (ctx_cfg.get("inherit_context") or {}) if isinstance(ctx_cfg, dict) else {}
            include_from_args = inherit_cfg.get("include_boxes_from_args") or []
            if isinstance(include_from_args, list) and include_from_args:
                existing = set(card_ids)
                for arg_key in include_from_args:
                    if not isinstance(arg_key, str) or not arg_key:
                        continue
                    if arg_key not in arguments:
                        continue
                    raw = arguments.get(arg_key)
                    box_ids: List[str] = []
                    if isinstance(raw, (str, int)) and str(raw):
                        box_ids = [str(raw)]
                    elif isinstance(raw, list):
                        for item in raw:
                            if isinstance(item, (str, int)) and str(item):
                                box_ids.append(str(item))
                    else:
                        raise BadRequestError(
                            f"inherit_context.{arg_key} must be a box_id or list[box_id]"
                        )

                    for box_id in box_ids:
                        box = await self.cardbox.get_box(box_id, project_id=project_id, conn=conn)
                        if not box:
                            raise NotFoundError(f"inherit_context: box not found: {box_id}")
                        for cid in list(box.card_ids or []):
                            if cid not in existing:
                                card_ids.append(cid)
                                existing.add(cid)

            if inherit_cfg.get("include_parent"):
                parent_card = _build_parent_pointer_card(project_id, source_agent_id)
                await self.cardbox.save_card(parent_card, conn=conn)
                card_ids.append(parent_card.card_id)
        else:
            instr = arguments.get("instruction")
            if instr:
                card = Card(
                    card_id=uuid6.uuid7().hex,
                    project_id=project_id,
                    type="task.instruction",
                    content=TextContent(text=str(instr)),
                    created_at=datetime.now(UTC),
                    author_id=tool_suffix,
                    metadata={"role": "user"},
                )
                await self.cardbox.save_card(card, conn=conn)
                card_ids.append(card.card_id)

        box_id = await self.cardbox.save_box(card_ids, project_id=project_id, conn=conn)
        box_id = str(box_id)

        if not target_profile_ref:
            raise BadRequestError("target_profile_config must resolve to a valid profile_box_id")

        profile_box_id = target_profile_ref

        return box_id, profile_box_id, card_ids
