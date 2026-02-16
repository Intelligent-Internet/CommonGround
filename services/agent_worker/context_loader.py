import logging
from typing import Optional, List, Dict, Any

import uuid6

from card_box_core.structures import Card as CoreCard, CardBox as CoreBox, ToolContent as CoreToolContent
from core.delegation_policy import extract_delegation_policy
from core.errors import ProtocolViolationError
from core.llm import LLMConfig
from core.utp_protocol import Card, FieldsSchemaContent, TextContent, extract_json_object
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient
from infra.stores import ResourceStore
from .models import ProfileData, _SafeFormatDict

# Local logger for prompt formatting issues.
logger = logging.getLogger("AgentWorker.ContextLoader")

class ResourceLoader:
    def __init__(self, cardbox: CardBoxClient, resource_store: ResourceStore):
        self.cardbox = cardbox
        self.resource_store = resource_store

    async def load_profile(
        self,
        project_id: str,
        agent_id: str,
        profile_box_id: Optional[str],
    ) -> Optional[ProfileData]:
        roster = await self.resource_store.fetch_roster_model(project_id, agent_id)
        profile_card_ids: List[str] = []
        display_name = None
        tags: List[str] = []
        worker_target: Optional[str] = None
        box_id = profile_box_id
        if roster:
            box_id = box_id or safe_str(roster.profile_box_id)
            display_name = roster.display_name
            tags = list(roster.tags or [])
            worker_target = roster.worker_target

        # Command-level override box_id wins
        if box_id is not None:
            box = await self.cardbox.get_box(box_id, project_id=project_id)
            if box:
                profile_card_ids = list(box.card_ids or [])

        if not profile_card_ids:
            roster_box = safe_str(roster.profile_box_id) if roster else None
            print(
                "[ResourceLoader] Missing profile_box_id/card_ids "
                f"agent_id={agent_id} box_id={box_id} roster_box_id={roster_box}"
            )
            return None

        profile_cards = await self.cardbox.get_cards(profile_card_ids, project_id=project_id)
        if not profile_cards:
            print(f"[ResourceLoader] Profile cards {profile_card_ids} not found")
            return None

        # Pick the sys.profile card if provided, else fall back to first
        profile_card = next((c for c in profile_cards if getattr(c, "type", "") == "sys.profile"), profile_cards[0])
        try:
            content = extract_json_object(profile_card)
        except ProtocolViolationError as exc:
            print(f"[ResourceLoader] Invalid sys.profile content: {exc}")
            return None

        name = content.get("name") or content.get("profile_id") or profile_card.metadata.get("name") or agent_id
        system_prompt_template = content.get("system_prompt_template") or content.get("system_prompt") or ""
        policy = extract_delegation_policy(content)
        allowed_downstream: List[Any] = []
        if policy:
            if policy.target_profiles is None:
                rows = await self.resource_store.list_profiles(project_id, limit=500, offset=0)
                tags_filter = set(policy.target_tags or [])
                for row in rows or []:
                    row_name = safe_str((row or {}).get("name")).strip()
                    row_tags = row.get("tags") if isinstance(row, dict) else None
                    row_tags_list = row_tags if isinstance(row_tags, list) else []
                    row_tags_list = [safe_str(tag) for tag in row_tags_list if safe_str(tag)]
                    if not row_name:
                        continue
                    if tags_filter and not (tags_filter.intersection(set(row_tags_list))):
                        continue
                    allowed_downstream.append(row_name)
            else:
                allowed_downstream = list(policy.target_profiles or [])
        else:
            allowed_downstream = []
        allowed_tools_raw = content.get("allowed_tools", None)
        allowed_tools = None if allowed_tools_raw is None else list(allowed_tools_raw or [])
        allowed_internal_tools_raw = content.get("allowed_internal_tools", None)
        allowed_internal_tools: List[str] = []
        if isinstance(allowed_internal_tools_raw, list):
            for item in allowed_internal_tools_raw:
                val = safe_str(item).strip()
                if val:
                    allowed_internal_tools.append(val)
        description = content.get("description") or profile_card.metadata.get("description", "")
        llm_conf_raw = content.get("llm_config") or {}
        llm_cfg = LLMConfig(**llm_conf_raw) if isinstance(llm_conf_raw, dict) else LLMConfig()
        must_end_with_raw = content.get("must_end_with")
        if must_end_with_raw is None:
            must_end_with = []
        elif isinstance(must_end_with_raw, list):
            must_end_with = []
            for item in must_end_with_raw:
                val = safe_str(item)
                if not val:
                    continue
                val = val.strip()
                if val:
                    must_end_with.append(val)
        elif isinstance(must_end_with_raw, str):
            must_end_with = [must_end_with_raw.strip()] if must_end_with_raw.strip() else []
        else:
            must_end_with = []

        downstream_desc = await self._build_downstream_desc(project_id, allowed_downstream)
        cleaned_allowed_downstream = []
        for item in (allowed_downstream or []):
            val = safe_str(item)
            if not val:
                continue
            if not val.strip():
                continue
            cleaned_allowed_downstream.append(val)

        return ProfileData(
            card_id=profile_card.card_id,
            name=name,
            system_prompt_template=system_prompt_template,
            allowed_downstream=cleaned_allowed_downstream,
            allowed_tools=allowed_tools,
            allowed_internal_tools=allowed_internal_tools,
            llm_config=llm_cfg,
            must_end_with=must_end_with,
            description=description,
            downstream_desc=downstream_desc,
            display_name=display_name,
            tags=tags,
            worker_target=worker_target,
        )

    async def _build_downstream_desc(self, project_id: str, downstream_refs: List[Any]) -> str:
        if not downstream_refs:
            return "No downstream profiles available."

        lines: List[str] = []
        for ref in downstream_refs:
            ref_id = ref
            cache_row = await self.resource_store.fetch_profile(project_id, ref_id)
            if not cache_row and isinstance(ref_id, (str, int)) and str(ref_id).strip():
                cache_row = await self.resource_store.find_profile_by_name(project_id, str(ref_id).strip())

            if cache_row:
                name = cache_row.get("name") or str(ref_id)
                tag_list = cache_row.get("tags") if isinstance(cache_row, dict) else None
                tags = tag_list if isinstance(tag_list, list) else []
                tag_text = ", ".join([safe_str(t) for t in tags if safe_str(t)]) if tags else ""
                if tag_text:
                    lines.append(f"- {name} ({tag_text})")
                else:
                    lines.append(f"- {name}")
                continue

            lines.append(f"- {ref_id}")
        return "\n".join(lines)


class ContextAssembler:
    @staticmethod
    async def assemble(
        cardbox: CardBoxClient,
        profile: ProfileData,
        project_id: str,
        context_card_ids: List[str],
        tool_specs: Optional[List[Dict[str, Any]]] = None,
        system_append: Optional[str] = None,
    ) -> CoreBox:
        """
        Assembles a CoreBox for to_api consumption.
        Renders the system prompt and creates/saves persistent cards for System and Tools.
        """
        from datetime import datetime

        # 1. Render System Prompt
        system_vars = _SafeFormatDict(
            {
                "downstream_profiles_desc": profile.downstream_desc,
                "downstream_roster": profile.downstream_desc,
                "downstream_desc": profile.downstream_desc,
                "display_name": profile.display_name or profile.name,
            }
        )
        if profile.system_prompt_template:
            try:
                system_text = profile.system_prompt_template.format_map(system_vars)
            except Exception:  # noqa: BLE001
                template_preview = profile.system_prompt_template[:200]
                logger.exception(
                    "Failed to format system_prompt_template: profile=%s card_id=%s keys=%s preview=%s",
                    safe_str(profile.name),
                    safe_str(profile.card_id),
                    list(system_vars.keys()),
                    template_preview,
                )
                raise
        else:
            system_text = ""
        system_text = system_text.strip()

        result_field_lines: List[str] = []
        filtered_context_ids: List[str] = []
        if context_card_ids:
            cards = await cardbox.get_cards(context_card_ids, project_id=project_id)
            card_map = {c.card_id: c for c in cards}
            for cid in context_card_ids:
                card = card_map.get(cid)
                if card and getattr(card, "type", "") == "task.result_fields":
                    content = getattr(card, "content", None)
                    if isinstance(content, FieldsSchemaContent):
                        for field in content.fields:
                            name = safe_str(getattr(field, "name", ""))
                            if not name:
                                continue
                            desc = safe_str(getattr(field, "description", ""))
                            if desc:
                                result_field_lines.append(f"- {name}: {desc}")
                            else:
                                result_field_lines.append(f"- {name}")
                    continue
                filtered_context_ids.append(cid)

        if result_field_lines:
            parts: List[str] = []
            if system_text:
                parts.append(system_text)
            parts.append("Required result fields:\n" + "\n".join(result_field_lines))
            system_text = "\n\n".join(parts).strip()
        if system_append:
            if system_text:
                system_text = f"{system_text}\n\n{system_append}".strip()
            else:
                system_text = system_append.strip()

        # 2. Create & Save System Card
        sys_card = Card(
            content=TextContent(text=system_text),
            type="sys.rendered_prompt",
            metadata={"role": "system", "profile_id": profile.card_id},
            card_id=uuid6.uuid7().hex,
            project_id=project_id,
            created_at=datetime.now(),
            author_id="system_assembler",
        )
        await cardbox.save_card(sys_card)

        # 3. Create & Save Tool Card (if tools exist)
        box = CoreBox()
        box.add(sys_card.card_id)

        if tool_specs:
            tool_card_id = uuid6.uuid7().hex
            tool_core = CoreCard(
                card_id=tool_card_id,
                content=CoreToolContent(tools=tool_specs),
                metadata={"role": "system", "type": "sys.tools"},
            )
            await cardbox.add_core_card(tool_core, tenant_id=project_id)
            box.add(tool_card_id)

        for cid in filtered_context_ids:
            box.add(cid)

        return box
