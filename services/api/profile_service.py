from __future__ import annotations

from datetime import UTC, datetime
from typing import List, Optional

import uuid6

from core.utp_protocol import Card, JsonContent
from infra.cardbox_client import CardBoxClient
from infra.stores import ProfileStore, ProjectStore
from services.api.profile_models import Profile, ProfileYaml
from core.errors import NotFoundError


class ProfileService:
    def __init__(self, *, project_store: ProjectStore, profile_store: ProfileStore, cardbox: CardBoxClient):
        self.project_store = project_store
        self.profile_store = profile_store
        self.cardbox = cardbox

    async def create_profile_from_yaml(self, *, project_id: str, profile: ProfileYaml) -> Profile:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")

        card_id = uuid6.uuid7().hex
        card = Card(
            card_id=card_id,
            project_id=project_id,
            type="sys.profile",
            content=JsonContent(data=profile.model_dump(mode="python")),
            created_at=datetime.now(UTC),
            author_id="api",
            metadata={"name": profile.name, "worker_target": profile.worker_target, "tags": profile.tags},
        )
        await self.cardbox.save_card(card)
        box_id = await self.cardbox.save_box([card_id], project_id=project_id)

        row = await self.profile_store.upsert_profile(
            project_id=project_id,
            profile_box_id=str(box_id),
            name=profile.name,
            worker_target=profile.worker_target,
            tags=profile.tags,
            system_prompt_compiled=profile.system_prompt_template,
        )
        return Profile.model_validate(row)

    async def list_profiles(self, *, project_id: str, limit: int = 200, offset: int = 0) -> List[Profile]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        rows = await self.profile_store.list_profiles(project_id=project_id, limit=limit, offset=offset)
        return [Profile.model_validate(r) for r in rows or []]

    async def get_profile(self, *, project_id: str, name: str) -> Optional[Profile]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        row = await self.profile_store.get_profile_by_name(project_id=project_id, name=name)
        return Profile.model_validate(row) if row else None

    async def delete_profile(self, *, project_id: str, name: str) -> bool:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        return await self.profile_store.delete_profile_by_name(project_id=project_id, name=name)
