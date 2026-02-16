from __future__ import annotations

import uuid6
from typing import List, Optional

from infra.project_bootstrap import ProjectBootstrapper
from infra.stores import ProjectAlreadyExistsError, ProjectStore
from core.errors import NotFoundError
from services.api.models import Project, ProjectCreate, ProjectUpdate


class ProjectService:
    def __init__(self, *, store: ProjectStore, bootstrapper: ProjectBootstrapper):
        self.store = store
        self.bootstrapper = bootstrapper

    async def create_project(self, data: ProjectCreate) -> Project:
        project_id = data.project_id or f"proj_{uuid6.uuid7().hex[:12]}"
        row = await self.store.create_project(project_id=project_id, title=data.title, owner_id=data.owner_id)
        if data.bootstrap:
            await self.bootstrapper.bootstrap(project_id)
        return Project.model_validate(row)

    async def get_project(self, project_id: str) -> Optional[Project]:
        row = await self.store.get_project(project_id)
        return Project.model_validate(row) if row else None

    async def list_projects(self, *, limit: int = 100, offset: int = 0) -> List[Project]:
        rows = await self.store.list_projects(limit=limit, offset=offset)
        return [Project.model_validate(r) for r in rows or []]

    async def update_project(self, project_id: str, data: ProjectUpdate) -> Optional[Project]:
        row = await self.store.update_project(project_id=project_id, title=data.title, status=data.status)
        return Project.model_validate(row) if row else None

    async def delete_project(self, project_id: str) -> bool:
        return await self.store.delete_project(project_id)

    async def bootstrap_project(self, project_id: str) -> None:
        if not await self.store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        await self.bootstrapper.bootstrap(project_id)


__all__ = ["ProjectService", "ProjectAlreadyExistsError"]
