from __future__ import annotations

from typing import List, Optional

from infra.stores import ProjectStore, ToolStore
from services.api.tool_models import Tool, ToolYaml
from core.errors import BadRequestError, NotFoundError


class ToolService:
    _INTERNAL_TOOL_NAMES = {
        "delegate_async",
        "launch_principal",
        "ask_expert",
        "fork_join",
        "provision_agent",
    }

    def __init__(self, *, project_store: ProjectStore, tool_store: ToolStore):
        self.project_store = project_store
        self.tool_store = tool_store

    @staticmethod
    def _is_external_target_subject(subject: str) -> bool:
        # External tool subjects must route to tool services, not sys.* services.
        # Example: cg.v1r3.{project_id}.{channel_id}.cmd.tool.search.call
        s = subject or ""
        return ".cmd.tool." in s and ".cmd.sys." not in s

    async def upsert_external_tool(self, *, project_id: str, tool: ToolYaml) -> Tool:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")

        if tool.tool_name in self._INTERNAL_TOOL_NAMES:
            raise BadRequestError(f"tool_name is reserved for internal tools: {tool.tool_name}")

        if tool.after_execution not in ("suspend", "terminate"):
            raise BadRequestError("after_execution must be 'suspend' or 'terminate'")

        if not self._is_external_target_subject(tool.target_subject):
            raise BadRequestError(
                "target_subject must be an external tool subject (contain '.cmd.tool.' and not '.cmd.sys.')"
            )

        row = await self.tool_store.upsert_tool(
            project_id=project_id,
            tool_name=tool.tool_name,
            description=tool.description,
            target_subject=tool.target_subject,
            after_execution=tool.after_execution,
            parameters=tool.parameters,
            options=tool.options,
        )
        return Tool.model_validate(row)

    async def list_external_tools(self, *, project_id: str, limit: int = 200, offset: int = 0) -> List[Tool]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        rows = await self.tool_store.list_tools(project_id=project_id, limit=limit, offset=offset)
        tools = [Tool.model_validate(r) for r in rows or []]
        return [
            t
            for t in tools
            if t.tool_name not in self._INTERNAL_TOOL_NAMES
            and t.target_subject
            and self._is_external_target_subject(t.target_subject)
        ]

    async def get_external_tool(self, *, project_id: str, tool_name: str) -> Optional[Tool]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        row = await self.tool_store.get_tool(project_id=project_id, tool_name=tool_name)
        if not row:
            return None
        tool = Tool.model_validate(row)
        if tool.tool_name in self._INTERNAL_TOOL_NAMES:
            return None
        if not tool.target_subject or not self._is_external_target_subject(tool.target_subject):
            return None
        return tool

    async def delete_external_tool(self, *, project_id: str, tool_name: str) -> bool:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        if tool_name in self._INTERNAL_TOOL_NAMES:
            raise BadRequestError(f"cannot delete internal tool via tool api: {tool_name}")

        existing = await self.tool_store.get_tool(project_id=project_id, tool_name=tool_name)
        if not existing:
            return False
        target_subject = existing.get("target_subject")
        if not isinstance(target_subject, str) or not self._is_external_target_subject(target_subject):
            raise BadRequestError("refuse to delete non-external tool via tool api")

        return await self.tool_store.delete_tool(project_id=project_id, tool_name=tool_name)
