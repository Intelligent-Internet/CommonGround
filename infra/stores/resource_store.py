import json
from typing import Optional, List, Dict, Any

from psycopg_pool import AsyncConnectionPool

from core.resource_models import ProjectAgent, ToolDefinition
from .base import BaseStore


class ResourceStore(BaseStore):
    """Accessor for roster / profile cache (with lightweight write helpers)."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def fetch_roster(
        self,
        project_id: str,
        agent_id: str,
        *,
        conn: Any = None,
    ) -> Optional[Dict[str, Any]]:
        return await self.fetch_one(
            """
            SELECT project_id,
                   agent_id,
                   profile_box_id::text AS profile_box_id,
                   worker_target,
                   tags,
                   display_name,
                   owner_agent_id,
                   metadata
            FROM resource.project_agents
            WHERE project_id=%s AND agent_id=%s
            """,
            (project_id, agent_id),
            conn=conn,
        )

    async def fetch_roster_model(self, project_id: str, agent_id: str) -> Optional[ProjectAgent]:
        row = await self.fetch_roster(project_id, agent_id)
        if not row:
            return None
        return ProjectAgent.model_validate(row)

    async def list_roster(
        self,
        project_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
        worker_target: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT project_id,
                   agent_id,
                   profile_box_id::text AS profile_box_id,
                   worker_target,
                   tags,
                   display_name,
                   owner_agent_id,
                   status,
                   metadata
            FROM resource.project_agents
            WHERE project_id=%s
        """
        params: List[Any] = [project_id]
        if worker_target:
            sql += " AND worker_target=%s"
            params.append(worker_target)
        if tag:
            sql += " AND tags @> %s::jsonb"
            params.append(json.dumps([tag]))
        sql += " ORDER BY agent_id ASC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        return await self.fetch_all(sql, tuple(params))

    async def fetch_profile(
        self,
        project_id: str,
        profile_ref: Any,
        *,
        conn: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Lookup by box_id in resource.profiles if table exists."""
        try:
            ref_id = profile_ref
            if ref_id is None:
                return None
            return await self.fetch_one(
                """
                SELECT profile_box_id, name, worker_target, tags, system_prompt_compiled
                FROM resource.profiles
                WHERE project_id=%s AND profile_box_id=%s
                """,
                (project_id, ref_id),
                conn=conn,
            )
        except Exception:
            return None

    async def find_profile_by_name(
        self,
        project_id: str,
        name: str,
        *,
        conn: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Lookup profile by name (case-insensitive) in resource.profiles."""
        sql = """
            SELECT profile_box_id, name, worker_target, tags
            FROM resource.profiles
            WHERE project_id=%s AND lower(name)=lower(%s)
            LIMIT 1
        """
        try:
            return await self.fetch_one(sql, (project_id, name), conn=conn)
        except Exception:
            return None

    async def list_profiles(self, project_id: str, *, limit: int = 500, offset: int = 0) -> List[Dict[str, Any]]:
        """List cached profiles (name/worker_target/tags/box_id) from resource.profiles."""
        sql = """
            SELECT profile_box_id, name, worker_target, tags
            FROM resource.profiles
            WHERE project_id=%s
            ORDER BY lower(name) ASC
            LIMIT %s OFFSET %s
        """
        return await self.fetch_all(sql, (project_id, limit, offset))

    async def list_agents_by_profile_box(
        self, project_id: str, profile_box_id: str
    ) -> List[Dict[str, Any]]:
        """Return roster agents referencing the given profile box."""
        if not profile_box_id:
            return []
        sql = """
            SELECT agent_id,
                   profile_box_id,
                   worker_target,
                   tags,
                   display_name,
                   metadata
            FROM resource.project_agents
            WHERE project_id=%s AND profile_box_id=%s
            ORDER BY agent_id ASC
        """
        return await self.fetch_all(sql, (project_id, profile_box_id))

    async def fetch_tools_for_project(self, project_id: str) -> List[Dict[str, Any]]:
        """Return all tool definitions for a project."""
        sql = """
            SELECT project_id, tool_name, description, parameters,
                   target_subject, after_execution, options
            FROM resource.tools
            WHERE project_id=%s
        """
        return await self.fetch_all(sql, (project_id,))

    async def fetch_tools_for_project_model(self, project_id: str) -> List[ToolDefinition]:
        rows = await self.fetch_tools_for_project(project_id)
        return [ToolDefinition.model_validate(row) for row in (rows or []) if row]

    async def fetch_tool_definition(self, project_id: str, tool_name: str) -> Optional[Dict[str, Any]]:
        """Return tool definition from resource.tools (used by PMO & worker)."""
        sql = """
            SELECT project_id, tool_name, description, parameters,
                   target_subject, after_execution, options
            FROM resource.tools
            WHERE project_id=%s AND tool_name=%s
        """
        return await self.fetch_one(sql, (project_id, tool_name))

    async def fetch_tool_definition_model(self, project_id: str, tool_name: str) -> Optional[ToolDefinition]:
        row = await self.fetch_tool_definition(project_id, tool_name)
        if not row:
            return None
        return ToolDefinition.model_validate(row)

    async def upsert_project_agent(
        self,
        *,
        project_id: str,
        agent_id: str,
        profile_box_id: str,
        worker_target: str,
        tags: List[str],
        display_name: Optional[str],
        owner_agent_id: Optional[str],
        metadata: Dict[str, Any],
        conn: Any = None,
    ) -> None:
        """Create or update roster entry for an agent (used by PMO spawn)."""
        sql = """
            INSERT INTO resource.project_agents (
                project_id, agent_id, profile_box_id, worker_target, tags, display_name, owner_agent_id, status, metadata
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, 'active', %s::jsonb)
            ON CONFLICT (project_id, agent_id) DO UPDATE
            SET profile_box_id=EXCLUDED.profile_box_id,
                worker_target=EXCLUDED.worker_target,
                tags=EXCLUDED.tags,
                display_name=COALESCE(EXCLUDED.display_name, resource.project_agents.display_name),
                owner_agent_id=EXCLUDED.owner_agent_id,
                metadata=COALESCE(resource.project_agents.metadata, '{}'::jsonb) || EXCLUDED.metadata
        """
        if conn is not None:
            await conn.execute(
                sql,
                (
                    project_id,
                    agent_id,
                    profile_box_id,
                    worker_target,
                    json.dumps(tags or []),
                    display_name,
                    owner_agent_id,
                    json.dumps(metadata),
                ),
            )
            return
        async with self.pool.connection() as own_conn:
            await own_conn.execute(
                sql,
                (
                    project_id,
                    agent_id,
                    profile_box_id,
                    worker_target,
                    json.dumps(tags or []),
                    display_name,
                    owner_agent_id,
                    json.dumps(metadata),
                ),
            )

    async def deactivate_project_agent(
        self,
        *,
        project_id: str,
        agent_id: str,
        reason: str,
    ) -> bool:
        sql = """
            UPDATE resource.project_agents
            SET status='inactive',
                metadata=COALESCE(metadata, '{}'::jsonb) || %s::jsonb
            WHERE project_id=%s AND agent_id=%s
        """
        payload = {"deactivated_reason": str(reason)}
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql,
                (json.dumps(payload), project_id, agent_id),
            )
            return bool(getattr(result, "rowcount", 0))
