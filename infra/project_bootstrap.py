from __future__ import annotations

import json
from pathlib import Path

from psycopg_pool import AsyncConnectionPool

from core.config import PROTOCOL_VERSION
from core.subject import subject_prefix
from core.json_schema import validate_json_schema, validate_json_schema_requires_array_items
from core.utils import REPO_ROOT


PROTOCOL_PREFIX = subject_prefix(PROTOCOL_VERSION)


DELEGATE_ASYNC_TOOL_DEF = {
    "tool_name": "delegate_async",
    "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.sys.pmo.internal.delegate_async",
    "after_execution": "suspend",
    "parameters": {
        "type": "object",
        "properties": {
            "target_strategy": {"type": "string", "enum": ["new", "reuse", "clone"]},
            "target_ref": {"type": "string"},
            "instruction": {"type": "string"},
            "context_box_id": {"type": "string"},
        },
        "required": ["target_strategy", "target_ref", "instruction"],
        "additionalProperties": False,
    },
    "options": {"ui_allowed": True},
}


async def _run_sql_file(conn, path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"sql file missing: {path}")
    await conn.execute(path.read_text())


async def ensure_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        await _run_sql_file(conn, REPO_ROOT / "scripts" / "setup" / "init_projects.sql")
        await _run_sql_file(conn, REPO_ROOT / "scripts" / "setup" / "init_db.sql")
        await _run_sql_file(conn, REPO_ROOT / "scripts" / "setup" / "init_pmo_db.sql")


async def ensure_project(pool: AsyncConnectionPool, *, project_id: str, title: str, owner_id: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO public.projects (project_id, title, status, owner_id)
            VALUES (%s, %s, 'active', %s)
            ON CONFLICT (project_id) DO NOTHING;
            """,
            (project_id, title, owner_id),
        )


async def seed_tools(pool: AsyncConnectionPool, project_id: str) -> None:
    sql_tool = """
    INSERT INTO resource.tools (
        project_id, tool_name, target_subject, after_execution, parameters, options
    ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
    ON CONFLICT (project_id, tool_name) DO UPDATE
      SET target_subject=EXCLUDED.target_subject,
          after_execution=EXCLUDED.after_execution,
          parameters=EXCLUDED.parameters,
          options=EXCLUDED.options;
    """
    tools = [
            # PMO tools (L1-internal handlers)
            {
                "tool_name": "provision_agent",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.sys.pmo.internal.provision_agent",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string", "enum": ["ensure", "create", "derive"]},
                        "agent_id": {"type": "string"},
                        "profile_name": {"type": "string"},
                        "profile_box_id": {"type": "string"},
                        "derived_from": {"type": "string"},
                        "display_name": {"type": "string"},
                        "owner_agent_id": {"type": "string"},
                        "worker_target": {"type": "string"},
                        "active_channel_id": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "metadata": {"type": "object", "additionalProperties": True},
                    },
                    "required": ["action", "agent_id"],
                    "additionalProperties": False,
                },
                "options": {"ui_allowed": True},
            },
            {
                "tool_name": "launch_principal",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.sys.pmo.internal.launch_principal",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "profile_name": {"type": "string"},
                        "instruction": {"type": "string"},
                    },
                    "required": ["profile_name", "instruction"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.load",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.load",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "path": {"type": "string"},
                        "session_id": {"type": "string"},
                    },
                    "required": ["skill_name"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.run_cmd",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.run_cmd",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "command": {"type": "string"},
                        "session_id": {"type": "string"},
                        "inputs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "artifact_id": {"type": "string"},
                                    "mount_path": {"type": "string"},
                                },
                                "required": ["artifact_id", "mount_path"],
                            },
                        },
                        "outputs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "path": {"type": "string"},
                                    "save_as": {"type": "string"},
                                },
                                "required": ["path"],
                            },
                        },
                        "timeout_sec": {"type": "number"},
                    },
                    "required": ["skill_name", "command"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.run_cmd_async",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.run_cmd_async",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "command": {"type": "string"},
                        "session_id": {"type": "string"},
                        "workdir": {"type": "string"},
                        "env": {"type": "object"},
                        "inputs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "artifact_id": {"type": "string"},
                                    "mount_path": {"type": "string"},
                                },
                                "required": ["artifact_id", "mount_path"],
                            },
                        },
                        "outputs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "path": {"type": "string"},
                                    "save_as": {"type": "string"},
                                },
                                "required": ["path"],
                            },
                        },
                        "timeout_sec": {"type": "number"},
                    },
                    "required": ["skill_name", "command"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.run_service",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.run_service",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "command": {"type": "string"},
                        "session_id": {"type": "string"},
                        "workdir": {"type": "string"},
                        "env": {"type": "object"},
                        "ready_pattern": {"type": "string"},
                        "startup_timeout_sec": {"type": "number"},
                    },
                    "required": ["skill_name", "command", "session_id"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.start_job",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.start_job",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "session_id": {"type": "string"},
                        "workdir": {"type": "string"},
                        "env": {"type": "object"},
                        "max_concurrency": {"type": "number"},
                        "max_wall_time_sec": {"type": "number"},
                        "steps": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "command": {"type": "string"},
                                    "timeout_sec": {"type": "number"},
                                    "env": {"type": "object"},
                                },
                                "required": ["command"],
                            },
                        },
                    },
                    "required": ["skill_name", "session_id", "steps"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.job_status",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.job_status",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "tail_bytes": {"type": "number"},
                    },
                    "required": ["job_id"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.job_watch",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.job_watch",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "tail_bytes": {"type": "number"},
                        "poll_interval_sec": {"type": "number"},
                        "heartbeat_sec": {"type": "number"},
                        "timeout_sec": {"type": "number"},
                        "dedupe_key": {"type": "string"},
                    },
                    "required": ["job_id"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.job_cancel",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.job_cancel",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                    "required": ["job_id"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.activate",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.activate",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "skill_name": {"type": "string"},
                        "session_id": {"type": "string"},
                        "timeout_sec": {"type": "number"},
                    },
                    "required": ["skill_name"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.task_status",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.task_status",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "provider": {"type": "string", "enum": ["cmd", "bg"]},
                        "task_id": {"type": "string"},
                        "query_command": {"type": "string"},
                        "workdir": {"type": "string"},
                        "session_id": {"type": "string"},
                        "tail_bytes": {"type": "number"},
                        "status_field": {"type": "string"},
                        "progress_field": {"type": "string"},
                        "events_field": {"type": "string"},
                        "done_states": {"type": "array", "items": {"type": "string"}},
                        "error_states": {"type": "array", "items": {"type": "string"}},
                        "timeout_sec": {"type": "number"},
                    },
                    "required": ["provider"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.task_cancel",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.task_cancel",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string"},
                    },
                    "required": ["task_id"],
                    "additionalProperties": False,
                },
                "options": {},
            },
            {
                "tool_name": "skills.task_watch",
                "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.skills.task_watch",
                "after_execution": "suspend",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "provider": {"type": "string", "enum": ["cmd", "bg"]},
                        "task_id": {"type": "string"},
                        "query_command": {"type": "string"},
                        "workdir": {"type": "string"},
                        "session_id": {"type": "string"},
                        "tail_bytes": {"type": "number"},
                        "status_field": {"type": "string"},
                        "progress_field": {"type": "string"},
                        "events_field": {"type": "string"},
                        "done_states": {"type": "array", "items": {"type": "string"}},
                        "error_states": {"type": "array", "items": {"type": "string"}},
                        "poll_interval_sec": {"type": "number"},
                        "heartbeat_sec": {"type": "number"},
                        "dedupe_key": {"type": "string"},
                        "timeout_sec": {"type": "number"},
                    },
                    "required": ["provider"],
                    "additionalProperties": False,
                },
                "options": {},
            },
        {
            "tool_name": "fork_join",
            "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.sys.pmo.internal.fork_join",
            "after_execution": "suspend",
            "parameters": {
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "target_strategy": {"type": "string", "enum": ["new", "reuse", "clone"]},
                                "target_ref": {"type": "string"},
                                "instruction": {"type": "string"},
                                "context_box_id": {"type": "string"},
                            },
                            "required": ["target_strategy", "target_ref", "instruction"],
                            "additionalProperties": False,
                        },
                    },
                    "fail_fast": {"type": "boolean"},
                    "deadline_seconds": {"type": "number"},
                },
                "required": ["tasks"],
                "additionalProperties": False,
            },
            "options": {"suspend_timeout_seconds": 120},
        },
        DELEGATE_ASYNC_TOOL_DEF,
        {
            "tool_name": "ask_expert",
            "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.sys.pmo.internal.ask_expert",
            "after_execution": "suspend",
            "parameters": {
                "type": "object",
                "properties": {
                    "expert_profile": {"type": "string"},
                    "question": {"type": "string"},
                    "include_history": {"type": "boolean"},
                },
                "required": ["expert_profile", "question"],
                "additionalProperties": False,
            },
            "options": {},
        },
        {
            "tool_name": "emit_marker",
            "target_subject": PROTOCOL_PREFIX + ".{project_id}.{channel_id}.cmd.tool.emit_marker.call",
            "after_execution": "suspend",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "markdown_content": {"type": "string"},
                },
                "required": ["title", "markdown_content"],
                "additionalProperties": False,
            },
            "options": {
                "args": {
                    "fixed": {"marker_type": "adoptable_recommendation"},
                }
            },
        },
    ]

    async with pool.connection() as conn:
        for tool in tools:
            params = tool.get("parameters") or {}
            validate_json_schema_requires_array_items(params, context=f"tool[{tool.get('tool_name')!s}].parameters")
            validate_json_schema(params, context=f"tool[{tool.get('tool_name')!s}].parameters")
            await conn.execute(
                sql_tool,
                (
                    project_id,
                    tool["tool_name"],
                    tool["target_subject"],
                    tool["after_execution"],
                    json.dumps(params),
                    json.dumps(tool.get("options") or {}),
                ),
            )
        # Remove deprecated internal tools from legacy generations.
        await conn.execute(
            """
            DELETE FROM resource.tools
            WHERE project_id=%s
              AND tool_name IN (
                'plan_modules',
                'pipe_plan_modules',
                'dispatch_turn',
                'discover_delegates',
                'delegate_agent_async'
              )
            """,
            (project_id,),
        )


async def bootstrap_cardbox(dsn: str) -> None:
    from card_box_core.adapters.async_storage import AsyncPostgresStorageAdapter
    from card_box_core.config import PostgresStorageAdapterSettings

    adapter = AsyncPostgresStorageAdapter(
        PostgresStorageAdapterSettings(dsn=dsn, auto_bootstrap=False)
    )
    try:
        await adapter.open()
        await adapter.setup_schema()
    finally:
        await adapter.close()


class ProjectBootstrapper:
    def __init__(self, *, pool: AsyncConnectionPool):
        self.pool = pool

    async def bootstrap(self, project_id: str) -> None:
        await seed_tools(self.pool, project_id)
