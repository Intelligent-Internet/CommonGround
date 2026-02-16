import argparse
import asyncio
import os
import sys

from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from core.utils import set_loop_policy
from infra.project_bootstrap import bootstrap_cardbox, ensure_schema
from scripts.utils.config import load_toml_config, resolve_cardbox_dsn

set_loop_policy()

# 0. Load Config (Env > Config File)
cfg = load_toml_config()
DSN, source = resolve_cardbox_dsn(cfg)
if source == "env":
    print("Using DSN from Environment (PG_DSN)")
elif source == "missing":
    print("❌ config.toml not found!")
    sys.exit(1)
else:
    print("Using DSN from config.toml")

if not DSN:
    print("❌ No DSN found. Set PG_DSN env var or configure in config.toml")
    sys.exit(1)

# Mask password for display
safe_dsn = DSN.split("@")[-1] if "@" in DSN else "..."
print(f"Target DB: ...@{safe_dsn}")


async def reset_db(grant_role: str | None = None):
    print("⚠️  WARNING: This will wipe ALL data (State, Agents, Tools) AND CardBox data (Cards/Boxes).")
    print("⚠️  All schemas will be dropped and re-created from scratch.")
    confirm = input("Are you sure? (y/n): ")
    if confirm.lower() != 'y':
        print("Aborted.")
        return

    pool = AsyncConnectionPool(DSN, open=False)
    await pool.open()

    # 1. Drop Tables (Clean Slate)
    drop_sqls = [
        "DROP SCHEMA IF EXISTS sandbox CASCADE;",
        "DROP SCHEMA IF EXISTS application CASCADE;",
        # Project schema tables
        "DROP TABLE IF EXISTS resource.tools CASCADE;",
        "DROP TABLE IF EXISTS resource.project_agents CASCADE;",
        "DROP TABLE IF EXISTS resource.profiles CASCADE;",
        "DROP TABLE IF EXISTS resource.skill_versions CASCADE;",
        "DROP TABLE IF EXISTS resource.skills CASCADE;",
        "DROP TABLE IF EXISTS resource.artifacts CASCADE;",
        "DROP TABLE IF EXISTS resource.sandboxes CASCADE;",
        # Issue #42: Batch tables
        "DROP TABLE IF EXISTS state.pmo_batch_tasks CASCADE;",
        "DROP TABLE IF EXISTS state.pmo_batches CASCADE;",
        # v1r2 audit + inbox tables
        "DROP TABLE IF EXISTS state.identity_edges CASCADE;",
        "DROP TABLE IF EXISTS state.execution_edges CASCADE;",
        "DROP TABLE IF EXISTS state.agent_inbox CASCADE;",
        "DROP TABLE IF EXISTS state.agent_state_pointers CASCADE;",
        "DROP TABLE IF EXISTS state.agent_state_head CASCADE;",
        "DROP TABLE IF EXISTS state.agent_steps CASCADE;",
        # CardBox (card-box-cg) schema objects
        "DROP TABLE IF EXISTS card_transformations CASCADE;",
        "DROP TABLE IF EXISTS card_box_history_logs CASCADE;",
        "DROP TABLE IF EXISTS card_operation_logs CASCADE;",
        "DROP TABLE IF EXISTS card_boxes CASCADE;",
        "DROP TABLE IF EXISTS cards CASCADE;",
        "DROP TABLE IF EXISTS sync_queue CASCADE;",
        "DROP TABLE IF EXISTS api_logs CASCADE;",
        "DROP TABLE IF EXISTS public.projects CASCADE;",
    ]

    async with pool.connection() as conn:
        print("🗑️  Dropping tables...")
        for query_str in drop_sqls:
            try:
                await conn.execute(query_str)
            except Exception as e:
                print(f"Error executing {query_str}: {e}")

    # 2. Run Init SQLs (Project)
    print("🏗️  Re-creating schema (Project)...")
    try:
        await ensure_schema(pool)
    except Exception as e:
        print(f"❌ Error executing ensure_schema: {e}")
        await pool.close()
        return

    await pool.close()

    # 3. Run Init Schema (CardBox) - using separate sync connection
    print("🏗️  Re-creating schema (CardBox)...")
    try:
        await bootstrap_cardbox(DSN)
    except Exception as e:
        print(f"❌ Error bootstrapping CardBox: {e}")
        return

    # 4. Grant Permissions (if requested)
    if grant_role:
        print(f"🔐 Granting permissions to role: {grant_role}...")
        pool = AsyncConnectionPool(DSN, open=False)
        await pool.open()
        try:
            async with pool.connection() as conn:
                # Grant Usage on Schemas
                await conn.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA public, resource, state, application TO {}").format(
                        sql.Identifier(grant_role)
                    )
                )
                
                for schema in ["public", "resource", "state", "application"]:
                    # Grant All on Tables
                    await conn.execute(
                        sql.SQL("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} TO {}").format(
                            sql.Identifier(schema), sql.Identifier(grant_role)
                        )
                    )
                    # Grant All on Sequences
                    await conn.execute(
                        sql.SQL("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                            sql.Identifier(schema), sql.Identifier(grant_role)
                        )
                    )
                print("✅ Permissions granted.")
        except Exception as e:
            print(f"❌ Error granting permissions: {e}")
        finally:
            await pool.close()

    print("✅ Database reset & schema re-creation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--grant", help="Grant privileges to this role (e.g. cardbox)")
    args = parser.parse_args()

    asyncio.run(reset_db(grant_role=args.grant))
