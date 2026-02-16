-- PMO schema
-- Run AFTER scripts/setup/init_db.sql. This file only contains PMO-specific tables.

-- Ensure schemas exist (idempotent for local dev)
CREATE SCHEMA IF NOT EXISTS resource;
CREATE SCHEMA IF NOT EXISTS state;

-- Enum: lifecycle effect after the tool is invoked
-- (Simplified to just 'suspend' or 'terminate' strings in check constraint, but keeping enum optional or just using text check)
-- DO $$
-- BEGIN
--     IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tool_lifecycle_effect') THEN
--         CREATE TYPE tool_lifecycle_effect AS ENUM ('continuing', 'suspending', 'terminating');
--     END IF;
-- END$$;

-- NOTE:
-- PMO routing/plan semantics are implemented via code-only internal handlers:
-- `cg.{ver}.{project}.{channel}.cmd.sys.pmo.internal.>`
-- This file intentionally does not create `resource.pmo_functions`.
