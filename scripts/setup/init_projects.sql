-- Minimal projects metadata table (public.projects)
-- Used by scripts/setup/seed.py and services/api

CREATE TABLE IF NOT EXISTS public.projects (
    project_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','inactive','archived')),
    owner_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_projects_owner_id ON public.projects (owner_id);

-- Fix permission denied issues after reset_db (for non-owner application users)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.projects TO public;
