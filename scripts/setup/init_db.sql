-- Database Initialization
-- Run this to setup the required schema for the Generic Agent Worker

CREATE SCHEMA IF NOT EXISTS resource;
CREATE SCHEMA IF NOT EXISTS state;
CREATE SCHEMA IF NOT EXISTS application;

-- 1. Roster Table (resource.project_agents)
CREATE TABLE IF NOT EXISTS resource.project_agents (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    profile_box_id UUID NOT NULL,
    worker_target TEXT NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    display_name TEXT,
    owner_agent_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    PRIMARY KEY (project_id, agent_id)
);

-- 1b. Tool Registry (for PMO / A2A server)
CREATE TABLE IF NOT EXISTS resource.tools (
    project_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    description TEXT,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    target_subject TEXT,
    after_execution TEXT,
    options JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (project_id, tool_name)
);

-- 1c. Profile cache (optional)
CREATE TABLE IF NOT EXISTS resource.profiles (
    project_id TEXT NOT NULL,
    profile_box_id TEXT NOT NULL,
    name TEXT NOT NULL,
    worker_target TEXT NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    system_prompt_compiled TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, profile_box_id)
);

-- Within a project, profile "name" must be unique (seed/idempotency + name lookup).
CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_profiles_project_name
ON resource.profiles (project_id, name);

-- 1c2. Skills registry (GCS-backed)
CREATE TABLE IF NOT EXISTS resource.skills (
    project_id TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    allow_scripts BOOLEAN NOT NULL DEFAULT FALSE,
    active_version_hash TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, skill_name)
);

CREATE TABLE IF NOT EXISTS resource.skill_versions (
    project_id TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    version_hash TEXT NOT NULL,
    storage_uri TEXT NOT NULL,
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, skill_name, version_hash)
);

CREATE INDEX IF NOT EXISTS idx_resource_skill_versions_active
ON resource.skill_versions (project_id, skill_name, created_at DESC);

-- 1c3. Artifact registry (GCS-backed)
CREATE TABLE IF NOT EXISTS resource.artifacts (
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    filename TEXT,
    mime TEXT,
    size BIGINT NOT NULL DEFAULT 0,
    sha256 TEXT,
    storage_uri TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, artifact_id)
);

-- 1c4. E2B Sandbox registry (per project+agent)
CREATE TABLE IF NOT EXISTS resource.sandboxes (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    domain TEXT,
    template TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    hard_expires_at TIMESTAMPTZ,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    lock_expires_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (project_id, agent_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_sandboxes_sandbox_id
ON resource.sandboxes (sandbox_id);

CREATE INDEX IF NOT EXISTS idx_resource_sandboxes_expires
ON resource.sandboxes (expires_at, hard_expires_at);

-- 1e. Skills task/job heads (status persistence)
CREATE TABLE IF NOT EXISTS state.skill_task_heads (
    task_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_state_skill_task_heads_project_status
ON state.skill_task_heads (project_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS state.skill_job_heads (
    job_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_state_skill_job_heads_project_status
ON state.skill_job_heads (project_id, status, updated_at DESC);

-- 1d. Legacy plan tables cleanup (plan_modules/plan_heads removed from runtime).
-- NOTE: Destructive cleanup has been disabled in init_db.sql to keep this script idempotent.
--       If you still need to remove legacy plan tables, do so in a dedicated migration script.
-- DROP TABLE IF EXISTS resource.plan_modules;
-- DROP TABLE IF EXISTS resource.plan_heads;
-- 2. State Head Table (state.agent_state_head)
CREATE TABLE IF NOT EXISTS state.agent_state_head (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    status TEXT DEFAULT 'idle',
    active_agent_turn_id TEXT,
    active_channel_id TEXT,
    turn_epoch BIGINT DEFAULT 0,
    active_recursion_depth INT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    parent_step_id TEXT,
    trace_id TEXT,
    profile_box_id UUID,
    context_box_id UUID,
    output_box_id UUID,
    expecting_correlation_id TEXT,
    waiting_tool_count INT NOT NULL DEFAULT 0,
    resume_deadline TIMESTAMPTZ,
    PRIMARY KEY (project_id, agent_id)
);
ALTER TABLE state.agent_state_head
    ADD COLUMN IF NOT EXISTS waiting_tool_count INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_agent_head_status_updated
ON state.agent_state_head(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_agent_head_suspended_waiting
ON state.agent_state_head(project_id, agent_id, updated_at DESC)
WHERE status='suspended' AND waiting_tool_count > 0;

CREATE TABLE IF NOT EXISTS state.turn_waiting_tools (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_turn_id TEXT NOT NULL,
    turn_epoch BIGINT NOT NULL,
    step_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    wait_status TEXT NOT NULL CHECK (wait_status IN ('waiting', 'received', 'applied', 'expired', 'dropped')),
    tool_result_card_id TEXT,
    received_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id)
);
CREATE INDEX IF NOT EXISTS idx_turn_waiting_tools_turn
ON state.turn_waiting_tools (project_id, agent_id, agent_turn_id, turn_epoch, wait_status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_turn_waiting_tools_timeout
ON state.turn_waiting_tools (project_id, agent_id, wait_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS state.turn_resume_ledger (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_turn_id TEXT NOT NULL,
    turn_epoch BIGINT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_result_card_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'applied', 'dropped')),
    attempt_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id, tool_result_card_id)
);
CREATE INDEX IF NOT EXISTS idx_turn_resume_ledger_claim
ON state.turn_resume_ledger (project_id, agent_id, agent_turn_id, turn_epoch, status, next_retry_at ASC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_turn_resume_ledger_lease
ON state.turn_resume_ledger (status, lease_expires_at ASC, updated_at DESC);

-- 2b. Agent State Pointers (state.agent_state_pointers)
-- Cross-turn pointers that should NOT be part of L0 CAS gating.
-- - memory_context_box_id: agent-level default context pointer (fallback when caller omits context)
-- - last_output_box_id: agent-level pointer to last completed output (output_mode="reuse", history_focus, debug)
CREATE TABLE IF NOT EXISTS state.agent_state_pointers (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    memory_context_box_id UUID,
    last_output_box_id UUID,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, agent_id)
);

-- 3. Step audit table (per ReAct step)
CREATE TABLE IF NOT EXISTS state.agent_steps (
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    agent_turn_id TEXT NOT NULL,
    channel_id TEXT,
    parent_step_id TEXT,
    trace_id TEXT,
    status TEXT NOT NULL,
    profile_box_id UUID,
    context_box_id UUID,
    output_box_id UUID,
    request_box_id UUID,
    api_log_id BIGINT,
    tool_call_ids JSONB,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    PRIMARY KEY (project_id, agent_id, step_id)
);
CREATE INDEX IF NOT EXISTS idx_agent_steps_agent_turn ON state.agent_steps(agent_turn_id);
CREATE INDEX IF NOT EXISTS idx_agent_steps_status ON state.agent_steps(status);
CREATE INDEX IF NOT EXISTS idx_agent_steps_channel ON state.agent_steps(channel_id);
CREATE INDEX IF NOT EXISTS idx_agent_steps_trace ON state.agent_steps(trace_id);
CREATE INDEX IF NOT EXISTS idx_agent_steps_parent_step ON state.agent_steps(parent_step_id);
CREATE INDEX IF NOT EXISTS idx_agent_steps_project_started ON state.agent_steps(project_id, started_at DESC, step_id DESC);

-- 4. Execution Inbox + Audit Edges (v1r2)
CREATE TABLE IF NOT EXISTS state.agent_inbox (
    inbox_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    message_type TEXT NOT NULL,
    enqueue_mode TEXT,
    correlation_id TEXT,
    recursion_depth INT NOT NULL,
    traceparent TEXT NOT NULL,
    tracestate TEXT,
    trace_id TEXT,
    parent_step_id TEXT,
    source_agent_id TEXT,
    source_step_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    processed_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_agent_inbox_agent_created
ON state.agent_inbox(project_id, agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_inbox_pending
ON state.agent_inbox(project_id, agent_id, created_at DESC)
WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_agent_inbox_queued_turn
ON state.agent_inbox(project_id, agent_id, created_at ASC)
WHERE status='queued' AND message_type='turn';
CREATE INDEX IF NOT EXISTS idx_agent_inbox_pending_created
ON state.agent_inbox(created_at)
WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_agent_inbox_correlation
ON state.agent_inbox USING hash (correlation_id);
CREATE INDEX IF NOT EXISTS idx_agent_inbox_trace
ON state.agent_inbox(project_id, trace_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_inbox_unique
ON state.agent_inbox(project_id, message_type, md5(correlation_id));

CREATE TABLE IF NOT EXISTS state.execution_edges (
    edge_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    channel_id TEXT,
    primitive TEXT NOT NULL,
    edge_phase TEXT NOT NULL,
    source_agent_id TEXT,
    source_agent_turn_id TEXT,
    source_step_id TEXT,
    target_agent_id TEXT,
    target_agent_turn_id TEXT,
    correlation_id TEXT,
    enqueue_mode TEXT,
    recursion_depth INT,
    trace_id TEXT,
    parent_step_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_execution_edges_correlation
ON state.execution_edges USING hash (correlation_id);
CREATE INDEX IF NOT EXISTS idx_execution_edges_trace
ON state.execution_edges(project_id, trace_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_edges_unique
ON state.execution_edges(project_id, md5(correlation_id), primitive, edge_phase);

CREATE TABLE IF NOT EXISTS state.identity_edges (
    edge_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    channel_id TEXT,
    action TEXT NOT NULL,
    source_agent_id TEXT,
    target_agent_id TEXT NOT NULL,
    trace_id TEXT,
    parent_step_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Batch Manager tables (Issue #42 / fanout-join batch orchestration)
CREATE TABLE IF NOT EXISTS state.pmo_batches (
    batch_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    parent_agent_id TEXT NOT NULL,
    parent_agent_turn_id TEXT NOT NULL,
    parent_tool_call_id TEXT NOT NULL,
    parent_turn_epoch BIGINT NOT NULL,
    parent_channel_id TEXT,
    task_count INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed', 'timeout', 'partial')),
    deadline_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pmo_batches_status_deadline ON state.pmo_batches(status, deadline_at)
WHERE status='running';
CREATE INDEX IF NOT EXISTS idx_pmo_batches_project_created ON state.pmo_batches(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS state.pmo_batch_tasks (
    batch_task_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','dispatched','success','failed','canceled','timeout','partial')),
    retryable BOOLEAN NOT NULL DEFAULT TRUE,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count INT NOT NULL DEFAULT 0,
    current_agent_turn_id TEXT,
    current_turn_epoch BIGINT,
    payload_hash TEXT,
    profile_box_id UUID,
    context_box_id UUID,
    output_box_id UUID,
    error_box_id UUID,
    error_detail TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_pmo_batch
        FOREIGN KEY (batch_id)
        REFERENCES state.pmo_batches(batch_id)
        ON DELETE CASCADE
);
ALTER TABLE state.pmo_batch_tasks
    ADD COLUMN IF NOT EXISTS retryable BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE state.pmo_batch_tasks
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_pmo_batch_tasks_batch_status ON state.pmo_batch_tasks(batch_id, status);
CREATE INDEX IF NOT EXISTS idx_pmo_batch_tasks_pending_created ON state.pmo_batch_tasks(status, created_at)
WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_pmo_batch_tasks_pending_retry ON state.pmo_batch_tasks(status, retryable, next_retry_at, created_at)
WHERE status='pending' AND retryable=TRUE;
CREATE INDEX IF NOT EXISTS idx_pmo_batch_tasks_project_batch ON state.pmo_batch_tasks(project_id, batch_id);
