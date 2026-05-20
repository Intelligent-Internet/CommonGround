from __future__ import annotations

import psycopg


SCHEMA_SQL = """
create table if not exists cg_agents (
  project_id text not null,
  agent_id text not null,
  role text null,
  description text null,
  enabled boolean not null,
  accepts_work boolean not null,
  capacity bigint not null,
  capabilities text[] not null,
  grants text[] not null,
  public_metadata jsonb not null default '{}'::jsonb,
  registered_by_agent_id text null,
  registration_provenance_kind text null,
  registration_provenance_ref text null,
  registration_provenance_payload_hash text null,
  admitted_spec_hash text null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  last_seen_at timestamptz null,
  primary key (project_id, agent_id)
);

create table if not exists cg_agent_credentials (
  credential_id text primary key,
  project_id text not null,
  agent_id text not null,
  secret_hash text not null,
  status text not null,
  issued_by_agent_id text null,
  provenance_kind text null,
  provenance_ref text null,
  provenance_payload_hash text null,
  expires_at timestamptz null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  revoked_at timestamptz null,
  last_used_at timestamptz null,
  constraint cg_agent_credentials_status_check check (
    status in ('active', 'revoked')
  )
);

create table if not exists cg_turns (
  project_id text not null,
  turn_id text not null,
  project_turn_seq bigint not null,
  target_agent_id text not null,
  turn_kind text not null default 'turn.conversation.v1',
  cause_kind text not null,
  cause_id text not null,
  state text not null,
  outcome text null,
  stop_requested boolean not null,
  current_claim_agent_id text null,
  current_claim_token_hash text null,
  claim_expires_at timestamptz null,
  spawn_key text not null,
  final_record_role text null,
  final_cardbox_ref_project_id text null,
  final_cardbox_ref_cardbox_id text null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  closed_at timestamptz null,
  last_turn_seq bigint not null,
  primary key (project_id, turn_id)
);

create table if not exists cg_project_turn_counters (
  project_id text primary key,
  last_assigned_turn_seq bigint not null
);

create table if not exists cg_semantic_records (
  project_id text not null,
  record_id text not null,
  turn_id text not null,
  turn_seq bigint not null,
  record_role text not null,
  cardbox_project_id text not null,
  cardbox_id text not null,
  created_at timestamptz not null,
  primary key (project_id, record_id),
  unique (project_id, turn_id, turn_seq)
);

create table if not exists cg_spawn_envelopes (
  project_id text not null,
  spawn_key text not null,
  turn_kind text not null default 'turn.conversation.v1',
  requested_by_agent_id text not null,
  cause_fingerprint text not null,
  bootstrap_bundle_ref_project_id text not null,
  bootstrap_bundle_ref_cardbox_id text not null,
  target_agent_id text not null,
  turn_id text not null,
  created_at timestamptz not null,
  primary key (project_id, spawn_key)
);

create table if not exists cg_kernel_ledger (
  ledger_seq bigint generated always as identity primary key,
  project_id text not null,
  event_type text not null,
  subject_kind text not null,
  subject_id text not null,
  actor_kind text not null,
  actor_id text not null,
  cause_kind text null,
  cause_id text null,
  created_at timestamptz not null,
  note text null,
  annotations jsonb not null,
  payload_ref_project_id text null,
  payload_ref_cardbox_id text null
);

create table if not exists cg_ledger_scope_index (
  project_id text not null,
  scope_kind text not null,
  scope_id text not null,
  ledger_seq bigint not null,
  primary key (project_id, scope_kind, scope_id, ledger_seq)
);

create unique index if not exists cg_provision_launch_started_one_per_turn
  on cg_semantic_records (project_id, turn_id)
  where record_role = 'provision_launch_started';

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_agent_credentials_agent_fk') then
    alter table cg_agent_credentials
      add constraint cg_agent_credentials_agent_fk
      foreign key (project_id, agent_id)
      references cg_agents (project_id, agent_id)
      on delete cascade;
  end if;
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_agent_fk') then
    alter table cg_turns
      add constraint cg_turns_agent_fk
      foreign key (project_id, target_agent_id)
      references cg_agents (project_id, agent_id);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_semantic_records_turn_fk') then
    alter table cg_semantic_records
      add constraint cg_semantic_records_turn_fk
      foreign key (project_id, turn_id)
      references cg_turns (project_id, turn_id)
      on delete cascade;
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_spawn_envelopes_turn_fk') then
    alter table cg_spawn_envelopes
      add constraint cg_spawn_envelopes_turn_fk
      foreign key (project_id, turn_id)
      references cg_turns (project_id, turn_id)
      on delete cascade;
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_ledger_scope_index_ledger_fk') then
    alter table cg_ledger_scope_index
      add constraint cg_ledger_scope_index_ledger_fk
      foreign key (ledger_seq)
      references cg_kernel_ledger (ledger_seq)
      on delete cascade;
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_semantic_records_cardbox_project_check') then
    alter table cg_semantic_records
      add constraint cg_semantic_records_cardbox_project_check
      check (cardbox_project_id = project_id);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_state_check') then
    alter table cg_turns
      add constraint cg_turns_state_check
      check (state in ('queued', 'running', 'suspended', 'closed'));
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_outcome_value_check') then
    alter table cg_turns
      add constraint cg_turns_outcome_value_check
      check (outcome is null or outcome in ('succeeded', 'failed', 'stopped'));
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_running_claim_check') then
    alter table cg_turns
      add constraint cg_turns_running_claim_check
      check (
        (
          state = 'running'
          and current_claim_agent_id is not null
          and current_claim_token_hash is not null
          and claim_expires_at is not null
        )
        or (
          state <> 'running'
          and current_claim_agent_id is null
          and current_claim_token_hash is null
          and claim_expires_at is null
        )
      );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_closed_outcome_check') then
    alter table cg_turns
      add constraint cg_turns_closed_outcome_check
      check (
        (
          state = 'closed'
          and outcome is not null
          and closed_at is not null
        )
        or (
          state <> 'closed'
          and outcome is null
          and closed_at is null
        )
      );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_final_result_ref_check') then
    alter table cg_turns
      add constraint cg_turns_final_result_ref_check
      check (
        (
          final_record_role is null
          and final_cardbox_ref_project_id is null
          and final_cardbox_ref_cardbox_id is null
        )
        or (
          final_record_role is not null
          and final_cardbox_ref_project_id = project_id
          and final_cardbox_ref_cardbox_id is not null
        )
      );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'cg_turns_project_turn_seq_unique') then
    alter table cg_turns
      add constraint cg_turns_project_turn_seq_unique
      unique (project_id, project_turn_seq);
  end if;
end $$;
"""


def _connect(pg_dsn: str):
    conn = psycopg.connect(pg_dsn)
    conn.autocommit = False
    return conn


def ensure_schema(pg_dsn: str) -> None:
    conn = _connect(pg_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_lock(hashtext(%s))", ("commonground_v3_schema",))
            cur.execute(SCHEMA_SQL)
            cur.execute("select pg_advisory_unlock(hashtext(%s))", ("commonground_v3_schema",))
        conn.commit()
    finally:
        conn.close()
