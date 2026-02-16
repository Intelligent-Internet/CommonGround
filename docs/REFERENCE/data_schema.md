# Data Schema (Code-Verified Reference)

This document is a code-verified snapshot of the database schema used by this repository.

Source of truth files:
- `scripts/setup/init_projects.sql`
- `scripts/setup/init_db.sql`
- `scripts/setup/init_pmo_db.sql` (currently adds no tables)
- `card-box-cg/card_box_core/adapters/postgres_schema.sql` (CardBox subsystem)

## 1. Schema namespaces created by bootstrap

`ensure_schema()` in `infra/project_bootstrap.py` executes:
1. `init_projects.sql`
2. `init_db.sql`
3. `init_pmo_db.sql`

Namespaces created in project DB bootstrap:
- `public`
- `resource`
- `state`
- `application` (created, no tables in current SQL)

## 2. `public` schema

### `public.projects`
Primary key:
- `(project_id)`

Columns:
| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| project_id | text | NO | - | PK |
| title | text | NO | - | - |
| status | text | NO | `'active'` | CHECK: `active/inactive/archived` |
| owner_id | text | NO | - | - |
| created_at | timestamptz | NO | `NOW()` | - |
| updated_at | timestamptz | NO | `NOW()` | - |

Indexes:
- `idx_projects_owner_id` on `(owner_id)`

Grants in SQL:
- `GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.projects TO public;`

## 3. `resource` schema

### `resource.project_agents`
Primary key:
- `(project_id, agent_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| profile_box_id | uuid | NO | - |
| worker_target | text | NO | - |
| tags | jsonb | NO | `'[]'::jsonb` |
| display_name | text | YES | - |
| owner_agent_id | text | YES | - |
| status | text | NO | `'active'` |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |
| last_seen_at | timestamptz | YES | - |

### `resource.tools`
Primary key:
- `(project_id, tool_name)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| tool_name | text | NO | - |
| description | text | YES | - |
| parameters | jsonb | NO | `'{}'::jsonb` |
| target_subject | text | YES | - |
| after_execution | text | YES | - |
| options | jsonb | NO | `'{}'::jsonb` |

### `resource.profiles`
Primary key:
- `(project_id, profile_box_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| profile_box_id | text | NO | - |
| name | text | NO | - |
| worker_target | text | NO | - |
| tags | jsonb | NO | `'[]'::jsonb` |
| system_prompt_compiled | text | YES | - |
| updated_at | timestamptz | YES | `NOW()` |

Indexes:
- Unique index `idx_resource_profiles_project_name` on `(project_id, name)`

### `resource.skills`
Primary key:
- `(project_id, skill_name)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| skill_name | text | NO | - |
| description | text | YES | - |
| enabled | boolean | NO | `TRUE` |
| allow_scripts | boolean | NO | `FALSE` |
| active_version_hash | text | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |
| updated_at | timestamptz | YES | `NOW()` |

### `resource.skill_versions`
Primary key:
- `(project_id, skill_name, version_hash)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| skill_name | text | NO | - |
| version_hash | text | NO | - |
| storage_uri | text | NO | - |
| description | text | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |

Indexes:
- `idx_resource_skill_versions_active` on `(project_id, skill_name, created_at DESC)`

### `resource.artifacts`
Primary key:
- `(project_id, artifact_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| artifact_id | text | NO | - |
| filename | text | YES | - |
| mime | text | YES | - |
| size | bigint | NO | `0` |
| sha256 | text | YES | - |
| storage_uri | text | NO | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |

### `resource.sandboxes`
Primary key:
- `(project_id, agent_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| sandbox_id | text | NO | - |
| domain | text | YES | - |
| template | text | YES | - |
| status | text | NO | `'active'` |
| created_at | timestamptz | YES | `NOW()` |
| last_used_at | timestamptz | YES | `NOW()` |
| expires_at | timestamptz | YES | - |
| hard_expires_at | timestamptz | YES | - |
| locked_by | text | YES | - |
| locked_at | timestamptz | YES | - |
| lock_expires_at | timestamptz | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |

Indexes:
- Unique index `idx_resource_sandboxes_sandbox_id` on `(sandbox_id)`
- `idx_resource_sandboxes_expires` on `(expires_at, hard_expires_at)`

## 4. `state` schema

### `state.skill_task_heads`
Primary key:
- `(task_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| task_id | text | NO | - |
| project_id | text | NO | - |
| status | text | NO | - |
| payload | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |
| updated_at | timestamptz | YES | `NOW()` |

Indexes:
- `idx_state_skill_task_heads_project_status` on `(project_id, status, updated_at DESC)`

### `state.skill_job_heads`
Primary key:
- `(job_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| job_id | text | NO | - |
| project_id | text | NO | - |
| status | text | NO | - |
| payload | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |
| updated_at | timestamptz | YES | `NOW()` |

Indexes:
- `idx_state_skill_job_heads_project_status` on `(project_id, status, updated_at DESC)`

### `state.agent_state_head`
Primary key:
- `(project_id, agent_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| status | text | YES | `'idle'` |
| active_agent_turn_id | text | YES | - |
| active_channel_id | text | YES | - |
| turn_epoch | bigint | YES | `0` |
| active_recursion_depth | int | YES | `0` |
| updated_at | timestamptz | YES | `NOW()` |
| parent_step_id | text | YES | - |
| trace_id | text | YES | - |
| profile_box_id | uuid | YES | - |
| context_box_id | uuid | YES | - |
| output_box_id | uuid | YES | - |
| expecting_correlation_id | text | YES | - |
| waiting_tool_count | int | NO | `0` |
| resume_deadline | timestamptz | YES | - |

Migration-safe statement in SQL:
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS waiting_tool_count INT NOT NULL DEFAULT 0`

Indexes:
- `idx_agent_head_status_updated` on `(status, updated_at)`
- Partial index `idx_agent_head_suspended_waiting` on `(project_id, agent_id, updated_at DESC)` where `status='suspended' AND waiting_tool_count > 0`

### `state.turn_waiting_tools`
Primary key:
- `(project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id)`

Columns:
| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| project_id | text | NO | - | - |
| agent_id | text | NO | - | - |
| agent_turn_id | text | NO | - | - |
| turn_epoch | bigint | NO | - | - |
| step_id | text | NO | - | - |
| tool_call_id | text | NO | - | - |
| tool_name | text | NO | - | - |
| wait_status | text | NO | - | CHECK: `waiting/received/applied/expired/dropped` |
| tool_result_card_id | text | YES | - | - |
| received_at | timestamptz | YES | - | - |
| applied_at | timestamptz | YES | - | - |
| created_at | timestamptz | NO | `NOW()` | - |
| updated_at | timestamptz | NO | `NOW()` | - |

Indexes:
- `idx_turn_waiting_tools_turn` on `(project_id, agent_id, agent_turn_id, turn_epoch, wait_status, created_at ASC)`
- `idx_turn_waiting_tools_timeout` on `(project_id, agent_id, wait_status, updated_at DESC)`

### `state.turn_resume_ledger`
Primary key:
- `(project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id, tool_result_card_id)`

Columns:
| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| project_id | text | NO | - | - |
| agent_id | text | NO | - | - |
| agent_turn_id | text | NO | - | - |
| turn_epoch | bigint | NO | - | - |
| tool_call_id | text | NO | - | - |
| tool_result_card_id | text | NO | - | - |
| status | text | NO | `'pending'` | CHECK: `pending/processing/applied/dropped` |
| attempt_count | int | NO | `0` | - |
| next_retry_at | timestamptz | NO | `NOW()` | - |
| lease_owner | text | YES | - | - |
| lease_expires_at | timestamptz | YES | - | - |
| last_error | text | YES | - | - |
| payload | jsonb | NO | `'{}'::jsonb` | - |
| created_at | timestamptz | NO | `NOW()` | - |
| updated_at | timestamptz | NO | `NOW()` | - |

Indexes:
- `idx_turn_resume_ledger_claim` on `(project_id, agent_id, agent_turn_id, turn_epoch, status, next_retry_at ASC, created_at ASC)`
- `idx_turn_resume_ledger_lease` on `(status, lease_expires_at ASC, updated_at DESC)`

### `state.agent_state_pointers`
Primary key:
- `(project_id, agent_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| memory_context_box_id | uuid | YES | - |
| last_output_box_id | uuid | YES | - |
| updated_at | timestamptz | YES | `NOW()` |

### `state.agent_steps`
Primary key:
- `(project_id, agent_id, step_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| step_id | text | NO | - |
| agent_turn_id | text | NO | - |
| channel_id | text | YES | - |
| parent_step_id | text | YES | - |
| trace_id | text | YES | - |
| status | text | NO | - |
| profile_box_id | uuid | YES | - |
| context_box_id | uuid | YES | - |
| output_box_id | uuid | YES | - |
| request_box_id | uuid | YES | - |
| api_log_id | bigint | YES | - |
| tool_call_ids | jsonb | YES | - |
| error | text | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| started_at | timestamptz | YES | `NOW()` |
| ended_at | timestamptz | YES | - |

Indexes:
- `idx_agent_steps_agent_turn` on `(agent_turn_id)`
- `idx_agent_steps_status` on `(status)`
- `idx_agent_steps_channel` on `(channel_id)`
- `idx_agent_steps_trace` on `(trace_id)`
- `idx_agent_steps_parent_step` on `(parent_step_id)`
- `idx_agent_steps_project_started` on `(project_id, started_at DESC, step_id DESC)`

### `state.agent_inbox`
Primary key:
- `(inbox_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| inbox_id | text | NO | - |
| project_id | text | NO | - |
| agent_id | text | NO | - |
| message_type | text | NO | - |
| enqueue_mode | text | YES | - |
| correlation_id | text | YES | - |
| recursion_depth | int | NO | - |
| traceparent | text | NO | - |
| tracestate | text | YES | - |
| trace_id | text | YES | - |
| parent_step_id | text | YES | - |
| source_agent_id | text | YES | - |
| source_step_id | text | YES | - |
| payload | jsonb | NO | `'{}'::jsonb` |
| status | text | NO | `'pending'` |
| processed_at | timestamptz | YES | - |
| archived_at | timestamptz | YES | - |
| created_at | timestamptz | YES | `NOW()` |

Indexes:
- `idx_agent_inbox_agent_created` on `(project_id, agent_id, created_at DESC)`
- Partial index `idx_agent_inbox_pending` on `(project_id, agent_id, created_at DESC)` where `status='pending'`
- Partial index `idx_agent_inbox_queued_turn` on `(project_id, agent_id, created_at ASC)` where `status='queued' AND message_type='turn'`
- Partial index `idx_agent_inbox_pending_created` on `(created_at)` where `status='pending'`
- Hash index `idx_agent_inbox_correlation` on `(correlation_id)`
- `idx_agent_inbox_trace` on `(project_id, trace_id)`
- Unique expression index `idx_agent_inbox_unique` on `(project_id, message_type, md5(correlation_id))`

Notes from runtime code (`infra/stores/execution_store.py`):
- Common status transitions include `pending -> processing -> consumed/error/skipped`.
- Requeue path can move stale `processing` rows back to `pending`.
- The SQL schema itself does not enforce a status enum for this table.

### `state.execution_edges`
Primary key:
- `(edge_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| edge_id | text | NO | - |
| project_id | text | NO | - |
| channel_id | text | YES | - |
| primitive | text | NO | - |
| edge_phase | text | NO | - |
| source_agent_id | text | YES | - |
| source_agent_turn_id | text | YES | - |
| source_step_id | text | YES | - |
| target_agent_id | text | YES | - |
| target_agent_turn_id | text | YES | - |
| correlation_id | text | YES | - |
| enqueue_mode | text | YES | - |
| recursion_depth | int | YES | - |
| trace_id | text | YES | - |
| parent_step_id | text | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |

Indexes:
- Hash index `idx_execution_edges_correlation` on `(correlation_id)`
- `idx_execution_edges_trace` on `(project_id, trace_id)`
- Unique expression index `idx_execution_edges_unique` on `(project_id, md5(correlation_id), primitive, edge_phase)`

### `state.identity_edges`
Primary key:
- `(edge_id)`

Columns:
| Column | Type | Null | Default |
| --- | --- | --- | --- |
| edge_id | text | NO | - |
| project_id | text | NO | - |
| channel_id | text | YES | - |
| action | text | NO | - |
| source_agent_id | text | YES | - |
| target_agent_id | text | NO | - |
| trace_id | text | YES | - |
| parent_step_id | text | YES | - |
| metadata | jsonb | NO | `'{}'::jsonb` |
| created_at | timestamptz | YES | `NOW()` |

### `state.pmo_batches`
Primary key:
- `(batch_id)`

Columns:
| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| batch_id | text | NO | - | PK |
| project_id | text | NO | - | - |
| parent_agent_id | text | NO | - | - |
| parent_agent_turn_id | text | NO | - | - |
| parent_tool_call_id | text | NO | - | - |
| parent_turn_epoch | bigint | NO | - | - |
| parent_channel_id | text | YES | - | - |
| task_count | int | NO | - | - |
| status | text | NO | `'running'` | CHECK: `running/success/failed/timeout/partial` |
| deadline_at | timestamptz | YES | - | - |
| metadata | jsonb | NO | `'{}'::jsonb` | - |
| created_at | timestamptz | YES | `NOW()` | - |
| updated_at | timestamptz | YES | `NOW()` | - |

Indexes:
- Partial index `idx_pmo_batches_status_deadline` on `(status, deadline_at)` where `status='running'`
- `idx_pmo_batches_project_created` on `(project_id, created_at DESC)`

### `state.pmo_batch_tasks`
Primary key:
- `(batch_task_id)`

Foreign key:
- `batch_id -> state.pmo_batches(batch_id)` with `ON DELETE CASCADE`

Columns:
| Column | Type | Null | Default | Notes |
| --- | --- | --- | --- | --- |
| batch_task_id | text | NO | - | PK |
| batch_id | text | NO | - | FK |
| project_id | text | NO | - | - |
| agent_id | text | NO | - | - |
| status | text | NO | `'pending'` | CHECK: `pending/dispatched/success/failed/canceled/timeout/partial` |
| retryable | boolean | NO | `TRUE` | added with `ADD COLUMN IF NOT EXISTS` |
| next_retry_at | timestamptz | NO | `NOW()` | added with `ADD COLUMN IF NOT EXISTS` |
| attempt_count | int | NO | `0` | - |
| current_agent_turn_id | text | YES | - | - |
| current_turn_epoch | bigint | YES | - | - |
| payload_hash | text | YES | - | - |
| profile_box_id | uuid | YES | - | - |
| context_box_id | uuid | YES | - | - |
| output_box_id | uuid | YES | - | - |
| error_box_id | uuid | YES | - | - |
| error_detail | text | YES | - | - |
| metadata | jsonb | NO | `'{}'::jsonb` | - |
| created_at | timestamptz | YES | `NOW()` | - |
| updated_at | timestamptz | YES | `NOW()` | - |

Indexes:
- `idx_pmo_batch_tasks_batch_status` on `(batch_id, status)`
- Partial index `idx_pmo_batch_tasks_pending_created` on `(status, created_at)` where `status='pending'`
- Partial index `idx_pmo_batch_tasks_pending_retry` on `(status, retryable, next_retry_at, created_at)` where `status='pending' AND retryable=TRUE`
- `idx_pmo_batch_tasks_project_batch` on `(project_id, batch_id)`

## 5. Legacy/removed tables

`resource.plan_heads` and `resource.plan_modules` are not created by current bootstrap SQL. In `init_db.sql`, drop statements are commented out and marked as legacy cleanup notes.

## 6. CardBox subsystem schema (separate SQL)

CardBox tables are defined in `card-box-cg/card_box_core/adapters/postgres_schema.sql`:
- `cards`
- `card_operation_logs`
- `card_transformations`
- `card_boxes`
- `card_box_history_logs`
- `sync_queue`
- `api_logs`

This repository's project bootstrap does not create these tables in `init_db.sql`; they belong to the CardBox adapter setup path.
