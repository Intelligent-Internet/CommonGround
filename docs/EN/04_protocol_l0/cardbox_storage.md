# CardBox: Immutable Fact Storage

CardBox is the L0 protocol layer's persistence foundation for facts and context. It stores references to semantic cards (`cards`) and card containers (`boxes`).
It does not hold the full state for `state.*` / `resource.*` tables (those are still persisted by their respective stores); instead it is the unified semantic storage entry for L0.

## 1. Core Design Philosophy

### 1.1 Everything Is a Card
All data units in the system—whether user instructions, LLM responses, tool call results, or static Project configuration and User Profile—are uniformly encapsulated as a **Card**.
- **Atomicity**: A Card is the smallest unit of information.
- **Immutability (semantic layer)**: Standard practice is to create a new Card and use a new `card_id` for every change.
  - The current implementation of `add_card` uses `ON CONFLICT DO UPDATE` for the same `card_id` (an idempotent replay-repair path), so repeated writes to the same ID should not be treated as an update model at business level.
- **Global identifier**: Every Card has a globally unique `card_id` (usually a UUID).

### 1.2 CardStore: Single Source of Semantic Truth
`CardStore` is the physical storage repository for all semantic `Card`s (mapped to Postgres `cards` table).
- **Decentralized references**: Upper-layer logic (`context_box`/`output_box`, resource snapshot, etc.) only keeps `card_id` references.
- **Auditability and traceability**: `metadata` and `created_at` support replay-based traceability; `deleted_at` provides soft-delete semantics (not full purge).

### 1.3 CardBox: Linear Context Container
`CardBox` is an ordered set of `card_id`s that defines a linear context (`Context`).
- **Purpose**: One CardBox instance typically represents a conversation history segment, a task context snapshot, or a set of output results.
- **Reference over copy**: A box stores only ID lists, so multiple boxes may reference the same Card (for example, the same Profile across multiple dialog turns).
- **Dual-Box pattern**: Workers use `context_box_id` (read-only input) and `output_box_id` (write-only output) to isolate side effects.
- **Visibility boundary (important)**: Boxes are centrally stored within a project, but an individual Agent Turn's LLM only sees the input snapshot assembled into `context_box_id` and the appended content of that Turn's `output_box_id`. Cross-agent sharing requires explicit `box_id` passing and context packing/inheritance (for example via PMO context packing or tools).
  - Reference: `03_kernel_l1/pmo_orchestration.md`
- **Box mutability and snapshot semantics**: A Box is a mutable container (append/update card_ids). A “snapshot” is protocol semantics for the set currently pointed to by `context_box_id` / `output_box_id`.
- **Box replacement (compression/rewrite)**: When context compression or rewriting is needed, upper layers can create a new box and replace `context_box_id` / `output_box_id`; the protocol does not require box IDs to remain immutable.

## 2. Data Model (Schema)

CardBox's core tables are intentionally simple and use JSONB to store polymorphic content.

### 2.1 `cards` Table
```sql
CREATE TABLE cards (
    card_id     TEXT PRIMARY KEY,       -- globally unique ID
    tenant_id   TEXT NOT NULL,         -- project-isolation key (corresponds to project_id)
    content     JSONB,                 -- primary payload
    tool_calls  JSONB,
    tool_call_id TEXT,
    ttl_seconds INTEGER,
    expires_at  TIMESTAMPTZ,
    deleted_at  TIMESTAMPTZ,
    metadata    JSONB,                 -- metadata (including type/role/step_id/trace_id, etc.)
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Semantic type is stored in metadata->>'type', not a dedicated column
```

### 2.2 `boxes` table (current implementation)
The current implementation stores Box data in the `card-box-cg` physical table `card_boxes` (separate from `cards`, with primary key `box_id`).
The older “field-store / dynamic-construction” wording is outdated and retained only as historical context.
In the L1 state machine, the `state` table explicitly stores Box ID pointers:
```sql
-- Example: Agent runtime state
state.agent_state_head (
    ...
    context_box_id  TEXT, -- points to container with input card list
    output_box_id   TEXT  -- points to container with output card list
    ...
)
```
> Both `cards` and `card_boxes` use `tenant_id=project_id` for read/write isolation; `type` and `role` are read/validated from `metadata`.

## 3. Key Card Types (Authoritative Definition)

The set of standard Card types (`type` field) defined by L0 protocol is defined in `core/utp_protocol.py`:

| Type | Description | `content` example |
| :--- | :--- | :--- |
| `task.instruction` | Task instruction (Role: user) | `"Please summarize this report"` |
| `agent.thought` | Thought / intent (Role: assistant) | `"I need to list key points first..."` |
| `tool.call` | Tool call request (Role: assistant) | `{"tool_name": "search", "arguments": {"q": "..."}}` |
| `tool.result` | Tool execution result (Role: tool) | `{"status": "success", "result": [...]}` |
| `task.deliverable` | Task deliverable (Role: assistant) | `{"summary": "...", "output": "..."}` |
| `sys.profile` | Agent static config (Role: system) | `{"name": "...", "llm_config": {...}}` |
| `sys.rendered_prompt` | Rendered system prompt snapshot (Role: system) | `"You are ..."` |
| `sys.tools` | Tool definition snapshot (Role: system) | `{"tools": [...]}` |
| `meta.parent_pointer` | Parent-task pointer (Role: system) | `{"parent_agent_id": "..."}` |
| `task.result_fields` | Result field constraints (Role: system; non-core fixed type) | `FieldsSchemaContent` |

> **Role authority**: The conversation role visible to the LLM is determined by `metadata.role`; `author_id` is for audit/trace only and is not used as a conversation role.
> **Field naming**: Only top-level `metadata` is used; `meta` is no longer supported.

## 4. SDK/API Lifecycle Consistency

### 4.1 Lifecycle and Access Entry
- `ServiceContext` initializes `CardBoxClient` (`cardbox` config section) via shared connection pool at startup.
- `ServiceRuntime` `open()/close()` initializes and closes CardBox adapters on start/stop, aligned with NATS/Postgres pool lifecycle.
- `CardBoxClient`, based on `card_box_core.AsyncPostgresStorageAdapter`, maps actual read/write operations to:
  - `add_card`: insert/idempotent update Card (`ON CONFLICT`).
  - `load/save_card_box`: create/update Box.
- `append_to_box`: appends new card references in JSONB.
  - `load_card_box/get_card(s)`: fetch Card/Box by `tenant_id` + ID.

### 4.2 Query and Batch APIs (current implementation)
- Current service-layer APIs exposed are:
  - Box reads: `GET /projects/{project_id}/boxes/{box_id}`, `POST /projects/{project_id}/boxes/batch`;
  - Box-card reads: `GET /projects/{project_id}/boxes/{box_id}/cards`, `POST /projects/{project_id}/cards/batch`;
  - Single-card reads: `GET /projects/{project_id}/cards/{card_id}`.
- Batch read behavior matches the SDK: deduplicate after request order, then query; missing cards are returned in `missing_*` lists.
- Writes mainly happen in the L0 processing chain (Worker/PMO/Tool), not through a generic write-card API.

## 5. Why CardBox?

1. **Prevents split-brain**: Distributed agents can drift in state. By forcing core semantics into Card/Box and using Postgres strong consistency, the risk of cross-component state drift is reduced.
2. **Context hygiene**: The Dual-Box mechanism ensures an Agent cannot tamper with input history (input box is read-only), and can only append new output.
3. **Debugging and replay**: With immutable logs, we can perfectly reproduce any Agent Turn and even branch/ fork history for comparison testing.
4. **Language interoperability**: JSONB makes it easy for Python, Go, Node.js and other services to read/write Card consistently.
