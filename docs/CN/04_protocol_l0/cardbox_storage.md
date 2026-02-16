# CardBox：不可变事实存储

CardBox 是 L0 协议层的事实与上下文持久化基础：用于存储语义卡片（`cards`）与卡片容器（`boxes`）的引用序列。  
它不承载 `state.*`/`resource.*` 表的完整状态（这些仍由各自 Store 持久化），而是作为 L0 语义真源的统一存储入口。

## 1. 核心设计哲学

### 1.1 万物皆卡 (Everything is a Card)
系统中所有数据单元——无论是用户指令、LLM 回复、工具调用结果，还是静态的 Project 配置、User Profile——都统一封装为 **Card**。
- **原子性**：Card 是最小的信息单元。
- **不可变性（语义层）**：标准做法是“变更则创建新 Card 并使用新 `card_id`”。  
  - 当前实现的 `add_card` 为同一 `card_id` 使用 `ON CONFLICT DO UPDATE`（幂等重放修复路径），因此不应将“重复写入同一 ID”作为业务层更新模型。
- **唯一标识**：每个 Card 拥有全局唯一的 `card_id`（通常为 UUID）。

### 1.2 CardStore：单一事实来源
`CardStore` 是所有语义 `Card` 的物理存储仓库（对应 Postgres 中的 `cards` 表）。
- **去中心化引用**：上层逻辑（如 `context_box`/`output_box`、resource snapshot）只持有 `card_id` 引用。
- **审计与追溯**：`metadata` 与 `created_at` 配合历史回放可支持可追溯性；同时 `deleted_at` 支持软删除语义（非完全 purge）。

### 1.3 CardBox：线性上下文容器
`CardBox` 是 `card_id` 的有序集合，定义了一个线性的上下文环境（Context）。
- **用途**：一个 CardBox 实例通常代表一段对话历史、一个任务的上下文快照、或者一组输出结果。
- **引用而非复制**：Box 内只存储 ID 列表，多个 Box 可以引用同一个 Card（例如多轮对话中引用同一份 Profile）。
- **Dual-Box 模式**：Worker 使用 `context_box_id`（只读输入）和 `output_box_id`（只写输出）来隔离副作用。
- **可见性边界（重要）**：Boxes 在项目内中心化存储，但单个 Agent Turn 的 LLM 只会看到被装配进 `context_box_id` 的输入快照，以及本次 Turn 的 `output_box_id` 追加内容；跨 Agent 共享需要显式传递 `box_id` 并进行上下文装配/继承（例如由 PMO context packing 或工具完成）。
  - 参考：`03_kernel_l1/pmo_orchestration.md`
- **Box 可变性与快照语义**：Box 是可变容器（append/update card_ids）；“快照”是协议语义，指当前 `context_box_id` / `output_box_id` 指针所指向的集合。
- **Box 可替换（压缩/重写）**：当需要上下文压缩或重写时，上层可以生成新的 box 并替换 `context_box_id` / `output_box_id` 指针，协议不要求 box_id 永远不变。

## 2. 数据模型 (Schema)

CardBox 的核心表结构设计简洁，利用 JSONB 存储多态内容。

### 2.1 cards 表
```sql
CREATE TABLE cards (
    card_id     TEXT PRIMARY KEY,       -- 全局唯一 ID
    tenant_id   TEXT NOT NULL,         -- 项目隔离键（对应 project_id）
    content     JSONB,                 -- 核心内容载荷
    tool_calls  JSONB,
    tool_call_id TEXT,
    ttl_seconds INTEGER,
    expires_at  TIMESTAMPTZ,
    deleted_at  TIMESTAMPTZ,
    metadata    JSONB,                 -- 元数据（含 type/role/step_id/trace_id 等）
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 语义类型位于 metadata->>'type'，不是独立字段
```

### 2.2 boxes 表（当前实现）
当前实现使用 `card-box-cg` 的 `card_boxes` 物理表存放 Box（与 `cards` 分表存储，主键为 `box_id`）。
旧“字段存储/动态构建”的说法已过时，仅作为历史概念参考。
在 L1 状态机中，`state` 表会显式存储 Box 的 ID 指针：
```sql
-- 示例：Agent 运行状态
state.agent_state_head (
    ...
    context_box_id  TEXT, -- 指向包含输入卡片列表的容器
    output_box_id   TEXT  -- 指向包含输出卡片列表的容器
    ...
)
```
> `cards` / `card_boxes` 均采用 `tenant_id=project_id` 读写隔离；`type` 与 `role` 在 `metadata` 中读取与约束。

## 3. 关键卡片类型（权威定义）

L0 协议定义的一组标准 Card 类型（`type` 字段）以 `core/utp_protocol.py` 为准：

| 类型 | 描述 | content 结构示例 |
| :--- | :--- | :--- |
| `task.instruction` | 任务指令（Role: user） | `"请总结这份报告"` |
| `agent.thought` | 思考与意图（Role: assistant） | `"我需要先列出要点..."` |
| `tool.call` | 工具调用请求（Role: assistant） | `{"tool_name": "search", "arguments": {"q": "..."}}` |
| `tool.result` | 工具执行结果（Role: tool） | `{"status": "success", "result": [...]}` |
| `task.deliverable` | 任务交付物（Role: assistant） | `{"summary": "...", "output": "..."}` |
| `sys.profile` | Agent 静态配置（Role: system） | `{"name": "...", "llm_config": {...}}` |
| `sys.rendered_prompt` | 渲染后的系统提示词快照（Role: system） | `"You are ..."` |
| `sys.tools` | 工具定义快照（Role: system） | `{"tools": [...]}` |
| `meta.parent_pointer` | 父级任务指针（Role: system） | `{"parent_agent_id": "..."}` |
| `task.result_fields` | 结果字段约束（Role: system；非固定核心类型） | `FieldsSchemaContent` |

> **Role 权威来源**：LLM 看到的对话角色由 `metadata.role` 决定；`author_id` 仅用于审计/追踪，不作为对话角色。  
> **字段命名**：顶层仅使用 `metadata`，不再支持 `meta`。

## 4. SDK/API 生命周期一致性

### 4.1 生命周期与访问入口
- `ServiceContext` 会在启动时通过共享连接池初始化 `CardBoxClient`（`cardbox` 配置段）。
- `ServiceRuntime` 的 `open()/close()` 分别在启动时初始化并关闭 CardBox 适配器，和 NATS/数据库池同生命周期。
- `CardBoxClient` 基于 `card_box_core.AsyncPostgresStorageAdapter`，实际写/读操作映射到：
  - `add_card`：插入/幂等更新 Card（`ON CONFLICT`）；
  - `load/save_card_box`：新建/更新 Box；
- `append_to_box`：按 JSONB 追加新卡引用；
  - `load_card_box/get_card(s)`：按 `tenant_id` + ID 拉取 Card/Box。

### 4.2 查询与批量 API（当前实现）
- 当前服务层 API 对外仅提供：
  - 盒子读取：`GET /projects/{project_id}/boxes/{box_id}`、`POST /projects/{project_id}/boxes/batch`；
  - 盒子内卡片读取：`GET /projects/{project_id}/boxes/{box_id}/cards`、`POST /projects/{project_id}/cards/batch`；
  - 单卡读取：`GET /projects/{project_id}/cards/{card_id}`。
- 批量读的行为与 SDK 一致：按请求顺序去重后查询，未命中卡片返回 `missing_*` 列表。
- 写入主要发生在 L0 处理链路（Worker/PMO/Tool）中，而不是通用写卡 API。

## 5. 为什么选择 CardBox？

1.  **解决脑裂 (Split-Brain)**：分布式 Agent 容易出现状态不一致。通过强制核心语义落盘为 Card/Box，并配合 Postgres 的强一致性，降低跨组件状态漂移风险。
2.  **上下文无污染**：Dual-Box 机制确保 Agent 无法篡改输入历史（Input Box 是只读快照），只能追加新的输出。
3.  **调试与回放**：基于不可变日志，我们可以完美重现任何一次 Agent Turn 的现场，甚至可以分叉（Fork）历史进行对比测试。
4.  **跨语言通用性**：JSONB 结构使得 Python、Go、Node.js 等不同语言的服务都能轻松读写 Card。
