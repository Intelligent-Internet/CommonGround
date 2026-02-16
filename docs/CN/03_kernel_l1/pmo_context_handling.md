# PMO Context Handling（Context Packing / Handover）

本文属于 L1 内核层，描述当前实现中 PMO 如何为下游 AgentTurn 组装 `context_box_id`。这也是跨 Agent 共享上下文的当前实现入口与可扩展点（可通过内部 handler + handover 配置新增拼装策略）。

## 1. 关键心智模型（P0）

- **中心化存储 ≠ 默认可见**：Cards/Boxes 在项目内统一落库（CardBox/PG），但单个 turn 的 LLM 只会看到 `context_box_id` 和该 turn 的 `output_box_id`（Worker 会把 `output_box_id` 与 `context_box_id` 结果合并后供 LLM 使用）。PMO 不会自动把项目历史“全量水合”进上下文。
- **`box_id` 不是安全边界（当前实现）**：系统层面仍未把 box 访问控制作为完整能力来强制；有读取权限的组件可通过 `box_id` 访问内容。
- **跨 Agent 共享必须显式发生**：共享上下文通过“显式传递 box_id 并重新打包 context box”实现，不存在全局隐式拼接。

## 2. 当前实现组件与边界

- `services/pmo/handover.py::HandoverPacker.pack_context`：核心拼装器。
- `services/pmo/internal_handlers/orchestration_primitives.py::pack_context_with_instruction`：当前 `delegate_async` 与 `fork_join` 的统一组装入口（固定 `instruction -> task.instruction`，默认追加 `meta.parent_pointer`）。
- `services/pmo/internal_handlers/delegate_async.py`：主要的委派路径入口，负责目标解析、继承来源 box 并调用组装器。
- `services/pmo/internal_handlers/fork_join.py`：按任务拼装 context，最终也走同一 helper。
- `services/pmo/internal_handlers/launch_principal.py`：自身未直接组装 context，而是构造 `delegate_async` 入参后复用 delegate 路径。
- `services/pmo/internal_handlers/ask_expert.py`：通过 `delegate_async` 继续复用同一上下文组装链路。

## 3. `pack_context`（handover）组装规则（当前实现）

函数签名为：

`pack_context(project_id, tool_suffix, source_agent_id, arguments, handover, conn=None)`

1) 目标 Profile 解析

- 优先取 `arguments["profile_name"]`，否则取 `handover["target_profile_config"]["profile_name"]`。
- 通过 `resource_store.find_profile_by_name` 解析为 `profile_box_id`；找不到会 `NotFoundError`。
- 解析失败/缺失会直接 `BadRequestError`。

2) `pack_arguments` 规则（若存在）

- 遍历 `handover["context_packing_config"]["pack_arguments"]` 的每个元素（只处理 `dict`）。
- 若规则的 `arg_key` 在 `arguments` 中存在：
  - `as_card_type` 默认为 `task.instruction`（可覆盖）。
  - `card_metadata` 复制后，缺省 `role: "user"`。
  - `task.result_fields`：参数值必须是 `list`，否则抛 `BadRequestError`，并作为 `FieldsSchemaContent` 存储。
  - 其他类型：`dict/list` 用 `JsonContent`，其他类型转字符串用 `TextContent`。
  - `author_id` 使用 `tool_suffix`，card id 用 `uuid6.uuid7().hex`。
- 每条规则写入一个 card，并把 card_id 追加到待打包列表。

3) `inherit_context.include_boxes_from_args`

- 读取 `handover["context_packing_config"]["inherit_context"]["include_boxes_from_args"]` 指定的参数名列表。
- 每个参数值可为单个 `box_id`（`str`/`int`）或列表。
- 每个 box 会读取其 `card_ids`，按顺序追加到新 context，并使用 set 去重（保留首次顺序）。
- box 不存在会 `NotFoundError`；参数类型异常会 `BadRequestError`。

4) `include_parent`

- 当 `inherit_context.include_parent` 为真时，追加一张 `meta.parent_pointer` 卡，`content.parent_agent_id = source_agent_id`，`metadata.role = "system"`。

5) 结果返回

- 创建 context box 后返回 `(context_box_id, target_profile_box_id, attached_card_ids)`。
- 如未形成有效 `target_profile_box_id`，会 `BadRequestError`。

## 4. 当前上下文装配调用链（真实路径）

- `launch_principal`  
  - 组装 `delegate_async` 入参（`target_strategy: new`, `target_ref: profile_name`, `instruction`）后执行 delegate 流程。
- `delegate_async`
  - 继承来源：
    - `context_box_id`（可选）；
    - 若 `target_strategy == "clone"`，还会继承克隆源当前 `output_box_id`；
  - 使用 `validate_box_ids_exist` 校验所有来源 box 合法。
  - 调用 `pack_context_with_instruction` 统一打包 context。
- `fork_join`
  - 每个 task 可携带 `context_box_id`；
  - `clone` 任务同样追加克隆源 `output_box_id`；
  - 逐 task 调 `pack_context_with_instruction`。
- `ask_expert`
  - `include_history=true` 时取当前 source agent 的 `output_box_id` 作为 `context_box_id`，再交给 `delegate_async`。

## 5. `pack_context_with_instruction` 的固定配方

`orchestration_primitives.py` 当前统一创建的 handover 配置：

- `pack_arguments`: `[{"arg_key": "instruction", "as_card_type": "task.instruction"}]`
- `inherit_context.include_boxes_from_args`: `["input_box_ids"]`（仅在有值时）
- `inherit_context.include_parent`: `True`

由此可见 `delegate_async/fork_join` 的 context 总是至少包含：
- 当前 `instruction`（`task.instruction`）
- `inherit_box_ids` 对应来源 boxes 的 card
- `meta.parent_pointer`（父指针）

## 6. 可扩展性与注意事项

- **扩展点在于 handler 与 handover 规则**：新增/修改 internal handler 可在自有流程中定义 custom handover，再调用 `pack_context` 组装。
- 当前 `launch_principal` 并未手写 handover，其 context 通过 `delegate_async` 路径复用上述统一行为。
- 输出侧仍由 Dispatcher/Worker 决定；PMO 侧不在此处写 `output_box_id`。

## 7. 常见误解（与当前实现一致）

- “Agent 的 box 就是私有”：不准确。私有性是可见性策略，不是持久化存储的物理隔离。
- “LLM 能直接读任何 box_id”：
  - PMO/Worker 侧仅会为该 turn 装配可见上下文；`output_box_id` 也仅在该 turn 生效。
  - 需通过显式 handover/inheritance 才会进入下游 LLM 的可见范围。
- “channel_id 能完全隔离历史内容”：
  - 当前更多是控制流与路由语义，Cards/Boxes 仍按 `project_id` 维度管理与访问。

## 8. 相关文档

- `docs/CN/01_getting_started/architecture_intro.md`（Box 可见性）
- `docs/CN/04_protocol_l0/state_machine.md`（Dual-Box 约束）
- `docs/CN/03_kernel_l1/agent_worker.md`（Worker 水合边界）

