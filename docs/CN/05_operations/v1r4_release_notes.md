# CommonGround V1R4 发布说明

日期：2026-03-12
状态：`v1` 系列最终稳定版

## 概要

`V1R4` 是 `CommonGround v1` 的最终稳定发布版本。

相对于发布前的 `main` 基线，这个分支就是完整的 `V1R4` release candidate：它包含了 `v1r4` 协议切换、context/authority hard cut、spawn/runtime 收口，以及发布前最后必须补掉的稳定性修复。

这次发布完成了迁移后遗留的主要止血和收口工作，但没有继续把当前中央化的 worker/tool 管道打磨成长期终态，以避免给后续 `v2` 带来返工负担。

这次发布的目标是：

- 让当前 `v1` runtime 可以稳定发布
- 保留足够干净的 kernel contract 供 `v2` 继承
- 不把现有中央 worker/tool 管道继续发展成新的复杂度中心

## 相对 `main` 的变化

### 1. 协议与 context authority hard cut

- 协议版本正式切到 `v1r4`，发布目标使用 `cg.v1r4.*` subjects。
- 控制面身份现在明确以 `CGContext` / headers 为权威来源，不再把 payload 当作权威来源。
- 迁移文档、quickstart、tools 和 first-party examples 已同步到 `v1r4` 合同。

### 2. Runtime 与 topology 收口

- spawn-facing dispatch 不再接受调用方自带的 child turn topology；真实 child turn identity 由 L0 分配。
- `BatchManager` 和 PMO orchestration 现在以真实 `agent_turn_id` 跟踪 child work，不再把旧 inbox/epoch archaeology 当作主要运行时真相。
- child termination、下游跟踪与 wakeup 继续围绕 L0 分配的真实 turn identity 工作。

### 3. 发布前最后的稳定性修复

- `resume/tool_result` 已改为最小单赢家 apply 路径，不再允许明显重复 append / 重复 apply。
- existing-agent 的 `provision_agent/ensure` 不再允许改写 `owner_agent_id`、`worker_target`、`tags` 或 `profile_box_id`。
- validation-failure 的上下文恢复不再信任 payload lineage 字段，只信 ingress context 和 `tool.call` metadata。
- watchdog / stop 完成路径现在会用提交后的 `(agent_turn_id, turn_epoch)` 发 `evt.agent.*`，不再带旧 epoch。
- skills runtime 现在会解析 owner-scoped 的内部 `session_key`；对外暴露的 `session_id` 只保留为调用者可见 alias。
- `tool_result` ingress 增加了轻量 source/card 校验，包括结果卡指针、`tool_call_id`、以及 source/author/function 一致性检查。

### 4. 发布文档清理

- tool callback 示例已改成当前真实的 `CGContext` / ingress helper 形态。
- skills 文档现在把 `session_id` 明确描述为 alias，而不是底层 sandbox identity。
- PMO context 文档现在明确要求 box inheritance 必须预授权，而不是只做 exists 校验。
- 本 release note 取代了之前的 `v1r4` convergence playbook 设计稿，并作为相对 `main` 的正式发布摘要。

## 明确 defer 到 V2 的内容

下面这些主题仍然重要，但不再是 `V1R4` 的发布前置条件：

- 中央 `tool_result / resume / UTP` 管道的完整重设计
- 完整严格的 source-authenticity / producer-binding 框架
- 完整 capability-style ACL
- 面向边缘 runtime 与 agent-owned execution 的正式 external runtime contract

这些主题继续由 `#417` 所代表的 `v2` 路线承接。

## 验证

`V1R4` 收口已通过以下验证：

- worker、PMO、skills、dispatcher、L0 report 以及本轮最终 blocker 修复的定向回归：最后一轮 blocker patch 为 `47 passed`，此前的大范围 `V1R4` 定向回归也已完成
- 完整 `tests/integration`：`7 passed, 1 skipped`
- 隔离本地栈上的 quickstart 验证（`NATS 4223`、`API 8099`）：
  - `demo_simple_principal_stream`
  - `demo_principal_fullflow`
  - `demo_principal_fullflow_api`
  - `demo_fork_join_word_count`
  - `demo_ui_action --mode chat`
  - `demo_ui_action --mode busy`
- 本轮改动 Python 模块的编译检查

## 升级提示

- `V1R4` 应作为 `v1` 的最终稳定目标版本看待。
- 现有集成方应停止依赖 payload control fields 和已过时的 callback helper 签名。
- 任何把 `session_id` 视为跨 agent 全局可复用身份的使用方式，都应调整为“仅把它当 alias”。
- 如果本地 `4222` 已被占用，请把 `[nats].servers` 或 `NATS_SERVERS` 指向其他本地端口；本次发版验证实际使用的是 `4223`。
