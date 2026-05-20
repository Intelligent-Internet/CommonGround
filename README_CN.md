# CommonGround Kernel

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-supported-336791.svg)](https://www.postgresql.org/)
[![Release: v3r1--preview](https://img.shields.io/badge/release-v3r1--preview-orange.svg)](docs/zh/release-notes.md)
[![API: v3r1](https://img.shields.io/badge/API-v3r1-555555.svg)](docs/zh/reference/http.md)
[![Discord](https://img.shields.io/badge/Discord-Join%20Community-7289DA.svg)](https://discord.com/invite/intelligentinternet)

[中文](README_CN.md) | [English](README.md)

![CommonGround Kernel banner](docs/assets/commonground-kernel-banner.png)

> Agent 很少能长期独自完成真实工作。真实工作会跨越人、工具、服务、runtime 与其他 Agent。

**CommonGround Kernel v3r1 preview** 是面向真实人类与 Agent 协作、多 Agent 工作的开源公共底层。它让 Agent 工作被持久保存、公开记录，并能被下一个接手的人、Agent、工具或外部 runtime 继续读取和使用。

CommonGround 把 Agent 工作变成持久的共同语境（durable common ground）。它不只是持久日志。它是一个保留下来的公共协作底座，让人类与 Agent 能够在交接、暂停、会话结束或 runtime 更换之后，重新建立共同基础。它保存后续参与者继续工作所需的共享任务状态、交接事实、边界输入输出、证据与恢复语境。

它也是一个小型的**宪法化账本内核（constitutional ledger kernel）**：它定义独立参与者协作所需的最小公共事实、工作边界、语义归属与因果关系，而不要求所有参与者被吸收到同一个中央 runtime 里。

**边缘是独立 Agent。Kernel 保存持久公共工作记录。**

---

## CommonGround 是什么？

CommonGround 是 Agent runtime、记忆系统与编排框架之下的共享工作底座。

这里的 **common ground（共同语境）** 指的是持续协作所需的公共基础：共享的任务状态、共享的交接事实、可供后续恢复的理由与证据，以及让工作在原始会话消失之后仍可检查、可恢复的边界输入输出。

它保存工作的公共记录：

- 请求了什么；
- 交接了什么；
- 返回了什么；
- 交付了什么；
- 后续工作可以依赖什么。

它不试图拥有每个工作流、runtime、记忆系统、协作界面或业务决策。相反，CommonGround 只保存独立人类、Agent、工具、服务与外部 runtime 协作所需的最小持久事实。

在 v3r1 preview 中，这个基础以 **Ledger Kernel** 形式暴露：一个围绕 Agent identity（Agent 身份）、Turn lifecycle（Turn 生命周期）、Turn-owned public semantics（归属于 Turn 的公共语义）、工作记录、claim fencing、causal lineage（因果谱系）与 pull-first inspection（拉取优先的检查方式）的小型 kernel。

## 为什么需要它

AI Agent 正在从“回答问题”走向“参与真实工作”。

但真实工作很少能装进一个 prompt。它会跨越人、Agent、工具、服务与时间。一个 Agent 研究市场，另一个 Agent 起草策略 memo，人类过后审查发生了什么，coding agent 接手其中一部分，未来的 Agent 又试图复用这次结果。

当这些工作只留在聊天窗口、临时会话、工具日志与 runtime 私有上下文里，下一个参与者就只能从碎片重新开始。

更深一层的问题，不只是 Agent 能不能给出一个好答案，而是这项工作能不能进入共享记忆、可复用经验、review 与下一轮工作的链条。

CommonGround 为这些工作提供持久共享记录，让后续的人类和 Agent 可以检查、恢复、审计、复用并继续构建。

## 第一性原理

CommonGround 遵循一个第一性原理：

> Assume nothing beyond what constraints demand.

这个原则让 kernel 保持小。CommonGround 不应把当前产品形态、runtime 设计、拓扑偏好、记忆策略或编排风格冻结成永久的系统本体。

v3r1 preview kernel 围绕几个宪法级承诺组织：

- **Agent 是稳定逻辑执行主体**：它可以由 LLM、人类、服务、脚本、外部 runtime、组织流程或混合系统承载。
- **Turn 是最小持久工作边界（durable work boundary）**：它是一项工作、一次委托、一次交互或一个恢复闭环，而不只是聊天回复或临时会话。
- **Turn-owned semantics 保存公共工作事实**：输入、选定观察、过程记录、交接、交付物、终止原因与 artifact 引用都归属于拥有它们的 Turn。
- **因果关系必须足以恢复工作**：parent/child 工作、完成、观察与吸收应该在会话、runtime 或通知消失后仍然可检查。
- **已保存历史不会自动变成私有记忆或业务效果**：上层可以解释、总结、吸收或复用记录，但 kernel 默认不替上层做这些判断。

## 从早期预览到 v3r1 Preview

早期 CommonGround preview 更强调编排、类似 swarm 的执行方式，以及多 Agent 协作。

v3r1 preview 保留这个方向，但移动了叙事重心。

编排很重要，因为执行现场才是协作真正发生的地方。每当工作被委托、恢复、完成或跨 Agent 交接，就会产生一些应该跨越会话与编排器（orchestrator）生命周期继续存在的边界。

基础变得更精确，不是因为愿景变小了，而是因为愿景更大了。CommonGround 不需要拥有每个工作流、runtime 或记忆系统；它需要保存让独立参与者能够协作的公共工作事实，而不是把所有人都压进同一个中央系统。

Commons 是目的地。Kernel 是第一块持久地基（durable ground）。

## Kernel 提供什么

CommonGround Kernel 当前提供：

- **Agent identity**：稳定逻辑执行主体，可由 LLM、人类、服务、脚本、外部 runtime、组织流程或混合系统承载。
- **Turn boundaries**：面向任务、委托、交互与恢复闭环的持久工作边界。
- **Turn-owned public semantics**：归属于 Turn 的输入、选定观察、交接、过程记录、最终交付物、终止原因与 artifact 引用。
- **Claim fencing and lifecycle**：claim、heartbeat、renew、suspend、resume、stop、finish 与 lease-expiry handling。
- **Causal lineage**：parent/child Turn 关系、child completion、parent observation 与 explicit absorption。
- **Ledger and feed**：服务于 pull-first recovery、inspection、audit 与 projection 的持久公共事实。
- **本地开发者接口**：CLI、HTTP 服务、Admin Service admission flow、BYOA 示例与 NanoBot 参考样例。

## CommonGround 不是什么

CommonGround Kernel 是刻意聚焦的。
它不是完整的 Agent 记忆产品，不是 Agent 私有记忆系统，不是 chain-of-thought 记录器，不是 Agent runtime，不是中央调度器，不是自动编排框架，不是 Slack bot 发布包，也不是你现有 Agent 框架的替代品。

记忆系统、编排框架、review 界面、知识蒸馏流水线、共享工作区、routine 与 playbook 都可以构建在 CommonGround records 之上。它们不是 kernel 本身。

CommonGround 是 **memory-ready，而不是 memory-complete**：它保存可用于记忆和复用的公共工作事实，但不把上层记忆产品一次性做完。

## 从 v1 升级的 Breaking Change

CommonGround Kernel v3r1 与早期公开的 v1 preview 不兼容。包结构、runtime 假设、本地启动路径与 API surface 都已经变化。已有 v1 集成应把 v3r1 视为新的 preview 线，而不是原地升级。

历史 v1 源码会保留在 [`legacy/v1`](https://github.com/Intelligent-Internet/CommonGround/tree/legacy/v1) branch。已有 v1 tags，包括 `v1r4` 与 `v1r4-hotfix`，保持不变。

## 当前发布范围

当前状态：**Initial Open Source Preview - v3r1**

当前服务/API 实现版本：**v3r1**

v3r1 preview 聚焦 Ledger Kernel 与本地优先的开发者路径。

当前预览版支持：

- 基于 PostgreSQL 的本地开发与测试执行。
- 基于 `uv` 的安装与运行。
- 通过 `cg local run` 启动单端口本地开发环境。
- 通过 `cg service run` 与 `cg admission run` 启动分离的服务入口。
- 以 CLI 为主的工作流：setup、profiles、dispatch、Turn inspection、project observation、reports、worker lifecycle 与 admission。
- BYOA work-memory reporting。
- BYOA conversation-worker examples。
- NanoBot 集成参考样例，作为可选的高级参考。

`v3r1` 是服务与客户端代码当前使用的预览 API 前缀。它不是长期兼容性承诺。

## 从 PyPI 安装

当公开包已经发布后，可直接安装：

```bash
uv tool install commonground-kernel
cg --version
```

如果要运行 `cg local run` 这类本地 service 命令，请安装 `server` extra：

```bash
uv tool install 'commonground-kernel[server]'
```

## 快速开始

第一个循环刻意保持很小：

1. 在本地运行 kernel；
2. 提交一条公共工作报告；
3. 检查保留下来的工作记录。

### 前置依赖

- Python `>=3.13`
- `uv`
- 一个可写入的本地 PostgreSQL database

### 1. 安装

```bash
uv tool install 'commonground-kernel[server]'
```

### 2. 准备数据库

创建一个 PostgreSQL database，然后把 `PG_DSN` 指向它：

```bash
export PG_DSN=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

### 3. 初始化本地项目

```bash
cg setup project seed --default-local
cg setup project status --default-local

cg setup project client-config --default-local \
  --base-url http://127.0.0.1:8000 \
  --admin-base-url http://127.0.0.1:8000
```

这会准备默认本地 `cg-demo` project，并写入本地 CLI 配置。

### 4. 运行本地服务组合

在一个长期运行的终端中：

```bash
cg local run --project-id cg-demo --host 127.0.0.1 --port 8000
```

它会提供：

- CommonGround Service API 位于 `/v3r1`
- Admin Service admission API 位于 `/admin/v1`

检查存活状态：

```bash
curl http://127.0.0.1:8000/healthz
```

### 5. 提交公共工作报告

在另一个终端中：

```bash
cat > report.json <<'EOF'
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "local-agent-report-001",
  "summary": "Local work completed and reported.",
  "records": [
    {
      "role": "summary",
      "payload": {
        "summary": "Completed the local task and retained public evidence."
      }
    }
  ]
}
EOF

cg profile ensure-agent \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --requested-agent-id local-agent \
  --profile-kind byoa.work_memory_reporter.v1 \
  --runtime-kind local.cli.v1 \
  --display-name "Local Agent"

cg report work-memory \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --agent-id local-agent \
  --manifest-file report.json
```

成功后，CLI 会打印一个 JSON envelope（封装响应）。复制 `result.turn.turn_id`。

### 6. 检查保留下来的 Turn

```bash
cg turn context \
  --profile cg-demo/local-agent \
  --project-id cg-demo \
  --turn-id <turn_id>
```

你应该能看到一个 closed `turn.work_memory_report.v1` Turn，包含提交的 manifest、公共工作记录与 final payload。

完整指南见 [开源快速开始指南](docs/zh/guides/open-source-quickstart.md)。
如果你要使用源码 checkout 和开发工作流，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## Agent 集成路径

选择与你的 Agent 和 CommonGround 关系最匹配、也最小的集成路径。

| 目标 | 接收 CommonGround 分配的工作 | 上报公共工作记录 | 需要 NanoBot | 典型入口 |
| --- | --- | --- | --- | --- |
| 本地 Agent 已完成工作，只需要发布选定公共记录 | 否 | 是 | 否 | `cg profile ensure-agent` + `cg report work-memory` |
| 外部 runtime 需要接收并完成 `turn.conversation.v1` 工作 | 是 | 可选 | 否 | `cg admission invite create` + `cg agent join` |
| NanoBot gateway 或 companion 需要管理 claim、context、child dispatch、resume 与 final absorption | 是 | 可选 | 当前需要 | `examples/nanobot/` |

先阅读 [Agent Integration Scenarios](docs/zh/guides/agent-integration-scenarios.md)，再选择：

- [BYOA Work-Memory Reporter](docs/zh/guides/byoa-work-memory-reporter.md)
- [BYOA Conversation Worker](docs/zh/guides/byoa-conversation-worker.md)
- `examples/nanobot/` 下的 NanoBot 高级 runtime 参考样例

NanoBot 适合作为参考集成，但前两个 BYOA 路径不依赖 NanoBot。

## 架构概览

```mermaid
flowchart LR
    subgraph External["外部人类、Agent、工具与 runtime"]
        CLI["cg CLI"]
        BYOA["BYOA agents"]
        Worker["外部 workers"]
        NanoBot["NanoBot 参考样例"]
    end

    subgraph Surface["CommonGround 服务接口"]
        HTTP["HTTP API /v3r1"]
        Admin["Admin Service /admin/v1"]
        SDK["SDK helpers / adapters"]
    end

    subgraph Kernel["CommonGround Kernel"]
        Agents["Agent identity"]
        Turns["Turn lifecycle"]
        Semantics["Turn-owned semantics"]
        Claims["Claim fencing"]
        Lineage["Causal lineage"]
        Feed["Ledger and feed"]
    end

    subgraph Truth["持久事实层"]
        PG[("PostgreSQL")]
        CardBox[("CardBox schema")]
    end

    CLI --> HTTP
    CLI --> Admin
    BYOA --> HTTP
    Worker --> HTTP
    NanoBot --> HTTP

    HTTP --> SDK
    Admin --> SDK
    SDK --> Kernel
    Kernel --> PG
    Kernel --> CardBox
```

推送通知、dashboard、summary 与 projection 可以让工作更容易被看到。它们不替代正确性基线：后续读者必须能回到持久事实。

## 仓库结构

```text
CommonGround/          Kernel、contracts、infra adapters、SDK helpers、服务接口与 CLI
Integrations/nanobot/  可选的高级 runtime / companion / provisioning 集成
examples/              BYOA、work-memory reporter、skill 与 NanoBot 示例
docs/                  当前架构、指南、参考文档与设计材料
tests/                 回归测试与可执行契约
CG-Cardbox/            PostgreSQL schema 重置路径使用的 CardBox submodule
```

## 文档

从这里开始：

- [Docs index](docs/index.md)
- [English docs](docs/en/index.md)
- [中文文档](docs/zh/index.md)

核心阅读：

1. [什么是 CommonGround Kernel](docs/zh/introduction/what-is-commonground.md)
2. [开源快速开始指南](docs/zh/guides/open-source-quickstart.md)
3. [Agent 集成场景](docs/zh/guides/agent-integration-scenarios.md)
4. [发布说明](docs/zh/release-notes.md)
5. [CLI Reference](docs/zh/reference/cli.md)
6. [HTTP Reference](docs/zh/reference/http.md)
7. [环境变量](docs/zh/reference/environment.md)

设计参考：

1. [宪法](docs/zh/01-constitution.md)
2. [三平面模型](docs/zh/02-three-plane-model.md)
3. [设计审查原则](docs/zh/03-design-review-principles.md)

## 开发与测试

如果你要使用源码 checkout、开发环境以及 release/test workflows，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 安全与凭据

CommonGround Agent credentials 是 bearer secrets。

不要把真实 token、provider API keys、private DSNs、local token files、workstation paths 或 generated credential output 粘贴进 prompt、issue、PR、log、doc、manifest 或 test fixture。

公开材料中请使用 placeholder：

```text
<agent_credential_token>
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

不要在 public issue 中报告疑似漏洞。私密报告路径见 [SECURITY.md](SECURITY.md)。

## 下一步

持久工作记录不是终点。它们是原材料。

CommonGround 记录可以成为这些上层能力的基础：

- 记忆与审查；
- 知识蒸馏；
- 搜索与 dossier 界面；
- 共享工作区；
- routines 与 playbooks；
- 编排层；
- 组织学习。

这些更高层应该建立在稳定的公共工作事实之上，而不是脆弱的会话碎片之上。

## 贡献

Agent 协作的未来应该可共享、可检查、可恢复，并且在公开环境中构建。

欢迎社区贡献。

提交 PR 前：

- 阅读 [CONTRIBUTING.md](CONTRIBUTING.md)；
- 保持变更范围清晰；
- 当命令、行为、公开契约或环境变量改变时同步更新文档；
- 避免提交本地路径、生成的 runtime 输出、真实凭据、private DSNs 或个人数据库名称；
- 将 kernel、lifecycle、semantic-record、service 与 truth-schema 改动视为高风险设计工作。

## 社区与支持

- Discord: [Join our Discord community](https://discord.com/invite/intelligentinternet)
- Issues: [Open a GitHub issue](https://github.com/Intelligent-Internet/CommonGround/issues)
- Security: 报告安全问题前请阅读 [SECURITY.md](SECURITY.md)

## 许可证

CommonGround Kernel 使用 [Apache License 2.0](LICENSE)。
