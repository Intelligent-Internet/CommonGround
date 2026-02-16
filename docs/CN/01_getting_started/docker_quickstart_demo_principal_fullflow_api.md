# 快速上手

本页提供完整路径：环境准备 → 初始化 → 启动服务 → 资源准备 → 跑通 Demo。更多协议与实现细节见 L0/L1 文档。

> 拓扑概念与模式见 `architecture_intro.md`（架构总览），实现与行为以 L0 协议为准。

## 1. 环境准备

> 如果你是首次从 GitHub 拉取仓库，建议使用递归克隆以包含 `card-box-cg` 子模块：
> `git clone --recursive https://github.com/Intelligent-Internet/CommonGround.git`
> 若已克隆但缺少子模块内容，请在仓库根目录执行：
> `git submodule update --init --recursive`

### 依赖
- Python 3.12+（仓库主干基于 3.12）
- `uv`
- Postgres（建议 15+）
- NATS（2.10+，启用 JetStream）

> 全流程默认在 Docker Compose 环境中执行，涉及 `uv run` 的示例命令统一改为在 `api` 容器内运行。

### 安装依赖
```bash
uv sync
```

### 启动基础设施（可选，若本地已有可跳过）
```bash
docker-compose up -d nats postgres
```

### 默认模型与密钥策略（避免服务起步失败）

默认情况下：

- `judge` 走 `config.toml` 中 `[judge].model`，当前样例默认是 `gemini/gemini-2.5-flash`。
- `mock_search` 走 `services.tools.mock_search` 的 `MOCK_SEARCH_LLM_MODEL`/`MOCK_SEARCH_LLM_PROVIDER`，当前样例默认是 `gemini/gemini-2.5-flash` + `gemini`。

如果你没有配置 `GEMINI_API_KEY`，建议在启动前切到其他模型：

```bash
# 仅切 Judge
export CG__JUDGE__MODEL="gpt-5-mini"  # 或 moonshot/kimi-k2.5
export OPENAI_API_KEY="..."

# 仅切 mock_search
export MOCK_SEARCH_LLM_PROVIDER="openai"
export MOCK_SEARCH_LLM_MODEL="gpt-5-mini" # 或 moonshot/kimi-k2.5
export OPENAI_API_KEY="..."
export MOONSHOT_API_KEY="..."
```

也可以直接改 `config.toml`：`[judge].model`、`[tools.mock_search].llm_model`、`[tools.mock_search].llm_provider`。

> 说明：`CG__SECTION__KEY`（如 `CG__JUDGE__MODEL`）与 `config.toml` 同步，环境变量优先级更高。

### 配置文件
```bash
cp config.toml.sample config.toml
```
按需修改 `config.toml` 中的 `[nats]`、`[cardbox]`、DB DSN 与默认 project。

> 重要：`[protocol].version` 必须是 `v1r3`（且与部署协议一致）。若缺失或写错，NATS subject 会不匹配，Worker/PMO 可能收不到消息。

> 端口提示：docker-compose 默认 NATS 4222、Postgres 5432；仓库示例 DSN 可能使用 5433，请以本地 `config.toml` 为准。

## 2. 初始化数据库

> 注意：此操作会重建 `resource/state` 相关表，**同时会清空 CardBox（cards/card_boxes 等）数据**。

```bash
docker compose exec api sh -lc 'uv run -m scripts.setup.reset_db'
docker compose exec api sh -lc 'uv run -m scripts.setup.seed --project proj_demo_01'
```

## 3. 启动服务

> Demo 2/3 依赖管理 API 与资源准备，建议同时启动 API。

```bash
docker compose up -d --build
```


## 4. 资源准备（推荐）

> 💡 注意：Artifact 与 Skills API 依赖 Google Cloud Storage (GCS)。
> 若在 `config.toml` 中未配置 `[gcs]` 或缺少有效凭证，系统会优雅降级并禁用 `/skills:upload`、`/artifacts:upload` 等相关能力。
> 如果你当前只体验 Agent 编排主链路，可暂时忽略该限制。

### 方式 A：通过 API  bootstrap 项目与资源
```bash
curl -sS -X POST http://127.0.0.1:8099/projects \
  -H 'Content-Type: application/json' \
  -d '{"project_id":"proj_demo_01","title":"Demo","owner_id":"user_seed","bootstrap":true}'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/associate_search.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/principal_planner_fullflow.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/profiles \
  -F file=@examples/profiles/chat_assistant.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/tools \
  -F file=@examples/tools/web_search_tool.yaml
```

### 方式 B：脚本自动确保资源
`examples/quickstarts/demo_principal_fullflow.py` 默认会自动 bootstrap 与上传 profiles/tools；如需关闭，使用 `--no-ensure-resources`。

## 5. 跑通 Demo

> 以下示例在 Docker 环境默认执行，请在 `api` 容器内运行命令，避免本地环境差异。

### Demo 1：最小流式输出
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_simple_principal_stream \
  --project proj_demo_01 \
  --channel public \
  --agent demo_stream_01 \
  --question "Hello"'
```

### Demo 2：全流程（fork_join → submit_result）
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_principal_fullflow \
  --project proj_demo_01 \
  --channel public \
  --profile-name Principal_Planner_FullFlow \
  "help me to do a research on k8s"'
```

### Demo 3：`fork_join` Map-Reduce（Word Count）
先启动确定性工具服务：
```bash
docker compose exec api sh -lc 'uv run -m services.tools.word_count'
```

再运行在线 demo（输入文本文件）：
```bash
docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_fork_join_word_count \
  --project proj_demo_01 \
  --channel public \
  --text-file /path/to/input.txt'
```

### Demo 4：UI Action 聊天（新入口）
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"ui_user_demo",
    "profile_name":"UI_Actor_Profile",
    "worker_target":"ui_worker",
    "tags":["ui"],
    "display_name":"UI Session",
    "owner_agent_id":"user_demo",
    "metadata":{"is_ui_agent":true},
    "init_state":true,
    "channel_id":"public"
  }'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_demo_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"chat_agent_demo",
    "profile_name":"Chat_Assistant",
    "worker_target":"worker_generic",
    "tags":["partner"],
    "display_name":"Chat Agent",
    "owner_agent_id":"user_demo",
    "init_state":true,
    "channel_id":"public"
  }'

docker compose exec api sh -lc 'uv run -m examples.quickstarts.demo_ui_action \
  --project proj_demo_01 \
  --channel public \
  --ui-agent-id ui_user_demo \
  --chat-agent-id chat_agent_demo'
```

## 6. 调试与观测（可选）

### 6.1 订阅流式输出
```bash
nats sub "cg.v1r3.proj_demo_01.public.str.agent.*.chunk"
```

### 6.2 Observer（终端 UI）
```bash
docker compose exec api sh -lc 'uv run -m scripts.admin.inspect_turn \
  --project proj_demo_01 \
  --agent-id demo_stream_01'
```

> 说明：当前仓库未提供可直接运行的 `services.observer` 入口；如需查看实时流，保留 6.1 的 `nats sub`。`scripts.admin.inspect_turn` 适合回放/追踪某个 agent 的最新 turn 与 step。

## 7. 进阶：上层编排接入 `fork_join`

当你需要在 Principal 中实现“任务拆解 → 并行执行 → 汇总回写”，请参考：`docs/CN/03_kernel_l1/pmo_orchestration.md`。

## 8. 环境变量附录

- `CG_CONFIG_TOML`：指定配置文件路径（默认 `./config.toml`）。
- `CG__SECTION__KEY`：配置覆盖（支持 JSON 值，优先级最高）。例如 `CG__NATS__SERVERS='[\"nats://nats:4222\"]'`。
- `PG_DSN`：覆盖 `[cardbox].postgres_dsn`（兼容脚本/工具服务）。
- `NATS_SERVERS`：覆盖 `[nats].servers`（逗号分隔）。
- `API_URL` / `PROJECT_ID`：seed 脚本便捷项。
- `CG_SENDER`：NATS Header `CG-Sender` 默认值覆盖。
- `CG__PROTOCOL__VERSION`：覆盖 `[protocol].version`（推荐替代 `config.toml` 设置）。
- `GEMINI_API_KEY`：默认 Gemini 流程的 API Key（或等效用途的 provider key）。
- `CG__JUDGE__MODEL`：覆盖 `config.toml` 的 `[judge].model`（示例：`gpt-5-mini`、`moonshot/kimi-k2.5`）。
- `MOCK_SEARCH_LLM_MODEL`：覆盖 `mock_search` 的 LLM 模型（示例：`gpt-5-mini`、`moonshot/kimi-k2.5`）。
- `MOCK_SEARCH_LLM_PROVIDER`：覆盖 `mock_search` 的 provider（示例：`openai`、`moonshot`）。
- `OPENAI_API_KEY` / `MOONSHOT_API_KEY`：对应 provider 的 API Key。

> 进阶：如需在 Profile 里直接定义 provider 与模型路由，请参考 `docs/CN/02_building_agents/defining_profiles.md` 中“切换大模型供应商（LiteLLM 支持）”章节。
- `MOCK_SEARCH_DEBUG`：开启 `services.tools.mock_search` 的调试输出（`1/true/yes/on`）。
- OpenAPI(Jina) 工具密钥：默认通过 `JINA_API_KEY` 注入（或在 `options.jina.auth_env` 指定环境变量名），**不要写入 cards/DB/日志**。

> 说明：默认优先读取 `config.toml`；环境变量用于覆盖配置或为脚本/工具服务提供运行时密钥。
