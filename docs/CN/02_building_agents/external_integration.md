# 外部集成与 I/O 边界（对齐当前实现）

该页用于对接 CommonGround 外部系统（脚本/服务/工具）。以下约定以当前仓库代码为准：  
- 协议版本默认 `v1r3` (`core/config_defaults.py`)  
- NATS/管理面均为 **可直接接入**，无额外网关层

> 本页主要描述“如何接入”，不是“如何定义工具协议”，工具开发流程另见 `creating_tools.md`。

## 1. 依赖与安装（当前）

`CommonGround` 仍是单体源码仓，但对外暴露了 `core/infra` 这两个 Python 包入口：

- `core`: UTP 数据模型、subject 规范、错误与头定义  
- `infra`: NATS、CardBox、工具执行与基础服务封装  

最小本地开发安装建议（与仓库内 `pyproject.toml` 一致）：

```bash
uv init my-consumer
cd my-consumer

uv add --editable ../CommonGround
uv add card-box-core
```

如果只做“纯脚本调用”而不是服务运行，仍可在脚本里直接 `from infra...`、`from core...` 使用。

## 2. 配置加载（与服务一致）

所有外部入口建议统一使用：

- `load_app_config()`（`core.app_config`）
- `config_to_dict(load_app_config())`（`core.app_config`）
- `service_cfg = cfg.get("nats")` / `service_cfg = cfg.get("cardbox")`

配置优先级顺序是：`config.toml` → 环境变量覆盖。  
环境变量覆盖兼容两套形式：

- 新式：`CG__SECTION__KEY=...`（支持 JSON 值）
  - 例如 `CG__NATS__SERVERS='["nats://localhost:4222"]`
- 兼容旧式：`PG_DSN`、`NATS_SERVERS`

示例：

```bash
export CG_CONFIG_TOML=/path/to/config.toml
export CG__NATS__SERVERS='["nats://nats:4222"]'
export CG__NATS__TLS_ENABLED=false
export PG_DSN="postgresql://postgres:postgres@postgres:5432/cardbox"
```

默认配置见 `core/config_defaults.py`，关键字段：

- `protocol.version`：默认 `v1r3`
- `nats.servers`、`nats.tls_enabled`、`nats.cert_dir`
- `cardbox.postgres_dsn`、`cardbox.tenant_id`、`cardbox.postgres_min_size`、`cardbox.postgres_max_size`
- `api.listen_host`、`api.port`

## 3. 外部 I/O 边界一览

## 3.1 管理 API（HTTP）

管理 API 在 `services/api/main.py` 实现。启动方式：

```bash
uv run -m services.api
```

默认监听 `127.0.0.1:8099`（本地开发）。在 Docker 部署场景下，可在 `config.docker.toml` 或配置文件中覆盖 `api.listen_host`。

### CORS 策略

默认情况下，API 的 CORS 被限制为仅本地来源（`localhost/127.0.0.1` 常用 UI 端口），以降低浏览器跨站误触发导致的安全风险。

若前端与 API 在其他远程域名/端口运行，需要手动修改 `services/api/main.py`，在 `CORSMiddleware` 的 `allow_origins` 中补充精确允许来源。  
生产环境请务必避免使用通配符。

外部系统主要可对接的入口：

- 健康检查：`GET /health`
- 项目：`POST /projects`、`GET /projects/{project_id}`、`PATCH`、`DELETE`
- Profile：`POST /projects/{project_id}/profiles`、`GET /projects/{project_id}/profiles/{name}`
- Tool：`POST /projects/{project_id}/tools`、`GET /projects/{project_id}/tools/{tool_name}`
- Skill：`POST /projects/{project_id}/skills:upload`、`GET /projects/{project_id}/skills`
- Agent：`GET /projects/{project_id}/agents`、`POST /projects/{project_id}/agents`
- Card/Box 查询：`GET /projects/{project_id}/cards/{card_id}`、`/cards/batch`、`/boxes/{box_id}`、`/boxes/batch`
- 流式/状态订阅结果读取：通常通过 NATS `evt`/`str`，不是额外 HTTP Push

可选测试/调试入口：

- `POST /projects/{project_id}/god/pmo/{tool_name}:call`（环境变量开关 `CG_ENABLE_GOD_API`）

## 3.2 NATS 边界（建议先看 `docs/CN/04_protocol_l0/nats_protocol.md`）

Subject 规范由 `core/subject.py` 统一生成：

```python
cg.{ver}.{project_id}.{channel_id}.{category}.{component}.{target}.{suffix}
```

外部接入时最关键的方向：

- `cmd` / `evt`：JetStream，使用 `subscribe_cmd` + `publish_event`
  - `services/pmo/service.py`、`services/agent_worker/loop.py`、`services/ui_worker/loop.py`
- `str`：Core NATS，使用 `subscribe_core` + `publish_core`
  - 示例订阅：`cg.v1r3.{project}.{channel}.str.agent.{agent_id}.chunk`

当前 `NATSClient` 会自动创建默认 stream：

- `cg_cmd_{v1r3}`
- `cg_evt_{v1r3}`

### 订阅/发布建议（外部边界）

- 外部 tool 服务：
  - 订阅：`cg.{v1r3}.{project}.{channel}.cmd.tool.{target}.*`
  - 回执：通过 tool 回调写 `tool.result` card，并走 L0 report 流回到 `state.agent_inbox`
- 外部 UI/事件消费方：
  - 订阅 `evt.agent.{agent_id}.task/step/state` 与 `str.agent.{agent_id}.chunk`
- 外部控制入口：
  - 通过 API 或 `cmd.sys.ui.action`（由 UI 相关服务消费）触发外部行为

### Header 与可见性约束（与实现一致）

`infra/nats_client.py` 的 `publish_event` 已自动补充：

- `traceparent`（可缺省则自动生成）
- `CG-Timestamp`
- `CG-Version`
- `CG-Msg-Type`
- `CG-Sender`

但实现侧仍要求在 `cmd/evt` 协议链路中显式维护：

- `CG-Recursion-Depth`（见 `core/headers.py`）
- `traceparent` 透传与 `tool` 回调上下文一致性

建议统一通过 `core.trace.ensure_trace_headers` + `core.headers.ensure_recursion_depth` 生成调用头，再交由 `publish_event`。

## 3.3 CardBox 边界（持久化状态）

`infra/cardbox_client.py` 提供生产可用的异步接口：

- `CardBoxClient(config=...).init()/close()`
- `save_card`/`get_cards`/`save_box`/`get_box`/`append_to_box` 等
- 所有外部读写都以 `core.utp_protocol.Card` 为语义模型

对外写卡时应优先使用标准 content：

- `ToolCallContent`、`ToolResultContent`
- `ToolCommandPayload`（用于工具回调/调用参数校验，见 `core.utp_protocol.py`）

## 4. 推荐外部工具服务接入模板（与现有服务一致）

### 最小职责

1. 用 `load_app_config` 获取统一配置  
2. 初始化 `NATSClient` + `CardBoxClient`（必要时 `NATSClient.connect()`/`CardBoxClient.init()`）  
3. `subscribe_cmd` 到自己的 `cmd.tool.<target>.*` subject  
4. 根据 `tool_call_card_id` 读取 `tool.call` Card  
5. 执行后写 `tool.result` Card 并通过回流路径触发执行边界  

### 代码形态对齐参考

- 基础连接与订阅风格：`services/tools/mock_search.py`
- 外部工具服务启动方式：`services/tools/openapi_service.py` 的 `main()`
- Tool payload 校验：`core.utp_protocol.ToolCommandPayload`

> 注意：本仓库工具回执的推荐路径是“回写 card + report + wakeup”，而不是直接在业务语义层再发 `cmd.agent.*.tool_result`。

## 5. 与当前代码不一致项（本页修订原因）

- 去掉了旧版“通用安装示例 + 过时示例脚本”中的不一致内容  
- 新增了 `CG__` 环境变量覆盖、`CG_CONFIG_TOML`、`PG_DSN` 兼容链路  
- 将事件边界从单一 `subscribe_core/evt` 示例改为完整 `cmd`/`evt` + `str` 双通道说明  
- 将管理 API 的边界（端点和启动方式）与 `services/api/main.py` 实际实现对齐  
- 补齐 `tool result` 回流机制与 `recursion depth / trace` 的外部约束
