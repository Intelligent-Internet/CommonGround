# Defining Profiles

本文说明如何编写 Profile YAML，以及 `delegation_policy` 的实际约束。

## 1. Profile YAML 与 API 结构

### 1.1 上传入参（`POST /projects/{project_id}/profiles`）

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `name` | 是 | - | profile 名称，上传时按大小写不敏感查重。 |
| `worker_target` | 是 | - | 匹配 worker 订阅。 |
| `tags` | 否 | `[]` | 允许字符串列表；若为字符串会按 `,` 切分。 |
| `description` | 否 | `null` | 仅用于上下文展示。 |
| `system_prompt_template` | 否 | `""` | 作为系统提示词模板。 |
| `system_prompt` | 否 | 无（兼容别名） | 与 `system_prompt_template` 等价，二者同时出现时取 `system_prompt_template`。 |
| `delegation_policy` | 否 | `null` | 见下方语义。 |
| `allowed_tools` | 否 | `[]` | 外部工具白名单。`[]` 表示不允许外部工具。 |
| `allowed_internal_tools` | 否 | `[]` | 内建工具白名单，如 `submit_result`。 |
| `must_end_with` | 否 | `[]` | 强制结束工具列表。 |
| `llm_config` | 否 | 见下方默认值 | 按 `core.llm.LLMConfig` 初始化。 |

### 1.2 返回模型（`Profile`）

上传成功后，管理 API 返回的是 `services/api/profile_models.py` 中的 `Profile`，不是完整 YAML。

- `project_id`
- `profile_box_id`
- `name`
- `worker_target`
- `tags`
- `system_prompt_compiled`
- `updated_at`

`system_prompt_compiled` 为数据库缓存值，当前实现中直接保存为上传的 `system_prompt_template`（未做二次编译）。

## 2. 最小 Profile

```yaml
name: "Associate_Search"
worker_target: "worker_generic"
tags:
  - "associate"
description: "Search specialist"
system_prompt_template: |
  You are a research assistant.
allowed_tools:
  - "web_search"
allowed_internal_tools: []
must_end_with: []
llm_config:
  provider: "gemini"
  model: "gemini/gemini-2.5-flash"
  temperature: 0.2
delegation_policy:
  target_tags: ["associate", "specialist"]
  target_profiles:
    - "Associate_Search"
```

参考文件：`examples/profiles/*.yaml`

## 2. delegation_policy 语义

- `target_tags`：委派目标标签白名单（任一匹配即可）。
- `target_profiles`：按 profile 名称白名单。
- `target_profiles: null`：不按名称限制（仍受标签限制）。
- `target_profiles: []`：禁止委派。

无 `delegation_policy` 时，`delegation_guard` 中会按硬拒策略处理（不会放行委派），`context_loader` 中会得到空的下游列表。

## 3. Profile 加载路径

上传 YAML 后的实际入库链路是：

- `POST /projects/{project_id}/profiles` 接收 YAML，经过 `normalize_profile_yaml` 规范化。
- 解析后的内容写入 `sys.profile` CardBox 卡片，`cardbox.save_card` 后放入一个新的 box，得到 `profile_box_id`。
- `ProfileStore.upsert_profile` 将 `project_id/profile_box_id/name/worker_target/tags/system_prompt_template` 写入 `resource.profiles`（`system_prompt_compiled` 字段也保存该模板文本）。
- 启动/委派时先解析 `profile_name -> profile_box_id`，再按 `profile_box_id` 读取 `resource.profiles` 与 roster。
- `ResourceLoader.load_profile` 再从 cardbox 取 `profile_box_id` 对应 box，选取 `sys.profile` 内容并补全运行时字段。

委派类接口也会先查 `resource.profiles.find_profile_by_name`（名称不区分大小写）确认目标 profile 是否存在，再走上面同一条加载路径。

## 4. 常见字段建议

- `worker_target`：必须与目标 worker 订阅配置一致。
- `tags`：用于能力分层，不要写环境信息。
- `allowed_tools`：最小授权，避免泛化权限。
- `must_end_with`：用于强制终止策略（如仅允许 `submit_result` 收敛）。

## 5. LLM 配置默认值（`llm_config`）

`llm_config` 未显式给出时，`core/llm.py` 的默认值会生效：

- `model = "gpt-3.5-turbo"`
- `temperature = 1.0`
- `stream = true`
- `api_key = null`
- `api_base = null`
- `max_tokens = null`
- `top_p = null`
- `stop = null`
- `tool_choice = null`
- `save_reasoning_content = true`
- `provider_options = null`
- `stream_options = null`

### 切换大模型供应商（LiteLLM 支持）

系统底层通过 LiteLLM 做模型路由。你可以在 Profile YAML 的 `llm_config` 里切换 provider；也可以配合环境变量在运行时覆盖。

示例 1：OpenAI（GPT-5 系列）

```yaml
llm_config:
  model: "gpt-5-mini"
  temperature: 0.7
  provider_options:
    custom_llm_provider: "openai"
```

启动前设置：

```bash
export OPENAI_API_KEY="sk-..."
```

示例 2：Kimi（Moonshot，OpenAI 兼容）

```yaml
llm_config:
  model: "moonshot/kimi-k2.5"
  provider_options:
    custom_llm_provider: "moonshot"
```

启动前设置：

```bash
export MOONSHOT_API_KEY="sk-..."
```

说明：
- `model` 与 provider 标识需符合 LiteLLM 可识别格式。
- 若同时存在 `config.toml` 与环境变量覆盖，环境变量优先。

## 6. 上传与校验

上传：
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/<project_id>/profiles \
  -F file=@examples/profiles/associate_search.yaml
```

查询：
```bash
curl -sS http://127.0.0.1:8099/projects/<project_id>/profiles
```

## 7. 进一步阅读

- API 与示例：`docs/CN/01_getting_started/quick_start.md`
- 协议约束：`docs/CN/04_protocol_l0/state_machine.md`
