# NATS Subject 清单（自动生成）

> ⚠️ 本文件由脚本自动生成，请勿手改。权威语义/约束请看：`docs/04_protocol_l0/nats_protocol.md`。

生成方式：

```bash
python scripts/admin/generate_nats_subjects_doc.py
```

- 扫描目录：`core`, `infra`, `services`, `scripts`, `examples`, `tests`
- 排除目录名：`.git`, `.venv`, `__pycache__`, `card-box-cg`

## JetStream / cmd（subscribe_cmd / publish_event）
| Subject pattern | Transport | Direction | API | Queue | Durable | Sources |
| --- | --- | --- | --- | --- | --- | --- |
| `cg.{PROTOCOL_VERSION}.*.*.cmd.tool.skills.*` | jetstream | subscribe | subscribe_cmd | `tool_skills` | `tool_skills_cmd_{PROTOCOL_VERSION}` | `services/tools/skills_service.py:1125` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.pmo.internal.{tool_name}` | jetstream | publish | publish_event |  |  | `examples/quickstarts/demo_principal_fullflow.py:164`, `scripts/benchmarks/bench_fork_join_batch.py:282` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.ui.action` | jetstream | publish | publish_event |  |  | `tests/integration/test_ui_action_flow.py:235` |

## JetStream / evt（subscribe_cmd / publish_event）
_（未发现）_

## Core NATS / str（subscribe_core / publish_core）
| Subject pattern | Transport | Direction | API | Queue | Durable | Sources |
| --- | --- | --- | --- | --- | --- | --- |
| `cg.v1r3.{project_id}.{channel_id}.str.agent.{agent_id}.chunk` | core | subscribe | subscribe_core |  |  | `examples/quickstarts/demo_simple_principal_stream.py:247` |

## 其他 / 未归类（literal / format_subject / unknown）
| Subject pattern | Transport | Direction | API | Queue | Durable | Sources |
| --- | --- | --- | --- | --- | --- | --- |
| `cg.v1r3.proj_1.public.cmd.sys.pmo.internal.delegate_async` | unknown | literal | literal |  |  | `tests/pmo/test_protocol.py:67` |
| `cg.v1r3.proj_1.public.cmd.tool.demo.execute` | unknown | literal | literal |  |  | `tests/tools/test_tool_runner.py:144`, `tests/tools/test_tool_runner.py:185`, `tests/tools/test_tool_runner.py:214`, `tests/tools/test_tool_runner.py:253`, `tests/tools/test_tool_runner.py:299` |
| `cg.v1r3.proj_1.public.str.agent.agent_1.chunk` | unknown | literal | literal |  |  | `tests/services/agent_worker/test_react_streaming.py:35`, `tests/services/agent_worker/test_react_streaming.py:80`, `tests/services/agent_worker/test_react_streaming.py:104` |
| `cg.v1r3.{project_id}.{channel_id}.evt.agent.{agent_id}.step` | core | subscribe | subscribe_core |  |  | `examples/quickstarts/demo_simple_principal_stream.py:260` |
| `cg.v1r3.{project_id}.{channel_id}.evt.agent.{agent_id}.task` | core | subscribe | subscribe_core |  |  | `examples/quickstarts/demo_simple_principal_stream.py:248` |
| `cg.{PROTOCOL_VERSION}.proj_1.public.cmd.sys.pmo.internal.delegate_async` | unknown | literal | format_subject |  |  | `tests/pmo/test_protocol.py:39` |
| `cg.{PROTOCOL_VERSION}.proj_1.public.cmd.sys.ui.action` | unknown | literal | format_subject |  |  | `tests/pmo/test_protocol.py:50` |
| `cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.cmd.agent.{target}.wakeup` | unknown | literal | format_subject |  |  | `tests/integration/test_submit_result_inbox_guard.py:642` |
| `cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.cmd.sys.ui.action` | jetstream | publish | publish_event |  |  | `examples/quickstarts/demo_ui_action.py:117`, `examples/quickstarts/demo_ui_action.py:183`, `examples/quickstarts/demo_ui_action.py:185` |
| `cg.{PROTOCOL_VERSION}.{head.project_id}.{head.active_channel_id}.cmd.agent.{target}.wakeup` | unknown | literal | format_subject |  |  | `services/pmo/l0_guard/guard.py:226` |
| `cg.{PROTOCOL_VERSION}.{parts.project_id}.{parts.channel_id}.cmd.agent.{target}.wakeup` | unknown | literal | format_subject |  |  | `services/tools/skills_service.py:2415` |
| `cg.{PROTOCOL_VERSION}.{parts.project_id}.{parts.channel_id}.evt.sys.ui.action_ack` | unknown | literal | format_subject |  |  | `services/ui_worker/loop.py:152` |
| `cg.{PROTOCOL_VERSION}.{parts.project_id}.{parts.channel_id}.evt.sys.ui.marker` | unknown | literal | format_subject |  |  | `services/tools/emit_marker.py:117` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.agent.worker_generic.wakeup` | unknown | literal | format_subject |  |  | `tests/integration/test_watchdog_safety_net.py:200` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.agent.{target}.wakeup` | unknown | literal | format_subject |  |  | `infra/l0_engine.py:731`, `services/agent_worker/loop.py:1363`, `services/pmo/l0_guard/guard.py:522`, `services/tools/tool_caller.py:256` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.agent.{agent_id}.state` | unknown | literal | format_subject |  |  | `core/subject.py:114` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.agent.{agent_id}.step` | unknown | literal | format_subject |  |  | `core/subject.py:110` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.agent.{agent_id}.task` | unknown | literal | format_subject |  |  | `core/subject.py:106`, `examples/quickstarts/demo_principal_fullflow.py:807`, `scripts/utils/nats.py:22`, `tests/integration/test_watchdog_safety_net.py:113` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.evt.agent.{str(child_agent_id)}.task` | unknown | literal | format_subject |  |  | `examples/quickstarts/demo_principal_fullflow.py:743` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.str.agent.{agent_id}.chunk` | unknown | literal | format_subject |  |  | `examples/quickstarts/demo_principal_fullflow.py:806`, `services/agent_worker/react_streaming.py:11` |
| `cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.str.agent.{str(child_agent_id)}.chunk` | unknown | literal | format_subject |  |  | `examples/quickstarts/demo_principal_fullflow.py:742` |
| `cg.{PROTOCOL_VERSION}.{str(args.project)}.{str(args.channel)}.evt.agent.{str(args.agent_id)}.task` | unknown | literal | format_subject |  |  | `examples/quickstarts/demo_fork_join_word_count.py:493` |
| `cg.{PROTOCOL_VERSION}.{str(args.project)}.{str(args.channel)}.str.agent.{str(args.agent_id)}.chunk` | unknown | literal | format_subject |  |  | `examples/quickstarts/demo_fork_join_word_count.py:485` |
| `cg.{protocol_version}.{project_id}.{channel_id}.cmd.{component}.{target}.{suffix}` | unknown | literal | format_subject |  |  | `core/subject.py:91` |
| `cg.{protocol_version}.{project_id}.{channel_id}.evt.{component}.{target}.{suffix}` | unknown | literal | format_subject |  |  | `core/subject.py:102` |
