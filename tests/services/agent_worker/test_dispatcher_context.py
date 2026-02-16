from services.agent_worker.dispatcher_context import (
    apply_tool_arg_options,
    parse_tool_args,
    render_template,
)


class _DummyLogger:
    def __init__(self) -> None:
        self.warning_calls: list[str] = []

    def warning(self, msg, *args) -> None:
        self.warning_calls.append(msg % args if args else str(msg))


def test_render_template_keeps_non_string_values() -> None:
    assert render_template(123, {"a": 1}) == 123
    assert render_template("x-{a}", {"a": 1}) == "x-1"


def test_parse_tool_args_handles_dict_and_missing() -> None:
    args, err = parse_tool_args({"function": {"arguments": {"k": "v"}}})
    assert args == {"k": "v"}
    assert err is None

    args, err = parse_tool_args({"function": {}})
    assert args == {}
    assert err is None


def test_parse_tool_args_handles_invalid_json_string() -> None:
    args, err = parse_tool_args({"function": {"arguments": "{not json"}})
    assert args == {"_raw": "{not json"}
    assert err is not None
    assert "Invalid arguments JSON" in err


def test_parse_tool_args_handles_non_object_json() -> None:
    args, err = parse_tool_args({"function": {"arguments": "[1,2]"}})
    assert args == {"_raw": [1, 2]}
    assert err is None


def test_apply_tool_arg_options_applies_defaults_fixed_and_env_rules() -> None:
    logger = _DummyLogger()
    merged_args, applied_defaults, applied_meta = apply_tool_arg_options(
        args={"keep": "v", "secret": "llm"},
        tool_options={
            "args": {
                "defaults": {"greet": "hello-{agent_id}", "keep": "should_not_apply"},
                "fixed": {"keep": "forced", "project": "{project_id}"},
            },
            "envs": {"secret": "MY_SECRET_ENV"},
        },
        context={"agent_id": "agent_1", "project_id": "proj_1"},
        logger=logger,
    )
    assert merged_args == {
        "keep": "forced",
        "greet": "hello-agent_1",
        "project": "proj_1",
    }
    assert applied_defaults == {"greet": "hello-agent_1"}
    assert applied_meta["fixed"] == {"keep": "forced", "project": "proj_1"}
    assert applied_meta["envs"] == {"secret": "MY_SECRET_ENV"}
    assert len(logger.warning_calls) == 2
    assert "env-injected parameter" in logger.warning_calls[0]


def test_apply_tool_arg_options_ignores_non_string_default_keys() -> None:
    logger = _DummyLogger()
    merged_args, applied_defaults, applied_meta = apply_tool_arg_options(
        args={},
        tool_options={"args": {"defaults": {1: "x", "ok": "v"}}},
        context={},
        logger=logger,
    )
    assert merged_args == {"ok": "v"}
    assert applied_defaults == {"ok": "v"}
    assert applied_meta["fixed"] == {}
    assert applied_meta["envs"] == {}
    assert any("Non-string key in tool args defaults ignored" in msg for msg in logger.warning_calls)
