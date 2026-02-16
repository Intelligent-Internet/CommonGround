from core.config_loader import apply_env_overrides, apply_legacy_env_overrides


def test_apply_env_overrides_parses_json_and_fallback(monkeypatch):
    cfg = {}
    monkeypatch.setenv("CG__NATS__SERVERS", '["nats://a","nats://b"]')
    monkeypatch.setenv("CG__WORKER__MAX_STEPS", "42")
    monkeypatch.setenv("CG__OPENAI__API_KEY", "sk-test")

    apply_env_overrides(cfg)

    assert cfg["nats"]["servers"] == ["nats://a", "nats://b"]
    assert cfg["worker"]["max_steps"] == 42
    assert cfg["openai"]["api_key"] == "sk-test"


def test_env_overrides_override_legacy(monkeypatch):
    cfg = {}
    monkeypatch.setenv("NATS_SERVERS", "nats://legacy")
    monkeypatch.setenv("CG__NATS__SERVERS", '["nats://new"]')

    apply_legacy_env_overrides(cfg)
    apply_env_overrides(cfg)

    assert cfg["nats"]["servers"] == ["nats://new"]
