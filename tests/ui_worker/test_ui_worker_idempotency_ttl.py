from __future__ import annotations

from core.config_defaults import DEFAULT_UI_IDEM_TTL_S, default_config
from services.ui_worker.loop import UIWorkerService


def test_default_ui_worker_idempotency_ttl_matches_cmd_replay_window() -> None:
    cfg = default_config()
    assert DEFAULT_UI_IDEM_TTL_S == 24 * 60 * 60
    assert cfg["ui_worker"]["idempotency_ttl_seconds"] == DEFAULT_UI_IDEM_TTL_S


def test_ui_worker_idempotency_ttl_defaults_to_24h() -> None:
    service = UIWorkerService({"ui_worker": {"worker_target": "ui_worker"}})
    assert service.idem.ttl_s == DEFAULT_UI_IDEM_TTL_S


def test_ui_worker_idempotency_ttl_clamps_short_values() -> None:
    service = UIWorkerService(
        {
            "ui_worker": {
                "worker_target": "ui_worker",
                "idempotency_ttl_seconds": 60,
            }
        }
    )
    assert service.idem.ttl_s == DEFAULT_UI_IDEM_TTL_S


def test_ui_worker_idempotency_ttl_accepts_longer_values() -> None:
    custom_ttl = 2 * 24 * 60 * 60
    service = UIWorkerService(
        {
            "ui_worker": {
                "worker_target": "ui_worker",
                "idempotency_ttl_seconds": custom_ttl,
            }
        }
    )
    assert service.idem.ttl_s == custom_ttl
