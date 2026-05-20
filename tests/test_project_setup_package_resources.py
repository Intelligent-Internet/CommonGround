from __future__ import annotations

from importlib import resources

from Integrations.admin_service.project_setup import _cardbox_schema_sql


def test_cardbox_schema_sql_loads_from_installed_package_resource() -> None:
    expected = resources.files("cardbox.adapters").joinpath("postgres_schema.sql").read_text(encoding="utf-8")

    assert _cardbox_schema_sql() == expected
    assert "create table" in expected.lower()
