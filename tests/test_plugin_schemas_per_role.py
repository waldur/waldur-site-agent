"""Plugin schemas are applied for every backend an offering uses, not only backend_type."""

from __future__ import annotations

import logging
from typing import Any, Optional

import pytest
from pydantic import ConfigDict, Field

from waldur_site_agent.common import plugin_schemas
from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)
from waldur_site_agent.common.structures import RootConfiguration


class _OrderSettings(PluginBackendSettingsSchema):
    api_url: str
    models: Optional[list] = None


class _UsageSettings(PluginBackendSettingsSchema):
    api_url: str
    usage_cache_ttl: Optional[float] = None


class _OpenUsageSettings(PluginBackendSettingsSchema):
    model_config = ConfigDict(extra="allow")

    api_url: str
    usage_cache_ttl: Optional[float] = None


class _UsageComponent(PluginComponentSchema):
    metric: str = Field(...)


@pytest.fixture
def fake_schemas(monkeypatch):
    settings: dict[str, Any] = {"order-be": _OrderSettings, "usage-be": _UsageSettings}
    components: dict[str, Any] = {"usage-be": _UsageComponent}
    monkeypatch.setattr(plugin_schemas, "get_plugin_backend_settings_schemas", lambda: settings)
    monkeypatch.setattr(plugin_schemas, "get_plugin_component_schemas", lambda: components)
    return settings, components


def _offering(**overrides: Any) -> dict[str, Any]:
    offering = {
        "name": "Composed",
        "waldur_api_url": "https://waldur.example.com/api/",
        "waldur_api_token": "token",
        "waldur_offering_uuid": "0" * 32,
        "backend_type": "order-be",
        "order_processing_backend": "order-be",
        "reporting_backend": "usage-be",
        "backend_settings": {"api_url": "https://llm.example.com", "models": ["a"]},
        "backend_components": {
            "tokens": {
                "measured_unit": "k",
                "unit_factor": 1,
                "accounting_type": "usage",
                "label": "Tokens",
                "metric": "total_tokens",
            }
        },
    }
    offering.update(overrides)
    return offering


def _load(offering: dict[str, Any]):
    return RootConfiguration(offerings=[offering]).to_agent_configuration().waldur_offerings[0]


def _schema_warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_reporting_backend_settings_are_validated(fake_schemas, caplog):
    """A value only the reporting backend's schema rejects is reported."""
    offering = _offering()
    offering["backend_settings"]["usage_cache_ttl"] = "not-a-number"

    with caplog.at_level(logging.WARNING):
        _load(offering)

    assert any("usage-be" in m and "usage_cache_ttl" in m for m in _schema_warnings(caplog))


def test_composed_offering_with_valid_settings_is_quiet(fake_schemas, caplog):
    """Keys owned by different roles do not count as unknown to each other."""
    offering = _offering()
    offering["backend_settings"]["usage_cache_ttl"] = 30

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings["usage_cache_ttl"] == 30
    assert loaded.backend_settings["models"] == ["a"]


def test_key_unknown_to_every_role_is_reported(fake_schemas, caplog):
    """A typo is caught when every role's schema is strict."""
    offering = _offering()
    offering["backend_settings"]["api_ulr"] = "typo"

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert any("api_ulr" in m for m in _schema_warnings(caplog))
    # Validation failures stay warnings: the raw settings are kept.
    assert loaded.backend_settings["api_ulr"] == "typo"


def test_lenient_role_schema_allows_unknown_keys(fake_schemas, caplog):
    """If any role's schema allows extra keys, unknown keys are not reported."""
    fake_schemas[0]["usage-be"] = _OpenUsageSettings
    offering = _offering()
    offering["backend_settings"]["something_else"] = 1

    with caplog.at_level(logging.WARNING):
        _load(offering)

    assert _schema_warnings(caplog) == []


def test_reporting_backend_component_fields_are_validated(fake_schemas, caplog):
    """A component field required by the reporting backend's schema is checked."""
    offering = _offering()
    del offering["backend_components"]["tokens"]["metric"]
    offering["backend_components"]["tokens"]["metrc"] = "total_tokens"

    with caplog.at_level(logging.WARNING):
        _load(offering)

    assert any("usage-be" in m and "tokens" in m for m in _schema_warnings(caplog))


def test_single_backend_behaviour_unchanged(fake_schemas, caplog):
    """One backend for every role: validated by that backend's schema, as before."""
    offering = _offering(reporting_backend="order-be", backend_components={})
    offering["backend_settings"]["unexpected"] = True

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert any(
        "Plugin schema validation failed for order-be settings" in m
        for m in _schema_warnings(caplog)
    )
    assert loaded.backend_settings["unexpected"] is True


class _TypedSettings(PluginBackendSettingsSchema):
    api_url: str
    port: Optional[int] = None
    verify: Optional[bool] = None


class _TypedComponent(PluginComponentSchema):
    metric: str = Field(...)
    weight: Optional[int] = None


def test_valid_settings_are_kept_exactly_as_written(fake_schemas, caplog):
    """Validation only warns: a value the schema would coerce is not rewritten."""
    fake_schemas[0]["order-be"] = _TypedSettings
    offering = _offering(reporting_backend="order-be", backend_components={})
    offering["backend_settings"] = {"api_url": "https://x", "port": "8080", "verify": "false"}

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings == {"api_url": "https://x", "port": "8080", "verify": "false"}


def test_composed_settings_are_kept_exactly_as_written(fake_schemas, caplog):
    fake_schemas[0]["order-be"] = _TypedSettings
    offering = _offering()
    offering["backend_settings"] = {"api_url": "https://x", "port": "8080", "usage_cache_ttl": "30"}

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings["port"] == "8080"
    assert loaded.backend_settings["usage_cache_ttl"] == "30"


def test_valid_component_fields_are_kept_exactly_as_written(fake_schemas, caplog):
    fake_schemas[1]["usage-be"] = _TypedComponent
    offering = _offering(backend_type="usage-be", order_processing_backend="usage-be")
    offering["backend_settings"] = {"api_url": "https://x"}
    offering["backend_components"]["tokens"]["weight"] = "3"

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert _schema_warnings(caplog) == []
    assert loaded.backend_components["tokens"].model_extra["weight"] == "3"


def test_schema_less_role_keys_are_not_reported_as_unknown(fake_schemas, caplog):
    """A role whose backend ships no schema may read any key, so none is 'unknown'."""
    offering = _offering(reporting_backend="third-party-usage", backend_components={})
    offering["backend_settings"]["third_party_key"] = "value"

    with caplog.at_level(logging.WARNING):
        loaded = _load(offering)

    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings["third_party_key"] == "value"


def test_schema_less_role_does_not_hide_errors_in_known_keys(fake_schemas, caplog):
    offering = _offering(reporting_backend="third-party-usage", backend_components={})
    offering["backend_settings"]["models"] = "not-a-list"

    with caplog.at_level(logging.WARNING):
        _load(offering)

    assert any("order-be" in m and "models" in m for m in _schema_warnings(caplog))


def test_one_schema_shared_by_two_roles_warns_once(fake_schemas, caplog):
    """Two entry points registering the same class are validated once."""
    fake_schemas[0]["usage-be"] = _OrderSettings
    offering = _offering(backend_components={})
    offering["backend_settings"]["models"] = "not-a-list"

    with caplog.at_level(logging.WARNING):
        _load(offering)

    assert len(_schema_warnings(caplog)) == 1
