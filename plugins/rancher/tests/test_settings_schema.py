"""The rancher settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration

BACKENDS = ['rancher']

VALID_SETTINGS: dict[str, Any] = {
    "backend_url": "https://rancher.example.com",
    "username": "token-abc",
    "password": "s",
    "cluster_id": "c-m-1",
    "project_prefix": "waldur-",
    "default_role": "workloads-manage",
    "keycloak_enabled": True,
    "keycloak": {
        "keycloak_url": "https://kc.example.com/auth/",
        "keycloak_realm": "waldur"
    }
}


def _load(settings: dict[str, Any], backend: str):
    offering = {
        "name": "Schema test",
        "waldur_api_url": "https://waldur.example.com/api/",
        "waldur_api_token": "token",
        "waldur_offering_uuid": "0" * 32,
        "backend_type": backend,
        "order_processing_backend": backend,
        "backend_settings": settings,
    }
    return RootConfiguration(offerings=[offering]).to_agent_configuration().waldur_offerings[0]


def _schema_warnings(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "Plugin schema validation failed" in r.getMessage()
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_valid_settings_load_without_warnings(backend, caplog):
    with caplog.at_level(logging.WARNING):
        loaded = _load(copy.deepcopy(VALID_SETTINGS), backend)
    assert _schema_warnings(caplog) == []
    assert set(VALID_SETTINGS) <= set(loaded.backend_settings)


@pytest.mark.parametrize("backend", BACKENDS)
def test_misspelt_key_is_reported(backend, caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    settings['cluster'] = 'c-m-1'
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('cluster' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['cluster'] == 'c-m-1'


@pytest.mark.parametrize("backend", BACKENDS)
def test_misspelt_keycloak_key_is_reported(backend, caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    settings["keycloak"]["keycloak_relm"] = "waldur"
    with caplog.at_level(logging.WARNING):
        _load(settings, backend)
    assert any("keycloak_relm" in m for m in _schema_warnings(caplog))


@pytest.mark.parametrize("backend", BACKENDS)
def test_verify_cert_may_be_a_ca_bundle_path(backend, caplog):
    """The client passes verify_cert to verify=, which takes a CA bundle path too."""
    settings = copy.deepcopy(VALID_SETTINGS)
    settings["verify_cert"] = "/etc/ssl/certs/internal-ca.pem"
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings["verify_cert"] == "/etc/ssl/certs/internal-ca.pem"
