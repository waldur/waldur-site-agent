"""The harbor settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration

BACKENDS = ['harbor']

VALID_SETTINGS: dict[str, Any] = {
    "harbor_url": "https://harbor.example.com",
    "robot_username": "robot$waldur",
    "robot_password": "s",
    "default_storage_quota_gb": 10,
    "oidc_group_prefix": "waldur-",
    "project_role_id": 2
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
    settings['harbour_url'] = 'https://harbor.example.com'
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('harbour_url' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['harbour_url'] == 'https://harbor.example.com'


@pytest.mark.parametrize("backend", BACKENDS)
def test_missing_required_key_is_reported(backend, caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    del settings['robot_password']
    with caplog.at_level(logging.WARNING):
        _load(settings, backend)
    assert any('robot_password' in m for m in _schema_warnings(caplog))
