"""The ceph-s3 settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration

BACKENDS = ['ceph_s3', 'croit_usage']

VALID_SETTINGS: dict[str, Any] = {
    "api_url": "https://croit.example.com",
    "token": "t",
    "s3_endpoint": "https://s3.example.com",
    "default_placement": "default-placement",
    "timeout": 30
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
    settings['s3_endpiont'] = 'https://s3.example.com'
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('s3_endpiont' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['s3_endpiont'] == 'https://s3.example.com'


def test_shared_schema_warns_once_for_both_backends(caplog):
    """ceph_s3 and croit_usage register one class; a composed offering warns once."""
    settings = copy.deepcopy(VALID_SETTINGS)
    settings["timeout"] = "not-a-number"
    offering = {
        "name": "Schema test",
        "waldur_api_url": "https://waldur.example.com/api/",
        "waldur_api_token": "token",
        "waldur_offering_uuid": "0" * 32,
        "backend_type": "ceph_s3",
        "order_processing_backend": "ceph_s3",
        "reporting_backend": "croit_usage",
        "backend_settings": settings,
    }
    with caplog.at_level(logging.WARNING):
        RootConfiguration(offerings=[offering]).to_agent_configuration()
    assert len(_schema_warnings(caplog)) == 1
