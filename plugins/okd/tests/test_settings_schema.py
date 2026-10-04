"""The okd settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration

BACKENDS = ['okd']

VALID_SETTINGS: dict[str, Any] = {
    "api_url": "https://okd.example.com:6443",
    "token": "t",
    "verify_cert": True,
    "namespace_prefix": "waldur-",
    "customer_prefix": "org-",
    "project_prefix": "proj-",
    "allocation_prefix": "alloc-",
    "default_role": "edit"
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
    settings['verify_certificate'] = True
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('verify_certificate' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['verify_certificate'] == True


@pytest.mark.parametrize("backend", BACKENDS)
def test_verify_cert_may_be_a_ca_bundle_path(backend, caplog):
    """The client passes verify_cert to verify=, which takes a CA bundle path too."""
    settings = copy.deepcopy(VALID_SETTINGS)
    settings["verify_cert"] = "/etc/ssl/certs/internal-ca.pem"
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert _schema_warnings(caplog) == []
    assert loaded.backend_settings["verify_cert"] == "/etc/ssl/certs/internal-ca.pem"
