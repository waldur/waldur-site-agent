"""The cscs-dwdi settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration

BACKENDS = ['cscs-dwdi-compute', 'cscs-dwdi-inference']

VALID_SETTINGS: dict[str, Any] = {
    "cscs_dwdi_api_url": "https://dwdi.example.com",
    "cscs_dwdi_client_id": "id",
    "cscs_dwdi_client_secret": "s",
    "cscs_dwdi_oidc_token_url": "https://auth.example.com/token",
    "cscs_dwdi_cluster": "alps"
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
    settings['cscs_dwdi_clustr'] = 'alps'
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('cscs_dwdi_clustr' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['cscs_dwdi_clustr'] == 'alps'


@pytest.mark.parametrize("backend", BACKENDS)
def test_missing_required_key_is_reported(backend, caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    del settings['cscs_dwdi_api_url']
    with caplog.at_level(logging.WARNING):
        _load(settings, backend)
    assert any('cscs_dwdi_api_url' in m for m in _schema_warnings(caplog))


def test_storage_backend_requires_the_storage_keys(caplog):
    with caplog.at_level(logging.WARNING):
        _load(copy.deepcopy(VALID_SETTINGS), "cscs-dwdi-storage")
    messages = _schema_warnings(caplog)
    assert any("storage_filesystem" in m for m in messages)
    assert any("storage_data_type" in m for m in messages)


def test_storage_backend_with_storage_keys_is_quiet(caplog):
    settings = {
        **copy.deepcopy(VALID_SETTINGS),
        "storage_filesystem": "lustre",
        "storage_data_type": "indirect",
        "storage_path_mapping": {"alloc_a": "/capstor/scratch/a"},
    }
    with caplog.at_level(logging.WARNING):
        _load(settings, "cscs-dwdi-storage")
    assert _schema_warnings(caplog) == []
