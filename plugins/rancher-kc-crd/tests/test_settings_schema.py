"""The rancher-kc-crd settings schema is applied when the agent loads its configuration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import pytest

from waldur_site_agent.common.structures import RootConfiguration
from waldur_site_agent_rancher_kc_crd.schemas import RancherKcCrdBackendSettingsSchema
from waldur_site_agent_rancher_kc_crd.translator import (
    IdentitySettings,
    resolve_identity_settings,
)

BACKENDS = ['rancher-kc-crd']

VALID_SETTINGS: dict[str, Any] = {
    "namespace": "waldur-system",
    "role_map": {
        "PROJECT.MANAGER": "project-owner"
    },
    "cluster_role_map": {
        "PROJECT.ADMIN": "cluster-member"
    },
    "waldur_api_url": "https://waldur.example.com/api/",
    "waldur_api_token": "t"
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
    settings['role_mapping'] = {'PROJECT.MANAGER': 'project-owner'}
    with caplog.at_level(logging.WARNING):
        loaded = _load(settings, backend)
    assert any('role_mapping' in m for m in _schema_warnings(caplog))
    # A schema problem is a warning: the settings are still loaded as written.
    assert loaded.backend_settings['role_mapping'] == {'PROJECT.MANAGER': 'project-owner'}


# ---------------------------------------------------------------------
# Member identity settings
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({}, IdentitySettings("username", "username", None, "${value}", True)),
        (
            {"keycloak_use_user_id": True},
            IdentitySettings("uuid", "id", None, "${value}", False),
        ),
        (
            {"keycloak_use_user_id": False, "keycloak_user_identity_source": "uuid"},
            IdentitySettings("uuid", "username", None, "${value}", True),
        ),
        (
            {
                "keycloak_user_identity_source": "civil_number",
                "keycloak_user_identity_template": "EE${value}",
            },
            IdentitySettings("civil_number", "username", None, "EE${value}", True),
        ),
        (
            {
                "keycloak_user_identity_source": "civil_number",
                "keycloak_user_lookup": "attribute",
                "keycloak_lookup_attribute": "personalCode",
            },
            IdentitySettings("civil_number", "attribute", "personalCode", "${value}", False),
        ),
        (
            {
                "keycloak_user_identity_source": "civil_number",
                "keycloak_user_identity_lowercase": False,
            },
            IdentitySettings("civil_number", "username", None, "${value}", False),
        ),
    ],
)
def test_identity_settings_resolve(settings, expected):
    assert resolve_identity_settings(settings) == expected
    RancherKcCrdBackendSettingsSchema(**settings)


@pytest.mark.parametrize(
    ("settings", "error"),
    [
        (
            {"keycloak_user_identity_source": "civil_number", "keycloak_user_lookup": "id"},
            "only works with keycloak_user_identity_source=uuid",
        ),
        ({"keycloak_user_lookup": "id"}, "only works with keycloak_user_identity_source=uuid"),
        ({"keycloak_user_lookup": "attribute"}, "requires keycloak_lookup_attribute"),
        ({"keycloak_lookup_attribute": "personalCode"}, "only used with"),
        (
            {"keycloak_use_user_id": True, "keycloak_user_identity_source": "civil_number"},
            "deprecated form",
        ),
        (
            {
                "keycloak_use_user_id": True,
                "keycloak_user_lookup": "attribute",
                "keycloak_lookup_attribute": "a",
            },
            "deprecated form",
        ),
        ({"keycloak_user_identity_template": "EE"}, "must contain"),
        ({"keycloak_user_identity_source": "email"}, "keycloak_user_identity_source"),
    ],
)
def test_invalid_identity_settings_are_rejected(settings, error):
    with pytest.raises(ValueError, match=error):
        resolve_identity_settings(settings)
    with pytest.raises(ValueError):  # noqa: PT011
        RancherKcCrdBackendSettingsSchema(**settings)


def test_invalid_identity_combination_is_reported_on_load(caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    settings["keycloak_user_identity_source"] = "civil_number"
    settings["keycloak_user_lookup"] = "id"
    with caplog.at_level(logging.WARNING):
        _load(settings, "rancher-kc-crd")
    assert any("keycloak_user_lookup=id" in m for m in _schema_warnings(caplog))


def test_civil_number_settings_load_without_warnings(caplog):
    settings = copy.deepcopy(VALID_SETTINGS)
    settings.update(
        keycloak_user_identity_source="civil_number",
        keycloak_user_lookup="attribute",
        keycloak_lookup_attribute="personalCode",
        keycloak_user_identity_template="EE${value}",
        keycloak_user_identity_lowercase=False,
    )
    with caplog.at_level(logging.WARNING):
        _load(settings, "rancher-kc-crd")
    assert _schema_warnings(caplog) == []
