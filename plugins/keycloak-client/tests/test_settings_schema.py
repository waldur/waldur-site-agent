"""The schema of the ``keycloak:`` block shared by the rancher, k8s-ut-namespace and opennebula plugins."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from waldur_site_agent_keycloak_client.schemas import KeycloakSettingsSchema


def test_every_key_the_client_reads_is_accepted():
    settings = {
        "keycloak_url": "https://kc.example.com/auth/",
        "keycloak_realm": "waldur",
        "keycloak_user_realm": "master",
        "client_id": "admin-cli",
        "keycloak_username": "admin",
        "keycloak_password": "secret",
        "keycloak_ssl_verify": True,
    }
    assert KeycloakSettingsSchema(**settings).model_dump(exclude_unset=True) == settings


def test_misspelt_key_is_rejected():
    with pytest.raises(ValidationError, match="keycloak_relm"):
        KeycloakSettingsSchema(keycloak_relm="waldur")


def test_ssl_verify_may_be_a_ca_bundle_path():
    """python-keycloak's verify= takes a CA bundle path as well as a bool."""
    settings = {"keycloak_ssl_verify": "/etc/ssl/certs/internal-ca.pem"}
    assert KeycloakSettingsSchema(**settings).keycloak_ssl_verify == settings["keycloak_ssl_verify"]
