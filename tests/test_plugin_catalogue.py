"""The plugin catalogue tells Waldur which backends fill which role and which settings are secret."""

from __future__ import annotations

import json
import sys
import typing
from typing import Any, Optional

import pytest
from pydantic import BaseModel, Field, ValidationError

from waldur_site_agent.backend.backends import BackendRole, BaseBackend
from waldur_site_agent.common import plugin_catalogue, plugin_schemas, structures
from waldur_site_agent.common.plugin_schemas import SECRET

if sys.version_info >= (3, 10):
    from importlib.metadata import version
else:
    from importlib_metadata import version

# Words that make a settings field look like a credential, matched against the
# underscore-separated parts of its name.
CREDENTIAL_WORDS = {
    "secret",
    "password",
    "passwd",
    "pwd",
    "token",
    "credential",
    "credentials",
    "key",
    "apikey",
    "private",
    "pem",
    "dsn",
}

# Fields whose names look like credentials but hold something else, keyed by
# module.Model.field so an exemption never covers a real credential of the same
# name elsewhere. Each needs a reason; adding a real credential here instead of
# marking it leaks it into Waldur.
NOT_SECRET = {
    "waldur_site_agent_envoy_ai_gateway.schemas.EnvoyAIGatewayBackendSettingsSchema.apikey_secret": (
        "name of the Kubernetes Secret holding the keys, not a key"
    ),
    "waldur_site_agent_envoy_ai_gateway.schemas.EnvoyAIGatewayBackendSettingsSchema.blocked_secret": (
        "name of the Kubernetes Secret holding blocked keys"
    ),
    "waldur_site_agent_cscs_dwdi.schemas.CSCSDWDIBackendSettingsSchema.cscs_dwdi_oidc_token_url": (
        "URL of the token endpoint"
    ),
    "waldur_site_agent_cscs_dwdi.schemas.CSCSDWDIStorageBackendSettingsSchema.cscs_dwdi_oidc_token_url": (
        "URL of the token endpoint"
    ),
    "waldur_site_agent.common.structures.Offering.oidc_token_url": "URL of the token endpoint",
    "waldur_site_agent_slurm.schemas.SlurmRestApiConfig.token_file": "path to a file holding the token",
    "waldur_site_agent_slurm.schemas.SlurmRestApiConfig.token_env": (
        "name of an environment variable holding the token"
    ),
    "waldur_site_agent_digitalocean.schemas.DigitalOceanBackendSettingsSchema.default_ssh_key_id": (
        "identifier of a public key"
    ),
    "waldur_site_agent_digitalocean.schemas.DigitalOceanBackendSettingsSchema.default_ssh_key_fingerprint": (
        "fingerprint of a public key"
    ),
    "waldur_site_agent_digitalocean.schemas.DigitalOceanBackendSettingsSchema.default_ssh_key_name": (
        "name of a public key"
    ),
    "waldur_site_agent_digitalocean.schemas.DigitalOceanBackendSettingsSchema.default_ssh_public_key": (
        "public key"
    ),
    "waldur_site_agent_ldap.schemas.LdapSettingsSchema.generate_vpn_password": "flag",
}


def _nested_models(annotation: Any) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    found: list[type[BaseModel]] = []
    for arg in typing.get_args(annotation):
        found.extend(_nested_models(arg))
    return found


def _all_models(roots: list[type[BaseModel]]) -> list[type[BaseModel]]:
    seen: list[type[BaseModel]] = []
    pending = list(roots)
    while pending:
        model = pending.pop()
        if model in seen:
            continue
        seen.append(model)
        for field_info in model.model_fields.values():
            pending.extend(_nested_models(field_info.annotation))
    return seen


def _looks_like_credential(name: str) -> bool:
    return bool(CREDENTIAL_WORDS & set(name.lower().split("_")))


def test_every_credential_setting_is_marked_secret() -> None:
    settings_schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    if not settings_schemas:
        pytest.skip("no plugins installed")

    unmarked = sorted(
        f"{model.__module__}.{model.__name__}.{name}"
        for model in _all_models([*settings_schemas.values(), structures.Offering])
        for name, field_info in model.model_fields.items()
        if _looks_like_credential(name)
        and f"{model.__module__}.{model.__name__}.{name}" not in NOT_SECRET
        and not plugin_schemas.is_secret_field(field_info)
    )

    assert not unmarked, (
        "Mark these with json_schema_extra=SECRET, or list them in NOT_SECRET "
        f"with a reason: {unmarked}"
    )


def test_every_exemption_names_a_real_field() -> None:
    settings_schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    if not settings_schemas:
        pytest.skip("no plugins installed")

    fields = {
        f"{model.__module__}.{model.__name__}.{name}"
        for model in _all_models([*settings_schemas.values(), structures.Offering])
        for name in model.model_fields
    }

    assert not sorted(set(NOT_SECRET) - fields)


def test_secret_marker_does_not_change_parsing() -> None:
    class _Settings(plugin_schemas.PluginBackendSettingsSchema):
        client_secret: str = Field(..., json_schema_extra=SECRET)

    assert _Settings(client_secret="s3cret").client_secret == "s3cret"
    assert _Settings.model_json_schema()["properties"]["client_secret"]["secret"] is True


def test_secret_paths_follow_nested_models() -> None:
    class _Inner(BaseModel):
        password: str = Field(..., json_schema_extra=SECRET)
        host: str

    class _Outer(BaseModel):
        token: Optional[str] = Field(default=None, json_schema_extra=SECRET)
        inner: _Inner
        optional_inner: Optional[_Inner] = None
        by_name: dict[str, _Inner] = {}
        listed: list[_Inner] = []

    assert plugin_catalogue.secret_paths(_Outer.model_json_schema()) == [
        "by_name.*.password",
        "inner.password",
        "listed[].password",
        "optional_inner.password",
        "token",
    ]


def test_base_backend_supports_every_role() -> None:
    assert set(BaseBackend.supported_roles) == set(BackendRole)


def _plugin(catalogue: dict[str, Any], name: str) -> dict[str, Any]:
    plugins = {plugin["name"]: plugin for plugin in catalogue["plugins"]}
    if name not in plugins:
        pytest.skip(f"plugin {name} not installed")
    return plugins[name]


def test_catalogue_describes_roles_and_secrets() -> None:
    catalogue = plugin_catalogue.build_catalogue()

    slurm = _plugin(catalogue, "slurm")
    assert set(slurm["roles"]) == {"order_processing", "membership_sync", "reporting"}
    assert "default_account" in slurm["settings_schema"]["properties"]
    assert slurm["component_schema"] is not None

    dwdi = _plugin(catalogue, "cscs-dwdi-compute")
    assert dwdi["roles"] == ["reporting"]
    assert dwdi["secret_settings"] == ["cscs_dwdi_client_secret"]
    assert dwdi["settings_schema"]["properties"]["cscs_dwdi_client_secret"]["secret"] is True

    ldap = _plugin(catalogue, "ldap")
    assert ldap["roles"] == ["username_management"]
    assert "ldap.bind_password" in ldap["secret_settings"]


def test_catalogue_is_stamped_with_versions() -> None:
    catalogue = plugin_catalogue.build_catalogue()

    assert catalogue["catalogue_version"] == plugin_catalogue.CATALOGUE_VERSION
    assert catalogue["agent_version"] == version("waldur-site-agent")
    assert catalogue["offering_secret_fields"] == ["oidc_client_secret", "waldur_api_token"]
    names = [plugin["name"] for plugin in catalogue["plugins"]]
    assert names == sorted(names)
    for plugin in catalogue["plugins"]:
        assert plugin["version"]
        assert plugin["roles"]


def test_main_writes_the_catalogue(tmp_path) -> None:
    output = tmp_path / "catalogue.json"

    plugin_catalogue.main(["--output", str(output)])

    assert json.loads(output.read_text()) == json.loads(
        json.dumps(plugin_catalogue.build_catalogue())
    )


def test_main_refuses_to_write_a_catalogue_with_broken_schemas(tmp_path, monkeypatch) -> None:
    catalogue = plugin_catalogue.build_catalogue()
    catalogue["plugins"][0]["schema_errors"] = {"settings_schema": "TypeError: boom"}
    monkeypatch.setattr(plugin_catalogue, "build_catalogue", lambda: catalogue)
    output = tmp_path / "catalogue.json"

    with pytest.raises(SystemExit) as exit_info:
        plugin_catalogue.main(["--output", str(output)])

    assert exit_info.value.code == 1
    assert not output.exists()


def test_validation_errors_do_not_repeat_the_rejected_value() -> None:
    class _Settings(plugin_schemas.PluginBackendSettingsSchema):
        api_token: int = Field(..., json_schema_extra=SECRET)

    with pytest.raises(ValidationError) as error:
        _Settings(api_token="s3cret-value")

    assert "s3cret-value" not in str(error.value)
