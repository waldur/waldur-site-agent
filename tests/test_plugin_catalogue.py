"""The plugin catalogue tells Waldur which backends fill which role and which settings are secret
or sensitive."""

from __future__ import annotations

import json
import re
import typing
from importlib.metadata import version
from typing import Any

import pytest
from pydantic import BaseModel, Field, ValidationError

from waldur_site_agent.backend.backends import BackendRole, BaseBackend
from waldur_site_agent.common import plugin_catalogue, plugin_schemas, structures
from waldur_site_agent.common.plugin_schemas import SECRET, SENSITIVE

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


# Words that make a settings field look like it decides where the agent connects
# (and so where its credentials go), what it reads, writes, runs or installs on
# its host or on customers' machines, or what access it grants. Matched against
# the words of its name, split at underscores and camelCase humps.
SENSITIVE_WORDS = {
    # where it connects
    "url",
    "urls",
    "uri",
    "uris",
    "host",
    "hosts",
    "hostname",
    "server",
    "address",
    "port",
    "endpoint",
    "endpoints",
    "proxy",
    "socket",
    "kubeconfig",
    "context",
    "namespace",
    "realm",
    "verify",
    "ssl",
    "tls",
    "starttls",
    "insecure",
    "cert",
    "cacert",
    # what it reads, writes, runs or installs
    "path",
    "paths",
    "file",
    "files",
    "dir",
    "dirs",
    "directory",
    "mount",
    "home",
    "shell",
    "mode",
    "env",
    "provider",
    "secret",
    "subject",
    "template",
    "umask",
    "owner",
    "permission",
    "permissions",
    "ssh",
    "dns",
    "security",
    # what access it grants
    "role",
    "roles",
    "group",
    "groups",
    "dn",
    "ou",
    "uid",
    "gid",
    "partition",
}
# Word sequences that do the same, matched against the joined words.
SENSITIVE_PHRASES = ("user_data",)

# Fields whose names look sensitive but are not, keyed by <model>.<field> as the
# settings JSON Schema names them (its title, or its $defs key). Each needs a
# reason; adding a real endpoint, path or access setting here lets anyone who may
# edit the offering point the agent elsewhere or widen what it grants.
NOT_SENSITIVE = {
    "WelcomeEmailSchema.from_address": "sender address written into the mail, not a server",
    "HomedirQuotaConfig.max_files": "a count of files (the CephFS inode quota), not a path",
    "SlurmBackendSettingsSchema.hostname": (
        "deprecated and never read by the agent; declared so moved legacy configurations validate"
    ),
    # Blocks: the fields inside them that qualify are marked one by one.
    "SlurmBackendSettingsSchema.project_directory": "a block; its fields are marked",
    "LdapSettingsSchema.access_groups": "a list of blocks; each group's name is marked",
    "LdapSettingsSchema.project_groups": "a block; its fields that qualify are marked",
    # Object classes and personal groups shape the entries the agent creates;
    # who is put in which group is decided by the marked fields.
    "LdapSettingsSchema.user_group_object_classes": "object classes of new entries",
    "LdapSettingsSchema.project_group_object_classes": "object classes of new entries",
    "LdapConnectionSchema.project_group_object_classes": "object classes of new entries",
    "SlurmLdapSettings.user_group_object_classes": "object classes of new entries",
    "SlurmLdapSettings.project_group_object_classes": "object classes of new entries",
    "LdapSettingsSchema.personal_groups": "whether an account gets a group of its own only",
    "SlurmLdapSettings.personal_groups": "whether an account gets a group of its own only",
}


def _settings_fields(schema: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    """Every (model, field, property) a settings JSON Schema declares, nested ones too.

    Walking the exported schema rather than the pydantic models reaches what the
    catalogue publishes, including models named by a forward reference.
    """
    models = [(schema.get("title", ""), schema)]
    models.extend(schema.get("$defs", {}).items())
    return [
        (model, name, prop)
        for model, node in models
        for name, prop in (node.get("properties") or {}).items()
        if isinstance(prop, dict)
    ]


def _words(name: str) -> list[str]:
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return [word for word in snake.lower().split("_") if word]


def _looks_sensitive(name: str) -> bool:
    words = _words(name)
    joined = "_".join(words)
    return bool(SENSITIVE_WORDS & set(words)) or any(
        phrase in joined for phrase in SENSITIVE_PHRASES
    )


def _unmarked_sensitive_looking(schemas: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            f"{model}.{name}"
            for schema in schemas
            for model, name, prop in _settings_fields(schema)
            if _looks_sensitive(name)
            and f"{model}.{name}" not in NOT_SENSITIVE
            and prop.get("sensitive") is not True
            and prop.get("secret") is not True
        }
    )


def _installed_settings_schemas() -> list[dict[str, Any]]:
    """The settings and component JSON Schemas of every installed plugin."""
    settings_schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    if not settings_schemas:
        pytest.skip("no plugins installed")
    schema_classes: list[type[BaseModel]] = [
        *settings_schemas.values(),
        *plugin_schemas.get_plugin_component_schemas().values(),
    ]
    return [schema_class.model_json_schema() for schema_class in schema_classes]


def test_every_endpoint_or_path_setting_is_marked_sensitive() -> None:
    unmarked = _unmarked_sensitive_looking(_installed_settings_schemas())

    assert not unmarked, (
        "Mark these with json_schema_extra=SENSITIVE, or list them in NOT_SENSITIVE "
        f"with a reason: {unmarked}"
    )


def test_an_unmarked_endpoint_or_path_setting_is_caught() -> None:
    class _Settings(plugin_schemas.PluginBackendSettingsSchema):
        backup_dir: str
        target_api_url: str = Field(..., json_schema_extra=SENSITIVE)
        socks_proxy: str
        exempt_path: str
        name_prefix: str
        apiUrl: str  # noqa: N815
        cloud_user_data: str
        admin_role_map: dict[str, str]

    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(NOT_SENSITIVE, "_Settings.exempt_path", "test")
        unmarked = _unmarked_sensitive_looking([_Settings.model_json_schema()])

    assert unmarked == [
        "_Settings.admin_role_map",
        "_Settings.apiUrl",
        "_Settings.backup_dir",
        "_Settings.cloud_user_data",
        "_Settings.socks_proxy",
    ]


def test_every_sensitive_exemption_names_a_real_field() -> None:
    fields = {
        f"{model}.{name}"
        for schema in _installed_settings_schemas()
        for model, name, _prop in _settings_fields(schema)
    }

    assert not sorted(set(NOT_SENSITIVE) - fields)


def test_no_setting_is_marked_both_secret_and_sensitive() -> None:
    both = sorted(
        {
            f"{model}.{name}"
            for schema in _installed_settings_schemas()
            for model, name, prop in _settings_fields(schema)
            if prop.get("secret") is True and prop.get("sensitive") is True
        }
    )

    assert not both, f"A secret stays in secret_settings only; unmark SENSITIVE: {both}"


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
        token: str | None = Field(default=None, json_schema_extra=SECRET)
        inner: _Inner
        optional_inner: _Inner | None = None
        by_name: dict[str, _Inner] = {}
        listed: list[_Inner] = []

    assert plugin_catalogue.secret_paths(_Outer.model_json_schema()) == [
        "by_name.*.password",
        "inner.password",
        "listed[].password",
        "optional_inner.password",
        "token",
    ]


def test_sensitive_marker_does_not_change_parsing() -> None:
    class _Settings(plugin_schemas.PluginBackendSettingsSchema):
        api_url: str = Field(..., json_schema_extra=SENSITIVE)

    assert _Settings(api_url="https://example.org").api_url == "https://example.org"
    assert _Settings.model_json_schema()["properties"]["api_url"]["sensitive"] is True
    assert plugin_schemas.is_sensitive_field(_Settings.model_fields["api_url"])
    assert not plugin_schemas.is_secret_field(_Settings.model_fields["api_url"])


def test_sensitive_paths_follow_nested_models_and_leave_out_secrets() -> None:
    class _Inner(BaseModel):
        uri: str = Field(..., json_schema_extra=SENSITIVE)
        password: str = Field(..., json_schema_extra=SECRET)

    class _Outer(BaseModel):
        api_url: str | None = Field(default=None, json_schema_extra=SENSITIVE)
        token_file: str = Field(..., json_schema_extra={**SECRET, **SENSITIVE})
        inner: _Inner
        optional_inner: _Inner | None = None
        by_name: dict[str, _Inner] = {}
        listed: list[_Inner] = []

    schema = _Outer.model_json_schema()

    assert plugin_catalogue.sensitive_paths(schema) == [
        "api_url",
        "by_name.*.uri",
        "inner.uri",
        "listed[].uri",
        "optional_inner.uri",
    ]
    assert "token_file" in plugin_catalogue.secret_paths(schema)


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


def test_catalogue_lists_sensitive_settings_apart_from_secrets() -> None:
    catalogue = plugin_catalogue.build_catalogue()

    for plugin in catalogue["plugins"]:
        assert "sensitive_settings" in plugin, plugin["name"]
        assert not set(plugin["sensitive_settings"]) & set(plugin["secret_settings"]), plugin[
            "name"
        ]

    slurm = _plugin(catalogue, "slurm")
    for path in ("rest_api.token_file", "rest_api.url", "execution_mode", "slurm_bin_path"):
        assert path in slurm["sensitive_settings"]
    assert slurm["settings_schema"]["properties"]["slurm_bin_path"]["default"] == "/usr/bin"

    assert "saml_mapping_file" in _plugin(catalogue, "opennebula")["sensitive_settings"]
    for name in ("cscs-dwdi-compute", "cscs-dwdi-inference", "cscs-dwdi-storage"):
        dwdi = _plugin(catalogue, name)
        assert "storage_path_mapping" in dwdi["sensitive_settings"]
        assert "cscs_dwdi_oidc_token_url" in dwdi["sensitive_settings"]
        assert "cscs_dwdi_client_secret" not in dwdi["sensitive_settings"]

    assert "ldap.uri" in _plugin(catalogue, "ldap")["sensitive_settings"]


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
