"""Offering configurations that come from Waldur (``configuration_source: waldur``).

Such an offering's block in the local file holds only what the agent needs to reach
Waldur and the secrets Waldur never stores. At load the agent reads the rest from
``GET marketplace-provider-offerings/{uuid}/site_agent_config/``, merges the local
secrets into ``backend_settings``, checks that every secret Waldur lists as required
is set, and validates the result like a local configuration. Any failure stops the
load: there is no cached fallback.

For an offering configured locally, ``warn_on_configuration_drift`` compares the file
with what Waldur would serve and logs one warning per difference, best effort.

The endpoint is called through the HTTP client of ``AuthenticatedClient`` rather than
a generated SDK function, so the agent does not wait for a client release.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from pydantic import ValidationError

from waldur_site_agent.backend import logger
from waldur_site_agent.common import structures

SOURCE_LOCAL = "local"
SOURCE_WALDUR = "waldur"

# What a Waldur-sourced offering's local block may hold besides its secret
# backend_settings: how to reach Waldur, and the name it is logged under.
BOOTSTRAP_KEYS = (
    "name",
    "configuration_source",
    "waldur_api_url",
    "waldur_offering_uuid",
    "waldur_api_token",
    "oidc_token_url",
    "oidc_client_id",
    "oidc_client_secret",
    "verify_ssl",
)
ALLOWED_LOCAL_KEYS = frozenset((*BOOTSTRAP_KEYS, "backend_settings"))
# What Waldur must never decide for the agent: where it connects, with which
# credentials and TLS verification, and where the configuration comes from. Waldur's
# response is stripped of these whether or not the local file sets them. ``name`` is
# the one bootstrap key Waldur may supply.
LOCAL_ONLY_KEYS = tuple(key for key in BOOTSTRAP_KEYS if key != "name")

REQUIRED = "required"
REQUIRED_WITH_PARENT = "required_with_parent"
OPTIONAL = "optional"

# Reading the configuration is part of startup; a Waldur that does not answer in
# this time fails the load rather than hanging the agent. The drift check is only
# advisory and gives up sooner.
LOAD_TIMEOUT = 60.0
DRIFT_TIMEOUT = 10.0

DRIFT_COMPONENT_FIELDS = ("accounting_type", "unit_factor", "unit_factor_reporting", "limit")
DRIFT_ROLE_FIELDS = (
    "backend_type",
    "order_processing_backend",
    "membership_sync_backend",
    "reporting_backend",
    "username_management_backend",
)

# Where a detailed error from Waldur is cut, so a long HTML page does not flood the log.
_MAX_ERROR_DETAIL = 300


class ConfigurationSourceError(ValueError):
    """An offering's configuration could not be taken from Waldur."""


@dataclass
class ServedConfiguration:
    """What Waldur serves for one offering."""

    url: str
    offering: dict[str, Any]
    secrets: list[dict[str, Any]] = field(default_factory=list)
    version: str = ""


def site_agent_config_path(offering_uuid: str) -> str:
    """Path of the endpoint, relative to the client's base URL as the SDK's paths are."""
    return f"/api/marketplace-provider-offerings/{offering_uuid}/site_agent_config/"


def _label(name: Any, uuid: Any) -> str:  # noqa: ANN401
    return f"Offering {name!r} ({uuid})" if name and name != uuid else f"Offering {uuid}"


def _endpoint_url(offering: structures.Offering) -> str:
    # The same base URL get_client derives from waldur_api_url.
    base = offering.waldur_api_url.rstrip("/").removesuffix("/api")
    return base + site_agent_config_path(offering.waldur_offering_uuid)


def fetch_configuration(
    offering: structures.Offering,
    user_agent: Optional[str] = None,
    timeout: float = LOAD_TIMEOUT,
) -> ServedConfiguration:
    """Read an offering's configuration from Waldur with the offering's credentials.

    Raises:
        ConfigurationSourceError: Waldur is unreachable, answers with anything but
            200, or serves something that is not an agent configuration. The message
            names the offering, the URL and the status.
    """
    from waldur_site_agent.common import utils  # noqa: PLC0415

    label = _label(offering.name, offering.waldur_offering_uuid)
    url = _endpoint_url(offering)
    try:
        with utils.get_client_for_offering(offering, user_agent, timeout=timeout) as client:
            response = client.get_httpx_client().get(
                site_agent_config_path(offering.waldur_offering_uuid)
            )
    except (httpx.HTTPError, utils.OIDCAuthError) as exc:
        msg = f"{label}: cannot read its configuration from Waldur at {url}: {exc}"
        raise ConfigurationSourceError(msg) from exc

    if response.status_code != httpx.codes.OK:
        detail = " ".join(response.text.split())[:_MAX_ERROR_DETAIL]
        hint = {
            400: " (is it a site agent offering?)",
            401: " (check the offering's credentials)",
            403: " (the token needs permission to update the offering's integration)",
            404: " (unknown offering, or a Waldur without this endpoint)",
        }.get(response.status_code, "")
        msg = (
            f"{label}: Waldur answered HTTP {response.status_code}{hint} "
            f"for its configuration at {url}"
        )
        if detail:
            msg += f": {detail}"
        raise ConfigurationSourceError(msg)

    try:
        payload = response.json()
    except ValueError as exc:
        msg = f"{label}: Waldur did not return JSON for its configuration at {url}"
        raise ConfigurationSourceError(msg) from exc
    served = payload.get("offering") if isinstance(payload, dict) else None
    secrets = payload.get("secrets", []) if isinstance(payload, dict) else None
    if not isinstance(served, dict) or not isinstance(secrets, list):
        msg = f"{label}: Waldur returned no offering configuration at {url}"
        raise ConfigurationSourceError(msg)
    return ServedConfiguration(
        url=url,
        offering=served,
        secrets=[entry for entry in secrets if isinstance(entry, dict) and entry.get("field")],
        version=str(payload.get("version") or ""),
    )


# ---------------------------------------------------------------------------
# Loading an offering from Waldur
# ---------------------------------------------------------------------------


def check_local_keys(offering_data: dict[str, Any]) -> None:
    """Refuse a local key that Waldur's configuration would silently replace."""
    unknown = sorted(str(key) for key in offering_data if key not in ALLOWED_LOCAL_KEYS)
    if unknown:
        label = _label(offering_data.get("name"), offering_data.get("waldur_offering_uuid"))
        msg = (
            f"{label} takes its configuration from Waldur (configuration_source: waldur), "
            f"so its local block may hold only {', '.join(BOOTSTRAP_KEYS)} and secret "
            f"backend_settings. Set these in Waldur instead: {', '.join(unknown)}"
        )
        raise ConfigurationSourceError(msg)


def _bootstrap_offering(offering_data: dict[str, Any], global_proxy: str) -> structures.Offering:
    """An offering holding only what is needed to call Waldur."""
    bootstrap = {key: offering_data[key] for key in BOOTSTRAP_KEYS if key in offering_data}
    bootstrap.setdefault("name", str(offering_data.get("waldur_offering_uuid") or ""))
    try:
        offering = structures.Offering(**bootstrap, backend_type="")
    except ValidationError as exc:
        label = _label(offering_data.get("name"), offering_data.get("waldur_offering_uuid"))
        msg = (
            f"{label}: cannot reach Waldur for its configuration, the local block is "
            f"incomplete: {_validation_problems(exc)}"
        )
        raise ConfigurationSourceError(msg) from exc
    offering._global_proxy = global_proxy
    return offering


def _validation_problems(exc: ValidationError) -> str:
    """Where and what, never the input: the data can hold credentials."""
    return "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'offering'}: {error['msg']}"
        for error in exc.errors(include_input=False)
    )


def load_offering_data(
    offering_data: dict[str, Any],
    global_proxy: str = "",
    user_agent: Optional[str] = None,
) -> tuple[dict[str, Any], ServedConfiguration]:
    """Build a Waldur-sourced offering's configuration: Waldur's, plus the local secrets.

    Returns the merged raw offering data, ready for the same validation as a
    local offering, and what Waldur served.

    Raises:
        ConfigurationSourceError: a local key that is not allowed, Waldur failing,
            or a required secret missing.
    """
    check_local_keys(offering_data)
    bootstrap = _bootstrap_offering(offering_data, global_proxy)
    served = fetch_configuration(bootstrap, user_agent)
    return merge_local_settings(offering_data, served), served


def secret_paths_for_backends(backend_names: list[str]) -> set[str]:
    """Dotted paths, inside backend_settings, of the secrets these plugins declare."""
    from waldur_site_agent.common import plugin_catalogue, plugin_schemas  # noqa: PLC0415

    schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    paths: set[str] = set()
    for name in backend_names:
        schema_class = schemas.get(name)
        if schema_class is None:
            continue
        try:
            paths.update(plugin_catalogue.secret_paths(schema_class.model_json_schema()))
        except Exception as exc:
            logger.debug("Cannot read the secret settings of backend %s: %s", name, exc)
    return paths


def _backend_names(offering_data: dict[str, Any]) -> list[str]:
    from waldur_site_agent.common import plugin_schemas  # noqa: PLC0415

    names = plugin_schemas.offering_backend_names(offering_data)
    username_backend = str(offering_data.get("username_management_backend") or "").lower()
    if username_backend and username_backend not in names:
        names.append(username_backend)
    return names


def _served_secret_paths(served: ServedConfiguration) -> set[str]:
    prefix = "backend_settings."
    return {
        str(entry["field"])[len(prefix) :]
        for entry in served.secrets
        if str(entry["field"]).startswith(prefix)
    }


def _leaves(settings: dict[str, Any], prefix: str = "") -> Iterator[str]:
    """Dotted paths of the values in nested dicts; an empty dict holds none."""
    for key, value in settings.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            yield from _leaves(value, f"{path}.")
        else:
            yield path


def _path_matches(path: str, pattern: str) -> bool:
    parts, pattern_parts = path.split("."), pattern.split(".")
    return len(parts) == len(pattern_parts) and all(
        expected in ("*", actual) for actual, expected in zip(parts, pattern_parts)
    )


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


_MISSING = object()


def _resolve(data: dict[str, Any], path: str) -> Any:  # noqa: ANN401
    node: Any = data
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _is_empty(value: Any) -> bool:  # noqa: ANN401
    return value is _MISSING or value is None or value in ("", {}, [])


def _missing_required_secrets(data: dict[str, Any], served: ServedConfiguration) -> list[str]:
    missing = []
    for entry in served.secrets:
        path = str(entry["field"])
        requirement = entry.get("requirement", OPTIONAL)
        if any(part == "*" or part.endswith("[]") for part in path.split(".")):
            continue  # one per key or item: nothing to point the operator at
        if requirement == REQUIRED_WITH_PARENT and "." in path:
            parent = _resolve(data, path.rsplit(".", 1)[0])
            if not isinstance(parent, dict):
                continue  # the block is not configured, so neither is its secret
        elif requirement not in (REQUIRED, REQUIRED_WITH_PARENT):
            continue
        if _is_empty(_resolve(data, path)):
            backend = entry.get("backend")
            missing.append(f"{path} (backend {backend})" if backend else path)
    return missing


def _secrets_without_their_block(
    local_settings: dict[str, Any], served_settings: dict[str, Any], served: ServedConfiguration
) -> list[str]:
    """Local secrets Waldur lists as part of a block that Waldur does not serve."""
    prefix = "backend_settings."
    with_parent = [
        str(entry["field"])[len(prefix) :]
        for entry in served.secrets
        if entry.get("requirement") == REQUIRED_WITH_PARENT
        and str(entry["field"]).startswith(prefix)
    ]
    return [
        f"{prefix}{path}"
        for path in _leaves(local_settings)
        if "." in path
        and any(_path_matches(path, pattern) for pattern in with_parent)
        and not isinstance(_resolve(served_settings, path.rsplit(".", 1)[0]), dict)
    ]


def merge_local_settings(
    offering_data: dict[str, Any], served: ServedConfiguration
) -> dict[str, Any]:
    """Waldur's offering configuration with the local bootstrap fields and secrets.

    Raises:
        ConfigurationSourceError: a local backend setting that is not a secret, or a
            secret Waldur requires that is not set locally.
    """
    uuid = offering_data.get("waldur_offering_uuid")
    data = copy.deepcopy(served.offering)
    from_waldur = sorted(key for key in LOCAL_ONLY_KEYS if key in data)
    for key in from_waldur:
        del data[key]
    for key in BOOTSTRAP_KEYS:
        if key in offering_data:
            data[key] = offering_data[key]
    if not data.get("name"):
        data["name"] = str(uuid)
    label = _label(data["name"], uuid)
    if from_waldur:
        logger.warning(
            "%s: ignoring keys Waldur sent that only the local file may set: %s",
            label,
            ", ".join(from_waldur),
        )

    local_settings = offering_data.get("backend_settings") or {}
    if not isinstance(local_settings, dict):
        msg = f"{label}: backend_settings must be a mapping of secret settings"
        raise ConfigurationSourceError(msg)
    allowed = _served_secret_paths(served) | secret_paths_for_backends(_backend_names(data))
    not_secret = [
        f"backend_settings.{path}"
        for path in _leaves(local_settings)
        if not any(_path_matches(path, pattern) for pattern in allowed)
    ]
    if not_secret:
        msg = (
            f"{label} takes its configuration from Waldur (configuration_source: waldur), "
            "so its local backend_settings may hold only the secrets Waldur lists for it. "
            f"Set these in Waldur instead: {', '.join(not_secret)}"
        )
        raise ConfigurationSourceError(msg)
    served_settings = data.get("backend_settings") or {}
    if not isinstance(served_settings, dict):
        served_settings = {}
    orphans = _secrets_without_their_block(local_settings, served_settings, served)
    if orphans:
        # Merged in, the secret would make a block of its own that holds nothing
        # else, and the backend would fail on it at every cycle instead of here.
        msg = (
            f"{label}: the local configuration file sets secrets for blocks that Waldur "
            f"does not configure: {', '.join(orphans)}. Configure the block in Waldur, "
            "or remove the secret from the local file."
        )
        raise ConfigurationSourceError(msg)
    data["backend_settings"] = _deep_merge(served_settings, local_settings)

    missing = _missing_required_secrets(data, served)
    if missing:
        msg = (
            f"{label}: Waldur requires secrets that are not set in the local configuration "
            f"file: {', '.join(missing)}. Add them to the offering's backend_settings there."
        )
        raise ConfigurationSourceError(msg)

    ignored = sorted(set(served.offering) - set(structures.Offering.model_fields))
    if ignored:
        logger.debug("%s: Waldur serves keys the agent does not read: %s", label, ignored)
    return data


def build_offering(data: dict[str, Any], served: ServedConfiguration) -> structures.Offering:
    """Validate a Waldur-sourced offering's merged data like a local one, and log it."""
    label = _label(data.get("name"), data.get("waldur_offering_uuid"))
    try:
        offering = structures.parse_offering(data)
    except ValidationError as exc:
        # The data holds the merged secrets: report where and what, never the input.
        problems = _validation_problems(exc)
        msg = (
            f"{label}: the configuration Waldur serves at {served.url} "
            f"(version {served.version or 'unknown'}) is not valid: {problems}"
        )
        raise ConfigurationSourceError(msg) from exc
    offering._configuration_version = served.version
    logger.info(
        "Offering %s (%s): configuration source waldur, version %s",
        offering.name,
        offering.waldur_offering_uuid,
        served.version or "unknown",
        configuration_source=SOURCE_WALDUR,
        configuration_version=served.version,
    )
    return offering


# ---------------------------------------------------------------------------
# Drift between a local configuration and Waldur's
# ---------------------------------------------------------------------------


@dataclass
class Drift:
    """One difference between a local offering configuration and Waldur's.

    ``local`` and ``waldur`` are kept only where they cannot be a credential
    (component numbers, backend roles). A backend setting's values are never
    kept: a plugin may hold a secret under a key no schema marks as one, and
    the warnings can be shipped to Waldur with the agent's logs.
    """

    subject: str
    local: Any = None
    waldur: Any = None
    values_shown: bool = True


def _same(local: Any, waldur: Any) -> bool:  # noqa: ANN401
    """Equal, counting an empty string as unset and 1 as 1.0."""
    local = None if local == "" else local
    waldur = None if waldur == "" else waldur
    if isinstance(local, (int, float)) and isinstance(waldur, (int, float)):
        return float(local) == float(waldur)
    return bool(local == waldur)


def _component_values(component: dict[str, Any]) -> Optional[dict[str, Any]]:
    try:
        model = structures.BackendComponent(**component)
    except ValidationError:
        return None
    values = model.model_dump(mode="json")
    return {name: values.get(name) for name in DRIFT_COMPONENT_FIELDS}


def _without_paths(settings: dict[str, Any], patterns: set[str]) -> dict[str, Any]:
    def strip(node: dict[str, Any], prefix: str) -> dict[str, Any]:
        kept: dict[str, Any] = {}
        for key, value in node.items():
            path = f"{prefix}{key}"
            if any(_path_matches(path, pattern) for pattern in patterns):
                continue
            kept[key] = strip(value, f"{path}.") if isinstance(value, dict) else value
        return kept

    return strip(settings, "")


def _setting_defaults(backend_names: list[str]) -> dict[str, Any]:
    """Defaults the offering's plugins give their settings, for keys a file leaves out."""
    from waldur_site_agent.common import plugin_schemas  # noqa: PLC0415

    schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    defaults: dict[str, Any] = {}
    for name in backend_names:
        schema_class = schemas.get(name)
        if schema_class is None:
            continue
        for key, info in schema_class.model_fields.items():
            if key not in defaults and not info.is_required():
                defaults[key] = info.get_default(call_default_factory=True)
    return defaults


def configuration_drift(
    offering: structures.Offering, served: ServedConfiguration
) -> list[Drift]:
    """Differences between a locally configured offering and what Waldur serves.

    Compares, per component, the fields that change what is billed or allocated;
    each non-secret backend setting (a key a file leaves out counts as its plugin
    default); and the backend roles.
    """
    drift: list[Drift] = []
    waldur = served.offering

    for role in DRIFT_ROLE_FIELDS:
        default = structures.Offering.model_fields[role].default
        local_value = getattr(offering, role)
        waldur_value = waldur.get(role, default)
        if not _same(local_value, waldur_value):
            drift.append(Drift(role, local_value, waldur_value))

    served_components = waldur.get("backend_components") or {}
    local_components = offering.backend_components_dict
    for name, component in local_components.items():
        if name not in served_components:
            drift.append(Drift(f"component {name}", "configured", "missing"))
            continue
        local_values = _component_values(component)
        waldur_values = _component_values(served_components[name])
        if local_values is None or waldur_values is None:
            continue
        drift.extend(
            Drift(
                f"component {name} {field_name}",
                local_values[field_name],
                waldur_values[field_name],
            )
            for field_name in DRIFT_COMPONENT_FIELDS
            if not _same(local_values[field_name], waldur_values[field_name])
        )

    backend_names = _backend_names(offering.model_dump())
    secrets = _served_secret_paths(served) | secret_paths_for_backends(backend_names)
    local_settings = _without_paths(offering.backend_settings, secrets)
    served_settings = waldur.get("backend_settings") or {}
    if not isinstance(served_settings, dict):
        served_settings = {}
    waldur_settings = _without_paths(served_settings, secrets)
    defaults = _setting_defaults(backend_names)
    for key in sorted(set(local_settings) | set(waldur_settings)):
        local_value = local_settings.get(key, defaults.get(key))
        waldur_value = waldur_settings.get(key, defaults.get(key))
        if not _same(local_value, waldur_value):
            drift.append(Drift(f"backend_settings.{key}", values_shown=False))
    return drift


def warn_on_configuration_drift(
    configuration: structures.WaldurAgentConfiguration, timeout: float = DRIFT_TIMEOUT
) -> None:
    """Warn about each difference between a local offering configuration and Waldur's.

    Best effort: an offering whose configuration Waldur does not serve (an older
    Waldur, a token without the permission, Waldur unreachable) is skipped with a
    debug message. Waldur-sourced offerings have nothing to compare.
    """
    for offering in configuration.waldur_offerings:
        if offering.configuration_source != SOURCE_LOCAL:
            continue
        try:
            served = fetch_configuration(offering, configuration.waldur_user_agent, timeout)
            drift = configuration_drift(offering, served)
        except Exception as exc:
            logger.debug(
                "Offering %s: configuration drift check skipped: %s", offering.name, exc
            )
            continue
        for item in drift:
            if not item.values_shown:
                logger.warning(
                    "Offering %s (%s): %s differs between the local configuration and Waldur",
                    offering.name,
                    offering.waldur_offering_uuid,
                    item.subject,
                    configuration_drift=item.subject,
                )
                continue
            logger.warning(
                "Offering %s (%s): %s is %r in the local configuration but %r in Waldur",
                offering.name,
                offering.waldur_offering_uuid,
                item.subject,
                item.local,
                item.waldur,
                configuration_drift=item.subject,
                local_value=item.local,
                waldur_value=item.waldur,
            )
