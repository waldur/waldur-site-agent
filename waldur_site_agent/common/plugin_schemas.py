"""Plugin schema management for dynamic validation.

This module provides infrastructure for plugins to register their own
Pydantic models for validating plugin-specific configuration fields.
"""

from __future__ import annotations

import sys
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.quota import HomedirQuotaConfig

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points


class TargetComponentConfig(BaseModel):
    """Configuration for a single target component mapping.

    Used by plugins that need to convert Waldur-facing components
    to backend-facing components with a conversion factor.
    """

    model_config = ConfigDict(extra="forbid")

    factor: float = Field(
        default=1.0,
        gt=0,
        description="Conversion factor: target_value = source_value * factor",
    )


class PluginComponentSchema(BaseModel):
    """Base class for plugin-specific component field validation.

    Plugins should inherit from this class to define their component
    field validation schemas.
    """

    model_config = ConfigDict(extra="forbid")  # Plugin schemas should be explicit


class PluginBackendSettingsSchema(BaseModel):
    """Base class for plugin-specific backend settings validation.

    Plugins should inherit from this class to define their backend
    settings validation schemas.
    """

    model_config = ConfigDict(extra="forbid")  # Plugin schemas should be explicit


class CommonBackendSettingsSchema(PluginBackendSettingsSchema):
    """Backend settings the agent core reads for any backend.

    A strict plugin schema inherits these so an offering may set them without
    being reported as unknown keys.
    """

    customer_prefix: Optional[str] = Field(
        default=None, description="Prefix for customer-level backend ids"
    )
    project_prefix: Optional[str] = Field(
        default=None, description="Prefix for project-level backend ids"
    )
    allocation_prefix: Optional[str] = Field(
        default=None, description="Prefix for resource (allocation) backend ids"
    )
    default_account: Optional[str] = Field(
        default=None, description="Account new user associations are created under"
    )
    soft_delete: Optional[bool] = Field(
        default=None,
        description="On termination, keep the backend resource and only remove its users",
    )
    check_backend_id_uniqueness: Optional[bool] = Field(
        default=None, description="Check a new backend id against the offering's history"
    )
    check_all_offerings: Optional[bool] = Field(
        default=None,
        description="Check backend id uniqueness against every offering, not only this one",
    )
    backend_id_max_retries: Optional[int] = Field(
        default=None, ge=1, description="Attempts to find a unique backend id (default 50)"
    )
    periodic_limits: Optional[dict[str, Any]] = Field(
        default=None, description="Periodic limits subscription (event mode)"
    )


class HomedirSettingsSchema(PluginBackendSettingsSchema):
    """Backend settings for POSIX home directory management.

    These settings are consumed by core code — ``BaseBackend.create_user_homedirs``
    and the standalone ``create_homedirs_for_offering_users`` command — so they
    live here rather than in any one plugin. Any backend that declares
    ``supports_user_homedirs`` should inherit this schema so its configuration
    validates the same way.
    """

    enable_user_homedir_account_creation: Optional[bool] = Field(
        default=True, description="Create home directories for users"
    )
    default_homedir_umask: Optional[str] = Field(
        default="0077", description="Umask for created home directories"
    )
    homedir_base_path: Optional[str] = Field(
        default=None,
        description=(
            "Base path for user home directories (e.g. '/cephfs/home'). "
            "When set, quota is applied to {homedir_base_path}/{username}. "
            "When unset, the path is looked up from the system passwd database."
        ),
    )
    homedir_quota: Optional[HomedirQuotaConfig] = Field(
        default=None,
        description="Filesystem quota settings for user home directories",
    )

    @field_validator("default_homedir_umask")
    @classmethod
    def validate_umask(cls, v: Optional[str]) -> Optional[str]:
        """Validate that umask is a valid octal permission."""

        def _raise_umask_error(value: str) -> None:
            msg = f"Invalid umask range: {value}"
            raise ValueError(msg)

        if v is not None:
            try:
                # Try to parse as octal
                umask_value = int(v, 8)
                max_umask = 0o777
                if umask_value < 0 or umask_value > max_umask:
                    _raise_umask_error(v)
            except ValueError as e:
                msg = f"default_homedir_umask must be valid octal permissions (e.g., '0077'): {e}"
                raise ValueError(msg) from e
        return v


def get_plugin_component_schemas() -> dict[str, type[PluginComponentSchema]]:
    """Discover and load plugin component schemas via entry points.

    Returns:
        Dictionary mapping backend names to their component schema classes
    """
    schemas = {}

    try:
        for entry_point in entry_points(group="waldur_site_agent.component_schemas"):
            try:
                schema_class = entry_point.load()
                if issubclass(schema_class, PluginComponentSchema):
                    schemas[entry_point.name] = schema_class
                else:
                    logger.warning("%s schema is not a PluginComponentSchema", entry_point.name)
            except Exception as e:
                logger.warning("Failed to load component schema %s: %s", entry_point.name, e)
    except Exception as e:
        # No plugin schemas found or entry_points failed
        logger.debug("No plugin schemas found: %s", e)

    return schemas


def get_plugin_backend_settings_schemas() -> dict[str, type[PluginBackendSettingsSchema]]:
    """Discover and load plugin backend settings schemas via entry points.

    Returns:
        Dictionary mapping backend names to their settings schema classes
    """
    schemas = {}

    try:
        for entry_point in entry_points(group="waldur_site_agent.backend_settings_schemas"):
            try:
                schema_class = entry_point.load()
                if issubclass(schema_class, PluginBackendSettingsSchema):
                    schemas[entry_point.name] = schema_class
                else:
                    logger.warning(
                        "%s schema is not a PluginBackendSettingsSchema", entry_point.name
                    )
            except Exception as e:
                logger.warning("Failed to load backend settings schema %s: %s", entry_point.name, e)
    except Exception as e:
        # No plugin schemas found or entry_points failed
        logger.debug("No plugin schemas found: %s", e)

    return schemas


def _core_component_fields() -> set[str]:
    from waldur_site_agent.common.structures import BackendComponent  # noqa: PLC0415

    return set(BackendComponent.model_fields.keys())


def validate_component_with_plugin_schema(
    backend_type: str, component_name: str, component_data: dict[str, Any]
) -> dict[str, Any]:
    """Check a component's plugin fields against the backend's component schema.

    Problems are logged as warnings. The component data is returned exactly as
    written: the schema only reports, it never rewrites values.
    """
    return validate_component_for_backends([backend_type], component_name, component_data)


def validate_backend_settings_with_plugin_schema(
    backend_type: str, settings_data: dict[str, Any]
) -> dict[str, Any]:
    """Check backend settings against the backend's settings schema.

    Problems are logged as warnings. The settings are returned exactly as
    written: the schema only reports, it never rewrites values.
    """
    return validate_backend_settings_for_backends([backend_type], settings_data)


def offering_backend_names(offering_data: dict[str, Any]) -> list[str]:
    """Return every backend an offering uses: backend_type plus each role's backend.

    An offering can compose backends from different plugins (for example
    ``order_processing_backend: litellm`` with ``reporting_backend: litellm-usage``);
    each of them reads the same ``backend_settings``. Order is preserved and
    duplicates and empty values are dropped.
    """
    names: list[str] = []
    for key in (
        "backend_type",
        "order_processing_backend",
        "membership_sync_backend",
        "reporting_backend",
    ):
        value = offering_data.get(key) or ""
        name = str(value).lower()
        if name and name not in names:
            names.append(name)
    return names


def _forbids_extra(schema_class: type[BaseModel]) -> bool:
    return schema_class.model_config.get("extra") == "forbid"


def _check_against_schemas(
    kind: str,
    backend_names: list[str],
    data: dict[str, Any],
    schemas: dict[str, Any],
) -> None:
    """Log a warning for every problem the backends' schemas find in *data*.

    Each distinct schema class is checked once, even when several backends
    register it. A strict schema (``extra="forbid"``) is shown only the keys it
    declares whenever another backend shares the data, so keys that backend
    owns do not fail it; a key no schema declares is reported only when every
    backend in use has a strict schema — a backend without one may read any key.
    """
    names = [name for name in backend_names if name]
    by_class: dict[Any, list[str]] = {}
    for name in names:
        if name in schemas:
            by_class.setdefault(schemas[name], []).append(name)
    if not by_class:
        return

    # Someone else reads this data too: another schema, or a backend without one.
    sharing = len(by_class) > 1 or any(name not in schemas for name in names)
    for schema_class, class_names in by_class.items():
        label = ", ".join(class_names)
        if sharing and _forbids_extra(schema_class):
            subset = {k: v for k, v in data.items() if k in schema_class.model_fields}
        else:
            subset = dict(data)
        try:
            schema_class(**subset)
        except Exception as e:
            logger.warning("Plugin schema validation failed for %s %s: %s", label, kind, e)

    every_role_strict = all(name in schemas for name in names) and all(
        _forbids_extra(schema_class) for schema_class in by_class
    )
    if sharing and every_role_strict:
        known: set[str] = set()
        for schema_class in by_class:
            known.update(schema_class.model_fields)
        unknown = sorted(k for k in data if k not in known)
        if unknown:
            logger.warning(
                "Plugin schema validation failed for %s %s: unknown keys %s",
                ", ".join(n for v in by_class.values() for n in v),
                kind,
                ", ".join(unknown),
            )


def validate_backend_settings_for_backends(
    backend_names: list[str], settings_data: dict[str, Any]
) -> dict[str, Any]:
    """Check backend settings against the schema of every backend the offering uses.

    Problems are logged as warnings and the settings are returned exactly as
    written.
    """
    _check_against_schemas(
        "settings", backend_names, settings_data, get_plugin_backend_settings_schemas()
    )
    return settings_data


def validate_component_for_backends(
    backend_names: list[str], component_name: str, component_data: dict[str, Any]
) -> dict[str, Any]:
    """Check a component's plugin fields against every backend's component schema.

    Problems are logged as warnings and the component is returned exactly as
    written.
    """
    core_fields = _core_component_fields()
    plugin_fields = {k: v for k, v in component_data.items() if k not in core_fields}
    if plugin_fields:
        _check_against_schemas(
            f"component {component_name}",
            backend_names,
            plugin_fields,
            get_plugin_component_schemas(),
        )
    return component_data
