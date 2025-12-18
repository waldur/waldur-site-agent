"""Plugin schema management for dynamic validation.

This module provides infrastructure for plugins to register their own
Pydantic models for validating plugin-specific configuration fields.
"""

from __future__ import annotations

import sys
from typing import Any

from pydantic import BaseModel, ConfigDict

from waldur_site_agent.backend import logger

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points


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


def validate_component_with_plugin_schema(
    backend_type: str, component_name: str, component_data: dict[str, Any]
) -> dict[str, Any]:
    """Validate component data using plugin-specific schema if available.

    This function separates core fields from plugin-specific fields,
    validates plugin fields with the plugin schema, and recombines them.

    Args:
        backend_type: The backend type (e.g., "slurm", "mup")
        component_name: Name of the component (e.g., "cpu", "storage")
        component_data: Raw component configuration data

    Returns:
        Component data with plugin-specific fields validated
    """
    plugin_schemas = get_plugin_component_schemas()

    if backend_type in plugin_schemas:
        try:
            # Separate core fields from plugin fields
            from waldur_site_agent.common.structures import BackendComponent  # noqa: PLC0415

            core_fields = set(BackendComponent.model_fields.keys())

            plugin_fields = {k: v for k, v in component_data.items() if k not in core_fields}
            core_data = {k: v for k, v in component_data.items() if k in core_fields}

            if plugin_fields:
                # Validate plugin-specific fields only
                plugin_schema_class = plugin_schemas[backend_type]
                validated_plugin_model = plugin_schema_class(**plugin_fields)
                validated_plugin_data = validated_plugin_model.model_dump(exclude_unset=True)

                # Combine core data with validated plugin data
                return {**core_data, **validated_plugin_data}
            # No plugin fields to validate
            return component_data

        except Exception as e:
            logger.warning(
                "Plugin schema validation failed for %s.%s: %s", backend_type, component_name, e
            )
            # Fall back to unvalidated data
            return component_data

    # No plugin schema - return data as-is
    return component_data


def validate_backend_settings_with_plugin_schema(
    backend_type: str, settings_data: dict[str, Any]
) -> dict[str, Any]:
    """Validate backend settings using plugin-specific schema if available.

    Args:
        backend_type: The backend type (e.g., "slurm", "mup")
        settings_data: Raw backend settings configuration data

    Returns:
        Validated settings data
    """
    plugin_schemas = get_plugin_backend_settings_schemas()

    if backend_type in plugin_schemas:
        try:
            # Validate using plugin schema
            plugin_schema_class = plugin_schemas[backend_type]
            validated_plugin_data = plugin_schema_class(**settings_data)

            # Return the validated data as dict
            return validated_plugin_data.model_dump(exclude_unset=True)
        except Exception as e:
            logger.warning("Plugin schema validation failed for %s settings: %s", backend_type, e)
            # Fall back to unvalidated data
            return settings_data

    # No plugin schema - return data as-is
    return settings_data
