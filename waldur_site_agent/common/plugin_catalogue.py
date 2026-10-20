"""Describe the installed plugins for Waldur: roles, settings and component schemas.

Waldur offers a list of backends per role and validates an offering's non-secret
backend settings before any agent has connected, so it needs this description
ahead of time rather than from a running agent. ``waldur_site_export_plugin_catalogue``
writes it as one JSON document that Waldur vendors.

Settings marked with ``plugin_schemas.SECRET`` are listed per plugin under
``secret_settings``: Waldur refuses to store them, and they stay in the agent's
own configuration file.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

from waldur_site_agent.common import plugin_schemas, structures, utils

if sys.version_info >= (3, 10):
    from importlib.metadata import version
else:
    from importlib_metadata import version

# Bumped when the shape of the document changes, so Waldur can refuse one it
# does not understand.
CATALOGUE_VERSION = 1

USERNAME_MANAGEMENT_ROLE = "username_management"


def secret_paths(schema: dict[str, Any]) -> list[str]:
    """Return the dotted paths of every property marked secret in a JSON Schema.

    Nested models are followed through ``$ref``; a dict of models contributes
    ``<field>.*`` and a list of them ``<field>[]``.
    """
    defs = schema.get("$defs", {})
    found: set[str] = set()

    def walk(node: object, path: str, seen: frozenset[str]) -> None:
        if not isinstance(node, dict):
            return
        if node.get("secret") is True and path:
            found.add(path)
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            name = ref.rsplit("/", 1)[-1]
            if name not in seen:
                walk(defs.get(name), path, seen | {name})
        for key in ("anyOf", "oneOf", "allOf"):
            for option in node.get(key, []):
                walk(option, path, seen)
        for name, prop in node.get("properties", {}).items():
            walk(prop, f"{path}.{name}" if path else name, seen)
        additional = node.get("additionalProperties")
        if isinstance(additional, dict):
            walk(additional, f"{path}.*", seen)
        items = node.get("items")
        if isinstance(items, dict):
            walk(items, f"{path}[]", seen)

    walk(schema, "", frozenset())
    return sorted(found)


def _schema_entry(schema_class: Optional[type]) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    if schema_class is None:
        return None, None
    try:
        return schema_class.model_json_schema(), None  # type: ignore[attr-defined]
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _plugin_entry(
    name: str,
    package: str,
    package_version: str,
    roles: list[str],
    settings_schemas: dict[str, Any],
    component_schemas: dict[str, Any],
) -> dict[str, Any]:
    settings_schema, settings_error = _schema_entry(settings_schemas.get(name))
    component_schema, component_error = _schema_entry(component_schemas.get(name))
    entry: dict[str, Any] = {
        "name": name,
        "package": package,
        "version": package_version,
        "roles": roles,
        "settings_schema": settings_schema,
        "component_schema": component_schema,
        "secret_settings": secret_paths(settings_schema) if settings_schema else [],
    }
    errors = {
        key: error
        for key, error in (
            ("settings_schema", settings_error),
            ("component_schema", component_error),
        )
        if error
    }
    if errors:
        entry["schema_errors"] = errors
    return entry


def build_catalogue() -> dict[str, Any]:
    """Build the catalogue of every backend installed alongside the agent."""
    settings_schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    component_schemas = plugin_schemas.get_plugin_component_schemas()

    plugins: dict[str, dict[str, Any]] = {}
    for name, (backend_class, package, package_version) in utils.BACKENDS.items():
        roles = [role.value for role in backend_class.supported_roles]
        plugins[name] = _plugin_entry(
            name, package, package_version, roles, settings_schemas, component_schemas
        )
    for name, (_, package, package_version) in utils.USERNAME_BACKENDS.items():
        if name in plugins:
            plugins[name]["roles"].append(USERNAME_MANAGEMENT_ROLE)
            continue
        plugins[name] = _plugin_entry(
            name,
            package,
            package_version,
            [USERNAME_MANAGEMENT_ROLE],
            settings_schemas,
            component_schemas,
        )

    offering_schema = structures.Offering.model_json_schema()
    return {
        "catalogue_version": CATALOGUE_VERSION,
        "agent_version": version("waldur-site-agent"),
        "offering_secret_fields": [
            path for path in secret_paths(offering_schema) if "." not in path
        ],
        "plugins": [plugins[name] for name in sorted(plugins)],
    }


def main(argv: Optional[list[str]] = None) -> None:
    """Write the plugin catalogue as JSON to a file or stdout."""
    parser = argparse.ArgumentParser(
        description="Export the installed site agent plugins as a catalogue for Waldur"
    )
    parser.add_argument(
        "-o", "--output", help="File to write the catalogue to (default: stdout)", default=None
    )
    args = parser.parse_args(argv)

    catalogue = build_catalogue()
    # Waldur refuses the secrets the catalogue lists, so a schema that failed to
    # load would publish a plugin with no secrets at all. Refuse to write it.
    broken = {
        plugin["name"]: plugin["schema_errors"]
        for plugin in catalogue["plugins"]
        if plugin.get("schema_errors")
    }
    if broken:
        sys.stderr.write(f"Plugin schemas failed to load: {json.dumps(broken, indent=2)}\n")
        sys.exit(1)

    document = json.dumps(catalogue, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(document, encoding="utf-8")
    else:
        sys.stdout.write(document)
