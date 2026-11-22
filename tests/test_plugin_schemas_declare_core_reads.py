"""Every backend setting the agent core reads is declared on each backend's schema.

The core reads some backend settings for any backend (prefixes, soft_delete,
backend id uniqueness). A plugin schema that does not declare them leaves them
out of the plugin catalogue, so Waldur treats them as unknown for that backend.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import waldur_site_agent
from waldur_site_agent.common import plugin_schemas, utils

PACKAGE = Path(waldur_site_agent.__file__).parent

# Settings the core reads only for one kind of backend, with that kind.
HOMEDIR_SETTINGS = set(plugin_schemas.HomedirSettingsSchema.model_fields) - set(
    plugin_schemas.CommonBackendSettingsSchema.model_fields
)
ONE_BACKEND = {
    "target_api_url": "waldur",
    "target_api_token": "waldur",
}


def _core_reads() -> dict[str, str]:
    """Keys the core reads as ``….backend_settings.get("key")`` or ``[...]``."""
    found: dict[str, str] = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            receiver = key = None
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                receiver, key = node.func.value, node.args[0].value
            elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
                receiver, key = node.value, node.slice.value
            name = None
            if isinstance(receiver, ast.Attribute):
                name = receiver.attr
            elif isinstance(receiver, ast.Name):
                name = receiver.id
            if name == "backend_settings" and isinstance(key, str):
                line = getattr(node, "lineno", 0)
                found.setdefault(key, f"{path.relative_to(PACKAGE)}:{line}")
    return found


def test_the_core_reads_are_found() -> None:
    reads = _core_reads()

    assert {"soft_delete", "check_backend_id_uniqueness", "allocation_prefix"} <= set(reads)


def test_every_backend_declares_the_settings_the_core_reads() -> None:
    settings_schemas = plugin_schemas.get_plugin_backend_settings_schemas()
    if not settings_schemas:
        pytest.skip("no plugins installed")
    reads = _core_reads()

    undeclared = []
    for name, (backend_class, _package, _version) in utils.BACKENDS.items():
        schema = settings_schemas.get(name)
        if schema is None:
            continue
        for key, where in reads.items():
            if key in ONE_BACKEND and ONE_BACKEND[key] != name:
                continue
            if key in HOMEDIR_SETTINGS and not backend_class.supports_user_homedirs:
                continue
            if key not in schema.model_fields:
                undeclared.append(f"{name}: {key} (read at {where})")

    assert not undeclared, (
        "Inherit CommonBackendSettingsSchema (or HomedirSettingsSchema), or declare: "
        f"{sorted(undeclared)}"
    )
