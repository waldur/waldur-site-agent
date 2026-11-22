"""Every backend setting the SLURM plugin reads is declared on its settings schema.

The schema is open (``extra="allow"``), so a key the code reads but the schema
does not declare is accepted unchecked, missing from the plugin catalogue, and
so neither offered by Waldur nor marked secret or sensitive there.
"""

from __future__ import annotations

import ast
from pathlib import Path

import waldur_site_agent_slurm
from waldur_site_agent_slurm.schemas import (
    ProjectDirectoryConfig,
    SlurmBackendSettingsSchema,
    SlurmRestApiConfig,
)

PACKAGE = Path(waldur_site_agent_slurm.__file__).parent

# Expressions the plugin reads a block of settings from, by the source text of
# the receiver, and the schema that declares that block.
RECEIVERS = {
    "backend_settings": SlurmBackendSettingsSchema,
    "self.backend_settings": SlurmBackendSettingsSchema,
    "self._project_dir_config": ProjectDirectoryConfig,
    "rest_settings": SlurmRestApiConfig,
}


def _reads() -> dict[str, set[tuple[str, str]]]:
    """Keys read as ``<receiver>.get("key")`` or ``<receiver>["key"]``, by receiver."""
    found: dict[str, set[tuple[str, str]]] = {receiver: set() for receiver in RECEIVERS}
    for path in sorted(PACKAGE.glob("*.py")):
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
                receiver, key = ast.unparse(node.func.value), node.args[0].value
            elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
                receiver, key = ast.unparse(node.value), node.slice.value
            if receiver in found and isinstance(key, str):
                found[receiver].add((key, f"{path.name}:{node.lineno}"))
    return found


def test_every_read_setting_is_declared() -> None:
    undeclared = sorted(
        f"{receiver}[{key!r}] at {where}"
        for receiver, reads in _reads().items()
        for key, where in reads
        if key not in RECEIVERS[receiver].model_fields
    )

    assert not undeclared, f"Declare these on the settings schema: {undeclared}"


def test_the_reads_are_found() -> None:
    reads = {receiver: {key for key, _ in keys} for receiver, keys in _reads().items()}

    assert {"slurm_bin_path", "execution_mode", "ldap"} <= reads["backend_settings"] | reads[
        "self.backend_settings"
    ]
    assert {"group_owner_source", "group_name", "owner"} <= reads["self._project_dir_config"]
    assert {"url", "username", "token_file"} <= reads["rest_settings"]
