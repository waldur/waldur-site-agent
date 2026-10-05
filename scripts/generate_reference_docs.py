#!/usr/bin/env python3
"""Generate the reference sections of the docs from code, or check they are current.

Usage:
    uv run python scripts/generate_reference_docs.py          # rewrite generated sections
    uv run python scripts/generate_reference_docs.py --check  # exit 1 if anything is stale

Generated (rewritten between ``<!-- BEGIN GENERATED: name -->`` / ``<!-- END GENERATED:
name -->`` markers):

- ``docs/configuration.md``: one summary table per configuration model (global settings,
  ``log_shipping``, offering settings, component settings), built from the pydantic
  ``Field(description=...)`` definitions;
- ``README.md``: the plugin table (``scripts/generate_plugin_table.py``).

Checked only (the prose is hand-written; the check fails when it no longer matches the code):

- every configuration field has its own section in ``docs/configuration.md``;
- the backend tables in ``docs/configuration.md`` list exactly the entry points the plugins
  register;
- the environment-variable table in ``docs/configuration.md`` lists exactly the variables the
  agent reads, with the defaults the code uses;
- the console-script table in ``README.md`` lists exactly ``[project.scripts]``;
- every field of a plugin's settings schema is mentioned in that plugin's README.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from enum import Enum
from typing import Any, Callable, get_origin

from pydantic import BaseModel

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

CONFIGURATION_MD = ROOT / "docs" / "configuration.md"
README_MD = ROOT / "README.md"
PLUGINS_DIR = ROOT / "plugins"

# ---------------------------------------------------------------------------
# Marker handling
# ---------------------------------------------------------------------------


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def _marker_pattern(name: str) -> re.Pattern[str]:
    return re.compile(
        rf"(<!-- BEGIN GENERATED: {re.escape(name)} -->\n)(.*?)(<!-- END GENERATED: "
        rf"{re.escape(name)} -->)",
        re.DOTALL,
    )


def replace_section(text: str, name: str, body: str, path: Path) -> str:
    pattern = _marker_pattern(name)
    if not pattern.search(text):
        msg = f"{_rel(path)}: markers for generated section {name!r} not found"
        raise SystemExit(msg)
    return pattern.sub(lambda m: m.group(1) + body.rstrip("\n") + "\n" + m.group(3), text)


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _table(header: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
    ]
    lines += ["| " + " | ".join(_cell(c) for c in row) + " |" for row in rows]
    # Descriptions and anchors push some rows past pymarkdown's line limit.
    return f"<!-- pyml disable-num-lines {len(lines)} line-length -->\n" + "\n".join(lines)


# Fields documented together under one heading rather than a heading each.
COVERED_BY = {
    "backend_settings": "## Backend-Specific Settings",
    "backend_components": "## Backend Components",
    "order_processing_backend": "#### Backend Selection",
    "membership_sync_backend": "#### Backend Selection",
    "reporting_backend": "#### Backend Selection",
    "username_management_backend": "#### Backend Selection",
}


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------


def _type_name(annotation: Any) -> str:  # noqa: ANN401
    """Readable type: ``str``, ``int | None`` shown as ``int``, models by class name."""
    # On Python 3.9 a generic such as dict[str, Any] also passes isinstance(..., type).
    is_class = isinstance(annotation, type) and get_origin(annotation) is None
    if is_class and issubclass(annotation, Enum):
        return " | ".join(str(member.value) for member in annotation)
    if is_class:
        return annotation.__name__
    text = str(annotation)
    text = re.sub(r"typing\.|waldur_site_agent\.common\.structures\.|<class '|'>", "", text)
    for pattern in (r"Optional\[(.*)\]", r"Union\[(.*), NoneType\]", r"(.*) \| None"):
        match = re.fullmatch(pattern, text)
        if match:
            text = match.group(1)
    return text


def _default(field: Any) -> str:  # noqa: ANN401
    if field.is_required():
        return "—"
    if field.default_factory is not None:
        value = field.default_factory()
        if isinstance(value, BaseModel):
            return "see below"
        return "`{}`" if value == {} else ("`[]`" if value == [] else f"`{value!r}`")
    value = field.default
    if isinstance(value, BaseModel):
        return "see below"
    if value is None:
        return "—"
    if hasattr(value, "value"):
        value = value.value
    if value == "":
        return '`""`'
    if isinstance(value, bool):
        return f"`{str(value).lower()}`"
    return f"`{value}`"


def _slug(heading: str) -> str:
    """Anchor the docs site gives a heading (Python-Markdown toc slugify)."""
    text = re.sub(r"[^\w\s-]", "", heading.lstrip("#").strip()).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def _anchor_for(name: str, headings: list[str]) -> str | None:
    covered = COVERED_BY.get(name)
    for heading in headings:
        if f"`{name}`" in heading or (covered and heading == covered):
            return _slug(heading)
    return None


def model_table(model: Any, skip: tuple[str, ...] = (), link: bool = True) -> str:  # noqa: ANN401
    headings = [line for line in CONFIGURATION_MD.read_text().splitlines() if line.startswith("#")]
    rows = []
    for name, field in model.model_fields.items():
        if name in skip:
            continue
        anchor = _anchor_for(name, headings) if link else None
        key = f"[`{name}`](#{anchor})" if anchor else f"`{name}`"
        rows.append(
            [
                key,
                f"`{_type_name(field.annotation)}`",
                "yes" if field.is_required() else "no",
                _default(field),
                field.description or "",
            ]
        )
    return _table(["Key", "Type", "Required", "Default", "Description"], rows)


def _models() -> dict[str, Any]:
    from waldur_site_agent.common import structures  # noqa: PLC0415

    return {
        "root": structures.RootConfiguration,
        "log_shipping": structures.LogShippingConfig,
        "offering": structures.Offering,
        "component": structures.BackendComponent,
    }


def configuration_sections() -> dict[str, str]:
    models = _models()
    return {
        "global-settings": model_table(models["root"], skip=("offerings",)),
        "log-shipping-settings": model_table(models["log_shipping"], link=False),
        "offering-settings": model_table(models["offering"]),
        "component-settings": model_table(models["component"]),
    }


# ---------------------------------------------------------------------------
# Backends from plugin entry points
# ---------------------------------------------------------------------------


def _plugin_projects() -> list[dict[str, Any]]:
    projects = []
    for pyproject in sorted(PLUGINS_DIR.glob("*/pyproject.toml")):
        data = tomllib.loads(pyproject.read_text())
        project = data.get("project", {})
        project["_dir"] = pyproject.parent.name
        projects.append(project)
    return projects


def _registered_names(group: str) -> set[str]:
    names: set[str] = set()
    for project in _plugin_projects():
        names |= set(project.get("entry-points", {}).get(group, {}))
    return names


# ---------------------------------------------------------------------------
# Checks on hand-written sections
# ---------------------------------------------------------------------------


def check_field_sections() -> list[str]:
    """Every configuration field has a section in configuration.md."""
    text = CONFIGURATION_MD.read_text()
    headings = "\n".join(line for line in text.splitlines() if line.startswith("#"))
    problems = []
    models = _models()
    for label, model in (
        ("global setting", models["root"]),
        ("offering setting", models["offering"]),
        ("component setting", models["component"]),
    ):
        for name in model.model_fields:
            if name == "offerings":
                continue
            covered = COVERED_BY.get(name)
            if covered and covered in headings:
                continue
            if f"`{name}`" not in headings:
                problems.append(f"docs/configuration.md: {label} `{name}` has no section")
    return problems


def _env_vars_from_code() -> dict[str, str]:
    """Variables read with os.environ.get / os.getenv and their defaults."""
    found: dict[str, str] = {}
    for path in (
        ROOT / "waldur_site_agent" / "common" / "__init__.py",
        ROOT / "waldur_site_agent" / "common" / "healthz.py",
    ):
        tree = ast.parse(path.read_text())
        constants = {
            node.targets[0].id: node.value.value
            for node in tree.body
            if isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        }
        or_defaults: dict[int, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or) and len(node.values) == 2:
                fallback = node.values[1]
                if isinstance(fallback, ast.Constant) and isinstance(fallback.value, str):
                    or_defaults[id(node.values[0])] = fallback.value
                elif isinstance(fallback, ast.Name) and fallback.id in constants:
                    or_defaults[id(node.values[0])] = constants[fallback.id]
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 1:
                continue
            func = node.func
            is_get = (
                isinstance(func, ast.Attribute)
                and func.attr in ("get", "getenv")
                and (
                    (isinstance(func.value, ast.Attribute) and func.value.attr == "environ")
                    or (isinstance(func.value, ast.Name) and func.value.id == "os")
                )
            )
            if not is_get:
                continue
            name_node = node.args[0]
            if isinstance(name_node, ast.Constant):
                name = name_node.value
            elif isinstance(name_node, ast.Name) and name_node.id in constants:
                name = constants[name_node.id]
            else:
                continue
            default = or_defaults.get(id(node), "")
            if len(node.args) > 1:
                default_node = node.args[1]
                if isinstance(default_node, ast.Constant):
                    default = str(default_node.value)
                elif isinstance(default_node, ast.Name) and default_node.id in constants:
                    default = constants[default_node.id]
            if isinstance(name, str) and name.startswith("WALDUR_SITE_AGENT_"):
                found[name] = default
    return found


def check_env_vars() -> list[str]:
    text = CONFIGURATION_MD.read_text()
    section = text.split("## Environment Variables", 1)[-1].split("\n## ", 1)[0]
    documented = {
        m.group(1): m.group(2)
        for m in re.finditer(r"^\| `(WALDUR_SITE_AGENT_[A-Z_]+)` \| `([^`]*)` \|", section, re.M)
    }
    code = _env_vars_from_code()
    problems = [
        f"docs/configuration.md: `{n}` is read by the agent but missing from Environment Variables"
        for n in sorted(set(code) - set(documented))
    ]
    problems += [
        f"docs/configuration.md: `{n}` is documented but the agent does not read it"
        for n in sorted(set(documented) - set(code))
    ]
    problems += [
        f"docs/configuration.md: `{n}` default is `{documented[n]}`, the code uses `{code[n]}`"
        for n in sorted(set(code) & set(documented))
        if documented[n] != code[n]
    ]
    return problems


def check_backends() -> list[str]:
    """The backend tables in configuration.md list exactly the registered entry points."""
    text = CONFIGURATION_MD.read_text()
    problems = []
    processing = text.split("**Processing backends**", 1)[-1].split("**Username management", 1)[0]
    documented = set()
    for line in processing.splitlines():
        if line.startswith("| `"):
            documented |= set(re.findall(r"`([^`]+)`", line.split("|")[1]))
    username = text.split("**Username management backends**", 1)[-1].split("\n\n", 1)[0]
    documented_username = set(re.findall(r"`([a-z0-9_-]+)`\s*\(`", username))
    for group, docs, label in (
        ("waldur_site_agent.backends", documented, "processing backend"),
        ("waldur_site_agent.username_management_backends", documented_username, "username backend"),
    ):
        registered = _registered_names(group)
        problems += [
            f"docs/configuration.md: {label} `{n}` is registered but not listed"
            for n in sorted(registered - docs)
        ]
        problems += [
            f"docs/configuration.md: {label} `{n}` is listed but no plugin registers it"
            for n in sorted(docs - registered)
        ]
    return problems


def check_console_scripts() -> list[str]:
    scripts = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])
    documented = set(re.findall(r"^\| `(waldur_[a-z_]+)` \|", README_MD.read_text(), re.M))
    problems = [
        f"README.md: console script `{s}` is missing from the commands table"
        for s in sorted(scripts - documented)
    ]
    problems += [
        f"README.md: `{s}` is listed as a command but is not in [project.scripts]"
        for s in sorted(documented - scripts)
    ]
    return problems


def check_plugin_settings() -> list[str]:
    if sys.version_info >= (3, 10):
        from importlib.metadata import entry_points  # noqa: PLC0415
    else:
        from importlib_metadata import entry_points  # noqa: PLC0415

    from waldur_site_agent.common.plugin_schemas import (  # noqa: PLC0415
        CommonBackendSettingsSchema,
    )

    common = set(CommonBackendSettingsSchema.model_fields)
    problems = []
    seen: set[str] = set()
    for ep in entry_points(group="waldur_site_agent.backend_settings_schemas"):
        dist = ep.dist.name if ep.dist else ""
        plugin_dir = next(
            (p["_dir"] for p in _plugin_projects() if p.get("name") == dist), None
        )
        if plugin_dir is None:
            continue
        schema = ep.load()
        readme = PLUGINS_DIR / plugin_dir / "README.md"
        text = readme.read_text() if readme.exists() else ""
        for name in schema.model_fields:
            key = f"{plugin_dir}:{name}"
            if name in common or key in seen:
                continue
            seen.add(key)
            if f"`{name}`" not in text and not re.search(
                rf"^\s*{re.escape(name)}:", text, re.M
            ):
                problems.append(
                    f"plugins/{plugin_dir}/README.md: setting `{name}` of {schema.__name__} "
                    "is not documented"
                )
    return problems


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _plugin_table_update(readme: str) -> str:
    import generate_plugin_table  # noqa: PLC0415

    return generate_plugin_table.render(readme)


def run(check: bool) -> int:
    targets: list[tuple[Path, Callable[[str], str]]] = [
        (
            CONFIGURATION_MD,
            lambda text: _apply(text, configuration_sections(), CONFIGURATION_MD),
        ),
        (README_MD, _plugin_table_update),
    ]
    stale = []
    for path, update in targets:
        current = path.read_text()
        new = update(current)
        if new != current:
            stale.append(path)
            if not check:
                path.write_text(new)

    problems = (
        check_field_sections()
        + check_backends()
        + check_env_vars()
        + check_console_scripts()
        + check_plugin_settings()
    )
    for path in stale:
        verb = "is stale" if check else "updated"
        print(f"{_rel(path)}: generated sections {verb}")
    for problem in problems:
        print(problem)
    if (check and stale) or problems:
        if check and stale:
            print("Run: uv run python scripts/generate_reference_docs.py")
        return 1
    return 0


def _apply(text: str, sections: dict[str, str], path: Path) -> str:
    for name, body in sections.items():
        text = replace_section(text, name, body, path)
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="fail instead of rewriting")
    raise SystemExit(run(check=parser.parse_args().check))


if __name__ == "__main__":
    main()
