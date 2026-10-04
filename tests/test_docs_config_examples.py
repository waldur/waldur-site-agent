"""Every agent configuration example in the docs must load and do something.

Config examples are the first thing an operator copies, and they drifted: blocks
missing a required key, ``offerings`` written as a mapping, settings under keys the
agent never reads (pydantic ignores unknown keys, so those load "fine" and are
silently dropped), and backend names that are not registered entry points, which
load but leave the agent doing nothing.

This module collects every fenced ```yaml block that contains ``offerings:`` from
the repository's Markdown, plus the example config files, and checks each one
against the real configuration loader. A deliberately partial snippet opts out with
an HTML comment on the line before its fence (a blank line in between is allowed)::

    <!-- docs-check: skip -->

    ```yaml
    offerings:
      - backend_settings: ...
    ```
"""

from __future__ import annotations

import logging
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, List, Optional

import pytest
import yaml

from waldur_site_agent.common import structures

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:  # pragma: no cover - Python 3.9 CI job
    from importlib_metadata import entry_points

REPO_ROOT = Path(__file__).resolve().parents[1]

SKIP_MARKER = "docs-check: skip"
DUMMY_TOKEN = "docs-check-dummy-token"  # noqa: S105 - placeholder, never sent anywhere
ROLE_KEYS = ("order_processing_backend", "membership_sync_backend", "reporting_backend")

MARKDOWN_GLOBS = (
    "README.md",
    "docs/**/*.md",
    "plugins/*/README.md",
    "plugins/*/docs/**/*.md",
    "helm/**/README.md",
)
EXAMPLE_GLOBS = ("examples/*.y*ml*", "plugins/*/examples/*.y*ml*")

FENCE = re.compile(r"^(?P<indent>[ \t]*)```ya?ml[ \t]*\n(?P<body>.*?)^(?P=indent)```", re.M | re.S)


def _entry_point_names(group: str) -> set[str]:
    return {ep.name for ep in entry_points(group=group)}


BACKENDS = _entry_point_names("waldur_site_agent.backends")
USERNAME_BACKENDS = _entry_point_names("waldur_site_agent.username_management_backends")


@dataclass
class Example:
    """One config example: where it lives, its YAML text, and why it is skipped (if)."""

    location: str
    text: str
    skip_reason: Optional[str] = None


def _skip_marked(text_before_fence: str) -> bool:
    """True when the last non-blank line before a fence is the skip marker."""
    for line in reversed(text_before_fence.splitlines()):
        if line.strip():
            return SKIP_MARKER in line
    return False


def markdown_examples(path: Path, root: Path) -> Iterator[Example]:
    """Yield the ```yaml blocks of a Markdown file that contain ``offerings:``."""
    text = path.read_text(encoding="utf-8")
    for match in FENCE.finditer(text):
        body = textwrap.dedent(match.group("body"))
        if "offerings:" not in body:
            continue
        line = text[: match.start()].count("\n") + 1
        location = f"{path.relative_to(root)}:{line}"
        skip = "marked <!-- docs-check: skip -->" if _skip_marked(text[: match.start()]) else None
        yield Example(location, body, skip)


def file_examples(path: Path, root: Path) -> Iterator[Example]:
    """Yield each YAML document of an example config file."""
    text = path.read_text(encoding="utf-8")
    documents = text.split("\n---\n") if "\n---\n" in text else [text]
    for index, document in enumerate(documents):
        suffix = f"#{index + 1}" if len(documents) > 1 else ""
        yield Example(f"{path.relative_to(root)}{suffix}", document)


def collect_examples(root: Path) -> List[Example]:
    """All config examples under ``root``, in a stable order."""
    examples: List[Example] = []
    markdown = sorted({p for pattern in MARKDOWN_GLOBS for p in root.glob(pattern)})
    for path in markdown:
        examples.extend(markdown_examples(path, root))
    files = sorted({p for pattern in EXAMPLE_GLOBS for p in root.glob(pattern) if p.is_file()})
    for path in files:
        examples.extend(file_examples(path, root))
    return examples


def _unknown_keys(raw: dict, model: type, where: str, anchors: bool = False) -> List[str]:
    """Keys the model does not define; pydantic would ignore them without a word.

    At the top level a key starting with ``.`` is allowed as a YAML anchor holder
    (``.ldap: &ldap_settings``), the docker-compose convention for shared blocks.
    """
    known = set(model.model_fields)  # type: ignore[attr-defined]
    return [
        f"{where}: unknown key {key!r}"
        for key in raw
        if key not in known and not (anchors and str(key).startswith("."))
    ]


class _SchemaWarnings(logging.Handler):
    """Collect the plugin settings-schema warnings the loader logs."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        # The agent logs through structlog, which hands the stdlib record a dict.
        message = record.msg.get("event", "") if isinstance(record.msg, dict) else record.getMessage()
        if "schema validation failed" in message.lower():
            self.messages.append(" ".join(message.split())[:400])


def config_problems(text: str) -> Optional[List[str]]:
    """Problems with one config example; ``None`` when it is not an agent config.

    An example without a top-level ``offerings`` key (a Kubernetes manifest in an
    examples directory, say) is not an agent config and is not checked.
    """
    raw: Any = yaml.safe_load(text)
    if not isinstance(raw, dict) or "offerings" not in raw:
        return None
    problems: List[str] = []
    offerings = raw["offerings"]
    if not isinstance(offerings, list):
        return ["`offerings` must be a list of offerings"]

    problems += _unknown_keys(raw, structures.RootConfiguration, "top level", anchors=True)
    if isinstance(raw.get("log_shipping"), dict):
        problems += _unknown_keys(raw["log_shipping"], structures.LogShippingConfig, "log_shipping")
    for offering in offerings:
        if not isinstance(offering, dict):
            problems.append("every offering must be a mapping")
            continue
        name = offering.get("name", "<unnamed>")
        problems += _unknown_keys(offering, structures.Offering, f"offering {name!r}")
        if not offering.get("waldur_api_token") and not offering.get("oidc_token_url"):
            offering["waldur_api_token"] = DUMMY_TOKEN  # docs show placeholders for secrets
        backend_type = offering.get("backend_type")
        if backend_type and backend_type not in BACKENDS:
            problems.append(f"offering {name!r}: backend_type {backend_type!r} is not a registered backend")
        roles = {key: offering.get(key) for key in ROLE_KEYS}
        if not any(roles.values()):
            problems.append(f"offering {name!r}: no *_backend set, the agent would do nothing")
        for key, value in roles.items():
            if value and value not in BACKENDS:
                problems.append(f"offering {name!r}: {key} {value!r} is not a registered backend")
        username_backend = offering.get("username_management_backend")
        if username_backend and username_backend not in USERNAME_BACKENDS:
            problems.append(
                f"offering {name!r}: username_management_backend {username_backend!r} "
                "is not a registered username backend"
            )

    warnings = _SchemaWarnings()
    root_logger = logging.getLogger()
    root_logger.addHandler(warnings)
    try:
        structures.RootConfiguration(**raw).to_agent_configuration()
    except Exception as exc:  # noqa: BLE001 - every loader failure is a finding here
        problems.append(f"does not load: {' '.join(str(exc).split())}")
    finally:
        root_logger.removeHandler(warnings)
    problems += [f"settings schema: {message}" for message in warnings.messages]
    return problems


EXAMPLES = collect_examples(REPO_ROOT)


@pytest.mark.parametrize("example", EXAMPLES, ids=[example.location for example in EXAMPLES])
def test_config_example_loads(example: Example) -> None:
    """The example loads through the real loader, uses known keys and registered backends."""
    if example.skip_reason:
        pytest.skip(example.skip_reason)
    try:
        problems = config_problems(example.text)
    except yaml.YAMLError as exc:
        pytest.fail(f"{example.location}: invalid YAML: {exc}")
    if problems is None:
        pytest.skip("not an agent configuration (no top-level `offerings`)")
    assert not problems, f"{example.location}:\n  " + "\n  ".join(problems)


def test_examples_are_found() -> None:
    """Guard against the collector silently matching nothing."""
    checked = [example for example in EXAMPLES if not example.skip_reason]
    assert len(checked) >= 20, f"only {len(checked)} config examples found"


# --- The checker itself -------------------------------------------------------

VALID = """
offerings:
  - name: Example
    waldur_api_url: https://waldur.example.com/api/
    waldur_offering_uuid: "00000000000000000000000000000000"
    backend_type: slurm
    order_processing_backend: slurm
    backend_settings:
      default_account: root
      customer_prefix: c_
      project_prefix: p_
      allocation_prefix: a_
    backend_components:
      cpu: {limit: 10, measured_unit: k-Hours, unit_factor: 60000, accounting_type: limit, label: CPU}
"""


def test_checker_accepts_a_complete_example() -> None:
    assert config_problems(VALID) == []


def test_checker_reports_a_missing_backend_type() -> None:
    problems = config_problems(VALID.replace("    backend_type: slurm\n", ""))
    assert problems and any("backend_type" in p and "does not load" in p for p in problems)


def test_checker_reports_an_unknown_key() -> None:
    problems = config_problems(VALID.replace("    backend_settings:", "    backend:\n      a: 1\n    backend_settings:"))
    assert problems and any("unknown key 'backend'" in p for p in problems)


def test_checker_reports_an_unregistered_backend() -> None:
    problems = config_problems(VALID.replace("order_processing_backend: slurm", "order_processing_backend: cscs-dwdi"))
    assert problems and any("'cscs-dwdi' is not a registered backend" in p for p in problems)


def test_checker_reports_offerings_as_a_mapping() -> None:
    assert config_problems("offerings:\n  my-offering:\n    name: x\n") == ["`offerings` must be a list of offerings"]


def test_collector_honours_the_skip_marker(tmp_path: Path) -> None:
    doc = tmp_path / "docs" / "page.md"
    doc.parent.mkdir()
    doc.write_text(
        "```yaml\n" + VALID.lstrip() + "```\n\n<!-- docs-check: skip -->\n\n```yaml\nofferings:\n  - x: 1\n```\n",
        encoding="utf-8",
    )
    found = collect_examples(tmp_path)
    assert [e.location for e in found] == ["docs/page.md:1", "docs/page.md:19"]
    assert found[0].skip_reason is None
    assert found[1].skip_reason


def test_collector_reads_indented_fences_and_multi_document_files(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text(
        "1. Step\n\n   ```yaml\n" + textwrap.indent(VALID.lstrip(), "   ") + "   ```\n", encoding="utf-8"
    )
    (tmp_path / "examples").mkdir()
    (tmp_path / "examples" / "two.yaml").write_text(VALID + "\n---\nkind: ConfigMap\n", encoding="utf-8")
    found = collect_examples(tmp_path)
    assert [e.location for e in found] == ["README.md:3", "examples/two.yaml#1", "examples/two.yaml#2"]
    assert config_problems(found[0].text) == []
    assert config_problems(found[2].text) is None
