"""The STOMP messages the agent handles are declared by mastermind's contract.

The message types come with waldur-api-client (``waldur_api_client.stomp_messages``),
generated from the AsyncAPI document shipped next to them, so bumping the client
pin brings the REST and STOMP contracts of the same mastermind commit. mypy then
checks the handlers against the new types; these tests check the subscriptions.
"""

import re
import sys
from importlib import resources
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import yaml
from waldur_api_client import stomp_messages

from waldur_site_agent.event_processing.event_subscription_manager import (
    OBJECT_TYPE_TO_HANDLER_STOMP,
)

DOCUMENT = yaml.safe_load(
    resources.files("waldur_api_client").joinpath("asyncapi.yaml").read_text()
)
HANDLED_TYPES = {object_type.value for object_type in OBJECT_TYPE_TO_HANDLER_STOMP}


def test_every_handled_object_type_has_a_message():
    missing = HANDLED_TYPES - set(stomp_messages.MESSAGES_BY_OBJECT_TYPE)
    assert not missing, (
        f"The agent handles {sorted(missing)}, but mastermind's AsyncAPI document "
        "declares no message for them."
    )


def test_no_handled_object_type_is_declared_not_for_the_agent():
    excluded = set(DOCUMENT["info"].get("x-not-site-agent-object-types", {}))
    assert not HANDLED_TYPES & excluded


def test_mypy_hook_checks_against_the_pinned_client():
    """The mypy hook types the handlers with the client the agent runs with."""
    root = Path(__file__).resolve().parent.parent
    pins = [
        re.sub(r"\s+", "", dep)
        for dep in tomllib.loads((root / "pyproject.toml").read_text())["project"]["dependencies"]
        if dep.startswith("waldur-api-client")
    ]
    assert pins, "pyproject.toml must pin waldur-api-client"
    hooks = yaml.safe_load((root / ".pre-commit-config.yaml").read_text())
    (mypy_hook,) = [
        hook for repo in hooks["repos"] for hook in repo["hooks"] if hook["id"] == "mypy"
    ]
    hook_pins = [
        re.sub(r"\s+", "", dep)
        for dep in mypy_hook["additional_dependencies"]
        if dep.startswith("waldur-api-client")
    ]
    assert hook_pins == pins, (
        "Bump waldur-api-client in .pre-commit-config.yaml's mypy hook to match "
        "pyproject.toml, or mypy checks the handlers against another contract."
    )
