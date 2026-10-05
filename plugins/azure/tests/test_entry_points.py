"""The backend and schema names operators put in their configuration."""

import sys

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points


def _names(group):
    return {ep.name for ep in entry_points(group=group)}


def test_azure_is_a_backend():
    assert "azure" in _names("waldur_site_agent.backends")


def test_azure_registers_its_configuration_schemas():
    """Without these, a malformed offering is only caught at the first API call."""
    assert "azure" in _names("waldur_site_agent.backend_settings_schemas")
    assert "azure" in _names("waldur_site_agent.component_schemas")
