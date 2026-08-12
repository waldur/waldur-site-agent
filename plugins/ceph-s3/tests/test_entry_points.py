"""The backend names operators put in their configuration."""

import sys

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points


def _backends():
    return {ep.name: ep for ep in entry_points(group="waldur_site_agent.backends")}


def test_ceph_s3_is_the_management_backend():
    assert "ceph_s3" in _backends()


def test_croit_usage_is_the_reporting_backend():
    """Metering is a separate backend because only croit can do it."""
    backends = _backends()
    assert "croit_usage" in backends
    assert backends["croit_usage"].load() is not backends["ceph_s3"].load()


def test_the_pre_rename_name_is_gone():
    """No deployment carries croit_s3, so nothing needs it kept alive.

    Leaving an alias would suggest a migration path that has no users, and
    hide a stale configuration behind a name that still resolves.
    """
    assert "croit_s3" not in _backends()
