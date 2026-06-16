"""Tests for backend_id (allocation/account name) generation in BaseBackend.

The agent derives SLURM-style names by prepending an offering-configured prefix
to the Waldur-supplied slug:

- resource (allocation) name: ``allocation_prefix`` + resource slug, lowercased
- project account name:        ``project_prefix`` + project slug
- customer account name:       ``customer_prefix`` + customer slug

These tests pin that mapping and confirm that different offering settings
(prefixes) produce different names for the same slug.
"""

from unittest.mock import MagicMock

import pytest

from waldur_site_agent.backend.backends import BaseBackend


class ConcreteBackend(BaseBackend):
    """Minimal concrete subclass to exercise the generic name helpers."""

    def ping(self, raise_exception=False):
        return True

    def list_backend_components(self):
        return []

    def list_components(self):
        return []

    def _get_usage_report(self, resource_backend_ids):
        return {}

    def _collect_resource_limits(self, waldur_resource):
        return {}, {}

    def _pre_create_resource(self, *args, **kwargs):
        pass

    def diagnostics(self):
        return {}

    def downscale_resource(self, *args, **kwargs):
        pass

    def get_resource_metadata(self, *args, **kwargs):
        return {}

    def pause_resource(self, *args, **kwargs):
        pass

    def restore_resource(self, *args, **kwargs):
        pass


def _make_backend(settings):
    backend = ConcreteBackend(backend_settings=settings, backend_components={})
    backend.client = MagicMock()
    return backend


class TestResourceBackendId:
    @pytest.mark.parametrize(
        ("allocation_prefix", "slug", "expected"),
        [
            ("hpc_", "sample-resource-1", "hpc_sample-resource-1"),
            ("alloc_", "sample-resource-1", "alloc_sample-resource-1"),
            ("", "sample-resource-1", "sample-resource-1"),
            # The resource backend_id is lowercased.
            ("HPC_", "Sample-Resource", "hpc_sample-resource"),
        ],
    )
    def test_prefix_and_slug(self, allocation_prefix, slug, expected):
        backend = _make_backend({"allocation_prefix": allocation_prefix})
        assert backend._get_resource_backend_id(slug) == expected

    def test_missing_prefix_setting_defaults_to_empty(self):
        backend = _make_backend({})
        assert backend._get_resource_backend_id("sample-resource-1") == "sample-resource-1"

    def test_different_prefixes_produce_different_names(self):
        slug = "sample-resource-1"
        a = _make_backend({"allocation_prefix": "hpc_"})
        b = _make_backend({"allocation_prefix": "lab_"})
        assert a._get_resource_backend_id(slug) != b._get_resource_backend_id(slug)


class TestProjectBackendId:
    @pytest.mark.parametrize(
        ("project_prefix", "slug", "expected"),
        [
            ("hpc_", "project-1", "hpc_project-1"),
            ("p_", "project-1", "p_project-1"),
            ("", "project-1", "project-1"),
        ],
    )
    def test_prefix_and_slug(self, project_prefix, slug, expected):
        backend = _make_backend({"project_prefix": project_prefix})
        assert backend._get_project_backend_id(slug) == expected

    def test_missing_prefix_setting_defaults_to_empty(self):
        backend = _make_backend({})
        assert backend._get_project_backend_id("project-1") == "project-1"


class TestCustomerBackendId:
    @pytest.mark.parametrize(
        ("customer_prefix", "slug", "expected"),
        [
            ("hpc_", "org-b", "hpc_org-b"),
            ("org_", "org-b", "org_org-b"),
            ("", "org-b", "org-b"),
        ],
    )
    def test_prefix_and_slug(self, customer_prefix, slug, expected):
        backend = _make_backend({"customer_prefix": customer_prefix})
        assert backend._get_customer_backend_id(slug) == expected

    def test_missing_prefix_setting_defaults_to_empty(self):
        backend = _make_backend({})
        assert backend._get_customer_backend_id("org-b") == "org-b"


def test_prefixes_are_independent_per_scope():
    """A single offering's three prefixes map each scope to a distinct name."""
    backend = _make_backend(
        {
            "allocation_prefix": "a_",
            "project_prefix": "p_",
            "customer_prefix": "c_",
        }
    )
    assert backend._get_resource_backend_id("x") == "a_x"
    assert backend._get_project_backend_id("x") == "p_x"
    assert backend._get_customer_backend_id("x") == "c_x"
