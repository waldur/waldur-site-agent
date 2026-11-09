"""The order-form switches: quota management and the Pod Security level."""

from __future__ import annotations

import sys

import pytest
from waldur_api_client.models.resource_attributes import ResourceAttributes

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_k8s_ut_namespace.backend import (
    K8sNamespaceBackend,
    K8sUtNamespaceBackend,
)

from conftest import make_backend

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points

CR_NAME = "waldur-test-ns"


def _prepare(settings, components, resource, attributes=None, backend_class=None):
    """Create a backend whose clients are mocked, ready to create the resource.

    ``attributes`` is what the order form recorded; leaving it out is the order
    that named neither switch, which is what an old resource looks like.
    """
    kwargs = {"backend_class": backend_class} if backend_class else {}
    backend, mock_k8s, mock_kc = make_backend(settings, components, **kwargs)
    if attributes is not None:
        resource.attributes = ResourceAttributes.from_dict(attributes)

    mock_kc.get_group_by_name.return_value = None
    mock_kc.create_group.side_effect = ["g-admin", "g-rw", "g-ro"]
    mock_k8s.create_managed_namespace.return_value = {"metadata": {"name": CR_NAME}}
    return backend, mock_k8s, mock_kc


def _created_spec(settings, components, resource, attributes=None, backend_class=None):
    """Create the resource and return the spec handed to the CR."""
    backend, mock_k8s, _ = _prepare(settings, components, resource, attributes, backend_class)
    backend.create_resource(resource)
    return mock_k8s.create_managed_namespace.call_args[0][1]


class TestQuotaEnabledSwitch:
    """``spec.quota.enabled`` is always written, and says what the order meant."""

    def test_quota_managed_by_default(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["enabled"] is True

    def test_enabled_is_sent_explicitly(
        self, backend_settings, backend_components, waldur_resource
    ):
        """The CRD defaults the field to true, so omitting it would re-enable it."""
        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert "enabled" in spec["quota"]

    def test_attribute_turns_quota_off(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False},
        )

        assert spec["quota"]["enabled"] is False

    def test_string_false_attribute_turns_quota_off(
        self, backend_settings, backend_components, waldur_resource
    ):
        """A non-empty string is truthy, so ``"false"`` must not enable the quota."""
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "false"},
        )

        assert spec["quota"]["enabled"] is False

    def test_setting_turns_quota_off(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["enabled"] is False

    def test_attribute_overrides_setting(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": True},
        )

        assert spec["quota"]["enabled"] is True

    def test_null_attribute_falls_back_to_setting(
        self, backend_settings, backend_components, waldur_resource
    ):
        """An untouched form field arrives as null, not as "off"."""
        backend_settings["quota_enabled"] = False

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": None},
        )

        assert spec["quota"]["enabled"] is False

    def test_quantities_sent_when_quota_disabled(
        self, backend_settings, backend_components, waldur_resource
    ):
        """The entitlement is still recorded; only its enforcement is switched off."""
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False},
        )

        assert spec["quota"]["cpu"] == "4"
        assert spec["quota"]["memory"] == "8Gi"

    def test_invalid_attribute_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend, mock_k8s, mock_kc = _prepare(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "maybe"},
        )

        with pytest.raises(BackendError, match="quota_enabled"):
            backend.create_resource(waldur_resource)

        mock_k8s.create_managed_namespace.assert_not_called()

    def test_invalid_attribute_leaves_no_groups(
        self, backend_settings, backend_components, waldur_resource
    ):
        """A refused order must not leave Keycloak groups behind."""
        backend, _, mock_kc = _prepare(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "maybe"},
        )

        with pytest.raises(BackendError):
            backend.create_resource(waldur_resource)

        mock_kc.create_group.assert_not_called()

    def test_invalid_setting_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = "sometimes"
        backend, _, _ = _prepare(backend_settings, backend_components, waldur_resource)

        with pytest.raises(BackendError, match="quota_enabled"):
            backend.create_resource(waldur_resource)


class TestSecurityLevel:
    """``spec.security.level`` is written only when a level was resolved."""

    @pytest.mark.parametrize("level", ["privileged", "baseline", "restricted"])
    def test_attribute_sets_level(
        self, backend_settings, backend_components, waldur_resource, level
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": level},
        )

        assert spec["security"] == {"level": level}

    def test_level_is_normalised(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "  Restricted "},
        )

        assert spec["security"] == {"level": "restricted"}

    def test_setting_sets_level(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "baseline"

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["security"] == {"level": "baseline"}

    def test_attribute_overrides_setting(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "privileged"

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "restricted"},
        )

        assert spec["security"] == {"level": "restricted"}

    def test_no_level_leaves_security_out(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Unset means the operator's own default applies, which the agent must not guess."""
        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert "security" not in spec

    def test_empty_attribute_leaves_security_out(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": ""},
        )

        assert "security" not in spec

    def test_invalid_attribute_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend, mock_k8s, mock_kc = _prepare(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "unconfined"},
        )

        with pytest.raises(BackendError) as excinfo:
            backend.create_resource(waldur_resource)

        message = str(excinfo.value)
        assert "security_level" in message
        assert "privileged" in message
        assert "baseline" in message
        assert "restricted" in message
        mock_k8s.create_managed_namespace.assert_not_called()
        mock_kc.create_group.assert_not_called()

    def test_invalid_setting_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "unconfined"
        backend, _, _ = _prepare(backend_settings, backend_components, waldur_resource)

        with pytest.raises(BackendError, match="default_security_level"):
            backend.create_resource(waldur_resource)


class TestSecurityLevelMetadata:
    """``status.securityLevel`` is the level actually in force."""

    def test_status_level_surfaced(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}},
            "status": {"securityLevel": "restricted"},
        }

        metadata = backend.get_resource_metadata(CR_NAME)

        assert metadata["security_level"] == "restricted"

    def test_absent_status_level_is_not_reported(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}},
            "status": {},
        }

        metadata = backend.get_resource_metadata(CR_NAME)

        assert "security_level" not in metadata


class TestK8sNamespaceAlias:
    """The deployment-neutral name resolves to the same backend."""

    def _names(self, group):
        return {ep.name for ep in entry_points(group=group)}

    def test_alias_is_a_backend(self):
        assert "k8s-namespace" in self._names("waldur_site_agent.backends")

    def test_alias_has_settings_schema(self):
        assert "k8s-namespace" in self._names("waldur_site_agent.backend_settings_schemas")

    def test_original_name_still_registered(self):
        assert "k8s-ut-namespace" in self._names("waldur_site_agent.backends")

    def test_alias_loads_alias_class(self):
        loaded = {
            ep.name: ep.load() for ep in entry_points(group="waldur_site_agent.backends")
        }

        assert loaded["k8s-namespace"] is K8sNamespaceBackend
        assert loaded["k8s-ut-namespace"] is K8sUtNamespaceBackend

    def test_both_names_share_one_schema(self):
        schemas = {
            ep.name: ep.load()
            for ep in entry_points(group="waldur_site_agent.backend_settings_schemas")
        }

        assert schemas["k8s-namespace"] is schemas["k8s-ut-namespace"]

    def test_alias_reports_its_own_backend_type(
        self, backend_settings, backend_components
    ):
        backend, _, _ = make_backend(
            backend_settings, backend_components, backend_class=K8sNamespaceBackend
        )

        assert backend.backend_type == "k8s-namespace"

    def test_alias_behaves_like_the_parent(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False, "security_level": "baseline"},
            backend_class=K8sNamespaceBackend,
        )

        assert spec["quota"]["enabled"] is False
        assert spec["security"] == {"level": "baseline"}
