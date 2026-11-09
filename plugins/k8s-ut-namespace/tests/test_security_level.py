"""The Pod Security level: ``spec.security.level``, gated per offering."""

from __future__ import annotations

import logging

import pytest
from waldur_api_client.models.resource_attributes import ResourceAttributes

from waldur_site_agent.backend.exceptions import BackendError

from conftest import make_backend

CR_NAME = "waldur-test-ns"


def _prepare(settings, components, resource, attributes=None):
    """Create a backend whose clients are mocked, ready to create the resource."""
    backend, mock_k8s, mock_kc = make_backend(settings, components)
    if attributes is not None:
        resource.attributes = ResourceAttributes.from_dict(attributes)

    mock_kc.get_group_by_name.return_value = None
    mock_kc.create_group.side_effect = ["g-admin", "g-rw", "g-ro"]
    # The API server echoes the stored object; a CRD that prunes a field is
    # simulated by tests that override this.
    mock_k8s.create_managed_namespace.side_effect = lambda name, spec: {
        "metadata": {"name": name},
        "spec": spec,
    }
    return backend, mock_k8s, mock_kc


def _created_spec(settings, components, resource, attributes=None):
    """Create the resource and return the spec handed to the CR."""
    backend, mock_k8s, _ = _prepare(settings, components, resource, attributes)
    backend.create_resource(resource)
    return mock_k8s.create_managed_namespace.call_args[0][1]


def _refused(settings, components, resource, attributes=None):
    """Create the resource, expect a refusal, and check nothing was created."""
    backend, mock_k8s, mock_kc = _prepare(settings, components, resource, attributes)
    with pytest.raises(BackendError) as excinfo:
        backend.create_resource(resource)
    mock_kc.create_group.assert_not_called()
    mock_k8s.create_managed_namespace.assert_not_called()
    return str(excinfo.value)


class TestSecurityLevelDefaults:
    """Without an attribute the offering's default applies, or nothing at all."""

    def test_no_level_leaves_security_out(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Unset means the operator's own default applies, which the agent must not guess."""
        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert "security" not in spec

    @pytest.mark.parametrize("level", ["restricted", " Restricted "])
    def test_setting_sets_level(
        self, backend_settings, backend_components, waldur_resource, level
    ):
        backend_settings["default_security_level"] = level

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["security"] == {"level": "restricted"}

    def test_default_needs_no_allow_list(
        self, backend_settings, backend_components, waldur_resource
    ):
        """The default is the offering admin's own choice, even ``privileged``."""
        backend_settings["default_security_level"] = "privileged"

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["security"] == {"level": "privileged"}

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_empty_attribute_uses_default(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        backend_settings["default_security_level"] = "baseline"

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": value},
        )

        assert spec["security"] == {"level": "baseline"}

    @pytest.mark.parametrize("value", ["", "   "])
    def test_blank_setting_leaves_security_out(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        backend_settings["default_security_level"] = value

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert "security" not in spec

    def test_invalid_setting_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "unconfined"

        message = _refused(backend_settings, backend_components, waldur_resource)

        assert "'default_security_level' backend setting" in message
        assert "'unconfined'" in message


class TestSecurityLevelAllowList:
    """The order may pick a level only from ``allowed_security_levels``."""

    def test_allowed_level_applied(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allowed_security_levels"] = ["baseline", "restricted"]

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "baseline"},
        )

        assert spec["security"] == {"level": "baseline"}

    def test_allowed_level_overrides_default(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "restricted"
        backend_settings["allowed_security_levels"] = ["baseline"]

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "baseline"},
        )

        assert spec["security"] == {"level": "baseline"}

    def test_values_matched_case_insensitively(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allowed_security_levels"] = ["Baseline"]

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "  BASELINE "},
        )

        assert spec["security"] == {"level": "baseline"}

    def test_level_not_in_allow_list_refused(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "restricted"
        backend_settings["allowed_security_levels"] = ["restricted"]

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "baseline"},
        )

        assert "Attribute 'security_level'" in message
        assert "'baseline'" in message
        assert "allows only these security levels: restricted" in message

    def test_no_allow_list_refuses_any_choice(
        self, backend_settings, backend_components, waldur_resource
    ):
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "baseline"},
        )

        assert "Attribute 'security_level'" in message
        assert "allowed levels: none" in message

    def test_error_lists_allow_list_and_default(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "restricted"
        backend_settings["allowed_security_levels"] = ["baseline"]

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "privileged"},
        )

        assert "baseline, restricted" in message

    @pytest.mark.parametrize(
        "settings",
        [
            {},
            {"default_security_level": "baseline"},
            {"allowed_security_levels": ["baseline", "restricted"]},
        ],
    )
    def test_privileged_never_implied(
        self, backend_settings, backend_components, waldur_resource, settings
    ):
        """Privileged pods are node-level access on a shared cluster."""
        backend_settings.update(settings)

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "privileged"},
        )

        assert "Attribute 'security_level'" in message

    def test_privileged_applied_when_listed(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allowed_security_levels"] = ["privileged"]

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "privileged"},
        )

        assert spec["security"] == {"level": "privileged"}

    @pytest.mark.parametrize("value", ["Restricted", " restricted\t", "RESTRICTED"])
    def test_attribute_equal_to_default_needs_no_allow_list(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        backend_settings["default_security_level"] = "restricted"

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": value},
        )

        assert spec["security"] == {"level": "restricted"}

    @pytest.mark.parametrize("value", ["unconfined", 1, ["baseline"]])
    def test_invalid_attribute_refused(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        backend_settings["allowed_security_levels"] = ["baseline"]

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": value},
        )

        assert "Attribute 'security_level'" in message
        assert "allows only these security levels: baseline" in message

    def test_long_attribute_is_truncated_in_error(
        self, backend_settings, backend_components, waldur_resource
    ):
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "p" * 5000},
        )

        assert len(message) < 250
        assert "..." in message

    @pytest.mark.parametrize("configured", [["unconfined"], "baseline"])
    def test_invalid_allow_list_rejected(
        self, backend_settings, backend_components, waldur_resource, configured
    ):
        backend_settings["allowed_security_levels"] = configured

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "baseline"},
        )

        assert "'allowed_security_levels' backend setting" in message

    def test_malformed_allow_list_unused_when_attribute_equals_default(
        self, backend_settings, backend_components, waldur_resource
    ):
        """The allow-list is consulted only when the order asks for something else."""
        backend_settings["default_security_level"] = "restricted"
        backend_settings["allowed_security_levels"] = ["unconfined"]

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"security_level": "restricted"},
        )

        assert spec["security"] == {"level": "restricted"}


class TestPrunedFieldsFailClosed:
    """A CRD too old for a switch drops it silently; the order must not succeed."""

    @staticmethod
    def _pruning(mock_k8s, *paths):
        def create(name, spec):
            stored = {key: dict(value) if isinstance(value, dict) else value
                      for key, value in spec.items()}
            for path in paths:
                section, field = path.split(".")
                stored.get(section, {}).pop(field, None)
                if section in stored and not stored[section]:
                    del stored[section]
            return {"metadata": {"name": name}, "spec": stored}

        mock_k8s.create_managed_namespace.side_effect = create

    def test_pruned_security_level_fails_and_cleans_up(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "restricted"
        backend, mock_k8s, mock_kc = _prepare(
            backend_settings, backend_components, waldur_resource
        )
        self._pruning(mock_k8s, "security.level")
        mock_kc.get_group_by_name.side_effect = [None, None, None] + [
            {"id": g} for g in ("g-admin", "g-rw", "g-ro")
        ]

        with pytest.raises(BackendError) as excinfo:
            backend.create_resource(waldur_resource)

        assert "does not support spec.security.level" in str(excinfo.value)
        mock_k8s.delete_managed_namespace.assert_called_once_with(CR_NAME)
        assert mock_kc.delete_group.call_count == 3

    def test_failed_rollback_is_named_in_the_error(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["default_security_level"] = "restricted"
        backend, mock_k8s, _ = _prepare(backend_settings, backend_components, waldur_resource)
        self._pruning(mock_k8s, "security.level")
        mock_k8s.delete_managed_namespace.side_effect = BackendError("connection reset")

        with pytest.raises(BackendError) as excinfo:
            backend.create_resource(waldur_resource)

        message = str(excinfo.value)
        assert "removing it failed (connection reset)" in message
        assert f"delete ManagedNamespace {CR_NAME} in waldur-system by hand" in message
        assert "removed again" not in message

    def test_pruned_quota_switch_fails_and_cleans_up(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False
        backend, mock_k8s, _ = _prepare(backend_settings, backend_components, waldur_resource)
        self._pruning(mock_k8s, "quota.enabled")

        with pytest.raises(BackendError) as excinfo:
            backend.create_resource(waldur_resource)

        assert "does not support spec.quota.enabled" in str(excinfo.value)
        mock_k8s.delete_managed_namespace.assert_called_once_with(CR_NAME)

    def test_pruned_enabled_true_is_harmless(
        self, backend_settings, backend_components, waldur_resource
    ):
        """A CRD without the field enforces the quota, which is what was asked for."""
        backend, mock_k8s, _ = _prepare(backend_settings, backend_components, waldur_resource)
        self._pruning(mock_k8s, "quota.enabled")

        backend.create_resource(waldur_resource)

        mock_k8s.delete_managed_namespace.assert_not_called()

    def test_stored_fields_pass(self, backend_settings, backend_components, waldur_resource):
        backend_settings["default_security_level"] = "baseline"
        backend_settings["quota_enabled"] = False
        backend, mock_k8s, _ = _prepare(backend_settings, backend_components, waldur_resource)

        backend.create_resource(waldur_resource)

        mock_k8s.delete_managed_namespace.assert_not_called()


class TestSecurityLevelMetadata:
    """``status.securityLevel`` is the level actually in force."""

    def test_status_level_surfaced(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}},
            "status": {"securityLevel": "baseline"},
        }

        metadata = backend.get_resource_metadata(CR_NAME)

        assert metadata["security_level"] == "baseline"

    def test_absent_status_level_is_not_reported(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}, "security": {"level": "restricted"}},
            "status": {},
        }

        metadata = backend.get_resource_metadata(CR_NAME)

        assert "security_level" not in metadata

    def test_null_status_is_tolerated(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}},
            "status": None,
        }

        metadata = backend.get_resource_metadata(CR_NAME)

        assert metadata["name"] == CR_NAME
        assert "security_level" not in metadata

    def test_mismatch_between_spec_and_status_is_logged(
        self, backend_settings, backend_components, caplog
    ):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"security": {"level": "restricted"}},
            "status": {"securityLevel": "privileged"},
        }

        with caplog.at_level(logging.WARNING):
            metadata = backend.get_resource_metadata(CR_NAME)

        assert metadata["security_level"] == "privileged"
        assert any(
            "requests security level restricted" in r.getMessage() for r in caplog.records
        )


    def test_ready_namespace_without_reported_level_is_logged(
        self, backend_settings, backend_components, caplog
    ):
        """A controller older than the CRD stores the level but never applies it."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"security": {"level": "restricted"}},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }

        with caplog.at_level(logging.WARNING):
            metadata = backend.get_resource_metadata(CR_NAME)

        assert "security_level" not in metadata
        assert any("reports no applied level" in r.getMessage() for r in caplog.records)

    def test_not_yet_ready_namespace_is_not_logged(
        self, backend_settings, backend_components, caplog
    ):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"security": {"level": "restricted"}},
            "status": {},
        }

        with caplog.at_level(logging.WARNING):
            backend.get_resource_metadata(CR_NAME)

        assert not any("reports no applied level" in r.getMessage() for r in caplog.records)

class TestSetResourceLimitsKeepsLevel:
    """A limit update patches the quantities only."""

    def test_patch_does_not_touch_security(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)

        backend.set_resource_limits(CR_NAME, {"cpu": 8, "ram": 16, "storage": 200, "gpu": 2})

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert set(patch_body["spec"]) == {"quota"}
