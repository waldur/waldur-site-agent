"""The quota enforcement switch: ``spec.quota.enabled``, gated per offering."""

from __future__ import annotations

import json

import pytest
from waldur_api_client.models.resource_attributes import ResourceAttributes

from waldur_site_agent.backend.exceptions import BackendError

from conftest import make_backend

CR_NAME = "waldur-test-ns"
ANNOTATION = "waldur.io/quota-before-pause"
LEGACY_ANNOTATION = "waldur.io/quota-enabled-before-pause"


def _prepare(settings, components, resource, attributes=None):
    """Create a backend whose clients are mocked, ready to create the resource.

    ``attributes`` is what the order form recorded; leaving it out is the order
    that named no switch, which is what an old resource looks like.
    """
    backend, mock_k8s, mock_kc = make_backend(settings, components)
    if attributes is not None:
        resource.attributes = ResourceAttributes.from_dict(attributes)

    mock_kc.get_group_by_name.return_value = None
    mock_kc.create_group.side_effect = ["g-admin", "g-rw", "g-ro"]
    mock_k8s.create_managed_namespace.return_value = {"metadata": {"name": CR_NAME}}
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


class TestQuotaEnabledDefaults:
    """``spec.quota.enabled`` is always written, and follows the offering."""

    def test_quota_enforced_by_default(
        self, backend_settings, backend_components, waldur_resource
    ):
        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"] == {
            "cpu": "4",
            "memory": "8Gi",
            "storage": "100Gi",
            "gpu": "1",
            "enabled": True,
        }

    def test_setting_turns_quota_off(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["enabled"] is False

    def test_quantities_sent_when_quota_disabled(
        self, backend_settings, backend_components, waldur_resource
    ):
        """The entitlement is still recorded; only its enforcement is switched off."""
        backend_settings["quota_enabled"] = False

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["cpu"] == "4"
        assert spec["quota"]["memory"] == "8Gi"

    def test_null_setting_falls_back_to_true(
        self, backend_settings, backend_components, waldur_resource
    ):
        """An empty ``quota_enabled:`` in the YAML is not "off"."""
        backend_settings["quota_enabled"] = None

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["enabled"] is True

    def test_string_setting_is_parsed(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = "off"

        spec = _created_spec(backend_settings, backend_components, waldur_resource)

        assert spec["quota"]["enabled"] is False

    def test_invalid_setting_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = "sometimes"

        message = _refused(backend_settings, backend_components, waldur_resource)

        assert "'quota_enabled' backend setting" in message
        assert "'sometimes'" in message


class TestQuotaOverrideAllowed:
    """With ``allow_quota_override`` the order's attribute wins."""

    @pytest.fixture(autouse=True)
    def _allow(self, backend_settings):
        backend_settings["allow_quota_override"] = True

    @pytest.mark.parametrize(
        "value", [False, "false", " False ", "no", "off", "f", "N", 0, "0", 0.0]
    )
    def test_attribute_turns_quota_off(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": value},
        )

        assert spec["quota"]["enabled"] is False
        assert spec["quota"]["cpu"] == "4"

    @pytest.mark.parametrize("value", [True, "t", "Y", "on", 1, 1.0])
    def test_drf_true_spellings_accepted(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        """Whatever Waldur's BooleanField accepted is not refused here."""
        backend_settings["quota_enabled"] = False

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": value},
        )

        assert spec["quota"]["enabled"] is True

    def test_attribute_turns_quota_on_over_setting(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "true"},
        )

        assert spec["quota"]["enabled"] is True

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_empty_attribute_falls_back_to_setting(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        """An untouched form field arrives as null or empty, not as "off"."""
        backend_settings["quota_enabled"] = False

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": value},
        )

        assert spec["quota"]["enabled"] is False

    def test_invalid_attribute_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "maybe"},
        )

        assert "Attribute 'quota_enabled'" in message
        assert "'maybe'" in message

    @pytest.mark.parametrize("value", [[False], {}, 2, 0.5, "2"])
    def test_non_boolean_attribute_rejected(
        self, backend_settings, backend_components, waldur_resource, value
    ):
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": value},
        )

        assert "Attribute 'quota_enabled' must be true or false" in message

    def test_long_attribute_is_truncated_in_error(
        self, backend_settings, backend_components, waldur_resource
    ):
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "x" * 5000},
        )

        assert len(message) < 200
        assert "..." in message

    def test_string_override_setting_is_parsed(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allow_quota_override"] = "yes"

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False},
        )

        assert spec["quota"]["enabled"] is False


class TestQuotaOverrideNotAllowed:
    """Without ``allow_quota_override`` the order cannot differ from the offering."""

    @pytest.mark.parametrize("override", [None, False, "false"])
    def test_differing_attribute_fails_the_order(
        self, backend_settings, backend_components, waldur_resource, override
    ):
        """Waldur stores undeclared attributes, so an ungated one would uncap the namespace."""
        if override is not None:
            backend_settings["allow_quota_override"] = override

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False},
        )

        assert "Attribute 'quota_enabled'" in message
        assert "does not allow" in message
        assert "allow_quota_override" in message

    def test_turning_quota_on_is_also_gated(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["quota_enabled"] = False

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "true"},
        )

        assert "Attribute 'quota_enabled'" in message

    @pytest.mark.parametrize(
        ("setting", "value"), [(None, True), (True, "yes"), (False, False), (False, "0")]
    )
    def test_attribute_equal_to_default_goes_through(
        self, backend_settings, backend_components, waldur_resource, setting, value
    ):
        if setting is not None:
            backend_settings["quota_enabled"] = setting

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": value},
        )

        assert spec["quota"]["enabled"] is (setting is not False)

    def test_invalid_attribute_rejected(
        self, backend_settings, backend_components, waldur_resource
    ):
        """An unreadable value is reported as such, not as a gate refusal."""
        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": "maybe"},
        )

        assert "must be true or false" in message
        assert "'maybe'" in message


class TestMalformedOverrideSetting:
    """``allow_quota_override`` is read only when an order differs from the default."""

    def test_malformed_override_named_when_order_differs(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allow_quota_override"] = "sometimes"

        message = _refused(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": False},
        )

        assert "'allow_quota_override' backend setting" in message
        assert "'sometimes'" in message

    def test_malformed_override_unused_when_order_matches(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["allow_quota_override"] = "sometimes"

        spec = _created_spec(
            backend_settings,
            backend_components,
            waldur_resource,
            {"quota_enabled": True},
        )

        assert spec["quota"]["enabled"] is True


def _cr(enabled, annotations=None, quota=None):
    quota = {"cpu": "4", "memory": "8Gi"} if quota is None else quota
    return {
        "metadata": {"name": CR_NAME, "annotations": annotations or {}},
        "spec": {"quota": {**quota, "enabled": enabled}},
    }


def _snapshot(**quota):
    return json.dumps(quota, sort_keys=True)


class TestPauseForcesEnforcement:
    """Pause and downscale shrink the quota, which only bites when it is enforced."""

    @pytest.mark.parametrize(
        ("operation", "quota"),
        [
            ("pause_resource", {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}),
            ("downscale_resource", {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}),
        ],
    )
    def test_quota_is_forced_on_and_snapshotted(
        self, backend_settings, backend_components, operation, quota
    ):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(False)

        assert getattr(backend, operation)(CR_NAME) is True

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            CR_NAME,
            {
                "spec": {"quota": {**quota, "enabled": True}},
                "metadata": {
                    "annotations": {
                        ANNOTATION: _snapshot(cpu="4", memory="8Gi", enabled=False)
                    }
                },
            },
        )

    @pytest.mark.parametrize(
        "annotations",
        [{ANNOTATION: _snapshot(cpu="4", enabled=False)}, {LEGACY_ANNOTATION: "false"}],
    )
    def test_second_pause_keeps_first_snapshot(
        self, backend_settings, backend_components, annotations
    ):
        """Downscale then pause: the quota from before both is what restore needs."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, annotations, {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}
        )

        assert backend.pause_resource(CR_NAME) is True

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert "metadata" not in patch_body
        assert patch_body["spec"]["quota"]["enabled"] is True

    def test_quota_without_enabled_is_snapshotted_as_is(
        self, backend_settings, backend_components
    ):
        """A CR created before the switch existed: restore treats absent as enforced."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": CR_NAME},
            "spec": {"quota": {"cpu": "4"}},
        }

        assert backend.pause_resource(CR_NAME) is True

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert patch_body["metadata"]["annotations"] == {ANNOTATION: _snapshot(cpu="4")}

    def test_missing_cr_fails_pause(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = None

        assert backend.pause_resource(CR_NAME) is False
        mock_k8s.patch_managed_namespace.assert_not_called()


class TestRestorePutsQuotaBack:
    """Restore writes back the whole snapshot; no limit update is needed after it."""

    REMOVE_ANNOTATIONS = {"annotations": {ANNOTATION: None, LEGACY_ANNOTATION: None}}

    def test_restore_puts_quantities_and_switch_back(
        self, backend_settings, backend_components
    ):
        backend_settings["allow_quota_override"] = True
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True,
            {ANNOTATION: _snapshot(cpu="4", memory="8Gi", enabled=False)},
            {"cpu": "0", "memory": "0Gi", "storage": "0Gi"},
        )

        assert backend.restore_resource(CR_NAME) is True

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            CR_NAME,
            {
                "metadata": self.REMOVE_ANNOTATIONS,
                "spec": {
                    "quota": {"cpu": "4", "memory": "8Gi", "storage": None, "enabled": False}
                },
            },
        )

    def test_restore_of_empty_quota_removes_pause_quantities(
        self, backend_settings, backend_components
    ):
        """A namespace with no quantities before the pause has none after it."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {ANNOTATION: _snapshot()}, {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}
        )

        assert backend.restore_resource(CR_NAME) is True

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert patch_body["spec"]["quota"] == {
            "cpu": None,
            "memory": None,
            "storage": None,
            "enabled": True,
        }

    def test_restore_without_annotation_is_a_no_op(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(False)

        assert backend.restore_resource(CR_NAME) is True
        mock_k8s.patch_managed_namespace.assert_not_called()

    def test_legacy_annotation_restores_switch_only(
        self, backend_settings, backend_components
    ):
        """The earlier annotation held only ``enabled``; the quantities are unknown."""
        backend_settings["allow_quota_override"] = True
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {LEGACY_ANNOTATION: "false"}, {"cpu": "0"}
        )

        assert backend.restore_resource(CR_NAME) is True

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            CR_NAME,
            {"metadata": self.REMOVE_ANNOTATIONS, "spec": {"quota": {"enabled": False}}},
        )

    def test_malformed_snapshot_fails_restore(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(True, {ANNOTATION: "[1, 2]"})

        assert backend.restore_resource(CR_NAME) is False
        mock_k8s.patch_managed_namespace.assert_not_called()

    def test_restore_rechecks_offering_settings(
        self, backend_settings, backend_components
    ):
        """An override the offering no longer allows is not reinstated."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {ANNOTATION: _snapshot(cpu="4", enabled=False)}
        )

        assert backend.restore_resource(CR_NAME) is True

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert patch_body["spec"]["quota"]["enabled"] is True
        assert patch_body["spec"]["quota"]["cpu"] == "4"

    def test_restore_of_offering_default_needs_no_override(
        self, backend_settings, backend_components
    ):
        backend_settings["quota_enabled"] = False
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {ANNOTATION: _snapshot(cpu="4", enabled=False)}
        )

        assert backend.restore_resource(CR_NAME) is True

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert patch_body["spec"]["quota"]["enabled"] is False

    def test_limit_update_after_restore_keeps_switch(
        self, backend_settings, backend_components
    ):
        backend_settings["allow_quota_override"] = True
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.side_effect = [
            _cr(True, {ANNOTATION: _snapshot(cpu="4", enabled=False)}),
            _cr(False),
        ]

        backend.restore_resource(CR_NAME)
        backend.set_resource_limits(CR_NAME, {"cpu": 4, "ram": 8, "storage": 100, "gpu": 1})

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert "enabled" not in patch_body["spec"]["quota"]


class TestSetResourceLimitsKeepsSwitch:
    """A limit update patches the quantities only."""

    def test_patch_does_not_touch_enabled(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)

        backend.set_resource_limits(CR_NAME, {"cpu": 8, "ram": 16, "storage": 200, "gpu": 2})

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert "enabled" not in patch_body["spec"]["quota"]


class TestLimitUpdateWhilePaused:
    """A limit update during a pause updates the saved quota, not the paused spec."""

    LIMITS = {"cpu": 2, "ram": 4, "storage": 50, "gpu": 0}
    QUOTA = {"cpu": "2", "memory": "4Gi", "storage": "50Gi", "gpu": "0"}

    def test_update_goes_into_snapshot(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        cr = _cr(
            True,
            {ANNOTATION: _snapshot(cpu="16", memory="64Gi", enabled=False)},
            {"cpu": "0", "memory": "0Gi", "storage": "0Gi"},
        )
        cr["metadata"]["resourceVersion"] = "42"
        mock_k8s.get_managed_namespace.return_value = cr

        backend.set_resource_limits(CR_NAME, self.LIMITS)

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            CR_NAME,
            {
                "metadata": {
                    "annotations": {
                        ANNOTATION: _snapshot(**self.QUOTA, enabled=False),
                        LEGACY_ANNOTATION: None,
                    },
                    "resourceVersion": "42",
                }
            },
        )

    def test_restore_after_update_brings_back_new_limits(
        self, backend_settings, backend_components
    ):
        backend_settings["allow_quota_override"] = True
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {ANNOTATION: _snapshot(cpu="16", enabled=False)}, {"cpu": "0"}
        )
        backend.set_resource_limits(CR_NAME, self.LIMITS)
        saved = mock_k8s.patch_managed_namespace.call_args[0][1]["metadata"]["annotations"]
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {ANNOTATION: saved[ANNOTATION]}, {"cpu": "0"}
        )

        backend.restore_resource(CR_NAME)

        quota = mock_k8s.patch_managed_namespace.call_args[0][1]["spec"]["quota"]
        assert quota == {**self.QUOTA, "enabled": False}

    def test_legacy_annotation_is_converted(self, backend_settings, backend_components):
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(
            True, {LEGACY_ANNOTATION: "false"}, {"cpu": "0"}
        )

        backend.set_resource_limits(CR_NAME, self.LIMITS)

        patch_body = mock_k8s.patch_managed_namespace.call_args[0][1]
        assert "spec" not in patch_body
        assert json.loads(patch_body["metadata"]["annotations"][ANNOTATION]) == {
            **self.QUOTA,
            "enabled": False,
        }
        assert patch_body["metadata"]["annotations"][LEGACY_ANNOTATION] is None

    def test_malformed_snapshot_fails_update(self, backend_settings, backend_components):
        """Better a failed update than a lifted pause."""
        backend, mock_k8s, _ = make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = _cr(True, {ANNOTATION: "[1]"})

        with pytest.raises(BackendError):
            backend.set_resource_limits(CR_NAME, self.LIMITS)
        mock_k8s.patch_managed_namespace.assert_not_called()
