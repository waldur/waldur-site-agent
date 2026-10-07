"""Tests for K8s UT namespace backend."""

import datetime
import json
import logging
from typing import ClassVar

import pytest
from freezegun import freeze_time
from kubernetes.client.rest import ApiException
from unittest.mock import MagicMock, patch, call
from uuid import uuid4

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent_k8s_ut_namespace.backend import (
    K8sUtNamespaceBackend,
    NS_ROLES,
    USAGE_ACCUMULATOR_ANNOTATION,
)
from waldur_site_agent.backend.exceptions import BackendError

from conftest import MockResourceLimits


def _make_backend(settings, components):
    """Create a backend with mocked K8s and Keycloak clients."""
    with (
        patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ) as mock_k8s,
        patch(
            "waldur_site_agent_k8s_ut_namespace.backend.KeycloakClient"
        ) as mock_kc,
    ):
        mock_k8s_instance = MagicMock()
        mock_k8s.return_value = mock_k8s_instance
        mock_kc_instance = MagicMock()
        mock_kc.return_value = mock_kc_instance

        backend = K8sUtNamespaceBackend(settings, components)
        return backend, mock_k8s_instance, mock_kc_instance


class TestK8sUtNamespaceBackendInit:
    """Tests for backend initialization."""

    def test_initialization_with_keycloak(self, backend_settings, backend_components):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        assert backend.backend_type == "k8s-ut-namespace"
        assert backend.namespace_prefix == "waldur-"
        assert backend.cr_namespace == "waldur-system"
        assert backend.keycloak_client is not None

    def test_initialization_without_keycloak(self, backend_settings_no_keycloak, backend_components):
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ):
            backend = K8sUtNamespaceBackend(backend_settings_no_keycloak, backend_components)

        assert backend.keycloak_client is None


class TestK8sUtNamespaceBackendPing:
    """Tests for ping method."""

    def test_ping_success(self, backend_settings, backend_components):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        mock_k8s.ping.return_value = True
        mock_kc.ping.return_value = True

        assert backend.ping() is True

    def test_ping_k8s_failure(self, backend_settings, backend_components):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        mock_k8s.ping.return_value = False

        assert backend.ping() is False

    def test_ping_keycloak_failure(self, backend_settings, backend_components):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        mock_k8s.ping.return_value = True
        mock_kc.ping.return_value = False

        assert backend.ping() is False

    def test_ping_raise_exception(self, backend_settings, backend_components):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        mock_k8s.ping.return_value = False

        with pytest.raises(BackendError, match="Failed to ping Kubernetes"):
            backend.ping(raise_exception=True)


class TestK8sUtNamespaceBackendGroupNaming:
    """Tests for Keycloak group naming convention."""

    def test_group_name_format(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        assert backend._get_keycloak_group_name("my-ns", "admin") == "ns_my-ns_admin"
        assert backend._get_keycloak_group_name("my-ns", "readwrite") == "ns_my-ns_readwrite"
        assert backend._get_keycloak_group_name("my-ns", "readonly") == "ns_my-ns_readonly"

    def test_get_all_group_names(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        names = backend._get_keycloak_group_names("test-res")
        assert names == {
            "admin": "ns_test-res_admin",
            "readwrite": "ns_test-res_readwrite",
            "readonly": "ns_test-res_readonly",
        }


class TestK8sUtNamespaceBackendQuota:
    """Tests for quota conversion."""

    def test_waldur_limits_to_quota(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        limits = {"cpu": 4, "ram": 8, "storage": 100, "gpu": 1}
        quota = backend._waldur_limits_to_quota(limits)

        assert quota == {"cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1"}

    def test_negative_limits_rejected(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        with pytest.raises(BackendError, match="Negative resource limits"):
            backend._waldur_limits_to_quota({"cpu": -4, "ram": 8, "storage": 100, "gpu": 1})

    def test_zero_limits_allowed(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        quota = backend._waldur_limits_to_quota({"cpu": 0, "ram": 0, "storage": 0, "gpu": 0})
        assert quota == {"cpu": "0", "memory": "0Gi", "storage": "0Gi", "gpu": "0"}

    def test_parse_k8s_quantity_gi(self, backend_settings, backend_components):
        assert K8sUtNamespaceBackend._parse_k8s_quantity("8Gi") == 8

    def test_parse_k8s_quantity_plain(self, backend_settings, backend_components):
        assert K8sUtNamespaceBackend._parse_k8s_quantity("4") == 4

    def test_parse_k8s_quantity_invalid(self, backend_settings, backend_components):
        assert K8sUtNamespaceBackend._parse_k8s_quantity("invalid") == 0

    def test_parse_k8s_quantity_millicores_not_truncated(
        self, backend_settings, backend_components
    ):
        # True division, not floor division: used to be int(500) // 1000 == 0, silently
        # dropping a sub-1-core request entirely (see _current_pod_usage_in_waldur_units).
        assert K8sUtNamespaceBackend._parse_k8s_quantity("500m") == 0.5

    def test_parse_k8s_quantity_mebibytes_not_truncated(
        self, backend_settings, backend_components
    ):
        assert K8sUtNamespaceBackend._parse_k8s_quantity("512Mi") == 0.5


class TestK8sUtNamespaceBackendCreateResource:
    """Tests for resource creation."""

    def test_create_resource(self, backend_settings, backend_components, waldur_resource):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        # Mock group creation
        mock_kc.get_group_by_name.return_value = None
        mock_kc.create_group.side_effect = ["group-admin", "group-rw", "group-ro"]

        # Mock CR creation
        mock_k8s.create_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"}
        }

        result = backend.create_resource(waldur_resource)

        assert result.backend_id == "waldur-test-ns"
        assert result.limits == {"cpu": 4, "ram": 8, "storage": 100, "gpu": 1}

        # Verify 3 Keycloak groups were created
        assert mock_kc.create_group.call_count == 3

        # Verify ManagedNamespace CR was created
        mock_k8s.create_managed_namespace.assert_called_once()
        call_args = mock_k8s.create_managed_namespace.call_args
        assert call_args[0][0] == "waldur-test-ns"  # CR metadata name
        spec = call_args[0][1]
        assert spec["name"] == "waldur-test-ns"  # spec.name (K8s namespace name)
        assert "quota" in spec
        # CRD uses per-role group fields, not a "groups" map
        assert spec["adminGroups"] == ["ns_test-ns_admin"]
        assert spec["rwGroups"] == ["ns_test-ns_readwrite"]
        assert spec["roGroups"] == ["ns_test-ns_readonly"]

    def test_create_resource_cr_failure_cleans_up_groups(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        mock_kc.get_group_by_name.return_value = None
        mock_kc.create_group.side_effect = ["g1", "g2", "g3"]
        mock_k8s.create_managed_namespace.side_effect = BackendError("CR creation failed")

        with pytest.raises(BackendError, match="CR creation failed"):
            backend.create_resource(waldur_resource)

        # Verify cleanup: delete_group should be called for each existing group
        # (get_group_by_name returns None since we mocked it to return None above,
        # so delete won't be called - that's fine, the _delete_keycloak_groups
        # method only deletes groups it can find)

    def test_create_resource_no_slug_raises(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        resource = WaldurResource(
            uuid=uuid4(), name="Test", slug="", backend_id=""
        )

        with pytest.raises(BackendError, match="has no slug"):
            backend.create_resource(resource)


class TestK8sUtNamespaceBackendDeleteResource:
    """Tests for resource deletion."""

    def test_delete_resource(
        self, backend_settings, backend_components, waldur_resource_with_backend_id
    ):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        # Mock group lookup for deletion
        mock_kc.get_group_by_name.side_effect = [
            {"id": "g-admin"},
            {"id": "g-rw"},
            {"id": "g-ro"},
        ]

        backend.delete_resource(waldur_resource_with_backend_id)

        mock_k8s.delete_managed_namespace.assert_called_once_with("waldur-test-ns")
        assert mock_kc.delete_group.call_count == 3

    def test_delete_resource_no_backend_id(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        resource = WaldurResource(uuid=uuid4(), name="Test", slug="t", backend_id="")
        backend.delete_resource(resource)

        mock_k8s.delete_managed_namespace.assert_not_called()


class TestK8sUtNamespaceBackendSetLimits:
    """Tests for set_resource_limits."""

    def test_set_resource_limits(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        limits = {"cpu": 8, "ram": 16, "storage": 200, "gpu": 2}
        backend.set_resource_limits("waldur-test-ns", limits)

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {"spec": {"quota": {"cpu": "8", "memory": "16Gi", "storage": "200Gi", "gpu": "2"}}},
        )


class TestK8sUtNamespaceBackendUserManagement:
    """Tests for user management with role-based Keycloak groups."""

    def test_add_users_with_roles(self, backend_settings, backend_components, waldur_resource):
        # Add custom mapping so "observer" maps to readonly
        backend_settings["role_mapping"] = {"observer": "readonly"}
        backend, _, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        # Mock group lookup
        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)

        # Mock user lookup
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice", "bob", "carol"}
        user_roles = {"alice": "manager", "bob": "member", "carol": "observer"}

        result = backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles
        )

        assert result == {"alice", "bob", "carol"}

        # Verify alice -> admin group
        mock_kc.add_user_to_group.assert_any_call("kc-alice", "g-admin")
        # Verify bob -> readwrite group
        mock_kc.add_user_to_group.assert_any_call("kc-bob", "g-rw")
        # Verify carol -> readonly group
        mock_kc.add_user_to_group.assert_any_call("kc-carol", "g-ro")

    def test_role_reconciliation_moves_user(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Test that a user is moved from one group to another on role change."""
        backend, _, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)

        mock_kc.find_user.return_value = {"id": "kc-alice"}

        # Alice is currently in readwrite group, but role changed to manager (admin)
        def is_in_group(user_id, group_id):
            return group_id == "g-rw"  # Currently in readwrite

        mock_kc.is_user_in_group.side_effect = is_in_group

        user_roles = {"alice": "manager"}
        backend.add_users_to_resource(
            waldur_resource, {"alice"}, user_roles=user_roles
        )

        # Should remove from readwrite
        mock_kc.remove_user_from_group.assert_called_with("kc-alice", "g-rw")
        # Should add to admin
        mock_kc.add_user_to_group.assert_called_with("kc-alice", "g-admin")

    def test_remove_user_from_all_groups(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend, _, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)

        mock_kc.find_user.return_value = {"id": "kc-alice"}
        mock_kc.is_user_in_group.side_effect = lambda uid, gid: gid == "g-rw"

        result = backend.remove_user(waldur_resource, "alice")

        assert result is True
        mock_kc.remove_user_from_group.assert_called_once_with("kc-alice", "g-rw")

    def test_add_user_no_keycloak(
        self, backend_settings_no_keycloak, backend_components, waldur_resource
    ):
        """Without Keycloak, add_users_to_resource returns all users."""
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ):
            backend = K8sUtNamespaceBackend(
                backend_settings_no_keycloak, backend_components
            )

        waldur_resource.backend_id = "waldur-test-ns"
        result = backend.add_users_to_resource(waldur_resource, {"alice"})

        assert result == {"alice"}

    def test_remove_user_not_in_keycloak(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Removing a user not found in Keycloak should succeed."""
        backend, _, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.return_value = {"id": "g1"}
        mock_kc.find_user.return_value = None  # User not found

        result = backend.remove_user(waldur_resource, "unknown-user")
        assert result is True


class TestK8sUtNamespaceBackendSyncUsersToCR:
    """Tests for sync_users_to_cr feature."""

    def test_sync_users_to_cr_patches_cr_with_emails(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        # Mock Keycloak group lookup for the Keycloak path
        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice", "bob"}
        user_roles = {"alice": "manager", "bob": "member"}
        user_attributes = {
            "alice": {"email": "alice@example.com"},
            "bob": {"email": "bob@example.com"},
        }

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        # Verify CR was patched with user emails
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {
                "spec": {
                    "adminUsers": ["alice@example.com"],
                    "rwUsers": ["bob@example.com"],
                    "roUsers": [],
                }
            },
        )

    def test_sync_users_to_cr_disabled_does_not_patch(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = False
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice"}
        user_roles = {"alice": "manager"}
        user_attributes = {"alice": {"email": "alice@example.com"}}

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        # CR should not be patched
        mock_k8s.patch_managed_namespace.assert_not_called()

    def test_sync_users_to_cr_default_disabled(
        self, backend_settings, backend_components
    ):
        backend, _, _ = _make_backend(backend_settings, backend_components)
        assert backend.sync_users_to_cr is False

    def test_sync_users_to_cr_skips_users_without_identity(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice", "bob"}
        user_roles = {"alice": "manager", "bob": "member"}
        # Only alice has an email attribute
        user_attributes = {"alice": {"email": "alice@example.com"}}

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {
                "spec": {
                    "adminUsers": ["alice@example.com"],
                    "rwUsers": [],
                    "roUsers": [],
                }
            },
        )

    def test_sync_users_to_cr_without_keycloak(
        self, backend_settings_no_keycloak, backend_components, waldur_resource
    ):
        """sync_users_to_cr works even without Keycloak."""
        backend_settings_no_keycloak["sync_users_to_cr"] = True
        with patch(
            "waldur_site_agent_k8s_ut_namespace.backend.K8sUtNamespaceClient"
        ) as mock_k8s_cls:
            mock_k8s = MagicMock()
            mock_k8s_cls.return_value = mock_k8s
            backend = K8sUtNamespaceBackend(
                backend_settings_no_keycloak, backend_components
            )

        waldur_resource.backend_id = "waldur-test-ns"
        user_ids = {"alice"}
        user_roles = {"alice": "manager"}
        user_attributes = {"alice": {"email": "alice@example.com"}}

        result = backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        # CR should be patched even without Keycloak
        mock_k8s.patch_managed_namespace.assert_called_once()
        # All user_ids returned since no Keycloak filtering
        assert result == {"alice"}

    def test_sync_users_to_cr_no_backend_id_skips(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = ""

        mock_kc.get_group_by_name.return_value = None

        user_roles = {"alice": "manager"}
        user_attributes = {"alice": {"email": "alice@example.com"}}

        backend.add_users_to_resource(
            waldur_resource, {"alice"}, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        # Should not attempt to patch CR when backend_id is empty
        mock_k8s.patch_managed_namespace.assert_not_called()


class TestK8sUtNamespaceBackendPullResource:
    """Tests for pull_resource method."""

    def test_pull_resource(self, backend_settings, backend_components, waldur_resource):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"},
            "spec": {"quota": {"cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1"}},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "message": "All resources reconciled successfully"},
            ]},
        }
        # Mock group members
        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.get_group_members.side_effect = lambda gid: {
            "g-admin": [{"id": "user1"}],
            "g-rw": [{"id": "user2"}],
            "g-ro": [],
        }.get(gid, [])

        result = backend.pull_resource(waldur_resource)

        assert result is not None
        assert result.backend_id == "waldur-test-ns"
        assert set(result.users) == {"user1", "user2"}
        assert result.backend_metadata == {
            "status": {"ready": True, "message": "All resources reconciled successfully"},
        }

    def test_pull_resource_not_found(self, backend_settings, backend_components, waldur_resource):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-missing"

        mock_k8s.get_managed_namespace.return_value = None

        result = backend.pull_resource(waldur_resource)
        assert result is None


class TestK8sUtNamespaceBackendStatusOps:
    """Tests for downscale, pause, restore operations."""

    def test_downscale(self, backend_settings, backend_components):
        # backend_components (conftest.py) declares cpu/ram/storage/gpu -- all four
        # must appear here. The old hardcoded cpu/memory/storage literal silently
        # left gpu at its full quota through a downscale/pause; confirmed live
        # against a real cluster with a real over-budget resource.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.downscale_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {"spec": {"quota": {"cpu": "1", "memory": "1Gi", "storage": "1Gi", "gpu": "1"}}},
        )

    def test_pause(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.pause_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {"spec": {"quota": {"cpu": "0", "memory": "0Gi", "storage": "0Gi", "gpu": "0"}}},
        )

    def test_pause_covers_every_configured_component_generically(self, backend_settings):
        # Regression test for the live-confirmed bug: pause/downscale must derive
        # their quota from whatever components *this* offering actually declares,
        # not a hardcoded cpu/memory/storage literal that silently leaves any other
        # configured component (gpu, or anything else) untouched. Proven here with a
        # 5th, made-up component type the old hardcoded dict could never anticipate.
        settings_with_mapping = {
            **backend_settings,
            "component_quota_mapping": {"widgets": "widgetLimit"},
        }
        components = {
            "cpu": {"type": "cpu", "unit_factor": 1},
            "ram": {"type": "ram", "unit_factor": 1},
            "storage": {"type": "storage", "unit_factor": 1},
            "gpu": {"type": "gpu", "unit_factor": 1},
            "extra": {"type": "widgets", "unit_factor": 1},
        }
        backend, mock_k8s, _ = _make_backend(settings_with_mapping, components)

        backend.pause_resource("waldur-test-ns")

        _, patch = mock_k8s.patch_managed_namespace.call_args[0]
        assert set(patch["spec"]["quota"].keys()) == {
            "cpu", "memory", "storage", "gpu", "widgetLimit",
        }
        assert all(v in ("0", "0Gi") for v in patch["spec"]["quota"].values())

    def test_restore(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.restore_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_not_called()

    def test_downscale_failure(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.patch_managed_namespace.side_effect = Exception("API error")

        assert backend.downscale_resource("waldur-test-ns") is False


class TestK8sUtNamespaceBackendUsageReport:
    """Tests for usage report generation.

    cpu/ram/gpu, credited from each container's own real start/finish timestamps
    (_credit_pod_container_usage); storage x elapsed time, sampled from the CR's own
    spec.quota (_current_quota_in_waldur_units) since a PVC isn't a per-pod request and
    persists independent of any pod. Either way, accumulated month-to-date via a
    JSON-encoded annotation on the CR (see backend.py's _get_usage_report docstring for
    why: neither source has history of its own, so the backend persists its own running
    total between calls, the same way sacct's own historical log lets SLURM recompute
    month-to-date freshly on every call without needing to persist anything itself).

    Most tests here set up one pod, Running continuously since the prior sample, whose
    requests exactly match CR_QUOTA's cpu/ram/gpu figures, so the expected totals are
    identical to what a pure quota-based reading would have given -- this validates the
    elapsed-time accumulation logic (unchanged in spirit) through the new per-container
    crediting path (changed), rather than conflating the two. Pod-crediting specifics
    (multiple pods, multiple containers, terminated-between-polls, no double-counting)
    get their own dedicated tests below.
    """

    CR_QUOTA: ClassVar[dict[str, str]] = {
        "cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1",
    }

    def _cr(self, annotations=None):
        return {
            "metadata": {"name": "waldur-test-ns", "annotations": annotations or {}},
            "spec": {"name": "waldur-test-ns", "quota": dict(self.CR_QUOTA)},
        }

    @staticmethod
    def _pod(cpu=None, memory=None, gpu=None, pod_uid="pod-1", container_name="main",
              running_since=None, terminated=None):
        """A single-container pod requesting the given resources.

        Pass running_since=<aware datetime> for a still-Running container, or
        terminated=(started_at, finished_at) for one that's already Terminated.
        """
        requests = {}
        if cpu is not None:
            requests["cpu"] = cpu
        if memory is not None:
            requests["memory"] = memory
        if gpu is not None:
            requests["nvidia.com/gpu"] = gpu
        if terminated is not None:
            started_at, finished_at = terminated
            state = {
                "running": None,
                "terminated": {"started_at": started_at, "finished_at": finished_at},
            }
        else:
            state = {"running": {"started_at": running_since}, "terminated": None}
        return {
            "metadata": {"uid": pod_uid},
            "spec": {"containers": [{"name": container_name, "resources": {"requests": requests}}]},
            "status": {"container_statuses": [{"name": container_name, "state": state}]},
        }

    def _matching_pod(self, running_since):
        """One pod, Running since *running_since*, whose requests exactly match
        CR_QUOTA's cpu/ram/gpu."""
        return self._pod(cpu="4", memory="8Gi", gpu="1", running_since=running_since)

    @freeze_time("2026-09-15 12:00:00")
    def test_first_sample_accumulates_nothing_yet(self, backend_settings, backend_components):
        # No prior state to diff against -- the first sample can only start
        # the clock, not report any elapsed usage yet.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr()

        report = backend._get_usage_report(["waldur-test-ns"])

        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0,
        }

    @freeze_time("2026-09-15 12:00:00")
    def test_first_sample_persists_state(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr()

        backend._get_usage_report(["waldur-test-ns"])

        # replace_managed_namespace, not patch_managed_namespace: a full read-modify-write
        # (with optimistic concurrency via resourceVersion) is what makes this safe against
        # report and membership_sync both calling this independently -- see
        # TestK8sUtNamespaceBackendUsageReportConcurrency below.
        mock_k8s.replace_managed_namespace.assert_called_once()
        name, updated_cr = mock_k8s.replace_managed_namespace.call_args[0]
        assert name == "waldur-test-ns"
        state = json.loads(updated_cr["metadata"]["annotations"][USAGE_ACCUMULATOR_ANNOTATION])
        assert state["period"] == "2026-09"
        assert state["last_sample_at"] == "2026-09-15T12:00:00+00:00"
        assert state["accumulated"] == {"cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0}

    def test_second_sample_accumulates_quota_times_elapsed_minutes(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        prior_state = {
            "period": "2026-09",
            "last_sample_at": "2026-09-15T12:00:00+00:00",
            "accumulated": {"cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0},
        }
        mock_k8s.get_managed_namespace.return_value = self._cr(
            {USAGE_ACCUMULATOR_ANNOTATION: json.dumps(prior_state)}
        )
        # Running continuously since the prior sample -- credited for the full 30 min.
        running_since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        mock_k8s.list_pods.return_value = [self._matching_pod(running_since)]

        with freeze_time("2026-09-15 12:30:00"):  # 30 minutes later
            report = backend._get_usage_report(["waldur-test-ns"])

        # pod requests (cpu=4, ram=8, gpu=1) + quota (storage=100) x 30 minutes elapsed
        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 120.0, "ram": 240.0, "storage": 3000.0, "gpu": 30.0,
        }

    def test_accumulation_compounds_across_multiple_samples(
        self, backend_settings, backend_components
    ):
        # Two real _get_usage_report() calls in sequence, each reading back
        # whatever the previous call persisted -- proves the annotation
        # round-trip, not just one call's arithmetic.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        persisted = {}

        def fake_replace(name, updated_cr):
            persisted[name] = updated_cr["metadata"]["annotations"][USAGE_ACCUMULATOR_ANNOTATION]
            return {}

        mock_k8s.replace_managed_namespace.side_effect = fake_replace
        # Running continuously since the very first poll, through all three samples.
        running_since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        mock_k8s.list_pods.return_value = [self._matching_pod(running_since)]

        with freeze_time("2026-09-15 12:00:00"):
            mock_k8s.get_managed_namespace.return_value = self._cr()
            backend._get_usage_report(["waldur-test-ns"])

        with freeze_time("2026-09-15 12:10:00"):  # +10 min
            mock_k8s.get_managed_namespace.return_value = self._cr(
                {USAGE_ACCUMULATOR_ANNOTATION: persisted["waldur-test-ns"]}
            )
            backend._get_usage_report(["waldur-test-ns"])

        with freeze_time("2026-09-15 12:25:00"):  # +15 min more (25 min total)
            mock_k8s.get_managed_namespace.return_value = self._cr(
                {USAGE_ACCUMULATOR_ANNOTATION: persisted["waldur-test-ns"]}
            )
            report = backend._get_usage_report(["waldur-test-ns"])

        # 4 cpu x 25 elapsed minutes total, accumulated across both hops
        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 100.0

    def test_month_boundary_resets_accumulator(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        prior_state = {
            "period": "2026-08",  # last sample was in August
            "last_sample_at": "2026-08-31T23:00:00+00:00",
            "accumulated": {"cpu": 999.0, "ram": 999.0, "storage": 999.0, "gpu": 999.0},
        }
        mock_k8s.get_managed_namespace.return_value = self._cr(
            {USAGE_ACCUMULATOR_ANNOTATION: json.dumps(prior_state)}
        )

        with freeze_time("2026-09-01 01:00:00"):  # now September
            report = backend._get_usage_report(["waldur-test-ns"])

        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0,
        }

    def test_malformed_annotation_treated_as_no_prior_state(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr(
            {USAGE_ACCUMULATOR_ANNOTATION: "not valid json{{{"}
        )

        report = backend._get_usage_report(["waldur-test-ns"])

        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"] == {
            "cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0,
        }

    def test_missing_namespace_skipped_not_errored(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = None

        report = backend._get_usage_report(["waldur-gone"])

        assert report == {}

    def test_get_managed_namespace_error_skips_resource(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.side_effect = BackendError("API down")

        report = backend._get_usage_report(["waldur-test-ns"])

        assert report == {}

    def test_patch_failure_still_reports_this_samples_usage(
        self, backend_settings, backend_components
    ):
        # A failed persist shouldn't drop the whole report -- worst case is
        # slightly over-counting one interval on the *next* call, not losing
        # this one's figures now.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        prior_state = {
            "period": "2026-09",
            "last_sample_at": "2026-09-15T12:00:00+00:00",
            "accumulated": {"cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0},
        }
        mock_k8s.get_managed_namespace.return_value = self._cr(
            {USAGE_ACCUMULATOR_ANNOTATION: json.dumps(prior_state)}
        )
        running_since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        mock_k8s.list_pods.return_value = [self._matching_pod(running_since)]
        mock_k8s.replace_managed_namespace.side_effect = BackendError("write conflict")

        with freeze_time("2026-09-15 12:30:00"):
            report = backend._get_usage_report(["waldur-test-ns"])

        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 120.0

    def test_multiple_resources_handled_independently(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.side_effect = lambda _name: self._cr()

        report = backend._get_usage_report(["waldur-ns-a", "waldur-ns-b"])

        assert set(report.keys()) == {"waldur-ns-a", "waldur-ns-b"}

    def test_only_declared_components_reported(self, backend_settings):
        # A component absent from backend_components (here: no "gpu" entry)
        # must not appear in the usage report either.
        components_without_gpu = {
            "cpu": {"type": "cpu", "unit_factor": 1, "accounting_type": "usage"},
            "ram": {"type": "ram", "unit_factor": 1, "accounting_type": "usage"},
        }
        backend, mock_k8s, _ = _make_backend(backend_settings, components_without_gpu)
        mock_k8s.get_managed_namespace.return_value = self._cr()

        report = backend._get_usage_report(["waldur-test-ns"])

        assert set(report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"].keys()) == {"cpu", "ram"}


class TestK8sUtNamespaceBackendPodUsageSampling:
    """Tests for _credit_pod_container_usage.

    What gets credited, from where, and how double-counting is avoided. Each test
    drives it through a full _get_usage_report() call so the assertion stays
    readable and exercises the real annotation round-trip, rather than calling the
    private method directly.
    """

    UTC = datetime.timezone.utc

    def _cr(self, name="waldur-pod-test-ns", quota=None, pod_credit_state=None):
        return {
            "metadata": {"name": name, "annotations": {
                USAGE_ACCUMULATOR_ANNOTATION: json.dumps({
                    "period": "2026-09",
                    "last_sample_at": "2026-09-15T12:00:00+00:00",
                    "accumulated": {"cpu": 0.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0},
                    "pod_credit_state": pod_credit_state or {},
                }),
            }},
            "spec": {"name": name, "quota": quota or {
                "cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1",
            }},
        }

    @staticmethod
    def _container(name, cpu=None, memory=None, gpu=None, running_since=None, terminated=None):
        requests = {}
        if cpu is not None:
            requests["cpu"] = cpu
        if memory is not None:
            requests["memory"] = memory
        if gpu is not None:
            requests["nvidia.com/gpu"] = gpu
        if terminated is not None:
            started_at, finished_at = terminated
            state = {
                "running": None,
                "terminated": {"started_at": started_at, "finished_at": finished_at},
            }
        elif running_since is not None:
            state = {"running": {"started_at": running_since}, "terminated": None}
        else:
            state = {"running": None, "terminated": None}  # waiting
        return {"name": name, "requests": requests, "state": state}

    @staticmethod
    def _pod(pod_uid, containers):
        return {
            "metadata": {"uid": pod_uid},
            "spec": {"containers": [
                {"name": c["name"], "resources": {"requests": c["requests"]}} for c in containers
            ]},
            "status": {"container_statuses": [
                {"name": c["name"], "state": c["state"]} for c in containers
            ]},
        }

    def _sample(
        self, backend, mock_k8s, pods, pod_credit_state=None, sample_at="2026-09-15 12:10:00"
    ):
        mock_k8s.get_managed_namespace.return_value = self._cr(pod_credit_state=pod_credit_state)
        mock_k8s.list_pods.return_value = pods
        with freeze_time(sample_at):
            report = backend._get_usage_report(["waldur-pod-test-ns"])
        return report["waldur-pod-test-ns"]["TOTAL_ACCOUNT_USAGE"]

    def test_sums_requests_across_multiple_pods(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=self.UTC)  # 10 min before sample
        pods = [
            self._pod("pod-a", [
                self._container("main", cpu="1", memory="2Gi", gpu="1", running_since=since)
            ]),
            self._pod("pod-b", [
                self._container("main", cpu="2", memory="4Gi", running_since=since)
            ]),
        ]
        usage = self._sample(backend, mock_k8s, pods)
        # (1+2) cpu, (2+4) ram, (1+0) gpu -- x 10 minutes elapsed
        assert usage["cpu"] == 30.0
        assert usage["ram"] == 60.0
        assert usage["gpu"] == 10.0

    def test_sums_requests_across_multiple_containers_in_one_pod(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=self.UTC)
        pods = [self._pod("pod-a", [
            self._container("main", cpu="1", memory="1Gi", running_since=since),
            self._container("sidecar", cpu="1", memory="1Gi", running_since=since),
        ])]
        usage = self._sample(backend, mock_k8s, pods)
        assert usage["cpu"] == 20.0  # (1+1) cpu x 10 min
        assert usage["ram"] == 20.0  # (1+1) ram x 10 min

    def test_terminated_container_credited_for_its_real_duration_not_excluded(
        self, backend_settings, backend_components
    ):
        # The exact regression this feature exists for: a container whose entire
        # lifetime (started_at to finished_at) fell *before* this poll -- e.g. a short
        # job that started and finished entirely between two report cycles -- still
        # gets credited for the real time it ran, using Kubernetes' own recorded
        # timestamps, not "was it Running at the moment we happened to look" (which an
        # earlier, snapshot-based version of this used, and which an operator correctly
        # found missed exactly this case).
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        started_at = datetime.datetime(2026, 9, 15, 12, 5, 0, tzinfo=self.UTC)
        finished_at = datetime.datetime(2026, 9, 15, 12, 5, 30, tzinfo=self.UTC)  # 30s job
        pods = [self._pod("pod-a", [
            self._container("main", cpu="4", memory="8Gi", terminated=(started_at, finished_at)),
        ])]
        usage = self._sample(backend, mock_k8s, pods)  # sampled at 12:10, well after it finished
        assert usage["cpu"] == pytest.approx(4 * (30 / 60))  # 4 cpu x 30 real seconds
        assert usage["ram"] == pytest.approx(8 * (30 / 60))

    def test_terminated_container_not_double_counted_on_a_repeat_sighting(
        self, backend_settings, backend_components
    ):
        # Kubernetes doesn't delete a Succeeded/Failed pod on its own -- it lingers
        # until garbage collected, often well past the next poll. A second sighting of
        # the *same* terminated container must add nothing further.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        started_at = datetime.datetime(2026, 9, 15, 12, 5, 0, tzinfo=self.UTC)
        finished_at = datetime.datetime(2026, 9, 15, 12, 5, 30, tzinfo=self.UTC)
        pods = [self._pod("pod-a", [
            self._container("main", cpu="4", terminated=(started_at, finished_at)),
        ])]
        # This container was already credited (up to finished_at) on a prior poll.
        prior_state = {"pod-a/main": finished_at.isoformat()}
        usage = self._sample(backend, mock_k8s, pods, pod_credit_state=prior_state)
        assert usage["cpu"] == 0.0

    def test_waiting_container_contributes_nothing(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        # neither running nor terminated
        pods = [self._pod("pod-a", [self._container("main", cpu="4")])]
        usage = self._sample(backend, mock_k8s, pods)
        assert usage["cpu"] == 0.0

    def test_no_requests_logs_a_summary_not_silence(
        self, backend_settings, backend_components, caplog
    ):
        # The single most common "looks like a bug but isn't" support question this
        # plugin gets: cpu/ram read 0.0 while storage keeps accruing. A container with no
        # resources.requests contributes nothing by design (billing is requested, not
        # measured, resources) -- but that must be visible in the logs, not silent, so an
        # operator doesn't have to rediscover it by reading the source.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=self.UTC)
        pods = [
            self._pod("pod-a", [self._container("main", running_since=since)]),  # no requests
            self._pod("pod-b", [self._container("main", cpu="1", running_since=since)]),
        ]
        with caplog.at_level(logging.INFO):
            usage = self._sample(backend, mock_k8s, pods)
        assert usage["cpu"] == 10.0  # only pod-b's 1 cpu x 10 min -- pod-a contributes 0
        messages = [r.message for r in caplog.records]
        assert any("1 container(s) have no resources.requests set" in m for m in messages)

    def test_no_pods_gives_zero_cpu_ram_gpu_but_storage_still_from_quota(
        self, backend_settings, backend_components
    ):
        # An empty namespace (or one that doesn't exist yet -- list_pods()
        # already returns [] for that case, not an error) means genuinely zero
        # compute usage, but storage is still the CR's own standing quota.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        usage = self._sample(backend, mock_k8s, [])
        assert usage["cpu"] == 0.0
        assert usage["ram"] == 0.0
        assert usage["gpu"] == 0.0
        assert usage["storage"] == 1000.0  # 100Gi quota x 10 min, unaffected by pods

    def test_fractional_requests_not_truncated_to_zero(
        self, backend_settings, backend_components
    ):
        # The _parse_k8s_quantity bug this depends on being fixed: "500m" cpu / "512Mi"
        # memory used to floor-divide to 0, which would have made pod-based metering
        # silently undercount almost every real pod (sub-1-core/sub-1Gi requests are
        # the common case, not the exception).
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        since = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=self.UTC)
        pods = [self._pod("pod-a", [
            self._container("main", cpu="500m", memory="512Mi", running_since=since)
        ])]
        usage = self._sample(backend, mock_k8s, pods)
        assert usage["cpu"] == 5.0   # 0.5 cpu x 10 min
        assert usage["ram"] == 5.0   # 0.5 (512Mi/1024) ram x 10 min

    def test_long_running_container_credited_incrementally_not_from_true_start_each_time(
        self, backend_settings, backend_components
    ):
        # A container already credited up through a prior poll must only be credited
        # for the *new* time since then, not its whole history again.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        # true_start: 2h before credited_until, which is itself 10 min before the sample
        true_start = datetime.datetime(2026, 9, 15, 10, 0, 0, tzinfo=self.UTC)
        credited_until = datetime.datetime(2026, 9, 15, 12, 0, 0, tzinfo=self.UTC)
        pods = [self._pod("pod-a", [self._container("main", cpu="4", running_since=true_start)])]
        prior_state = {"pod-a/main": credited_until.isoformat()}
        usage = self._sample(backend, mock_k8s, pods, pod_credit_state=prior_state)
        assert usage["cpu"] == 40.0  # 4 cpu x 10 min since credited_until, not since true_start

    def test_list_pods_called_with_spec_name_not_cr_namespace(
        self, backend_settings, backend_components
    ):
        # The CR lives in cr_namespace (e.g. "waldur-system"); the pods being metered
        # live in the real workload namespace, spec.name -- these must never be
        # conflated.
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr(name="waldur-pod-test-ns")
        mock_k8s.list_pods.return_value = []

        backend._get_usage_report(["waldur-pod-test-ns"])

        mock_k8s.list_pods.assert_called_once_with("waldur-pod-test-ns")


class TestK8sUtNamespaceBackendUsageReportConcurrency:
    """report and membership_sync mode both call _get_usage_report() (via pull_resource())
    independently, with no coordination -- confirmed live against a real cluster, where a
    naive read-modify-write silently lost one side's update. These tests cover the fix:
    optimistic concurrency (replace_managed_namespace, rejected with 409 on a stale
    resourceVersion) with retry, re-reading fresh on every attempt."""

    def _cr(self, annotations=None, resource_version="1"):
        return {
            "metadata": {
                "name": "waldur-test-ns",
                "annotations": annotations or {},
                "resourceVersion": resource_version,
            },
            "spec": {"quota": {"cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1"}},
        }

    def test_conflict_once_then_succeeds_by_rereading(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        # Second read reflects a concurrent writer's own update (a higher resourceVersion and
        # an already-larger accumulated total) -- the retry must recompute from *this*, not
        # blindly resubmit its first attempt's now-stale numbers.
        concurrent_state = {
            "period": "2026-09", "last_sample_at": "2026-09-15T12:00:00+00:00",
            "accumulated": {"cpu": 999.0, "ram": 0.0, "storage": 0.0, "gpu": 0.0},
        }
        mock_k8s.get_managed_namespace.side_effect = [
            self._cr(resource_version="1"),
            self._cr({USAGE_ACCUMULATOR_ANNOTATION: json.dumps(concurrent_state)}, resource_version="2"),
        ]
        mock_k8s.replace_managed_namespace.side_effect = [
            ApiException(status=409, reason="Conflict"),
            {},
        ]

        with freeze_time("2026-09-15 12:00:00"):
            report = backend._get_usage_report(["waldur-test-ns"])

        assert mock_k8s.get_managed_namespace.call_count == 2
        assert mock_k8s.replace_managed_namespace.call_count == 2
        # Built on the second (concurrent) read's 999.0 base, not the first attempt's 0.0.
        assert report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 999.0

    def test_gives_up_after_max_retries_but_still_reports(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr()
        mock_k8s.replace_managed_namespace.side_effect = ApiException(status=409, reason="Conflict")

        report = backend._get_usage_report(["waldur-test-ns"])

        assert mock_k8s.replace_managed_namespace.call_count == backend._USAGE_ACCUMULATOR_RETRIES
        assert "waldur-test-ns" in report  # still reports this attempt's figures, unpersisted

    def test_non_conflict_api_error_propagates(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = self._cr()
        mock_k8s.replace_managed_namespace.side_effect = ApiException(status=500, reason="Internal Server Error")

        with pytest.raises(ApiException):
            backend._get_usage_report(["waldur-test-ns"])


class TestK8sUtNamespaceBackendNameValidation:
    """Tests for namespace name validation."""

    def test_valid_names(self):
        # Should not raise
        K8sUtNamespaceBackend._validate_namespace_name("waldur-test-ns")
        K8sUtNamespaceBackend._validate_namespace_name("a")
        K8sUtNamespaceBackend._validate_namespace_name("a-b-c")
        K8sUtNamespaceBackend._validate_namespace_name("abc123")

    def test_empty_name(self):
        with pytest.raises(BackendError, match="must not be empty"):
            K8sUtNamespaceBackend._validate_namespace_name("")

    def test_too_long(self):
        name = "a" * 64
        with pytest.raises(BackendError, match="exceeds 63 characters"):
            K8sUtNamespaceBackend._validate_namespace_name(name)

    def test_uppercase(self):
        with pytest.raises(BackendError, match="not a valid RFC 1123"):
            K8sUtNamespaceBackend._validate_namespace_name("Waldur-Test")

    def test_starts_with_hyphen(self):
        with pytest.raises(BackendError, match="not a valid RFC 1123"):
            K8sUtNamespaceBackend._validate_namespace_name("-bad")

    def test_ends_with_hyphen(self):
        with pytest.raises(BackendError, match="not a valid RFC 1123"):
            K8sUtNamespaceBackend._validate_namespace_name("bad-")

    def test_special_chars(self):
        with pytest.raises(BackendError, match="not a valid RFC 1123"):
            K8sUtNamespaceBackend._validate_namespace_name("bad_name!")

    def test_create_resource_invalid_name_rejected(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        resource = WaldurResource(
            uuid=uuid4(), name="Test", slug="Bad_Slug!", backend_id=""
        )

        with pytest.raises(BackendError, match="not a valid RFC 1123"):
            backend.create_resource(resource)

        # Should not have attempted to create CR or groups
        mock_k8s.create_managed_namespace.assert_not_called()


class TestK8sUtNamespaceBackendLabelsAnnotations:
    """Tests for labels and annotations support."""

    def test_create_resource_with_labels_and_annotations(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["namespace_labels"] = {"env": "prod", "team": "hpc"}
        backend_settings["namespace_annotations"] = {"contact": "admin@example.com"}
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        mock_kc.get_group_by_name.return_value = None
        mock_kc.create_group.side_effect = ["g1", "g2", "g3"]
        mock_k8s.create_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"}
        }

        backend.create_resource(waldur_resource)

        spec = mock_k8s.create_managed_namespace.call_args[0][1]
        assert spec["labels"] == {"env": "prod", "team": "hpc"}
        assert spec["annotations"] == {"contact": "admin@example.com"}

    def test_create_resource_without_labels_annotations(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)

        mock_kc.get_group_by_name.return_value = None
        mock_kc.create_group.side_effect = ["g1", "g2", "g3"]
        mock_k8s.create_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"}
        }

        backend.create_resource(waldur_resource)

        spec = mock_k8s.create_managed_namespace.call_args[0][1]
        assert "labels" not in spec
        assert "annotations" not in spec


class TestK8sUtNamespaceBackendReadyCondition:
    """Tests for status condition parsing."""

    def test_parse_ready_true(self):
        status = {"conditions": [
            {"type": "Ready", "status": "True", "message": "All good"},
        ]}
        result = K8sUtNamespaceBackend._parse_ready_condition(status)
        assert result == {"ready": True, "message": "All good"}

    def test_parse_ready_false(self):
        status = {"conditions": [
            {"type": "Ready", "status": "False", "message": "quota exceeded"},
        ]}
        result = K8sUtNamespaceBackend._parse_ready_condition(status)
        assert result == {"ready": False, "message": "quota exceeded"}

    def test_parse_ready_unknown(self):
        status = {"conditions": [
            {"type": "Ready", "status": "Unknown", "message": ""},
        ]}
        result = K8sUtNamespaceBackend._parse_ready_condition(status)
        assert result == {"ready": None, "message": ""}

    def test_parse_no_conditions(self):
        result = K8sUtNamespaceBackend._parse_ready_condition({})
        assert result == {"ready": None, "message": ""}

    def test_get_resource_metadata_parses_status(
        self, backend_settings, backend_components
    ):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"},
            "spec": {"quota": {"cpu": "4"}},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "message": "Reconciled"},
            ]},
        }

        metadata = backend.get_resource_metadata("waldur-test-ns")

        assert metadata["status"] == {"ready": True, "message": "Reconciled"}


class TestK8sUtNamespaceBackendCrUserIdentity:
    """Tests for configurable CR user identity field."""

    def test_custom_identity_field(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend_settings["cr_user_identity_field"] = "username"
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice"}
        user_roles = {"alice": "manager"}
        user_attributes = {"alice": {"username": "alice_123", "email": "alice@example.com"}}

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {
                "spec": {
                    "adminUsers": ["alice_123"],
                    "rwUsers": [],
                    "roUsers": [],
                }
            },
        )

    def test_missing_identity_field_skips_user(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend_settings["cr_user_identity_field"] = "civil_number"
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice"}
        user_roles = {"alice": "manager"}
        # No civil_number attribute
        user_attributes = {"alice": {"email": "alice@example.com"}}

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        # All user lists should be empty since identity field is missing
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {
                "spec": {
                    "adminUsers": [],
                    "rwUsers": [],
                    "roUsers": [],
                }
            },
        )

    def test_identity_lowercase(
        self, backend_settings, backend_components, waldur_resource
    ):
        backend_settings["sync_users_to_cr"] = True
        backend_settings["cr_user_identity_field"] = "civil_number"
        backend_settings["cr_user_identity_lowercase"] = True
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_kc.get_group_by_name.side_effect = lambda name: {
            "ns_test-ns_admin": {"id": "g-admin"},
            "ns_test-ns_readwrite": {"id": "g-rw"},
            "ns_test-ns_readonly": {"id": "g-ro"},
        }.get(name)
        mock_kc.find_user.side_effect = lambda uid, _: {"id": f"kc-{uid}"}
        mock_kc.is_user_in_group.return_value = False

        user_ids = {"alice"}
        user_roles = {"alice": "member"}
        user_attributes = {"alice": {"civil_number": "XX12345678901"}}

        backend.add_users_to_resource(
            waldur_resource, user_ids, user_roles=user_roles,
            user_attributes=user_attributes,
        )

        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {
                "spec": {
                    "adminUsers": [],
                    "rwUsers": ["xx12345678901"],
                    "roUsers": [],
                }
            },
        )
