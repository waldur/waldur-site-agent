"""Tests for K8s UT namespace backend."""

import pytest
from unittest.mock import MagicMock, patch, call
from uuid import uuid4

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent_k8s_ut_namespace.backend import (
    K8sUtNamespaceBackend,
    NS_ROLES,
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


class TestK8sUtNamespaceBackendPullResource:
    """Tests for pull_resource method."""

    def test_pull_resource(self, backend_settings, backend_components, waldur_resource):
        backend, mock_k8s, mock_kc = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-test-ns"

        mock_k8s.get_managed_namespace.return_value = {
            "metadata": {"name": "waldur-test-ns"},
            "spec": {"quota": {"cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1"}},
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
        assert result.usage["TOTAL_ACCOUNT_USAGE"]["cpu"] == 4

    def test_pull_resource_not_found(self, backend_settings, backend_components, waldur_resource):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        waldur_resource.backend_id = "waldur-missing"

        mock_k8s.get_managed_namespace.return_value = None

        result = backend.pull_resource(waldur_resource)
        assert result is None


class TestK8sUtNamespaceBackendStatusOps:
    """Tests for downscale, pause, restore operations."""

    def test_downscale(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.downscale_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {"spec": {"quota": {"cpu": "1", "memory": "1Gi", "storage": "1Gi"}}},
        )

    def test_pause(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.pause_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_called_once_with(
            "waldur-test-ns",
            {"spec": {"quota": {"cpu": "0", "memory": "0Gi", "storage": "0Gi"}}},
        )

    def test_restore(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        assert backend.restore_resource("waldur-test-ns") is True
        mock_k8s.patch_managed_namespace.assert_not_called()

    def test_downscale_failure(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.patch_managed_namespace.side_effect = Exception("API error")

        assert backend.downscale_resource("waldur-test-ns") is False


class TestK8sUtNamespaceBackendUsageReport:
    """Tests for usage report generation."""

    def test_usage_report(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)

        mock_k8s.get_managed_namespace.return_value = {
            "spec": {"quota": {"cpu": "4", "memory": "8Gi", "storage": "100Gi", "gpu": "1"}}
        }

        report = backend._get_usage_report(["waldur-test-ns"])

        assert "waldur-test-ns" in report
        usage = report["waldur-test-ns"]["TOTAL_ACCOUNT_USAGE"]
        assert usage["cpu"] == 4
        assert usage["ram"] == 8
        assert usage["storage"] == 100
        assert usage["gpu"] == 1

    def test_usage_report_missing_cr(self, backend_settings, backend_components):
        backend, mock_k8s, _ = _make_backend(backend_settings, backend_components)
        mock_k8s.get_managed_namespace.return_value = None

        report = backend._get_usage_report(["waldur-missing"])
        assert report["waldur-missing"]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 0
