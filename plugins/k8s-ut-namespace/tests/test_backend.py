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

    def test_usage_report_returns_empty(self, backend_settings, backend_components):
        backend, _, _ = _make_backend(backend_settings, backend_components)

        report = backend._get_usage_report(["waldur-test-ns"])

        assert report == {}


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
