"""Tests for OpenNebula Keycloak SAML integration."""

import os
from unittest.mock import MagicMock, patch

import pyone
import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_opennebula.backend import (
    DEFAULT_VDC_ROLES,
    OpenNebulaBackend,
)


def _make_waldur_resource(
    backend_id="test-vdc",
    name="Test VDC",
    offering_plugin_options=None,
    attributes=None,
):
    """Build a mock WaldurResource."""
    resource = MagicMock(spec=WaldurResource)
    resource.backend_id = backend_id
    resource.name = name
    resource.offering_plugin_options = offering_plugin_options or {}
    resource.attributes = attributes or {}
    resource.limits = {}
    resource.project_slug = "test-project"
    return resource


def _make_backend(settings=None, components=None):
    """Create an OpenNebulaBackend with mocked client."""
    base_settings = {
        "api_url": "http://localhost:2633/RPC2",
        "credentials": "oneadmin:testpass",
        "zone_id": 0,
    }
    if settings:
        base_settings.update(settings)

    components = components or {
        "cpu": {"limit": 100, "measured_unit": "cores", "unit_factor": 1},
        "ram": {"limit": 1024, "measured_unit": "MB", "unit_factor": 1},
        "storage": {"limit": 10240, "measured_unit": "MB", "unit_factor": 1},
    }

    with patch(
        "waldur_site_agent_opennebula.backend.OpenNebulaClient"
    ) as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        backend = OpenNebulaBackend(base_settings, components)

    return backend


def _make_keycloak_backend(keycloak_settings=None):
    """Create a backend with Keycloak enabled and mocked KeycloakClient."""
    kc_settings = keycloak_settings or {
        "keycloak_url": "https://keycloak.example.com/auth/",
        "keycloak_realm": "waldur",
        "keycloak_username": "admin",
        "keycloak_password": "secret",
    }
    settings = {
        "api_url": "http://localhost:2633/RPC2",
        "credentials": "oneadmin:testpass",
        "keycloak_enabled": True,
        "keycloak": kc_settings,
    }

    with (
        patch(
            "waldur_site_agent_opennebula.backend.OpenNebulaClient"
        ) as mock_client_cls,
        patch(
            "waldur_site_agent_keycloak_client.KeycloakClient"
        ) as mock_kc_cls,
    ):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_kc = MagicMock()
        mock_kc_cls.return_value = mock_kc
        backend = OpenNebulaBackend(settings, {
            "cpu": {"limit": 100, "measured_unit": "cores", "unit_factor": 1},
        })

    return backend


# ── Keycloak init ────────────────────────────────────────────────


class TestKeycloakInit:
    """Test Keycloak client initialization in __init__."""

    def test_keycloak_enabled_creates_client(self):
        backend = _make_keycloak_backend()
        assert backend.keycloak_client is not None

    def test_keycloak_disabled_no_client(self):
        backend = _make_backend()
        assert backend.keycloak_client is None

    def test_keycloak_disabled_explicitly(self):
        backend = _make_backend({"keycloak_enabled": False})
        assert backend.keycloak_client is None

    def test_keycloak_init_failure_logs_warning(self):
        settings = {
            "api_url": "http://localhost:2633/RPC2",
            "credentials": "oneadmin:testpass",
            "keycloak_enabled": True,
            "keycloak": {},
        }
        with (
            patch(
                "waldur_site_agent_opennebula.backend.OpenNebulaClient"
            ),
            patch(
                "waldur_site_agent_keycloak_client.KeycloakClient",
                side_effect=Exception("connection refused"),
            ),
        ):
            backend = OpenNebulaBackend(settings, {})

        assert backend.keycloak_client is None

    def test_default_vdc_roles_set(self):
        backend = _make_backend()
        assert backend.vdc_roles == DEFAULT_VDC_ROLES

    def test_custom_vdc_roles_override_defaults(self):
        custom_roles = [
            {"name": "viewer", "one_group_suffix": "viewers",
             "default_view": "user", "views": "user"},
        ]
        backend = _make_backend({"vdc_roles": custom_roles})
        assert backend.vdc_roles == custom_roles

    def test_default_user_role(self):
        backend = _make_backend()
        assert backend.default_user_role == "user"

    def test_custom_default_user_role(self):
        backend = _make_backend({"default_user_role": "admin"})
        assert backend.default_user_role == "admin"

    def test_default_saml_mapping_file(self):
        backend = _make_backend()
        assert backend.saml_mapping_file == "/var/lib/one/keycloak_groups.yaml"

    def test_custom_saml_mapping_file(self):
        backend = _make_backend({"saml_mapping_file": "/tmp/test.yaml"})
        assert backend.saml_mapping_file == "/tmp/test.yaml"


# ── Resource slug ────────────────────────────────────────────────


class TestResourceSlug:
    """Test _get_resource_slug()."""

    def test_uses_backend_id(self):
        resource = _make_waldur_resource(backend_id="my-vdc")
        assert OpenNebulaBackend._get_resource_slug(resource) == "my-vdc"

    def test_falls_back_to_name(self):
        resource = _make_waldur_resource(backend_id="", name="My VDC")
        assert OpenNebulaBackend._get_resource_slug(resource) == "my_vdc"

    def test_unnamed_resource(self):
        resource = _make_waldur_resource(backend_id="", name=None)
        assert OpenNebulaBackend._get_resource_slug(resource) == "unnamed"


# ── Keycloak group creation ──────────────────────────────────────


class TestKeycloakGroupCreation:
    """Test _create_keycloak_groups()."""

    def test_creates_parent_and_child_groups(self):
        backend = _make_keycloak_backend()
        backend.keycloak_client.get_group_by_name.return_value = None
        backend.keycloak_client.create_group.side_effect = [
            "parent-id", "child-admin", "child-user", "child-cloud"
        ]
        resource = _make_waldur_resource(backend_id="test-vdc")

        result = backend._create_keycloak_groups(resource)

        assert result == {
            "admin": "child-admin",
            "user": "child-user",
            "cloud": "child-cloud",
        }
        assert backend.keycloak_client.create_group.call_count == 4

    def test_reuses_existing_parent_group(self):
        backend = _make_keycloak_backend()

        def get_group(name):
            if name == "vdc_test-vdc":
                return {"id": "existing-parent", "subGroups": []}
            return None

        backend.keycloak_client.get_group_by_name.side_effect = get_group
        backend.keycloak_client.create_group.side_effect = [
            "child-admin", "child-user", "child-cloud"
        ]
        resource = _make_waldur_resource(backend_id="test-vdc")

        result = backend._create_keycloak_groups(resource)

        # Parent was not created (only 3 child calls)
        assert backend.keycloak_client.create_group.call_count == 3
        assert len(result) == 3

    def test_returns_empty_on_keycloak_failure(self):
        backend = _make_keycloak_backend()
        backend.keycloak_client.get_group_by_name.side_effect = Exception("timeout")
        resource = _make_waldur_resource(backend_id="test-vdc")

        result = backend._create_keycloak_groups(resource)

        assert result == {}

    def test_returns_empty_when_keycloak_disabled(self):
        backend = _make_backend()
        resource = _make_waldur_resource()

        result = backend._create_keycloak_groups(resource)

        assert result == {}


# ── ONE Keycloak group creation ──────────────────────────────────


class TestOneKeycloakGroupCreation:
    """Test _create_one_keycloak_groups()."""

    def test_creates_one_groups_per_role(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-admin-id", "user": "kc-user-id", "cloud": "kc-cloud-id"}
        result = backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        assert backend.client._create_group.call_count == 3
        backend.client._create_group.assert_any_call("testvdc-admins")
        backend.client._create_group.assert_any_call("testvdc-users")
        backend.client._create_group.assert_any_call("testvdc-cloud")

    def test_sets_saml_group_template(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        # Check that _update_group_template was called for each group
        assert backend.client._update_group_template.call_count == 3

        # Verify SAML_GROUP is in at least one template call
        calls = backend.client._update_group_template.call_args_list
        first_template = calls[0][0][1]
        assert 'SAML_GROUP="/vdc_testvdc/admin"' in first_template

    def test_sets_fireedge_views_per_role(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        calls = backend.client._update_group_template.call_args_list

        # Admin role: groupadmin view
        admin_template = calls[0][0][1]
        assert 'DEFAULT_VIEW="groupadmin"' in admin_template
        assert 'VIEWS="groupadmin,user,cloud"' in admin_template

        # User role: user view
        user_template = calls[1][0][1]
        assert 'DEFAULT_VIEW="user"' in user_template
        assert 'VIEWS="user"' in user_template

    def test_adds_groups_to_vdc(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        backend._create_one_keycloak_groups("testvdc", 42, kc_groups)

        assert backend.client._add_group_to_vdc.call_count == 3
        backend.client._add_group_to_vdc.assert_any_call(42, 101)
        backend.client._add_group_to_vdc.assert_any_call(42, 102)
        backend.client._add_group_to_vdc.assert_any_call(42, 103)

    def test_returns_saml_mapping(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        result = backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        assert result == {
            "/vdc_testvdc/admin": 101,
            "/vdc_testvdc/user": 102,
            "/vdc_testvdc/cloud": 103,
        }

    def test_admin_role_gets_add_admin_call(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [101, 102, 103]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        # Only the admin group should get _add_admin_to_group
        backend.client._add_admin_to_group.assert_called_once_with(101, 0)

    def test_skips_missing_role_in_kc_groups(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.return_value = 101

        # Only provide "user" role, missing "admin" and "cloud"
        kc_groups = {"user": "kc-user-id"}
        result = backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        assert len(result) == 1
        assert "/vdc_testvdc/user" in result

    def test_continues_on_single_group_failure(self):
        backend = _make_keycloak_backend()
        backend.client._create_group.side_effect = [
            BackendError("fail"), 102, 103
        ]

        kc_groups = {"admin": "kc-id", "user": "kc-id", "cloud": "kc-id"}
        result = backend._create_one_keycloak_groups("testvdc", 1, kc_groups)

        # First group failed, but user and cloud should succeed
        assert len(result) == 2


# ── SAML mapping file ────────────────────────────────────────────


class TestKeycloakMappingFile:
    """Test _update_saml_mapping_file() and _remove_saml_mapping_entries()."""

    def test_creates_file_if_not_exists(self, tmp_path):
        backend = _make_backend()
        backend.saml_mapping_file = str(tmp_path / "mappings.yaml")

        backend._update_saml_mapping_file({"/vdc_test/admin": 101})

        assert os.path.exists(backend.saml_mapping_file)
        import yaml
        with open(backend.saml_mapping_file) as fh:
            data = yaml.safe_load(fh)
        assert data == {"/vdc_test/admin": 101}

    def test_merges_with_existing_content(self, tmp_path):
        backend = _make_backend()
        mapping_file = tmp_path / "mappings.yaml"
        backend.saml_mapping_file = str(mapping_file)

        import yaml
        with open(mapping_file, "w") as fh:
            yaml.safe_dump({"/vdc_old/user": 50}, fh)

        backend._update_saml_mapping_file({"/vdc_new/admin": 101})

        with open(mapping_file) as fh:
            data = yaml.safe_load(fh)
        assert data == {"/vdc_old/user": 50, "/vdc_new/admin": 101}

    def test_handles_empty_file(self, tmp_path):
        backend = _make_backend()
        mapping_file = tmp_path / "mappings.yaml"
        mapping_file.write_text("")
        backend.saml_mapping_file = str(mapping_file)

        backend._update_saml_mapping_file({"/vdc_test/user": 102})

        import yaml
        with open(mapping_file) as fh:
            data = yaml.safe_load(fh)
        assert data == {"/vdc_test/user": 102}

    def test_atomic_write_uses_tmp(self, tmp_path):
        backend = _make_backend()
        mapping_file = tmp_path / "mappings.yaml"
        backend.saml_mapping_file = str(mapping_file)

        backend._update_saml_mapping_file({"/vdc_test/user": 102})

        # Temp file should be cleaned up
        assert not os.path.exists(str(mapping_file) + ".tmp")
        assert os.path.exists(str(mapping_file))

    def test_remove_entries_by_slug(self, tmp_path):
        backend = _make_backend()
        mapping_file = tmp_path / "mappings.yaml"
        backend.saml_mapping_file = str(mapping_file)

        import yaml
        with open(mapping_file, "w") as fh:
            yaml.safe_dump({
                "/vdc_keep/admin": 10,
                "/vdc_remove/admin": 20,
                "/vdc_remove/user": 21,
            }, fh)

        backend._remove_saml_mapping_entries("remove")

        with open(mapping_file) as fh:
            data = yaml.safe_load(fh)
        assert data == {"/vdc_keep/admin": 10}

    def test_remove_noop_when_no_match(self, tmp_path):
        backend = _make_backend()
        mapping_file = tmp_path / "mappings.yaml"
        backend.saml_mapping_file = str(mapping_file)

        import yaml
        original = {"/vdc_other/admin": 10}
        with open(mapping_file, "w") as fh:
            yaml.safe_dump(original, fh)

        backend._remove_saml_mapping_entries("nonexistent")

        with open(mapping_file) as fh:
            data = yaml.safe_load(fh)
        assert data == original

    def test_remove_handles_missing_file(self, tmp_path):
        backend = _make_backend()
        backend.saml_mapping_file = str(tmp_path / "nonexistent.yaml")

        # Should not raise
        backend._remove_saml_mapping_entries("anything")


# ── VDC create with Keycloak ─────────────────────────────────────


class TestVDCCreateWithKeycloak:
    """Test VDC creation flow with Keycloak integration."""

    def test_full_vdc_creation_calls_keycloak(self):
        backend = _make_keycloak_backend()
        backend.client.get_resource.return_value = None
        backend.client.create_resource.return_value = None
        backend.client._network_metadata = None

        mock_vdc = MagicMock()
        mock_vdc.ID = 42
        backend.client._get_vdc_by_name.return_value = mock_vdc

        backend.keycloak_client.get_group_by_name.return_value = None
        backend.keycloak_client.keycloak_admin.get_group_children.return_value = []
        backend.keycloak_client.create_group.side_effect = [
            "parent-id", "child-admin", "child-user", "child-cloud"
        ]
        backend.client._create_group.side_effect = [101, 102, 103]

        resource = _make_waldur_resource(backend_id="test-vdc")
        backend._current_waldur_resource = resource

        with patch.object(backend, "_update_saml_mapping_file"):
            result = backend._create_vdc_resource("test-vdc", "Test", "Org")

        assert result is True
        # Keycloak parent + 3 children
        assert backend.keycloak_client.create_group.call_count == 4
        # ONE groups created
        assert backend.client._create_group.call_count == 3

    def test_keycloak_disabled_no_keycloak_calls(self):
        backend = _make_backend()
        backend.client.get_resource.return_value = None
        backend.client.create_resource.return_value = None
        backend.client._network_metadata = None
        backend._current_waldur_resource = _make_waldur_resource()

        result = backend._create_vdc_resource("test-vdc", "Test", "Org")

        assert result is True
        # No keycloak interaction
        assert not hasattr(backend, "keycloak_client") or backend.keycloak_client is None

    def test_keycloak_failure_does_not_prevent_vdc_creation(self):
        backend = _make_keycloak_backend()
        backend.client.get_resource.return_value = None
        backend.client.create_resource.return_value = None
        backend.client._network_metadata = None

        # Make Keycloak fail
        backend.keycloak_client.get_group_by_name.side_effect = Exception("timeout")

        resource = _make_waldur_resource(backend_id="test-vdc")
        backend._current_waldur_resource = resource

        result = backend._create_vdc_resource("test-vdc", "Test", "Org")

        # VDC creation should still succeed
        assert result is True

    def test_existing_vdc_skips_creation(self):
        backend = _make_keycloak_backend()
        backend.client.get_resource.return_value = MagicMock()  # Already exists

        result = backend._create_vdc_resource("test-vdc", "Test", "Org")

        assert result is False
        backend.keycloak_client.create_group.assert_not_called()


# ── VDC delete with Keycloak ─────────────────────────────────────


class TestVDCDeleteWithKeycloak:
    """Test VDC deletion cleanup of Keycloak groups."""

    def test_deletion_cleans_up_keycloak_groups(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        parent_group = {
            "id": "parent-id",
            "name": "vdc_test-vdc",
            "subGroups": [
                {"id": "child-1", "name": "admin"},
                {"id": "child-2", "name": "user"},
            ],
        }
        backend.keycloak_client.get_group_by_name.return_value = parent_group

        mock_group = MagicMock()
        mock_group.ID = 101
        backend.client._get_group_by_name.return_value = mock_group

        with patch.object(backend, "_remove_saml_mapping_entries"):
            backend._pre_delete_resource(resource)

        # Children deleted
        assert backend.keycloak_client.delete_group.call_count >= 3  # 2 children + parent

    def test_deletion_cleans_up_one_groups(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.get_group_by_name.return_value = {
            "id": "parent-id", "subGroups": [],
        }
        mock_group = MagicMock()
        mock_group.ID = 101
        backend.client._get_group_by_name.return_value = mock_group

        with patch.object(backend, "_remove_saml_mapping_entries"):
            backend._pre_delete_resource(resource)

        # ONE groups: admins, users, cloud
        assert backend.client._delete_group.call_count == 3

    def test_keycloak_disabled_no_cleanup(self):
        backend = _make_backend({"create_opennebula_user": False})
        resource = _make_waldur_resource(backend_id="test-vdc")

        # Should not raise
        backend._pre_delete_resource(resource)


# ── add_user ─────────────────────────────────────────────────────


class TestAddUser:
    """Test add_user() method."""

    def _setup_vdc_groups(self, backend, parent_id="parent-id"):
        """Set up mock Keycloak groups for VDC 'test-vdc'."""
        backend.keycloak_client.get_group_by_name.return_value = {
            "id": parent_id, "name": "vdc_test-vdc",
        }
        backend.keycloak_client.keycloak_admin.get_group_children.return_value = [
            {"id": "group-admin", "name": "admin"},
            {"id": "group-user", "name": "user"},
            {"id": "group-cloud", "name": "cloud"},
        ]

    def test_adds_user_to_default_role_group(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = {"id": "user-123"}
        self._setup_vdc_groups(backend)

        result = backend.add_user(resource, "john")

        assert result is True
        backend.keycloak_client.add_user_to_group.assert_called_once_with(
            "user-123", "group-user"
        )

    def test_uses_specified_role(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = {"id": "user-123"}
        self._setup_vdc_groups(backend)

        result = backend.add_user(resource, "john", role="admin")

        assert result is True
        backend.keycloak_client.add_user_to_group.assert_called_once_with(
            "user-123", "group-admin"
        )

    def test_user_not_found_raises_error(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = None

        with pytest.raises(BackendError, match="not found in Keycloak"):
            backend.add_user(resource, "nonexistent")

    def test_creates_missing_groups(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = {"id": "user-123"}
        # Parent group not found -> _find_vdc_role_group returns None
        backend.keycloak_client.get_group_by_name.return_value = None
        # _create_keycloak_groups will create parent + children
        backend.keycloak_client.create_group.side_effect = [
            "parent-id", "child-admin", "child-user", "child-cloud"
        ]
        # After creating, get_group_children returns empty (groups just created)
        backend.keycloak_client.keycloak_admin.get_group_children.return_value = []

        result = backend.add_user(resource, "john")

        assert result is True
        # Groups were created (parent + 3 children)
        assert backend.keycloak_client.create_group.call_count == 4

    def test_keycloak_disabled_raises_error(self):
        backend = _make_backend()
        resource = _make_waldur_resource()

        with pytest.raises(BackendError, match="not enabled"):
            backend.add_user(resource, "john")


# ── remove_user ──────────────────────────────────────────────────


class TestRemoveUser:
    """Test remove_user() method."""

    def test_removes_from_all_role_groups(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = {"id": "user-123"}
        backend.keycloak_client.get_group_by_name.return_value = {
            "id": "parent-id", "name": "vdc_test-vdc",
        }
        backend.keycloak_client.keycloak_admin.get_group_children.return_value = [
            {"id": "group-admin", "name": "admin"},
            {"id": "group-user", "name": "user"},
            {"id": "group-cloud", "name": "cloud"},
        ]

        result = backend.remove_user(resource, "john")

        assert result is True
        # Called for each role (admin, user, cloud)
        assert backend.keycloak_client.remove_user_from_group.call_count == 3

    def test_user_not_found_returns_false(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = None

        result = backend.remove_user(resource, "nonexistent")

        assert result is False

    def test_group_not_found_skips_silently(self):
        backend = _make_keycloak_backend()
        resource = _make_waldur_resource(backend_id="test-vdc")

        backend.keycloak_client.find_user.return_value = {"id": "user-123"}
        # Parent group not found
        backend.keycloak_client.get_group_by_name.return_value = None

        result = backend.remove_user(resource, "john")

        assert result is True
        backend.keycloak_client.remove_user_from_group.assert_not_called()

    def test_keycloak_disabled_raises_error(self):
        backend = _make_backend()
        resource = _make_waldur_resource()

        with pytest.raises(BackendError, match="not enabled"):
            backend.remove_user(resource, "john")


# ── Ping / Diagnostics ──────────────────────────────────────────


class TestPingDiagnostics:
    """Test ping() and diagnostics() with Keycloak."""

    def test_ping_checks_opennebula_and_keycloak(self):
        backend = _make_keycloak_backend()
        backend.client.list_resources.return_value = []
        backend.keycloak_client.ping.return_value = True

        assert backend.ping() is True

    def test_ping_fails_when_keycloak_down(self):
        backend = _make_keycloak_backend()
        backend.client.list_resources.return_value = []
        backend.keycloak_client.ping.return_value = False

        assert backend.ping() is False

    def test_ping_fails_when_opennebula_down(self):
        backend = _make_keycloak_backend()
        backend.client.list_resources.side_effect = BackendError("down")

        assert backend.ping() is False

    def test_ping_raise_exception_keycloak(self):
        backend = _make_keycloak_backend()
        backend.client.list_resources.return_value = []
        backend.keycloak_client.ping.return_value = False

        with pytest.raises(BackendError, match="Keycloak"):
            backend.ping(raise_exception=True)

    def test_diagnostics_logs_keycloak_info(self, caplog):
        backend = _make_keycloak_backend()
        backend.client.list_resources.return_value = []
        backend.keycloak_client.ping.return_value = True

        import logging
        with caplog.at_level(logging.INFO):
            result = backend.diagnostics()

        assert result is True

    def test_ping_without_keycloak(self):
        backend = _make_backend()
        backend.client.list_resources.return_value = []

        assert backend.ping() is True


# ── Client group template methods ────────────────────────────────


class TestClientGroupTemplate:
    """Test _update_group_template() and _add_admin_to_group()."""

    def _make_client(self):
        """Create a real OpenNebulaClient with mocked pyone connection."""
        from waldur_site_agent_opennebula.client import OpenNebulaClient

        with patch("waldur_site_agent_opennebula.client.pyone.OneServer") as mock_server_cls:
            mock_one = MagicMock()
            mock_server_cls.return_value = mock_one
            client = OpenNebulaClient(
                api_url="http://localhost:2633/RPC2",
                credentials="oneadmin:testpass",
            )
        return client, mock_one

    def test_update_group_template_calls_api(self):
        client, mock_one = self._make_client()

        client._update_group_template(42, 'SAML_GROUP="/test"')

        mock_one.group.update.assert_called_once_with(42, 'SAML_GROUP="/test"', 1)

    def test_update_group_template_error(self):
        client, mock_one = self._make_client()
        mock_one.group.update.side_effect = pyone.OneException("fail")

        with pytest.raises(BackendError, match="Failed to update template"):
            client._update_group_template(42, "template")

    def test_add_admin_to_group_calls_api(self):
        client, mock_one = self._make_client()

        client._add_admin_to_group(42, 0)

        mock_one.group.addadmin.assert_called_once_with(42, 0)

    def test_add_admin_to_group_error(self):
        client, mock_one = self._make_client()
        mock_one.group.addadmin.side_effect = pyone.OneException("fail")

        with pytest.raises(BackendError, match="Failed to add admin"):
            client._add_admin_to_group(42, 0)


# ── Pre-create stores waldur resource ────────────────────────────


class TestPreCreateResource:
    """Test that _pre_create_resource stores the waldur resource."""

    def test_stores_current_waldur_resource(self):
        backend = _make_backend()
        resource = _make_waldur_resource()

        backend._pre_create_resource(resource)

        assert backend._current_waldur_resource is resource
