"""Tests for Keycloak client implementation."""

import pytest
from unittest.mock import MagicMock, patch
from keycloak.exceptions import KeycloakError

from waldur_site_agent_rancher.keycloak_client import KeycloakClient
from waldur_site_agent.backend.exceptions import BackendError


@pytest.fixture
def keycloak_settings():
    """Basic Keycloak settings for testing."""
    return {
        "keycloak_url": "https://keycloak.example.com/auth/",
        "keycloak_realm": "test",
        "client_id": "admin-cli",
        "keycloak_username": "admin",
        "keycloak_password": "test-password",
        "keycloak_ssl_verify": False,
    }


class TestKeycloakClient:
    """Test cases for KeycloakClient."""

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_initialization(self, mock_connection, mock_admin, keycloak_settings):
        """Test client initialization."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        assert client.server_url == "https://keycloak.example.com/auth/"
        assert client.realm_name == "test"
        assert client.client_id == "admin-cli"
        assert client.username == "admin"
        assert client.password == "test-password"
        assert client.verify_cert is False

        mock_connection.assert_called_once()
        mock_admin.assert_called_once()

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_initialization_failure(self, mock_connection, mock_admin, keycloak_settings):
        """Test initialization failure handling."""
        mock_connection.side_effect = Exception("Connection failed")

        with pytest.raises(BackendError, match="Failed to initialize Keycloak client"):
            KeycloakClient(keycloak_settings)

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_ping_success(self, mock_connection, mock_admin, keycloak_settings):
        """Test successful ping operation."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_realm.return_value = {"realm": "test"}
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        assert client.ping() is True
        mock_admin_instance.get_realm.assert_called_once_with("test")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_ping_failure(self, mock_connection, mock_admin, keycloak_settings):
        """Test ping failure."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_realm.side_effect = KeycloakError("Connection failed")
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        assert client.ping() is False

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_find_user_by_username(self, mock_connection, mock_admin, keycloak_settings):
        """Test finding user by username."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_users.return_value = [
            {"id": "user-123", "username": "testuser", "email": "test@example.com"}
        ]
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        user = client.find_user_by_username("testuser")

        assert user is not None
        assert user["id"] == "user-123"
        assert user["username"] == "testuser"

        mock_admin_instance.get_users.assert_called_once_with(
            {"username": "testuser", "exact": True}
        )

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_find_user_not_found(self, mock_connection, mock_admin, keycloak_settings):
        """Test finding user that doesn't exist."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_users.return_value = []
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        user = client.find_user_by_username("nonexistent")

        assert user is None

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_create_group(self, mock_connection, mock_admin, keycloak_settings):
        """Test group creation."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.create_group.return_value = "group-123"
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        group_id = client.create_group("test-group", "Test group description")

        assert group_id == "group-123"

        # Check that create_group was called with correct data
        call_args = mock_admin_instance.create_group.call_args[0][0]
        assert call_args["name"] == "test-group"
        assert call_args["attributes"]["description"] == ["Test group description"]
        assert call_args["attributes"]["managed_by"] == ["waldur-site-agent"]

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_create_subgroup(self, mock_connection, mock_admin, keycloak_settings):
        """Test subgroup creation."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.create_group.return_value = "subgroup-123"
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        group_id = client.create_group("test-subgroup", "Test subgroup", "parent-123")

        assert group_id == "subgroup-123"

        # Check that create_group was called with parent parameter
        call_args = mock_admin_instance.create_group.call_args
        assert call_args[1]["parent"] == "parent-123"

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_get_group_by_name(self, mock_connection, mock_admin, keycloak_settings):
        """Test getting group by name."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_groups.return_value = [
            {
                "id": "group-123",
                "name": "test-group",
                "subGroups": [{"id": "subgroup-123", "name": "test-subgroup"}],
            },
            {"id": "group-456", "name": "other-group", "subGroups": []},
        ]
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        # Test finding top-level group
        group = client.get_group_by_name("test-group")
        assert group is not None
        assert group["id"] == "group-123"

        # Test finding subgroup
        subgroup = client.get_group_by_name("test-subgroup")
        assert subgroup is not None
        assert subgroup["id"] == "subgroup-123"

        # Test group not found
        missing = client.get_group_by_name("missing-group")
        assert missing is None

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_delete_group(self, mock_connection, mock_admin, keycloak_settings):
        """Test group deletion."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        client.delete_group("group-123")

        mock_admin_instance.delete_group.assert_called_once_with("group-123")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_add_user_to_group(self, mock_connection, mock_admin, keycloak_settings):
        """Test adding user to group."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        client.add_user_to_group("user-123", "group-123")

        mock_admin_instance.group_user_add.assert_called_once_with("user-123", "group-123")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_remove_user_from_group(self, mock_connection, mock_admin, keycloak_settings):
        """Test removing user from group."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        client.remove_user_from_group("user-123", "group-123")

        mock_admin_instance.group_user_remove.assert_called_once_with("user-123", "group-123")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_create_project_groups(self, mock_connection, mock_admin, keycloak_settings):
        """Test creating project group structure."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.get_groups.return_value = []  # No existing groups
        mock_admin_instance.create_group.side_effect = ["parent-123", "child-123"]
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)
        parent_id, child_id = client.create_project_groups(
            "test-project", "Test project description"
        )

        assert parent_id == "parent-123"
        assert child_id == "child-123"

        # Check that both groups were created
        assert mock_admin_instance.create_group.call_count == 2

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_create_project_groups_existing(self, mock_connection, mock_admin, keycloak_settings):
        """Test creating project groups when they already exist."""
        mock_admin_instance = MagicMock()
        # Mock get_groups to return existing groups
        mock_admin_instance.get_groups.return_value = [
            {
                "id": "existing-parent",
                "name": "project-test-project",
                "subGroups": [{"id": "existing-child", "name": "test-project"}],
            }
        ]
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        # Mock get_group_by_name to return existing groups
        with patch.object(client, "get_group_by_name") as mock_get:
            mock_get.side_effect = [
                {"id": "existing-parent"},  # parent group
                {"id": "existing-child"},  # child group
            ]

            parent_id, child_id = client.create_project_groups("test-project")

            assert parent_id == "existing-parent"
            assert child_id == "existing-child"

            # Should not create new groups
            mock_admin_instance.create_group.assert_not_called()

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_delete_project_groups(self, mock_connection, mock_admin, keycloak_settings):
        """Test deleting project groups."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        # Mock get_group_by_name to return existing groups
        with (
            patch.object(client, "get_group_by_name") as mock_get,
            patch.object(client, "delete_group") as mock_delete,
        ):
            mock_get.side_effect = [
                {"id": "child-123"},  # child group
                {"id": "parent-123"},  # parent group
            ]

            client.delete_project_groups("test-project")

            # Should delete both groups
            assert mock_delete.call_count == 2
            mock_delete.assert_any_call("child-123")
            mock_delete.assert_any_call("parent-123")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_ensure_project_group_exists(self, mock_connection, mock_admin, keycloak_settings):
        """Test ensuring project group exists."""
        mock_admin_instance = MagicMock()
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        # Test with existing group
        with patch.object(client, "get_group_by_name") as mock_get:
            mock_get.return_value = {"id": "existing-123"}

            group_id = client.ensure_project_group_exists("test-project")
            assert group_id == "existing-123"

        # Test with non-existing group
        with (
            patch.object(client, "get_group_by_name") as mock_get,
            patch.object(client, "create_project_groups") as mock_create,
        ):
            mock_get.return_value = None
            mock_create.return_value = ("parent-123", "child-123")

            group_id = client.ensure_project_group_exists("test-project", "Test description")
            assert group_id == "child-123"
            mock_create.assert_called_once_with("test-project", "Test description")

    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakAdmin")
    @patch("waldur_site_agent_rancher.keycloak_client.KeycloakOpenIDConnection")
    def test_error_handling(self, mock_connection, mock_admin, keycloak_settings):
        """Test error handling in various operations."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.create_group.side_effect = KeycloakError("Creation failed")
        mock_admin.return_value = mock_admin_instance

        client = KeycloakClient(keycloak_settings)

        with pytest.raises(BackendError, match="Failed to create group"):
            client.create_group("test-group")

        mock_admin_instance.delete_group.side_effect = KeycloakError("Deletion failed")

        with pytest.raises(BackendError, match="Failed to delete group"):
            client.delete_group("group-123")

        mock_admin_instance.group_user_add.side_effect = KeycloakError("Add failed")

        with pytest.raises(BackendError, match="Failed to add user to group"):
            client.add_user_to_group("user-123", "group-123")
