import unittest
from unittest.mock import Mock, patch
import requests
import base64

from waldur_site_agent_mup.client import MUPClient, MUPError
from waldur_site_agent.backend.structures import ClientResource, Association


class MUPClientTest(unittest.TestCase):
    """Test suite for MUP client HTTP interactions"""

    def setUp(self):
        """Set up test fixtures"""
        self.api_url = "https://mup-api.example.com/api"
        self.username = "test_user"
        self.password = "test_password"

        # Create client instance
        with patch("requests.Session"):
            self.client = MUPClient(self.api_url, self.username, self.password)

        # Sample data
        self.sample_project = {
            "id": 1,
            "title": "Test Project",
            "description": "A test project",
            "pi": "pi@example.com",
            "grant_number": "waldur_test_project",
            "active": True,
            "agency": "FCT",
        }

        self.sample_allocation = {
            "id": 1,
            "type": "compute",
            "identifier": "alloc_test",
            "size": 10,
            "used": 5,
            "active": True,
            "project": 1,
        }

        self.sample_user = {
            "id": 1,
            "username": "testuser",
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "User",
        }

    def test_init_sets_authentication_headers(self):
        """Test that initialization sets up correct authentication headers"""
        with patch("requests.Session") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session

            client = MUPClient(self.api_url, self.username, self.password)

            # Check that session was configured
            mock_session_class.assert_called_once()

            # Verify headers were set
            expected_auth = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            mock_session.headers.update.assert_called_once_with(
                {
                    "Authorization": f"Basic {expected_auth}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )

    def test_make_request_success(self):
        """Test successful HTTP request"""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}

        self.client.session = Mock()
        self.client.session.request.return_value = mock_response

        response = self.client._make_request("GET", "/api/test")

        self.assertEqual(response, mock_response)
        self.client.session.request.assert_called_once_with(
            "GET", "https://mup-api.example.com/api/test"
        )
        mock_response.raise_for_status.assert_called_once()

    def test_make_request_http_error(self):
        """Test HTTP request with error response"""
        self.client.session = Mock()
        self.client.session.request.side_effect = requests.exceptions.HTTPError("404 Not Found")

        with self.assertRaises(MUPError) as context:
            self.client._make_request("GET", "/api/nonexistent")

        self.assertIn("API request failed", str(context.exception))

    def test_make_request_connection_error(self):
        """Test HTTP request with connection error"""
        self.client.session = Mock()
        self.client.session.request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        with self.assertRaises(MUPError) as context:
            self.client._make_request("GET", "/api/test")

        self.assertIn("API request failed", str(context.exception))

    def test_get_projects(self):
        """Test getting projects list"""
        expected_projects = [self.sample_project]

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = expected_projects
            mock_request.return_value = mock_response

            result = self.client.get_projects()

            self.assertEqual(result, expected_projects)
            mock_request.assert_called_once_with("GET", "/api/projects/list/")

    def test_get_project(self):
        """Test getting specific project"""
        project_id = 1

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_project
            mock_request.return_value = mock_response

            result = self.client.get_project(project_id)

            self.assertEqual(result, self.sample_project)
            mock_request.assert_called_once_with("GET", f"/api/projects/view/{project_id}")

    def test_create_project(self):
        """Test creating new project"""
        project_data = {
            "title": "New Project",
            "description": "A new project",
            "pi": "pi@example.com",
        }

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_project
            mock_request.return_value = mock_response

            result = self.client.create_project(project_data)

            self.assertEqual(result, self.sample_project)
            mock_request.assert_called_once_with("POST", "/api/projects/add/", json=project_data)

    def test_update_project(self):
        """Test updating existing project"""
        project_id = 1
        project_data = {"title": "Updated Project"}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_project
            mock_request.return_value = mock_response

            result = self.client.update_project(project_id, project_data)

            self.assertEqual(result, self.sample_project)
            mock_request.assert_called_once_with(
                "PUT", f"/api/projects/{project_id}/edit", json=project_data
            )

    def test_activate_project(self):
        """Test activating project"""
        project_id = 1

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"status": "activated"}
            mock_request.return_value = mock_response

            result = self.client.activate_project(project_id)

            self.assertEqual(result, {"status": "activated"})
            mock_request.assert_called_once_with(
                "PUT", f"/api/projects/{project_id}/activate", json={}
            )

    def test_deactivate_project(self):
        """Test deactivating project"""
        project_id = 1

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"status": "deactivated"}
            mock_request.return_value = mock_response

            result = self.client.deactivate_project(project_id)

            self.assertEqual(result, {"status": "deactivated"})
            mock_request.assert_called_once_with(
                "PUT", f"/api/projects/{project_id}/deactivate", json={}
            )

    def test_get_project_allocations(self):
        """Test getting project allocations"""
        project_id = 1
        expected_allocations = [self.sample_allocation]

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = expected_allocations
            mock_request.return_value = mock_response

            result = self.client.get_project_allocations(project_id)

            self.assertEqual(result, expected_allocations)
            mock_request.assert_called_once_with(
                "GET", f"/api/projects/{project_id}/allocations/list"
            )

    def test_create_allocation(self):
        """Test creating allocation"""
        project_id = 1
        allocation_data = {"type": "compute", "identifier": "alloc_new", "size": 20}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_allocation
            mock_request.return_value = mock_response

            result = self.client.create_allocation(project_id, allocation_data)

            self.assertEqual(result, self.sample_allocation)
            mock_request.assert_called_once_with(
                "POST", f"/api/projects/{project_id}/allocations/add", json=allocation_data
            )

    def test_update_allocation(self):
        """Test updating allocation"""
        project_id = 1
        allocation_id = 1
        allocation_data = {"size": 30}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_allocation
            mock_request.return_value = mock_response

            result = self.client.update_allocation(project_id, allocation_id, allocation_data)

            self.assertEqual(result, self.sample_allocation)
            mock_request.assert_called_once_with(
                "PUT",
                f"/api/projects/{project_id}/allocations/edit/{allocation_id}",
                json=allocation_data,
            )

    def test_get_allocation(self):
        """Test getting specific allocation"""
        project_id = 1
        allocation_id = 1

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_allocation
            mock_request.return_value = mock_response

            result = self.client.get_allocation(project_id, allocation_id)

            self.assertEqual(result, self.sample_allocation)
            mock_request.assert_called_once_with(
                "GET", f"/api/projects/{project_id}/allocations/view/{allocation_id}"
            )

    def test_get_project_members(self):
        """Test getting project members"""
        project_id = 1
        expected_members = [{"id": 1, "active": True, "member": self.sample_user}]

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = expected_members
            mock_request.return_value = mock_response

            result = self.client.get_project_members(project_id)

            self.assertEqual(result, expected_members)
            mock_request.assert_called_once_with("GET", f"/api/projects/{project_id}/members/list")

    def test_add_project_member(self):
        """Test adding project member"""
        project_id = 1
        member_data = {"user_id": 1, "active": True}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"status": "added"}
            mock_request.return_value = mock_response

            result = self.client.add_project_member(project_id, member_data)

            self.assertEqual(result, {"status": "added"})
            mock_request.assert_called_once_with(
                "POST", f"/api/projects/{project_id}/members/add", json=member_data
            )

    def test_toggle_member_status(self):
        """Test toggling member status"""
        project_id = 1
        member_id = 1
        status_data = {"active": False}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"status": "updated"}
            mock_request.return_value = mock_response

            result = self.client.toggle_member_status(project_id, member_id, status_data)

            self.assertEqual(result, {"status": "updated"})
            mock_request.assert_called_once_with(
                "PUT",
                f"/api/projects/{project_id}/members/{member_id}/toggle-status",
                json=status_data,
            )

    def test_get_users(self):
        """Test getting users list"""
        expected_users = [self.sample_user]

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = expected_users
            mock_request.return_value = mock_response

            result = self.client.get_users()

            self.assertEqual(result, expected_users)
            mock_request.assert_called_once_with("GET", "/api/user/list/")

    def test_get_user(self):
        """Test getting specific user"""
        user_id = 1

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_user
            mock_request.return_value = mock_response

            result = self.client.get_user(user_id)

            self.assertEqual(result, self.sample_user)
            mock_request.assert_called_once_with("GET", f"/api/user/view/{user_id}")

    def test_create_user_request(self):
        """Test creating user registration request"""
        user_data = {
            "username": "newuser",
            "email": "new@example.com",
            "first_name": "New",
            "last_name": "User",
        }

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = {"id": 2, "status": "pending"}
            mock_request.return_value = mock_response

            result = self.client.create_user_request(user_data)

            self.assertEqual(result, {"id": 2, "status": "pending"})
            mock_request.assert_called_once_with("POST", "/api/user/add/", json=user_data)

    def test_update_user(self):
        """Test updating user information"""
        user_id = 1
        user_data = {"first_name": "Updated"}

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = self.sample_user
            mock_request.return_value = mock_response

            result = self.client.update_user(user_id, user_data)

            self.assertEqual(result, self.sample_user)
            mock_request.assert_called_once_with("PUT", f"/api/user/edit/{user_id}", json=user_data)

    def test_get_research_fields(self):
        """Test getting research fields"""
        expected_fields = [{"id": 1, "name": "Computer Science"}, {"id": 2, "name": "Physics"}]

        with patch.object(self.client, "_make_request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = expected_fields
            mock_request.return_value = mock_response

            result = self.client.get_research_fields()

            self.assertEqual(result, expected_fields)
            mock_request.assert_called_once_with("GET", "/api/research-fields/list/")

    def test_list_resources(self):
        """Test list_resources interface method"""
        projects = [self.sample_project]

        with patch.object(self.client, "get_projects", return_value=projects):
            accounts = self.client.list_resources()

            self.assertEqual(len(accounts), 1)
            account = accounts[0]
            self.assertIsInstance(account, ClientResource)
            self.assertEqual(account.name, "waldur_test_project")
            self.assertEqual(account.description, "Test Project")
            self.assertEqual(account.organization, "FCT")

    def test_get_resource_found(self):
        """Test get_resource interface method - account found"""
        projects = [self.sample_project]

        with patch.object(self.client, "get_projects", return_value=projects):
            account = self.client.get_resource("waldur_test_project")

            self.assertIsNotNone(account)
            self.assertIsInstance(account, ClientResource)
            self.assertEqual(account.name, "waldur_test_project")

    def test_get_resource_not_found(self):
        """Test get_resource interface method - account not found"""
        projects = [self.sample_project]

        with patch.object(self.client, "get_projects", return_value=projects):
            account = self.client.get_resource("nonexistent_project")

            self.assertIsNone(account)

    def test_create_resource(self):
        """Test create_resource interface method"""
        result = self.client.create_resource("test_account", "Test Account", "Test Org")

        # This method just returns the name as it's handled by backend
        self.assertEqual(result, "test_account")

    def test_delete_resource(self):
        """Test delete_resource interface method"""
        projects = [self.sample_project]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "deactivate_project") as mock_deactivate:
                result = self.client.delete_resource("waldur_test_project")

                self.assertEqual(result, "waldur_test_project")
                mock_deactivate.assert_called_once_with(1)

    def test_set_resource_limits(self):
        """Test set_resource_limits interface method"""
        projects = [self.sample_project]
        allocations = [self.sample_allocation]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_allocations", return_value=allocations):
                with patch.object(self.client, "update_allocation") as mock_update:
                    result = self.client.set_resource_limits("waldur_test_project", {"cpu": 20})

                    self.assertIn("Updated allocation size to 20", result)
                    mock_update.assert_called_once()

    def test_get_resource_limits(self):
        """Test get_resource_limits interface method"""
        projects = [self.sample_project]
        allocations = [self.sample_allocation]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_allocations", return_value=allocations):
                limits = self.client.get_resource_limits("waldur_test_project")

                self.assertEqual(limits, {"cpu": 10})

    def test_get_resource_user_limits(self):
        """Test get_resource_user_limits interface method"""
        result = self.client.get_resource_user_limits("test_account")

        # MUP doesn't support per-user limits
        self.assertEqual(result, {})

    def test_set_resource_user_limits(self):
        """Test set_resource_user_limits interface method"""
        result = self.client.set_resource_user_limits("test_account", "user1", {"cpu": 5})

        # MUP doesn't support per-user limits
        self.assertEqual(result, "User limits not supported for user1")

    def test_get_association_found(self):
        """Test get_association interface method - association found"""
        projects = [self.sample_project]
        members = [
            {
                "id": 1,
                "active": True,
                "member": {"username": "testuser", "email": "test@example.com"},
            }
        ]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_members", return_value=members):
                association = self.client.get_association("testuser", "waldur_test_project")

                self.assertIsNotNone(association)
                self.assertIsInstance(association, Association)
                self.assertEqual(association.account, "waldur_test_project")
                self.assertEqual(association.user, "testuser")
                self.assertEqual(association.value, 1)

    def test_get_association_not_found(self):
        """Test get_association interface method - association not found"""
        projects = [self.sample_project]
        members = []

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_members", return_value=members):
                association = self.client.get_association("nonexistent", "waldur_test_project")

                self.assertIsNone(association)

    def test_create_association(self):
        """Test create_association interface method"""
        result = self.client.create_association("user1", "test_account")

        # This method returns a message as it's handled by backend
        self.assertEqual(result, "Association created for user1 in test_account")

    def test_delete_association(self):
        """Test delete_association interface method"""
        result = self.client.delete_association("user1", "test_account")

        # This method returns a message as it's handled by backend
        self.assertEqual(result, "Association deleted for user1 from test_account")

    def test_get_usage_report(self):
        """Test get_usage_report interface method"""
        projects = [self.sample_project]
        allocations = [self.sample_allocation]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_allocations", return_value=allocations):
                usage_data = self.client.get_usage_report(["waldur_test_project"])

                self.assertEqual(len(usage_data), 1)
                self.assertEqual(usage_data[0]["account"], "waldur_test_project")
                self.assertEqual(usage_data[0]["used"], 5)
                self.assertEqual(usage_data[0]["total"], 10)

    def test_list_account_users(self):
        """Test list_account_users interface method"""
        projects = [self.sample_project]
        members = [
            {
                "id": 1,
                "active": True,
                "member": {"username": "user1", "email": "user1@example.com"},
            },
            {
                "id": 2,
                "active": False,
                "member": {"username": "user2", "email": "user2@example.com"},
            },
        ]

        with patch.object(self.client, "get_projects", return_value=projects):
            with patch.object(self.client, "get_project_members", return_value=members):
                users = self.client.list_resource_users("waldur_test_project")

                # Should only return active users
                self.assertEqual(users, ["user1"])


if __name__ == "__main__":
    unittest.main()
