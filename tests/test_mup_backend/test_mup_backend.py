import unittest
import uuid
from unittest import mock
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from waldur_site_agent.backends.mup_backend.backend import MUPBackend
from waldur_site_agent.backends.mup_backend.client import MUPClient, MUPError
from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.backends.structures import Account, Association, Resource


class MUPBackendTest(unittest.TestCase):
    """Test suite for MUP backend functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mup_settings = {
            "api_url": "https://mup-api.example.com/api",
            "username": "test_user",
            "password": "test_password",
            "default_research_field": 1,
            "default_agency": "FCT",
            "project_prefix": "waldur_",
            "allocation_prefix": "alloc_",
            "default_allocation_type": "compute",
            "default_storage_limit": 1000,
        }

        self.mup_components = {
            "cpu": {
                "measured_unit": "core-hours",
                "unit_factor": 1,
                "accounting_type": "limit",
                "label": "CPU Cores",
            }
        }

        self.project_uuid = str(uuid.uuid4())
        self.resource_uuid = str(uuid.uuid4())
        self.customer_uuid = str(uuid.uuid4())

        self.sample_waldur_resource = {
            "uuid": self.resource_uuid,
            "name": "test-resource",
            "project_uuid": self.project_uuid,
            "project_name": "Test Project",
            "offering": {"uuid": str(uuid.uuid4()), "name": "MUP Offering"},
            "limits": {"cpu": 10},
        }

        self.sample_mup_project = {
            "id": 1,
            "title": "Test Project",
            "description": "A test project",
            "pi": "pi@example.com",
            "grant_number": f"waldur_{self.project_uuid}",
            "active": True,
            "agency": "FCT",
        }

        self.sample_mup_allocation = {
            "id": 1,
            "type": "compute",
            "identifier": f"alloc_{self.resource_uuid}",
            "size": 10,
            "used": 0,
            "active": True,
            "project": 1,
        }

        # Sample user context for testing
        self.sample_user_context = {
            "team": [
                {
                    "uuid": "user-uuid-1",
                    "email": "pi@example.com",
                    "first_name": "Principal",
                    "last_name": "Investigator",
                }
            ],
            "offering_users": [
                {
                    "username": "pi_user",
                    "user_uuid": "user-uuid-1",
                }
            ],
            "user_mappings": {
                "user-uuid-1": {
                    "uuid": "user-uuid-1",
                    "email": "pi@example.com",
                    "first_name": "Principal",
                    "last_name": "Investigator",
                }
            },
            "offering_user_mappings": {
                "user-uuid-1": {
                    "username": "pi_user",
                    "user_uuid": "user-uuid-1",
                }
            },
        }

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_init_with_valid_settings(self, mock_client_class):
        """Test backend initialization with valid settings"""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        self.assertEqual(backend.backend_type, "mup")
        self.assertEqual(backend.project_prefix, "waldur_")
        self.assertEqual(backend.allocation_prefix, "alloc_")
        self.assertEqual(backend.default_research_field, 1)
        self.assertEqual(backend.default_agency, "FCT")

        mock_client_class.assert_called_once_with(
            api_url="https://mup-api.example.com/api",
            username="test_user",
            password="test_password",
        )

    def test_init_missing_required_settings(self):
        """Test backend initialization fails with missing required settings"""
        incomplete_settings = {"api_url": "https://example.com"}

        with self.assertRaises(ValueError) as context:
            MUPBackend(incomplete_settings, self.mup_components)

        self.assertIn("Missing required setting: username", str(context.exception))

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_ping_success(self, mock_client_class):
        """Test successful ping to MUP backend"""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.return_value = [{"id": 1, "name": "Computer Science"}]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.ping()

        self.assertTrue(result)
        mock_client.get_research_fields.assert_called_once()

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_ping_failure(self, mock_client_class):
        """Test ping failure with MUP backend"""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.side_effect = MUPError("Connection failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.ping()

        self.assertFalse(result)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_ping_failure_with_exception(self, mock_client_class):
        """Test ping failure raises exception when requested"""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.side_effect = MUPError("Connection failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)

        with self.assertRaises(BackendError):
            backend.ping(raise_exception=True)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_list_components(self, mock_client_class):
        """Test listing backend components"""
        backend = MUPBackend(self.mup_settings, self.mup_components)
        components = backend.list_components()

        self.assertEqual(components, ["cpu"])

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_research_fields_caching(self, mock_client_class):
        """Test research fields caching"""
        mock_client = mock_client_class.return_value
        research_fields = [{"id": 1, "name": "Computer Science"}]
        mock_client.get_research_fields.return_value = research_fields

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # First call
        result1 = backend.get_research_fields()
        # Second call should use cache
        result2 = backend.get_research_fields()

        self.assertEqual(result1, research_fields)
        self.assertEqual(result2, research_fields)
        mock_client.get_research_fields.assert_called_once()

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_or_create_user_existing(self, mock_client_class):
        """Test getting existing user"""
        mock_client = mock_client_class.return_value
        existing_users = [{"id": 1, "email": "pi@example.com", "username": "pi_user"}]
        mock_client.get_users.return_value = existing_users

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_user = {
            "email": "pi@example.com",
            "username": "pi_user",
            "first_name": "Principal",
            "last_name": "Investigator",
        }

        user_id = backend._get_or_create_user(waldur_user)

        self.assertEqual(user_id, 1)
        mock_client.get_users.assert_called_once()
        mock_client.create_user_request.assert_not_called()

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_or_create_user_create_new(self, mock_client_class):
        """Test creating new user"""
        mock_client = mock_client_class.return_value
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 2}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_user = {
            "email": "new@example.com",
            "username": "new_user",
            "first_name": "New",
            "last_name": "User",
        }

        user_id = backend._get_or_create_user(waldur_user)

        self.assertEqual(user_id, 2)
        mock_client.get_users.assert_called_once()
        mock_client.create_user_request.assert_called_once()

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_or_create_user_no_email(self, mock_client_class):
        """Test handling user without email"""
        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_user = {"username": "user_without_email"}

        user_id = backend._get_or_create_user(waldur_user)

        self.assertIsNone(user_id)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_project_by_waldur_id_found(self, mock_client_class):
        """Test finding project by Waldur ID"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_waldur_id(self.project_uuid)

        self.assertEqual(project, self.sample_mup_project)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_project_by_waldur_id_not_found(self, mock_client_class):
        """Test project not found by Waldur ID"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = []

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_waldur_id("nonexistent_uuid")

        self.assertIsNone(project)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_mup_project_success(self, mock_client_class):
        """Test successful MUP project creation"""
        mock_client = mock_client_class.return_value
        mock_client.create_project.return_value = self.sample_mup_project

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_project = {
            "uuid": self.project_uuid,
            "name": "Test Project",
            "description": "A test project",
        }
        pi_email = "pi@example.com"

        result = backend._create_mup_project(waldur_project, pi_email)

        self.assertEqual(result, self.sample_mup_project)
        mock_client.create_project.assert_called_once()

        # Check project data structure
        call_args = mock_client.create_project.call_args[0][0]
        self.assertEqual(call_args["title"], "Test Project")
        self.assertEqual(call_args["pi"], "pi@example.com")
        self.assertEqual(call_args["grant_number"], f"waldur_{self.project_uuid}")

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_mup_project_failure(self, mock_client_class):
        """Test MUP project creation failure"""
        mock_client = mock_client_class.return_value
        mock_client.create_project.side_effect = MUPError("Project creation failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_project = {"uuid": self.project_uuid, "name": "Test Project"}
        pi_email = "pi@example.com"

        result = backend._create_mup_project(waldur_project, pi_email)

        self.assertIsNone(result)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_resource_success(self, mock_client_class):
        """Test successful resource creation"""
        mock_client = mock_client_class.return_value
        # Mock for user creation
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(self.sample_waldur_resource, self.sample_user_context)

        self.assertIsInstance(result, Resource)
        self.assertEqual(result.backend_type, "mup")
        self.assertEqual(result.marketplace_uuid, self.resource_uuid)
        self.assertEqual(result.backend_id, f"alloc_{self.resource_uuid}")
        self.assertEqual(result.limits["cpu"], 10)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_resource_no_project_uuid(self, mock_client_class):
        """Test resource creation failure with no project UUID"""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        waldur_resource = self.sample_waldur_resource.copy()
        del waldur_resource["project_uuid"]

        with self.assertRaises(BackendError) as context:
            backend.create_resource(waldur_resource)

        self.assertIn("No project UUID found", str(context.exception))

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_resource_project_activation(self, mock_client_class):
        """Test project activation during resource creation"""
        mock_client = mock_client_class.return_value
        # Mock for user creation
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 1}

        # Project exists but is inactive
        inactive_project = self.sample_mup_project.copy()
        inactive_project["active"] = False
        mock_client.get_projects.return_value = [inactive_project]
        mock_client.activate_project.return_value = {"status": "activated"}
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(self.sample_waldur_resource, self.sample_user_context)

        mock_client.activate_project.assert_called_once_with(1)
        self.assertIsInstance(result, Resource)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_resource_with_real_pi_from_context(self, mock_client_class):
        """Test resource creation uses real PI from user context"""
        mock_client = mock_client_class.return_value
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_projects.return_value = []  # No existing project
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(self.sample_waldur_resource, self.sample_user_context)

        # Verify project was created with real PI email from user context
        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        self.assertEqual(project_data["pi"], "pi@example.com")  # Real email from user context

        # Verify user was added to project during creation
        mock_client.add_project_member.assert_called()

        self.assertIsInstance(result, Resource)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_create_resource_without_user_context(self, mock_client_class):
        """Test resource creation falls back to default PI when no user context"""
        mock_client = mock_client_class.return_value
        mock_client.get_users.return_value = []
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_projects.return_value = []  # No existing project
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.create_allocation.return_value = self.sample_mup_allocation

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(self.sample_waldur_resource)  # No user context

        # Verify project was created with fallback PI email
        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        self.assertTrue(project_data["pi"].endswith(".example.com"))  # Fallback email

        # Verify no users were added during creation (since no context)
        mock_client.add_project_member.assert_not_called()

        self.assertIsInstance(result, Resource)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_collect_limits(self, mock_client_class):
        """Test limits collection"""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        waldur_resource = {"limits": {"cpu": 10}}

        allocation_limits, waldur_limits = backend._collect_limits(waldur_resource)

        self.assertEqual(allocation_limits["cpu"], 10)  # unit_factor = 1
        self.assertEqual(waldur_limits["cpu"], 10)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_collect_limits_with_dict_values(self, mock_client_class):
        """Test limits collection with dictionary values"""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        waldur_resource = {"limits": {"cpu": {"value": 20}}}

        allocation_limits, waldur_limits = backend._collect_limits(waldur_resource)

        self.assertEqual(allocation_limits["cpu"], 20)
        self.assertEqual(waldur_limits["cpu"], 20)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_resource_metadata(self, mock_client_class):
        """Test getting resource metadata"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        metadata = backend.get_resource_metadata(f"waldur_{self.project_uuid}")

        expected_metadata = {
            "mup_project_id": 1,
            "mup_allocation_id": 1,
            "allocation_type": "compute",
            "allocation_size": 10,
            "allocation_used": 0,
        }

        self.assertEqual(metadata, expected_metadata)

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_get_usage_report(self, mock_client_class):
        """Test usage report generation"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [
            {"id": 1, "type": "compute", "used": 5, "size": 10}
        ]
        mock_client.get_project_members.return_value = [
            {"id": 1, "active": True, "member": {"username": "user1", "email": "user1@example.com"}}
        ]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        accounts = [f"waldur_{self.project_uuid}"]
        report = backend._get_usage_report(accounts)

        account_key = f"waldur_{self.project_uuid}"
        self.assertIn(account_key, report)
        self.assertIn("TOTAL_ACCOUNT_USAGE", report[account_key])
        self.assertEqual(report[account_key]["TOTAL_ACCOUNT_USAGE"]["cpu"], 5)
        self.assertIn("user1", report[account_key])

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_add_users_to_resource(self, mock_client_class):
        """Test adding users to resource"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]
        mock_client.get_users.return_value = []  # User doesn't exist
        mock_client.create_user_request.return_value = {"id": 2}
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        user_ids = {"newuser@example.com"}
        resource_backend_id = f"alloc_{self.resource_uuid}"

        added_users = backend.add_users_to_resource(resource_backend_id, user_ids)

        self.assertEqual(added_users, user_ids)
        mock_client.add_project_member.assert_called_once()

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_remove_users_from_account(self, mock_client_class):
        """Test removing users from account"""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]
        mock_client.get_project_members.return_value = [
            {"id": 1, "active": True, "member": {"username": "user1", "email": "user1@example.com"}}
        ]
        mock_client.toggle_member_status.return_value = {"status": "deactivated"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        usernames = {"user1"}
        resource_backend_id = f"alloc_{self.resource_uuid}"

        removed_users = backend.remove_users_from_account(resource_backend_id, usernames)

        self.assertEqual(removed_users, ["user1"])
        mock_client.toggle_member_status.assert_called_once_with(1, 1, {"active": False})

    @patch("waldur_site_agent.backends.mup_backend.backend.MUPClient")
    def test_unsupported_operations_warning_only(self, mock_client_class):
        """Test that unsupported operations return False and log warnings"""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        # These operations are not supported by MUP but should not raise exceptions
        self.assertFalse(backend.downscale_resource("test_account"))
        self.assertFalse(backend.pause_resource("test_account"))
        self.assertFalse(backend.restore_resource("test_account"))


if __name__ == "__main__":
    unittest.main()
