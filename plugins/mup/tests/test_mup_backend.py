"""Tests for MUP backend functionality."""

import unittest
import uuid
from unittest.mock import Mock, patch

import pytest
from waldur_api_client.models import OfferingUserState
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.project_user import ProjectUser
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_api_client.types import Unset
from waldur_site_agent_mup.backend import MUPBackend
from waldur_site_agent_mup.client import MUPError

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo


class MUPBackendTest(unittest.TestCase):
    """Test suite for MUP backend functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
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

        # Settings with custom user creation defaults for testing
        self.custom_user_settings = {
            **self.mup_settings,
            "default_user_salutation": "Prof.",
            "default_user_gender": "Female",
            "default_user_birth_year": 1985,
            "default_user_country": "Germany",
            "default_user_institution_type": "Industry",
            "default_user_institution": "Max Planck Institute",
            "default_user_biography": "Custom researcher profile",
            "user_funding_agency_prefix": "CUSTOM-PREFIX-",
        }

        self.mup_components = {
            "cpu": {
                "measured_unit": "core-hours",
                "unit_factor": 1,
                "accounting_type": "limit",
                "label": "CPU Cores",
                "mup_allocation_type": "Deucalion x86_64",
            }
        }

        self.project_uuid = uuid.uuid4()
        self.resource_uuid = uuid.uuid4()
        self.customer_uuid = uuid.uuid4()

        self.sample_waldur_resource = WaldurResource(
            uuid=self.resource_uuid,
            name="test-resource",
            project_uuid=self.project_uuid,
            project_name="Test Project",
            offering_uuid=uuid.uuid4(),
            offering_name="MUP Offering",
            limits=ResourceLimits.from_dict({"cpu": 10}),
        )

        self.sample_mup_project = {
            "id": 1,
            "title": "Test Project",
            "description": "A test project",
            "pi": "pi@example.com",
            "grant_number": f"waldur_{self.resource_uuid.hex}",
            "active": True,
            "agency": "FCT",
        }

        self.sample_mup_allocation = {
            "id": 1,
            "type": "compute",
            "identifier": f"alloc_{self.resource_uuid.hex}",
            "size": 10,
            "used": 0,
            "active": True,
            "project": 1,
        }

        # Sample user context for testing
        self.sample_user_context = {
            "team": [
                ProjectUser(
                    uuid="user-uuid-1",
                    email="pi@example.com",
                    username="pi_user",
                    full_name="Principal Investigator",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="pi_user",
                    offering_user_state=OfferingUserState.OK,
                )
            ],
            "offering_users": [
                OfferingUser(
                    username="pi_user",
                    user_uuid="user-uuid-1",
                    offering_uuid="offering-uuid-1",
                    created="2024-01-01T00:00:00Z",
                    modified="2024-01-01T00:00:00Z",
                )
            ],
            "user_mappings": {
                "user-uuid-1": ProjectUser(
                    uuid="user-uuid-1",
                    email="pi@example.com",
                    username="pi_user",
                    full_name="Principal Investigator",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="pi_user",
                    offering_user_state=OfferingUserState.OK,
                )
            },
            "offering_user_mappings": {
                "user-uuid-1": OfferingUser(
                    username="pi_user",
                    user_uuid="user-uuid-1",
                    offering_uuid="offering-uuid-1",
                    created="2025-01-01T00:00:00Z",
                    modified="2025-01-01T00:00:00Z",
                )
            },
        }

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_init_with_valid_settings(self, mock_client_class) -> None:
        """Test backend initialization with valid settings."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        assert backend.backend_type == "mup"
        assert backend.project_prefix == "waldur_"
        assert backend.allocation_prefix == "alloc_"
        assert backend.default_research_field == 1
        assert backend.default_agency == "FCT"

        mock_client_class.assert_called_once_with(
            api_url="https://mup-api.example.com/api",
            username="test_user",
            password="test_password",
        )

    def test_init_missing_required_settings(self) -> None:
        """Test backend initialization fails with missing required settings."""
        incomplete_settings = {"api_url": "https://example.com"}

        with pytest.raises(ValueError) as context:
            MUPBackend(incomplete_settings, self.mup_components)

        assert "Missing required setting: username" in str(context.value)

    def test_init_invalid_accounting_type(self) -> None:
        """Test backend initialization fails with invalid accounting_type."""
        invalid_components = {
            "cpu": {
                "measured_unit": "core-hours",
                "unit_factor": 1,
                "accounting_type": "usage",  # Invalid for MUP
                "label": "CPU Cores",
                "mup_allocation_type": "Deucalion x86_64",
            }
        }

        with pytest.raises(ValueError) as context:
            MUPBackend(self.mup_settings, invalid_components)

        assert (
            "MUP backend only supports components with accounting_type='limit'"
            in str(context.value)
        )
        assert "Component 'cpu' has accounting_type='usage'" in str(context.value)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_ping_success(self, mock_client_class) -> None:
        """Test successful ping to MUP backend."""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.return_value = [
            {"id": 1, "name": "Computer Science"}
        ]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.ping()

        assert result
        mock_client.get_research_fields.assert_called_once()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_ping_failure(self, mock_client_class) -> None:
        """Test ping failure with MUP backend."""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.side_effect = MUPError("Connection failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.ping()

        assert not result

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_ping_failure_with_exception(self, mock_client_class) -> None:
        """Test ping failure raises exception when requested."""
        mock_client = mock_client_class.return_value
        mock_client.get_research_fields.side_effect = MUPError("Connection failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)

        with pytest.raises(BackendError):
            backend.ping(raise_exception=True)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_list_components(self, mock_client_class) -> None:
        """Test listing backend components."""
        backend = MUPBackend(self.mup_settings, self.mup_components)
        components = backend.list_components()

        assert components == ["cpu"]

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_research_fields_caching(self, mock_client_class) -> None:
        """Test research fields caching."""
        mock_client = mock_client_class.return_value
        research_fields = [{"id": 1, "name": "Computer Science"}]
        mock_client.get_research_fields.return_value = research_fields

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # First call
        result1 = backend.get_research_fields()
        # Second call should use cache
        result2 = backend.get_research_fields()

        assert result1 == research_fields
        assert result2 == research_fields
        mock_client.get_research_fields.assert_called_once()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_or_create_user_existing(self, mock_client_class) -> None:
        """Test getting existing user."""
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

        assert user_id == 1
        mock_client.get_users.assert_called_once()
        mock_client.create_user_request.assert_not_called()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_or_create_user_create_new(self, mock_client_class) -> None:
        """Test creating new user."""
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

        assert user_id == 2
        mock_client.get_users.assert_called_once()
        mock_client.create_user_request.assert_called_once()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_or_create_user_no_email(self, mock_client_class) -> None:
        """Test handling user without email."""
        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_user = {"username": "user_without_email"}

        user_id = backend._get_or_create_user(waldur_user)

        assert user_id is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_project_by_waldur_id_found(self, mock_client_class) -> None:
        """Test finding project by Waldur ID."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_waldur_id(self.resource_uuid.hex)

        assert project == self.sample_mup_project

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_project_by_waldur_id_not_found(self, mock_client_class) -> None:
        """Test project not found by Waldur ID."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = []

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_waldur_id("nonexistent_uuid")

        assert project is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_mup_project_success(self, mock_client_class) -> None:
        """Test successful MUP project creation."""
        mock_client = mock_client_class.return_value
        mock_client.create_project.return_value = self.sample_mup_project

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_project = {
            "uuid": self.project_uuid.hex,
            "name": "Test Project",
            "description": "A test project",
        }
        pi_email = "pi@example.com"

        result = backend._create_mup_project(waldur_project, pi_email)

        assert result == self.sample_mup_project
        mock_client.create_project.assert_called_once()

        # Check project data structure
        call_args = mock_client.create_project.call_args[0][0]
        assert call_args["title"] == "Test Project"
        assert call_args["pi"] == "pi@example.com"
        assert call_args["grant_number"] == f"waldur_{self.project_uuid.hex}"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_mup_project_failure(self, mock_client_class) -> None:
        """Test MUP project creation failure."""
        mock_client = mock_client_class.return_value
        mock_client.create_project.side_effect = MUPError("Project creation failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_project = {"uuid": self.project_uuid.hex, "name": "Test Project"}
        pi_email = "pi@example.com"

        result = backend._create_mup_project(waldur_project, pi_email)

        assert result is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_success(self, mock_client_class) -> None:
        """Test successful resource creation."""
        mock_client = mock_client_class.return_value
        # Mock for user creation
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        assert isinstance(result, BackendResourceInfo)
        assert result.backend_id == "1"
        assert result.limits["cpu"] == 10

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_no_project_uuid(self, mock_client_class) -> None:
        """Test resource creation failure with no project UUID."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        waldur_resource = self.sample_waldur_resource
        waldur_resource.project_uuid = Unset()

        with pytest.raises(BackendError) as context:
            backend.create_resource(waldur_resource)

        assert "No project UUID found" in str(context.value)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_project_activation(self, mock_client_class) -> None:
        """Test project activation during resource creation."""
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
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        mock_client.activate_project.assert_called_once_with(1)
        assert isinstance(result, BackendResourceInfo)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_with_real_pi_from_context(self, mock_client_class) -> None:
        """Test resource creation uses real PI from user context."""
        mock_client = mock_client_class.return_value
        mock_client.get_users.return_value = []  # No existing users
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_projects.return_value = []  # No existing project
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        # Verify project was created with real PI email from user context
        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        assert project_data["pi"] == "pi@example.com"  # Real email from user context

        # Verify user was added to project during creation
        mock_client.add_project_member.assert_called()

        assert isinstance(result, BackendResourceInfo)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_without_user_context(self, mock_client_class) -> None:
        """Test resource creation falls back to default PI when no user context."""
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
        assert project_data["pi"].endswith(".example.com")  # Fallback email

        # Verify no users were added during creation (since no context)
        mock_client.add_project_member.assert_not_called()

        assert isinstance(result, BackendResourceInfo)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_collect_limits(self, mock_client_class) -> None:
        """Test limits collection."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        waldur_resource = WaldurResource(limits=ResourceLimits.from_dict({"cpu": 10}))

        allocation_limits, waldur_limits = backend._collect_resource_limits(
            waldur_resource
        )

        assert allocation_limits["cpu"] == 10  # unit_factor = 1
        assert waldur_limits["cpu"] == 10

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_resource_metadata(self, mock_client_class) -> None:
        """Test getting resource metadata."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        metadata = backend.get_resource_metadata("1")

        expected_metadata = {
            "mup_project_id": 1,
            "mup_allocation_id": 1,
            "allocation_type": "compute",
            "allocation_size": 10,
            "allocation_used": 0,
        }

        assert metadata == expected_metadata

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_usage_report(self, mock_client_class) -> None:
        """Test usage report generation."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [
            {"id": 1, "type": "compute", "used": 5, "size": 10}
        ]
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "member": {"username": "user1", "email": "user1@example.com"},
            }
        ]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        accounts = ["1"]
        report = backend._get_usage_report(accounts)

        account_key = "1"
        assert account_key in report
        assert "TOTAL_ACCOUNT_USAGE" in report[account_key]
        assert report[account_key]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 5
        assert "user1" in report[account_key]

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_to_resource(self, mock_client_class) -> None:
        """Test adding users to resource."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]
        mock_client.get_users.return_value = []  # User doesn't exist
        mock_client.create_user_request.return_value = {"id": 2}
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        user_ids = {"newuser@example.com"}
        resource_backend_id = "1"

        added_users = backend.add_users_to_resource(resource_backend_id, user_ids)

        assert added_users == user_ids
        mock_client.add_project_member.assert_called_once()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_remove_users_from_account(self, mock_client_class) -> None:
        """Test removing users from account."""
        mock_client = mock_client_class.return_value
        mock_client.get_projects.return_value = [self.sample_mup_project]
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "member": {"username": "user1", "email": "user1@example.com"},
            }
        ]
        mock_client.toggle_member_status.return_value = {"status": "deactivated"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        usernames = {"user1"}
        resource_backend_id = "1"

        removed_users = backend.remove_users_from_resource(
            resource_backend_id, usernames
        )

        assert removed_users == ["user1"]
        mock_client.toggle_member_status.assert_called_once_with(
            1, 1, {"active": False}
        )

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_unsupported_operations_warning_only(self, mock_client_class) -> None:
        """Test that unsupported operations return False and log warnings."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        # These operations are not supported by MUP but should not raise exceptions
        assert not backend.downscale_resource("test_account")
        assert not backend.pause_resource("test_account")
        assert not backend.restore_resource("test_account")

    def test_configurable_user_creation_defaults(self) -> None:
        """Test that user creation defaults are configurable through backend_settings."""
        backend = MUPBackend(self.custom_user_settings, self.mup_components)

        # Verify that custom settings are loaded correctly
        assert backend.user_defaults["salutation"] == "Prof."
        assert backend.user_defaults["gender"] == "Female"
        assert backend.user_defaults["year_of_birth"] == 1985
        assert backend.user_defaults["country"] == "Germany"
        assert backend.user_defaults["type_of_institution"] == "Industry"
        assert backend.user_defaults["affiliated_institution"] == "Max Planck Institute"
        assert backend.user_defaults["biography"] == "Custom researcher profile"
        assert backend.user_defaults["funding_agency_prefix"] == "CUSTOM-PREFIX-"

    def test_default_user_creation_fallbacks(self) -> None:
        """Test that default values are used when not specified in backend_settings."""
        minimal_settings = {
            "api_url": "https://mupdevb.macc.fccn.pt/",
            "username": "test",
            "password": "test",
        }
        backend = MUPBackend(minimal_settings, self.mup_components)

        # Verify default fallback values are used
        assert backend.user_defaults["salutation"] == "Dr."
        assert backend.user_defaults["gender"] == "Other"
        assert backend.user_defaults["year_of_birth"] == 1990
        assert backend.user_defaults["country"] == "Portugal"
        assert backend.user_defaults["type_of_institution"] == "Academic"
        assert backend.user_defaults["affiliated_institution"] == "Research Institution"
        assert (
            "Researcher using Waldur site agent" in backend.user_defaults["biography"]
        )
        assert backend.user_defaults["funding_agency_prefix"] == "WALDUR-SITE-AGENT-"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_creation_payload_construction(self, mock_client_class) -> None:
        """Test that user creation payloads are constructed with configurable defaults."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Mock user creation workflow
        mock_client.create_user_request.return_value = {"success": True, "id": 123}

        backend = MUPBackend(self.custom_user_settings, self.mup_components)

        result = backend._get_or_create_user(
            {
                "email": "test@example.com",
                "full_name": "Test User",
                "username": "testuser",
            }
        )

        # Verify create_user_request was called with custom defaults
        mock_client.create_user_request.assert_called_once()
        call_args = mock_client.create_user_request.call_args[0][
            0
        ]  # First argument is user_data

        assert call_args["salutation"] == "Prof."
        assert call_args["gender"] == "Female"
        assert call_args["year_of_birth"] == 1985
        assert call_args["country"] == "Germany"
        assert call_args["type_of_institution"] == "Industry"
        assert call_args["affiliated_institution"] == "Max Planck Institute"
        assert call_args["biography"] == "Custom researcher profile"
        assert call_args["funding_agency_grant"].startswith("CUSTOM-PREFIX-")
        assert result == 123

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_request_lookup_by_email(self, mock_client_class) -> None:
        """Test that user ID can be found by looking up user requests by email."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Simulate user creation response without user_id
        mock_client.create_user_request.return_value = {
            "success": True
        }  # No ID returned

        # Mock user requests response for lookup
        mock_client.get_user_requests.return_value = [
            {"id": 456, "email": "other@example.com", "status": "pending"},
            {"id": 123, "email": "test@example.com", "status": "pending"},
            {"id": 789, "email": "another@example.com", "status": "approved"},
        ]

        backend = MUPBackend(self.mup_settings, self.mup_components)

        result = backend._get_or_create_user(
            {
                "email": "test@example.com",
                "full_name": "Test User",
                "username": "testuser",
            }
        )

        # Verify that user request lookup was performed and correct ID found
        mock_client.create_user_request.assert_called_once()
        mock_client.get_user_requests.assert_called_once()
        assert result == 123

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_request_lookup_no_match(self, mock_client_class) -> None:
        """Test handling when user request lookup finds no matching email."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Simulate user creation response without user_id
        mock_client.create_user_request.return_value = {
            "success": True
        }  # No ID returned

        # Mock user requests response with no matching email
        mock_client.get_user_requests.return_value = [
            {"id": 456, "email": "other@example.com", "status": "pending"},
            {"id": 789, "email": "another@example.com", "status": "approved"},
        ]

        backend = MUPBackend(self.mup_settings, self.mup_components)

        result = backend._get_or_create_user(
            {
                "email": "test@example.com",
                "full_name": "Test User",
                "username": "testuser",
            }
        )

        # Verify that lookup was attempted but no matching email was found
        mock_client.create_user_request.assert_called_once()
        mock_client.get_user_requests.assert_called_once()
        assert result is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_creation_error_handling(self, mock_client_class) -> None:
        """Test error handling when user creation fails."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Simulate user creation failure
        mock_client.create_user_request.side_effect = Exception("API Error")

        backend = MUPBackend(self.mup_settings, self.mup_components)

        with self.assertLogs(level="ERROR") as log:
            result = backend._get_or_create_user(
                {
                    "email": "test@example.com",
                    "full_name": "Test User",
                    "username": "testuser",
                }
            )

        # Verify error is logged and None is returned
        assert result is None
        assert any("Unexpected error creating user" in msg for msg in log.output)
        mock_client.get_user_requests.assert_not_called()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_request_lookup_error_handling(self, mock_client_class) -> None:
        """Test error handling when user request lookup fails."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Simulate user creation without ID
        mock_client.create_user_request.return_value = {
            "success": True
        }  # No ID returned

        # Simulate error during user request lookup
        mock_client.get_user_requests.side_effect = Exception("Lookup failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)

        with self.assertLogs(level="WARNING") as log:
            result = backend._get_or_create_user(
                {
                    "email": "test@example.com",
                    "full_name": "Test User",
                    "username": "testuser",
                }
            )

        # Verify error is handled gracefully and None is returned
        assert result is None
        assert any(
            "Error searching for user request by email" in msg for msg in log.output
        )

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_creation_with_direct_user_id_response(
        self, mock_client_class
    ) -> None:
        """Test user creation when API directly returns user_id."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing users
        mock_client.get_users.return_value = []

        # Simulate user creation response with direct ID returned
        mock_client.create_user_request.return_value = {"success": True, "id": 456}

        backend = MUPBackend(self.mup_settings, self.mup_components)

        result = backend._get_or_create_user(
            {
                "email": "test@example.com",
                "full_name": "Test User",
                "username": "testuser",
            }
        )

        # Verify that user request lookup is skipped when ID is directly available
        mock_client.create_user_request.assert_called_once()
        mock_client.get_user_requests.assert_not_called()
        assert result == 456


if __name__ == "__main__":
    unittest.main()
