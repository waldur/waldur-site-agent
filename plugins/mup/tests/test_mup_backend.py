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

from waldur_site_agent.backend.exceptions import BackendError, BackendNotReadyError
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

        grant_number = f"GRANT-{self.project_uuid.hex[:8]}"
        self.sample_waldur_resource = WaldurResource(
            uuid=self.resource_uuid,
            slug=f"resource-{self.resource_uuid.hex[:8]}",
            name="test-resource",
            project_uuid=self.project_uuid,
            project_slug=f"project-{self.project_uuid.hex[:8]}",
            project_name=f"Test Project / {grant_number} / Description",
            offering_uuid=uuid.uuid4(),
            offering_name="MUP Offering",
            limits=ResourceLimits.from_dict({"cpu": 10}),
        )
        self.grant_number = grant_number

        self.sample_mup_project = {
            "id": 1,
            "title": f"Test Project / {grant_number} / Description",
            "description": "A test project",
            "pi": "pi@example.com",
            "grant_number": grant_number,
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

        # Sample user context for testing - PI has PROJECT.MANAGER role
        self.sample_user_context = {
            "team": [
                ProjectUser(
                    uuid="user-uuid-1",
                    email="pi@example.com",
                    username="pi_user",
                    full_name="Principal Investigator",
                    role="PROJECT.MANAGER",  # PI role
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
                    user_email="pi@example.com",  # Ensure email is set
                    user_full_name="Principal Investigator",
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
                    role="PROJECT.MANAGER",
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
                    user_email="pi@example.com",
                    user_full_name="Principal Investigator",
                    state=OfferingUserState.OK,
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
        mock_client.get_user_by_email.return_value = {"id": 1, "email": "pi@example.com", "username": "pi_user"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        waldur_user = {
            "email": "pi@example.com",
            "username": "pi_user",
            "first_name": "Principal",
            "last_name": "Investigator",
        }

        user_id = backend._get_or_create_user(waldur_user)

        assert user_id == 1
        mock_client.get_user_by_email.assert_called_once_with("pi@example.com")
        mock_client.create_user_request.assert_not_called()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_or_create_user_create_new(self, mock_client_class) -> None:
        """Test creating new user."""
        mock_client = mock_client_class.return_value
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 2}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        email = "new@example.com"
        waldur_user = {
            "email": email,
            "username": "new_user",
            "first_name": "New",
            "last_name": "User",
        }

        user_id = backend._get_or_create_user(waldur_user)

        assert user_id == 2
        mock_client.get_user_by_email.assert_called_once_with(email)
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
        """Test finding project by grant number."""
        mock_client = mock_client_class.return_value
        mock_client.get_project_by_grant.return_value = self.sample_mup_project

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_grant(self.grant_number)

        assert project == self.sample_mup_project
        mock_client.get_project_by_grant.assert_called_once_with(self.grant_number)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_get_project_by_waldur_id_not_found(self, mock_client_class) -> None:
        """Test project not found by grant number."""
        mock_client = mock_client_class.return_value
        mock_client.get_project_by_grant.return_value = None

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project = backend._get_project_by_grant("nonexistent_grant")

        assert project is None
        mock_client.get_project_by_grant.assert_called_once_with("nonexistent_grant")

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_mup_project_success(self, mock_client_class) -> None:
        """Test successful MUP project creation."""
        mock_client = mock_client_class.return_value
        mock_client.create_project.return_value = self.sample_mup_project

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project_name = f"Test Project / {self.grant_number} / Description"
        pi_email = "pi@example.com"

        result = backend._create_mup_project(
            project_name=project_name,
            grant_number=self.grant_number,
            project_uuid=str(self.project_uuid),
            pi_user_email=pi_email,
            description="A test project",
        )

        assert result == self.sample_mup_project
        mock_client.create_project.assert_called_once()

        # Check project data structure
        call_args = mock_client.create_project.call_args[0][0]
        assert call_args["title"] == project_name
        assert call_args["pi"] == "pi@example.com"
        assert call_args["grant_number"] == self.grant_number

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_mup_project_failure(self, mock_client_class) -> None:
        """Test MUP project creation failure."""
        mock_client = mock_client_class.return_value
        mock_client.create_project.side_effect = MUPError("Project creation failed")

        backend = MUPBackend(self.mup_settings, self.mup_components)
        project_name = f"Test Project / {self.grant_number} / Description"
        pi_email = "pi@example.com"

        result = backend._create_mup_project(
            project_name=project_name,
            grant_number=self.grant_number,
            project_uuid=str(self.project_uuid),
            pi_user_email=pi_email,
        )

        assert result is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_success(self, mock_client_class) -> None:
        """Test successful resource creation."""
        mock_client = mock_client_class.return_value
        # Mock for user lookup and creation
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}
        # Mock project lookup - project doesn't exist initially
        mock_client.get_project_by_grant.return_value = None
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = []  # No existing members
        mock_client.get_resource.return_value = None  # Resource doesn't exist initially
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        assert isinstance(result, BackendResourceInfo)
        # backend_id is set to "project_id_allocation_id" by post_create_resource
        assert result.backend_id == "1_1"
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
        # Mock for user lookup and creation
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}

        # Project exists but is inactive
        inactive_project = self.sample_mup_project.copy()
        inactive_project["active"] = False
        mock_client.get_project_by_grant.return_value = inactive_project
        mock_client.get_project_members.return_value = []  # No existing members
        mock_client.get_resource.return_value = None  # Resource doesn't exist initially
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
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_project_by_grant.return_value = None
        mock_client.get_project_members.return_value = []
        mock_client.get_resource.return_value = None  # Resource doesn't exist initially
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        # Verify PI user was created first
        mock_client.create_user_request.assert_called()

        # Verify project was created with real PI email from user context
        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        assert project_data["pi"] == "pi@example.com"  # Real email from user context

        # Verify user was added to project during creation
        mock_client.add_project_member.assert_called()

        assert isinstance(result, BackendResourceInfo)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_create_resource_without_user_context(self, mock_client_class) -> None:
        """Test resource creation fails without user context (PI required)."""
        mock_client = mock_client_class.return_value
        mock_client.get_project_by_grant.return_value = None

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # Resource creation should fail without user context (PI is required)
        with pytest.raises(BackendError) as context:
            backend.create_resource(self.sample_waldur_resource)

        assert "user context" in str(context.value).lower() or "pi" in str(context.value).lower()

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
        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_allocations.return_value = [self.sample_mup_allocation]

        backend = MUPBackend(self.mup_settings, self.mup_components)
        metadata = backend.get_resource_metadata("1_1")

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
        mock_client.get_allocation_usage.return_value = {
            "total": 5,
            "users": {"user1": 5},
            "unit": "node.hour",
        }

        backend = MUPBackend(self.mup_settings, self.mup_components)

        accounts = ["1_1"]
        report = backend._get_usage_report(accounts)

        account_key = "1_1"
        assert account_key in report
        assert "TOTAL_ACCOUNT_USAGE" in report[account_key]
        assert report[account_key]["TOTAL_ACCOUNT_USAGE"]["cpu"] == 5
        assert "user1" in report[account_key]
        assert report[account_key]["user1"]["cpu"] == 5
        mock_client.get_allocation_usage.assert_called_once_with(1, 1)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_to_resource(self, mock_client_class) -> None:
        """Test adding a new user to a resource via the by-username lookup path."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = []
        # New primary lookup: get_user_by_username
        mock_client.get_user_by_username.return_value = {
            "id": 2,
            "username": "newuser",
            "email": "newuser@example.com",
        }
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        self.sample_waldur_resource.backend_id = "1_1"

        added_users = backend.add_users_to_resource(
            self.sample_waldur_resource, {"newuser"}
        )

        assert added_users == {"newuser"}
        mock_client.get_user_by_username.assert_called_once_with("newuser")
        mock_client.get_users.assert_not_called()
        mock_client.add_project_member.assert_called_once()
        call_member_data = mock_client.add_project_member.call_args[0][1]
        assert call_member_data["email"] == "newuser@example.com"
        assert call_member_data["user_id"] == 2

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_already_a_member_treated_as_success(self, mock_client_class) -> None:
        """MUP returns 400 'already a member' for the PI; should be treated as success."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        # PI is not in the members list (MUP doesn't include PI there)
        mock_client.get_project_members.return_value = []
        mock_client.get_user_by_username.return_value = {
            "id": 53,
            "username": "pi_user",
            "email": "pi@example.com",
        }
        mock_client.add_project_member.side_effect = MUPError(
            "400 Bad Request. Response Body: {\"detail\":\"User is already a member of this project.\"}"
        )

        backend = MUPBackend(self.mup_settings, self.mup_components)
        self.sample_waldur_resource.backend_id = "1_1"

        added_users = backend.add_users_to_resource(
            self.sample_waldur_resource, {"pi_user"}
        )

        assert added_users == {"pi_user"}

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_fallback_to_email_lookup(self, mock_client_class) -> None:
        """If by-username lookup fails, fall back to by-email using user_emails kwarg."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = []
        mock_client.get_user_by_username.return_value = None  # not found by username
        mock_client.get_user_by_email.return_value = {
            "id": 5,
            "username": "some.mup.user",
            "email": "actual@example.com",
        }
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        self.sample_waldur_resource.backend_id = "1_1"

        added_users = backend.add_users_to_resource(
            self.sample_waldur_resource,
            {"waldur.username"},
            user_emails={"waldur.username": "actual@example.com"},
        )

        assert added_users == {"waldur.username"}
        mock_client.get_user_by_email.assert_called_once_with("actual@example.com")
        call_member_data = mock_client.add_project_member.call_args[0][1]
        assert call_member_data["email"] == "actual@example.com"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_creates_via_offering_user(self, mock_client_class) -> None:
        """User not in MUP yet but has OK OfferingUser — should be created then added."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = []
        mock_client.get_user_by_username.return_value = None
        mock_client.get_user_by_email.return_value = None
        # After creation, second lookup returns the new user
        created_user = {"id": 10, "username": "new.mup.user", "email": "newbie@example.com"}
        mock_client.get_user_by_username.side_effect = [None, created_user]
        mock_client.create_user_request.return_value = {"id": 10}
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        self.sample_waldur_resource.backend_id = "1_1"

        added_users = backend.add_users_to_resource(
            self.sample_waldur_resource,
            {"new.mup.user"},
            user_emails={"new.mup.user": "newbie@example.com"},
            offering_user_states={"new.mup.user": OfferingUserState.OK},
            user_attributes={
                "new.mup.user": {"full_name": "New User", "email": "newbie@example.com"},
            },
        )

        assert added_users == {"new.mup.user"}
        mock_client.create_user_request.assert_called_once()
        mock_client.add_project_member.assert_called_once()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_add_users_not_in_mup_no_offering_user_warns(self, mock_client_class) -> None:
        """User not in MUP and no OK OfferingUser — log warning and skip."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = []
        mock_client.get_user_by_username.return_value = None
        mock_client.get_user_by_email.return_value = None

        backend = MUPBackend(self.mup_settings, self.mup_components)
        self.sample_waldur_resource.backend_id = "1_1"

        added_users = backend.add_users_to_resource(
            self.sample_waldur_resource,
            {"ghost.user"},
        )

        assert added_users == set()
        mock_client.add_project_member.assert_not_called()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_pull_backend_resource_returns_members(self, mock_client_class) -> None:
        """_pull_backend_resource should return current MUP project members as users."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = [
            {"id": 1, "username": "deucalion.user", "email": "du@example.com", "active": True},
        ]
        mock_client.get_allocation_usage.return_value = {
            "total": 0,
            "users": {},
            "unit": "node.hour",
        }

        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend._pull_backend_resource("1_1")

        assert result is not None
        assert "deucalion.user" in result.users
        assert result.backend_id == "1_1"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_pull_backend_resource_legacy_id_returns_none(self, mock_client_class) -> None:
        """Legacy alloc_* backend IDs cannot be parsed — should return None gracefully."""
        backend = MUPBackend(self.mup_settings, self.mup_components)
        result = backend._pull_backend_resource("alloc_ju-revie-1-18")
        assert result is None

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_remove_users_from_account(self, mock_client_class) -> None:
        """Test removing users from account."""
        mock_client = mock_client_class.return_value

        mock_client.get_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "username": "user1",
                "email": "user1@example.com",
            }
        ]
        mock_client.toggle_member_status.return_value = {"status": "deactivated"}

        backend = MUPBackend(self.mup_settings, self.mup_components)
        usernames = {"user1"}

        # remove_users_from_resource now takes WaldurResource, not a plain string
        self.sample_waldur_resource.backend_id = "1_1"

        removed_users = backend.remove_users_from_resource(
            self.sample_waldur_resource, usernames
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

        # Mock user search returns no existing user
        mock_client.get_user_by_email.return_value = None

        # Mock user creation workflow - new API returns ID directly
        mock_client.create_user_request.return_value = {"id": 123}

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
    def test_user_creation_error_handling(self, mock_client_class) -> None:
        """Test error handling when user creation fails."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing user
        mock_client.get_user_by_email.return_value = None

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

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_creation_with_direct_user_id_response(
        self, mock_client_class
    ) -> None:
        """Test user creation when API directly returns user_id."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock user search returns no existing user
        mock_client.get_user_by_email.return_value = None

        mock_client.create_user_request.return_value = {"id": 456}

        backend = MUPBackend(self.mup_settings, self.mup_components)

        result = backend._get_or_create_user(
            {
                "email": "test@example.com",
                "full_name": "Test User",
                "username": "testuser",
            }
        )

        # Verify that ID is returned directly from API response
        mock_client.create_user_request.assert_called_once()
        assert result == 456

    def test_parse_backend_id_combined_format(self) -> None:
        """Test parsing the new combined 'project_id_allocation_id' backend_id."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        project_id, alloc_id = backend._parse_backend_id("42_7")
        assert project_id == 42
        assert alloc_id == 7

    def test_parse_backend_id_legacy_format(self) -> None:
        """Test parsing legacy 'project_id' backend_id falls back gracefully."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        project_id, alloc_id = backend._parse_backend_id("42")
        assert project_id == 42
        assert alloc_id is None

    def test_parse_backend_id_invalid_raises(self) -> None:
        """Test that an unparseable backend_id raises ValueError."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        with pytest.raises(ValueError):
            backend._parse_backend_id("not_a_number")

    def test_extract_grant_from_project_name_valid(self) -> None:
        """Test grant extraction from valid project name format."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        # Valid format: "Part1 / GrantNumber / Part3"
        result = backend._extract_grant_from_project_name("EHPC-AIF-FL / EHPC-ANF-2020FL01-114 / Description")
        assert result == "EHPC-ANF-2020FL01-114"

        # Valid with spaces
        result = backend._extract_grant_from_project_name("Part1 / GRANT-123 / Part3")
        assert result == "GRANT-123"

        # Valid with only two parts (grant is second)
        result = backend._extract_grant_from_project_name("Part1 / GRANT-456")
        assert result == "GRANT-456"

    def test_extract_grant_from_project_name_invalid_no_slashes(self) -> None:
        """Test grant extraction fails when project name has no slashes."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        with pytest.raises(BackendError) as context:
            backend._extract_grant_from_project_name("No slashes here")

        assert "does not contain grant number" in str(context.value)

    def test_extract_grant_from_project_name_invalid_empty_grant(self) -> None:
        """Test grant extraction fails when grant number is empty."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        with pytest.raises(BackendError) as context:
            backend._extract_grant_from_project_name("Part1 /  / Part3")

        assert "Grant number is empty" in str(context.value)

    def test_extract_grant_from_project_name_invalid_missing_project_name(self) -> None:
        """Test grant extraction fails when project name is None or empty."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        with pytest.raises(BackendError) as context:
            backend._extract_grant_from_project_name("")

        assert "Project name is required" in str(context.value)

    def test_extract_grant_from_project_name_special_characters(self) -> None:
        """Test grant extraction handles special characters in grant number."""
        backend = MUPBackend(self.mup_settings, self.mup_components)

        # Grant with hyphens and numbers
        result = backend._extract_grant_from_project_name("Part1 / GRANT-2024-ABC-123 / Part3")
        assert result == "GRANT-2024-ABC-123"

        # Grant with underscores
        result = backend._extract_grant_from_project_name("Part1 / GRANT_2024_123 / Part3")
        assert result == "GRANT_2024_123"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_pi_auto_added_to_project(self, mock_client_class) -> None:
        """Test that PI is automatically added to project when project is created."""
        mock_client = mock_client_class.return_value

        # PI doesn't exist yet
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}

        # Project doesn't exist
        mock_client.get_project_by_grant.return_value = None
        mock_client.create_project.return_value = self.sample_mup_project

        # After project creation, PI is auto-added by MUP (flat member format)
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "email": "pi@example.com",
                "username": "pi_user",
            }
        ]

        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = self.sample_mup_allocation

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # Create resource - this should create PI, create project, then add other users
        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        mock_client.create_user_request.assert_called()

        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        assert project_data["pi"] == "pi@example.com"

        mock_client.get_project_members.assert_called()

        members_call = mock_client.get_project_members.call_args[0][0]
        assert members_call == self.sample_mup_project["id"]

        if mock_client.add_project_member.called:
            add_member_calls = mock_client.add_project_member.call_args_list
            for call in add_member_calls:
                member_data = call[0][1]
                assert member_data["email"] != "pi@example.com", "PI should not be added again"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_multiple_resources_same_project(self, mock_client_class) -> None:
        """Test creating multiple resources in the same project (same grant number)."""
        mock_client = mock_client_class.return_value

        # First resource: create project and users
        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}
        mock_client.get_project_by_grant.return_value = None
        mock_client.create_project.return_value = self.sample_mup_project
        # Flat member format (MUP API)
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "email": "pi@example.com",
                "username": "pi_user",
            }
        ]
        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = self.sample_mup_allocation
        mock_client.add_project_member.return_value = {"status": "success"}

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # Create first resource
        backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        mock_client.reset_mock()

        mock_client.get_user_by_email.side_effect = [
            {"id": 1, "email": "pi@example.com"},
            None,
        ]
        mock_client.create_user_request.return_value = {"id": 2}
        mock_client.get_project_by_grant.return_value = self.sample_mup_project
        # Existing members: PI and user from first resource (flat format)
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "email": "pi@example.com",
                "username": "pi_user",
            },
            {
                "id": 2,
                "active": True,
                "email": "user1@example.com",
                "username": "user1",
            },
        ]
        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = {
            "id": 2,
            "type": "compute",
            "identifier": f"alloc_{uuid.uuid4().hex}",
            "size": 20,
            "used": 0,
            "active": True,
            "project": 1,
        }
        mock_client.add_project_member.return_value = {"status": "success"}

        # Create second resource in same project
        second_resource = WaldurResource(
            uuid=uuid.uuid4(),
            slug=f"resource-{uuid.uuid4().hex[:8]}",
            name="test-resource-2",
            project_uuid=self.project_uuid,
            project_slug=f"project-{self.project_uuid.hex[:8]}",
            project_name=f"Test Project / {self.grant_number} / Description",  # Same grant
            offering_uuid=uuid.uuid4(),
            offering_name="MUP Offering",
            limits=ResourceLimits.from_dict({"cpu": 20}),
        )

        # User context with new user (not PI, not existing user)
        second_user_context = {
            "team": [
                ProjectUser(
                    uuid="user-uuid-1",
                    email="pi@example.com",
                    username="pi_user",
                    full_name="Principal Investigator",
                    role="PROJECT.MANAGER",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="pi_user",
                    offering_user_state=OfferingUserState.OK,
                ),
                ProjectUser(
                    uuid="user-uuid-2",
                    email="newuser@example.com",
                    username="newuser",
                    full_name="New User",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-2/",
                    expiration_time=None,
                    offering_user_username="newuser",
                    offering_user_state=OfferingUserState.OK,
                ),
            ],
            "user_mappings": {
                "user-uuid-1": ProjectUser(
                    uuid="user-uuid-1",
                    email="pi@example.com",
                    username="pi_user",
                    full_name="Principal Investigator",
                    role="PROJECT.MANAGER",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="pi_user",
                    offering_user_state=OfferingUserState.OK,
                ),
                "user-uuid-2": ProjectUser(
                    uuid="user-uuid-2",
                    email="newuser@example.com",
                    username="newuser",
                    full_name="New User",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-2/",
                    expiration_time=None,
                    offering_user_username="newuser",
                    offering_user_state=OfferingUserState.OK,
                ),
            },
            "offering_user_mappings": {
                "user-uuid-1": OfferingUser(
                    username="pi_user",
                    user_uuid="user-uuid-1",
                    offering_uuid="offering-uuid-1",
                    user_email="pi@example.com",
                    user_full_name="Principal Investigator",
                    state=OfferingUserState.OK,
                    created="2025-01-01T00:00:00Z",
                    modified="2025-01-01T00:00:00Z",
                ),
                "user-uuid-2": OfferingUser(
                    username="newuser",
                    user_uuid="user-uuid-2",
                    offering_uuid="offering-uuid-2",
                    user_email="newuser@example.com",
                    user_full_name="New User",
                    state=OfferingUserState.OK,
                    created="2025-01-01T00:00:00Z",
                    modified="2025-01-01T00:00:00Z",
                ),
            },
        }

        backend.create_resource(second_resource, second_user_context)

        mock_client.create_project.assert_not_called()

        get_user_calls = mock_client.get_user_by_email.call_args_list
        assert len(get_user_calls) >= 1  # At least called for new user


        if mock_client.create_user_request.called:
            create_user_calls = mock_client.create_user_request.call_args_list
            # Should only create newuser, not PI (PI already exists)
            assert len(create_user_calls) == 1

        # Verify new user was added to project (PI is already a member, so skipped)
        mock_client.add_project_member.assert_called_once()
        member_data = mock_client.add_project_member.call_args[0][1]
        assert member_data["email"] == "newuser@example.com"

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_pi_already_exists_in_mup(self, mock_client_class) -> None:
        """Test creating project when PI user already exists in MUP (from another project)."""
        mock_client = mock_client_class.return_value

        existing_pi_email = "pi@example.com"
        mock_client.get_user_by_email.return_value = {
            "id": 99,
            "email": existing_pi_email,
            "username": "pi_user",
        }

        # Project doesn't exist
        mock_client.get_project_by_grant.return_value = None
        mock_client.create_project.return_value = self.sample_mup_project
        mock_client.get_project_members.return_value = [
            {
                "id": 1,
                "active": True,
                "email": existing_pi_email,
                "username": "pi_user",
            }
        ]
        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = self.sample_mup_allocation

        backend = MUPBackend(self.mup_settings, self.mup_components)

        backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        # Verify existing PI user was found (not created)
        mock_client.get_user_by_email.assert_called_with(existing_pi_email)
        mock_client.create_user_request.assert_not_called()

        # Verify project was created with existing PI email
        mock_client.create_project.assert_called_once()
        project_data = mock_client.create_project.call_args[0][0]
        assert project_data["pi"] == existing_pi_email

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_project_already_active(self, mock_client_class) -> None:
        """Test that active project is not activated again."""
        mock_client = mock_client_class.return_value

        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}

        # Project exists and is already active
        active_project = self.sample_mup_project.copy()
        active_project["active"] = True
        mock_client.get_project_by_grant.return_value = active_project
        mock_client.get_project_members.return_value = []
        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = self.sample_mup_allocation

        backend = MUPBackend(self.mup_settings, self.mup_components)

        result = backend.create_resource(
            self.sample_waldur_resource, self.sample_user_context
        )

        # Verify activate_project was NOT called (project already active)
        mock_client.activate_project.assert_not_called()

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_project_activation_failure(self, mock_client_class) -> None:
        """Test that project activation failure is handled gracefully."""
        mock_client = mock_client_class.return_value

        mock_client.get_user_by_email.return_value = None
        mock_client.create_user_request.return_value = {"id": 1}

        # Project exists but is inactive
        inactive_project = self.sample_mup_project.copy()
        inactive_project["active"] = False
        mock_client.get_project_by_grant.return_value = inactive_project
        mock_client.get_project_members.return_value = []

        mock_client.activate_project.side_effect = MUPError("Activation failed")

        mock_client.get_resource.return_value = None
        mock_client.create_allocation.return_value = self.sample_mup_allocation

        backend = MUPBackend(self.mup_settings, self.mup_components)

        with self.assertLogs(level="WARNING") as log:
            result = backend.create_resource(
                self.sample_waldur_resource, self.sample_user_context
            )

        # Verify activation was attempted
        mock_client.activate_project.assert_called_once()

        assert any("Failed to activate project" in msg for msg in log.output)

        assert isinstance(result, BackendResourceInfo)

    @patch("waldur_site_agent_mup.backend.MUPClient")
    def test_user_context_without_pi_clear_error(self, mock_client_class) -> None:
        """Test that missing PI in user context defers processing with clear message."""
        mock_client = mock_client_class.return_value
        mock_client.get_project_by_grant.return_value = None

        backend = MUPBackend(self.mup_settings, self.mup_components)

        # User context without PROJECT.MANAGER role
        user_context_no_pi = {
            "team": [
                ProjectUser(
                    uuid="user-uuid-1",
                    email="user1@example.com",
                    username="user1",
                    full_name="Regular User",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="user1",
                    offering_user_state=OfferingUserState.OK,
                )
            ],
            "user_mappings": {
                "user-uuid-1": ProjectUser(
                    uuid="user-uuid-1",
                    email="user1@example.com",
                    username="user1",
                    full_name="Regular User",
                    role="admin",
                    url="https://waldur.example.com/api/users/user-uuid-1/",
                    expiration_time=None,
                    offering_user_username="user1",
                    offering_user_state=OfferingUserState.OK,
                ),
            },
            "offering_user_mappings": {
                "user-uuid-1": OfferingUser(
                    username="user1",
                    user_uuid="user-uuid-1",
                    offering_uuid="offering-uuid-1",
                    user_email="user1@example.com",
                    user_full_name="Regular User",
                    state=OfferingUserState.OK,
                    created="2025-01-01T00:00:00Z",
                    modified="2025-01-01T00:00:00Z",
                ),
            },
        }

        with pytest.raises(BackendNotReadyError) as context:
            backend.create_resource(
                self.sample_waldur_resource, user_context_no_pi
            )

        error_msg = str(context.value)
        assert "PROJECT.MANAGER" in error_msg or "PI" in error_msg
        assert "found" in error_msg.lower() or "missing" in error_msg.lower()


if __name__ == "__main__":
    unittest.main()
