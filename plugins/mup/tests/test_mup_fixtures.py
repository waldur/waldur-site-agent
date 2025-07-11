"""Test fixtures for MUP backend tests"""

import uuid
from waldur_site_agent.common.structures import Offering


# MUP-specific offering configuration
MUP_OFFERING = Offering(
    uuid="d629d5e45567425da9cdbdc1af67b32c",
    name="mup-test-offering",
    api_url="http://localhost:8081/api/",
    api_token="9e1132b9616ebfe943ddf632ca32bbb7e1109a32",
    backend_type="mup",
    backend_settings={
        "api_url": "https://mup-api.example.com/api",
        "username": "test_user",
        "password": "test_password",
        "default_research_field": 1,
        "default_agency": "FCT",
        "project_prefix": "waldur_",
        "allocation_prefix": "alloc_",
        "default_allocation_type": "compute",
        "default_storage_limit": 1000,
    },
    backend_components={
        "cpu": {
            "measured_unit": "core-hours",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "CPU Cores",
        },
        "storage": {
            "measured_unit": "GB",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "Storage Space",
        },
    },
)


def create_sample_waldur_resource(resource_uuid=None, project_uuid=None):
    """Create a sample Waldur resource for testing"""
    if resource_uuid is None:
        resource_uuid = str(uuid.uuid4())
    if project_uuid is None:
        project_uuid = str(uuid.uuid4())

    return {
        "uuid": resource_uuid,
        "name": "test-mup-resource",
        "project": {
            "uuid": project_uuid,
            "name": "Test MUP Project",
            "description": "A test project for MUP integration",
            "customer_users": [
                {
                    "uuid": str(uuid.uuid4()),
                    "username": "pi_user",
                    "email": "pi@example.com",
                    "first_name": "Principal",
                    "last_name": "Investigator",
                },
                {
                    "uuid": str(uuid.uuid4()),
                    "username": "researcher1",
                    "email": "researcher1@example.com",
                    "first_name": "Researcher",
                    "last_name": "One",
                },
            ],
        },
        "offering": {"uuid": str(uuid.uuid4()), "name": "MUP Computing Offering"},
        "limits": {"cpu": 16, "storage": 500},
    }


def create_sample_mup_project(project_id=1, waldur_project_uuid=None):
    """Create a sample MUP project for testing"""
    if waldur_project_uuid is None:
        waldur_project_uuid = str(uuid.uuid4())

    return {
        "id": project_id,
        "title": "Test MUP Project",
        "description": "A test project for MUP integration",
        "pi": "pi@example.com",
        "co_pi": None,
        "science_field": 1,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "agency": "FCT",
        "grant_number": f"waldur_{waldur_project_uuid}",
        "max_storage": 1000,
        "ai_included": False,
        "active": True,
    }


def create_sample_mup_allocation(allocation_id=1, project_id=1, resource_uuid=None):
    """Create a sample MUP allocation for testing"""
    if resource_uuid is None:
        resource_uuid = str(uuid.uuid4())

    return {
        "id": allocation_id,
        "type": "compute",
        "identifier": f"alloc_{resource_uuid}",
        "size": 16,
        "used": 0,
        "active": True,
        "project": project_id,
    }


def create_sample_mup_user(user_id=1, email="test@example.com", username="testuser"):
    """Create a sample MUP user for testing"""
    return {
        "id": user_id,
        "username": username,
        "email": email,
        "first_name": "Test",
        "last_name": "User",
        "research_fields": [1],
        "agency": "FCT",
        "has_read_and_accepted_terms_of_service": True,
        "has_read_and_accepted_data_sharing_policy": True,
        "has_subscribe_newsletter": False,
    }


def create_sample_mup_member(member_id=1, user_id=1, active=True):
    """Create a sample MUP project member for testing"""
    return {"id": member_id, "active": active, "member": create_sample_mup_user(user_id)}


def create_sample_research_fields():
    """Create sample research fields for testing"""
    return [
        {"id": 1, "name": "Computer Science"},
        {"id": 2, "name": "Physics"},
        {"id": 3, "name": "Mathematics"},
        {"id": 4, "name": "Biology"},
        {"id": 5, "name": "Chemistry"},
    ]


def create_waldur_order(order_type="Create", resource_uuid=None, project_uuid=None):
    """Create a sample Waldur order for testing"""
    if resource_uuid is None:
        resource_uuid = str(uuid.uuid4())
    if project_uuid is None:
        project_uuid = str(uuid.uuid4())

    base_order = {
        "uuid": str(uuid.uuid4()),
        "type": order_type,
        "state": "pending-provider" if order_type == "Create" else "executing",
        "attributes": {"name": "test_mup_resource"},
        "offering_type": "Marketplace.MUP",
        "project_slug": "test-project",
        "customer_slug": "test-customer",
        "resource_uuid": resource_uuid,
        "project_uuid": project_uuid,
        "customer_uuid": str(uuid.uuid4()),
        "offering_uuid": str(uuid.uuid4()),
        "project_name": "Test Project",
        "customer_name": "Test Customer",
    }

    if order_type == "Create":
        base_order.update({"limits": {"cpu": 16, "storage": 500}})
    elif order_type == "Update":
        base_order.update(
            {
                "limits": {"cpu": 32, "storage": 1000},
                "attributes": {
                    "old_limits": {"cpu": 16, "storage": 500},
                    "name": "test_mup_resource",
                },
            }
        )
    elif order_type == "Terminate":
        base_order.update({"resource_name": "test_mup_resource"})

    return base_order


# Error scenarios for testing
class MUPTestScenarios:
    """Common test scenarios for MUP backend testing"""

    @staticmethod
    def user_creation_failure():
        """Scenario where user creation fails"""
        return {
            "error_type": "user_creation",
            "error_message": "Failed to create user: Email already exists",
        }

    @staticmethod
    def project_creation_failure():
        """Scenario where project creation fails"""
        return {
            "error_type": "project_creation",
            "error_message": "Failed to create project: Invalid PI email",
        }

    @staticmethod
    def allocation_creation_failure():
        """Scenario where allocation creation fails"""
        return {
            "error_type": "allocation_creation",
            "error_message": "Failed to create allocation: Insufficient quota",
        }

    @staticmethod
    def api_connection_failure():
        """Scenario where API connection fails"""
        return {
            "error_type": "api_connection",
            "error_message": "Connection to MUP API failed: Network unreachable",
        }

    @staticmethod
    def authentication_failure():
        """Scenario where authentication fails"""
        return {
            "error_type": "authentication",
            "error_message": "Authentication failed: Invalid credentials",
        }


# Mock responses for different MUP API endpoints
class MUPMockResponses:
    """Mock HTTP responses for MUP API testing"""

    @staticmethod
    def research_fields_response():
        return create_sample_research_fields()

    @staticmethod
    def empty_users_response():
        return []

    @staticmethod
    def users_response():
        return [
            create_sample_mup_user(1, "pi@example.com", "pi_user"),
            create_sample_mup_user(2, "researcher1@example.com", "researcher1"),
        ]

    @staticmethod
    def empty_projects_response():
        return []

    @staticmethod
    def projects_response(waldur_project_uuid=None):
        return [create_sample_mup_project(1, waldur_project_uuid)]

    @staticmethod
    def project_creation_response(waldur_project_uuid=None):
        return create_sample_mup_project(1, waldur_project_uuid)

    @staticmethod
    def user_creation_response():
        return {"id": 1, "status": "created"}

    @staticmethod
    def allocation_creation_response(resource_uuid=None):
        return create_sample_mup_allocation(1, 1, resource_uuid)

    @staticmethod
    def project_members_response():
        return [create_sample_mup_member(1, 1, True), create_sample_mup_member(2, 2, False)]

    @staticmethod
    def project_allocations_response(resource_uuid=None):
        return [create_sample_mup_allocation(1, 1, resource_uuid)]
