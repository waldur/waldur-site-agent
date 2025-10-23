"""Integration tests for hierarchical storage API endpoints."""

import json
import os
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

# Skip all tests in this module if configuration is not available
pytestmark = pytest.mark.skipif(
    not os.getenv("WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH"),
    reason="WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH not set - skipping API integration tests",
)

try:
    from waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main import app
except SystemExit:
    # If import fails due to configuration issues, skip all tests
    pytest.skip("Configuration not available for API tests", allow_module_level=True)


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def mock_waldur_resources():
    """Create mock Waldur resources for testing."""

    def create_mock_resource(
        resource_uuid=None,
        customer_slug="test-customer",
        customer_name="Test Customer",
        project_slug="test-project",
        project_name="Test Project",
        offering_slug="capstor",
        provider_slug="cscs",
        provider_name="CSCS",
        storage_data_type="store",
        storage_limit=150.0,
    ):
        if resource_uuid is None:
            resource_uuid = str(uuid4())

        resource = Mock()
        resource.uuid = Mock()
        resource.uuid.hex = resource_uuid
        resource.slug = project_slug
        resource.name = project_name
        resource.state = "OK"
        resource.customer_slug = customer_slug
        resource.customer_name = customer_name
        resource.customer_uuid = Mock()
        resource.customer_uuid.hex = str(uuid4())
        resource.project_slug = project_slug
        resource.project_name = project_name
        resource.project_uuid = Mock()
        resource.project_uuid.hex = str(uuid4())
        resource.offering_slug = offering_slug
        resource.provider_slug = provider_slug
        resource.provider_name = provider_name
        resource.offering_uuid = Mock()
        resource.offering_uuid.hex = str(uuid4())

        # Mock limits
        resource.limits = Mock()
        resource.limits.additional_properties = {"storage": storage_limit}

        # Mock attributes
        resource.attributes = Mock()
        resource.attributes.additional_properties = {
            "storage_data_type": storage_data_type,
            "permissions": "2770",
        }

        return resource

    return [
        create_mock_resource(
            customer_slug="mch",
            customer_name="MCH",
            project_slug="msclim",
            project_name="MSCLIM",
            offering_slug="capstor",
            storage_data_type="store",
        ),
        create_mock_resource(
            customer_slug="eth",
            customer_name="ETH",
            project_slug="climate-data",
            project_name="Climate Data",
            offering_slug="vast",
            storage_data_type="scratch",
        ),
        create_mock_resource(
            customer_slug="mch",
            customer_name="MCH",
            project_slug="user-homes",
            project_name="User Homes",
            offering_slug="capstor",
            storage_data_type="users",
        ),
    ]


@pytest.fixture
def mock_offering_customers():
    """Mock customer data from offering."""
    return {
        "mch": {
            "itemId": "mch-customer-id",
            "key": "mch",
            "name": "MCH",
            "uuid": "mch-customer-uuid",
        },
        "eth": {
            "itemId": "eth-customer-id",
            "key": "eth",
            "name": "ETH",
            "uuid": "eth-customer-uuid",
        },
    }


class TestHierarchicalStorageAPI:
    """Test the hierarchical storage resource API."""

    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_offering_customers"
    )
    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_resources_by_offering_slugs"
    )
    def test_three_tier_hierarchy_response(
        self,
        mock_get_resources,
        mock_get_customers,
        client,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that the API returns a proper three-tier hierarchy."""
        mock_get_customers.return_value = mock_offering_customers
        mock_get_resources.return_value = (
            mock_waldur_resources,
            {
                "current": 1,
                "limit": 100,
                "offset": 0,
                "pages": 1,
                "total": len(mock_waldur_resources),
            },
        )

        response = client.get("/api/storage-resources/")

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert "status" in data
        assert "resources" in data
        assert data["status"] == "success"

        resources = data["resources"]

        # Group by type
        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Verify we have all three tiers
        assert len(tenants) > 0
        assert len(customers) > 0
        assert len(projects) > 0

        # Verify hierarchy relationships
        # All tenants should have no parent
        for tenant in tenants:
            assert tenant["parentItemId"] is None

        # All customers should have a parent tenant
        tenant_ids = {t["itemId"] for t in tenants}
        for customer in customers:
            assert customer["parentItemId"] is not None
            assert customer["parentItemId"] in tenant_ids

        # All projects should have a parent customer
        customer_ids = {c["itemId"] for c in customers}
        for project in projects:
            assert project["parentItemId"] is not None
            assert project["parentItemId"] in customer_ids

    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_offering_customers"
    )
    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_resources_by_offering_slugs"
    )
    def test_mount_point_hierarchy(
        self,
        mock_get_resources,
        mock_get_customers,
        client,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that mount points follow the correct hierarchy."""
        mock_get_customers.return_value = mock_offering_customers
        mock_get_resources.return_value = (
            mock_waldur_resources,
            {
                "current": 1,
                "limit": 100,
                "offset": 0,
                "pages": 1,
                "total": len(mock_waldur_resources),
            },
        )

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Find a complete hierarchy chain
        capstor_resources = [r for r in resources if r["storageSystem"]["key"] == "capstor"]
        capstor_tenants = [r for r in capstor_resources if r["target"]["targetType"] == "tenant"]
        capstor_customers = [
            r for r in capstor_resources if r["target"]["targetType"] == "customer"
        ]
        capstor_projects = [r for r in capstor_resources if r["target"]["targetType"] == "project"]

        if capstor_tenants and capstor_customers and capstor_projects:
            tenant = capstor_tenants[0]
            customer = next(c for c in capstor_customers if c["parentItemId"] == tenant["itemId"])
            project = next(p for p in capstor_projects if p["parentItemId"] == customer["itemId"])

            # Verify mount point hierarchy
            tenant_mount = tenant["mountPoint"]["default"]
            customer_mount = customer["mountPoint"]["default"]
            project_mount = project["mountPoint"]["default"]

            # Customer mount should start with tenant mount
            assert customer_mount.startswith(tenant_mount + "/")

            # Project mount should start with customer mount
            assert project_mount.startswith(customer_mount + "/")

    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_offering_customers"
    )
    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_resources_by_offering_slug"
    )
    def test_storage_system_filter_maintains_hierarchy(
        self,
        mock_get_resources,
        mock_get_customers,
        client,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that filtering by storage system maintains the hierarchy."""
        mock_get_customers.return_value = mock_offering_customers
        # Filter to only capstor resources
        capstor_resources = [r for r in mock_waldur_resources if r.offering_slug == "capstor"]
        mock_get_resources.return_value = (
            capstor_resources,
            {"current": 1, "limit": 100, "offset": 0, "pages": 1, "total": len(capstor_resources)},
        )

        response = client.get("/api/storage-resources/?storage_system=capstor")

        assert response.status_code == 200
        data = response.json()
        resources = data["resources"]

        # Verify all resources are from capstor
        for resource in resources:
            assert resource["storageSystem"]["key"] == "capstor"

        # Verify hierarchy is maintained
        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Should have at least one of each type for capstor
        assert len(tenants) >= 1
        assert len(customers) >= 1
        assert len(projects) >= 1

        # Verify parent-child relationships
        tenant_ids = {t["itemId"] for t in tenants}
        customer_ids = {c["itemId"] for c in customers}

        for customer in customers:
            assert customer["parentItemId"] in tenant_ids

        for project in projects:
            assert project["parentItemId"] in customer_ids

    def test_pagination_with_hierarchy(self, client):
        """Test that pagination works correctly with hierarchical resources."""
        response = client.get("/api/storage-resources/?page=1&page_size=5")

        assert response.status_code == 200
        data = response.json()

        # Verify pagination info is present
        assert "pagination" in data
        pagination = data["pagination"]
        assert "current" in pagination
        assert "limit" in pagination
        assert "total" in pagination

        # Verify resources are limited by page_size
        resources = data["resources"]
        assert len(resources) <= 5

    def test_debug_mode_returns_hierarchy_info(self, client):
        """Test that debug mode returns hierarchy information."""
        response = client.get("/api/storage-resources/?debug=true")

        assert response.status_code == 200
        data = response.json()

        # Verify debug structure
        assert "debug_mode" in data
        assert data["debug_mode"] is True
        assert "agent_config" in data

        # Verify agent config includes storage systems
        config = data["agent_config"]
        assert "configured_storage_systems" in config
        assert isinstance(config["configured_storage_systems"], dict)

    def test_invalid_storage_system_filter(self, client):
        """Test filtering with an invalid storage system."""
        response = client.get("/api/storage-resources/?storage_system=nonexistent")

        assert response.status_code == 200
        data = response.json()

        # Should return empty results for non-configured storage system
        assert data["status"] == "success"
        assert data["resources"] == []
        assert data["pagination"]["total"] == 0

    def test_empty_storage_system_parameter(self, client):
        """Test handling of empty storage_system parameter."""
        response = client.get("/api/storage-resources/?storage_system=")

        assert response.status_code == 422
        data = response.json()

        # Should return validation error
        assert "detail" in data
        assert any("storage_system cannot be empty" in str(error) for error in data["detail"])

    def test_data_type_filter_affects_hierarchy(self, client):
        """Test that data_type filter affects the hierarchy appropriately."""
        response = client.get("/api/storage-resources/?data_type=store")

        assert response.status_code == 200
        data = response.json()
        resources = data["resources"]

        # All resources should be store type
        for resource in resources:
            assert resource["storageDataType"]["key"] == "store"

        # Should still have hierarchy
        types = {r["target"]["targetType"] for r in resources}
        # Might not have all types if data is limited, but structure should be consistent
        assert len(types) >= 1


class TestHierarchyValidation:
    """Test hierarchy validation and consistency."""

    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_offering_customers"
    )
    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_resources_by_offering_slugs"
    )
    def test_no_orphaned_resources(
        self,
        mock_get_resources,
        mock_get_customers,
        client,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that no resources are orphaned in the hierarchy."""
        mock_get_customers.return_value = mock_offering_customers
        mock_get_resources.return_value = (
            mock_waldur_resources,
            {
                "current": 1,
                "limit": 100,
                "offset": 0,
                "pages": 1,
                "total": len(mock_waldur_resources),
            },
        )

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Create maps for validation
        resource_map = {r["itemId"]: r for r in resources}

        tenants = [r for r in resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in resources if r["target"]["targetType"] == "project"]

        # Verify all tenants have no parent (top-level)
        for tenant in tenants:
            assert tenant["parentItemId"] is None

        # Verify all customers have valid parent tenants
        for customer in customers:
            parent_id = customer["parentItemId"]
            assert parent_id is not None
            assert parent_id in resource_map
            parent = resource_map[parent_id]
            assert parent["target"]["targetType"] == "tenant"

        # Verify all projects have valid parent customers
        for project in projects:
            parent_id = project["parentItemId"]
            assert parent_id is not None
            assert parent_id in resource_map
            parent = resource_map[parent_id]
            assert parent["target"]["targetType"] == "customer"

    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_offering_customers"
    )
    @patch(
        "waldur_site_agent_cscs_hpc_storage.backend.CscsHpcStorageBackend._get_resources_by_offering_slugs"
    )
    def test_consistent_storage_metadata(
        self,
        mock_get_resources,
        mock_get_customers,
        client,
        mock_waldur_resources,
        mock_offering_customers,
    ):
        """Test that storage system metadata is consistent across hierarchy levels."""
        mock_get_customers.return_value = mock_offering_customers
        mock_get_resources.return_value = (
            mock_waldur_resources,
            {
                "current": 1,
                "limit": 100,
                "offset": 0,
                "pages": 1,
                "total": len(mock_waldur_resources),
            },
        )

        response = client.get("/api/storage-resources/")
        data = response.json()
        resources = data["resources"]

        # Group resources by storage system and data type
        hierarchy_groups = {}

        for resource in resources:
            storage_key = resource["storageSystem"]["key"]
            data_type_key = resource["storageDataType"]["key"]
            group_key = f"{storage_key}-{data_type_key}"

            if group_key not in hierarchy_groups:
                hierarchy_groups[group_key] = {"tenant": None, "customers": [], "projects": []}

            target_type = resource["target"]["targetType"]
            if target_type == "tenant":
                hierarchy_groups[group_key]["tenant"] = resource
            elif target_type == "customer":
                hierarchy_groups[group_key]["customers"].append(resource)
            elif target_type == "project":
                hierarchy_groups[group_key]["projects"].append(resource)

        # Verify each group has consistent metadata
        for group_key, group in hierarchy_groups.items():
            storage_system = None
            storage_file_system = None
            storage_data_type = None

            # Collect metadata from all resources in the group
            all_resources = [group["tenant"]] + group["customers"] + group["projects"]
            all_resources = [r for r in all_resources if r is not None]

            for resource in all_resources:
                if storage_system is None:
                    storage_system = resource["storageSystem"]
                    storage_file_system = resource["storageFileSystem"]
                    storage_data_type = resource["storageDataType"]
                else:
                    # All resources in the group should have identical metadata
                    assert resource["storageSystem"] == storage_system
                    assert resource["storageFileSystem"] == storage_file_system
                    assert resource["storageDataType"] == storage_data_type

    def test_quota_assignment_by_level(self, client):
        """Test that quotas are assigned only to project-level resources."""
        response = client.get("/api/storage-resources/")

        if response.status_code == 200:
            data = response.json()
            resources = data["resources"]

            for resource in resources:
                target_type = resource["target"]["targetType"]

                if target_type in ["tenant", "customer"]:
                    # Tenants and customers should not have quotas
                    assert resource["quotas"] is None
                elif target_type == "project":
                    # Projects should have quotas
                    assert resource["quotas"] is not None
                    assert len(resource["quotas"]) > 0

                    # Verify quota structure
                    for quota in resource["quotas"]:
                        assert "type" in quota
                        assert "quota" in quota
                        assert "unit" in quota
                        assert "enforcementType" in quota
                        assert quota["type"] in ["space", "inodes"]
                        assert quota["enforcementType"] in ["soft", "hard"]


class TestAPIResponseStructure:
    """Test the structure and format of API responses."""

    def test_response_schema_compliance(self, client):
        """Test that the API response follows the expected schema."""
        response = client.get("/api/storage-resources/")

        assert response.status_code == 200
        data = response.json()

        # Verify top-level structure
        required_fields = ["status", "resources", "pagination"]
        for field in required_fields:
            assert field in data

        assert data["status"] == "success"
        assert isinstance(data["resources"], list)
        assert isinstance(data["pagination"], dict)

        # Verify pagination structure
        pagination_fields = ["current", "limit", "offset", "pages", "total"]
        for field in pagination_fields:
            assert field in data["pagination"]

        # Verify resource structure
        if data["resources"]:
            resource = data["resources"][0]
            resource_fields = [
                "itemId",
                "status",
                "mountPoint",
                "permission",
                "quotas",
                "target",
                "storageSystem",
                "storageFileSystem",
                "storageDataType",
            ]
            for field in resource_fields:
                assert field in resource

            # Verify nested structures
            assert "targetType" in resource["target"]
            assert "targetItem" in resource["target"]
            assert "default" in resource["mountPoint"]
            assert "permissionType" in resource["permission"]
            assert "value" in resource["permission"]

    def test_error_response_structure(self, client):
        """Test error response structure."""
        # Test invalid enum value
        response = client.get("/api/storage-resources/?storage_system=invalid")

        assert (
            response.status_code == 200
        )  # Returns success with empty results for non-configured systems
        data = response.json()
        assert data["status"] == "success"
        assert data["resources"] == []

    def test_filters_applied_info(self, client):
        """Test that filters_applied information is included in responses."""
        response = client.get(
            "/api/storage-resources/?storage_system=capstor&data_type=store&status=active"
        )

        assert response.status_code == 200
        data = response.json()

        # Check if filters_applied is documented (implementation dependent)
        # This tests the expected behavior based on the implementation
        if "filters_applied" in data:
            filters = data["filters_applied"]
            assert "storage_system" in filters
            assert "data_type" in filters
            assert "status" in filters
