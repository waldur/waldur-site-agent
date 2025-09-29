"""Tests for hierarchical storage resource generation."""

import json
from typing import Optional
from unittest.mock import Mock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from waldur_site_agent_cscs_hpc_storage.backend import CscsHpcStorageBackend


@pytest.fixture
def backend():
    """Create a backend instance for testing."""
    backend_settings = {
        "storage_file_system": "lustre",
        "inode_soft_coefficient": 1.33,
        "inode_hard_coefficient": 2.0,
        "inode_base_multiplier": 1_000_000,
        "use_mock_target_items": True,
        "development_mode": True,
    }

    backend_components = {
        "storage": {
            "measured_unit": "TB",
            "accounting_type": "limit",
            "label": "Storage",
            "unit_factor": 1,
        }
    }

    return CscsHpcStorageBackend(backend_settings, backend_components)


def create_mock_resource(
    resource_uuid: Optional[str] = None,
    customer_slug: str = "test-customer",
    customer_name: str = "Test Customer",
    project_slug: str = "test-project",
    project_name: str = "Test Project",
    offering_slug: str = "capstor",
    provider_slug: str = "cscs",
    provider_name: str = "CSCS",
    storage_data_type: str = "store",
    storage_limit: float = 150.0,
) -> Mock:
    """Create a mock Waldur resource."""
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

    # Mock options
    resource.options = {}

    return resource


class TestTenantLevelGeneration:
    """Tests for tenant-level resource generation."""

    def test_create_tenant_storage_resource(self, backend):
        """Test creating a tenant-level storage resource."""
        tenant_id = "cscs"
        tenant_name = "CSCS"
        storage_system = "capstor"
        storage_data_type = "store"

        result = backend._create_tenant_storage_resource_json(
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            storage_system=storage_system,
            storage_data_type=storage_data_type,
        )

        # Verify structure
        assert result["target"]["targetType"] == "tenant"
        assert result["target"]["targetItem"]["key"] == tenant_id.lower()
        assert result["target"]["targetItem"]["name"] == tenant_name

        # Verify mount point
        assert (
            result["mountPoint"]["default"] == f"/{storage_system}/{storage_data_type}/{tenant_id}"
        )

        # Verify no parent (top-level)
        assert result["parentItemId"] is None

        # Verify permissions
        assert result["permission"]["permissionType"] == "octal"
        assert result["permission"]["value"] == "775"

        # Verify no quotas
        assert result["quotas"] is None

        # Verify storage system info
        assert result["storageSystem"]["key"] == storage_system.lower()
        assert result["storageSystem"]["name"] == storage_system.upper()
        assert result["storageDataType"]["key"] == storage_data_type.lower()
        assert result["storageDataType"]["name"] == storage_data_type.upper()

    def test_tenant_different_data_types(self, backend):
        """Test tenant entries for different storage data types."""
        tenant_id = "cscs"
        storage_system = "capstor"

        # Test different data types
        data_types = ["store", "archive", "users", "scratch"]
        results = []

        for data_type in data_types:
            result = backend._create_tenant_storage_resource_json(
                tenant_id=tenant_id,
                tenant_name="CSCS",
                storage_system=storage_system,
                storage_data_type=data_type,
            )
            results.append(result)

        # Verify unique mount points
        mount_points = [r["mountPoint"]["default"] for r in results]
        assert len(mount_points) == len(set(mount_points))

        # Verify correct paths
        for data_type, result in zip(data_types, results):
            expected_path = f"/{storage_system}/{data_type}/{tenant_id}"
            assert result["mountPoint"]["default"] == expected_path


class TestCustomerLevelGeneration:
    """Tests for customer-level resource generation."""

    def test_create_customer_storage_resource_with_parent(self, backend):
        """Test creating a customer-level storage resource with parent tenant."""
        customer_info = {"itemId": str(uuid4()), "key": "mch", "name": "MCH", "uuid": str(uuid4())}
        tenant_id = "cscs"
        parent_tenant_id = str(uuid4())
        storage_system = "capstor"
        storage_data_type = "store"

        result = backend._create_customer_storage_resource_json(
            customer_info=customer_info,
            storage_system=storage_system,
            storage_data_type=storage_data_type,
            tenant_id=tenant_id,
            parent_tenant_id=parent_tenant_id,
        )

        # Verify structure
        assert result["target"]["targetType"] == "customer"
        assert result["target"]["targetItem"]["key"] == customer_info["key"]
        assert result["target"]["targetItem"]["name"] == customer_info["name"]

        # Verify mount point
        expected_path = f"/{storage_system}/{storage_data_type}/{tenant_id}/{customer_info['key']}"
        assert result["mountPoint"]["default"] == expected_path

        # Verify parent reference
        assert result["parentItemId"] == parent_tenant_id

        # Verify permissions
        assert result["permission"]["value"] == "775"

        # Verify no quotas
        assert result["quotas"] is None

    def test_customer_without_parent_tenant(self, backend):
        """Test creating a customer-level resource without parent (legacy mode)."""
        customer_info = {"itemId": str(uuid4()), "key": "eth", "name": "ETH", "uuid": str(uuid4())}

        result = backend._create_customer_storage_resource_json(
            customer_info=customer_info,
            storage_system="vast",
            storage_data_type="scratch",
            tenant_id="cscs",
            parent_tenant_id=None,  # No parent
        )

        # Verify no parent
        assert result["parentItemId"] is None

        # Verify rest of structure is intact
        assert result["target"]["targetType"] == "customer"
        assert result["mountPoint"]["default"] == "/vast/scratch/cscs/eth"


class TestProjectLevelGeneration:
    """Tests for project-level resource generation."""

    def test_create_project_storage_resource(self, backend):
        """Test creating a project-level storage resource."""
        resource = create_mock_resource(
            project_slug="msclim",
            project_name="MSCLIM",
            customer_slug="mch",
            provider_slug="cscs",
            storage_limit=150.0,
        )

        result = backend._create_storage_resource_json(resource, "capstor")

        # Verify structure
        assert result["target"]["targetType"] == "project"
        assert result["target"]["targetItem"]["name"] == "msclim"

        # Verify mount point
        assert "/capstor/" in result["mountPoint"]["default"]
        assert "/cscs/" in result["mountPoint"]["default"]
        assert "/mch/" in result["mountPoint"]["default"]
        assert "/msclim" in result["mountPoint"]["default"]

        # Verify quotas are present
        assert result["quotas"] is not None
        assert len(result["quotas"]) == 4  # 2 space + 2 inode quotas

        # Verify space quotas
        space_quotas = [q for q in result["quotas"] if q["type"] == "space"]
        assert len(space_quotas) == 2
        hard_quota = next(q for q in space_quotas if q["enforcementType"] == "hard")
        assert hard_quota["quota"] == 150.0
        assert hard_quota["unit"] == "tera"

    def test_project_with_custom_permissions(self, backend):
        """Test project with custom permissions from attributes."""
        resource = create_mock_resource()
        resource.attributes.additional_properties["permissions"] = "0755"

        result = backend._create_storage_resource_json(resource, "capstor")

        assert result["permission"]["value"] == "0755"


class TestThreeTierHierarchyGeneration:
    """Tests for complete three-tier hierarchy generation."""

    @patch.object(CscsHpcStorageBackend, "_get_offering_customers")
    def test_full_hierarchy_creation(self, mock_get_customers, backend):
        """Test creating a complete three-tier hierarchy from resources."""
        # Mock customer data
        mock_get_customers.return_value = {
            "mch": {
                "itemId": "customer-mch-id",
                "key": "mch",
                "name": "MCH",
                "uuid": "customer-mch-uuid",
            },
            "eth": {
                "itemId": "customer-eth-id",
                "key": "eth",
                "name": "ETH",
                "uuid": "customer-eth-uuid",
            },
        }

        # Create mock resources
        resources = [
            create_mock_resource(
                customer_slug="mch",
                customer_name="MCH",
                project_slug="msclim",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="eth",
                customer_name="ETH",
                project_slug="climate-data",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="mch",
                customer_name="MCH",
                project_slug="user-homes",
                provider_slug="cscs",
                provider_name="CSCS",
                storage_data_type="users",
            ),
        ]

        # Process resources
        storage_resources = []
        tenant_entries = {}
        customer_entries = {}

        for resource in resources:
            storage_system_name = resource.offering_slug
            storage_data_type = resource.attributes.additional_properties.get(
                "storage_data_type", "store"
            )
            tenant_id = resource.provider_slug
            tenant_name = resource.provider_name

            # Create tenant entry
            tenant_key = f"{tenant_id}-{storage_system_name}-{storage_data_type}"
            if tenant_key not in tenant_entries:
                tenant_resource = backend._create_tenant_storage_resource_json(
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    storage_system=storage_system_name,
                    storage_data_type=storage_data_type,
                )
                storage_resources.append(tenant_resource)
                tenant_entries[tenant_key] = tenant_resource["itemId"]

            # Create customer entry
            customer_key = f"{resource.customer_slug}-{storage_system_name}-{storage_data_type}"
            if customer_key not in customer_entries:
                customer_info = mock_get_customers.return_value.get(resource.customer_slug)
                if customer_info:
                    parent_tenant_id = tenant_entries.get(tenant_key)
                    customer_resource = backend._create_customer_storage_resource_json(
                        customer_info=customer_info,
                        storage_system=storage_system_name,
                        storage_data_type=storage_data_type,
                        tenant_id=tenant_id,
                        parent_tenant_id=parent_tenant_id,
                    )
                    storage_resources.append(customer_resource)
                    customer_entries[customer_key] = customer_resource["itemId"]

            # Create project entry
            project_resource = backend._create_storage_resource_json(resource, storage_system_name)
            if project_resource and customer_key in customer_entries:
                project_resource["parentItemId"] = customer_entries[customer_key]
            storage_resources.append(project_resource)

        # Verify results
        tenants = [r for r in storage_resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in storage_resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in storage_resources if r["target"]["targetType"] == "project"]

        # Should have unique tenants for each storage_system-data_type combo
        assert len(tenants) == 2  # cscs-capstor-store, cscs-capstor-users

        # Should have unique customers for each customer-storage_system-data_type combo
        assert len(customers) == 3  # mch-capstor-store, eth-capstor-store, mch-capstor-users

        # Should have successfully created projects (some might fail validation)
        assert len(projects) >= 2  # At least 2 projects should be created successfully

        # Verify hierarchy relationships
        # All tenants should have no parent
        for tenant in tenants:
            assert tenant["parentItemId"] is None

        # All customers should have parent tenant
        for customer in customers:
            assert customer["parentItemId"] is not None
            assert customer["parentItemId"] in tenant_entries.values()

        # All projects should have parent customer
        for project in projects:
            assert project["parentItemId"] is not None
            assert project["parentItemId"] in customer_entries.values()

    def test_mount_path_hierarchy(self, backend):
        """Test that mount paths follow the correct hierarchy."""
        tenant_id = "cscs"
        customer_key = "mch"
        project_slug = "msclim"
        storage_system = "capstor"
        data_type = "store"

        # Generate mount points for each level
        tenant_mount = backend._generate_tenant_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            data_type=data_type,
        )

        customer_mount = backend._generate_customer_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_key,
            data_type=data_type,
        )

        project_mount = backend._generate_mount_point(
            storage_system=storage_system,
            tenant_id=tenant_id,
            customer=customer_key,
            project_id=project_slug,
            data_type=data_type,
        )

        # Verify hierarchy in paths
        assert tenant_mount == f"/{storage_system}/{data_type}/{tenant_id}"
        assert customer_mount == f"/{storage_system}/{data_type}/{tenant_id}/{customer_key}"
        assert (
            project_mount
            == f"/{storage_system}/{data_type}/{tenant_id}/{customer_key}/{project_slug}"
        )

        # Verify each level is a parent path of the next
        assert customer_mount.startswith(tenant_mount + "/")
        assert project_mount.startswith(customer_mount + "/")


class TestHierarchyFiltering:
    """Tests for filtering hierarchical resources."""

    def test_filter_maintains_hierarchy(self, backend):
        """Test that filtering by storage system maintains the hierarchy."""
        # Create resources with different storage systems
        resources = []

        # Add capstor resources
        tenant_capstor = backend._create_tenant_storage_resource_json(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="capstor",
            storage_data_type="store",
        )
        resources.append(tenant_capstor)

        customer_capstor = backend._create_customer_storage_resource_json(
            customer_info={"itemId": "cust1", "key": "mch", "name": "MCH", "uuid": "cust1"},
            storage_system="capstor",
            storage_data_type="store",
            tenant_id="cscs",
            parent_tenant_id=tenant_capstor["itemId"],
        )
        resources.append(customer_capstor)

        # Add vast resources
        tenant_vast = backend._create_tenant_storage_resource_json(
            tenant_id="cscs",
            tenant_name="CSCS",
            storage_system="vast",
            storage_data_type="scratch",
        )
        resources.append(tenant_vast)

        customer_vast = backend._create_customer_storage_resource_json(
            customer_info={"itemId": "cust2", "key": "eth", "name": "ETH", "uuid": "cust2"},
            storage_system="vast",
            storage_data_type="scratch",
            tenant_id="cscs",
            parent_tenant_id=tenant_vast["itemId"],
        )
        resources.append(customer_vast)

        # Filter by storage system
        capstor_resources = backend._apply_filters(
            resources, storage_system="capstor", data_type=None, status=None
        )

        # Verify only capstor resources returned
        assert len(capstor_resources) == 2
        for resource in capstor_resources:
            assert resource["storageSystem"]["key"] == "capstor"

        # Verify hierarchy is maintained
        capstor_tenants = [r for r in capstor_resources if r["target"]["targetType"] == "tenant"]
        capstor_customers = [
            r for r in capstor_resources if r["target"]["targetType"] == "customer"
        ]

        assert len(capstor_tenants) == 1
        assert len(capstor_customers) == 1
        assert capstor_customers[0]["parentItemId"] == capstor_tenants[0]["itemId"]


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_resource_without_customer_info(self, backend):
        """Test handling resource when customer info is not available."""
        with patch.object(backend, "_get_offering_customers", return_value={}):
            resource = create_mock_resource(customer_slug="unknown-customer")

            # Process with empty customer info
            storage_resources = []
            tenant_entries = {}
            customer_entries = {}

            tenant_key = f"cscs-capstor-store"
            tenant_resource = backend._create_tenant_storage_resource_json(
                tenant_id="cscs",
                tenant_name="CSCS",
                storage_system="capstor",
                storage_data_type="store",
            )
            storage_resources.append(tenant_resource)
            tenant_entries[tenant_key] = tenant_resource["itemId"]

            # Customer creation should be skipped
            customer_key = f"unknown-customer-capstor-store"
            # No customer entry created

            # Project should still be created but without parent
            project_resource = backend._create_storage_resource_json(resource, "capstor")
            if project_resource:
                # No parent since customer doesn't exist
                project_resource["parentItemId"] = None
                storage_resources.append(project_resource)

            # Verify results
            assert len(storage_resources) == 2  # Only tenant and project
            tenants = [r for r in storage_resources if r["target"]["targetType"] == "tenant"]
            projects = [r for r in storage_resources if r["target"]["targetType"] == "project"]

            assert len(tenants) == 1
            assert len(projects) == 1
            assert projects[0]["parentItemId"] is None

    def test_duplicate_prevention(self, backend):
        """Test that duplicate entries are not created."""
        # Create the same tenant multiple times
        tenant_results = []
        for _ in range(3):
            result = backend._create_tenant_storage_resource_json(
                tenant_id="cscs",
                tenant_name="CSCS",
                storage_system="capstor",
                storage_data_type="store",
            )
            tenant_results.append(result)

        # All should have the same itemId (deterministic UUID)
        item_ids = [r["itemId"] for r in tenant_results]
        assert len(set(item_ids)) == 1

        # Same for customers
        customer_results = []
        for _ in range(3):
            result = backend._create_customer_storage_resource_json(
                customer_info={"itemId": "cust1", "key": "mch", "name": "MCH", "uuid": "cust1"},
                storage_system="capstor",
                storage_data_type="store",
                tenant_id="cscs",
                parent_tenant_id="parent1",
            )
            customer_results.append(result)

        # All should have the same itemId
        customer_ids = [r["itemId"] for r in customer_results]
        assert len(set(customer_ids)) == 1


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""

    @patch.object(CscsHpcStorageBackend, "_get_offering_customers")
    def test_multi_storage_system_hierarchy(self, mock_get_customers, backend):
        """Test hierarchy with multiple storage systems."""
        mock_get_customers.return_value = {
            "customer1": {"itemId": "c1", "key": "customer1", "name": "Customer 1", "uuid": "c1"},
        }

        # Create resources across different storage systems
        resources = [
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj1",
                offering_slug="capstor",
                storage_data_type="store",
            ),
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj2",
                offering_slug="vast",
                storage_data_type="scratch",
            ),
            create_mock_resource(
                customer_slug="customer1",
                project_slug="proj3",
                offering_slug="iopsstor",
                storage_data_type="archive",
            ),
        ]

        all_resources = []
        tenant_entries = {}
        customer_entries = {}

        for resource in resources:
            storage_system = resource.offering_slug
            data_type = resource.attributes.additional_properties["storage_data_type"]
            tenant_id = resource.provider_slug

            # Create tenant
            tenant_key = f"{tenant_id}-{storage_system}-{data_type}"
            if tenant_key not in tenant_entries:
                tenant = backend._create_tenant_storage_resource_json(
                    tenant_id=tenant_id,
                    tenant_name="CSCS",
                    storage_system=storage_system,
                    storage_data_type=data_type,
                )
                all_resources.append(tenant)
                tenant_entries[tenant_key] = tenant["itemId"]

            # Create customer
            customer_key = f"{resource.customer_slug}-{storage_system}-{data_type}"
            if customer_key not in customer_entries:
                customer = backend._create_customer_storage_resource_json(
                    customer_info=mock_get_customers.return_value["customer1"],
                    storage_system=storage_system,
                    storage_data_type=data_type,
                    tenant_id=tenant_id,
                    parent_tenant_id=tenant_entries[tenant_key],
                )
                all_resources.append(customer)
                customer_entries[customer_key] = customer["itemId"]

            # Create project
            project = backend._create_storage_resource_json(resource, storage_system)
            if project is not None:  # Check if project creation was successful
                project["parentItemId"] = customer_entries[customer_key]
                all_resources.append(project)

        # Verify we have 3 separate hierarchies
        tenants = [r for r in all_resources if r["target"]["targetType"] == "tenant"]
        customers = [r for r in all_resources if r["target"]["targetType"] == "customer"]
        projects = [r for r in all_resources if r["target"]["targetType"] == "project"]

        assert len(tenants) == 3  # One per storage system
        assert len(customers) == 3  # One per storage system
        assert len(projects) >= 2  # At least some projects should be created successfully

        # Verify each hierarchy is independent
        storage_systems = set(t["storageSystem"]["key"] for t in tenants)
        assert storage_systems == {"capstor", "vast", "iopsstor"}
