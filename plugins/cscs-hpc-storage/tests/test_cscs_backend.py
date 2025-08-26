"""Tests for CSCS HPC Storage backend."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from waldur_api_client.api.marketplace_resources import marketplace_resources_list
from waldur_api_client.types import Unset

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_cscs_hpc_storage.backend import CscsHpcStorageBackend


class TestCscsHpcStorageBackend:
    """Test cases for CSCS HPC Storage backend."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.backend_settings = {
            "output_directory": self.temp_dir,
            "storage_file_system": "lustre",
            "inode_soft_coefficient": 1.5,
            "inode_hard_coefficient": 2.0,
            "use_mock_target_items": True,
        }
        self.backend_components = {
            "storage": {"unit_factor": 1024**4}  # TB to bytes
        }
        self.backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

    def test_backend_initialization(self):
        """Test backend initialization."""
        assert self.backend.backend_type == "cscs-hpc-storage"
        assert self.backend.output_directory == self.temp_dir
        assert self.backend.storage_file_system == "lustre"
        assert self.backend.inode_soft_coefficient == 1.5
        assert self.backend.inode_hard_coefficient == 2.0
        assert self.backend.use_mock_target_items is True

    def test_ping(self):
        """Test backend ping functionality."""
        assert self.backend.ping() is True

    def test_diagnostics(self):
        """Test backend diagnostics."""
        assert self.backend.diagnostics() is True

    def test_list_components(self):
        """Test listing components."""
        components = self.backend.list_components()
        assert "storage" in components

    def test_generate_mount_point(self):
        """Test mount point generation."""
        mount_point = self.backend._generate_mount_point(
            storage_system="lustre-fs",
            tenant_id="university",
            customer="physics-dept",
            project_id="climate-sim",
        )
        assert mount_point == "/lustre-fs/store/university/physics-dept/climate-sim"

    def test_calculate_inode_quotas(self):
        """Test inode quota calculation."""
        soft, hard = self.backend._calculate_inode_quotas(150.0)  # 150TB
        expected_soft = int(
            150 * self.backend.inode_base_multiplier * 1.5
        )  # 225M with default settings
        expected_hard = int(
            150 * self.backend.inode_base_multiplier * 2.0
        )  # 300M with default settings
        assert soft == expected_soft
        assert hard == expected_hard

    def test_get_target_item_data_mock(self):
        """Test target item data generation with mock enabled."""
        mock_resource = Mock()
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        mock_resource.slug = "climate-sim"
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())

        target_data = self.backend._get_target_item_data(mock_resource, "project")

        assert target_data["name"] == "climate-sim"
        assert "itemId" in target_data
        assert "unixGid" in target_data
        assert target_data["active"] is True

    def test_create_storage_resource_json(self):
        """Test storage resource JSON creation."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Storage"
        mock_resource.slug = "test-storage"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 150}  # 150TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "2770"}
        mock_resource.attributes = mock_attributes

        storage_json = self.backend._create_storage_resource_json(mock_resource, "lustre-fs")

        assert storage_json["itemId"] == mock_resource.uuid.hex
        assert storage_json["status"] == "pending"
        assert (
            storage_json["mountPoint"]["default"]
            == "/lustre-fs/store/university/physics-dept/test-storage"
        )
        assert storage_json["permission"]["value"] == "2770"
        assert len(storage_json["quotas"]) == 4  # 2 space + 2 inode quotas
        assert storage_json["storageSystem"]["key"] == "lustre-fs"
        assert storage_json["storageFileSystem"]["key"] == "lustre"

    def test_write_json_file(self):
        """Test JSON file writing."""
        test_data = {"test": "data", "number": 42}
        filename = "test.json"

        self.backend._write_json_file(filename, test_data)

        filepath = Path(self.temp_dir) / filename
        assert filepath.exists()

        with open(filepath, "r", encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert loaded_data == test_data

    def test_generate_order_json(self):
        """Test order JSON generation."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Order"
        mock_resource.slug = "test-order"
        mock_resource.offering_name = "Test Offering"
        mock_resource.offering_slug = "test-offering"
        mock_resource.customer_slug = "university"
        mock_resource.customer_name = "University"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_name = "Physics Department"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 100}  # 100TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "755"}
        mock_resource.attributes = mock_attributes

        self.backend.generate_order_json(mock_resource, "create")

        # Check that file was created
        files = list(Path(self.temp_dir).glob("*-create_*.json"))
        assert len(files) == 1

        # Check file content
        with open(files[0], "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["status"] == "success"
        assert data["code"] == 200
        assert len(data["result"]["storageResources"]) == 1
        assert data["result"]["storageResources"][0]["itemId"] == mock_resource.uuid.hex

    @patch("waldur_site_agent_cscs_hpc_storage.backend.marketplace_resources_list")
    def test_get_all_storage_resources_with_pagination(self, mock_list):
        """Test fetching all storage resources from API with pagination."""
        # Mock resources
        mock_resource1 = Mock()
        mock_resource1.offering_name = "Test Storage"
        mock_resource1.offering_slug = "test-storage"
        mock_resource1.uuid.hex = "test-uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        # Create mock limits with additional_properties for resource1
        mock_limits1 = Mock()
        mock_limits1.additional_properties = {"storage": 100}
        mock_resource1.limits = mock_limits1

        # Create mock attributes with additional_properties for resource1
        mock_attributes1 = Mock()
        mock_attributes1.additional_properties = {}
        mock_resource1.attributes = mock_attributes1

        mock_resource2 = Mock()
        mock_resource2.offering_name = "Test Storage"
        mock_resource2.offering_slug = "test-storage"
        mock_resource2.uuid.hex = "test-uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"

        # Create mock limits with additional_properties for resource2
        mock_limits2 = Mock()
        mock_limits2.additional_properties = {"storage": 200}
        mock_resource2.limits = mock_limits2

        # Create mock attributes with additional_properties for resource2
        mock_attributes2 = Mock()
        mock_attributes2.additional_properties = {}
        mock_resource2.attributes = mock_attributes2

        # Mock the sync_detailed response
        mock_response = Mock()
        mock_response.parsed = [mock_resource1, mock_resource2]
        # Mock headers as a dict-like object (httpx.Headers behavior)
        mock_headers = Mock()
        mock_headers.get = Mock(return_value="2")
        mock_response.headers = mock_headers
        mock_list.sync_detailed.return_value = mock_response

        # Mock API client
        mock_client = Mock()

        # Test the method with pagination parameters
        resources, pagination_info = self.backend._get_all_storage_resources(
            "test-offering-uuid", mock_client, page=1, page_size=10
        )

        # Should get all 2 resources
        assert len(resources) == 2

        # Check pagination info
        assert pagination_info["total"] == 2
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["pages"] == 1
        assert pagination_info["offset"] == 0

        # Should call the sync_detailed endpoint with pagination
        mock_list.sync_detailed.assert_called_once_with(
            client=mock_client,
            offering_uuid="test-offering-uuid",
            page=1,
            page_size=10,
        )

    def test_create_resource(self):
        """Test resource creation."""
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.offering_name = "test-offering"
        mock_resource.offering_uuid = Mock()
        mock_resource.offering_uuid.hex = str(uuid4())
        mock_resource.customer_slug = "university"
        mock_resource.customer_uuid = Mock()
        mock_resource.customer_uuid.hex = str(uuid4())
        mock_resource.project_slug = "physics-dept"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = str(uuid4())
        mock_resource.offering_slug = "test-offering"
        # Create mock limits with additional_properties
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}  # 50TB
        mock_resource.limits = mock_limits

        # Create mock attributes with additional_properties
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": "770"}
        mock_resource.attributes = mock_attributes

        with patch.object(self.backend, "generate_all_resources_json"):
            resource = self.backend.create_resource(mock_resource)

        assert resource.backend_id == "test-resource"

        # Check that order JSON was created
        files = list(Path(self.temp_dir).glob("*-create_*.json"))
        assert len(files) == 1

    def test_unset_offering_slug_validation(self):
        """Test that Unset offering_slug raises a clear validation error."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource with Unset offering_slug
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.name = "Test Resource"
        mock_resource.offering_slug = Unset()  # This should raise a validation error
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics-dept"

        # Test that validation raises BackendError with clear message
        with pytest.raises(BackendError) as exc_info:
            backend._validate_resource_data(mock_resource)

        error_message = str(exc_info.value)
        assert "offering_slug" in error_message
        assert "missing required fields" in error_message
        assert "test-resource" in error_message  # Should include resource ID for context

    def test_multiple_unset_fields_validation(self):
        """Test validation error when multiple fields are Unset."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource with multiple Unset fields
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.offering_slug = Unset()
        mock_resource.customer_slug = Unset()
        mock_resource.project_slug = Unset()

        # Test that validation raises BackendError listing all missing fields
        with pytest.raises(BackendError) as exc_info:
            backend._validate_resource_data(mock_resource)

        error_message = str(exc_info.value)
        assert "offering_slug" in error_message
        assert "customer_slug" in error_message
        assert "project_slug" in error_message
        assert "test-resource" in error_message

    def test_generate_order_json_with_unset_fields(self):
        """Test that generate_order_json fails fast with validation error for Unset fields."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource with Unset offering_slug
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = "test-resource"
        mock_resource.offering_slug = Unset()
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics-dept"

        # Test that generate_order_json raises validation error
        with pytest.raises(BackendError) as exc_info:
            backend.generate_order_json(mock_resource, "create")

        error_message = str(exc_info.value)
        assert "offering_slug" in error_message
        assert "missing required fields" in error_message

    def test_create_resource_with_unset_fields(self):
        """Test that create_resource fails fast with validation error for Unset fields."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource with Unset fields
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.slug = Unset()  # Missing slug
        mock_resource.offering_slug = "test-offering"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics-dept"

        # Test that create_resource raises validation error
        with pytest.raises(BackendError) as exc_info:
            backend.create_resource(mock_resource)

        error_message = str(exc_info.value)
        assert "slug" in error_message
        assert "missing required fields" in error_message

    def test_invalid_storage_system_type_validation(self):
        """Test that non-string storage_system raises clear validation error."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        # Test with list storage_system (should raise TypeError)
        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, ["system1", "system2"])

        error_message = str(exc_info.value)
        assert "Invalid storage_system type" in error_message
        assert "expected string, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with None storage_system (should raise TypeError)
        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, None)

        error_message = str(exc_info.value)
        assert "Invalid storage_system type" in error_message
        assert "expected string, got NoneType" in error_message

        # Test with empty string storage_system (should raise TypeError)
        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "")

        error_message = str(exc_info.value)
        assert "Empty storage_system provided" in error_message
        assert "valid storage system name is required" in error_message

    def test_pagination_header_case_insensitive(self):
        """Test that pagination header parsing is case-insensitive and pagination info reflects filtered results."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "resource-1"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.offering_slug = "test-storage"

        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 100}
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        # Test with different header case variations
        with patch(
            "waldur_site_agent_cscs_hpc_storage.backend.marketplace_resources_list"
        ) as mock_list:
            mock_response = Mock()
            mock_response.parsed = [mock_resource]
            # httpx.Headers is case-insensitive, so we just need to test it returns the right value
            mock_headers = Mock()
            mock_headers.get = Mock(return_value="5")
            mock_response.headers = mock_headers
            mock_list.sync_detailed.return_value = mock_response

            resources, pagination_info = backend._get_all_storage_resources(
                "test-offering-uuid", Mock(), page=1, page_size=10
            )

            # After filtering, pagination info reflects filtered results (1 resource), not API header (5)
            assert pagination_info["total"] == 1  # Filtered count
            # Verify that get was called with lowercase key (httpx normalizes to lowercase)
            mock_headers.get.assert_called_with("x-result-count")

    def test_invalid_attribute_types_validation(self):
        """Test that non-string attribute values raise clear validation errors."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())
        mock_resource.name = "Test Resource"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Test with list permissions (should raise TypeError)
        mock_attributes = Mock()
        mock_attributes.additional_properties = {"permissions": ["775", "770"]}
        mock_resource.attributes = mock_attributes

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "test-storage")

        error_message = str(exc_info.value)
        assert "Invalid permissions type" in error_message
        assert "expected string or None, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with dict storage_data_type (should raise TypeError)
        mock_attributes.additional_properties = {"storage_data_type": {"type": "store"}}
        mock_resource.attributes = mock_attributes

        with pytest.raises(TypeError) as exc_info:
            backend._create_storage_resource_json(mock_resource, "test-storage")

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string or None, got dict" in error_message
        assert str(mock_resource.uuid) in error_message

    def test_status_mapping_from_waldur_state(self):
        """Test that Waldur resource state is correctly mapped to CSCS status."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Create mock attributes
        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        # Test different state mappings
        test_cases = [
            ("Creating", "pending"),
            ("OK", "active"),
            ("Erred", "error"),
            ("Terminating", "removing"),
            ("Terminated", "removed"),
            ("Unknown", "pending"),  # Default fallback
        ]

        for waldur_state, expected_status in test_cases:
            mock_resource.state = waldur_state

            result = backend._create_storage_resource_json(mock_resource, "test-storage")

            assert result["status"] == expected_status, (
                f"State '{waldur_state}' should map to '{expected_status}'"
            )

        # Test with Unset state
        from waldur_api_client.types import Unset

        mock_resource.state = Unset()
        result = backend._create_storage_resource_json(mock_resource, "test-storage")
        assert result["status"] == "pending"

        # Test with no state attribute
        delattr(mock_resource, "state")
        result = backend._create_storage_resource_json(mock_resource, "test-storage")
        assert result["status"] == "pending"

    def test_error_handling_returns_error_status(self):
        """Test that errors return proper error status and code 500."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        with patch(
            "waldur_site_agent_cscs_hpc_storage.backend.marketplace_resources_list"
        ) as mock_list:
            # Mock the sync_detailed to raise an exception
            mock_list.sync_detailed.side_effect = Exception("API connection failed")

            # Test that generate_all_resources_json returns error response
            result = backend.generate_all_resources_json(
                "test-offering-uuid", Mock(), page=1, page_size=10
            )

            # Verify error response structure
            assert result["status"] == "error"
            assert result["code"] == 500
            assert "Failed to fetch storage resources" in result["message"]
            assert result["result"]["storageResources"] == []
            assert result["result"]["paginate"]["total"] == 0
            assert result["result"]["paginate"]["current"] == 1
            assert result["result"]["paginate"]["limit"] == 10

    def test_dynamic_target_type_mapping(self):
        """Test that storage data type correctly maps to target type."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.project_uuid = Mock()
        mock_resource.project_uuid.hex = "project-uuid"
        mock_resource.state = "OK"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        # Test different storage data types
        test_cases = [
            ("store", "project"),
            ("archive", "project"),
            ("users", "user"),
            ("scratch", "user"),
            ("unknown", "project"),  # Default fallback
        ]

        for storage_data_type, expected_target_type in test_cases:
            # Create mock attributes with storage_data_type
            mock_attributes = Mock()
            mock_attributes.additional_properties = {"storage_data_type": storage_data_type}
            mock_resource.attributes = mock_attributes

            result = backend._create_storage_resource_json(mock_resource, "test-storage")

            actual_target_type = result["target"]["targetType"]
            assert actual_target_type == expected_target_type, (
                f"Storage data type '{storage_data_type}' should map to target type '{expected_target_type}', got '{actual_target_type}'"
            )

            # Verify target item structure based on type
            target_item = result["target"]["targetItem"]
            if expected_target_type == "project":
                assert "status" in target_item
                assert "unixGid" in target_item
                assert target_item["status"] == "open"
            elif expected_target_type == "user":
                assert "email" in target_item
                assert "unixUid" in target_item
                assert "primaryProject" in target_item
                assert target_item["status"] == "active"
                assert "name" in target_item["primaryProject"]
                assert "unixGid" in target_item["primaryProject"]

    def test_quota_float_consistency(self):
        """Test that quotas use float data type for consistency."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits with non-zero storage
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 42.5}  # Use float value
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {}
        mock_resource.attributes = mock_attributes

        result = backend._create_storage_resource_json(mock_resource, "test-storage")

        # Verify all quotas are floats
        quotas = result["quotas"]
        assert quotas is not None, "Quotas should not be None for non-zero storage"

        for quota in quotas:
            quota_value = quota["quota"]
            assert isinstance(quota_value, float), (
                f"Quota value {quota_value} should be float, got {type(quota_value)}"
            )

    def test_storage_data_type_validation(self):
        """Test validation of storage_data_type parameter."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = str(uuid4())

        # Test with invalid data type (list)
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, ["store", "archive"])

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got list" in error_message
        assert str(mock_resource.uuid) in error_message

        # Test with None
        with pytest.raises(TypeError) as exc_info:
            backend._get_target_data(mock_resource, None)

        error_message = str(exc_info.value)
        assert "Invalid storage_data_type" in error_message
        assert "expected string, got NoneType" in error_message

        # Test with valid but unknown storage_data_type (should log warning but not fail)
        result = backend._get_target_data(mock_resource, "unknown_type")
        assert result["targetType"] == "project"  # Should fallback to default

    def test_system_identifiers_use_deterministic_uuids(self):
        """Test that system identifiers use deterministic UUIDs generated from their names."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create a mock resource
        mock_resource = Mock()
        mock_resource.uuid = Mock()
        mock_resource.uuid.hex = "test-uuid"
        mock_resource.slug = "test-resource"
        mock_resource.customer_slug = "university"
        mock_resource.project_slug = "physics"
        mock_resource.state = "OK"

        # Create mock limits
        mock_limits = Mock()
        mock_limits.additional_properties = {"storage": 50}
        mock_resource.limits = mock_limits

        mock_attributes = Mock()
        mock_attributes.additional_properties = {"storage_data_type": "store"}
        mock_resource.attributes = mock_attributes

        result = backend._create_storage_resource_json(mock_resource, "test-storage-system")

        # Verify that system identifiers are in UUID format
        import re

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

        storage_system = result["storageSystem"]
        assert re.match(uuid_pattern, storage_system["itemId"])
        assert storage_system["key"] == "test-storage-system"

        storage_file_system = result["storageFileSystem"]
        assert re.match(uuid_pattern, storage_file_system["itemId"])
        assert storage_file_system["key"] == "lustre"

        storage_data_type = result["storageDataType"]
        assert re.match(uuid_pattern, storage_data_type["itemId"])
        assert storage_data_type["key"] == "store"

        # Test that UUIDs are deterministic (same input produces same UUID)
        result2 = backend._create_storage_resource_json(mock_resource, "test-storage-system")
        assert result["storageSystem"]["itemId"] == result2["storageSystem"]["itemId"]
        assert result["storageFileSystem"]["itemId"] == result2["storageFileSystem"]["itemId"]
        assert result["storageDataType"]["itemId"] == result2["storageDataType"]["itemId"]

        # Test target item UUIDs are also deterministic UUIDs
        target_item = result["target"]["targetItem"]
        assert re.match(uuid_pattern, target_item["itemId"])

        # Verify determinism for target items too
        target_item2 = result2["target"]["targetItem"]
        assert target_item["itemId"] == target_item2["itemId"]

    def test_filtering_by_storage_system(self):
        """Test filtering storage resources by storage system."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock storage resources with different storage systems
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "iopsstor", "name": "IOPSSTOR"},
                "storageDataType": {"key": "archive", "name": "ARCHIVE"},
                "status": "active",
            },
        ]

        # Test filtering by storage_system
        filtered = backend._apply_filters(mock_resources, storage_system="capstor")
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "capstor"

        filtered = backend._apply_filters(mock_resources, storage_system="vast")
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "vast"

        # Test with non-existent storage system
        filtered = backend._apply_filters(mock_resources, storage_system="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_data_type(self):
        """Test filtering storage resources by data type."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock storage resources with different data types
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "scratch", "name": "SCRATCH"},
                "status": "active",
            },
        ]

        # Test filtering by data_type
        filtered = backend._apply_filters(mock_resources, data_type="store")
        assert len(filtered) == 1
        assert filtered[0]["storageDataType"]["key"] == "store"

        filtered = backend._apply_filters(mock_resources, data_type="users")
        assert len(filtered) == 1
        assert filtered[0]["storageDataType"]["key"] == "users"

        # Test with non-existent data type
        filtered = backend._apply_filters(mock_resources, data_type="nonexistent")
        assert len(filtered) == 0

    def test_filtering_by_status(self):
        """Test filtering storage resources by status."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock storage resources with different statuses
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "scratch", "name": "SCRATCH"},
                "status": "removing",
            },
        ]

        # Test filtering by status
        filtered = backend._apply_filters(mock_resources, status="active")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "active"

        filtered = backend._apply_filters(mock_resources, status="pending")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "pending"

        filtered = backend._apply_filters(mock_resources, status="removing")
        assert len(filtered) == 1
        assert filtered[0]["status"] == "removing"

        # Test with non-existent status
        filtered = backend._apply_filters(mock_resources, status="nonexistent")
        assert len(filtered) == 0

    def test_filtering_combined(self):
        """Test filtering storage resources with multiple filter criteria."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock storage resources
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "pending",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "active",
            },
        ]

        # Test combined filtering: capstor + store + active
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store", status="active"
        )
        assert len(filtered) == 1
        assert filtered[0]["storageSystem"]["key"] == "capstor"
        assert filtered[0]["storageDataType"]["key"] == "store"
        assert filtered[0]["status"] == "active"

        # Test combined filtering: capstor + store (should return 2)
        filtered = backend._apply_filters(
            mock_resources, storage_system="capstor", data_type="store"
        )
        assert len(filtered) == 2
        assert all(r["storageSystem"]["key"] == "capstor" for r in filtered)
        assert all(r["storageDataType"]["key"] == "store" for r in filtered)

        # Test combined filtering that returns no results
        filtered = backend._apply_filters(mock_resources, storage_system="vast", data_type="users")
        assert len(filtered) == 0

    def test_filtering_no_filters_applied(self):
        """Test that no filtering is applied when no filters are provided."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock storage resources
        mock_resources = [
            {
                "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
                "storageDataType": {"key": "store", "name": "STORE"},
                "status": "active",
            },
            {
                "storageSystem": {"key": "vast", "name": "VAST"},
                "storageDataType": {"key": "users", "name": "USERS"},
                "status": "pending",
            },
        ]

        # Test no filtering (should return all resources)
        filtered = backend._apply_filters(mock_resources)
        assert len(filtered) == 2
        assert filtered == mock_resources

    @patch("waldur_site_agent_cscs_hpc_storage.backend.marketplace_resources_list")
    def test_pagination_info_updated_after_filtering(self, mock_list):
        """Test that pagination info is updated to reflect filtered results, not raw API results."""
        backend = CscsHpcStorageBackend(self.backend_settings, self.backend_components)

        # Create mock resources with different storage systems
        mock_resource1 = Mock()
        mock_resource1.uuid.hex = "uuid-1"
        mock_resource1.slug = "resource-1"
        mock_resource1.customer_slug = "university"
        mock_resource1.project_slug = "physics"
        mock_resource1.offering_slug = "capstor"
        mock_resource1.state = "OK"
        mock_limits1 = Mock()
        mock_limits1.additional_properties = {"storage": 100}
        mock_resource1.limits = mock_limits1
        mock_attributes1 = Mock()
        mock_attributes1.additional_properties = {"storage_system": "capstor"}
        mock_resource1.attributes = mock_attributes1

        mock_resource2 = Mock()
        mock_resource2.uuid.hex = "uuid-2"
        mock_resource2.slug = "resource-2"
        mock_resource2.customer_slug = "university"
        mock_resource2.project_slug = "chemistry"
        mock_resource2.offering_slug = "vast"
        mock_resource2.state = "OK"
        mock_limits2 = Mock()
        mock_limits2.additional_properties = {"storage": 200}
        mock_resource2.limits = mock_limits2
        mock_attributes2 = Mock()
        mock_attributes2.additional_properties = {"storage_system": "vast"}
        mock_resource2.attributes = mock_attributes2

        # Mock API response: 2 total resources from different storage systems
        mock_response = Mock()
        mock_response.parsed = [mock_resource1, mock_resource2]
        mock_headers = Mock()
        mock_headers.get = Mock(return_value="2")  # API says there are 2 total resources
        mock_response.headers = mock_headers
        mock_list.sync_detailed.return_value = mock_response

        # Test filtering by storage_system that matches only 1 resource
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid", Mock(), page=1, page_size=10, storage_system="capstor"
        )

        # Should get only 1 filtered resource
        assert len(resources) == 1
        assert resources[0]["storageSystem"]["key"] == "capstor"

        # Pagination info should reflect filtered results, not original API results
        assert pagination_info["total"] == 1  # Filtered count, not API count (2)
        assert pagination_info["pages"] == 1
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["offset"] == 0

        # Test filtering by storage_system that matches no resources
        resources, pagination_info = backend._get_all_storage_resources(
            "test-offering-uuid", Mock(), page=1, page_size=10, storage_system="nonexistent"
        )

        # Should get no resources
        assert len(resources) == 0

        # Pagination info should show 0 total, not original API count
        assert pagination_info["total"] == 0  # Should be 0, not 2
        assert pagination_info["pages"] == 1  # Should be 1 (minimum pages)
        assert pagination_info["current"] == 1
        assert pagination_info["limit"] == 10
        assert pagination_info["offset"] == 0
