"""Tests for CSCS HPC Storage backend."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from waldur_api_client.api.marketplace_resources import marketplace_resources_list

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

    @patch("waldur_site_agent_cscs_hpc_storage.backend.get_all_paginated")
    def test_get_all_storage_resources_with_pagination(self, mock_get_all_paginated):
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

        # Mock the utility function to return all resources
        mock_get_all_paginated.return_value = [mock_resource1, mock_resource2]

        # Mock API client
        mock_client = Mock()

        # Test the method
        resources = self.backend._get_all_storage_resources("test-offering-uuid", mock_client)

        # Should get all 2 resources
        assert len(resources) == 2

        # Should call the pagination utility with correct parameters
        mock_get_all_paginated.assert_called_once_with(
            marketplace_resources_list.sync,
            mock_client,
            offering_uuid="test-offering-uuid",
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
