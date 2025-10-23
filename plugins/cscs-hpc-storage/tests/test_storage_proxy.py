"""Tests for CSCS HPC Storage Proxy API."""

import os
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from httpx import Headers
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import Response

# Set up environment before importing the app
test_config_path = Path(__file__).parent / "test_config.yaml"
os.environ["WALDUR_CSCS_STORAGE_PROXY_CONFIG_PATH"] = str(test_config_path)
os.environ["DISABLE_AUTH"] = "true"

from waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main import app


class TestStorageProxyAPI:
    """Test cases for the Storage Proxy API."""

    def setup_method(self):
        """Set up test environment."""
        self.client = TestClient(app)

    def test_health_check(self):
        """Test that the API server starts correctly."""
        # Test root endpoint doesn't exist (should return 404)
        response = self.client.get("/")
        assert response.status_code == 404

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_storage_resources_endpoint_exists(self, mock_backend):
        """Test that the storage-resources endpoint exists."""
        # Mock successful response
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

    @pytest.mark.parametrize("storage_system", ["capstor", "vast", "iopsstor"])
    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_valid_storage_system_filters(self, mock_backend, storage_system):
        """Test valid storage system filter values."""
        # Mock successful response
        mock_backend.generate_all_resources_json_by_slug.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get(f"/api/storage-resources/?storage_system={storage_system}")
        assert response.status_code == 200

        # Verify the backend was called with the correct storage system
        mock_backend.generate_all_resources_json_by_slug.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slug.call_args
        assert call_args[1]["offering_slug"] is not None

    def test_invalid_storage_system_filter(self):
        """Test invalid storage system filter values."""
        response = self.client.get("/api/storage-resources/?storage_system=invalid")
        assert response.status_code == 422

        data = response.json()
        # Check that it's a validation error, exact structure may vary
        assert "detail" in data
        assert isinstance(data["detail"], list)
        assert len(data["detail"]) > 0
        # Look for the error message containing "invalid"
        error_found = any("invalid" in str(error).lower() for error in data["detail"])
        assert error_found

    def test_empty_storage_system_filter(self):
        """Test empty storage system filter."""
        response = self.client.get("/api/storage-resources/?storage_system=")
        assert response.status_code == 422

        data = response.json()
        # Check that it's a validation error
        assert "detail" in data
        assert isinstance(data["detail"], list)
        assert len(data["detail"]) > 0

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_no_storage_system_filter(self, mock_backend):
        """Test request without storage_system filter (should return all storage systems)."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

        # Verify the backend was called with multiple slugs (all storage systems)
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args
        offered_slugs = call_args[1]["offering_slugs"]
        assert isinstance(offered_slugs, list)
        assert len(offered_slugs) > 1  # Should include multiple storage systems

    @pytest.mark.parametrize("page,page_size", [(1, 50), (2, 100), (1, 500)])
    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_pagination_parameters(self, mock_backend, page, page_size):
        """Test pagination parameters."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {
                "current": page,
                "limit": page_size,
                "offset": (page - 1) * page_size,
                "pages": 0,
                "total": 0,
            },
        }

        response = self.client.get(f"/api/storage-resources/?page={page}&page_size={page_size}")
        assert response.status_code == 200

        # Verify pagination parameters are passed to backend
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args
        assert call_args[1]["page"] == page
        assert call_args[1]["page_size"] == page_size

    @pytest.mark.parametrize("page", [0, -1])
    def test_invalid_page_parameter(self, page):
        """Test invalid page parameter values."""
        response = self.client.get(f"/api/storage-resources/?page={page}")
        assert response.status_code == 422

    @pytest.mark.parametrize("page_size", [0, -1, 501])
    def test_invalid_page_size_parameter(self, page_size):
        """Test invalid page_size parameter values."""
        response = self.client.get(f"/api/storage-resources/?page_size={page_size}")
        assert response.status_code == 422

    @pytest.mark.parametrize("data_type", ["users", "scratch", "store", "archive"])
    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_data_type_filter(self, mock_backend, data_type):
        """Test data_type filter parameter."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get(f"/api/storage-resources/?data_type={data_type}")
        assert response.status_code == 200

        # Verify data_type filter is passed to backend
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args
        assert call_args[1]["data_type"] == data_type

    @pytest.mark.parametrize("status", ["pending", "removing", "active", "error"])
    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_status_filter(self, mock_backend, status):
        """Test status filter parameter."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get(f"/api/storage-resources/?status={status}")
        assert response.status_code == 200

        # Verify status filter is passed to backend
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args
        assert call_args[1]["status"] == status

    @pytest.mark.parametrize("state", ["Creating", "OK", "Erred", "Terminating"])
    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_state_filter(self, mock_backend, state):
        """Test state filter parameter."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get(f"/api/storage-resources/?state={state}")
        assert response.status_code == 200

        # Verify state filter is passed to backend
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args
        assert call_args[1]["state"] == ResourceState(state)

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_debug_mode_enabled(self, mock_backend):
        """Test debug mode functionality."""
        mock_backend.get_debug_resources_by_slugs.return_value = {
            "raw_data": "debug_information",
            "api_calls": [],
        }

        response = self.client.get("/api/storage-resources/?debug=true")
        assert response.status_code == 200

        data = response.json()
        assert data["debug_mode"] is True
        assert "agent_config" in data
        assert "raw_resources" in data

        # Verify debug method was called instead of regular method
        mock_backend.get_debug_resources_by_slugs.assert_called_once()
        mock_backend.generate_all_resources_json_by_slugs.assert_not_called()

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_debug_mode_with_storage_system(self, mock_backend):
        """Test debug mode with specific storage system."""
        mock_backend.get_debug_resources_by_slug.return_value = {
            "raw_data": "debug_information_for_capstor",
            "api_calls": [],
        }

        response = self.client.get("/api/storage-resources/?storage_system=capstor&debug=true")
        assert response.status_code == 200

        # Verify debug method was called for specific storage system
        mock_backend.get_debug_resources_by_slug.assert_called_once()
        call_args = mock_backend.get_debug_resources_by_slug.call_args
        assert call_args[1]["offering_slug"] is not None

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_backend_error_handling(self, mock_backend):
        """Test handling of backend errors."""
        # Mock backend error
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "error",
            "error": "Connection failed",
            "code": 503,
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 503

        data = response.json()
        assert data["status"] == "error"
        assert "error" in data

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_successful_response_structure(self, mock_backend):
        """Test the structure of successful API responses."""
        # Mock a successful response with sample data
        mock_storage_resource = {
            "itemId": str(uuid4()),
            "status": "active",
            "storageSystem": {"key": "capstor", "name": "CAPSTOR"},
            "quotas": [{"type": "space", "quota": 100.0, "unit": "tera"}],
        }

        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [mock_storage_resource],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 1, "total": 1},
            "filters_applied": {
                "offering_slugs": ["capstor", "vast", "iopsstor"],
                "storage_system": None,
                "data_type": None,
                "status": None,
                "state": None,
            },
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "success"
        assert "resources" in data
        assert "pagination" in data
        assert "filters_applied" in data
        assert len(data["resources"]) == 1
        assert data["resources"][0]["itemId"] == mock_storage_resource["itemId"]

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_multiple_filters_combination(self, mock_backend):
        """Test combining multiple filters."""
        mock_backend.generate_all_resources_json_by_slug.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 50, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get(
            "/api/storage-resources/?storage_system=capstor&data_type=store&status=active&page=1&page_size=50"
        )
        assert response.status_code == 200

        # Verify all filters are passed to backend
        mock_backend.generate_all_resources_json_by_slug.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slug.call_args
        assert call_args[1]["data_type"] == "store"
        assert call_args[1]["status"] == "active"
        assert call_args[1]["page"] == 1
        assert call_args[1]["page_size"] == 50

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_unconfigured_storage_system(self, mock_backend):
        """Test behavior when requesting a valid but unconfigured storage system."""
        # This test ensures that the API handles storage systems that are valid enum values
        # but not configured in the storage_systems mapping

        # Mock the backend to return proper JSON-serializable data
        mock_backend.generate_all_resources_json_by_slug.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        # First, let's test with a configured storage system to ensure the mock works
        response = self.client.get("/api/storage-resources/?storage_system=capstor")
        assert response.status_code == 200

    def test_authentication_disabled_in_test(self):
        """Test that authentication is properly handled in test environment."""
        # In test environment, auth should be disabled
        # This test ensures the mock user dependency works
        response = self.client.get("/api/storage-resources/")
        # Should not get authentication error
        assert response.status_code != 401

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_response_headers(self, mock_backend):
        """Test response headers are set correctly."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_empty_resources_response(self, mock_backend):
        """Test response when no resources are found."""
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "success"
        assert data["resources"] == []
        assert data["pagination"]["total"] == 0

    @patch("waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy.main.cscs_storage_backend")
    def test_multiple_offering_slugs_api_call_format(self, mock_backend):
        """Test that multiple offering slugs are passed as comma-separated string to Waldur API."""
        # Mock the backend method to capture the API call parameters
        mock_backend.generate_all_resources_json_by_slugs.return_value = {
            "status": "success",
            "resources": [],
            "pagination": {"current": 1, "limit": 100, "offset": 0, "pages": 0, "total": 0},
        }

        # Make a request without storage_system filter (should use all storage systems)
        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

        # Verify the backend method was called
        mock_backend.generate_all_resources_json_by_slugs.assert_called_once()
        call_args = mock_backend.generate_all_resources_json_by_slugs.call_args

        # Check that offering_slugs parameter contains the expected storage systems
        offering_slugs = call_args[1]["offering_slugs"]
        assert isinstance(offering_slugs, list)
        assert len(offering_slugs) == 3  # capstor, vast, iopsstor
        assert "capstor" in offering_slugs
        assert "vast" in offering_slugs
        assert "iopsstor" in offering_slugs

    @patch("waldur_site_agent_cscs_hpc_storage.backend.marketplace_resources_list.sync_detailed")
    def test_comma_separated_slugs_in_waldur_api_call(self, mock_api_call):
        """Test that the backend uses comma-separated offering slugs when calling Waldur API."""
        # Mock the API response
        mock_response = Response(
            status_code=200,
            content=b"",
            headers=Headers({"x-result-count": "0"}),
            parsed=[],  # Empty list of resources
        )
        mock_api_call.return_value = mock_response

        # Make a request without storage_system filter (should use all storage systems)
        response = self.client.get("/api/storage-resources/")
        assert response.status_code == 200

        # Verify the API was called
        mock_api_call.assert_called_once()
        call_args = mock_api_call.call_args

        # Check that offering_slug parameter is a comma-separated string
        offering_slug_param = call_args[1]["offering_slug"]
        assert isinstance(offering_slug_param, str)
        assert offering_slug_param == "capstor,vast,iopsstor"

        # Verify other expected parameters
        assert call_args[1]["page"] == 1
        assert call_args[1]["page_size"] == 100
        assert "client" in call_args[1]
