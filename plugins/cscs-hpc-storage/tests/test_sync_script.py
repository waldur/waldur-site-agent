"""Tests for CSCS HPC Storage sync script."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from waldur_site_agent_cscs_hpc_storage.sync_script import (
    main,
    setup_logging,
    sync_offering_resources,
)


class TestSyncScript:
    """Test cases for CSCS HPC Storage sync script."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.offering_config = {
            "name": "Test CSCS Offering",
            "waldur_api_url": "http://localhost:8081/api/",
            "waldur_api_token": "test-token",
            "waldur_offering_uuid": str(uuid4()),
            "backend_type": "cscs-hpc-storage",
            "backend_settings": {
                "output_directory": self.temp_dir,
                "storage_file_system": "lustre",
            },
            "backend_components": {"storage": {}},
        }

    def test_setup_logging(self):
        """Test logging setup."""
        # Should not raise any exceptions
        setup_logging(verbose=False)
        setup_logging(verbose=True)

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.get_client")
    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.CscsHpcStorageBackend")
    def test_sync_offering_resources_success(self, mock_backend_class, mock_get_client):
        """Test successful offering synchronization."""
        # Mock client and backend
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_backend = Mock()
        mock_backend._get_all_storage_resources.return_value = [
            {"itemId": "test-resource-1"},
            {"itemId": "test-resource-2"},
        ]
        mock_backend_class.return_value = mock_backend

        # Test successful sync
        result = sync_offering_resources(self.offering_config, dry_run=False)

        assert result is True
        mock_get_client.assert_called_once_with(
            api_url="http://localhost:8081/api/", access_token="test-token"
        )
        mock_backend_class.assert_called_once()
        mock_backend.generate_all_resources_json.assert_called_once_with(
            self.offering_config["waldur_offering_uuid"], mock_client
        )

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.get_client")
    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.CscsHpcStorageBackend")
    def test_sync_offering_resources_dry_run(self, mock_backend_class, mock_get_client):
        """Test dry run mode."""
        # Mock client and backend
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_backend = Mock()
        mock_backend._get_all_storage_resources.return_value = [{"itemId": "test-resource-1"}]
        mock_backend_class.return_value = mock_backend

        # Test dry run
        result = sync_offering_resources(self.offering_config, dry_run=True)

        assert result is True
        mock_backend._get_all_storage_resources.assert_called_once()
        # Should not call generate_all_resources_json in dry run mode
        mock_backend.generate_all_resources_json.assert_not_called()

    def test_sync_offering_resources_wrong_backend_type(self):
        """Test skipping offering with wrong backend type."""
        config = self.offering_config.copy()
        config["backend_type"] = "slurm"

        result = sync_offering_resources(config)

        assert result is True  # Should succeed but skip

    def test_sync_offering_resources_missing_uuid(self):
        """Test error handling for missing offering UUID."""
        config = self.offering_config.copy()
        del config["waldur_offering_uuid"]

        result = sync_offering_resources(config)

        assert result is False

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.get_client")
    def test_sync_offering_resources_api_error(self, mock_get_client):
        """Test error handling for API failures."""
        mock_get_client.side_effect = Exception("API connection failed")

        result = sync_offering_resources(self.offering_config)

        assert result is False

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.load_configuration")
    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.sync_offering_resources")
    @patch("sys.argv", ["waldur_cscs_storage_sync", "--config", "/fake/config.yaml"])
    def test_main_success(self, mock_sync_offering, mock_load_config):
        """Test main function with successful sync."""
        # Mock configuration
        mock_offering = Mock()
        mock_offering.uuid = str(uuid4())
        mock_offering.name = self.offering_config["name"]
        mock_offering.api_url = self.offering_config["waldur_api_url"]
        mock_offering.api_token = self.offering_config["waldur_api_token"]
        mock_offering.backend_type = self.offering_config["backend_type"]
        mock_offering.backend_settings = self.offering_config["backend_settings"]
        mock_offering.backend_components = self.offering_config["backend_components"]

        mock_config = Mock()
        mock_config.waldur_offerings = [mock_offering]
        mock_load_config.return_value = mock_config

        mock_sync_offering.return_value = True

        # Should not raise SystemExit
        with patch("pathlib.Path.exists", return_value=True):
            main()

        mock_sync_offering.assert_called_once()

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.load_configuration")
    @patch("sys.argv", ["waldur_cscs_storage_sync", "--config", "/fake/config.yaml"])
    def test_main_config_not_found(self, mock_load_config):
        """Test main function with missing config file."""
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(SystemExit) as exc_info:
                main()

            assert exc_info.value.code == 1

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.load_configuration")
    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.sync_offering_resources")
    @patch(
        "sys.argv",
        [
            "waldur_cscs_storage_sync",
            "--config",
            "/fake/config.yaml",
            "--offering-uuid",
            "specific-uuid",
        ],
    )
    def test_main_specific_offering(self, mock_sync_offering, mock_load_config):
        """Test main function with specific offering UUID."""
        specific_uuid = "specific-uuid"

        # Mock configuration with multiple offerings - create proper Mock objects
        mock_offering1 = Mock()
        mock_offering1.uuid = specific_uuid
        mock_offering1.name = self.offering_config["name"]
        mock_offering1.api_url = self.offering_config["waldur_api_url"]
        mock_offering1.api_token = self.offering_config["waldur_api_token"]
        mock_offering1.backend_type = self.offering_config["backend_type"]
        mock_offering1.backend_settings = self.offering_config["backend_settings"]
        mock_offering1.backend_components = self.offering_config["backend_components"]

        mock_offering2 = Mock()
        mock_offering2.uuid = "other-uuid"
        mock_offering2.name = self.offering_config["name"]
        mock_offering2.api_url = self.offering_config["waldur_api_url"]
        mock_offering2.api_token = self.offering_config["waldur_api_token"]
        mock_offering2.backend_type = self.offering_config["backend_type"]
        mock_offering2.backend_settings = self.offering_config["backend_settings"]
        mock_offering2.backend_components = self.offering_config["backend_components"]

        mock_config = Mock()
        mock_config.waldur_offerings = [mock_offering1, mock_offering2]
        mock_load_config.return_value = mock_config

        mock_sync_offering.return_value = True

        with patch("pathlib.Path.exists", return_value=True):
            main()

        # Should only sync the specific offering - build expected dict manually
        expected_offering_dict = {
            "name": self.offering_config["name"],
            "waldur_api_url": self.offering_config["waldur_api_url"],
            "waldur_api_token": self.offering_config["waldur_api_token"],
            "waldur_offering_uuid": specific_uuid,
            "backend_type": self.offering_config["backend_type"],
            "backend_settings": self.offering_config["backend_settings"],
            "backend_components": self.offering_config["backend_components"],
        }
        mock_sync_offering.assert_called_once_with(expected_offering_dict, False)

    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.load_configuration")
    @patch("waldur_site_agent_cscs_hpc_storage.sync_script.sync_offering_resources")
    @patch(
        "sys.argv",
        [
            "waldur_cscs_storage_sync",
            "--config",
            "/fake/config.yaml",
            "--dry-run",
            "--verbose",
        ],
    )
    def test_main_dry_run_verbose(self, mock_sync_offering, mock_load_config):
        """Test main function with dry run and verbose flags."""
        mock_offering = Mock()
        offering_uuid = str(uuid4())
        mock_offering.uuid = offering_uuid
        mock_offering.name = self.offering_config["name"]
        mock_offering.api_url = self.offering_config["waldur_api_url"]
        mock_offering.api_token = self.offering_config["waldur_api_token"]
        mock_offering.backend_type = self.offering_config["backend_type"]
        mock_offering.backend_settings = self.offering_config["backend_settings"]
        mock_offering.backend_components = self.offering_config["backend_components"]

        mock_config = Mock()
        mock_config.waldur_offerings = [mock_offering]
        mock_load_config.return_value = mock_config

        mock_sync_offering.return_value = True

        with patch("pathlib.Path.exists", return_value=True):
            main()

        # Should call with dry_run=True - build expected dict
        expected_offering_dict = {
            "name": self.offering_config["name"],
            "waldur_api_url": self.offering_config["waldur_api_url"],
            "waldur_api_token": self.offering_config["waldur_api_token"],
            "waldur_offering_uuid": offering_uuid,
            "backend_type": self.offering_config["backend_type"],
            "backend_settings": self.offering_config["backend_settings"],
            "backend_components": self.offering_config["backend_components"],
        }
        mock_sync_offering.assert_called_once_with(expected_offering_dict, True)
