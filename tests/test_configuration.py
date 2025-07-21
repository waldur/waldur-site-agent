"""Tests for configuration loading functions."""

import tempfile
from pathlib import Path

import yaml

from waldur_site_agent.common.utils import load_configuration


class TestConfigurationLoading:
    """Test cases for configuration loading utilities."""

    def test_load_configuration_sets_user_agent_and_version(self):
        """Test that load_configuration sets proper user agent and version."""
        # Create a temporary config file
        config_data = {
            "offerings": [
                {
                    "name": "Test Offering",
                    "waldur_api_url": "http://localhost:8081/api/",
                    "waldur_api_token": "test-token",
                    "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
                    "backend_type": "test-backend",
                    "backend_settings": {},
                    "backend_components": {},
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file_path = f.name

        try:
            # Load configuration
            configuration = load_configuration(config_file_path, user_agent_suffix="sync")

            # Verify user agent is set correctly
            assert configuration.waldur_user_agent is not None
            assert configuration.waldur_user_agent.startswith("waldur-site-agent-sync/")
            assert "0.1.0" in configuration.waldur_user_agent  # Version should be in user agent

            # Verify version is set
            assert configuration.waldur_site_agent_version is not None
            assert configuration.waldur_site_agent_version == "0.1.0"

            # Verify offerings are loaded
            assert len(configuration.waldur_offerings) == 1
            assert configuration.waldur_offerings[0].name == "Test Offering"

        finally:
            # Clean up temp file
            Path(config_file_path).unlink()

    def test_load_configuration_with_sentry(self):
        """Test that load_configuration handles Sentry DSN correctly."""
        # Create a temporary config file with Sentry DSN
        config_data = {
            "offerings": [
                {
                    "name": "Test Offering",
                    "waldur_api_url": "http://localhost:8081/api/",
                    "waldur_api_token": "test-token",
                    "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
                    "backend_type": "test-backend",
                    "backend_settings": {},
                    "backend_components": {},
                }
            ],
            "sentry_dsn": "https://example@sentry.io/123456",
            "timezone": "Europe/London",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file_path = f.name

        try:
            # Load configuration
            configuration = load_configuration(config_file_path, user_agent_suffix="sync")

            # Verify Sentry DSN is stored (but we can't easily test if Sentry was initialized)
            assert configuration.sentry_dsn == "https://example@sentry.io/123456"

            # Verify timezone
            assert configuration.timezone == "Europe/London"

        finally:
            # Clean up temp file
            Path(config_file_path).unlink()
