"""Tests for configuration loading functions."""

import tempfile
from pathlib import Path

import yaml

from waldur_site_agent.common.structures import AccountingType, BackendComponent
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

            # Verify version is set
            assert configuration.waldur_site_agent_version is not None

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


class TestBackendComponentPrepaidFields:
    """Test cases for BackendComponent prepaid duration fields."""

    def test_accepts_all_prepaid_duration_fields(self):
        component = BackendComponent(
            measured_unit="k-Hours",
            accounting_type=AccountingType.USAGE,
            label="CPU",
            is_prepaid=True,
            min_prepaid_duration=3,
            max_prepaid_duration=36,
            prepaid_duration_step=3,
            min_renewal_duration=1,
            max_renewal_duration=12,
            renewal_duration_step=1,
        )
        assert component.is_prepaid is True
        assert component.min_prepaid_duration == 3
        assert component.max_prepaid_duration == 36
        assert component.prepaid_duration_step == 3
        assert component.min_renewal_duration == 1
        assert component.max_renewal_duration == 12
        assert component.renewal_duration_step == 1

    def test_prepaid_fields_default_to_none(self):
        component = BackendComponent(
            measured_unit="Hours",
            accounting_type=AccountingType.USAGE,
            label="CPU",
        )
        assert component.is_prepaid is None
        assert component.min_prepaid_duration is None
        assert component.max_prepaid_duration is None
        assert component.prepaid_duration_step is None
        assert component.min_renewal_duration is None
        assert component.max_renewal_duration is None
        assert component.renewal_duration_step is None

    def test_to_dict_includes_prepaid_fields_when_set(self):
        component = BackendComponent(
            measured_unit="k-Hours",
            accounting_type=AccountingType.USAGE,
            label="CPU",
            is_prepaid=True,
            min_prepaid_duration=1,
            max_prepaid_duration=12,
        )
        d = component.to_dict()
        assert d["is_prepaid"] is True
        assert d["min_prepaid_duration"] == 1
        assert d["max_prepaid_duration"] == 12
        assert "prepaid_duration_step" not in d  # unset fields excluded

    def test_to_dict_excludes_unset_prepaid_fields(self):
        component = BackendComponent(
            measured_unit="Hours",
            accounting_type=AccountingType.USAGE,
            label="CPU",
        )
        d = component.to_dict()
        assert "is_prepaid" not in d
        assert "min_prepaid_duration" not in d
