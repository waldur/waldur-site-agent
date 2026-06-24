"""Tests for configuration loading functions."""

import tempfile
import unittest
from pathlib import Path
from typing import ClassVar

import pytest
import yaml
from pydantic import ValidationError

from waldur_site_agent.common.structures import AccountingType, BackendComponent, LogShippingConfig, Offering
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

    def test_load_configuration_with_oidc(self):
        """Test that load_configuration accepts OIDC fields when waldur_api_token is blank."""
        config_data = {
            "offerings": [
                {
                    "name": "OIDC Offering",
                    "waldur_api_url": "http://localhost:8081/api/",
                    "waldur_api_token": "",
                    "oidc_token_url": "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token",
                    "oidc_client_id": "waldur-agent",
                    "oidc_client_secret": "supersecret",
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
            configuration = load_configuration(config_file_path, user_agent_suffix="sync")
            offering = configuration.waldur_offerings[0]
            assert offering.waldur_api_token == ""
            assert offering.oidc_token_url == "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token"  # noqa: S105
            assert offering.oidc_client_id == "waldur-agent"
            assert offering.oidc_client_secret == "supersecret"  # noqa: S105
        finally:
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


class TestOfferingAuthValidation:
    """Test Offering model validation for authentication config."""

    BASE_FIELDS: ClassVar[dict] = {
        "name": "Test",
        "waldur_api_url": "http://localhost:8081/api/",
        "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
        "backend_type": "test-backend",
    }

    def test_offering_requires_token_or_oidc(self):
        """Offering with no token and no OIDC config should raise a validation error."""
        with pytest.raises(ValidationError, match="oidc_token_url"):
            Offering(**self.BASE_FIELDS, waldur_api_token="")

    def test_offering_accepts_static_token(self):
        """Offering with a static token should be valid."""
        offering = Offering(**self.BASE_FIELDS, waldur_api_token="mytoken")  # noqa: S106
        assert offering.waldur_api_token == "mytoken"  # noqa: S105

    def test_offering_accepts_oidc_config(self):
        """Offering with OIDC fields and no static token should be valid."""
        offering = Offering(
            **self.BASE_FIELDS,
            waldur_api_token="",
            oidc_token_url="https://idp.example.com/token",  # noqa: S106
            oidc_client_id="my-client",
            oidc_client_secret="my-secret",  # noqa: S106
        )
        assert offering.oidc_token_url == "https://idp.example.com/token"  # noqa: S105
        assert offering.oidc_client_id == "my-client"
        assert offering.oidc_client_secret == "my-secret"  # noqa: S105

    def test_offering_rejects_partial_oidc_config(self):
        """Offering with only some OIDC fields set should raise a validation error."""
        with pytest.raises(ValidationError):
            Offering(
                **self.BASE_FIELDS,
                waldur_api_token="",
                oidc_token_url="https://idp.example.com/token",  # noqa: S106
                # missing oidc_client_id and oidc_client_secret
            )

    def test_offering_rejects_non_http_oidc_token_url(self):
        """oidc_token_url must be an http(s) URL when set."""
        with pytest.raises(ValidationError, match="oidc_token_url"):
            Offering(
                **self.BASE_FIELDS,
                waldur_api_token="",
                oidc_token_url="ftp://idp.example.com/token",  # noqa: S106
                oidc_client_id="my-client",
                oidc_client_secret="my-secret",  # noqa: S106
            )


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


# ---------------------------------------------------------------------------
# LogShippingConfig — unit tests (no file I/O required)
# ---------------------------------------------------------------------------

class TestLogShippingConfig(unittest.TestCase):
    """Tests for the LogShippingConfig Pydantic model."""

    def test_defaults_disabled(self):
        """LogShippingConfig is disabled by default."""
        cfg = LogShippingConfig()
        assert cfg.enabled is False
        assert cfg.ship_interval_seconds == 60
        assert cfg.buffer_size_mb == 1

    def test_enabled_flag_can_be_set(self):
        """enabled=True is accepted."""
        cfg = LogShippingConfig(enabled=True)
        assert cfg.enabled is True

    def test_custom_interval_and_buffer(self):
        """Custom values are stored correctly."""
        cfg = LogShippingConfig(enabled=True, ship_interval_seconds=30, buffer_size_mb=5)
        assert cfg.ship_interval_seconds == 30
        assert cfg.buffer_size_mb == 5

    def test_ship_interval_minimum_is_10(self):
        """ship_interval_seconds < 10 must raise ValidationError."""
        with pytest.raises(Exception):
            LogShippingConfig(ship_interval_seconds=5)

    def test_buffer_size_minimum_is_1(self):
        """buffer_size_mb < 1 must raise ValidationError."""
        with pytest.raises(Exception):
            LogShippingConfig(buffer_size_mb=0)


class TestGlobalLogShippingConfig(unittest.TestCase):
    """Tests for the global log_shipping configuration."""

    _BASE_OFFERING = dict(
        name="test",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="tok",
        waldur_offering_uuid="11111111-1111-1111-1111-111111111111",
        backend_type="slurm",
    )

    def test_default_log_shipping_disabled(self):
        """Global log_shipping is disabled by default."""
        ls = LogShippingConfig()
        assert ls.enabled is False

    def test_log_shipping_parses_dict(self):
        """LogShippingConfig parses a dict with custom values."""
        ls = LogShippingConfig(enabled=True, ship_interval_seconds=45)
        assert ls.enabled is True
        assert ls.ship_interval_seconds == 45

    def test_log_shipping_loaded_from_yaml_config(self):
        """Full round-trip: YAML → load_configuration → global log_shipping field."""
        config_data = {
            "log_shipping": {
                "enabled": True,
                "ship_interval_seconds": 120,
                "buffer_size_mb": 2,
            },
            "offerings": [self._BASE_OFFERING],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file_path = f.name

        try:
            configuration = load_configuration(config_file_path)
            ls = configuration.log_shipping
            assert ls.enabled is True
            assert ls.ship_interval_seconds == 120
            assert ls.buffer_size_mb == 2
        finally:
            Path(config_file_path).unlink()
