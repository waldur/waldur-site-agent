"""Tests for REST execution mode selection and configuration validation."""

import pytest
from pydantic import ValidationError
from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import SlurmRestClient
from waldur_site_agent_slurm.schemas import SlurmBackendSettingsSchema, SlurmRestApiConfig

BASE_SETTINGS = {
    "default_account": "root",
    "customer_prefix": "c_",
    "project_prefix": "p_",
    "allocation_prefix": "a_",
}

REST_API_SETTINGS = {
    "url": "http://localhost:6820",
    "username": "waldur-agent",
    "token_env": "SLURM_JWT",
}


class TestBackendClientSelection:
    def test_default_mode_uses_cli_client(self):
        backend = SlurmBackend(dict(BASE_SETTINGS), {})
        assert isinstance(backend.client, SlurmClient)

    def test_rest_mode_uses_rest_client(self):
        settings = {
            **BASE_SETTINGS,
            "cluster_name": "testcluster",
            "execution_mode": "rest",
            "rest_api": dict(REST_API_SETTINGS),
        }
        backend = SlurmBackend(settings, {})
        assert isinstance(backend.client, SlurmRestClient)
        assert backend.client.cluster_name == "testcluster"

    def test_rest_mode_without_rest_api_raises(self):
        settings = {**BASE_SETTINGS, "cluster_name": "testcluster", "execution_mode": "rest"}
        with pytest.raises(BackendError, match="rest_api settings are missing"):
            SlurmBackend(settings, {})

    def test_rest_mode_without_cluster_name_raises(self):
        settings = {
            **BASE_SETTINGS,
            "execution_mode": "rest",
            "rest_api": dict(REST_API_SETTINGS),
        }
        with pytest.raises(BackendError, match="cluster_name is not set"):
            SlurmBackend(settings, {})

    def test_unknown_execution_mode_raises(self):
        # A typo must not silently degrade to CLI mode (schema validation
        # only catches this when the config loader runs).
        settings = {**BASE_SETTINGS, "execution_mode": "rset"}
        with pytest.raises(BackendError, match="Unknown SLURM execution_mode 'rset'"):
            SlurmBackend(settings, {})


class TestSchemaValidation:
    def test_cli_mode_needs_no_rest_settings(self):
        schema = SlurmBackendSettingsSchema(**BASE_SETTINGS)
        assert schema.execution_mode == "cli"
        assert schema.rest_api is None

    def test_valid_rest_configuration(self):
        schema = SlurmBackendSettingsSchema(
            **BASE_SETTINGS,
            cluster_name="testcluster",
            execution_mode="rest",
            rest_api=REST_API_SETTINGS,
        )
        assert schema.rest_api.url == "http://localhost:6820"
        assert schema.rest_api.api_version == "v0.0.43"

    def test_rest_mode_requires_rest_api(self):
        with pytest.raises(ValidationError, match="rest_api settings are missing"):
            SlurmBackendSettingsSchema(
                **BASE_SETTINGS, cluster_name="testcluster", execution_mode="rest"
            )

    def test_rest_mode_requires_cluster_name(self):
        with pytest.raises(ValidationError, match="cluster_name is not set"):
            SlurmBackendSettingsSchema(
                **BASE_SETTINGS, execution_mode="rest", rest_api=REST_API_SETTINGS
            )

    def test_rest_api_requires_token_source(self):
        with pytest.raises(ValidationError, match="token_file or token_env"):
            SlurmRestApiConfig(url="http://localhost:6820", username="waldur-agent")

    def test_invalid_execution_mode_rejected(self):
        with pytest.raises(ValidationError):
            SlurmBackendSettingsSchema(**BASE_SETTINGS, execution_mode="ssh")
