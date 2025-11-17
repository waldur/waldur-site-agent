"""Tests for CroitS3Backend."""

import pytest
from unittest.mock import Mock, patch

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent_croit_s3.backend import CroitS3Backend
from waldur_site_agent_croit_s3.exceptions import CroitS3UserExistsError


class TestCroitS3Backend:
    """Test CroitS3Backend functionality."""

    @pytest.fixture
    def backend_settings(self):
        """Backend settings for testing."""
        return {
            "api_url": "https://test.croit.io",
            "username": "admin",
            "password": "secret",
            "verify_ssl": False,
            "user_prefix": "waldur_",
            "slug_separator": "_",
            "max_username_length": 64,
        }

    @pytest.fixture
    def backend_components(self):
        """Backend components for testing."""
        return {
            "s3_storage": {
                "accounting_type": "usage",
                "backend_name": "storage",
                "unit_factor": 1073741824,  # GB to bytes
                "enforce_limits": True,
            },
            "s3_objects": {
                "accounting_type": "usage",
                "backend_name": "objects",
                "enforce_limits": True,
            },
            "s3_user": {
                "accounting_type": "limit",
                "backend_name": "user_quota",
            },
        }

    @pytest.fixture
    def mock_client(self):
        """Mock CroitS3Client."""
        client = Mock()
        client.ping.return_value = True
        client.list_users.return_value = []
        client.create_user.return_value = {}
        client.delete_user.return_value = True
        client.get_user_info.return_value = {"uid": "test_user", "name": "Test User"}
        client.get_user_keys.return_value = {
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }
        client.get_user_buckets.return_value = []
        client.set_user_bucket_quota.return_value = None
        client.get_user_quota.return_value = {"bucket_quota": {}, "user_quota": {}}
        return client

    @pytest.fixture
    def waldur_resource(self):
        """Mock Waldur resource."""
        resource = Mock(spec=WaldurResource)
        resource.uuid = "12345678-1234-5678-9abc-123456789abc"
        resource.name = "Test S3 Storage"
        resource.organization = {"slug": "test-org", "name": "Test Organization"}
        resource.project = {"slug": "test-project", "name": "Test Project"}

        # Mock limits
        limits = Mock(spec=ResourceLimits)
        limits.s3_storage = 100  # 100 GB
        limits.s3_objects = 10000  # 10k objects
        resource.limits = limits

        # Mock attributes for resource options
        resource.attributes = {
            "storage_limit": 100,  # 100 GB
            "object_limit": 10000,  # 10k objects
        }

        return resource

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_backend_initialization(
        self, mock_client_class, backend_settings, backend_components
    ):
        """Test backend initialization."""
        mock_client_class.return_value = Mock()

        backend = CroitS3Backend(backend_settings, backend_components)

        assert backend.backend_type == "croit_s3"
        assert backend.user_prefix == "waldur_"
        assert backend.slug_separator == "_"
        assert backend.max_username_length == 64
        mock_client_class.assert_called_once()

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_missing_required_setting(self, mock_client_class, backend_components):
        """Test initialization with missing required setting."""
        incomplete_settings = {"api_url": "https://test.croit.io"}

        with pytest.raises(
            ValueError,
            match="Either 'token' or both 'username' and 'password' must be provided",
        ):
            CroitS3Backend(incomplete_settings, backend_components)

    def test_clean_slug(self, backend_settings, backend_components):
        """Test slug cleaning functionality."""
        with patch("waldur_site_agent_croit_s3.backend.CroitS3Client"):
            backend = CroitS3Backend(backend_settings, backend_components)

            # Test various slug cleaning scenarios
            assert backend._clean_slug("test-org") == "test_org"
            assert backend._clean_slug("Test@Org!") == "test_org"
            assert (
                backend._clean_slug("org___with__underscores") == "org_with_underscores"
            )
            assert backend._clean_slug("___leading_trailing___") == "leading_trailing"
            assert backend._clean_slug("") == "default"
            assert backend._clean_slug("123") == "123"

    def test_generate_username(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Test username generation from slugs."""
        with patch("waldur_site_agent_croit_s3.backend.CroitS3Client"):
            backend = CroitS3Backend(backend_settings, backend_components)

            username = backend._generate_username(waldur_resource)

            # Should be: waldur_test_org_test_project_12345678
            assert username.startswith("waldur_")
            assert "test_org" in username
            assert "test_project" in username
            assert username.endswith("12345678")  # First 8 chars of UUID

    def test_generate_username_truncation(
        self, backend_settings, backend_components, waldur_resource
    ):
        """Test username truncation when too long."""
        # Set very short max length to force truncation
        backend_settings["max_username_length"] = 20

        waldur_resource.organization = {"slug": "very-long-organization-name"}
        waldur_resource.project = {"slug": "very-long-project-name"}

        with patch("waldur_site_agent_croit_s3.backend.CroitS3Client"):
            backend = CroitS3Backend(backend_settings, backend_components)

            username = backend._generate_username(waldur_resource)

            assert len(username) <= 20
            assert username.startswith("waldur_")
            assert username.endswith("12345678")

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_create_resource_success(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test successful resource creation."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.create_resource(waldur_resource)

        assert result is not None
        assert result.backend_id.startswith("waldur_")
        mock_client.create_user.assert_called_once()
        mock_client.set_user_bucket_quota.assert_called_once()

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_create_resource_user_exists(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test resource creation when user already exists."""
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CroitS3UserExistsError("User exists")

        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.create_resource(waldur_resource)

        # Should still return username even if user exists
        assert result is not None
        assert result.backend_id.startswith("waldur_")

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_delete_resource(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test resource deletion."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        # Set the backend_id that would be used for deletion
        waldur_resource.backend_id = "waldur_test_org_test_project_12345678"

        result = backend.delete_resource(waldur_resource)

        assert result is None
        mock_client.delete_user.assert_called_once_with(
            "waldur_test_org_test_project_12345678"
        )

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_get_usage_report(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test usage reporting."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_buckets.return_value = [
            {
                "bucket": "test-bucket-1",
                "usageSum": {
                    "size": 1073741824,
                    "numObjects": 100,
                },  # 1 GB, 100 objects
            },
            {
                "bucket": "test-bucket-2",
                "usageSum": {
                    "size": 2147483648,
                    "numObjects": 200,
                },  # 2 GB, 200 objects
            },
        ]

        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        report = backend._get_usage_report(["waldur_test_org_test_project_12345678"])

        assert "waldur_test_org_test_project_12345678" in report
        user_report = report["waldur_test_org_test_project_12345678"]

        # Storage: 3 GB total, converted to GB units (3 * 1073741824 / 1073741824 = 3)
        assert user_report["s3_storage"]["usage"] == 3

        # Objects: 300 total
        assert user_report["s3_objects"]["usage"] == 300

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_get_resource_metadata(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test resource metadata retrieval."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_info.return_value = {
            "uid": "waldur_test_org_test_project_12345678",
            "name": "Test User",
            "email": "test@example.com",
            "suspended": False,
        }
        mock_client.get_user_buckets.return_value = [
            {
                "bucket": "test-bucket",
                "usageSum": {"size": 1024000, "numObjects": 50},
            }
        ]
        mock_client.get_user_keys.return_value = {
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        }
        mock_client.get_user_quota.return_value = {
            "bucket_quota": {"enabled": True, "maxSize": 100 * 1024 * 1024 * 1024},
            "user_quota": {"enabled": False},
        }
        # Set the API URL to match expected endpoint
        mock_client.api_url = "https://test.croit.io/api"

        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        metadata = backend.get_resource_metadata(
            "waldur_test_org_test_project_12345678"
        )

        assert "s3_credentials" in metadata
        assert "user_info" in metadata
        assert "storage_summary" in metadata
        assert "quotas" in metadata
        assert "backend_info" in metadata

        # Check credentials
        credentials = metadata["s3_credentials"]
        assert credentials["access_key"] == "AKIAIOSFODNN7EXAMPLE"
        assert credentials["secret_key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert credentials["endpoint"] == "https://test.croit.io"

        # Check storage summary
        storage = metadata["storage_summary"]
        assert storage["bucket_count"] == 1
        assert storage["total_size_bytes"] == 1024000
        assert storage["total_objects"] == 50

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_apply_bucket_quotas(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test bucket quota application."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("test_user", waldur_resource.attributes)

        # Check that quota was set with correct values
        mock_client.set_user_bucket_quota.assert_called_once()
        call_args = mock_client.set_user_bucket_quota.call_args[0]
        quota = call_args[1]

        assert quota["enabled"] is True
        assert quota["maxSize"] == 100 * 1073741824  # 100 GB in bytes
        assert quota["maxObjects"] == 10000

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_update_resource_limits(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test resource limits update."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        # Create new limits
        new_limits = Mock(spec=ResourceLimits)
        new_limits.s3_storage = 200  # 200 GB
        new_limits.s3_objects = 20000  # 20k objects

        result = backend.update_resource_limits("test_user", new_limits)

        assert result is True
        # S3 limits are not updated dynamically, so no client calls are expected
        mock_client.set_user_bucket_quota.assert_not_called()

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_list_components(
        self, mock_client_class, backend_settings, backend_components
    ):
        """Test component listing."""
        mock_client_class.return_value = Mock()
        backend = CroitS3Backend(backend_settings, backend_components)

        components = backend.list_components()

        assert "s3_storage" in components
        assert "s3_objects" in components
        assert "s3_user" in components

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_ping(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test backend ping."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.ping()

        assert result is True
        mock_client.ping.assert_called_once()

    @patch("waldur_site_agent_croit_s3.backend.CroitS3Client")
    def test_diagnostics(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test backend diagnostics."""
        mock_client_class.return_value = mock_client
        backend = CroitS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.diagnostics()

        assert result is True
        mock_client.ping.assert_called_once()
        mock_client.list_users.assert_called_once()
