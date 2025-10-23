"""Tests for OKD backend implementation."""

import unittest
from unittest.mock import MagicMock, patch

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.offering_user import OfferingUser
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import ClientResource

from waldur_site_agent_okd.backend import OkdBackend
from waldur_site_agent_okd.client import OkdClient
from waldur_site_agent.backend.structures import BackendResourceInfo


class TestOkdBackend(unittest.TestCase):
    """Test cases for OKD backend."""

    def setUp(self):
        """Set up test fixtures."""
        self.okd_settings = {
            "api_url": "https://api.okd.example.com:8443",
            "token": "test-token",
            "verify_cert": False,
            "namespace_prefix": "waldur-",
            "customer_prefix": "org-",
            "project_prefix": "proj-",
            "allocation_prefix": "alloc-",
            "default_role": "edit",
        }

        self.okd_components = {
            "cpu": {
                "measured_unit": "Core",
                "accounting_type": "limit",
                "label": "CPU",
                "unit_factor": 1,
            },
            "memory": {
                "measured_unit": "GB",
                "accounting_type": "limit",
                "label": "Memory",
                "unit_factor": 1,
            },
            "storage": {
                "measured_unit": "GB",
                "accounting_type": "limit",
                "label": "Storage",
                "unit_factor": 1,
            },
            "pods": {
                "measured_unit": "Count",
                "accounting_type": "limit",
                "label": "Pods",
                "unit_factor": 1,
            },
        }

        self.backend = OkdBackend(self.okd_settings, self.okd_components)
        self.backend.client = MagicMock(spec=OkdClient)

    def test_ping_successful(self):
        """Test successful ping to OKD cluster."""
        self.backend.client.ping.return_value = True
        result = self.backend.ping()
        self.assertTrue(result)
        self.backend.client.ping.assert_called_once()

    def test_ping_failed(self):
        """Test failed ping to OKD cluster."""
        self.backend.client.ping.return_value = False
        result = self.backend.ping()
        self.assertFalse(result)

    def test_ping_with_exception(self):
        """Test ping with raise_exception flag."""
        self.backend.client.ping.return_value = False
        with self.assertRaises(BackendError):
            self.backend.ping(raise_exception=True)

    def test_list_components(self):
        """Test listing available components."""
        components = self.backend.list_components()
        self.assertEqual(components, ["cpu", "memory", "storage", "pods"])

    def test_get_customer_backend_id(self):
        """Test customer backend ID generation."""
        customer_id = self.backend._get_customer_backend_id("acme-corp")
        self.assertEqual(customer_id, "waldur-org-acme-corp")

    def test_get_project_backend_id(self):
        """Test project backend ID generation."""
        project_id = self.backend._get_project_backend_id("dev-project")
        self.assertEqual(project_id, "waldur-proj-dev-project")

    def test_get_allocation_backend_id(self):
        """Test allocation backend ID generation."""
        allocation_id = self.backend._get_allocation_backend_id("alloc-123")
        self.assertEqual(allocation_id, "waldur-alloc-alloc-123")

    def test_create_resource(self):
        """Test creating OKD project for resource."""
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.uuid.hex = "abcd1234"
        waldur_resource.name = "Test Resource"
        waldur_resource.customer_slug = "acme"
        waldur_resource.project_slug = "dev"
        waldur_resource.customer_name = "ACME Corp"
        waldur_resource.project_name = "Development"
        waldur_resource.backend_id = None
        waldur_resource.limits = MagicMock()
        waldur_resource.limits.cpu = 0
        waldur_resource.limits.memory = 0

        self.backend.client.get_resource.return_value = None
        self.backend.client.create_resource.return_value = "waldur-alloc-abcd1234"

        result = self.backend.create_resource(waldur_resource)

        self.assertIsInstance(result, BackendResourceInfo)
        self.assertEqual(result.backend_id, "waldur-alloc-abcd1234")
        self.backend.client.create_resource.assert_called()
        # Verify backend_id was set on the resource
        self.assertEqual(waldur_resource.backend_id, "waldur-alloc-abcd1234")

    def test_create_resource_missing_slugs(self):
        """Test creating resource with missing slug fields."""
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.uuid = "test-uuid"
        waldur_resource.customer_slug = None
        waldur_resource.project_slug = "dev"

        with self.assertRaises(BackendError) as context:
            self.backend.create_resource(waldur_resource)

        self.assertIn("missing slug fields", str(context.exception))

    def test_delete_resource(self):
        """Test deleting OKD project."""
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.backend_id = "waldur-alloc-test123"
        waldur_resource.uuid = "test-uuid"

        self.backend.delete_resource(waldur_resource)

        self.backend.client.delete_resource.assert_called_once_with("waldur-alloc-test123")

    def test_delete_resource_no_backend_id(self):
        """Test deleting resource without backend_id."""
        waldur_resource = MagicMock(spec=WaldurResource)
        waldur_resource.backend_id = ""
        waldur_resource.uuid = "test-uuid"

        # Should not raise exception, just log warning
        self.backend.delete_resource(waldur_resource)

        self.backend.client.delete_resource.assert_not_called()

    def test_set_resource_limits(self):
        """Test setting resource quotas."""
        resource_backend_id = "waldur-alloc-test123"
        limits = {"cpu": 4, "memory": 16, "storage": 100, "pods": 10}

        self.backend.set_resource_limits(resource_backend_id, limits)

        self.backend.client.set_resource_limits.assert_called_once_with(resource_backend_id, limits)

    def test_set_resource_limits_exception(self):
        """Test setting resource limits with exception."""
        resource_backend_id = "waldur-alloc-test123"
        limits = {"cpu": 4, "memory": 16}

        self.backend.client.set_resource_limits.side_effect = BackendError("Test error")

        with self.assertRaises(BackendError):
            self.backend.set_resource_limits(resource_backend_id, limits)

    def test_get_usage_report(self):
        """Test getting usage report."""
        resource_ids = ["waldur-alloc-test1", "waldur-alloc-test2"]

        self.backend.client.get_usage_report.return_value = [
            {"resource_id": "waldur-alloc-test1", "usage": {"cpu": 2, "memory": 8, "storage": 50}},
            {"resource_id": "waldur-alloc-test2", "usage": {"cpu": 1, "memory": 4}},
        ]

        report = self.backend._get_usage_report(resource_ids)

        self.assertIn("waldur-alloc-test1", report)
        self.assertIn("waldur-alloc-test2", report)
        self.assertEqual(report["waldur-alloc-test1"]["TOTAL_ACCOUNT_USAGE"]["cpu"], 2)
        self.assertEqual(
            report["waldur-alloc-test2"]["TOTAL_ACCOUNT_USAGE"]["storage"], 0
        )  # Default

    def test_downscale_resource(self):
        """Test downscaling resource."""
        resource_id = "waldur-alloc-test123"

        result = self.backend.downscale_resource(resource_id)

        self.assertTrue(result)
        self.backend.client.set_resource_limits.assert_called_once()
        call_args = self.backend.client.set_resource_limits.call_args[0]
        self.assertEqual(call_args[0], resource_id)
        self.assertEqual(call_args[1]["pods"], 1)  # Minimal pods

    def test_pause_resource(self):
        """Test pausing resource."""
        resource_id = "waldur-alloc-test123"

        result = self.backend.pause_resource(resource_id)

        self.assertTrue(result)
        self.backend.client.set_resource_limits.assert_called_once()
        call_args = self.backend.client.set_resource_limits.call_args[0]
        self.assertEqual(call_args[0], resource_id)
        self.assertEqual(call_args[1]["pods"], 0)  # Zero pods

    def test_restore_resource(self):
        """Test restoring resource."""
        resource_id = "waldur-alloc-test123"

        result = self.backend.restore_resource(resource_id)

        self.assertTrue(result)
        self.backend.client.set_resource_limits.assert_called_once()
        call_args = self.backend.client.set_resource_limits.call_args[0]
        self.assertEqual(call_args[0], resource_id)
        self.assertGreater(call_args[1]["pods"], 0)  # Restored pods

    def test_create_user_association(self):
        """Test creating user association."""
        resource_id = "waldur-alloc-test123"
        user = MagicMock(spec=OfferingUser)
        user.username = "testuser"

        self.backend.client.get_association.return_value = None

        result = self.backend.create_user_association(resource_id, user)

        self.assertTrue(result)
        self.backend.client.create_association.assert_called_once_with(
            "testuser", resource_id, "edit"
        )

    def test_create_user_association_existing(self):
        """Test creating association for already associated user."""
        resource_id = "waldur-alloc-test123"
        user = MagicMock(spec=OfferingUser)
        user.username = "testuser"

        self.backend.client.get_association.return_value = MagicMock()  # Existing association

        result = self.backend.create_user_association(resource_id, user)

        self.assertTrue(result)
        self.backend.client.create_association.assert_not_called()

    def test_delete_user_association(self):
        """Test deleting user association."""
        resource_id = "waldur-alloc-test123"
        user = MagicMock(spec=OfferingUser)
        user.username = "testuser"

        self.backend.client.get_association.return_value = MagicMock()  # Existing association

        result = self.backend.delete_user_association(resource_id, user)

        self.assertTrue(result)
        self.backend.client.delete_association.assert_called_once_with("testuser", resource_id)

    def test_delete_user_association_not_existing(self):
        """Test deleting non-existing association."""
        resource_id = "waldur-alloc-test123"
        user = MagicMock(spec=OfferingUser)
        user.username = "testuser"

        self.backend.client.get_association.return_value = None  # No association

        result = self.backend.delete_user_association(resource_id, user)

        self.assertTrue(result)
        self.backend.client.delete_association.assert_not_called()

    def test_get_resource_metadata(self):
        """Test getting resource metadata."""
        resource_id = "waldur-alloc-test123"

        mock_resource = ClientResource(
            name="waldur-alloc-test123",
            organization="waldur-org-acme",
            description="Test Resource",
        )

        self.backend.client.get_resource.return_value = mock_resource
        self.backend.client.get_resource_limits.return_value = {"cpu": 4, "memory": 16}
        self.backend.client.list_resource_users.return_value = ["user1", "user2"]

        metadata = self.backend.get_resource_metadata(resource_id)

        self.assertEqual(metadata["name"], "waldur-alloc-test123")
        self.assertEqual(metadata["organization"], "waldur-org-acme")
        self.assertEqual(metadata["quotas"]["cpu"], 4)
        self.assertIn("user1", metadata["users"])


if __name__ == "__main__":
    unittest.main()
