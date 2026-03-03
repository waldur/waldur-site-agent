import unittest
from unittest import mock

from waldur_site_agent.common import structures, utils
from waldur_site_agent.backend.backends import UnknownUsernameManagementBackend


class TestGetUsernameManagementBackend(unittest.TestCase):
    def setUp(self):
        self.offering = structures.Offering(
            name="Test offering",
            waldur_offering_uuid="abc123",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
        )

    def test_none_backend_returns_unknown(self):
        self.offering.username_management_backend = None
        backend, version = utils.get_username_management_backend(self.offering)
        self.assertIsInstance(backend, UnknownUsernameManagementBackend)
        self.assertEqual(version, "unknown")

    def test_uninstalled_backend_falls_back_to_unknown(self):
        """When a backend name is configured but the corresponding plugin
        package is not installed, should fall back gracefully."""
        self.offering.username_management_backend = "nonexistent_backend"
        with mock.patch.dict(utils.USERNAME_BACKENDS, {}, clear=True):
            backend, version = utils.get_username_management_backend(self.offering)
        self.assertIsInstance(backend, UnknownUsernameManagementBackend)
        self.assertEqual(version, "unknown")

    def test_default_base_backend_falls_back_when_plugin_not_installed(self):
        """The default value for username_management_backend is 'base'.
        If the basic_username_management plugin is not installed,
        should fall back gracefully instead of raising KeyError."""
        self.assertEqual(self.offering.username_management_backend, "base")
        with mock.patch.dict(utils.USERNAME_BACKENDS, {}, clear=True):
            backend, version = utils.get_username_management_backend(self.offering)
        self.assertIsInstance(backend, UnknownUsernameManagementBackend)
        self.assertEqual(version, "unknown")
