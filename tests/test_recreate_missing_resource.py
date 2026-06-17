"""Tests for BaseBackend.recreate_missing_resource."""

import unittest
from unittest import mock

from waldur_site_agent.backend.backends import BaseBackend


class TestRecreateMissingResource(unittest.TestCase):
    """Recreation of backend resources that are missing on the backend."""

    def setUp(self):
        self.backend = mock.Mock(spec=BaseBackend)
        self.backend.client = mock.Mock()
        self.waldur_resource = mock.Mock()
        self.waldur_resource.backend_id = "test-account"

    def _call(self):
        return BaseBackend.recreate_missing_resource(self.backend, self.waldur_resource)

    def test_existing_resource_is_not_recreated(self):
        self.backend.client.get_resource.return_value = {"name": "test-account"}

        self.assertFalse(self._call())
        self.backend.create_resource_with_id.assert_not_called()

    def test_missing_resource_is_recreated_with_original_backend_id(self):
        self.backend.client.get_resource.return_value = None

        self.assertTrue(self._call())
        self.backend.create_resource_with_id.assert_called_once_with(
            self.waldur_resource, "test-account"
        )

    def test_creation_error_propagates(self):
        self.backend.client.get_resource.return_value = None
        self.backend.create_resource_with_id.side_effect = Exception("backend down")

        with self.assertRaises(Exception):
            self._call()
