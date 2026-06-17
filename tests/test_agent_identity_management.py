"""Tests for agent identity registration."""

import unittest
import uuid
from unittest import mock

from waldur_api_client import AuthenticatedClient
from waldur_api_client.models import AgentIdentityRequest

from waldur_site_agent.common import WALDUR_SITE_AGENT_VERSION, structures
from waldur_site_agent.common.agent_identity_management import AgentIdentityManager


class TestRegisterIdentity(unittest.TestCase):
    """Tests for AgentIdentityManager.register_identity."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering_uuid = "11111111-1111-1111-1111-111111111111"
        self.offering = structures.Offering(
            name="test-offering",
            waldur_offering_uuid=self.offering_uuid,
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="test-token",
            backend_type="slurm",
        )
        self.client = AuthenticatedClient(
            base_url="https://waldur.example.com",
            token="test-token",
            headers={},
        )
        self.identity_name = f"agent-{self.offering_uuid}"
        self.mock_identity = mock.Mock()
        self.mock_identity.uuid = uuid.UUID(self.offering_uuid)
        self.mock_identity.name = self.identity_name

    @mock.patch(
        "waldur_site_agent.common.agent_identity_management.marketplace_site_agent_identities_create"
    )
    @mock.patch(
        "waldur_site_agent.common.agent_identity_management.marketplace_site_agent_identities_list"
    )
    def test_register_identity_sends_create_request(self, mock_list, mock_create):
        """New identity is created with a serializable AgentIdentityRequest body."""
        mock_list.sync.return_value = []
        mock_create.sync.return_value = self.mock_identity

        manager = AgentIdentityManager(self.offering, self.client)
        result = manager.register_identity(self.identity_name)

        self.assertEqual(result, self.mock_identity)
        mock_list.sync.assert_called_once_with(client=self.client, name=self.identity_name)
        mock_create.sync.assert_called_once()

        body = mock_create.sync.call_args.kwargs["body"]
        self.assertIsInstance(body, AgentIdentityRequest)
        self.assertEqual(body.name, self.identity_name)
        self.assertEqual(str(body.offering), self.offering_uuid)
        self.assertEqual(body.version, WALDUR_SITE_AGENT_VERSION)
        self.assertTrue(body.dependencies)

        payload = body.to_dict()
        self.assertIn("dependencies", payload)
        self.assertTrue(
            all(set(dependency) == {"package", "version"} for dependency in payload["dependencies"])
        )
