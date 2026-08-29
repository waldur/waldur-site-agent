"""Tests for STOMP subscription setup functionality."""

import json
import unittest
import uuid
from unittest import mock

import httpx
from waldur_api_client import AuthenticatedClient
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import AgentIdentity, ObservableObjectTypeEnum

from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.structures import UnifiedQueue
from waldur_site_agent.event_processing import utils


class TestDetermineObjectTypes(unittest.TestCase):
    """Tests for _determine_observable_object_types function."""

    def test_all_features_enabled(self):
        """Test returns all object types when all features are enabled."""
        offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="slurm",
            order_processing_backend="slurm",
            membership_sync_backend="slurm",
            resource_import_enabled=True,
            backend_settings={"periodic_limits": {"enabled": True}},
        )

        result = utils._determine_observable_object_types(offering)

        expected = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.RESOURCE_API_KEY_ROTATION,
            ObservableObjectTypeEnum.USER_ROLE,
            ObservableObjectTypeEnum.RESOURCE,
            ObservableObjectTypeEnum.SERVICE_ACCOUNT,
            ObservableObjectTypeEnum.COURSE_ACCOUNT,
            ObservableObjectTypeEnum.OFFERING_USER,
            ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC,
            ObservableObjectTypeEnum.IMPORTABLE_RESOURCES,
            ObservableObjectTypeEnum.RESOURCE_PERIODIC_LIMITS,
        ]
        self.assertEqual(result, expected)

    def test_only_order_processing(self):
        """Test returns only ORDER when only order processing is enabled."""
        offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="slurm",
            order_processing_backend="slurm",
        )

        result = utils._determine_observable_object_types(offering)

        self.assertEqual(
            result,
            [
                ObservableObjectTypeEnum.ORDER,
                ObservableObjectTypeEnum.RESOURCE_API_KEY_ROTATION,
            ],
        )

    def test_only_membership_sync(self):
        """Test returns membership types when only membership sync is enabled."""
        offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="slurm",
            membership_sync_backend="slurm",
        )

        result = utils._determine_observable_object_types(offering)

        expected = [
            ObservableObjectTypeEnum.USER_ROLE,
            ObservableObjectTypeEnum.RESOURCE,
            ObservableObjectTypeEnum.SERVICE_ACCOUNT,
            ObservableObjectTypeEnum.COURSE_ACCOUNT,
            ObservableObjectTypeEnum.OFFERING_USER,
            ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC,
        ]
        self.assertEqual(result, expected)

    def test_periodic_limits_enabled(self):
        """Test includes RESOURCE_PERIODIC_LIMITS when periodic limits enabled."""
        offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="slurm",
            order_processing_backend="slurm",
            backend_settings={"periodic_limits": {"enabled": True}},
        )

        result = utils._determine_observable_object_types(offering)

        self.assertIn(ObservableObjectTypeEnum.RESOURCE_PERIODIC_LIMITS, result)
        self.assertIn(ObservableObjectTypeEnum.ORDER, result)

    def test_no_features_enabled(self):
        """Test returns empty list when no features are enabled."""
        offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="slurm",
        )

        result = utils._determine_observable_object_types(offering)

        self.assertEqual(result, [])


class TestRegisterIdentity(unittest.TestCase):
    """Tests for _register_agent_identity function."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-offering-uuid",
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
            order_processing_backend="slurm",
        )
        self.waldur_rest_client = AuthenticatedClient(
            base_url="https://waldur.example.com",
            token="test_token",
            headers={},
        )
        self.mock_identity = mock.Mock(spec=AgentIdentity)
        self.mock_identity.uuid = uuid.uuid4()
        self.mock_identity.name = f"agent-{self.offering.waldur_offering_uuid}"

    @mock.patch(
        "waldur_site_agent.event_processing.utils.agent_identity_management.AgentIdentityManager"
    )
    def test_successful_registration(self, mock_manager_class):
        """Test successful agent identity registration."""
        # Setup mock
        mock_manager = mock_manager_class.return_value
        mock_manager.register_identity.return_value = self.mock_identity

        # Call function
        result = utils._register_agent_identity(self.offering, self.waldur_rest_client)

        # Verify
        self.assertIsNotNone(result)
        identity, manager = result
        self.assertEqual(identity, self.mock_identity)
        self.assertEqual(manager, mock_manager)
        mock_manager_class.assert_called_once_with(self.offering, self.waldur_rest_client)
        mock_manager.register_identity.assert_called_once_with(
            f"agent-{self.offering.waldur_offering_uuid}"
        )

    @mock.patch(
        "waldur_site_agent.event_processing.utils.agent_identity_management.AgentIdentityManager"
    )
    def test_api_error_returns_none(self, mock_manager_class):
        """Test returns None when API call fails with UnexpectedStatus."""
        # Setup mock to raise UnexpectedStatus
        mock_manager = mock_manager_class.return_value
        mock_manager.register_identity.side_effect = UnexpectedStatus(
            500, b"API Error", "https://test.com/api/"
        )

        # Call function
        result = utils._register_agent_identity(self.offering, self.waldur_rest_client)

        # Verify
        self.assertIsNone(result)
        mock_manager.register_identity.assert_called_once()

    @mock.patch(
        "waldur_site_agent.event_processing.utils.agent_identity_management.AgentIdentityManager"
    )
    def test_timeout_returns_none(self, mock_manager_class):
        """Test returns None when API call times out."""
        # Setup mock to raise TimeoutException
        mock_manager = mock_manager_class.return_value
        mock_manager.register_identity.side_effect = httpx.TimeoutException("Timeout")

        # Call function
        result = utils._register_agent_identity(self.offering, self.waldur_rest_client)

        # Verify
        self.assertIsNone(result)
        mock_manager.register_identity.assert_called_once()


class TestSetupUnifiedConnection(unittest.TestCase):
    """Tests for _setup_unified_stomp_connection (one register_queue + one connection)."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-offering-uuid",
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
            order_processing_backend="slurm",
            stomp_ws_host="stomp.example.com",
            stomp_ws_port=443,
            stomp_ws_path="/rmqws-stomp",
        )
        self.mock_identity = mock.Mock(spec=AgentIdentity)
        self.mock_identity.uuid = uuid.uuid4()
        self.mock_identity.name = f"agent-{self.offering.waldur_offering_uuid}"
        self.mock_identity.user_uuid = uuid.uuid4()

        self.unified_queue = UnifiedQueue(
            queue_name=f"consumer_{uuid.uuid4().hex}",
            rmq_username=uuid.uuid4().hex,
            vhost=uuid.uuid4().hex,
            observable_object_types=["order", "user_role"],
            agent_identity_uuid=self.mock_identity.uuid.hex,
        )
        self.mock_identity_manager = mock.Mock()
        self.object_types = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
        ]

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_successful_unified_setup(self, mock_esm_class):
        """One register_queue + one connection -> (connection, unified_queue, offering)."""
        self.mock_identity_manager.register_queue.return_value = self.unified_queue
        mock_connection = mock.Mock()
        mock_esm = mock_esm_class.return_value
        mock_esm.setup_stomp_connection.return_value = mock_connection
        mock_esm.start_stomp_connection.return_value = True

        result = utils._setup_unified_stomp_connection(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_types,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result[0], mock_connection)
        self.assertEqual(result[1], self.unified_queue)
        self.assertEqual(result[2], self.offering)
        # register_queue called ONCE with the full type list (not per-type).
        self.mock_identity_manager.register_queue.assert_called_once_with(
            self.mock_identity, self.object_types
        )

    def test_register_queue_fails(self):
        """Returns None when register_queue raises."""
        self.mock_identity_manager.register_queue.side_effect = UnexpectedStatus(
            500, b"API Error", "https://test.com/api/"
        )
        result = utils._setup_unified_stomp_connection(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_types,
        )
        self.assertIsNone(result)

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_connection_start_fails(self, mock_esm_class):
        """Returns None when the STOMP connection fails to start."""
        self.mock_identity_manager.register_queue.return_value = self.unified_queue
        mock_esm = mock_esm_class.return_value
        mock_esm.setup_stomp_connection.return_value = mock.Mock()
        mock_esm.start_stomp_connection.return_value = False

        result = utils._setup_unified_stomp_connection(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_types,
        )
        self.assertIsNone(result)

    def test_timeout_during_registration(self):
        """Returns None when register_queue times out."""
        self.mock_identity_manager.register_queue.side_effect = httpx.TimeoutException("Timeout")
        result = utils._setup_unified_stomp_connection(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_types,
        )
        self.assertIsNone(result)


class TestSetupStompSubscriptionsIntegration(unittest.TestCase):
    """Integration tests for setup_stomp_offering_subscriptions function."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering = common_structures.Offering(
            name="test-offering",
            waldur_offering_uuid="test-offering-uuid",
            waldur_api_url="https://waldur.example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
            order_processing_backend="slurm",
            membership_sync_backend="slurm",
            stomp_ws_host="stomp.example.com",
            stomp_ws_port=443,
            stomp_ws_path="/rmqws-stomp",
        )

    @mock.patch("waldur_site_agent.event_processing.utils._setup_unified_stomp_connection")
    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_successful_setup_one_connection_for_all_types(
        self,
        mock_get_client,
        mock_determine_types,
        mock_register_identity,
        mock_setup_unified,
    ):
        """Unified: ONE connection regardless of how many object types are enabled."""
        mock_get_client.return_value = mock.Mock()
        mock_determine_types.return_value = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
            ObservableObjectTypeEnum.RESOURCE,
        ]
        mock_identity = mock.Mock()
        mock_identity_manager = mock.Mock()
        mock_register_identity.return_value = (mock_identity, mock_identity_manager)

        mock_consumer = (mock.Mock(), mock.Mock(), self.offering)
        mock_setup_unified.return_value = mock_consumer

        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Exactly one consumer, from exactly one _setup_unified_stomp_connection call,
        # which received the FULL type list.
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], mock_consumer)
        self.assertEqual(mock_setup_unified.call_count, 1)
        called_types = mock_setup_unified.call_args.args[4]
        self.assertEqual(len(called_types), 3)

    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_identity_registration_fails_returns_empty(
        self, mock_get_client, mock_determine_types, mock_register_identity
    ):
        """Test returns empty list when identity registration fails."""
        mock_get_client.return_value = mock.Mock()
        mock_determine_types.return_value = [ObservableObjectTypeEnum.ORDER]
        mock_register_identity.return_value = None  # Registration fails

        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")
        self.assertEqual(result, [])

    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_no_object_types_returns_empty(self, mock_get_client, mock_determine_types):
        """No enabled features -> no queue registered, empty result."""
        mock_get_client.return_value = mock.Mock()
        mock_determine_types.return_value = []  # No object types

        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")
        self.assertEqual(result, [])

    @mock.patch("waldur_site_agent.event_processing.utils._setup_unified_stomp_connection")
    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    def test_connection_setup_fails_returns_empty(
        self,
        mock_get_client,
        mock_determine_types,
        mock_register_identity,
        mock_setup_unified,
    ):
        """Test returns empty list when the unified connection setup fails."""
        mock_get_client.return_value = mock.Mock()
        mock_determine_types.return_value = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
        ]
        mock_register_identity.return_value = (mock.Mock(), mock.Mock())
        mock_setup_unified.return_value = None  # setup fails

        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")
        self.assertEqual(result, [])
        self.assertEqual(mock_setup_unified.call_count, 1)
