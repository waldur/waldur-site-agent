"""Tests for STOMP subscription setup functionality."""

import json
import unittest
import uuid
from unittest import mock

import httpx
from waldur_api_client import AuthenticatedClient
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import AgentIdentity, EventSubscription, ObservableObjectTypeEnum

from waldur_site_agent.common import structures as common_structures
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
            ObservableObjectTypeEnum.USER_ROLE,
            ObservableObjectTypeEnum.RESOURCE,
            ObservableObjectTypeEnum.SERVICE_ACCOUNT,
            ObservableObjectTypeEnum.COURSE_ACCOUNT,
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

        self.assertEqual(result, [ObservableObjectTypeEnum.ORDER])

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


class TestSetupSingleSubscription(unittest.TestCase):
    """Tests for _setup_single_stomp_subscription function."""

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
        self.waldur_rest_client = AuthenticatedClient(
            base_url="https://waldur.example.com",
            token="test_token",
            headers={},
        )
        self.mock_identity = mock.Mock(spec=AgentIdentity)
        self.mock_identity.uuid = uuid.uuid4()
        self.mock_identity.name = f"agent-{self.offering.waldur_offering_uuid}"
        self.mock_identity.user_uuid = uuid.uuid4()

        self.mock_event_subscription = mock.Mock(spec=EventSubscription)
        self.mock_event_subscription.uuid = uuid.uuid4()
        self.mock_event_subscription.user_uuid = uuid.uuid4()

        self.mock_identity_manager = mock.Mock()
        self.object_type = ObservableObjectTypeEnum.ORDER

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_successful_subscription_setup(self, mock_esm_class):
        """Test successful STOMP subscription setup."""
        # Setup mocks
        self.mock_identity_manager.register_event_subscription.return_value = (
            self.mock_event_subscription
        )
        self.mock_identity_manager.create_event_subscription_queue.return_value = mock.Mock()

        mock_connection = mock.Mock()
        mock_esm = mock_esm_class.return_value
        mock_esm.setup_stomp_connection.return_value = mock_connection
        mock_esm.start_stomp_connection.return_value = True

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify
        self.assertIsNotNone(result)
        self.assertEqual(result[0], mock_connection)
        self.assertEqual(result[1], self.mock_event_subscription)
        self.assertEqual(result[2], self.offering)

    def test_event_subscription_registration_fails(self):
        """Test returns None when event subscription registration fails."""
        # Setup mock to raise exception
        self.mock_identity_manager.register_event_subscription.side_effect = UnexpectedStatus(
            500, b"API Error", "https://test.com/api/"
        )

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify
        self.assertIsNone(result)

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_queue_creation_fails(self, mock_esm_class):
        """Test returns None when queue creation returns None."""
        # Setup mocks
        self.mock_identity_manager.register_event_subscription.return_value = (
            self.mock_event_subscription
        )
        self.mock_identity_manager.create_event_subscription_queue.return_value = None

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify
        self.assertIsNone(result)
        mock_esm_class.assert_not_called()

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_connection_setup_fails(self, mock_esm_class):
        """Test returns None when connection setup fails."""
        # Setup mocks
        self.mock_identity_manager.register_event_subscription.return_value = (
            self.mock_event_subscription
        )
        self.mock_identity_manager.create_event_subscription_queue.return_value = mock.Mock()

        mock_esm = mock_esm_class.return_value
        mock_esm.setup_stomp_connection.return_value = None
        mock_esm.start_stomp_connection.return_value = False

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify - even if connection setup returns None, we still try to start it
        # The function should return None because start returns False
        self.assertIsNone(result)

    @mock.patch("waldur_site_agent.event_processing.utils.EventSubscriptionManager")
    def test_connection_start_fails(self, mock_esm_class):
        """Test returns None when connection start returns False."""
        # Setup mocks
        self.mock_identity_manager.register_event_subscription.return_value = (
            self.mock_event_subscription
        )
        self.mock_identity_manager.create_event_subscription_queue.return_value = mock.Mock()

        mock_connection = mock.Mock()
        mock_esm = mock_esm_class.return_value
        mock_esm.setup_stomp_connection.return_value = mock_connection
        mock_esm.start_stomp_connection.return_value = False

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify
        self.assertIsNone(result)

    def test_timeout_during_registration(self):
        """Test returns None when timeout occurs during registration."""
        # Setup mock to raise timeout
        self.mock_identity_manager.register_event_subscription.side_effect = httpx.TimeoutException(
            "Timeout"
        )

        # Call function
        result = utils._setup_single_stomp_subscription(
            self.offering,
            self.mock_identity,
            self.mock_identity_manager,
            "test-agent",
            self.object_type,
        )

        # Verify
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

    @mock.patch("waldur_site_agent.event_processing.utils._setup_single_stomp_subscription")
    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
    def test_successful_setup_all_features(
        self,
        mock_get_client,
        mock_determine_types,
        mock_register_identity,
        mock_setup_single,
    ):
        """Test successful setup when all features are enabled."""
        # Setup mocks
        mock_rest_client = mock.Mock()
        mock_get_client.return_value = mock_rest_client

        mock_determine_types.return_value = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
        ]

        mock_identity = mock.Mock()
        mock_identity_manager = mock.Mock()
        mock_register_identity.return_value = (mock_identity, mock_identity_manager)

        mock_consumer1 = (mock.Mock(), mock.Mock(), self.offering)
        mock_consumer2 = (mock.Mock(), mock.Mock(), self.offering)
        mock_setup_single.side_effect = [mock_consumer1, mock_consumer2]

        # Call function
        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Verify
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], mock_consumer1)
        self.assertEqual(result[1], mock_consumer2)
        mock_setup_single.assert_called()
        self.assertEqual(mock_setup_single.call_count, 2)

    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
    def test_identity_registration_fails_returns_empty(
        self, mock_get_client, mock_determine_types, mock_register_identity
    ):
        """Test returns empty list when identity registration fails."""
        # Setup mocks
        mock_rest_client = mock.Mock()
        mock_get_client.return_value = mock_rest_client

        mock_determine_types.return_value = [ObservableObjectTypeEnum.ORDER]
        mock_register_identity.return_value = None  # Registration fails

        # Call function
        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Verify
        self.assertEqual(result, [])

    @mock.patch("waldur_site_agent.event_processing.utils._setup_single_stomp_subscription")
    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
    def test_partial_subscription_failures(
        self,
        mock_get_client,
        mock_determine_types,
        mock_register_identity,
        mock_setup_single,
    ):
        """Test returns successful subscriptions when some fail."""
        # Setup mocks
        mock_rest_client = mock.Mock()
        mock_get_client.return_value = mock_rest_client

        mock_determine_types.return_value = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
            ObservableObjectTypeEnum.RESOURCE,
        ]

        mock_identity = mock.Mock()
        mock_identity_manager = mock.Mock()
        mock_register_identity.return_value = (mock_identity, mock_identity_manager)

        mock_consumer1 = (mock.Mock(), mock.Mock(), self.offering)
        # First succeeds, second fails, third succeeds
        mock_setup_single.side_effect = [mock_consumer1, None, mock_consumer1]

        # Call function
        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Verify - only 2 successful subscriptions returned
        self.assertEqual(len(result), 2)
        self.assertEqual(mock_setup_single.call_count, 3)

    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
    def test_no_object_types_returns_empty(
        self, mock_get_client, mock_determine_types, mock_register_identity
    ):
        """Test returns empty list when no features are enabled."""
        # Setup mocks
        mock_rest_client = mock.Mock()
        mock_get_client.return_value = mock_rest_client
        mock_determine_types.return_value = []  # No object types
        mock_register_identity.return_value = (mock.Mock(), mock.Mock())  # Identity succeeds but no types

        # Call function
        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Verify
        self.assertEqual(result, [])

    @mock.patch("waldur_site_agent.event_processing.utils._setup_single_stomp_subscription")
    @mock.patch("waldur_site_agent.event_processing.utils._register_agent_identity")
    @mock.patch("waldur_site_agent.event_processing.utils._determine_observable_object_types")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client")
    def test_all_subscriptions_fail(
        self,
        mock_get_client,
        mock_determine_types,
        mock_register_identity,
        mock_setup_single,
    ):
        """Test returns empty list when all individual setups fail."""
        # Setup mocks
        mock_rest_client = mock.Mock()
        mock_get_client.return_value = mock_rest_client

        mock_determine_types.return_value = [
            ObservableObjectTypeEnum.ORDER,
            ObservableObjectTypeEnum.USER_ROLE,
        ]

        mock_identity = mock.Mock()
        mock_identity_manager = mock.Mock()
        mock_register_identity.return_value = (mock_identity, mock_identity_manager)

        # All setups fail
        mock_setup_single.return_value = None

        # Call function
        result = utils.setup_stomp_offering_subscriptions(self.offering, "test-agent")

        # Verify
        self.assertEqual(result, [])
        self.assertEqual(mock_setup_single.call_count, 2)
