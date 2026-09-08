"""Tests for agent telemetry registration being non-fatal.

Registering the agent identity, its service and its processors is telemetry:
it makes the agent visible in Waldur, but a polling agent does its work through
the marketplace REST API and never reads any of it back. A Waldur that refuses
the registration - an older Mastermind whose site-agent offering-type whitelist
does not cover the offering, say - must therefore not stop the offering from
being processed.
"""

import unittest
import uuid
from unittest import mock

from waldur_api_client import AuthenticatedClient
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import ObservableObjectTypeEnum

from waldur_site_agent.common import structures
from waldur_site_agent.common.agent_identity_management import (
    AgentIdentityDoesNotExistError,
    AgentIdentityManager,
    ensure_agent_telemetry,
)
from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing import utils as event_utils

OFFERING_UUID = "11111111-1111-1111-1111-111111111111"


def _rejected_offering_error():
    """The 400 Mastermind returns for an offering type it does not whitelist."""
    return UnexpectedStatus(
        400,
        b'{"offering":["Object with uuid=' + OFFERING_UUID.encode() + b' does not exist."]}',
        None,
    )


def _make_offering():
    return structures.Offering(
        name="test-offering",
        waldur_offering_uuid=OFFERING_UUID,
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="test-token",
        backend_type="slurm",
    )


def _make_client():
    return AuthenticatedClient(
        base_url="https://waldur.example.com",
        token="test-token",
        headers={},
    )


class TestEnsureAgentTelemetry(unittest.TestCase):
    """Tests for agent_identity_management.ensure_agent_telemetry."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering = _make_offering()
        self.client = _make_client()
        self.log_shipping = mock.Mock(spec=structures.LogShippingConfig)
        self.log_shipping.enabled = False

        self.identity = mock.Mock()
        self.identity.uuid = uuid.UUID(OFFERING_UUID)
        self.identity.name = f"agent-{OFFERING_UUID}"

    def _call(self):
        return ensure_agent_telemetry(
            self.offering, self.client, "membership-sync", self.log_shipping
        )

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "register_identity")
    def test_service_returned_when_registration_succeeds(self, mock_identity, mock_service):
        """The registered service is handed back to the caller."""
        mock_identity.return_value = self.identity
        mock_service.return_value = mock.Mock()

        self.assertIs(self._call(), mock_service.return_value)
        mock_identity.assert_called_once_with(f"agent-{OFFERING_UUID}")
        mock_service.assert_called_once_with(self.identity, "membership-sync", "membership-sync")

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "register_identity")
    def test_rejected_identity_returns_none(self, mock_identity, mock_service):
        """A Waldur that rejects the identity yields None instead of raising."""
        mock_identity.side_effect = _rejected_offering_error()

        self.assertIsNone(self._call())
        mock_service.assert_not_called()

    @mock.patch("waldur_site_agent.common.agent_identity_management.utils.ensure_log_shipper")
    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "register_identity")
    def test_failing_log_shipper_does_not_lose_the_service(
        self, mock_identity, mock_service, mock_log_shipper
    ):
        """A shipper that will not start costs the logs, not the whole cycle."""
        mock_identity.return_value = self.identity
        mock_service.return_value = mock.Mock()
        mock_log_shipper.side_effect = RuntimeError("can't start new thread")

        self.assertIs(self._call(), mock_service.return_value)

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "register_identity")
    def test_rejected_service_returns_none(self, mock_identity, mock_service):
        """A failure after the identity is registered is swallowed too."""
        mock_identity.return_value = self.identity
        mock_service.side_effect = _rejected_offering_error()

        self.assertIsNone(self._call())


class TestProcessorRegister(unittest.TestCase):
    """Tests for OfferingBaseProcessor.register."""

    def setUp(self) -> None:
        """Build a processor without running its network-bound constructor."""
        self.processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
        self.processor.offering = _make_offering()
        self.processor.waldur_rest_client = _make_client()
        self.processor.resource_backend = mock.Mock()
        self.processor.resource_backend_version = "1.0.0"

    @mock.patch.object(AgentIdentityManager, "register_processor")
    def test_no_service_skips_registration(self, mock_register_processor):
        """Without a service there is nothing to attach the processor to."""
        self.assertIsNone(self.processor.register(None))
        mock_register_processor.assert_not_called()

    @mock.patch.object(AgentIdentityManager, "register_processor")
    def test_rejected_processor_returns_none(self, mock_register_processor):
        """A rejected processor registration does not propagate."""
        mock_register_processor.side_effect = _rejected_offering_error()

        self.assertIsNone(self.processor.register(mock.Mock()))


class TestPollingAgentsSurviveRejectedIdentity(unittest.TestCase):
    """The polling agents keep processing offerings Waldur refuses to register."""

    def setUp(self) -> None:
        """Set up an offering that polls rather than listening over STOMP."""
        self.offering = mock.Mock()
        self.offering.name = "test-offering"
        self.offering.uuid = OFFERING_UUID
        self.offering.stomp_enabled = False
        self.offering.stomp_membership_sync_enabled = None

        self.config = mock.Mock(spec=structures.WaldurAgentConfiguration)
        self.config.waldur_offerings = [self.offering]
        self.config.waldur_user_agent = "test-agent"
        self.config.waldur_site_agent_mode = "membership-sync"
        self.config.timezone = "UTC"
        self.config.reporting_periods = 1
        self.config.global_proxy = None
        self.config.expose_backend_error_details = True
        self.config.log_shipping = mock.Mock(spec=structures.LogShippingConfig)
        self.config.log_shipping.enabled = False

    def _assert_offering_processed(self, module_path, utils_attr, processors_attr, processor_class):
        module = __import__(module_path, fromlist=["_process_offerings"])
        with (
            mock.patch.object(
                AgentIdentityManager,
                "register_identity",
                side_effect=_rejected_offering_error(),
            ),
            mock.patch.object(module, utils_attr) as mock_utils,
            mock.patch.object(module, processors_attr) as mock_processors,
        ):
            mock_utils.get_backend_for_offering.return_value = (mock.Mock(), "1.0.0")
            module._process_offerings(self.config)

        processor = getattr(mock_processors, processor_class).return_value
        processor.register.assert_called_once_with(None)
        processor.process_offering.assert_called_once()

    def test_membership_sync_continues(self):
        """Membership sync runs even though the identity was rejected."""
        self._assert_offering_processed(
            "waldur_site_agent.polling_processing.agent_membership_sync",
            "common_utils",
            "common_processors",
            "OfferingMembershipProcessor",
        )

    def test_order_process_continues(self):
        """Order processing runs even though the identity was rejected."""
        self._assert_offering_processed(
            "waldur_site_agent.polling_processing.agent_order_process",
            "utils",
            "processors",
            "OfferingOrderProcessor",
        )

    def test_report_continues(self):
        """Usage reporting runs even though the identity was rejected."""
        self._assert_offering_processed(
            "waldur_site_agent.polling_processing.agent_report",
            "utils",
            "common_processors",
            "OfferingReportProcessor",
        )


class TestEventPathSurvivesMissingIdentity(unittest.TestCase):
    """The event path treats the identity as telemetry too.

    A STOMP message is delivered with ``ack="auto"``, so a handler that raises
    neither requeues nor retries — an identity lookup must never be able to
    cost a real order or membership event.
    """

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.offering = _make_offering()
        self.client = _make_client()

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "get_identity")
    def test_missing_identity_yields_no_service(self, mock_get, mock_service):
        """A handler gets None rather than an exception when the identity is gone."""
        mock_get.side_effect = AgentIdentityDoesNotExistError(
            f"Unable to get the identity agent-{OFFERING_UUID}"
        )

        result = handlers.register_event_process_service(
            self.offering, self.client, ObservableObjectTypeEnum.ORDER
        )

        self.assertIsNone(result)
        mock_service.assert_not_called()

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "get_identity")
    def test_transient_lookup_failure_yields_no_service(self, mock_get, mock_service):
        """The everyday trigger is a transient API failure, not a deleted identity."""
        mock_get.side_effect = _rejected_offering_error()

        self.assertIsNone(
            handlers.register_event_process_service(
                self.offering, self.client, ObservableObjectTypeEnum.USER_ROLE
            )
        )
        mock_service.assert_not_called()

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "get_identity")
    def test_refused_service_yields_no_service(self, mock_get, mock_service):
        """A refused service registration does not escape either."""
        mock_get.return_value = mock.Mock()
        mock_service.side_effect = _rejected_offering_error()

        self.assertIsNone(
            handlers.register_event_process_service(
                self.offering, self.client, ObservableObjectTypeEnum.RESOURCE
            )
        )

    @mock.patch.object(AgentIdentityManager, "register_service")
    @mock.patch.object(AgentIdentityManager, "get_identity")
    def test_service_returned_when_lookup_succeeds(self, mock_get, mock_service):
        """The happy path is unchanged: name carries the observable object."""
        mock_get.return_value = mock.Mock()
        mock_service.return_value = mock.Mock()

        result = handlers.register_event_process_service(
            self.offering, self.client, ObservableObjectTypeEnum.ORDER
        )

        self.assertIs(result, mock_service.return_value)
        registered_name = mock_service.call_args[0][1]
        self.assertIn(str(ObservableObjectTypeEnum.ORDER), registered_name)

    @mock.patch("waldur_site_agent.event_processing.utils.common_processors")
    @mock.patch("waldur_site_agent.event_processing.utils.get_client_for_offering")
    @mock.patch.object(AgentIdentityManager, "register_identity")
    def test_initial_pass_runs_without_an_identity(
        self, mock_identity, mock_client, mock_processors
    ):
        """A refused identity costs telemetry, not the startup reconciliation."""
        mock_identity.side_effect = _rejected_offering_error()
        offering = mock.Mock()
        offering.name = "test-offering"
        offering.uuid = OFFERING_UUID
        offering.order_processing_backend = "slurm"
        offering.membership_sync_backend = "slurm"
        offering.stomp_membership_sync_enabled = True

        event_utils.process_offering(offering)

        for processor_class in (
            mock_processors.OfferingOrderProcessor,
            mock_processors.OfferingMembershipProcessor,
        ):
            processor_class.return_value.register.assert_called_once_with(None)
            processor_class.return_value.process_offering.assert_called_once()
