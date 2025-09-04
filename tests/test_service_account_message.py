from datetime import datetime
from unittest import mock, TestCase
import json

from waldur_api_client.models import (
    ProjectServiceAccount,
    Resource,
    ServiceProvider,
    ServiceAccountState,
)
import paho.mqtt.client as mqtt
import stomp.utils

from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.event_processing import handlers
import respx
from waldur_site_agent.common import structures
import uuid
from waldur_site_agent.backend import backends


class ServiceAccountMessageTest(TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.offering_uuid = uuid.uuid4().hex
        self.offering = structures.Offering(
            name="test-offering",
            uuid=self.offering_uuid,
            api_url=f"{self.BASE_URL}/api",
            api_token="test_token",
        )
        self.waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="test-alloc-01",
            backend_id="test-allocation-01",
            project_uuid=uuid.uuid4(),
            project_name="Test project",
        )
        self.service_provider = ServiceProvider(uuid=uuid.uuid4())
        self.service_account = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="svc-test-account",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            customer_abbreviation="",
        )

    def _setup_common_mocks(self):
        respx.get(f"{self.BASE_URL}/api/users/me/").respond(
            200,
            json={
                "username": "test",
                "email": "test@example.com",
                "full_name": "Test User",
                "is_staff": False,
            },
        )
        customer_uuid = uuid.uuid4().hex
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering_uuid}/"
        ).respond(200, json={"customer_uuid": customer_uuid, "components": []})
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/?customer_uuid={customer_uuid}"
        ).respond(200, json=[self.service_provider.to_dict()])
        respx.get(url__regex=r".*/api/marketplace-provider-resources/.*").respond(
            200, json=[self.waldur_resource.to_dict()]
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/project_service_accounts/?username={self.service_account.username}"
        ).respond(200, json=[self.service_account.to_dict()])

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_service_account_creation_processing(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_service_account_creation(self.service_account.username)

        mock_backend.add_users_to_resource.assert_called_once()

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_service_account_removal_processing(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering)
        processor.process_service_account_removal(
            self.service_account.username, self.waldur_resource.project_uuid.hex
        )

        mock_backend.remove_users_from_resource.assert_called_once_with(
            self.waldur_resource.backend_id, {self.service_account.username}
        )

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_mqtt_handler_create_action(self, mock_processor_class):
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "service_account_uuid": self.service_account.uuid.hex,
            "service_account_username": self.service_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "create",
        }

        mock_client = mock.Mock(spec=mqtt.Client)
        mock_msg = mock.Mock(spec=mqtt.MQTTMessage)
        mock_msg.payload.decode.return_value = json.dumps(message)
        mock_msg.topic = "test/topic"

        userdata = {
            "offering": self.offering,
            "user_agent": "test-agent",
        }

        with mock.patch("json.loads", return_value=message):
            handlers.on_service_account_message_mqtt(mock_client, userdata, mock_msg)

        mock_processor.process_service_account_creation.assert_called_once_with(
            self.service_account.username
        )

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_mqtt_handler_remove_action(self, mock_processor_class):
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "service_account_uuid": self.service_account.uuid.hex,
            "service_account_username": self.service_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "delete",
        }

        mock_client = mock.Mock(spec=mqtt.Client)
        mock_msg = mock.Mock(spec=mqtt.MQTTMessage)
        mock_msg.payload.decode.return_value = json.dumps(message)
        mock_msg.topic = "test/topic"

        userdata = {
            "offering": self.offering,
            "user_agent": "test-agent",
        }

        with mock.patch("json.loads", return_value=message):
            handlers.on_service_account_message_mqtt(mock_client, userdata, mock_msg)

        mock_processor.process_service_account_removal.assert_called_once_with(
            self.service_account.username, self.waldur_resource.project_uuid.hex
        )

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_stomp_handler_create_action(self, mock_processor_class):
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "service_account_uuid": self.service_account.uuid.hex,
            "service_account_username": self.service_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "create",
        }

        mock_frame = mock.Mock(spec=stomp.utils.Frame)
        mock_frame.body = json.dumps(message)

        with mock.patch("json.loads", return_value=message):
            handlers.on_service_account_message_stomp(mock_frame, self.offering, "test-agent")

        mock_processor.process_service_account_creation.assert_called_once_with(
            self.service_account.username
        )

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_stomp_handler_remove_action(self, mock_processor_class):
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "service_account_uuid": self.service_account.uuid.hex,
            "service_account_username": self.service_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "delete",
        }

        mock_frame = mock.Mock(spec=stomp.utils.Frame)
        mock_frame.body = json.dumps(message)

        with mock.patch("json.loads", return_value=message):
            handlers.on_service_account_message_stomp(mock_frame, self.offering, "test-agent")

        mock_processor.process_service_account_removal.assert_called_once_with(
            self.service_account.username, self.waldur_resource.project_uuid.hex
        )

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_service_accounts_with_active_and_closed_accounts(
        self, mock_get_backend_for_offering
    ):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        active_account = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="svc-active-account",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            customer_abbreviation="",
        )

        closed_account = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="svc-closed-account",
            state=ServiceAccountState.CLOSED,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            customer_abbreviation="",
        )

        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/project_service_accounts/?project_uuid={self.waldur_resource.project_uuid.hex}"
        ).respond(200, json=[active_account.to_dict(), closed_account.to_dict()])

        processor = OfferingMembershipProcessor(self.offering)
        processor._sync_resource_service_accounts(self.waldur_resource)

        mock_backend.add_users_to_resource.assert_called_once_with(
            self.waldur_resource.backend_id, {"svc-active-account"}
        )
        mock_backend.remove_users_from_resource.assert_called_once_with(
            self.waldur_resource.backend_id, {"svc-closed-account"}
        )

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_service_accounts_no_service_provider(
        self, mock_get_backend_for_offering
    ):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering)
        processor.service_provider = None
        processor._sync_resource_service_accounts(self.waldur_resource)

        mock_backend.add_users_to_resource.assert_not_called()
        mock_backend.remove_users_from_resource.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_service_accounts_empty_usernames(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = mock_backend

        self._setup_common_mocks()

        account_without_username = ProjectServiceAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            token=None,
            expires_at=None,
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            username="",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            customer_abbreviation="",
        )

        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/project_service_accounts/?project_uuid={self.waldur_resource.project_uuid.hex}"
        ).respond(200, json=[account_without_username.to_dict()])

        processor = OfferingMembershipProcessor(self.offering)
        processor._sync_resource_service_accounts(self.waldur_resource)

        mock_backend.add_users_to_resource.assert_called_once_with(
            self.waldur_resource.backend_id, set()
        )
        mock_backend.remove_users_from_resource.assert_called_once_with(
            self.waldur_resource.backend_id, set()
        )
