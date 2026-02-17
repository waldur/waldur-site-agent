from datetime import datetime
from unittest import mock, TestCase
import json

from stomp.constants import HDR_DESTINATION
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models import (
    CourseAccount,
    Resource,
    ServiceProvider,
    ServiceAccountState,
)
import stomp.utils

from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.structures import AccountType
from waldur_site_agent.event_processing import handlers
import respx
from waldur_site_agent.common import structures
import uuid
from waldur_site_agent.backend import backends


class CourseAccountMessageTest(TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.offering_uuid = uuid.uuid4().hex
        self.offering = structures.Offering(
            name="test-offering",
            waldur_offering_uuid=self.offering_uuid,
            waldur_api_url=f"{self.BASE_URL}/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
        )
        self.waldur_rest_client = AuthenticatedClient(
            base_url=self.BASE_URL,
            token="test_token",
            timeout=600,
            headers={},
        )
        self.waldur_resource = Resource(
            uuid=uuid.uuid4(),
            name="test-alloc-01",
            backend_id="test-allocation-01",
            project_uuid=uuid.uuid4(),
            project_name="Test project",
            project_slug="test-project",
        )
        self.service_provider = ServiceProvider(uuid=uuid.uuid4())
        self.course_account = CourseAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            user_uuid=uuid.uuid4(),
            username="course-test-account",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            error_traceback="",
            project_slug=self.waldur_resource.project_slug,
            project_start_date=datetime.now(),
            project_end_date=datetime.now(),
        )

    def tearDown(self) -> None:
        respx.stop()

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
            f"{self.BASE_URL}/api/marketplace-service-providers/",
            params={"customer_uuid": customer_uuid},
        ).respond(200, json=[self.service_provider.to_dict()])
        respx.get(url__regex=r".*/api/marketplace-provider-resources/.*").respond(
            200, json=[self.waldur_resource.to_dict()]
        )
        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/course_accounts/",
            params={"username": self.course_account.username, "page_size": 100},
        ).respond(200, json=[self.course_account.to_dict()])

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_course_account_creation_processing(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = (mock_backend, "1.0.0")

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering, self.waldur_rest_client)
        processor.process_account_creation(self.course_account.username, AccountType.COURSE_ACCOUNT)

        mock_backend.add_users_to_resource.assert_called_once()

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_course_account_removal_processing(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = (mock_backend, "1.0.0")

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering, self.waldur_rest_client)
        processor.process_account_removal(
            self.course_account.username, self.waldur_resource.project_uuid.hex
        )

        # Check that remove_users_from_resource was called with the resource and username
        mock_backend.remove_users_from_resource.assert_called_once()
        args, kwargs = mock_backend.remove_users_from_resource.call_args
        assert len(args) == 2
        # Check that the resource has the expected backend_id
        assert args[0].backend_id == self.waldur_resource.backend_id
        # Check that the username set is correct
        assert args[1] == {self.course_account.username}

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.agent_identity_management.marketplace_site_agent_identities_register_service"
    )
    @mock.patch(
        "waldur_site_agent.event_processing.handlers.agent_identity_management.marketplace_site_agent_identities_list"
    )
    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_stomp_handler_create_action(
        self,
        mock_processor_class,
        mock_list_identities,
        mock_register_service,
    ):
        # Setup mocks for agent identity registration
        mock_agent_identity = mock.Mock()
        mock_agent_identity.uuid = uuid.uuid4()
        mock_agent_identity.name = f"agent-{self.offering_uuid}"
        mock_list_identities.sync.return_value = [mock_agent_identity]

        mock_agent_service = mock.Mock()
        mock_agent_service.uuid = uuid.uuid4()
        mock_agent_service.name = "event_process"
        mock_register_service.sync.return_value = mock_agent_service

        # Setup processor mock
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "account_uuid": self.course_account.uuid.hex,
            "account_username": self.course_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "create",
        }

        test_frame = stomp.utils.Frame(
            cmd="MESSAGE",
            headers={HDR_DESTINATION: "/queue/abc_course_account"},
            body=json.dumps(message),
        )

        handlers.on_account_message_stomp(test_frame, self.offering, "test-agent")

        # Verify agent identity was checked/created
        mock_list_identities.sync.assert_called_once()

        # Verify agent service was registered
        mock_register_service.sync.assert_called_once()

        # Verify processor was called
        mock_processor.register.assert_called_once_with(mock_agent_service)
        mock_processor.process_account_creation.assert_called_once_with(
            self.course_account.username, AccountType.COURSE_ACCOUNT
        )

    @mock.patch(
        "waldur_site_agent.event_processing.handlers.agent_identity_management.marketplace_site_agent_identities_register_service"
    )
    @mock.patch(
        "waldur_site_agent.event_processing.handlers.agent_identity_management.marketplace_site_agent_identities_list"
    )
    @mock.patch(
        "waldur_site_agent.event_processing.handlers.common_processors.OfferingMembershipProcessor"
    )
    def test_stomp_handler_remove_action(
        self,
        mock_processor_class,
        mock_list_identities,
        mock_register_service,
    ):
        # Setup mocks for agent identity registration
        mock_agent_identity = mock.Mock()
        mock_agent_identity.uuid = uuid.uuid4()
        mock_agent_identity.name = f"agent-{self.offering_uuid}"
        mock_list_identities.sync.return_value = [mock_agent_identity]

        mock_agent_service = mock.Mock()
        mock_agent_service.uuid = uuid.uuid4()
        mock_agent_service.name = "event_process"
        mock_register_service.sync.return_value = mock_agent_service

        # Setup processor mock
        mock_processor = mock.Mock()
        mock_processor_class.return_value = mock_processor

        message = {
            "account_uuid": self.course_account.uuid.hex,
            "account_username": self.course_account.username,
            "project_uuid": self.waldur_resource.project_uuid.hex,
            "action": "delete",
        }

        test_frame = stomp.utils.Frame(
            "MESSAGE",
            headers={HDR_DESTINATION: "/queue/abc_course_account"},
            body=json.dumps(message),
        )
        handlers.on_account_message_stomp(test_frame, self.offering, "test-agent")

        # Verify agent identity was checked/created
        mock_list_identities.sync.assert_called_once()

        # Verify agent service was registered
        mock_register_service.sync.assert_called_once()

        # Verify processor was called
        mock_processor.register.assert_called_once_with(mock_agent_service)
        mock_processor.process_account_removal.assert_called_once_with(
            self.course_account.username, self.waldur_resource.project_uuid.hex
        )

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_course_accounts_with_active_and_closed_accounts(
        self, mock_get_backend_for_offering
    ):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = (mock_backend, "1.0.0")

        self._setup_common_mocks()

        active_account = CourseAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            user_uuid=uuid.uuid4(),
            username="course-active-account",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            error_traceback="",
            project_slug=self.waldur_resource.project_slug,
            project_start_date=datetime.now(),
            project_end_date=datetime.now(),
        )

        closed_account = CourseAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            user_uuid=uuid.uuid4(),
            username="course-closed-account",
            state=ServiceAccountState.CLOSED,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            error_traceback="",
            project_slug=self.waldur_resource.project_slug,
            project_start_date=datetime.now(),
            project_end_date=datetime.now(),
        )

        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/course_accounts/",
            params={"project_uuid": self.waldur_resource.project_uuid.hex, "page_size": 100},
        ).respond(200, json=[active_account.to_dict(), closed_account.to_dict()])

        processor = OfferingMembershipProcessor(self.offering, self.waldur_rest_client)
        processor._sync_resource_course_accounts(self.waldur_resource)

        # Check that add_users_to_resource was called correctly
        mock_backend.add_users_to_resource.assert_called_once()
        add_args, _ = mock_backend.add_users_to_resource.call_args
        assert add_args[0].backend_id == self.waldur_resource.backend_id
        assert add_args[1] == {"course-active-account"}

        # Check that remove_users_from_resource was called correctly
        mock_backend.remove_users_from_resource.assert_called_once()
        remove_args, _ = mock_backend.remove_users_from_resource.call_args
        assert remove_args[0].backend_id == self.waldur_resource.backend_id
        assert remove_args[1] == {"course-closed-account"}

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_course_accounts_no_service_provider(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = (mock_backend, "1.0.0")

        self._setup_common_mocks()

        processor = OfferingMembershipProcessor(self.offering, self.waldur_rest_client)
        processor.service_provider = None
        processor._sync_resource_course_accounts(self.waldur_resource)

        mock_backend.add_users_to_resource.assert_not_called()
        mock_backend.remove_users_from_resource.assert_not_called()

    @mock.patch("waldur_site_agent.common.processors.utils.get_backend_for_offering")
    def test_sync_resource_course_accounts_empty_usernames(self, mock_get_backend_for_offering):
        mock_backend = backends.UnknownBackend()
        mock_backend.backend_type = "test"
        mock_backend.add_users_to_resource = mock.Mock(return_value={})
        mock_backend.remove_users_from_resource = mock.Mock(return_value={})
        mock_get_backend_for_offering.return_value = (mock_backend, "1.0.0")

        self._setup_common_mocks()

        account_without_username = CourseAccount(
            url="",
            uuid=uuid.uuid4(),
            created=datetime.now(),
            modified=datetime.now(),
            error_message="",
            project=self.waldur_resource.project_uuid,
            project_uuid=self.waldur_resource.project_uuid,
            project_name=self.waldur_resource.project_name,
            user_uuid=uuid.uuid4(),
            username="",
            state=ServiceAccountState.OK,
            customer_uuid=uuid.uuid4(),
            customer_name="",
            error_traceback="",
            project_slug=self.waldur_resource.project_slug,
            project_start_date=datetime.now(),
            project_end_date=datetime.now(),
        )

        respx.get(
            f"{self.BASE_URL}/api/marketplace-service-providers/{self.service_provider.uuid.hex}/course_accounts/",
            params={"project_uuid": self.waldur_resource.project_uuid.hex, "page_size": 100},
        ).respond(200, json=[account_without_username.to_dict()])

        processor = OfferingMembershipProcessor(self.offering, self.waldur_rest_client)
        processor._sync_resource_course_accounts(self.waldur_resource)

        # Check that add_users_to_resource was called correctly with empty set
        mock_backend.add_users_to_resource.assert_called_once()
        add_args, _ = mock_backend.add_users_to_resource.call_args
        assert add_args[0].backend_id == self.waldur_resource.backend_id
        assert add_args[1] == set()

        # Check that remove_users_from_resource was called correctly with empty set
        mock_backend.remove_users_from_resource.assert_called_once()
        remove_args, _ = mock_backend.remove_users_from_resource.call_args
        assert remove_args[0].backend_id == self.waldur_resource.backend_id
        assert remove_args[1] == set()
