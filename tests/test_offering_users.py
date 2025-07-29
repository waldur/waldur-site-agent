from unittest import mock
import unittest
import respx

from waldur_site_agent.common import utils
from waldur_site_agent.common import structures
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.merged_plugin_options import MergedPluginOptions
from waldur_api_client.models.username_generation_policy_enum import UsernameGenerationPolicyEnum
from waldur_api_client.models.offering_user_state_enum import OfferingUserStateEnum
from waldur_site_agent.backend.backends import UnknownUsernameManagementBackend
import uuid


class TestOfferingUserUpdate(unittest.TestCase):
    BASE_URL = "https://waldur.example.com"

    def setUp(self) -> None:
        respx.start()
        self.offering = structures.Offering(
            name="Test offering",
            uuid=uuid.uuid4().hex,
        )
        self.provider_offering_details = ProviderOfferingDetails(
            plugin_options=MergedPluginOptions(
                username_generation_policy=UsernameGenerationPolicyEnum.SERVICE_PROVIDER,
            )
        )
        self.waldur_client = utils.get_client(f"{self.BASE_URL}/api", "test_token")
        self.offering_users = [
            OfferingUser(
                uuid=uuid.uuid4(),
                user_email="user00@example.com",
                is_restricted=False,
                username="",
                state=OfferingUserStateEnum.REQUESTED,
            ),
            OfferingUser(
                uuid=uuid.uuid4(),
                user_email="user01@example.com",
                is_restricted=False,
                username="",
                state=OfferingUserStateEnum.PENDING_ACCOUNT_LINKING,
            ),
        ]

    def tearDown(self) -> None:
        respx.stop()

    def mock_waldur_client(self):
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, json=self.provider_offering_details.to_dict())
        respx.post(
            f"{self.BASE_URL}/api/marketplace-offering-users/{self.offering_users[0].uuid}/begin_creating/"
        ).respond(200, json={})
        for offering_user in self.offering_users:
            respx.patch(
                f"{self.BASE_URL}/api/marketplace-offering-users/{offering_user.uuid}/"
            ).respond(200, json=offering_user.to_dict())
            respx.post(
                f"{self.BASE_URL}/api/marketplace-offering-users/{offering_user.uuid}/set_ok/"
            ).respond(200, json=offering_user.to_dict())

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    def test_offering_user_update(self, get_username_management_backend_mock):
        new_requested_username = "user00"
        new_pending_username = "user01"
        username_management_backend_mock = UnknownUsernameManagementBackend()
        username_management_backend_mock.generate_username = mock.Mock(
            side_effect=[new_requested_username, new_pending_username]
        )
        username_management_backend_mock.get_username = mock.Mock(return_value=None)
        get_username_management_backend_mock.return_value = username_management_backend_mock

        self.mock_waldur_client()
        utils.update_offering_users(self.offering, self.waldur_client, self.offering_users)

        username_management_backend_mock.get_username.assert_has_calls(
            [
                mock.call(self.offering_users[0]),
                mock.call(self.offering_users[1]),
            ]
        )
        username_management_backend_mock.generate_username.assert_has_calls(
            [
                mock.call(self.offering_users[0]),
                mock.call(self.offering_users[1]),
            ]
        )
        self.assertEqual(self.offering_users[0].username, new_requested_username)
        self.assertEqual(self.offering_users[1].username, new_pending_username)
