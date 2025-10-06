from unittest import mock
import unittest
import respx
import json

from waldur_site_agent.common import utils
from waldur_site_agent.common import structures
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.merged_plugin_options import MergedPluginOptions
from waldur_api_client.models.username_generation_policy_enum import UsernameGenerationPolicyEnum
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_site_agent.backend.backends import (
    UnknownUsernameManagementBackend,
    AbstractUsernameManagementBackend,
)
from waldur_site_agent.backend import exceptions as backend_exceptions
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
                state=OfferingUserState.REQUESTED,
            ),
            OfferingUser(
                uuid=uuid.uuid4(),
                user_email="user01@example.com",
                is_restricted=False,
                username="",
                state=OfferingUserState.PENDING_ACCOUNT_LINKING,
            ),
            OfferingUser(
                uuid=uuid.uuid4(),
                user_email="user02@example.com",
                is_restricted=False,
                username="",
                state=OfferingUserState.CREATING,
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
        new_creating_username = "user02"

        # Create a proper mock backend that is NOT UnknownUsernameManagementBackend
        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username = mock.Mock(
            side_effect=[new_requested_username, new_pending_username, new_creating_username]
        )
        get_username_management_backend_mock.return_value = username_management_backend_mock

        self.mock_waldur_client()
        result = utils.update_offering_users(self.offering, self.waldur_client, self.offering_users)

        # Verify that processing actually occurred (should return True since usernames were updated)
        self.assertTrue(result)

        username_management_backend_mock.get_or_create_username.assert_has_calls(
            [
                mock.call(self.offering_users[0]),
                mock.call(self.offering_users[1]),
                mock.call(self.offering_users[2]),
            ]
        )
        self.assertEqual(self.offering_users[0].username, new_requested_username)
        self.assertEqual(self.offering_users[1].username, new_pending_username)
        self.assertEqual(self.offering_users[2].username, new_creating_username)

    def _setup_error_test_mocks(
        self, endpoint_path: str, exception_class, error_message: str, comment_url: str
    ):
        """Helper method to setup common mocks for error handling tests."""
        # Use AbstractUsernameManagementBackend instead of UnknownUsernameManagementBackend
        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = exception_class(
            error_message, comment_url=comment_url
        )

        # Mock the API endpoints
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, json=self.provider_offering_details.to_dict())

        respx.post(
            f"{self.BASE_URL}/api/marketplace-offering-users/{self.offering_users[0].uuid}/begin_creating/"
        ).respond(200, json={})

        set_pending_mock = respx.post(
            f"{self.BASE_URL}/api/marketplace-offering-users/{self.offering_users[0].uuid}/{endpoint_path}/"
        ).respond(200, json={})

        return username_management_backend_mock, set_pending_mock

    def _verify_error_test_results(
        self,
        username_management_backend_mock,
        set_pending_mock,
        expected_comment: str,
        expected_url: str,
    ):
        """Helper method to verify common assertions for error handling tests."""
        # Verify the backend method was called during the error flow
        username_management_backend_mock.get_or_create_username.assert_called_once_with(
            self.offering_users[0]
        )

        # Verify the API was called with separate comment and comment_url fields
        assert set_pending_mock.called
        request_body = set_pending_mock.calls[0].request.content.decode()
        payload = json.loads(request_body)
        self.assertEqual(payload["comment"], expected_comment)
        self.assertEqual(payload["comment_url"], expected_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    def test_offering_user_update_with_account_linking_error(
        self, get_username_management_backend_mock
    ):
        """Test handling of OfferingUserAccountLinkingRequiredError with comment URL."""
        test_comment_url = "https://example.com/account-linking"
        error_message = "Please link your existing account"

        username_management_backend_mock, set_pending_mock = self._setup_error_test_mocks(
            "set_pending_account_linking",
            backend_exceptions.OfferingUserAccountLinkingRequiredError,
            error_message,
            test_comment_url,
        )
        get_username_management_backend_mock.return_value = username_management_backend_mock

        # This should not raise an exception and should handle the error gracefully
        utils.update_offering_users(self.offering, self.waldur_client, [self.offering_users[0]])

        self._verify_error_test_results(
            username_management_backend_mock, set_pending_mock, error_message, test_comment_url
        )

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    def test_offering_user_update_with_validation_error(self, get_username_management_backend_mock):
        """Test handling of OfferingUserAdditionalValidationRequiredError with comment URL."""
        test_comment_url = "https://example.com/validation-form"
        error_message = "Additional documents required"

        username_management_backend_mock, set_pending_mock = self._setup_error_test_mocks(
            "set_pending_additional_validation",
            backend_exceptions.OfferingUserAdditionalValidationRequiredError,
            error_message,
            test_comment_url,
        )
        get_username_management_backend_mock.return_value = username_management_backend_mock

        # This should not raise an exception and should handle the error gracefully
        utils.update_offering_users(self.offering, self.waldur_client, [self.offering_users[0]])

        self._verify_error_test_results(
            username_management_backend_mock, set_pending_mock, error_message, test_comment_url
        )

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    def test_unknown_username_management_backend_early_exit(
        self, get_username_management_backend_mock
    ):
        """Test that UnknownUsernameManagementBackend triggers early exit behavior."""
        # Return an actual UnknownUsernameManagementBackend instance
        get_username_management_backend_mock.return_value = UnknownUsernameManagementBackend()

        # Mock the offering details API call (should still be called)
        respx.get(
            f"{self.BASE_URL}/api/marketplace-provider-offerings/{self.offering.uuid}/"
        ).respond(200, json=self.provider_offering_details.to_dict())

        # Call the function
        result = utils.update_offering_users(self.offering, self.waldur_client, self.offering_users)

        # Verify that processing was skipped (should return False)
        self.assertFalse(result)

        # Verify that usernames remain unchanged (empty)
        self.assertEqual(self.offering_users[0].username, "")
        self.assertEqual(self.offering_users[1].username, "")
        self.assertEqual(self.offering_users[2].username, "")

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    def test_empty_offering_users_list(self, get_username_management_backend_mock):
        """Test that empty offering users list returns False immediately."""
        # This shouldn't even be called due to early exit
        get_username_management_backend_mock.return_value = mock.Mock(
            spec=AbstractUsernameManagementBackend
        )

        # Call with empty list
        result = utils.update_offering_users(self.offering, self.waldur_client, [])

        # Verify early exit behavior
        self.assertFalse(result)
        # Verify that get_username_management_backend was never called
        get_username_management_backend_mock.assert_not_called()
