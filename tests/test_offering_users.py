from unittest import mock
import unittest
import respx
import json

from waldur_site_agent.common import utils
from waldur_site_agent.common import structures
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.merged_plugin_options import MergedPluginOptions
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.username_generation_policy_enum import UsernameGenerationPolicyEnum
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
            waldur_offering_uuid=uuid.uuid4().hex,
            waldur_api_url="https://example.com/api/",
            waldur_api_token="test_token",
            backend_type="slurm",
        )
        self.provider_offering_details = ProviderOfferingDetails(
            plugin_options=MergedPluginOptions(
                service_provider_can_create_offering_user=True,
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
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_partial_update.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_validation_complete.sync_detailed"
    )
    def test_offering_user_update(
        self,
        set_validation_complete_mock,
        marketplace_offering_users_partial_update_mock,
        marketplace_offering_users_begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        new_requested_username = "user00"
        new_pending_username = "user01"
        new_creating_username = "user02"

        # Create a proper mock backend that is NOT UnknownUsernameManagementBackend
        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username = mock.Mock(
            side_effect=[new_requested_username, new_pending_username, new_creating_username]
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        # Mock all API calls directly instead of using respx
        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        # Create mock response objects with required attributes
        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200

        marketplace_offering_users_begin_creating_mock.return_value = mock_response
        marketplace_offering_users_partial_update_mock.return_value = mock_response
        set_validation_complete_mock.return_value = mock_response

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

        # set_validation_complete should only be called for the PENDING_ACCOUNT_LINKING user
        set_validation_complete_mock.assert_called_once_with(
            uuid=self.offering_users[1].uuid, client=self.waldur_client
        )

    def _setup_error_test_mocks(
        self,
        endpoint_path: str,
        exception_class,
        error_message: str,
        comment_url: str,
        marketplace_provider_offerings_retrieve_mock,
        marketplace_offering_users_begin_creating_mock,
        set_pending_method_mock,
    ):
        """Helper method to setup common mocks for error handling tests."""
        # Use AbstractUsernameManagementBackend instead of UnknownUsernameManagementBackend
        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = exception_class(
            error_message, comment_url=comment_url
        )

        # Mock the API calls directly instead of using respx
        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        # Create mock response objects with required attributes
        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200

        marketplace_offering_users_begin_creating_mock.return_value = mock_response
        set_pending_method_mock.return_value = mock_response

        return username_management_backend_mock, set_pending_method_mock

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
        set_pending_mock.assert_called_once()
        call_args = set_pending_mock.call_args

        # The API method is called with: sync_detailed(uuid, client=client, body=request_body)
        # We need to check the body parameter
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, expected_comment)
        self.assertEqual(body.comment_url, expected_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_account_linking.sync_detailed"
    )
    def test_offering_user_update_with_account_linking_error(
        self,
        marketplace_offering_users_set_pending_account_linking_mock,
        marketplace_offering_users_begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test handling of OfferingUserAccountLinkingRequiredError with comment URL."""
        test_comment_url = "https://example.com/account-linking"
        error_message = "Please link your existing account"

        username_management_backend_mock, set_pending_mock = self._setup_error_test_mocks(
            "set_pending_account_linking",
            backend_exceptions.OfferingUserAccountLinkingRequiredError,
            error_message,
            test_comment_url,
            marketplace_provider_offerings_retrieve_mock,
            marketplace_offering_users_begin_creating_mock,
            marketplace_offering_users_set_pending_account_linking_mock,
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        # This should not raise an exception and should handle the error gracefully
        utils.update_offering_users(self.offering, self.waldur_client, [self.offering_users[0]])

        self._verify_error_test_results(
            username_management_backend_mock, set_pending_mock, error_message, test_comment_url
        )

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_additional_validation.sync_detailed"
    )
    def test_offering_user_update_with_validation_error(
        self,
        marketplace_offering_users_set_pending_additional_validation_mock,
        marketplace_offering_users_begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test handling of OfferingUserAdditionalValidationRequiredError with comment URL."""
        test_comment_url = "https://example.com/validation-form"
        error_message = "Additional documents required"

        username_management_backend_mock, set_pending_mock = self._setup_error_test_mocks(
            "set_pending_additional_validation",
            backend_exceptions.OfferingUserAdditionalValidationRequiredError,
            error_message,
            test_comment_url,
            marketplace_provider_offerings_retrieve_mock,
            marketplace_offering_users_begin_creating_mock,
            marketplace_offering_users_set_pending_additional_validation_mock,
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        # This should not raise an exception and should handle the error gracefully
        utils.update_offering_users(self.offering, self.waldur_client, [self.offering_users[0]])

        self._verify_error_test_results(
            username_management_backend_mock, set_pending_mock, error_message, test_comment_url
        )

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    def test_unknown_username_management_backend_early_exit(
        self, marketplace_provider_offerings_retrieve_mock, get_username_management_backend_mock
    ):
        """Test that UnknownUsernameManagementBackend triggers early exit behavior."""
        # Return an actual UnknownUsernameManagementBackend instance
        get_username_management_backend_mock.return_value = (UnknownUsernameManagementBackend(), "")

        # Mock the offering details API call directly instead of using respx
        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        # Call the function
        result = utils.update_offering_users(self.offering, self.waldur_client, self.offering_users)

        # Verify that processing was skipped (should return False)
        self.assertFalse(result)

        # Verify that usernames remain unchanged (empty)
        self.assertEqual(self.offering_users[0].username, "")
        self.assertEqual(self.offering_users[1].username, "")
        self.assertEqual(self.offering_users[2].username, "")

        # Verify the API call was made to check username generation policy
        marketplace_provider_offerings_retrieve_mock.assert_called_once_with(
            client=self.waldur_client, uuid=self.offering.uuid
        )

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

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    def test_service_provider_cannot_create_offering_user(
        self, marketplace_provider_offerings_retrieve_mock, get_username_management_backend_mock
    ):
        """Test that missing service_provider_can_create_offering_user returns False."""
        offering_details_without_permission = ProviderOfferingDetails(
            plugin_options=MergedPluginOptions()
        )
        marketplace_provider_offerings_retrieve_mock.return_value = (
            offering_details_without_permission
        )

        # Call the function
        result = utils.update_offering_users(self.offering, self.waldur_client, self.offering_users)

        # Verify that processing was skipped (should return False)
        self.assertFalse(result)

        # Verify that usernames remain unchanged (empty)
        self.assertEqual(self.offering_users[0].username, "")
        self.assertEqual(self.offering_users[1].username, "")
        self.assertEqual(self.offering_users[2].username, "")

        # Verify that get_username_management_backend was never called due to early exit
        get_username_management_backend_mock.assert_not_called()

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_account_linking.sync_detailed"
    )
    def test_creating_user_transitions_to_pending_account_linking(
        self,
        set_pending_account_linking_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that a user in CREATING state transitions to PENDING_ACCOUNT_LINKING
        when backend raises OfferingUserAccountLinkingRequiredError.

        This covers the scenario where:
        1. User was moved from REQUESTED to CREATING
        2. Backend initially failed with a non-linking error
        3. On retry, backend raises OfferingUserAccountLinkingRequiredError
        4. The user should transition to PENDING_ACCOUNT_LINKING (not stay stuck in CREATING)
        """
        test_comment_url = "https://example.com/account-linking"
        error_message = "Please link your existing account"

        # User is already in CREATING state (was moved from REQUESTED in a previous run)
        creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="stuck_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAccountLinkingRequiredError(
                error_message, comment_url=test_comment_url
            )
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_pending_account_linking_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [creating_user])

        # Verify the state transition API was called
        set_pending_account_linking_mock.assert_called_once()
        call_args = set_pending_account_linking_mock.call_args
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, error_message)
        self.assertEqual(body.comment_url, test_comment_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_additional_validation.sync_detailed"
    )
    def test_creating_user_transitions_to_pending_validation(
        self,
        set_pending_validation_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that a user in CREATING state transitions to PENDING_ADDITIONAL_VALIDATION
        when backend raises OfferingUserAdditionalValidationRequiredError."""
        test_comment_url = "https://example.com/validation-form"
        error_message = "Additional documents required"

        creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="stuck_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAdditionalValidationRequiredError(
                error_message, comment_url=test_comment_url
            )
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_pending_validation_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [creating_user])

        # Verify the state transition API was called
        set_pending_validation_mock.assert_called_once()
        call_args = set_pending_validation_mock.call_args
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, error_message)
        self.assertEqual(body.comment_url, test_comment_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_account_linking.sync_detailed"
    )
    def test_already_pending_user_stays_in_pending_state(
        self,
        set_pending_account_linking_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that a user already in PENDING_ACCOUNT_LINKING does NOT re-trigger
        the state transition when backend still raises the same error."""
        pending_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="pending_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.PENDING_ACCOUNT_LINKING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAccountLinkingRequiredError("Still pending")
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        utils.update_offering_users(self.offering, self.waldur_client, [pending_user])

        # The state transition API should NOT be called since user is already in the right state
        set_pending_account_linking_mock.assert_not_called()

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_partial_update.sync"
    )
    def test_error_creating_user_is_retried(
        self,
        partial_update_mock,
        begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that ERROR_CREATING user is retried: begin_creating called, then username set."""
        error_creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="error_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.ERROR_CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.return_value = "error_user"
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        begin_creating_mock.return_value = mock_response

        result = utils.update_offering_users(
            self.offering, self.waldur_client, [error_creating_user]
        )

        self.assertTrue(result)
        # begin_creating should be called to transition from ERROR_CREATING to CREATING
        begin_creating_mock.assert_called_once_with(
            uuid=error_creating_user.uuid, client=self.waldur_client
        )
        # Username should be set
        self.assertEqual(error_creating_user.username, "error_user")
        partial_update_mock.assert_called_once()

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_error_creating.sync_detailed"
    )
    def test_backend_error_in_creating_sets_error_creating(
        self,
        set_error_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that BackendError in CREATING state triggers set_error_creating."""
        creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="failing_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.BackendError("SLURM command failed")
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_error_creating_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [creating_user])

        # set_error_creating should be called for BackendError
        set_error_creating_mock.assert_called_once_with(
            uuid=creating_user.uuid, client=self.waldur_client
        )

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_error_creating.sync_detailed"
    )
    def test_network_error_does_not_set_error_creating(
        self,
        set_error_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that a non-BackendError (e.g. network timeout) does NOT trigger set_error_creating."""
        creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="timeout_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            ConnectionError("Network unreachable")
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        utils.update_offering_users(self.offering, self.waldur_client, [creating_user])

        # set_error_creating should NOT be called for non-BackendError exceptions
        set_error_creating_mock.assert_not_called()

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_partial_update.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_validation_complete.sync_detailed"
    )
    def test_pending_user_uses_set_validation_complete(
        self,
        set_validation_complete_mock,
        partial_update_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that PENDING_ACCOUNT_LINKING user with successful username
        calls set_validation_complete (not set_ok)."""
        pending_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="pending_ok_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.PENDING_ACCOUNT_LINKING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.return_value = "pending_ok_user"
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_validation_complete_mock.return_value = mock_response

        result = utils.update_offering_users(self.offering, self.waldur_client, [pending_user])

        self.assertTrue(result)
        set_validation_complete_mock.assert_called_once_with(
            uuid=pending_user.uuid, client=self.waldur_client
        )
        self.assertEqual(pending_user.username, "pending_ok_user")

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_additional_validation.sync_detailed"
    )
    def test_pending_linking_cross_transitions_to_pending_validation(
        self,
        set_pending_validation_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that PENDING_ACCOUNT_LINKING user cross-transitions to
        PENDING_ADDITIONAL_VALIDATION when backend raises AdditionalValidationRequiredError."""
        test_comment_url = "https://example.com/validation-form"
        error_message = "Validation now required"

        pending_linking_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="cross_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.PENDING_ACCOUNT_LINKING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAdditionalValidationRequiredError(
                error_message, comment_url=test_comment_url
            )
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_pending_validation_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [pending_linking_user])

        # Should cross-transition to PENDING_ADDITIONAL_VALIDATION
        set_pending_validation_mock.assert_called_once()
        call_args = set_pending_validation_mock.call_args
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, error_message)
        self.assertEqual(body.comment_url, test_comment_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_account_linking.sync_detailed"
    )
    def test_pending_validation_cross_transitions_to_pending_linking(
        self,
        set_pending_linking_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that PENDING_ADDITIONAL_VALIDATION user cross-transitions to
        PENDING_ACCOUNT_LINKING when backend raises AccountLinkingRequiredError."""
        test_comment_url = "https://example.com/account-linking"
        error_message = "Linking now required"

        pending_validation_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="cross_user2@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAccountLinkingRequiredError(
                error_message, comment_url=test_comment_url
            )
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        set_pending_linking_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [pending_validation_user])

        # Should cross-transition to PENDING_ACCOUNT_LINKING
        set_pending_linking_mock.assert_called_once()
        call_args = set_pending_linking_mock.call_args
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, error_message)
        self.assertEqual(body.comment_url, test_comment_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_additional_validation.sync_detailed"
    )
    def test_already_pending_validation_stays_in_pending_validation(
        self,
        set_pending_validation_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that PENDING_ADDITIONAL_VALIDATION user does NOT re-trigger
        the transition when backend still raises the same error."""
        pending_validation_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="still_pending@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAdditionalValidationRequiredError("Still pending")
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        utils.update_offering_users(self.offering, self.waldur_client, [pending_validation_user])

        # Should NOT call set_pending_additional_validation since already in that state
        set_pending_validation_mock.assert_not_called()

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_pending_account_linking.sync_detailed"
    )
    def test_error_creating_user_transitions_to_pending_linking(
        self,
        set_pending_linking_mock,
        begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that ERROR_CREATING user transitions to PENDING_ACCOUNT_LINKING
        when backend raises OfferingUserAccountLinkingRequiredError."""
        test_comment_url = "https://example.com/account-linking"
        error_message = "Please link your account"

        error_creating_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="error_link_user@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.ERROR_CREATING,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAccountLinkingRequiredError(
                error_message, comment_url=test_comment_url
            )
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        begin_creating_mock.return_value = mock_response
        set_pending_linking_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [error_creating_user])

        # begin_creating should be called first to move from ERROR_CREATING to CREATING
        begin_creating_mock.assert_called_once()
        # Then set_pending_account_linking should be called
        set_pending_linking_mock.assert_called_once()
        call_args = set_pending_linking_mock.call_args
        body = call_args.kwargs["body"]
        self.assertEqual(body.comment, error_message)
        self.assertEqual(body.comment_url, test_comment_url)

    @mock.patch("waldur_site_agent.common.utils.get_username_management_backend")
    @mock.patch(
        "waldur_api_client.api.marketplace_provider_offerings.marketplace_provider_offerings_retrieve.sync"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_begin_creating.sync_detailed"
    )
    @mock.patch(
        "waldur_api_client.api.marketplace_offering_users.marketplace_offering_users_set_error_creating.sync_detailed"
    )
    def test_backend_error_in_requested_sets_error_creating(
        self,
        set_error_creating_mock,
        begin_creating_mock,
        marketplace_provider_offerings_retrieve_mock,
        get_username_management_backend_mock,
    ):
        """Test that BackendError during REQUESTED→CREATING processing triggers set_error_creating."""
        requested_user = OfferingUser(
            uuid=uuid.uuid4(),
            user_email="requested_fail@example.com",
            is_restricted=False,
            username="",
            state=OfferingUserState.REQUESTED,
        )

        username_management_backend_mock = mock.Mock(spec=AbstractUsernameManagementBackend)
        username_management_backend_mock.get_or_create_username.side_effect = (
            backend_exceptions.BackendError("SLURM command failed")
        )
        get_username_management_backend_mock.return_value = (username_management_backend_mock, "")

        marketplace_provider_offerings_retrieve_mock.return_value = self.provider_offering_details

        mock_response = mock.Mock()
        mock_response.parsed = None
        mock_response.status_code = 200
        begin_creating_mock.return_value = mock_response
        set_error_creating_mock.return_value = mock_response

        utils.update_offering_users(self.offering, self.waldur_client, [requested_user])

        # begin_creating should be called (REQUESTED → CREATING)
        begin_creating_mock.assert_called_once()
        # set_error_creating should be called after BackendError
        set_error_creating_mock.assert_called_once_with(
            uuid=requested_user.uuid, client=self.waldur_client
        )
