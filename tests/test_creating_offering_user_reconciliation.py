"""Tests for OfferingUser stuck-state reconciliation in the membership sync.

Validates that _get_waldur_offering_users() includes CREATING, ERROR_CREATING,
PENDING_ACCOUNT_LINKING, and PENDING_ADDITIONAL_VALIDATION users so the
downstream code (_group_users_by_state, _process_pending_users) can retry
username generation and recover stuck users.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest import mock

from waldur_api_client import models
from waldur_api_client.models import OfferingUserState

from tests.fixtures import OFFERING
from waldur_site_agent.backend import exceptions as backend_exceptions
from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.utils import _group_users_by_state, _process_pending_users
from waldur_site_agent.event_processing.handlers import _process_offering_user_message

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_offering_user(
    state: OfferingUserState,
    username: str = "",
    user_email: str = "test@example.com",
) -> models.OfferingUser:
    """Create a minimal OfferingUser with the given state."""
    return models.OfferingUser(
        uuid=uuid.uuid4(),
        username=username,
        user_uuid=uuid.uuid4(),
        user_email=user_email,
        offering_uuid=OFFERING.uuid,
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        state=state,
    )


def _make_processor_with_cache(
    users: list[models.OfferingUser],
) -> OfferingMembershipProcessor:
    """Create an OfferingMembershipProcessor with injected user cache.

    Bypasses the constructor (which makes API calls) via object.__new__().
    """
    processor = object.__new__(OfferingMembershipProcessor)
    processor.offering = OFFERING
    processor._offering_users_cache = users
    return processor


def _get_filtered_states(
    users: list[models.OfferingUser],
) -> set[OfferingUserState]:
    processor = _make_processor_with_cache(users)
    filtered = processor._get_waldur_offering_users()
    return {ou.state for ou in filtered}


# ============================================================================
# Group 1: State filter validation
# ============================================================================


class TestStateFilter:
    """_get_waldur_offering_users() must include stuck-state users in sync.

    These tests validate that the state filter in processors.py includes
    CREATING, ERROR_CREATING, PENDING_ACCOUNT_LINKING, and
    PENDING_ADDITIONAL_VALIDATION alongside OK and REQUESTED — so that
    stuck users are retried by _process_pending_users() on each sync cycle.
    """

    def test_creating_included(self):
        """CREATING users are included in membership sync for retry."""
        states = _get_filtered_states([
            _make_offering_user(OfferingUserState.OK, username="ok"),
            _make_offering_user(OfferingUserState.CREATING),
        ])
        assert OfferingUserState.CREATING in states

    def test_error_creating_included(self):
        """ERROR_CREATING users are included for retry via begin_creating."""
        states = _get_filtered_states([
            _make_offering_user(OfferingUserState.OK, username="ok"),
            _make_offering_user(OfferingUserState.ERROR_CREATING),
        ])
        assert OfferingUserState.ERROR_CREATING in states

    def test_pending_linking_included(self):
        """PENDING_ACCOUNT_LINKING users are included for resolution check."""
        states = _get_filtered_states([
            _make_offering_user(OfferingUserState.OK, username="ok"),
            _make_offering_user(OfferingUserState.PENDING_ACCOUNT_LINKING),
        ])
        assert OfferingUserState.PENDING_ACCOUNT_LINKING in states

    def test_pending_validation_included(self):
        """PENDING_ADDITIONAL_VALIDATION users are included for resolution check."""
        states = _get_filtered_states([
            _make_offering_user(OfferingUserState.OK, username="ok"),
            _make_offering_user(OfferingUserState.PENDING_ADDITIONAL_VALIDATION),
        ])
        assert OfferingUserState.PENDING_ADDITIONAL_VALIDATION in states

    def test_all_stuck_states_included(self):
        """All stuck states must be included in membership sync."""
        stuck_states = [
            OfferingUserState.CREATING,
            OfferingUserState.ERROR_CREATING,
            OfferingUserState.PENDING_ACCOUNT_LINKING,
            OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
        ]
        users = [_make_offering_user(OfferingUserState.OK, username="ok")]
        users += [_make_offering_user(s) for s in stuck_states]

        states = _get_filtered_states(users)

        for expected_state in stuck_states:
            assert expected_state in states, (
                f"{expected_state} is excluded from membership sync — "
                f"users in this state get stuck forever."
            )

    def test_all_stuck_states_reach_update_offering_users(self):
        """All stuck states reach _update_offering_users() for processing."""
        stuck_states = [
            OfferingUserState.CREATING,
            OfferingUserState.ERROR_CREATING,
            OfferingUserState.PENDING_ACCOUNT_LINKING,
            OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
        ]
        users = [_make_offering_user(OfferingUserState.OK, username="ok")]
        users += [_make_offering_user(s) for s in stuck_states]

        processor = _make_processor_with_cache(users)

        with mock.patch.object(
            processor, "_validate_offering_user_configuration", return_value=True
        ), mock.patch.object(
            processor, "_update_offering_users", return_value=False
        ) as mock_update:
            processor._refresh_local_offering_users()

            mock_update.assert_called_once()
            passed_states = {ou.state for ou in mock_update.call_args[0][0]}
            for expected_state in stuck_states:
                assert expected_state in passed_states, (
                    f"{expected_state} users never reach _update_offering_users() "
                    f"— they will never get usernames generated or retried."
                )


# ============================================================================
# Group 2: _group_users_by_state classification
# ============================================================================


class TestGroupUsersByState:
    """Verify _group_users_by_state correctly classifies all stuck states."""

    def test_all_stuck_states_classified_as_pending(self):
        """CREATING, ERROR_CREATING, PENDING_* are classified as pending."""
        ok = _make_offering_user(OfferingUserState.OK, username="ok")
        requested = _make_offering_user(OfferingUserState.REQUESTED)
        creating = _make_offering_user(OfferingUserState.CREATING)
        error = _make_offering_user(OfferingUserState.ERROR_CREATING)
        linking = _make_offering_user(OfferingUserState.PENDING_ACCOUNT_LINKING)
        validation = _make_offering_user(OfferingUserState.PENDING_ADDITIONAL_VALIDATION)

        requested_list, pending_list = _group_users_by_state(
            [ok, requested, creating, error, linking, validation]
        )

        requested_states = {u.state for u in requested_list}
        pending_states = {u.state for u in pending_list}

        assert requested_states == {OfferingUserState.REQUESTED}
        assert pending_states == {
            OfferingUserState.CREATING,
            OfferingUserState.ERROR_CREATING,
            OfferingUserState.PENDING_ACCOUNT_LINKING,
            OfferingUserState.PENDING_ADDITIONAL_VALIDATION,
        }
        assert OfferingUserState.OK not in requested_states | pending_states


# ============================================================================
# Group 3: Recovery paths via _process_pending_users
# ============================================================================


# Common mock paths
_MOCK_BEGIN_CREATING = (
    "waldur_api_client.api.marketplace_offering_users"
    ".marketplace_offering_users_begin_creating.sync_detailed"
)
_MOCK_PARTIAL_UPDATE = (
    "waldur_api_client.api.marketplace_offering_users"
    ".marketplace_offering_users_partial_update.sync"
)
_MOCK_SET_ERROR_CREATING = (
    "waldur_api_client.api.marketplace_offering_users"
    ".marketplace_offering_users_set_error_creating.sync_detailed"
)
_MOCK_SET_VALIDATION_COMPLETE = (
    "waldur_api_client.api.marketplace_offering_users"
    ".marketplace_offering_users_set_validation_complete.sync_detailed"
)
_MOCK_SET_PENDING_LINKING = (
    "waldur_api_client.api.marketplace_offering_users"
    ".marketplace_offering_users_set_pending_account_linking.sync_detailed"
)


class TestRecoveryPaths:
    """Test _process_pending_users() recovery for each stuck state."""

    def setup_method(self):
        self.mock_backend = mock.Mock()
        self.mock_client = mock.Mock()

    @mock.patch(_MOCK_PARTIAL_UPDATE)
    @mock.patch(_MOCK_BEGIN_CREATING)
    def test_creating_retried_username_succeeds(
        self, mock_begin_creating, mock_partial_update
    ):
        """CREATING user: username generation succeeds → username set."""
        user = _make_offering_user(OfferingUserState.CREATING)
        self.mock_backend.get_or_create_username.return_value = "newuser"

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is True
        self.mock_backend.get_or_create_username.assert_called_once_with(user)
        mock_partial_update.assert_called_once()
        mock_begin_creating.assert_not_called()

    @mock.patch(_MOCK_PARTIAL_UPDATE)
    @mock.patch(_MOCK_BEGIN_CREATING)
    def test_error_creating_retried_via_begin_creating(
        self, mock_begin_creating, mock_partial_update
    ):
        """ERROR_CREATING user: begin_creating called first, then username set."""
        user = _make_offering_user(OfferingUserState.ERROR_CREATING)
        self.mock_backend.get_or_create_username.return_value = "newuser"

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is True
        mock_begin_creating.assert_called_once_with(
            uuid=user.uuid, client=self.mock_client
        )
        mock_partial_update.assert_called_once()

    @mock.patch(_MOCK_PARTIAL_UPDATE)
    @mock.patch(_MOCK_SET_VALIDATION_COMPLETE)
    def test_pending_linking_resolved(
        self, mock_set_complete, mock_partial_update
    ):
        """PENDING_ACCOUNT_LINKING user: backend no longer raises error → OK."""
        user = _make_offering_user(OfferingUserState.PENDING_ACCOUNT_LINKING)
        self.mock_backend.get_or_create_username.return_value = "newuser"

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is True
        mock_set_complete.assert_called_once_with(
            uuid=user.uuid, client=self.mock_client
        )
        mock_partial_update.assert_called_once()

    @mock.patch(_MOCK_PARTIAL_UPDATE)
    @mock.patch(_MOCK_SET_VALIDATION_COMPLETE)
    def test_pending_validation_resolved(
        self, mock_set_complete, mock_partial_update
    ):
        """PENDING_ADDITIONAL_VALIDATION user: resolved → OK."""
        user = _make_offering_user(OfferingUserState.PENDING_ADDITIONAL_VALIDATION)
        self.mock_backend.get_or_create_username.return_value = "newuser"

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is True
        mock_set_complete.assert_called_once_with(
            uuid=user.uuid, client=self.mock_client
        )
        mock_partial_update.assert_called_once()

    @mock.patch(_MOCK_SET_ERROR_CREATING)
    def test_creating_network_error_stays_creating(self, mock_set_error):
        """CREATING user: ConnectionError → stays CREATING (not ERROR_CREATING)."""
        user = _make_offering_user(OfferingUserState.CREATING)
        self.mock_backend.get_or_create_username.side_effect = ConnectionError(
            "timeout"
        )

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is False
        mock_set_error.assert_not_called()

    @mock.patch(_MOCK_SET_ERROR_CREATING)
    def test_creating_backend_error_to_error_creating(self, mock_set_error):
        """CREATING user: BackendError → transitions to ERROR_CREATING."""
        user = _make_offering_user(OfferingUserState.CREATING)
        self.mock_backend.get_or_create_username.side_effect = (
            backend_exceptions.BackendError("slurm down")
        )

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is False
        mock_set_error.assert_called_once_with(
            uuid=user.uuid, client=self.mock_client
        )

    @mock.patch(_MOCK_SET_PENDING_LINKING)
    def test_pending_linking_still_required_idempotent(self, mock_set_pending):
        """PENDING_ACCOUNT_LINKING: backend still requires linking → no-op."""
        user = _make_offering_user(OfferingUserState.PENDING_ACCOUNT_LINKING)
        self.mock_backend.get_or_create_username.side_effect = (
            backend_exceptions.OfferingUserAccountLinkingRequiredError("link needed")
        )

        result = _process_pending_users([user], self.mock_backend, self.mock_client)

        assert result is False
        mock_set_pending.assert_not_called()


# ============================================================================
# Group 4: Event path gap documentation
# ============================================================================


class TestEventPathGaps:
    """Document that STOMP event paths do NOT generate usernames.

    This is by design: the polling-based membership sync is the only path
    that generates usernames.
    """

    def test_create_event_without_username_only_forwards_attributes(self):
        """OFFERING_USER 'create' event with empty username only forwards attributes."""
        message = {
            "action": "create",
            "offering_user_uuid": uuid.uuid4().hex,
            "user_uuid": uuid.uuid4().hex,
            "username": "",
            "state": "Requested",
            "attributes": {"email": "test@example.com"},
        }

        with mock.patch(
            "waldur_site_agent.event_processing.handlers._forward_user_attributes_to_backend"
        ) as mock_forward, mock.patch(
            "waldur_site_agent.event_processing.handlers.register_event_process_service"
        ), mock.patch(
            "waldur_site_agent.common.utils.get_client"
        ):
            _process_offering_user_message(message, OFFERING, "test-agent")

            mock_forward.assert_called_once_with(
                OFFERING, "", {"email": "test@example.com"}, "test-agent"
            )

    def test_role_change_skips_user_without_username(self):
        """process_user_role_changed() skips users without username."""
        creating_user = _make_offering_user(OfferingUserState.CREATING, username="")

        processor = _make_processor_with_cache([creating_user])
        processor.waldur_rest_client = mock.Mock()
        processor.resource_backend = mock.Mock()

        with mock.patch.object(
            processor,
            "_get_user_offering_users",
            return_value=[creating_user],
        ), mock.patch.object(
            processor, "_update_offering_users", return_value=False
        ), mock.patch.object(
            processor, "_get_waldur_resources"
        ) as mock_get_resources:
            processor.process_user_role_changed(
                user_uuid=str(creating_user.user_uuid),
                project_uuid="some-project-uuid",
                granted=True,
            )

            # Should return early because username is blank
            mock_get_resources.assert_not_called()
