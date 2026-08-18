"""Tests for team_fetch_attempts / team_fetch_delay retry logic in processors.py.

The retry loop inside _fetch_user_context_for_resource covers the race where a
STOMP create-order event arrives before Waldur has committed the initial team
membership row.  It fires only when the team list comes back empty; a non-empty
but stale team (new member missing) breaks on attempt 1.
"""

from unittest import mock

import pytest

_PATCH = "waldur_site_agent.common.processors"


def _make_processor(*, team_fetch_attempts: int = 1, team_fetch_delay: float = 3.0):
    """Return a minimal OfferingOrderProcessor with configurable retry settings."""
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    processor = OfferingOrderProcessor.__new__(OfferingOrderProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = "offering-uuid"
    processor.resource_backend = mock.Mock()
    processor.resource_backend.team_fetch_attempts = team_fetch_attempts
    processor.resource_backend.team_fetch_delay = team_fetch_delay
    processor._offering_users_cache = []
    return processor


def _make_team_member():
    member = mock.Mock()
    member.uuid = "user-uuid-1"
    return member


class TestTeamFetchAttempts:
    """team_fetch_attempts controls how many times the API is called."""

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_single_attempt_succeeds_without_sleep(self, mock_team_list, mock_sleep):
        """When team_fetch_attempts=1 and team is non-empty, no sleep is called."""
        mock_team_list.sync.return_value = [_make_team_member()]
        processor = _make_processor(team_fetch_attempts=1)

        result = processor._fetch_user_context_for_resource("resource-uuid")

        assert result["team"] != []
        mock_team_list.sync.assert_called_once()
        mock_sleep.assert_not_called()

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_retries_until_team_appears(self, mock_team_list, mock_sleep):
        """With team_fetch_attempts=3, retries until team returns non-empty."""
        member = _make_team_member()
        mock_team_list.sync.side_effect = [None, None, [member]]
        processor = _make_processor(team_fetch_attempts=3, team_fetch_delay=0.1)

        result = processor._fetch_user_context_for_resource("resource-uuid")

        assert result["team"] == [member]
        assert mock_team_list.sync.call_count == 3
        # sleeps between attempt 1→2 and 2→3 (not after the last successful one)
        assert mock_sleep.call_count == 2

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_all_attempts_fail_returns_empty_context(self, mock_team_list, mock_sleep):
        """When all attempts return empty, _fetch_user_context returns empty dict."""
        mock_team_list.sync.return_value = None
        processor = _make_processor(team_fetch_attempts=3, team_fetch_delay=0.1)

        result = processor._fetch_user_context_for_resource("resource-uuid")

        assert result == {
            "team": [],
            "offering_users": [],
            "user_mappings": {},
            "offering_user_mappings": {},
        }
        assert mock_team_list.sync.call_count == 3

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_no_sleep_after_final_failed_attempt(self, mock_team_list, mock_sleep):
        """Sleep is not called after the last attempt, even when it returns empty."""
        mock_team_list.sync.return_value = None
        processor = _make_processor(team_fetch_attempts=2, team_fetch_delay=0.1)

        processor._fetch_user_context_for_resource("resource-uuid")

        # 2 attempts → 1 sleep (only between attempt 1 and 2, not after 2)
        assert mock_sleep.call_count == 1

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_breaks_early_when_team_found(self, mock_team_list, mock_sleep):
        """Stops immediately after a non-empty team is returned."""
        member = _make_team_member()
        mock_team_list.sync.side_effect = [None, [member], Exception("should not reach")]
        processor = _make_processor(team_fetch_attempts=4, team_fetch_delay=0.1)

        result = processor._fetch_user_context_for_resource("resource-uuid")

        assert result["team"] == [member]
        assert mock_team_list.sync.call_count == 2
        assert mock_sleep.call_count == 1


class TestTeamFetchDelay:
    """team_fetch_delay controls the sleep duration between retries."""

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_sleep_called_with_configured_delay(self, mock_team_list, mock_sleep):
        """Sleep receives exactly the value from team_fetch_delay."""
        mock_team_list.sync.side_effect = [None, [_make_team_member()]]
        processor = _make_processor(team_fetch_attempts=2, team_fetch_delay=7.5)

        processor._fetch_user_context_for_resource("resource-uuid")

        mock_sleep.assert_called_once_with(7.5)

    @mock.patch(f"{_PATCH}.sleep")
    @mock.patch(f"{_PATCH}.marketplace_provider_resources_team_list")
    def test_default_delay_is_three_seconds(self, mock_team_list, mock_sleep):
        """Default team_fetch_delay (3.0) is used when the backend does not override it."""
        from waldur_site_agent.backend.backends import BaseBackend

        assert BaseBackend.team_fetch_delay == 3.0

        mock_team_list.sync.side_effect = [None, [_make_team_member()]]
        processor = _make_processor(team_fetch_attempts=2, team_fetch_delay=3.0)

        processor._fetch_user_context_for_resource("resource-uuid")

        mock_sleep.assert_called_once_with(3.0)


class TestTeamFetchDefaults:
    """Verify that BaseBackend ships the documented defaults."""

    def test_base_backend_default_attempts(self):
        from waldur_site_agent.backend.backends import BaseBackend

        assert BaseBackend.team_fetch_attempts == 1

    def test_base_backend_default_delay(self):
        from waldur_site_agent.backend.backends import BaseBackend

        assert BaseBackend.team_fetch_delay == 3.0
