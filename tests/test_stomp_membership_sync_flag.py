"""Tests for the stomp_membership_sync_enabled offering flag.

The flag decouples membership sync from order processing: an offering can run
orders over STOMP while keeping HTTP polling for membership sync. Three
components read it, and they must agree, otherwise membership sync either runs
twice or not at all.
"""

import logging
from typing import ClassVar

from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing.utils import _determine_observable_object_types

MEMBERSHIP_TYPES = {
    ObservableObjectTypeEnum.USER_ROLE,
    ObservableObjectTypeEnum.RESOURCE,
    ObservableObjectTypeEnum.SERVICE_ACCOUNT,
    ObservableObjectTypeEnum.COURSE_ACCOUNT,
    ObservableObjectTypeEnum.OFFERING_USER,
    ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC,
}


BASE_FIELDS: dict = {
    "name": "Test",
    "waldur_api_url": "http://localhost:8081/api/",
    "waldur_api_token": "mytoken",
    "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
    "backend_type": "test-backend",
    "membership_sync_backend": "test-backend",
    "order_processing_backend": "test-backend",
}


def _offering(**overrides: object) -> Offering:
    return Offering(**{**BASE_FIELDS, **overrides})


def _use_http_polling(offering: Offering) -> bool:
    """Mirror the decision made by polling_processing.agent_membership_sync."""
    use_stomp = (
        offering.stomp_membership_sync_enabled
        if offering.stomp_membership_sync_enabled is not None
        else offering.stomp_enabled
    )
    return not use_stomp


class TestStompSubscriptionGating:
    """STOMP membership subscriptions must follow the flag, not stomp_enabled."""

    def test_defaults_to_stomp_when_unset(self):
        offering = _offering(stomp_enabled=True)
        assert MEMBERSHIP_TYPES.issubset(set(_determine_observable_object_types(offering)))

    def test_explicit_true_subscribes(self):
        offering = _offering(stomp_enabled=True, stomp_membership_sync_enabled=True)
        assert MEMBERSHIP_TYPES.issubset(set(_determine_observable_object_types(offering)))

    def test_explicit_false_does_not_subscribe(self):
        offering = _offering(stomp_enabled=True, stomp_membership_sync_enabled=False)
        object_types = set(_determine_observable_object_types(offering))
        assert not (MEMBERSHIP_TYPES & object_types)
        # Order processing must remain unaffected.
        assert ObservableObjectTypeEnum.ORDER in object_types

    def test_opting_out_keeps_exactly_one_membership_sync_running(self):
        """The whole point of the flag: no double-sync, no gap."""
        offering = _offering(stomp_enabled=True, stomp_membership_sync_enabled=False)
        subscribes_over_stomp = bool(
            MEMBERSHIP_TYPES & set(_determine_observable_object_types(offering))
        )
        assert subscribes_over_stomp is False
        assert _use_http_polling(offering) is True


class TestOrphanedMembershipSyncWarning:
    """stomp_membership_sync_enabled=true without stomp_enabled runs nothing."""

    CAPLOG_LEVELS: ClassVar[dict] = {"logger": "waldur_site_agent.common.structures"}

    def test_warns_when_stomp_is_disabled(self, caplog):
        with caplog.at_level(logging.WARNING, logger=self.CAPLOG_LEVELS["logger"]):
            _offering(stomp_enabled=False, stomp_membership_sync_enabled=True)

        assert "MISCONFIGURATION" in caplog.text
        assert "Membership sync will NOT run at all" in caplog.text
        # The warning must name the offering so it is actionable in a multi-offering config.
        assert "Test" in caplog.text

    def test_warning_describes_the_broken_state(self, caplog):
        """A misconfiguration warning is only useful if it says how to fix it."""
        with caplog.at_level(logging.WARNING, logger=self.CAPLOG_LEVELS["logger"]):
            _offering(stomp_enabled=False, stomp_membership_sync_enabled=True)

        assert "stomp_enabled=true" in caplog.text

    def test_no_warning_for_valid_stomp_setup(self, caplog):
        with caplog.at_level(logging.WARNING, logger=self.CAPLOG_LEVELS["logger"]):
            _offering(stomp_enabled=True, stomp_membership_sync_enabled=True)
        assert "MISCONFIGURATION" not in caplog.text

    def test_no_warning_for_valid_polling_setup(self, caplog):
        with caplog.at_level(logging.WARNING, logger=self.CAPLOG_LEVELS["logger"]):
            _offering(stomp_enabled=False, stomp_membership_sync_enabled=False)
        assert "MISCONFIGURATION" not in caplog.text

    def test_no_warning_when_flag_is_unset(self, caplog):
        with caplog.at_level(logging.WARNING, logger=self.CAPLOG_LEVELS["logger"]):
            _offering(stomp_enabled=False)
        assert "MISCONFIGURATION" not in caplog.text
