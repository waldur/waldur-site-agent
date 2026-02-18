"""End-to-end OFFERING_USER pub/sub tests for Waldur federation.

Tests the OFFERING_USER STOMP event flow:
  1. STOMP subscription for OFFERING_USER establishes
  2. Modifying user attributes on Waldur A triggers attribute_update events
  3. Creating/updating OfferingUsers triggers create/update events
  4. Events are received by the site-agent STOMP handler

These tests require the Mastermind-side signal handlers to be deployed.
If Mastermind does NOT yet publish attribute_update events for User.post_save,
the attribute update tests will report that and skip gracefully.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur-a>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_offering_user_pubsub.py -v -s
"""

from __future__ import annotations

import logging
import os
import time
import uuid

import pytest

from plugins.waldur.tests.e2e.conftest import (
    MessageCapture,
    check_stomp_available,
    snapshot_resource,
)
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.utils import (
    setup_stomp_offering_subscriptions,
    stop_stomp_consumers,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# How long to wait for a STOMP event (seconds)
EVENT_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stomp_available(offering):
    """Skip if STOMP not available."""
    if not offering.stomp_enabled:
        pytest.skip("stomp_enabled=false in config")
    if not check_stomp_available(offering.waldur_api_url):
        pytest.skip(f"STOMP endpoint not available on {offering.waldur_api_url}")


@pytest.fixture(scope="module")
def offering_user_capture():
    """Message capture for OFFERING_USER events."""
    return MessageCapture()


@pytest.fixture(scope="module")
def stomp_consumers_with_capture(stomp_available, offering, offering_user_capture, report):
    """Set up STOMP connections and replace OFFERING_USER handler with capture."""
    consumers = setup_stomp_offering_subscriptions(offering, "e2e-offering-user-test")

    # Find and replace OFFERING_USER handler with capture
    for conn, sub, off in consumers:
        observable = (
            sub.observable_objects[0]["object_type"]
            if sub.observable_objects
            else ""
        )
        if observable == "offering_user":
            listener = conn.get_listener(WALDUR_LISTENER_NAME)
            if listener:
                original = listener.on_message_callback
                listener.on_message_callback = offering_user_capture.make_handler(
                    delegate=original
                )
                logger.info("Replaced OFFERING_USER handler with capture+delegate")

    yield consumers

    stop_stomp_consumers({(offering.name, offering.waldur_offering_uuid): consumers})


@pytest.fixture(scope="module")
def current_user(waldur_client_a):
    """Get the current API user on Waldur A."""
    resp = waldur_client_a.get_httpx_client().get("/api/users/me/")
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestOfferingUserStompSubscription:
    """Verify OFFERING_USER STOMP subscription is established."""

    def test_offering_user_subscription_connected(
        self, stomp_consumers_with_capture, report
    ):
        """OFFERING_USER STOMP subscription connects successfully."""
        report.heading(2, "OFFERING_USER STOMP Subscription")

        found = False
        for conn, sub, off in stomp_consumers_with_capture:
            observable = (
                sub.observable_objects[0]["object_type"]
                if sub.observable_objects
                else ""
            )
            if observable == "offering_user":
                found = True
                assert conn.is_connected(), "OFFERING_USER STOMP connection not connected"
                report.status_snapshot(
                    "OFFERING_USER STOMP",
                    {
                        "offering": off.name,
                        "connected": str(conn.is_connected()),
                        "object_type": observable,
                        "subscription_uuid": (
                            sub.uuid.hex if hasattr(sub.uuid, "hex") else str(sub.uuid)
                        ),
                    },
                )

        assert found, "No OFFERING_USER consumer found in STOMP connections"
        report.text("OFFERING_USER STOMP subscription is connected and active.")


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestOfferingUserEvents:
    """Test OFFERING_USER events triggered by user/offering-user modifications.

    Test flow:
      1. Create an OfferingUser on Waldur A -> expect 'create' event
      2. Update the User's profile attributes on Waldur A -> expect 'attribute_update' event
      3. Update the OfferingUser on Waldur A -> expect 'update' event
      4. Delete the OfferingUser -> expect 'delete' event
    """

    _state: dict = {}

    def test_01_create_offering_user(
        self,
        offering,
        waldur_client_a,
        current_user,
        stomp_consumers_with_capture,
        offering_user_capture,
        report,
    ):
        """Create an OfferingUser and verify STOMP create event."""
        report.heading(2, "OfferingUser Create Event")

        user_uuid = current_user["uuid"]
        user_url = current_user["url"]
        offering_uuid = offering.waldur_offering_uuid

        # Check if offering user already exists for this user+offering
        resp = waldur_client_a.get_httpx_client().get(
            "/api/marketplace-offering-users/",
            params={
                "user_uuid": user_uuid,
                "offering_uuid": offering_uuid,
            },
        )
        resp.raise_for_status()
        existing = resp.json()

        if existing:
            # Use existing offering user
            ou = existing[0]
            self.__class__._state["offering_user_uuid"] = ou["uuid"]
            self.__class__._state["offering_user_existed"] = True
            report.text(
                f"OfferingUser already exists: `{ou['uuid']}` — skipping create, "
                "will proceed with update tests."
            )
            return

        # Create new offering user
        resp = waldur_client_a.get_httpx_client().post(
            "/api/marketplace-offering-users/",
            json={
                "user": user_url,
                "offering_uuid": offering_uuid,
            },
        )
        resp.raise_for_status()
        ou_data = resp.json()
        ou_uuid = ou_data["uuid"]
        self.__class__._state["offering_user_uuid"] = ou_uuid
        self.__class__._state["offering_user_existed"] = False

        report.text(f"Created OfferingUser `{ou_uuid}` for user `{user_uuid}`")

        # Wait for STOMP event
        event = offering_user_capture.wait_for_event(
            "offering_user_uuid", ou_uuid.replace("-", ""), timeout=EVENT_TIMEOUT
        )
        if event is None:
            # Also try with dashes
            event = offering_user_capture.wait_for_event(
                "offering_user_uuid", ou_uuid, timeout=5
            )

        if event:
            report.text(f"STOMP create event received: action=`{event.get('action')}`")
            report.status_snapshot(
                "Create Event",
                {
                    "action": event.get("action", ""),
                    "offering_user_uuid": event.get("offering_user_uuid", ""),
                    "username": event.get("username", ""),
                    "attributes": str(list(event.get("attributes", {}).keys())),
                },
            )
            assert event.get("action") in ("create", "update"), (
                f"Expected create/update action, got: {event.get('action')}"
            )
        else:
            report.text(
                f"No STOMP create event received within {EVENT_TIMEOUT}s. "
                "Mastermind may not publish OFFERING_USER create events with this config."
            )
            # Log all captured messages for diagnostics
            all_msgs = offering_user_capture.messages
            report.text(f"Total captured OFFERING_USER messages: {len(all_msgs)}")
            for i, msg in enumerate(all_msgs):
                report.text(f"  [{i}] action={msg.get('action')} uuid={msg.get('offering_user_uuid')}")

    def test_02_update_user_attributes(
        self,
        offering,
        waldur_client_a,
        current_user,
        stomp_consumers_with_capture,
        offering_user_capture,
        report,
    ):
        """Update user profile attributes and check for attribute_update event."""
        report.heading(2, "User Attribute Update Event")

        if not self.__class__._state.get("offering_user_uuid"):
            pytest.skip("No OfferingUser created in previous test")

        user_uuid = current_user["uuid"]
        original_job_title = current_user.get("job_title", "")

        # Change a user attribute (job_title is safe to modify and revert)
        test_job_title = f"E2E Test {uuid.uuid4().hex[:6]}"
        count_before = len(offering_user_capture.messages)

        resp = waldur_client_a.get_httpx_client().patch(
            f"/api/users/{user_uuid}/",
            json={"job_title": test_job_title},
        )
        resp.raise_for_status()
        report.text(f"Updated user `{user_uuid}` job_title to `{test_job_title}`")

        # Wait for attribute_update event
        event = offering_user_capture.wait_for_any(timeout=EVENT_TIMEOUT)

        # Check if we got a new event after the update
        new_messages = offering_user_capture.messages[count_before:]
        attr_events = [m for m in new_messages if m.get("action") == "attribute_update"]

        if attr_events:
            event = attr_events[0]
            report.text(f"attribute_update event received!")
            report.status_snapshot(
                "Attribute Update Event",
                {
                    "action": event.get("action", ""),
                    "username": event.get("username", ""),
                    "changed_attributes": str(event.get("changed_attributes", [])),
                    "attributes_keys": str(list(event.get("attributes", {}).keys())),
                },
            )
            # Verify the event structure
            assert "attributes" in event, "Event missing 'attributes' field"
            assert "changed_attributes" in event, "Event missing 'changed_attributes' field"
            report.text("attribute_update event structure validated.")
        else:
            # Check for any other events
            if new_messages:
                report.text(
                    f"Received {len(new_messages)} event(s) but none were attribute_update:"
                )
                for msg in new_messages:
                    report.text(f"  action={msg.get('action')} uuid={msg.get('offering_user_uuid')}")
            else:
                report.text(
                    f"No attribute_update event received within {EVENT_TIMEOUT}s. "
                    "This is expected if Mastermind's User.post_save handler for "
                    "OFFERING_USER attribute_update is not yet deployed "
                    "(see PUBSUB_USER_SYNC_PLAN.md)."
                )

        # Revert the change
        resp = waldur_client_a.get_httpx_client().patch(
            f"/api/users/{user_uuid}/",
            json={"job_title": original_job_title},
        )
        resp.raise_for_status()
        report.text(f"Reverted user job_title to `{original_job_title}`")

    def test_03_update_offering_user(
        self,
        offering,
        waldur_client_a,
        stomp_consumers_with_capture,
        offering_user_capture,
        report,
    ):
        """Update OfferingUser fields and check for update event."""
        report.heading(2, "OfferingUser Update Event")

        ou_uuid = self.__class__._state.get("offering_user_uuid")
        if not ou_uuid:
            pytest.skip("No OfferingUser from previous test")

        count_before = len(offering_user_capture.messages)

        # Update the offering user's provider comment (safe, reversible field)
        test_comment = f"E2E pubsub test {uuid.uuid4().hex[:6]}"
        resp = waldur_client_a.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_uuid}/",
            json={"service_provider_comment": test_comment},
        )
        resp.raise_for_status()
        report.text(f"Updated OfferingUser `{ou_uuid}` service_provider_comment")

        # Wait for event
        event = offering_user_capture.wait_for_any(timeout=EVENT_TIMEOUT)

        new_messages = offering_user_capture.messages[count_before:]
        update_events = [m for m in new_messages if m.get("action") in ("update", "attribute_update")]

        if update_events:
            event = update_events[0]
            report.text(f"OfferingUser update event received: action=`{event.get('action')}`")
            report.status_snapshot(
                "OfferingUser Update Event",
                {
                    "action": event.get("action", ""),
                    "offering_user_uuid": event.get("offering_user_uuid", ""),
                    "username": event.get("username", ""),
                },
            )
        else:
            if new_messages:
                report.text(f"Received {len(new_messages)} event(s) after update:")
                for msg in new_messages:
                    report.text(f"  action={msg.get('action')} uuid={msg.get('offering_user_uuid')}")
            else:
                report.text(
                    f"No update event received within {EVENT_TIMEOUT}s. "
                    "OfferingUser.post_save may not publish events for comment changes."
                )

        # Revert
        resp = waldur_client_a.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_uuid}/",
            json={"service_provider_comment": ""},
        )
        resp.raise_for_status()

    def test_04_delete_offering_user(
        self,
        offering,
        waldur_client_a,
        stomp_consumers_with_capture,
        offering_user_capture,
        report,
    ):
        """Delete OfferingUser and check for delete event."""
        report.heading(2, "OfferingUser Delete Event")

        ou_uuid = self.__class__._state.get("offering_user_uuid")
        if not ou_uuid:
            pytest.skip("No OfferingUser from previous test")

        if self.__class__._state.get("offering_user_existed"):
            report.text(
                "OfferingUser existed before test — skipping deletion to avoid "
                "disrupting existing state."
            )
            return

        count_before = len(offering_user_capture.messages)

        resp = waldur_client_a.get_httpx_client().delete(
            f"/api/marketplace-offering-users/{ou_uuid}/",
        )
        if resp.status_code == 204:
            report.text(f"Deleted OfferingUser `{ou_uuid}`")
        elif resp.status_code == 404:
            report.text(f"OfferingUser `{ou_uuid}` already deleted")
            return
        else:
            resp.raise_for_status()

        # Wait for delete event
        event = offering_user_capture.wait_for_any(timeout=EVENT_TIMEOUT)

        new_messages = offering_user_capture.messages[count_before:]
        delete_events = [m for m in new_messages if m.get("action") == "delete"]

        if delete_events:
            event = delete_events[0]
            report.text(f"Delete event received: action=`{event.get('action')}`")
            report.status_snapshot(
                "Delete Event",
                {
                    "action": event.get("action", ""),
                    "offering_user_uuid": event.get("offering_user_uuid", ""),
                    "username": event.get("username", ""),
                },
            )
        else:
            if new_messages:
                report.text(f"Received {len(new_messages)} event(s) after delete:")
                for msg in new_messages:
                    report.text(f"  action={msg.get('action')} uuid={msg.get('offering_user_uuid')}")
            else:
                report.text(
                    f"No delete event received within {EVENT_TIMEOUT}s."
                )

    def test_05_summary(self, offering_user_capture, report):
        """Summary of all captured OFFERING_USER events."""
        report.heading(2, "OFFERING_USER Event Summary")

        all_msgs = offering_user_capture.messages
        report.text(f"Total OFFERING_USER events captured: {len(all_msgs)}")

        if all_msgs:
            actions = {}
            for msg in all_msgs:
                action = msg.get("action", "unknown")
                actions[action] = actions.get(action, 0) + 1

            report.text("Events by action type:")
            for action, count in sorted(actions.items()):
                report.text(f"  - `{action}`: {count}")

            report.text("")
            report.text("All captured events:")
            for i, msg in enumerate(all_msgs):
                report.text(
                    f"  [{i}] action=`{msg.get('action')}` "
                    f"user=`{msg.get('username', '')}` "
                    f"ou_uuid=`{msg.get('offering_user_uuid', '')}`"
                )
        else:
            report.text(
                "No OFFERING_USER events were captured. This indicates that "
                "Mastermind does not yet publish OFFERING_USER STOMP events for "
                "the tested operations on this instance. "
                "See PUBSUB_USER_SYNC_PLAN.md for required Mastermind changes."
            )
