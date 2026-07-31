"""Tests for Sentry grouping of structlog-rendered log records."""

import logging

from waldur_site_agent.common.sentry import before_send
from waldur_site_agent.common.structlog_message import (
    normalize_for_fingerprint,
    parse_structlog_message,
)


def _queue_error(subscription: str, suffix: str) -> str:
    """Build a structlog dict repr matching the broker errors seen in Sentry."""
    return (
        "{'event': \"Received an error NOT_FOUND - queue "
        f"'subscription_{subscription}_offering_37f4f67fa1ed44118459679ccd50e201_{suffix}'"
        " in vhost 'b281fcf8d77c41aa83d7ee61520c3dd8' process is stopped by supervisor\", "
        "'level': 'error', 'logger': 'waldur_site_agent.event_processing'}"
    )


def _record(msg: str, name: str = "waldur_site_agent.event_processing") -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )


def _fingerprint(msg: str, name: str = "waldur_site_agent.event_processing") -> list:
    event = before_send({"logentry": {"message": msg}}, {"log_record": _record(msg, name)})
    return event["fingerprint"]


class TestParseStructlogMessage:
    def test_extracts_event_and_context(self):
        parsed = parse_structlog_message(
            "{'event': 'There are no pending orders', 'level': 'info', 'logger': 'backend'}"
        )
        assert parsed is not None
        message, context = parsed
        assert message == "There are no pending orders"
        assert context == {"level": "info", "logger": "backend"}

    def test_accepts_dict_directly(self):
        parsed = parse_structlog_message({"event": "hello", "level": "info"})
        assert parsed == ("hello", {"level": "info"})

    def test_falls_back_to_error_key(self):
        """Celery task failures put the message under 'error', not 'event'."""
        parsed = parse_structlog_message(
            "{'error': 'The read operation timed out', 'exception': 'Traceback...'}"
        )
        assert parsed is not None
        assert parsed[0] == "The read operation timed out"
        assert parsed[1] == {"exception": "Traceback..."}

    def test_returns_none_for_plain_message(self):
        assert parse_structlog_message("plain warning text") is None

    def test_returns_none_for_malformed_dict(self):
        assert parse_structlog_message("{not a dict literal") is None
        assert parse_structlog_message("{'no_message_key': 1}") is None

    def test_returns_none_for_non_dict_literal(self):
        assert parse_structlog_message("{1, 2, 3}") is None


class TestNormalizeForFingerprint:
    def test_replaces_embedded_hex_identifiers(self):
        normalized = normalize_for_fingerprint(
            "queue 'subscription_2a0e7d3ff12743b5843fff837ae51391_offering_"
            "37f4f67fa1ed44118459679ccd50e201_resource' in vhost "
            "'b281fcf8d77c41aa83d7ee61520c3dd8'"
        )
        assert "2a0e7d3ff12743b5843fff837ae51391" not in normalized
        assert normalized.count("<hex>") == 3
        # Surrounding words must survive so distinct errors stay distinct.
        assert "subscription_<hex>_offering_<hex>_resource" in normalized

    def test_replaces_hyphenated_uuid(self):
        assert (
            normalize_for_fingerprint("task 37d34844-42c3-464b-a652-9e0fcc6b4a2e failed")
            == "task <uuid> failed"
        )

    def test_leaves_ordinary_text_alone(self):
        message = "Received an error NOT_FOUND from the broker"
        assert normalize_for_fingerprint(message) == message


class TestBeforeSend:
    def test_subscription_churn_does_not_create_new_groups(self):
        """Subscriptions rotate; the same queue type must stay one Sentry group.

        This is what stops the WALDUR-UT-AGENT groups multiplying over time -
        without it, every recreated subscription mints a fresh issue.
        """
        fingerprints = [
            _fingerprint(_queue_error("2a0e7d3ff12743b5843fff837ae51391", "order")),
            _fingerprint(_queue_error("60e2c6f13d2f4eaa83d1670441a566e8", "order")),
            _fingerprint(_queue_error("78934a7b7468432a840a44cc23fd46ab", "order")),
        ]
        assert len({tuple(f) for f in fingerprints}) == 1

    def test_queue_type_remains_a_grouping_signal(self):
        """A stuck 'order' queue is a different problem from a stuck 'user_role' one."""
        order = _fingerprint(_queue_error("2a0e7d3ff12743b5843fff837ae51391", "order"))
        user_role = _fingerprint(_queue_error("2a0e7d3ff12743b5843fff837ae51391", "user_role"))
        assert order != user_role

    def test_different_errors_keep_separate_fingerprints(self):
        assert _fingerprint("{'event': 'Broker unreachable'}") != _fingerprint(
            "{'event': 'Token rejected'}"
        )

    def test_same_message_from_different_loggers_stays_separate(self):
        assert _fingerprint("{'event': 'boom'}", "a.b") != _fingerprint("{'event': 'boom'}", "c.d")

    def test_title_is_replaced_and_context_preserved(self):
        msg = "{'event': 'Broker unreachable', 'level': 'error', 'logger': 'backend'}"
        event = before_send({"logentry": {"message": msg, "params": []}}, {"log_record": _record(msg)})

        assert event["logentry"]["message"] == "Broker unreachable"
        assert "params" not in event["logentry"]
        assert event["extra"]["level"] == "error"
        assert event["extra"]["logger"] == "backend"

    def test_existing_extra_is_not_clobbered(self):
        msg = "{'event': 'Broker unreachable', 'level': 'error'}"
        event = before_send(
            {"logentry": {"message": msg}, "extra": {"level": "kept"}},
            {"log_record": _record(msg)},
        )
        assert event["extra"]["level"] == "kept"

    def test_plain_log_message_is_untouched(self):
        original = {"logentry": {"message": "plain failure"}}
        event = before_send(original, {"log_record": _record("plain failure")})
        assert event["logentry"]["message"] == "plain failure"
        assert "fingerprint" not in event

    def test_non_log_event_is_untouched(self):
        """Exception events carry no log_record and must keep default grouping."""
        event = before_send({"exception": {"values": []}}, {})
        assert "fingerprint" not in event
