"""Sentry event processing for structlog-rendered log records.

sentry-sdk's logging integration takes the issue title straight from the log
record's message. Because structlog defers rendering to the handler, that
message is ``str(event_dict)`` - so every distinct queue name, UUID or embedded
traceback mints a brand new Sentry issue group for what is one underlying bug.

``before_send`` below restores the readable message, moves the remaining
structlog keys into the event's extra data, and pins a grouping fingerprint that
ignores volatile identifiers.
"""

from typing import Any

from .structlog_message import normalize_for_fingerprint, parse_structlog_message


def before_send(event: dict, hint: dict) -> dict:
    """Normalise structlog-rendered log events before they are sent to Sentry.

    Args:
        event: The Sentry event payload.
        hint: Sentry's hint mapping; carries ``log_record`` for log-derived
            events.

    Returns:
        The event, modified in place when it originated from a structlog record.
    """
    record: Any = hint.get("log_record")
    if record is None:
        return event

    parsed = parse_structlog_message(getattr(record, "msg", None))
    if parsed is None:
        return event

    message, context = parsed

    logentry = event.get("logentry")
    if isinstance(logentry, dict):
        logentry["message"] = message
        # Params belong to the dict repr we just discarded.
        logentry.pop("params", None)

    if context:
        extra = event.setdefault("extra", {})
        if isinstance(extra, dict):
            for key, value in context.items():
                extra.setdefault(key, value)

    event["fingerprint"] = [
        getattr(record, "name", "") or "",
        normalize_for_fingerprint(message),
    ]
    return event
