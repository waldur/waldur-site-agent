"""Sentry event processing for structlog-rendered log records.

sentry-sdk's logging integration takes the issue title straight from the log
record's message. Because structlog defers rendering to the handler, that
message is ``str(event_dict)`` - so every distinct queue name, UUID or embedded
traceback mints a brand new Sentry issue group for what is one underlying bug.

``before_send`` below restores the readable message, moves the remaining
structlog keys into the event's extra data, and pins a grouping fingerprint that
ignores volatile identifiers.
"""

import re
from typing import Any, Optional

from .structlog_message import normalize_for_fingerprint, parse_structlog_message

# Secrets that travel as query parameters rather than in a request body. croit's
# create-key endpoint is the reason this exists: it takes the S3 secret as
# ?secretKey=, so any component that captures a URL captures a live credential.
# A denylist rather than an allowlist because an allowlist silently stops covering
# a parameter someone adds later.
# Hyphenated spellings are not stylistic variants to be tidied away: RadosGW's
# Admin Ops API takes the S3 secret as a query parameter literally named
# "secret-key", so omitting it leaks a live credential into any log line or
# Sentry event that carries the URL.
_SECRET_QUERY_PARAMS = (
    "secretKey",
    "secret_key",
    "secret-key",
    "api_key",
    "apiKey",
    "api-key",
    "token",
    "password",
)
_SECRET_QUERY_RE = re.compile(
    r"\b(" + "|".join(_SECRET_QUERY_PARAMS) + r")=[^&\s\"']+",
    re.IGNORECASE,
)


def scrub_secret_query_params(text: str) -> str:
    """Replace the value of any secret-bearing query parameter with a placeholder."""
    return _SECRET_QUERY_RE.sub(r"\1=[REDACTED]", text)


def before_breadcrumb(crumb: dict, hint: dict) -> Optional[dict]:
    """Strip secret query parameters from a breadcrumb before it leaves the process.

    sentry-sdk's logging integration turns every INFO record into a breadcrumb, so
    a transport that logs request URLs exports credentials on the next captured
    event. The httpx logger is quieted at source (see ``configure_logger``); this
    catches anything that reaches a breadcrumb by another route.
    """
    del hint
    message = crumb.get("message")
    if isinstance(message, str):
        crumb["message"] = scrub_secret_query_params(message)
    return crumb


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
