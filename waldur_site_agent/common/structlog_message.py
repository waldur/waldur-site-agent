"""Helpers for reading structlog-rendered stdlib log records.

structlog is configured with ``ProcessorFormatter.wrap_for_formatter`` as the
final processor, so ``LogRecord.msg`` holds the structlog event *dict* and
rendering is deferred to each handler's ``ProcessorFormatter``. Any consumer
that reads ``record.getMessage()`` directly - the buffered log handler, or
sentry-sdk's logging integration - therefore sees ``str(event_dict)`` with all
of the variable data baked into one opaque string.

These helpers recover the human-readable message and the surrounding context
from such a record, and derive a grouping key that is stable across runs.
"""

import ast
import re
from typing import Optional

# Keys that may carry the human-readable message, in order of preference.
# "event" is structlog's default; celery task failures land under "error".
_MESSAGE_KEYS = ("event", "message", "error")

# Volatile tokens that make otherwise identical messages look unique. Bounded by
# non-alphanumerics rather than \b so that identifiers embedded in longer names
# ("subscription_<hex32>_offering_<hex32>_resource") are matched too.
_VOLATILE_PATTERNS = (
    (
        re.compile(
            r"(?<![0-9a-zA-Z])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
            r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?![0-9a-zA-Z])"
        ),
        "<uuid>",
    ),
    (re.compile(r"(?<![0-9a-zA-Z])[0-9a-fA-F]{16,}(?![0-9a-zA-Z])"), "<hex>"),
)


def parse_structlog_message(msg: object) -> Optional[tuple[str, dict]]:
    """Recover the message and context from a structlog-rendered log record.

    Args:
        msg: The ``LogRecord.msg`` value - either the event dict itself, or the
            string repr of one, or an ordinary log message.

    Returns:
        A ``(message, context)`` pair when ``msg`` is a structlog event dict,
        where ``context`` holds the remaining keys. ``None`` when ``msg`` is an
        ordinary message that needs no unwrapping.
    """
    if isinstance(msg, dict):
        data = msg
    elif isinstance(msg, str):
        stripped = msg.strip()
        # Cheap guard so ast.literal_eval is not attempted on every log line.
        if not (stripped.startswith("{") and stripped.endswith("}")):
            return None
        try:
            data = ast.literal_eval(stripped)
        except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
            return None
        if not isinstance(data, dict):
            return None
    else:
        return None

    for key in _MESSAGE_KEYS:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value, {k: v for k, v in data.items() if k != key}

    return None


def normalize_for_fingerprint(message: str) -> str:
    """Replace volatile identifiers so equivalent messages share a group key.

    Args:
        message: The human-readable log message.

    Returns:
        The message with UUIDs and long hex identifiers replaced by
        placeholders.
    """
    for pattern, placeholder in _VOLATILE_PATTERNS:
        message = pattern.sub(placeholder, message)
    return message
