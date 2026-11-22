"""Turn a level -- what a resource holds now -- into the period's billed total.

Some backends can say how much a resource holds at this moment (stored gigabytes,
say) but keep no history of it, so they cannot report how much it held over the
month, which is what a GB-day price bills. The agent then accrues the level the way
Waldur accrues OpenStack usage (``_accumulate_hourly_usage`` in mastermind): each
pass adds a level times the time since the previous pass to the month's total.

The interval is billed at the level the previous pass measured, which the
checkpoint records. A pass that changes nothing can then skip writing: the next
one bills the longer interval at the same level, so nothing is lost and nothing is
billed back. (Billing the interval at the level measured now would charge a bucket
filled after days of idling for all of those days.)

The agent keeps no state of its own for this. The total and the moment it is
accrued to are written into the usage row's description, which Waldur stores
verbatim and returns, so a restart, a retry or a second agent instance reads the
same checkpoint. The exact total travels in the description too: the usage field
holds two decimals, and re-reading the rounded figure every pass would drop up to
half a cent of each increment for good.
"""

import datetime
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

logger = logging.getLogger(__name__)

# Waldur keeps usage to two decimals; a stored figure within half a unit of the
# description's exact total is the same figure.
_STORED_PRECISION = Decimal("0.005")

_CHECKPOINT = re.compile(
    r"Accrued (?P<total>\d+(?:\.\d+)?) to "
    r"(?P<at>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))"
    r"(?:, at a level of (?P<level>[0-9.eE+-]+))?"
)


@dataclass(frozen=True)
class Checkpoint:
    """How much was accrued, and up to which moment."""

    total: Decimal
    at: datetime.datetime
    # The level measured at that moment; None in a checkpoint written without one.
    level: Optional[Decimal] = None


@dataclass(frozen=True)
class Accrual:
    """The outcome of one pass for one component."""

    total: Decimal
    description: str
    elapsed_seconds: float
    capped_seconds: float


def utcnow() -> datetime.datetime:
    """The moment a pass accrues to; a seam for tests."""
    return datetime.datetime.now(datetime.timezone.utc)


def parse_checkpoint(description: object) -> Optional[Checkpoint]:
    """Read a checkpoint from a usage row's description, or None.

    Anything that does not parse -- an empty description, one a person wrote, one
    from a different reporting source -- is no checkpoint rather than an error.
    """
    if not isinstance(description, str):
        return None
    match = _CHECKPOINT.search(description)
    if match is None:
        return None
    try:
        total = Decimal(match.group("total"))
        at = datetime.datetime.fromisoformat(match.group("at").replace("Z", "+00:00"))
        level = Decimal(match.group("level")) if match.group("level") else None
    except (InvalidOperation, ValueError):
        return None
    if at.tzinfo is None or (level is not None and (not level.is_finite() or level < 0)):
        return None
    return Checkpoint(total=total, at=at, level=level)


def format_checkpoint(total: Decimal, at: datetime.datetime, level: float) -> str:
    """The description that records a checkpoint, readable by a person too."""
    stamp = at.astimezone(datetime.timezone.utc).isoformat(timespec="seconds")
    stamp = stamp.replace("+00:00", "Z")
    exact = total.quantize(Decimal("0.000001"))
    # repr keeps every digit of the level, which the next pass bills at.
    return f"Accrued {exact} to {stamp}, at a level of {float(level)!r} at that moment"


def latest_checkpoint(descriptions: list) -> Optional[Checkpoint]:
    """The newest checkpoint among a component's rows for the period.

    A plan switch inside the month splits it into one row per plan, and Waldur
    writes the description only to the current plan's row.
    """
    checkpoints = [cp for cp in (parse_checkpoint(d) for d in descriptions) if cp]
    return max(checkpoints, key=lambda cp: cp.at) if checkpoints else None


def accrue(
    *,
    level: float,
    unit_seconds: int,
    stored_total: Optional[float],
    checkpoint: Optional[Checkpoint],
    start_without_checkpoint: datetime.datetime,
    until: datetime.datetime,
    max_gap_seconds: int,
    opening_level: Optional[float] = None,
) -> Accrual:
    """Add the level held over the time since the checkpoint to the period's total.

    The interval is billed at the checkpoint's level, the one the previous pass
    measured; ``level`` is recorded for the next interval. Without a checkpoint
    there is no earlier level, so the first interval is billed at ``level``.

    Args:
        level: What the resource holds now, in component units (GB, say).
        unit_seconds: Seconds in one billed unit of time (86400 for a GB-day).
        stored_total: The period's figure Waldur holds now, or None if no row yet.
        checkpoint: The previous pass's checkpoint for this period, if any.
        start_without_checkpoint: Where accrual starts when there is no checkpoint.
        until: The moment accrued to.
        max_gap_seconds: The longest interval one pass may bill. A longer gap -- an
            agent outage -- is billed only this far, since the level may have changed
            anywhere in it.
        opening_level: Without a checkpoint, the level the interval was held at, if
            known from elsewhere (the previous month's last checkpoint).
    """
    held = Decimal(str(opening_level if opening_level is not None else level))
    if checkpoint is not None:
        start = checkpoint.at
        base = checkpoint.total
        if checkpoint.level is not None:
            held = checkpoint.level
        if stored_total is not None and abs(base - Decimal(str(stored_total))) > _STORED_PRECISION:
            # Someone changed the figure in Waldur since; theirs is the one billed.
            logger.warning(
                "The usage Waldur holds (%s) differs from the accrued total %s; "
                "continuing from Waldur's figure",
                stored_total,
                base,
            )
            base = Decimal(str(stored_total))
    else:
        start = start_without_checkpoint
        base = Decimal(str(stored_total)) if stored_total is not None else Decimal(0)

    elapsed = max(0.0, (until - start).total_seconds())
    capped = 0.0
    if elapsed > max_gap_seconds:
        capped = elapsed - max_gap_seconds
        elapsed = float(max_gap_seconds)

    increment = held * Decimal(str(elapsed)) / Decimal(unit_seconds)
    total = base + increment
    # Never record a moment before the checkpoint's own (a clock behind another
    # instance's): the next pass would bill that stretch a second time.
    recorded_at = until if checkpoint is None else max(until, checkpoint.at)
    return Accrual(
        total=total,
        description=format_checkpoint(total, recorded_at, level),
        elapsed_seconds=elapsed,
        capped_seconds=capped,
    )
