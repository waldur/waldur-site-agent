"""Dated ownership of an address: which resource a person's chat usage bills to, and when.

Chat usage reaches the proxy on Open WebUI's shared key and is recorded under the
person's address, so the only link from a usage row to a resource is the ownership the
plugin records on that address's LiteLLM user. The reporting backend recomputes the
whole month on every pass, which means that link has to answer "who owned this address
*on that day*", not "who owns it now": reading the current owner moved a removed
member's month off the resource's bill, and a moved member's earlier usage onto the new
resource.

So ownership is a list of periods on the user record's metadata, oldest first::

    [{"resource": "<backend id>", "from": "2026-10-01", "to": "2026-10-14"},
     {"resource": "<backend id>", "from": "2026-10-14", "to": None}]

Dates are the proxy's UTC days (``YYYY-MM-DD``), the same days its usage rows are
bucketed by, and both ends are inclusive. ``to: None`` is the open period -- the current
owner, the only one the membership diff and the ownership refusal look at. ``from: None``
is a period with no known start: what a record written before this module existed reads
as, so a current member's usage is attributed exactly as it was.

Usage is bucketed per day, so ownership is too. On the day an address moves, two periods
cover it; the earlier one wins, so the day of a move bills to the resource the person
left.
"""

from __future__ import annotations

import datetime
from typing import Optional

from .client import META_OWNERSHIP, META_RESOURCE, META_USERNAME

PERIOD_RESOURCE = "resource"
PERIOD_FROM = "from"
PERIOD_TO = "to"

# How many calendar months back a released address keeps its record, counting the
# current one. The reporting backend can only be asked about months this window covers
# -- the agent reports ``reporting_periods`` of them, which is at most 12 -- so a period
# that ended before the window can no longer affect any figure it reports.
DEFAULT_RETENTION_MONTHS = 12


def today() -> str:
    """The current UTC day, the calendar the proxy buckets usage rows by."""
    return datetime.datetime.now(tz=datetime.timezone.utc).date().isoformat()


def periods_of(metadata: dict) -> list:
    """Read the ownership periods recorded on a user's metadata, oldest first.

    A record written before periods existed carries only the owner's backend id. It is
    read as one open period with no start, so it attributes every day to that owner --
    which is what it did before. A record with neither is not owned at all.
    """
    raw = metadata.get(META_OWNERSHIP)
    if isinstance(raw, list):
        periods = []
        for item in raw:
            if not isinstance(item, dict) or not item.get(PERIOD_RESOURCE):
                continue
            periods.append(
                {
                    PERIOD_RESOURCE: str(item[PERIOD_RESOURCE]),
                    PERIOD_FROM: item.get(PERIOD_FROM) or None,
                    PERIOD_TO: item.get(PERIOD_TO) or None,
                }
            )
        return periods
    legacy = metadata.get(META_RESOURCE)
    if legacy:
        return [{PERIOD_RESOURCE: str(legacy), PERIOD_FROM: None, PERIOD_TO: None}]
    return []


def current_owner(periods: list) -> Optional[str]:
    """The resource holding the open period, or None when the address is released."""
    for period in periods:
        if period[PERIOD_TO] is None:
            return period[PERIOD_RESOURCE]
    return None


def owner_on(periods: list, day: str) -> Optional[str]:
    """The resource that owned the address on ``day`` (``YYYY-MM-DD``).

    The first period covering the day wins, so on the day of a move -- covered by both
    the period that closed and the one that opened -- the day stays with the resource
    the person left. That is the only direction that never lowers a figure already
    reported: the new resource can only claim after the old one released, so the old
    one has reported that day already, and handing it to the new one would take that
    usage back off its bill on the next pass.
    """
    for period in periods:
        start, end = period[PERIOD_FROM], period[PERIOD_TO]
        if (start is None or start <= day) and (end is None or day <= end):
            return period[PERIOD_RESOURCE]
    return None


def claim(periods: list, resource: str, day: str) -> list:
    """Open a period for ``resource`` from ``day``, unless it already holds one.

    The caller refuses an address another resource still owns before getting here;
    claiming over an open period would leave two owners for every day from now on.
    """
    owner = current_owner(periods)
    if owner == resource:
        return list(periods)
    if owner is not None:
        msg = f"address is still owned by resource {owner}"
        raise ValueError(msg)
    return [*periods, {PERIOD_RESOURCE: resource, PERIOD_FROM: day, PERIOD_TO: None}]


def release(periods: list, resource: str, day: str) -> list:
    """Close ``resource``'s open period on ``day``; periods of other owners stay as they are.

    ``day`` is inclusive: the person had access for part of it, so usage on the day they
    were removed still bills to the resource.
    """
    released = []
    for period in periods:
        if period[PERIOD_TO] is None and period[PERIOD_RESOURCE] == resource:
            period = dict(period, **{PERIOD_TO: day})
        released.append(period)
    return released


def retention_cutoff(day: str, months: int) -> str:
    """First day of the oldest month still reportable, ``months`` months back including this one."""
    current = datetime.date.fromisoformat(day)
    index = current.year * 12 + (current.month - 1) - (max(months, 1) - 1)
    return datetime.date(index // 12, index % 12 + 1, 1).isoformat()


def trim(periods: list, cutoff: str) -> list:
    """Drop closed periods that ended before ``cutoff``; they can bill no reportable day."""
    return [p for p in periods if p[PERIOD_TO] is None or p[PERIOD_TO] >= cutoff]


def expired(periods: list, cutoff: str) -> bool:
    """True when the address is released and none of its periods reaches ``cutoff``.

    Such a record can no longer move any reportable month's figure, so it can go. A
    record with no periods at all is not one this plugin released -- an unmanaged user,
    or one an admin made by hand -- and is never expired.
    """
    return bool(periods) and not trim(periods, cutoff)


def stamp(metadata: dict, periods: list, username: Optional[str] = None) -> dict:
    """The metadata to write back: the record's own keys, with ownership replaced.

    ``/user/update`` replaces ``metadata`` wholesale rather than merging it, so the keys
    this plugin does not own are carried over here or they would be lost on every write.

    ``waldur_resource`` is kept as a mirror of the current owner, ``None`` once released.
    An agent from before this module reads only that key, and during a rolling upgrade it
    must not keep attributing a released address to the resource that let it go. The
    mirror covers reading only: an older agent that *writes* the record replaces the
    periods with its own two keys, and the record then reads as an open period with no
    start. Upgrade every agent sharing the proxy together.
    """
    stamped = dict(metadata)
    stamped[META_OWNERSHIP] = [dict(period) for period in periods]
    stamped[META_RESOURCE] = current_owner(periods)
    if username is not None:
        stamped[META_USERNAME] = username
    return stamped
