"""Spending limits per period, and usage per day, as Waldur exposes them.

A Waldur with spending limits (waldur/waldur-mastermind#789, #817) lets a customer
cap a usage-based component per calendar period in UTC -- day, week, month,
quarter, year -- or for all time (``total``, which never resets), on a resource
(``Resource.spending_limits``) and on each of its API keys (``spending_limits``
on the key), both ``{component: {period: amount}}``. Day and week limits need
usage per UTC day, which ``set_usage`` and a key's ``report_usage`` take as
``daily_usages``.

A Waldur that predates them sends none of these fields and refuses nothing for
their absence, so the agent reports month totals to it exactly as before. Which
Waldur it is talking to is read off the responses: every offering component of a
Waldur with per-day usage carries ``spending_limit_periods``, and every API key
carries ``spending_limits``, null or not.

The fields are read whether or not the generated client knows them: an older
``waldur-api-client`` keeps them in ``additional_properties``.
"""

from __future__ import annotations

import datetime
from typing import Any, Optional

from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.resource_field_enum import ResourceFieldEnum
from waldur_api_client.types import UNSET, Unset

# The periods a limit can be set for, shortest first, as Waldur names them. ``total``
# never resets (waldur/waldur-mastermind#841).
DAY = "day"
WEEK = "week"
MONTH = "month"
QUARTER = "quarter"
YEAR = "year"
TOTAL = "total"
PERIODS = (DAY, WEEK, MONTH, QUARTER, YEAR, TOTAL)


def generated_field(model: object, name: str) -> Any:  # noqa: ANN401 - any JSON value
    """A field of a generated model as plain JSON, or ``UNSET`` when Waldur did not send it.

    A client that knows the field sets an attribute (``UNSET`` when absent); one that
    predates it keeps the value in ``additional_properties``. ``UNSET`` and ``None``
    differ: the first is a Waldur without the field, the second one that sent null.
    """
    value = getattr(model, name, UNSET)
    if isinstance(value, Unset):
        extra = getattr(model, "additional_properties", None)
        value = extra.get(name, UNSET) if isinstance(extra, dict) else UNSET
    to_dict = getattr(value, "to_dict", None)
    return to_dict() if callable(to_dict) else value


def _limits(value: object) -> dict:
    """``{component: {period: amount}}`` with the empty and malformed parts left out."""
    if not isinstance(value, dict):
        return {}
    result: dict = {}
    for component, periods in value.items():
        if not isinstance(periods, dict):
            continue
        amounts = {
            period: float(amount)
            for period, amount in periods.items()
            if period in PERIODS and isinstance(amount, (int, float, str)) and amount != ""
        }
        if amounts:
            result[str(component)] = amounts
    return result


class _ExtraField:
    """A ``field`` selection item for a field the generated enum predates.

    The generated listing writes each item's ``value`` into the query, and the field
    comes back in ``additional_properties``, where ``generated_field`` reads it.
    """

    def __init__(self, value: str) -> None:
        self.value = value


def resource_fields() -> list:
    """The resource field that carries spending limits, as a ``field`` selection item.

    A Waldur that predates the field ignores a selection naming it.
    """
    member = getattr(ResourceFieldEnum, "SPENDING_LIMITS", None)
    return [member if member is not None else _ExtraField("spending_limits")]


def resource_spending_limits(waldur_resource: object) -> Optional[dict]:
    """The resource's spending limits; None when the resource came without the field.

    It comes without when it was listed with a ``field`` selection that leaves it out
    -- a client that predates the field cannot ask for it -- and from a Waldur that
    predates spending limits.
    """
    value = generated_field(waldur_resource, "spending_limits")
    return None if isinstance(value, Unset) else _limits(value)


def key_spending_limits(value: object) -> Optional[dict]:
    """A key's spending limits from a command's ``spending_limits``.

    ``None`` when Waldur sent none: a Waldur that predates per-period key limits, whose
    command carries only the flat month ``limits``, or a key with no limits at all.
    """
    return None if value is None or isinstance(value, Unset) else _limits(value)


def accepts_daily_usage(waldur_components: Optional[list]) -> bool:
    """Whether the offering's Waldur takes usage per day (``daily_usages``)."""
    return any(
        not isinstance(generated_field(component, "spending_limit_periods"), Unset)
        for component in waldur_components or []
    )


def key_accepts_daily_usage(key: object) -> bool:
    """Whether a key's Waldur takes its usage per day, read off its listing row."""
    return not isinstance(generated_field(key, "spending_limits"), Unset)


def usage_based_types(waldur_components: Optional[list]) -> set:
    """The component types Waldur bills by usage: the only ones it takes per day."""
    return {
        component.type_
        for component in waldur_components or []
        if isinstance(component.type_, str) and component.billing_type == BillingTypeEnum.USAGE
    }


def utc_today() -> datetime.date:
    """Today in UTC, the calendar Waldur counts days in."""
    return datetime.datetime.now(tz=datetime.timezone.utc).date()


def fill_days(
    daily: dict, year: int, month: int, component_types: set, today: datetime.date
) -> dict:
    """``daily`` with every day of the month up to ``today`` and every type, zeros where absent.

    Waldur replaces the amount of a day reported again and keeps a day left out, so a
    day whose usage has moved elsewhere since -- chat moving with a reassigned key --
    has to be sent as zero to stop counting.
    """
    filled: dict = {}
    day = datetime.date(year, month, 1)
    while day.month == month and day <= today:
        usages = daily.get(day.isoformat(), {})
        filled[day.isoformat()] = {
            component: float(usages.get(component, 0)) for component in component_types
        }
        day += datetime.timedelta(days=1)
    return filled


def daily_usage_items(daily: dict) -> list:
    """``daily_usages`` entries for ``{date: {component: usage}}``, two decimals as Waldur keeps.

    Rounded cumulatively: each day is the rounded total up to it, less the rounded total
    up to the day before. Waldur's month is the sum of the days, which so equals the
    rounded month total a monthly report sent -- rounding each day alone would lose
    sub-cent days altogether.
    """
    items = []
    running: dict = {}
    for date, usages in sorted(daily.items()):
        for component, amount in sorted(usages.items()):
            before = running.get(component, 0.0)
            running[component] = before + float(amount)
            day = round(running[component], 2) - round(before, 2)
            items.append({"type": component, "date": date, "amount": f"{day:.2f}"})
    return items


def month_totals(daily: dict) -> dict:
    """``{component: usage}`` summed over ``{date: {component: usage}}``."""
    totals: dict = {}
    for usages in daily.values():
        for component, amount in usages.items():
            totals[component] = totals.get(component, 0.0) + float(amount)
    return totals
