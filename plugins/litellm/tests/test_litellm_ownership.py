"""Tests for dated address ownership (pure functions, no proxy)."""

from __future__ import annotations

import pytest
from waldur_site_agent_litellm import ownership


@pytest.mark.parametrize(
    ("day", "months", "cutoff"),
    [
        ("2026-10-15", 1, "2026-10-01"),
        ("2026-10-15", 2, "2026-09-01"),
        ("2026-01-31", 2, "2025-12-01"),
        ("2026-03-01", 12, "2025-04-01"),
        ("2026-12-31", 12, "2026-01-01"),
    ],
)
def test_the_retention_window_starts_on_the_first_of_its_oldest_month(day, months, cutoff):
    assert ownership.retention_cutoff(day, months) == cutoff


def test_malformed_periods_are_skipped_rather_than_read_as_owners():
    metadata = {
        "waldur_ownership": [
            "not a period",
            {"from": "2026-10-01", "to": None},
            {"resource": "abc", "from": "2026-10-01", "to": ""},
        ]
    }

    assert ownership.periods_of(metadata) == [
        {"resource": "abc", "from": "2026-10-01", "to": None}
    ]


def test_an_empty_period_list_means_released_even_if_the_mirror_still_names_an_owner():
    # The list is authoritative once present; the mirror is only for older agents.
    assert ownership.periods_of({"waldur_ownership": [], "waldur_resource": "abc"}) == []


def test_claiming_over_another_open_period_is_an_error():
    periods = [{"resource": "abc", "from": None, "to": None}]
    with pytest.raises(ValueError, match="abc"):
        ownership.claim(periods, "other", "2026-10-15")
