"""Tests for chat account provenance and the derived managed password."""

from __future__ import annotations

import datetime

import pytest
from waldur_site_agent_litellm import chat_accounts

SECRET = "ChangeMe-123"


def test_ascii_addresses_canonicalise_exactly_as_before():
    # strip().lower() was the old form; an existing record must not move.
    assert chat_accounts.canonical_email(" Ada@Example.COM ") == "ada@example.com"


def test_compatibility_characters_fold_onto_the_address_they_read_as():
    fullwidth = "ａda@example.com"  # FULLWIDTH LATIN SMALL LETTER A
    assert chat_accounts.canonical_email(fullwidth) == "ada@example.com"
    assert chat_accounts.canonical_email("STRAßE@example.com") == "strasse@example.com"


def test_the_derived_password_is_stable_per_address_and_differs_between_people():
    ada = chat_accounts.derived_password(SECRET, "ada@example.com")

    assert ada == chat_accounts.derived_password(SECRET, " ADA@example.com")
    assert ada != chat_accounts.derived_password(SECRET, "bob@example.com")
    assert ada != chat_accounts.derived_password("another-secret", "ada@example.com")
    assert SECRET not in ada


def test_the_derived_password_meets_open_webuis_default_complexity_rule():
    password = chat_accounts.derived_password(SECRET, "ada@example.com")

    assert len(password) >= 8
    assert any(c.isupper() for c in password)
    assert any(c.islower() for c in password)
    assert any(c.isdigit() for c in password)
    assert any(not c.isalnum() for c in password)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("2026-11-14", 1_794_614_400),
        (datetime.date(2026, 11, 14), 1_794_614_400),
        ("2026-11-14T12:00:00", 1_794_657_600),
        ("2026-11-14T12:00:00+02:00", 1_794_650_400),
        ("2026-11-14T12:00:00Z", 1_794_657_600),  # Python < 3.11 rejects "Z" natively
    ],
)
def test_the_cutoff_reads_dates_and_datetimes_as_utc(value, expected):
    assert chat_accounts.parse_cutoff(value) == expected


def test_an_unreadable_cutoff_raises():
    with pytest.raises(ValueError):
        chat_accounts.parse_cutoff("next tuesday")


@pytest.mark.parametrize(
    ("account", "backed"),
    [
        ({"oauth": {"oidc": {"sub": "f00"}}}, True),
        ({"oauth_sub": "oidc@f00"}, True),
        ({"scim": {"externalId": "f00"}}, True),
        ({"oauth": None}, False),
        ({}, False),
    ],
)
def test_only_an_account_from_the_identity_provider_is_identity_backed(account, backed):
    assert chat_accounts.is_identity_backed(account) is backed


@pytest.mark.parametrize(
    ("account", "older"),
    [
        ({"created_at": 1_700_000_000}, True),
        ({"created_at": 1_800_000_000}, False),
        ({"created_at": "garbage"}, False),
        ({}, False),
    ],
)
def test_an_account_is_older_than_the_cutoff_only_by_its_creation_time(account, older):
    assert chat_accounts.created_before(account, 1_794_614_400) is older


def test_no_cutoff_makes_no_account_old_enough():
    assert chat_accounts.created_before({"created_at": 1}, None) is False


def test_the_cli_prints_each_persons_password(monkeypatch, capsys):
    monkeypatch.setenv(chat_accounts.SECRET_ENV, SECRET)

    assert chat_accounts.main(["Ada@Example.com", "bob@example.com"]) == 0

    lines = capsys.readouterr().out.splitlines()
    assert lines == [
        f"ada@example.com\t{chat_accounts.derived_password(SECRET, 'ada@example.com')}",
        f"bob@example.com\t{chat_accounts.derived_password(SECRET, 'bob@example.com')}",
    ]
