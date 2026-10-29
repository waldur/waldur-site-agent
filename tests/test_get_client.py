"""Tests for the get_client base URL handling."""

import pytest

from waldur_site_agent.common import static_token_prefix
from waldur_site_agent.common.utils import get_client


@pytest.mark.parametrize(
    ("api_url", "expected_base_url"),
    [
        ("https://waldur.example.com/api/", "https://waldur.example.com"),
        ("https://waldur.example.com/api", "https://waldur.example.com"),
        ("http://waldur-mastermind-api", "http://waldur-mastermind-api"),
        ("http://waldur-mastermind-api/", "http://waldur-mastermind-api"),
        ("http://localhost:8081/api/", "http://localhost:8081"),
    ],
)
def test_get_client_strips_only_api_suffix(api_url, expected_base_url):
    """The /api suffix must be stripped as a path segment, not as a character set."""
    client = get_client(api_url, "token")

    assert client._base_url == expected_base_url


@pytest.mark.parametrize(
    ("token", "expected_header"),
    [
        ("0123456789abcdef", "Token 0123456789abcdef"),
        ("w_1789000000_abc", "Bearer w_1789000000_abc"),
    ],
)
def test_get_client_derives_prefix_from_token(token, expected_header):
    """A personal access token (w_...) is sent as Bearer, an API token as Token."""
    client = get_client("https://waldur.example.com/api/", token)

    assert client.get_httpx_client().headers["Authorization"] == expected_header


def test_get_client_explicit_prefix_wins():
    client = get_client("https://waldur.example.com/api/", "jwt", token_prefix="Bearer")

    assert client.get_httpx_client().headers["Authorization"] == "Bearer jwt"


def test_get_client_explicit_empty_prefix_is_kept():
    client = get_client("https://waldur.example.com/api/", "raw", token_prefix="")

    assert client.get_httpx_client().headers["Authorization"] == "raw"


@pytest.mark.parametrize(
    ("token", "expected"),
    [("w_1789000000_abc", "Bearer"), ("0123abcd", "Token"), ("", "Token"), (None, "Token")],
)
def test_static_token_prefix(token, expected):
    assert static_token_prefix(token) == expected
