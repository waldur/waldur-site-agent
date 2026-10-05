"""Tests for the get_client base URL handling."""

import pytest

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
