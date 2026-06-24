"""Unit tests for OIDC client_credentials authentication helpers."""

from collections.abc import Iterator
from unittest import mock

import pytest

from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering


@pytest.fixture(autouse=True)
def _clear_oidc_cache() -> Iterator[None]:
    """Ensure the module-level token cache does not leak between tests."""
    utils._OIDC_TOKEN_CACHE.clear()
    yield
    utils._OIDC_TOKEN_CACHE.clear()


def _mock_httpx_client(response: mock.MagicMock) -> mock.MagicMock:
    """Build a mock httpx.Client usable as a context manager."""
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    client.post.return_value = response
    return client


def _response(json_data: dict) -> mock.MagicMock:
    resp = mock.MagicMock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class TestFetchOidcToken:
    """Tests for fetch_oidc_token (HTTP, caching, error handling)."""

    def test_returns_access_token_and_posts_client_credentials(self):
        client = _mock_httpx_client(_response({"access_token": "jwt-abc", "expires_in": 300}))
        with mock.patch.object(utils.httpx, "Client", return_value=client) as mock_client_cls:
            result = utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")

        assert result == "jwt-abc"
        _, post_kwargs = client.post.call_args
        assert post_kwargs["data"] == {
            "grant_type": "client_credentials",
            "client_id": "cid",
            "client_secret": "secret",
        }
        _, client_kwargs = mock_client_cls.call_args
        assert client_kwargs["verify"] is True
        assert client_kwargs["proxy"] is None

    def test_passes_verify_ssl_and_proxy_to_httpx(self):
        client = _mock_httpx_client(_response({"access_token": "jwt", "expires_in": 300}))
        with mock.patch.object(utils.httpx, "Client", return_value=client) as mock_client_cls:
            utils.fetch_oidc_token(
                "https://idp.example.com/token",
                "cid",
                "secret",
                verify_ssl=False,
                proxy="socks5://localhost:1080",
            )

        _, client_kwargs = mock_client_cls.call_args
        assert client_kwargs["verify"] is False
        assert client_kwargs["proxy"] == "socks5://localhost:1080"

    def test_caches_token_and_does_not_refetch(self):
        client = _mock_httpx_client(_response({"access_token": "jwt", "expires_in": 300}))
        with mock.patch.object(utils.httpx, "Client", return_value=client) as mock_client_cls:
            first = utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")
            second = utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")

        assert first == second == "jwt"
        assert mock_client_cls.call_count == 1  # second call served from cache

    def test_refetches_when_token_near_expiry(self):
        # expires_in (1s) is within the 30s refresh margin -> treated as expired.
        client = _mock_httpx_client(_response({"access_token": "jwt", "expires_in": 1}))
        with mock.patch.object(utils.httpx, "Client", return_value=client) as mock_client_cls:
            utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")
            utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")

        assert mock_client_cls.call_count > 1  # refetched, not served from cache

    def test_raises_when_no_access_token(self):
        client = _mock_httpx_client(_response({"error": "invalid_client"}))
        with (
            mock.patch.object(utils.httpx, "Client", return_value=client),
            pytest.raises(utils.OIDCAuthError),
        ):
            utils.fetch_oidc_token("https://idp.example.com/token", "cid", "secret")


class TestGetClientForOffering:
    """Tests for branch selection (static token vs OIDC) and prefix mapping."""

    BASE = {  # noqa: RUF012
        "name": "t",
        "waldur_api_url": "https://w.example.com/api/",
        "waldur_offering_uuid": "12345678-1234-1234-1234-123456789abc",
        "backend_type": "test",
    }

    def test_static_token_uses_token_prefix_without_fetch(self):
        offering = Offering(**self.BASE, waldur_api_token="static")  # noqa: S106
        with (
            mock.patch.object(utils, "fetch_oidc_token") as mock_fetch,
            mock.patch.object(utils, "get_client") as mock_get_client,
        ):
            utils.get_client_for_offering(offering, "agent")

        mock_fetch.assert_not_called()
        args, _ = mock_get_client.call_args
        # positional: url, token, agent_header, verify_ssl, proxy, token_prefix
        assert args[1] == "static"
        assert args[5] == "Token"

    def test_oidc_uses_bearer_prefix_and_forwards_verify_and_proxy(self):
        offering = Offering(
            **self.BASE,
            waldur_api_token="",
            oidc_token_url="https://idp.example.com/token",  # noqa: S106
            oidc_client_id="cid",
            oidc_client_secret="secret",  # noqa: S106
            verify_ssl=False,
        )
        with (
            mock.patch.object(utils, "fetch_oidc_token", return_value="jwt") as mock_fetch,
            mock.patch.object(utils, "get_client") as mock_get_client,
        ):
            utils.get_client_for_offering(offering, "agent", proxy="http://proxy:3128")

        mock_fetch.assert_called_once_with(
            "https://idp.example.com/token",
            "cid",
            "secret",
            False,
            "http://proxy:3128",
        )
        args, _ = mock_get_client.call_args
        assert args[1] == "jwt"
        assert args[5] == "Bearer"
