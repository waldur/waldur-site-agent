"""Tests for CroitClient."""

import pytest
import httpx
from unittest.mock import Mock, patch

from waldur_site_agent_ceph_s3.clients.croit import CroitClient
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CroitS3GraphNotFoundError,
)


class TestCroitClient:
    """The croit statistics client: authentication, transport and errors."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        # A token, not a password: these tests script the transport call by call,
        # and a password would add a login request in front of each.
        return CroitClient(
            api_url="https://test.example.com",
            token="test_token",
            verify_ssl=False,
        )

    @pytest.fixture
    def mock_response(self):
        """Create mock response."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {}
        response.text = ""
        response.content = b""
        response.raise_for_status.return_value = None
        return response

    def test_client_initialization(self):
        """Test client initialization."""
        client = CroitClient(
            api_url="https://api.croit.io/",
            username="admin",
            password="secret",
            verify_ssl=True,
            timeout=60,
        )

        assert client.api_url == "https://api.croit.io/api"
        assert client.username == "admin"
        assert client.password == "secret"
        assert client.verify_ssl is True
        assert client.timeout == 60

    @patch("httpx.Client.request")
    def test_a_rejected_token_does_not_blame_a_user_called_none(self, mock_request):
        """Token auth leaves username unset, so the message must not name a user.

        This string reaches the Waldur order, and "Authentication failed for user
        None" tells an operator nothing about which credential was rejected -- nor
        that an expired token is the likeliest reason.
        """
        mock_response = Mock(status_code=401, text="")
        mock_request.return_value = mock_response
        client = CroitClient(api_url="https://test.example.com", token="t")

        with pytest.raises(CephS3AuthenticationError, match="API token"):
            client.get_user_storage_series("waldur-u", 0, 60)

    @patch("httpx.Client.request")
    def test_a_rejected_basic_auth_still_names_the_user(self, mock_request):
        """The username is not a secret, and it is the useful half under basic auth."""
        mock_response = Mock(status_code=401, text="")
        mock_request.return_value = mock_response
        client = CroitClient(
            api_url="https://test.example.com", username="admin", password="secret"
        )

        with pytest.raises(CephS3AuthenticationError, match="admin"):
            client.get_user_storage_series("waldur-u", 0, 60)

    @patch("httpx.Client.request")
    def test_authentication_error(self, mock_request, client):
        """Test authentication error handling."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3AuthenticationError):
            client.get_user_storage_series("waldur-u", 0, 60)

    @patch("waldur_site_agent_ceph_s3.clients.base.time.sleep")
    @patch("httpx.Client.request")
    def test_api_error(self, mock_request, mock_sleep, client):
        """Test general API error handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3APIError):
            client.get_user_storage_series("waldur-u", 0, 60)

        # 500 is retried before giving up: 1 initial attempt + 3 retries
        assert mock_request.call_count == 4

    @patch("waldur_site_agent_ceph_s3.clients.base.time.sleep")
    @patch("httpx.Client.request")
    def test_api_error_message_is_unwrapped(self, mock_request, mock_sleep, client):
        """Croit's own message reaches the caller without the JSON envelope."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = '{"code":500,"message":"Unable to remove user with buckets."}'
        mock_request.return_value = mock_response

        with pytest.raises(CephS3APIError) as exc_info:
            client.get_user_storage_series("waldur-u", 0, 60)

        assert str(exc_info.value) == "Unable to remove user with buckets."

    @patch("httpx.Client.request")
    def test_api_error_falls_back_to_status_and_body(self, mock_request, client):
        """A body without a usable message keeps the status code for context."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "<html>Bad Request</html>"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3APIError) as exc_info:
            client.get_user_storage_series("waldur-u", 0, 60)

        assert str(exc_info.value) == "API error 400: <html>Bad Request</html>"

    @patch("waldur_site_agent_ceph_s3.clients.base.time.sleep")
    @patch("httpx.Client.request")
    def test_transient_server_error_retried(
        self, mock_request, mock_sleep, client, mock_response
    ):
        """Test that transient 5xx responses are retried until success."""
        error_response = Mock()
        error_response.status_code = 503
        error_response.text = "Service Unavailable"
        mock_response.json.return_value = {"axis1": {"graphs": []}}
        mock_request.side_effect = [error_response, mock_response]

        result = client.get_user_storage_series("waldur-u", 0, 60)

        assert result == []
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("httpx.Client.request")
    def test_timeout_error(self, mock_request, client):
        """Test timeout error handling."""
        mock_request.side_effect = httpx.TimeoutException("Request timed out")

        with pytest.raises(CephS3APIError, match="Request timeout"):
            client.get_user_storage_series("waldur-u", 0, 60)

    @patch("httpx.Client.request")
    def test_connection_error(self, mock_request, client):
        """Test connection error handling."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        with pytest.raises(CephS3APIError, match="Connection error"):
            client.get_user_storage_series("waldur-u", 0, 60)

    def test_get_user_storage_series(self, client, mock_response):
        """Test the storage time series query that GB-day billing integrates."""
        mock_response.json.return_value = {
            "name": "S3 User Data",
            "axis1": {
                "unit": "BYTES",
                "graphs": [{"datapoints": [{"t": 1785542400, "v": 15728640.0}]}],
            },
        }

        with patch.object(client, "_request", return_value=mock_response) as request:
            points = client.get_user_storage_series(
                "waldur_u", 1785542400, 1785542400 + 86400
            )

        # A day fits one window: one request, at native resolution.
        request.assert_called_once_with(
            "GET",
            "/stats",
            params={
                "graph": "s3-user-data",
                "template-s3-user-name": "waldur_u",
                "startTime": 1785542400,
                "endTime": 1785542400 + 86400,
                "maxDataPoints": 9001,
            },
        )
        assert points == [{"t": 1785542400, "v": 15728640.0}]

    def test_get_user_storage_series_without_plots_is_empty(self, client, mock_response):
        """A user croit holds no series for yields no points rather than raising."""
        mock_response.json.return_value = {"axis1": {"unit": "BYTES", "graphs": []}}

        with patch.object(client, "_request", return_value=mock_response):
            assert client.get_user_storage_series("waldur_u", 0, 0) == []

    @patch("httpx.Client.request")
    def test_missing_graph_is_not_reported_as_a_missing_user(
        self, mock_request, client, mock_response
    ):
        """A renamed graph must not masquerade as a deleted S3 user.

        _request maps every 404 to CephS3UserNotFoundError, and the graph name
        lives in croit's own graphite-queries.yml rather than in a documented API
        contract. If an upgrade renamed it, "user is gone" would look like a
        terminated resource and billing would stop quietly.
        """
        mock_response.status_code = 404
        mock_response.text = "HTTP 404 Not Found"
        mock_request.return_value = mock_response

        with pytest.raises(CroitS3GraphNotFoundError):
            client.get_user_storage_series("waldur_u", 0, 0)

class TestClientLayering:
    """croit is a statistics source on the shared transport, not a management API."""

    def test_croit_client_shares_the_transport_but_manages_nothing(self):
        from waldur_site_agent_ceph_s3.clients.base import BaseS3AdminClient, HttpTransport
        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        assert issubclass(CroitClient, HttpTransport)
        assert not issubclass(CroitClient, BaseS3AdminClient)
        for method in ("create_user", "delete_user", "list_users", "create_user_key"):
            assert not hasattr(CroitClient, method), method

    def test_base_does_not_know_about_storage_series(self):
        """Metering is croit-only; the shared base must not imply otherwise."""
        from waldur_site_agent_ceph_s3.clients.base import BaseS3AdminClient
        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        assert not hasattr(BaseS3AdminClient, "get_user_storage_series")
        assert hasattr(CroitClient, "get_user_storage_series")

    def test_exceptions_are_reachable_under_the_ceph_names(self):
        from waldur_site_agent_ceph_s3 import exceptions

        assert exceptions.CephS3UserNotFoundError is exceptions.CephS3UserNotFoundError
        assert exceptions.CephS3UserExistsError is exceptions.CephS3UserExistsError
        assert issubclass(exceptions.CephS3APIError, exceptions.CephS3Error)


class TestCroitNativeResolution:
    """croit returns at most 10,000 points per request and averages past that,
    on steps that move with the window; a long window is read in chunks that
    stay on its fixed 180 s grid, so recomputing a month never changes the past."""

    START = 1793491200  # 2026-11-01T00:00:00Z
    CHUNK = 9000 * 180

    @pytest.fixture
    def client(self):
        return CroitClient(api_url="https://croit.example.com", token="test-token")

    def _answer(self, plot_name=None):
        """A fake /stats that answers every native sample in the asked window."""

        def request(method, path, params):
            response = Mock()
            points = [
                {"t": t, "v": 1.0}
                for t in range(params["startTime"], params["endTime"] + 1, 180)
            ]
            graph = {"datapoints": points}
            if plot_name:
                graph["name"] = plot_name
            response.json.return_value = {"axis1": {"graphs": [graph]}}
            return response

        return request

    def test_a_month_is_read_in_native_chunks_and_joined_once(self, client):
        end = self.START + 31 * 86400
        with patch.object(client, "_request", side_effect=self._answer()) as request:
            points = client.get_user_storage_series("waldur_u", self.START, end)

        windows = [
            (c.kwargs["params"]["startTime"], c.kwargs["params"]["endTime"])
            for c in request.call_args_list
        ]
        assert windows == [
            (self.START, self.START + self.CHUNK),
            (self.START + self.CHUNK, end),
        ]
        assert all(
            c.kwargs["params"]["maxDataPoints"] <= 10000 for c in request.call_args_list
        )
        times = [p["t"] for p in points]
        assert times == list(range(self.START, end + 1, 180))  # no gap, no repeat

    def test_bucket_series_is_chunked_and_picks_its_plot(self, client):
        end = self.START + 20 * 86400
        with patch.object(
            client, "_request", side_effect=self._answer("Data")
        ) as request:
            points = client.get_bucket_storage_series("b", "t1", self.START, end)

        assert request.call_count == 2
        assert request.call_args_list[0].kwargs["params"]["template-s3-bucket-name"] == "t1_b"
        assert [p["t"] for p in points] == list(range(self.START, end + 1, 180))

    def test_relative_times_are_resolved_before_chunking(self, client):
        with patch("time.time", return_value=self.START + 25 * 86400), patch.object(
            client, "_request", side_effect=self._answer()
        ) as request:
            client.get_user_storage_series("waldur_u", -25 * 86400, 0)

        first, second = (c.kwargs["params"] for c in request.call_args_list)
        assert first["startTime"] == self.START
        assert second["endTime"] == self.START + 25 * 86400
