"""Tests for CroitClient."""

import json
import pytest
import httpx
from unittest.mock import Mock, patch

from waldur_site_agent_ceph_s3.clients.croit import CroitClient
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3APIError,
    CephS3AuthenticationError,
    CroitS3GraphNotFoundError,
    CephS3UserExistsError,
    CephS3UserNotFoundError,
)


class TestCroitClient:
    """Test CroitClient functionality."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return CroitClient(
            api_url="https://test.example.com",
            username="test_user",
            password="test_pass",
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
    def test_ping_success(self, mock_request, client, mock_response):
        """Test successful ping."""
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_request.return_value = mock_response

        result = client.ping()

        assert result is True
        mock_request.assert_called_once()

    @patch("httpx.Client.request")
    def test_ping_failure(self, mock_request, client):
        """Test ping failure."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        result = client.ping()

        assert result is False

    @patch("httpx.Client.request")
    def test_ping_with_exception(self, mock_request, client):
        """Test ping raises exception when requested."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        with pytest.raises(Exception):
            client.ping(raise_exception=True)

    @patch("httpx.Client.request")
    def test_create_user_success(self, mock_request, client, mock_response):
        """Test successful user creation."""
        mock_response.status_code = 201
        mock_response.json.return_value = {"uid": "test_user", "name": "Test User"}
        mock_response.content = b'{"uid": "test_user", "name": "Test User"}'
        mock_request.return_value = mock_response

        result = client.create_user(
            uid="test_user", name="Test User", email="test@example.com"
        )

        assert result == {"uid": "test_user", "name": "Test User"}
        mock_request.assert_called_once_with(
            method="POST",
            url="https://test.example.com/api/s3/users",
            headers={},
            content=b'{"uid": "test_user", "name": "Test User", "email": "test@example.com"}',
        )

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
            client.list_users()

    @patch("httpx.Client.request")
    def test_a_rejected_basic_auth_still_names_the_user(self, mock_request):
        """The username is not a secret, and it is the useful half under basic auth."""
        mock_response = Mock(status_code=401, text="")
        mock_request.return_value = mock_response
        client = CroitClient(
            api_url="https://test.example.com", username="admin", password="secret"
        )

        with pytest.raises(CephS3AuthenticationError, match="admin"):
            client.list_users()

    @patch("httpx.Client.request")
    def test_create_user_translates_the_neutral_option_names(
        self, mock_request, client, mock_response
    ):
        """croit's API spells these camelCase; the shared contract does not."""
        mock_response.status_code = 201
        mock_response.content = b"{}"
        mock_request.return_value = mock_response

        client.create_user(
            uid="waldur_u",
            name="demo",
            tenant="acme",
            default_placement="fast-pool",
            default_storage_class="STANDARD_IA",
        )

        assert json.loads(mock_request.call_args.kwargs["content"]) == {
            "uid": "waldur_u",
            "name": "demo",
            "tenant": "acme",
            "defaultPlacement": "fast-pool",
            "defaultStorageClass": "STANDARD_IA",
        }

    @patch("httpx.Client.request")
    def test_create_user_exists(self, mock_request, client):
        """Test user creation when user already exists."""
        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "User already exists"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3UserExistsError):
            client.create_user(uid="existing_user", name="Existing User")

    @patch("httpx.Client.request")
    def test_delete_user_success(self, mock_request, client, mock_response):
        """Test successful user deletion."""
        mock_response.status_code = 204
        mock_request.return_value = mock_response

        result = client.delete_user("test_user")

        assert result is True
        mock_request.assert_called_once_with(
            method="DELETE",
            url="https://test.example.com/api/s3/users/test_user",
            headers={},
            content=None,
        )

    @patch("httpx.Client.request")
    def test_delete_user_not_found(self, mock_request, client):
        """Test user deletion when user not found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "User not found"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3UserNotFoundError):
            client.delete_user("nonexistent_user")

    @patch("httpx.Client.request")
    def test_get_user_info_success(self, mock_request, client, mock_response):
        """Test successful user info retrieval."""
        mock_response.json.return_value = [
            {
                "uid": "test_user",
                "name": "Test User",
                "email": "test@example.com",
                "suspended": False,
            }
        ]
        mock_request.return_value = mock_response

        result = client.get_user_info("test_user")

        assert result["uid"] == "test_user"
        assert result["name"] == "Test User"
        assert result["email"] == "test@example.com"
        assert result["suspended"] is False

    @patch("httpx.Client.request")
    def test_get_user_info_not_found(self, mock_request, client, mock_response):
        """Test user info retrieval when user not found."""
        mock_response.json.return_value = {"data": []}
        mock_request.return_value = mock_response

        with pytest.raises(CephS3UserNotFoundError):
            client.get_user_info("nonexistent_user")

    @patch("httpx.Client.request")
    def test_get_user_buckets(self, mock_request, client, mock_response):
        """Test user buckets retrieval."""
        mock_response.json.return_value = [
            {
                "bucket": "test-bucket-1",
                "owner": "test_user",
                "usageSum": {"size": 1024000, "numObjects": 10},
            },
            {
                "bucket": "test-bucket-2",
                "owner": "test_user",
                "usageSum": {"size": 2048000, "numObjects": 20},
            },
        ]
        mock_request.return_value = mock_response

        result = client.get_user_buckets("test_user")

        assert result == [
            {"name": "test-bucket-1", "size_bytes": 1024000, "num_objects": 10},
            {"name": "test-bucket-2", "size_bytes": 2048000, "num_objects": 20},
        ]

    @patch("httpx.Client.request")
    def test_set_user_bucket_quota(self, mock_request, client, mock_response):
        """Test setting user bucket quota."""
        mock_response.status_code = 204
        mock_request.return_value = mock_response

        quota = {"enabled": True, "max_size_bytes": 10737418240, "max_objects": 1000}
        client.set_user_bucket_quota("test_user", quota)

        mock_request.assert_called_once_with(
            method="PUT",
            url="https://test.example.com/api/s3/users/test_user/bucket-quota",
            headers={},
            content=b'{"enabled": true, "maxSize": 10737418240, "maxObjects": 1000}',
        )

    @patch("httpx.Client.request")
    def test_authentication_error(self, mock_request, client):
        """Test authentication error handling."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3AuthenticationError):
            client.list_users()

    @patch("waldur_site_agent_ceph_s3.clients.base.time.sleep")
    @patch("httpx.Client.request")
    def test_api_error(self, mock_request, mock_sleep, client):
        """Test general API error handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3APIError):
            client.list_users()

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
            client.delete_user("test_user")

        assert str(exc_info.value) == "Unable to remove user with buckets."

    @patch("httpx.Client.request")
    def test_api_error_falls_back_to_status_and_body(self, mock_request, client):
        """A body without a usable message keeps the status code for context."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "<html>Bad Request</html>"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3APIError) as exc_info:
            client.list_users()

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
        mock_response.json.return_value = {"data": []}
        mock_request.side_effect = [error_response, mock_response]

        result = client.ping()

        assert result is True
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("httpx.Client.request")
    def test_timeout_error(self, mock_request, client):
        """Test timeout error handling."""
        mock_request.side_effect = httpx.TimeoutException("Request timed out")

        with pytest.raises(CephS3APIError, match="Request timeout"):
            client.list_users()

    @patch("httpx.Client.request")
    def test_connection_error(self, mock_request, client):
        """Test connection error handling."""
        mock_request.side_effect = httpx.ConnectError("Connection failed")

        with pytest.raises(CephS3APIError, match="Connection error"):
            client.list_users()

    def test_list_user_keys(self, client, mock_response):
        """Test listing every access key of a user."""
        mock_response.json.return_value = [
            {"user": "waldur_u", "access_key": "AKIA1", "secret_key": "s1"},
            {"user": "waldur_u", "access_key": "AKIA2", "secret_key": "s2"},
        ]

        with patch.object(client, "_request", return_value=mock_response) as request:
            keys = client.list_user_keys("waldur_u")

        request.assert_called_once_with("GET", "/s3/users/waldur_u/keys")
        assert [key["access_key"] for key in keys] == ["AKIA1", "AKIA2"]

    def test_list_user_keys_handles_non_list_payload(self, client, mock_response):
        """Test that an unexpected payload yields an empty list."""
        mock_response.json.return_value = {}

        with patch.object(client, "_request", return_value=mock_response):
            assert client.list_user_keys("waldur_u") == []

    @patch("httpx.Client.request")
    def test_list_user_keys_of_an_unknown_user_raises_not_found(
        self, mock_request, client, mock_response
    ):
        """A 404 here means the user is gone, not that it holds no keys.

        Answering [] would let a rotation against a deleted resource read as a
        success with nothing to do. Driven through the real transport because the
        mapping lives in _request, not in list_user_keys.
        """
        mock_response.status_code = 404
        mock_response.text = "user not found"
        mock_request.return_value = mock_response

        with pytest.raises(CephS3UserNotFoundError):
            client.list_user_keys("nobody")

    def test_set_user_quota(self, client, mock_response):
        """The aggregate quota goes to /quota, not /bucket-quota.

        The two endpoints differ by one path segment and mean different things:
        one caps each bucket, the other caps the tenant.
        """
        mock_response.status_code = 204

        with patch.object(client, "_request", return_value=mock_response) as request:
            client.set_user_quota("waldur_u", {"enabled": True, "max_size_bytes": 100})

        request.assert_called_once_with(
            "PUT", "/s3/users/waldur_u/quota", json_data={"enabled": True, "maxSize": 100}
        )

    def test_set_user_quota_rejects_unexpected_status(self, client, mock_response):
        """A non-204 means the ceiling did not land and must not pass silently."""
        mock_response.status_code = 200

        with patch.object(client, "_request", return_value=mock_response):
            with pytest.raises(CephS3APIError):
                client.set_user_quota("waldur_u", {"enabled": True})

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
            points = client.get_user_storage_series("waldur_u", 1785542400, 0)

        request.assert_called_once_with(
            "GET",
            "/stats",
            params={
                "graph": "s3-user-data",
                "template-s3-user-name": "waldur_u",
                "startTime": 1785542400,
                "endTime": 0,
                "maxDataPoints": 2000,
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

    def test_create_user_key(self, client, mock_response):
        """Test adding an access/secret pair to a user."""
        mock_response.status_code = 204

        with patch.object(client, "_request", return_value=mock_response) as request:
            client.create_user_key("waldur_u", "AKIA1", "s3cret")

        request.assert_called_once_with(
            "PUT", "/s3/users/waldur_u/keys/AKIA1", params={"secretKey": "s3cret"}
        )

    def test_create_user_key_rejects_unexpected_status(self, client, mock_response):
        """Test that a non-204 response is an error."""
        mock_response.status_code = 200

        with patch.object(client, "_request", return_value=mock_response):
            with pytest.raises(CephS3APIError):
                client.create_user_key("waldur_u", "AKIA1", "s3cret")

    def test_delete_user_key(self, client, mock_response):
        """Test removing one access key from a user."""
        mock_response.status_code = 204

        # The client checks which keys the user holds before deleting; croit
        # answers 500 rather than 404 for one it does not hold.
        with patch.object(
            client, "list_user_keys", return_value=[{"access_key": "AKIA1"}]
        ), patch.object(client, "_request", return_value=mock_response) as request:
            client.delete_user_key("waldur_u", "AKIA1")

        request.assert_called_once_with("DELETE", "/s3/users/waldur_u/keys/AKIA1")

    def test_delete_user_key_rejects_unexpected_status(self, client, mock_response):
        """Test that a non-204 response is an error."""
        mock_response.status_code = 200

        with patch.object(
            client, "list_user_keys", return_value=[{"access_key": "AKIA1"}]
        ), patch.object(client, "_request", return_value=mock_response):
            with pytest.raises(CephS3APIError):
                client.delete_user_key("waldur_u", "AKIA1")

    @patch("httpx.Client.request")
    def test_deleting_an_absent_key_is_a_no_op(self, mock_request, client, mock_response):
        """An already-absent key is success, so a rotation retry can converge.

        A rotation whose reply was lost has already removed the old access key.
        The reconciliation pass re-issues that rotation, so the second delete must
        not mark the key Erred — that would turn a recoverable stall into a
        visible error.

        Measured against croit: it answers **500** for a key it does not hold, not
        404, so the client reads the key list first and skips the delete entirely.
        Driven through the real transport so the list call is a real request.
        """
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_request.return_value = mock_response

        client.delete_user_key("waldur_u", "AKIAGONE")

        # The DELETE is never attempted: the key is not in the list.
        assert all(call.kwargs.get("method") != "DELETE" for call in mock_request.call_args_list)


class TestIdentifiersCannotRewriteThePath:
    """httpx normalises dot-segments, so an identifier is a path, not a value.

    `client_id=".."` turns "delete one key" into croit's delete-the-whole-user
    endpoint. Both identifiers arrive from outside this process — one from a STOMP
    frame, one from a backend id the core builds by concatenating slugs — so they
    are checked where the URL is assembled rather than at each call site.
    """

    @pytest.fixture
    def client(self):
        return CroitClient(api_url="https://croit.test", token="t")

    @pytest.mark.parametrize(
        "access_key", ["..", "../", "AK/../../danger", "AKIA?admin=1", "", "AKIA/"]
    )
    def test_a_hostile_access_key_is_rejected(self, client, access_key):
        with pytest.raises(CephS3APIError, match="access key"):
            client.delete_user_key("waldur_u", access_key)

    @pytest.mark.parametrize(
        "uid", ["../..", "a/../../../v2/status", "u?admin=1", "", "u#frag", "..", "."]
    )
    def test_a_hostile_uid_is_rejected(self, client, uid):
        with pytest.raises(CephS3APIError, match="user id"):
            client.delete_user_key(uid, "AKIAEXAMPLE123456789")

    def test_a_legitimate_pair_is_accepted(self, client, monkeypatch):
        """The guard must not reject the identifiers the plugin actually mints."""
        seen = {}

        def fake_request(method, endpoint, json_data=None, params=None):
            seen["endpoint"] = endpoint

            class R:
                status_code = 204

                @staticmethod
                def json():
                    return []

                @staticmethod
                def raise_for_status():
                    return None

            return R()

        monkeypatch.setattr(client, "_request", fake_request)
        monkeypatch.setattr(
            client, "list_user_keys", lambda uid: [{"access_key": "AKIAEXAMPLE123456789"}]
        )
        client.delete_user_key("cust-0-pro", "AKIAEXAMPLE123456789")

        assert seen["endpoint"] == "/s3/users/cust-0-pro/keys/AKIAEXAMPLE123456789"

    def test_an_error_echoing_the_request_path_does_not_leak_the_secret(self, client):
        """croit's message reaches a Waldur-visible error_message verbatim."""

        class R:
            status_code = 400
            text = (
                '{"message": "invalid request: PUT /api/s3/users/u/keys/AKIA'
                '?secretKey=SUPERSECRET_value"}'
            )

        assert "SUPERSECRET_value" not in client._format_error(R())


class TestClientLayering:
    """The croit client is one flavour on a shared transport, not the transport."""

    def test_croit_client_is_built_on_the_shared_base(self):
        from waldur_site_agent_ceph_s3.clients.base import BaseS3AdminClient
        from waldur_site_agent_ceph_s3.clients.croit import CroitClient

        assert issubclass(CroitClient, BaseS3AdminClient)

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
