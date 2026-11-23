"""Privileged Waldur B accounts refused by the Identity Bridge (#112).

Waldur B (Mastermind >= 8.1.4) answers 403 when a non-staff caller pushes or
removes a staff, support or identity-manager account. The agent reports that
once per cycle as a WARNING, asks the bridge about such a user at most once
per cycle, and keeps ERROR with a traceback for every other failure.
"""

import logging
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest
from waldur_api_client import errors

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_waldur.backend import WaldurBackend
from waldur_site_agent_waldur.privileged_accounts import (
    IdentityBridgePrivilegedAccountError,
    PrivilegedAccountRegistry,
    is_privileged_account_refusal,
    privileged_accounts,
)
from waldur_site_agent_waldur.username_backend import (
    WaldurIdentityBridgeUsernameBackend,
)

# Exactly what Mastermind's DRF view returns: a JSON-encoded string.
PRIVILEGED_BODY = (
    '"Only staff may push or remove staff, support or identity manager accounts."'
)
TARGET_URL = "https://waldur-b.example.com"
BRIDGE_URL = f"{TARGET_URL}/api/identity-bridge/"
USERNAME = "alice@myaccessid.org"
USER_UUID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
UB_MODULE = "waldur_site_agent_waldur.username_backend"


def _response(status_code, text="", json=None):
    request = httpx.Request("POST", BRIDGE_URL)
    if json is not None:
        return httpx.Response(status_code, json=json, request=request)
    return httpx.Response(status_code, text=text, request=request)


def _errors_and_tracebacks(caplog):
    return [r for r in caplog.records if r.levelno >= logging.ERROR or r.exc_info]


def _privileged_warnings(caplog):
    return [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "staff, support or identity manager account" in r.getMessage()
    ]


@pytest.fixture()
def waldur_backend(backend_settings, backend_components_passthrough):
    settings = dict(backend_settings, identity_bridge_source="isd:efp")
    return WaldurBackend(settings, backend_components_passthrough)


@pytest.fixture()
def bridge_post(waldur_backend):
    """Replace Waldur B's HTTP client; returns the mocked ``post``."""
    http = MagicMock()
    api_client = MagicMock()
    api_client.get_httpx_client.return_value = http
    with patch.object(waldur_backend.client, "_api_client", api_client):
        yield http.post


@pytest.fixture()
def username_backend():
    settings = {
        "target_api_url": f"{TARGET_URL}/api/",
        "target_api_token": "test-token-waldur-b",
        "identity_bridge_source": "isd:efp",
    }
    with patch.object(WaldurIdentityBridgeUsernameBackend, "_log_attribute_config"):
        backend = WaldurIdentityBridgeUsernameBackend(backend_settings=settings)
    backend._allowed_fields_cache = None
    with patch.object(backend, "_fetch_allowed_fields", return_value=None):
        yield backend


def _offering_user(username=USERNAME):
    ou = MagicMock()
    ou.user_username = username
    for field in (
        "user_first_name",
        "user_last_name",
        "user_email",
        "user_gender",
        "user_birth_date",
    ):
        setattr(ou, field, None)
    return ou


class TestRecognition:
    @pytest.mark.parametrize("body", [PRIVILEGED_BODY, PRIVILEGED_BODY.encode()])
    def test_privileged_403(self, body):
        assert is_privileged_account_refusal(403, body)

    @pytest.mark.parametrize(
        ("status_code", "body"),
        [
            (403, '"Source \'isd:efp\' is not in your managed ISDs."'),
            (403, '"Only staff and identity managers are allowed to use the Identity Bridge."'),
            (403, ""),
            (400, PRIVILEGED_BODY),
            (500, PRIVILEGED_BODY),
        ],
    )
    def test_other_responses(self, status_code, body):
        assert not is_privileged_account_refusal(status_code, body)


class TestRegistry:
    def test_record_reports_first_sighting_only(self):
        registry = PrivilegedAccountRegistry(ttl_seconds=60)
        assert registry.record(TARGET_URL, USERNAME) is True
        assert registry.record(TARGET_URL, USERNAME) is False
        assert registry.is_known(TARGET_URL, USERNAME)

    def test_api_suffix_and_trailing_slash_match(self):
        registry = PrivilegedAccountRegistry(ttl_seconds=60)
        registry.record(f"{TARGET_URL}/api/", USERNAME)
        assert registry.is_known(TARGET_URL, USERNAME)
        assert not registry.is_known("https://other.example.com", USERNAME)

    def test_expires_after_ttl(self):
        registry = PrivilegedAccountRegistry(ttl_seconds=60)
        with patch(
            "waldur_site_agent_waldur.privileged_accounts.time.monotonic",
            side_effect=[1000.0, 1059.0, 1061.0, 1061.0],
        ):
            registry.record(TARGET_URL, USERNAME)  # 1000
            assert registry.is_known(TARGET_URL, USERNAME)  # 1059
            assert not registry.is_known(TARGET_URL, USERNAME)  # 1061
            assert registry.record(TARGET_URL, USERNAME) is True  # 1061


class TestClient:
    def test_privileged_403_raises_without_logging(self, waldur_backend, bridge_post, caplog):
        bridge_post.return_value = _response(403, PRIVILEGED_BODY)
        caplog.set_level(logging.DEBUG)
        with pytest.raises(IdentityBridgePrivilegedAccountError) as exc_info:
            waldur_backend.client.resolve_user_via_identity_bridge(USERNAME, "isd:efp")
        assert exc_info.value.username == USERNAME
        assert _errors_and_tracebacks(caplog) == []

    def test_other_403_still_logged_as_error(self, waldur_backend, bridge_post, caplog):
        bridge_post.return_value = _response(
            403, '"Source \'isd:efp\' is not in your managed ISDs."'
        )
        result = waldur_backend.client.resolve_user_via_identity_bridge(USERNAME, "isd:efp")
        assert result is None
        levels = [r.levelno for r in caplog.records]
        assert logging.ERROR in levels
        assert any(r.exc_info for r in caplog.records)

    def test_network_error_still_logged_as_error(self, waldur_backend, bridge_post, caplog):
        bridge_post.side_effect = httpx.ConnectError("boom")
        result = waldur_backend.client.resolve_user_via_identity_bridge(USERNAME, "isd:efp")
        assert result is None
        assert any(r.exc_info for r in caplog.records)


class TestResolveRemoteUser:
    def test_one_bridge_call_and_one_warning_per_cycle(
        self, waldur_backend, bridge_post, caplog
    ):
        bridge_post.return_value = _response(403, PRIVILEGED_BODY)
        caplog.set_level(logging.DEBUG)

        for _ in range(3):
            assert waldur_backend._resolve_remote_user(USERNAME) is None

        assert bridge_post.call_count == 1
        assert len(_privileged_warnings(caplog)) == 1
        assert USERNAME in _privileged_warnings(caplog)[0].getMessage()
        assert _errors_and_tracebacks(caplog) == []
        assert not any("not found on Waldur B" in r.getMessage() for r in caplog.records)

    def test_shared_across_backend_instances(
        self, waldur_backend, bridge_post, backend_settings, backend_components_passthrough
    ):
        """Each pass builds new backends; one refusal covers the whole cycle."""
        bridge_post.return_value = _response(403, PRIVILEGED_BODY)
        waldur_backend._resolve_remote_user(USERNAME)

        other = WaldurBackend(
            dict(backend_settings, identity_bridge_source="isd:efp"),
            backend_components_passthrough,
        )
        with patch.object(other.client, "resolve_user_via_identity_bridge") as resolve:
            assert other._resolve_remote_user(USERNAME) is None
        resolve.assert_not_called()

    def test_asks_again_once_the_record_expires(self, waldur_backend, bridge_post):
        bridge_post.return_value = _response(403, PRIVILEGED_BODY)
        waldur_backend._resolve_remote_user(USERNAME)
        privileged_accounts.clear()  # the TTL ran out: next cycle

        bridge_post.return_value = _response(
            200, json={"uuid": str(USER_UUID), "created": False, "updated_fields": []}
        )
        assert waldur_backend._resolve_remote_user(USERNAME) == USER_UUID
        assert bridge_post.call_count == 2

    def test_fail_action_raises(self, waldur_backend, bridge_post):
        waldur_backend.user_not_found_action = "fail"
        bridge_post.return_value = _response(403, PRIVILEGED_BODY)
        for _ in range(2):
            with pytest.raises(BackendError, match="identity manager account"):
                waldur_backend._resolve_remote_user(USERNAME)
        assert bridge_post.call_count == 1

    def test_other_users_unaffected(self, waldur_backend, bridge_post):
        bridge_post.side_effect = [
            _response(403, PRIVILEGED_BODY),
            _response(200, json={"uuid": str(USER_UUID)}),
        ]
        assert waldur_backend._resolve_remote_user(USERNAME) is None
        assert waldur_backend._resolve_remote_user("bob@myaccessid.org") == USER_UUID


class TestUsernameBackend:
    def _refusal(self):
        return errors.UnexpectedStatus(403, PRIVILEGED_BODY.encode(), BRIDGE_URL)

    def test_profile_push_logs_one_warning(self, username_backend, caplog):
        caplog.set_level(logging.DEBUG)
        with patch(f"{UB_MODULE}.identity_bridge") as ib:
            ib.sync.side_effect = self._refusal()
            username_backend.sync_user_profiles([_offering_user()])
            username_backend.sync_user_profiles([_offering_user()])

        assert ib.sync.call_count == 1
        assert len(_privileged_warnings(caplog)) == 1
        assert _errors_and_tracebacks(caplog) == []

    def test_profile_push_then_membership_resolve_hits_bridge_once(
        self, username_backend, waldur_backend, bridge_post, caplog
    ):
        """The order inside one pass: profile sync first, then membership sync."""
        with patch(f"{UB_MODULE}.identity_bridge") as ib:
            ib.sync.side_effect = self._refusal()
            username_backend.sync_user_profiles([_offering_user()])

        assert waldur_backend._resolve_remote_user(USERNAME) is None
        bridge_post.assert_not_called()
        assert len(_privileged_warnings(caplog)) == 1
        assert _errors_and_tracebacks(caplog) == []

    def test_generate_username_does_not_raise(self, username_backend, caplog):
        with patch(f"{UB_MODULE}.identity_bridge") as ib:
            ib.sync.side_effect = self._refusal()
            assert username_backend.generate_username(_offering_user()) == ""
        assert len(_privileged_warnings(caplog)) == 1

    def test_other_push_errors_still_logged_with_traceback(self, username_backend, caplog):
        with patch(f"{UB_MODULE}.identity_bridge") as ib:
            ib.sync.side_effect = errors.UnexpectedStatus(500, b"Server Error", BRIDGE_URL)
            username_backend.sync_user_profiles([_offering_user()])
        assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)
        assert _privileged_warnings(caplog) == []

    def test_deactivation_logs_one_warning(self, username_backend, caplog):
        with patch(f"{UB_MODULE}.identity_bridge_remove") as remove:
            remove.sync.side_effect = self._refusal()
            username_backend.deactivate_users({USERNAME})
            username_backend.deactivate_users({USERNAME})

        assert remove.sync.call_count == 1
        warnings = _privileged_warnings(caplog)
        assert len(warnings) == 1
        assert "leaves this user unchanged" in warnings[0].getMessage()
        assert _errors_and_tracebacks(caplog) == []

    def test_other_deactivation_errors_still_logged_with_traceback(
        self, username_backend, caplog
    ):
        with patch(f"{UB_MODULE}.identity_bridge_remove") as remove:
            remove.sync.side_effect = errors.UnexpectedStatus(500, b"Server Error", BRIDGE_URL)
            username_backend.deactivate_users({USERNAME})
        assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)
