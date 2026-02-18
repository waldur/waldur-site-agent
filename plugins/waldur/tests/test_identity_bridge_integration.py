"""Integration tests for WaldurIdentityBridgeUsernameBackend.

Tests the identity bridge username backend against a real Waldur instance (Waldur B).
Uses the puhuri-federation-config.yaml for connection details.

Environment variables:
    WALDUR_INTEGRATION_TESTS=true     - Gate: skip all if not set
    WALDUR_IB_NO_CLEANUP=true         - Skip cleanup (leave users on Waldur B)

Usage:
    WALDUR_INTEGRATION_TESTS=true \
    .venv/bin/python -m pytest plugins/waldur/tests/test_identity_bridge_integration.py -v -s
"""

from __future__ import annotations

import os
import uuid

import pytest
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.types import UNSET

from waldur_site_agent_waldur.username_backend import (
    WaldurIdentityBridgeUsernameBackend,
    _extract_attributes,
    _get_waldur_username,
)

INTEGRATION_TESTS = os.environ.get("WALDUR_INTEGRATION_TESTS", "false").lower() == "true"
NO_CLEANUP = os.environ.get("WALDUR_IB_NO_CLEANUP", "false").lower() == "true"

# Load config from puhuri-federation-config.yaml
CONFIG_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "puhuri-federation-config.yaml"
)


def _load_backend_settings() -> dict:
    """Load backend_settings from puhuri-federation-config.yaml."""
    import yaml

    with open(CONFIG_FILE) as f:
        config = yaml.safe_load(f)
    offering = config["offerings"][0]
    return offering["backend_settings"]


def _make_offering_user(
    username: str,
    first_name: str = "",
    last_name: str = "",
    email: str = "",
) -> OfferingUser:
    """Create an OfferingUser with test data."""
    return OfferingUser(
        user_username=username,
        user_first_name=first_name or UNSET,
        user_last_name=last_name or UNSET,
        user_email=email or UNSET,
        user_full_name=f"{first_name} {last_name}".strip() or UNSET,
    )


def _cleanup(backend, username):
    """Remove user from identity bridge unless NO_CLEANUP is set."""
    if not NO_CLEANUP:
        backend._remove_user_from_identity_bridge(username)


@pytest.fixture(scope="module")
def backend_settings():
    """Load backend settings from config file."""
    return _load_backend_settings()


@pytest.fixture(scope="module")
def backend(backend_settings):
    """Create WaldurIdentityBridgeUsernameBackend with real settings."""
    return WaldurIdentityBridgeUsernameBackend(backend_settings=backend_settings)


@pytest.fixture
def test_username():
    """Generate a unique test username for each test."""
    return f"ib-test-{uuid.uuid4().hex[:8]}@acc.myaccessid.org"


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestIdentityBridgePush:
    """Test pushing users to identity bridge on Waldur B."""

    def test_push_user_creates_on_waldur_b(self, backend, test_username):
        """POST /api/identity-bridge/ creates a new user on Waldur B."""
        ou = _make_offering_user(
            username=test_username,
            first_name="IntegTest",
            last_name="User",
            email="integtest@example.com",
        )
        result = backend._push_user_to_identity_bridge(ou)

        assert result["created"] is True
        assert "uuid" in result

        _cleanup(backend, test_username)

    def test_push_user_updates_existing(self, backend, test_username):
        """Second push updates attributes instead of creating."""
        ou = _make_offering_user(
            username=test_username,
            first_name="Original",
            last_name="Name",
            email="original@example.com",
        )
        result1 = backend._push_user_to_identity_bridge(ou)
        assert result1["created"] is True

        # Update with new attributes
        ou2 = _make_offering_user(
            username=test_username,
            first_name="Updated",
            last_name="Name",
            email="updated@example.com",
        )
        result2 = backend._push_user_to_identity_bridge(ou2)
        assert result2["created"] is False
        assert "updated_fields" in result2

        _cleanup(backend, test_username)

    def test_remove_user_deactivates(self, backend, test_username):
        """POST /api/identity-bridge/remove/ deactivates the user."""
        ou = _make_offering_user(username=test_username, first_name="ToRemove")
        backend._push_user_to_identity_bridge(ou)

        result = backend._remove_user_from_identity_bridge(test_username)
        assert result["deactivated"] is True


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestIdentityBridgeSync:
    """Test sync_user_profiles batch operation."""

    def test_sync_pushes_all_users(self, backend_settings):
        """sync_user_profiles pushes all offering users to Waldur B."""
        be = WaldurIdentityBridgeUsernameBackend(backend_settings=backend_settings)

        usernames = [
            f"ib-sync-{uuid.uuid4().hex[:8]}@acc.myaccessid.org"
            for _ in range(3)
        ]
        offering_users = [
            _make_offering_user(
                username=u,
                first_name=f"Sync{i}",
                last_name="Test",
                email=f"sync{i}@example.com",
            )
            for i, u in enumerate(usernames)
        ]

        be.sync_user_profiles(offering_users)

        # Verify users exist on Waldur B by pushing again (should get created=False)
        for u in usernames:
            ou = _make_offering_user(username=u)
            result = be._push_user_to_identity_bridge(ou)
            assert result["created"] is False, f"User {u} should already exist"

        for u in usernames:
            _cleanup(be, u)

    def test_sync_detects_and_deactivates_stale_users(self, backend_settings):
        """Users removed between sync cycles are deactivated via identity bridge."""
        import httpx

        be = WaldurIdentityBridgeUsernameBackend(backend_settings=backend_settings)

        user_a = f"ib-stale-a-{uuid.uuid4().hex[:8]}@acc.myaccessid.org"
        user_b = f"ib-stale-b-{uuid.uuid4().hex[:8]}@acc.myaccessid.org"

        # First sync: both users
        be.sync_user_profiles([
            _make_offering_user(username=user_a, first_name="A"),
            _make_offering_user(username=user_b, first_name="B"),
        ])

        # Second sync: only user_a (user_b should be deactivated)
        be.sync_user_profiles([
            _make_offering_user(username=user_a, first_name="A"),
        ])

        # user_b was deactivated — pushing again should fail with 400
        ou_b = _make_offering_user(username=user_b, first_name="B")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            be._push_user_to_identity_bridge(ou_b)
        assert exc_info.value.response.status_code == 400, (
            "Deactivated user should be rejected by identity bridge"
        )

        _cleanup(be, user_a)


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestIdentityBridgeGetUsername:
    """Test get_username and generate_username methods."""

    def test_get_username_returns_waldur_username(self, backend):
        """get_username extracts user_username from OfferingUser."""
        ou = _make_offering_user(username="some-user@acc.myaccessid.org")
        assert backend.get_username(ou) == "some-user@acc.myaccessid.org"

    def test_generate_username_pushes_and_returns(self, backend, test_username):
        """generate_username pushes to identity bridge and returns username."""
        ou = _make_offering_user(
            username=test_username,
            first_name="Gen",
            last_name="User",
        )
        result = backend.generate_username(ou)
        assert result == test_username

        _cleanup(backend, test_username)
