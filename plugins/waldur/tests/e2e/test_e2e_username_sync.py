"""End-to-end offering user username sync tests (Waldur B -> Waldur A).

Positive-flow test for sync_offering_user_usernames:
  1. Resolve the API token user across both Waldur instances (via CUID)
  2. Create offering users on Waldur A (source) and Waldur B (target)
  3. Set a known username on the Waldur B offering user
  4. Run sync_offering_user_usernames
  5. Verify the Waldur A offering user now carries the Waldur B username
  6. Verify idempotency (second sync is a no-op)
  7. Clean up created offering users

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_username_sync.py -v -s
"""

from __future__ import annotations

import logging
import os
import uuid as uuid_mod

import pytest

from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_list,
)
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.types import UNSET

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Username assigned on Waldur B that we expect to see on Waldur A after sync
TEST_USERNAME_B = f"e2e-sync-{uuid_mod.uuid4().hex[:8]}"


def _format_offering_user(ou) -> dict[str, str]:
    """Extract key fields from an OfferingUser for reporting."""
    uuid_val = (
        ou.uuid.hex
        if hasattr(ou.uuid, "hex")
        else str(ou.uuid)
        if not isinstance(ou.uuid, type(UNSET))
        else "UNSET"
    )
    user_uuid = (
        str(ou.user_uuid) if not isinstance(ou.user_uuid, type(UNSET)) else "UNSET"
    )
    username = ou.username if not isinstance(ou.username, type(UNSET)) else "UNSET"
    user_username = (
        ou.user_username
        if not isinstance(ou.user_username, type(UNSET))
        else "UNSET"
    )
    state = str(ou.state) if not isinstance(ou.state, type(UNSET)) else "UNSET"
    return {
        "uuid": uuid_val,
        "user_uuid": user_uuid,
        "username": username or "(empty)",
        "user_username": user_username or "(empty)",
        "state": state,
    }


_USABLE_STATES = {"OK", "Creating", "Requested"}


def _get_or_create_offering_user(client, user_url, offering_uuid):
    """Get existing (in usable state) or create new offering user.

    Returns (data_dict, created). Offering users in deletion/error states
    are ignored so that a fresh one can be created.
    """
    # Check for existing offering user
    resp = client.get_httpx_client().get(
        "/api/marketplace-offering-users/",
        params={"offering_uuid": offering_uuid},
    )
    resp.raise_for_status()
    existing = resp.json()

    # Extract user UUID from URL for matching
    user_uuid_from_url = user_url.rstrip("/").rsplit("/", 1)[-1].replace("-", "")

    stale_uuids = []
    for ou in existing:
        # Match by user URL or user UUID
        ou_user_uuid = (ou.get("user_uuid") or "").replace("-", "")
        is_match = (
            ou.get("user") == user_url
            or ou.get("user_url") == user_url
            or (ou_user_uuid and ou_user_uuid == user_uuid_from_url)
        )
        if not is_match:
            continue
        if ou.get("state", "") in _USABLE_STATES:
            return ou, False
        # Track stale offering users (deletion/error states) for cleanup
        stale_uuids.append(ou["uuid"])

    # Delete stale offering users so we can create a fresh one
    for stale_uuid in stale_uuids:
        _delete_offering_user(client, stale_uuid)

    # Create new offering user
    resp = client.get_httpx_client().post(
        "/api/marketplace-offering-users/",
        json={"user": user_url, "offering_uuid": offering_uuid},
    )
    resp.raise_for_status()
    return resp.json(), True


def _delete_offering_user(client, ou_uuid):
    """Delete an offering user, ignoring 404."""
    resp = client.get_httpx_client().delete(
        f"/api/marketplace-offering-users/{ou_uuid}/",
    )
    if resp.status_code not in (204, 404):
        resp.raise_for_status()
    return resp.status_code


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestPositiveUsernameSync:
    """Full positive flow: create offering users, sync username from B to A."""

    _state: dict = {}

    def test_01_resolve_user_across_instances(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Find a user that exists on both Waldur instances.

        Strategy:
          1. Try resolving the Waldur A API user on Waldur B via configured
             match field (CUID) or email fallback.
          2. If that fails, scan Waldur A offering users for one whose
             user_username resolves on Waldur B.
          3. If that also fails, scan Waldur B offering users and search for
             a matching user on Waldur A by email.
        """
        report.heading(2, "Username Sync: Resolve User Across Instances")

        match_field = offering.backend_settings.get("user_match_field", "cuid")
        report.text(f"**Configured match field:** `{match_field}`")

        # --- Strategy 1: Try the API token user ---
        resp = waldur_client_a.get_httpx_client().get("/api/users/me/")
        resp.raise_for_status()
        user_a = resp.json()
        user_a_username = user_a.get("username", "")
        user_a_email = user_a.get("email", "")

        report.status_snapshot(
            "Waldur A API user",
            {
                "uuid": user_a["uuid"],
                "username": user_a_username,
                "email": user_a_email,
            },
        )

        remote_uuid = backend._resolve_remote_user(user_a_username)

        # Fallback to email for API user
        if remote_uuid is None and user_a_email:
            remote_uuid = backend.client.resolve_user_by_field(
                user_a_email, "email"
            )
            if remote_uuid is not None:
                report.text(
                    f"API user resolved via email on Waldur B: `{remote_uuid}`"
                )
                self.__class__._state["original_match_field"] = (
                    backend.user_match_field
                )
                backend.user_match_field = "email"
                backend._user_uuid_cache.clear()

        if remote_uuid is not None:
            user_a_url = user_a["url"]
            self._store_resolved_user(
                report, waldur_client_b, offering,
                user_a["uuid"], user_a_url, user_a_username,
                remote_uuid,
            )
            return

        report.text(
            f"API user `{user_a_username}` not found on Waldur B. "
            "Scanning for a shared user..."
        )

        # --- Strategy 2: scan Waldur B offering users, match to Waldur A ---
        from uuid import UUID

        target_offering_uuid = offering.backend_settings["target_offering_uuid"]
        b_offering_users = marketplace_offering_users_list.sync_all(
            client=waldur_client_b,
            offering_uuid=[UUID(target_offering_uuid)],
            is_restricted=False,
        )
        report.text(
            f"Found {len(b_offering_users)} offering users on Waldur B "
            f"for offering `{target_offering_uuid}`"
        )

        for b_ou in b_offering_users:
            b_user_uuid = b_ou.user_uuid
            if isinstance(b_user_uuid, type(UNSET)) or not b_user_uuid:
                continue

            # Get the Waldur B user's email
            resp = waldur_client_b.get_httpx_client().get(
                f"/api/users/{b_user_uuid}/"
            )
            if resp.status_code != 200:
                continue
            b_user = resp.json()
            b_email = b_user.get("email", "")
            b_username = b_user.get("username", "")
            if not b_email and not b_username:
                continue

            # Search for this user on Waldur A by email
            search_field = b_email or b_username
            resp = waldur_client_a.get_httpx_client().get(
                "/api/users/", params={"email": b_email} if b_email else {"username": b_username}
            )
            if resp.status_code != 200:
                continue
            a_users = resp.json()
            if not a_users:
                continue

            # Found a matching user on Waldur A
            user_a_match = a_users[0]
            user_a_match_uuid = user_a_match["uuid"]
            user_a_match_url = user_a_match["url"]
            user_a_match_username = user_a_match.get("username", "")

            report.text(
                f"Found shared user: A=`{user_a_match_username}` "
                f"B=`{b_username}` (email: `{b_email}`)"
            )

            # Override match field to email so sync() can resolve
            self.__class__._state["original_match_field"] = (
                backend.user_match_field
            )
            backend.user_match_field = "email"
            backend._user_uuid_cache.clear()

            self._store_resolved_user(
                report, waldur_client_b, offering,
                user_a_match_uuid, user_a_match_url, user_a_match_username,
                UUID(str(b_user_uuid)),
            )
            return

        pytest.skip(
            "No shared user found between Waldur A and Waldur B "
            "for this offering pair"
        )

    def _store_resolved_user(
        self, report, waldur_client_b, offering,
        user_a_uuid, user_a_url, user_a_username, remote_uuid,
    ):
        """Store resolved user state for subsequent tests."""
        remote_uuid_str = str(remote_uuid)

        self.__class__._state["user_a_uuid"] = user_a_uuid
        self.__class__._state["user_a_url"] = user_a_url
        self.__class__._state["user_a_username"] = user_a_username

        # Get user details on Waldur B
        resp = waldur_client_b.get_httpx_client().get(
            f"/api/users/{remote_uuid_str}/"
        )
        if resp.status_code == 200:
            user_b = resp.json()
            user_b_url = user_b.get("url", "")
            report.status_snapshot(
                "Waldur B resolved user",
                {
                    "uuid": remote_uuid_str,
                    "username": user_b.get("username", ""),
                    "email": user_b.get("email", ""),
                },
            )
            self.__class__._state["user_b_uuid"] = remote_uuid_str
            self.__class__._state["user_b_url"] = user_b_url
        else:
            self.__class__._state["user_b_uuid"] = remote_uuid_str
            target_api = offering.backend_settings["target_api_url"].rstrip("/")
            self.__class__._state["user_b_url"] = (
                f"{target_api}/api/users/{remote_uuid_str}/"
            )

    def test_02_create_offering_users(
        self, offering, waldur_client_a, waldur_client_b, report
    ):
        """Create offering users on both Waldur A and B for the resolved user.

        Ensures both offering users end up in OK state so that the sync
        can find them. On offerings with service_provider_can_create_offering_user,
        newly created offering users start in Requested state; setting a username
        via PATCH transitions them to OK.
        """
        report.heading(2, "Username Sync: Create Offering Users")

        if "user_b_uuid" not in self.__class__._state:
            pytest.skip("User not resolved on Waldur B (test_01 skipped)")

        user_a_url = self.__class__._state["user_a_url"]
        user_b_url = self.__class__._state["user_b_url"]
        source_offering_uuid = offering.waldur_offering_uuid
        target_offering_uuid = offering.backend_settings["target_offering_uuid"]

        # --- Waldur A offering user ---
        ou_a, created_a = _get_or_create_offering_user(
            waldur_client_a, user_a_url, source_offering_uuid
        )
        ou_a_uuid = ou_a["uuid"]
        ou_a_state = ou_a.get("state", "")
        self.__class__._state["ou_a_uuid"] = ou_a_uuid
        self.__class__._state["ou_a_created"] = created_a
        self.__class__._state["ou_a_original_username"] = ou_a.get("username", "")

        action_a = "Created" if created_a else "Found existing"
        report.text(
            f"**Waldur A:** {action_a} offering user `{ou_a_uuid}` "
            f"(state: {ou_a_state}, username: `{ou_a.get('username', '')}`)"
        )

        # Transition Waldur A offering user to OK if needed.
        # Setting a username on a Requested/Creating offering user
        # auto-transitions it to OK in Mastermind.
        if ou_a_state not in ("OK", "Creating"):
            placeholder = "pending-sync"
            resp = waldur_client_a.get_httpx_client().patch(
                f"/api/marketplace-offering-users/{ou_a_uuid}/",
                json={"username": placeholder},
            )
            if resp.status_code < 400:
                updated = resp.json()
                new_state = updated.get("state", "")
                report.text(
                    f"  Set placeholder username -> state transitioned to `{new_state}`"
                )
            else:
                report.text(
                    f"  WARNING: Could not set username (status {resp.status_code}): "
                    f"{resp.text[:200]}"
                )

        # --- Waldur B offering user ---
        ou_b, created_b = _get_or_create_offering_user(
            waldur_client_b, user_b_url, target_offering_uuid
        )
        ou_b_uuid = ou_b["uuid"]
        ou_b_state = ou_b.get("state", "")
        self.__class__._state["ou_b_uuid"] = ou_b_uuid
        self.__class__._state["ou_b_created"] = created_b
        self.__class__._state["ou_b_original_username"] = ou_b.get("username", "")

        action_b = "Created" if created_b else "Found existing"
        report.text(
            f"**Waldur B:** {action_b} offering user `{ou_b_uuid}` "
            f"(state: {ou_b_state}, username: `{ou_b.get('username', '')}`)"
        )

        # Transition Waldur B offering user to OK if needed
        if ou_b_state not in ("OK",):
            resp = waldur_client_b.get_httpx_client().patch(
                f"/api/marketplace-offering-users/{ou_b_uuid}/",
                json={"username": "pre-sync-placeholder"},
            )
            if resp.status_code < 400:
                updated = resp.json()
                new_state = updated.get("state", "")
                report.text(
                    f"  Set placeholder username -> state transitioned to `{new_state}`"
                )
            else:
                report.text(
                    f"  WARNING: Could not set username (status {resp.status_code}): "
                    f"{resp.text[:200]}"
                )

    def test_03_set_username_on_waldur_b(
        self, waldur_client_b, report
    ):
        """Set a known test username on the Waldur B offering user."""
        report.heading(2, "Username Sync: Set Username on Waldur B")

        ou_b_uuid = self.__class__._state.get("ou_b_uuid")
        if not ou_b_uuid:
            pytest.skip("No Waldur B offering user (test_02 skipped)")

        resp = waldur_client_b.get_httpx_client().patch(
            f"/api/marketplace-offering-users/{ou_b_uuid}/",
            json={"username": TEST_USERNAME_B},
        )
        resp.raise_for_status()
        updated = resp.json()

        actual_username = updated.get("username", "")
        actual_state = updated.get("state", "")
        report.text(
            f"Set Waldur B offering user `{ou_b_uuid}` username to `{TEST_USERNAME_B}`"
        )
        report.text(f"**After PATCH:** username=`{actual_username}`, state=`{actual_state}`")

        assert actual_username == TEST_USERNAME_B, (
            f"Expected username {TEST_USERNAME_B}, got {actual_username}"
        )

    def test_04_run_sync(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Run sync_offering_user_usernames — should pull TEST_USERNAME_B to A."""
        report.heading(2, "Username Sync: Execute Sync")

        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        # Fetch the Waldur A offering user to see its user_username
        ou_a_uuid = self.__class__._state["ou_a_uuid"]
        resp = waldur_client_a.get_httpx_client().get(
            f"/api/marketplace-offering-users/{ou_a_uuid}/"
        )
        resp.raise_for_status()
        ou_a_data = resp.json()
        a_user_username = ou_a_data.get("user_username", "")

        a_state = ou_a_data.get("state", "")
        report.text(f"**Waldur A offering user user_username:** `{a_user_username}`")
        report.text(f"**Waldur A offering user state:** `{a_state}`")
        report.text(f"**Waldur A offering user username:** `{ou_a_data.get('username', '')}`")

        # Pre-populate the resolution cache: user_username -> Waldur B user UUID
        # This is needed because the configured match field (CUID) may not be
        # available, and email/username match requires user_username == email,
        # which is not always the case.
        from uuid import UUID

        user_b_uuid = self.__class__._state["user_b_uuid"]
        backend._user_uuid_cache.clear()
        backend._user_uuid_cache[a_user_username] = UUID(user_b_uuid)
        report.text(
            f"Pre-cached resolution: `{a_user_username}` -> `{user_b_uuid}`"
        )

        # Also check what the Waldur B offering user looks like
        ou_b_uuid = self.__class__._state["ou_b_uuid"]
        resp = waldur_client_b.get_httpx_client().get(
            f"/api/marketplace-offering-users/{ou_b_uuid}/"
        )
        if resp.status_code == 200:
            ou_b_data = resp.json()
            report.text(
                f"**Waldur B offering user:** state=`{ou_b_data.get('state', '')}` "
                f"username=`{ou_b_data.get('username', '')}` "
                f"user_uuid=`{ou_b_data.get('user_uuid', '')}`"
            )

        changed = backend.sync_offering_user_usernames(
            waldur_a_offering_uuid=offering.waldur_offering_uuid,
            waldur_rest_client=waldur_client_a,
        )

        self.__class__._state["sync_changed"] = changed
        report.text(f"**sync_offering_user_usernames returned:** `{changed}`")

        if changed:
            report.text("Usernames were updated on Waldur A.")
        else:
            report.text(
                "No changes made. Possible reasons:\n"
                "- Waldur A offering user already had the correct username\n"
                "- User identity resolution failed\n"
                "- Waldur B offering user not in OK state"
            )

    def test_05_verify_username_propagated(
        self, waldur_client_a, report
    ):
        """Verify the Waldur A offering user now has the Waldur B username."""
        report.heading(2, "Username Sync: Verify Propagation")

        ou_a_uuid = self.__class__._state.get("ou_a_uuid")
        if not ou_a_uuid:
            pytest.skip("No Waldur A offering user")

        resp = waldur_client_a.get_httpx_client().get(
            f"/api/marketplace-offering-users/{ou_a_uuid}/"
        )
        resp.raise_for_status()
        ou_a = resp.json()

        actual_username = ou_a.get("username", "")
        actual_state = ou_a.get("state", "")

        report.status_snapshot(
            "Waldur A offering user after sync",
            {
                "uuid": ou_a_uuid,
                "username": actual_username or "(empty)",
                "state": actual_state,
                "expected_username": TEST_USERNAME_B,
            },
        )

        assert actual_username == TEST_USERNAME_B, (
            f"Expected Waldur A offering user username to be '{TEST_USERNAME_B}' "
            f"(from Waldur B), but got '{actual_username}'"
        )
        report.text(
            f"Username `{TEST_USERNAME_B}` successfully propagated from Waldur B to A."
        )

    def test_06_idempotent_second_sync(
        self, offering, waldur_client_a, backend, report
    ):
        """Running sync again should be a no-op (idempotent)."""
        report.heading(2, "Username Sync: Idempotency Check")

        if not self.__class__._state.get("ou_b_uuid"):
            pytest.skip("No Waldur B offering user")

        changed = backend.sync_offering_user_usernames(
            waldur_a_offering_uuid=offering.waldur_offering_uuid,
            waldur_rest_client=waldur_client_a,
        )

        report.text(f"**Second sync returned:** `{changed}`")

        assert not changed, (
            "Second sync should return False (no changes), "
            "but it returned True"
        )
        report.text("Second sync was a no-op — idempotency confirmed.")

    def test_07_cleanup(
        self, waldur_client_a, waldur_client_b, backend, report
    ):
        """Clean up offering users created during the test."""
        report.heading(2, "Username Sync: Cleanup")

        # Restore original match field if we overrode it
        original_match_field = self.__class__._state.get("original_match_field")
        if original_match_field:
            backend.user_match_field = original_match_field
            backend._user_uuid_cache.clear()
            report.text(
                f"Restored backend match field to `{original_match_field}`"
            )

        ou_a_uuid = self.__class__._state.get("ou_a_uuid")
        ou_b_uuid = self.__class__._state.get("ou_b_uuid")
        ou_a_created = self.__class__._state.get("ou_a_created", False)
        ou_b_created = self.__class__._state.get("ou_b_created", False)
        ou_a_original = self.__class__._state.get("ou_a_original_username", "")

        # Waldur A: delete if we created it, otherwise restore original username
        if ou_a_uuid:
            if ou_a_created:
                status = _delete_offering_user(waldur_client_a, ou_a_uuid)
                report.text(
                    f"Deleted Waldur A offering user `{ou_a_uuid}` (status: {status})"
                )
            else:
                # Restore original username
                resp = waldur_client_a.get_httpx_client().patch(
                    f"/api/marketplace-offering-users/{ou_a_uuid}/",
                    json={"username": ou_a_original},
                )
                if resp.status_code < 400:
                    report.text(
                        f"Restored Waldur A offering user `{ou_a_uuid}` "
                        f"username to `{ou_a_original}`"
                    )
                else:
                    report.text(
                        f"WARNING: Failed to restore Waldur A offering user username "
                        f"(status {resp.status_code})"
                    )

        # Waldur B: delete if we created it, otherwise restore original username
        if ou_b_uuid:
            if ou_b_created:
                status = _delete_offering_user(waldur_client_b, ou_b_uuid)
                report.text(
                    f"Deleted Waldur B offering user `{ou_b_uuid}` (status: {status})"
                )
            else:
                ou_b_original = self.__class__._state.get(
                    "ou_b_original_username", ""
                )
                resp = waldur_client_b.get_httpx_client().patch(
                    f"/api/marketplace-offering-users/{ou_b_uuid}/",
                    json={"username": ou_b_original},
                )
                if resp.status_code < 400:
                    report.text(
                        f"Restored Waldur B offering user `{ou_b_uuid}` "
                        f"username to `{ou_b_original}`"
                    )
                else:
                    report.text(
                        f"WARNING: Failed to restore Waldur B offering user username "
                        f"(status {resp.status_code})"
                    )

        report.text("Cleanup complete.")
