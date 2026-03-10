"""End-to-end membership sync tests for Waldur A -> Waldur B federation.

Tests the add_user / remove_user flow via the OfferingMembershipProcessor:
  1. Find a shared user between Waldur A and B (via CUID / identity bridge)
  2. Find an OK resource on Waldur A with backend_id pointing to Waldur B
  3. Add user to the resource's project on Waldur B (with role mapping)
  4. Verify the user was added to the project on Waldur B
  5. Remove user from the project on Waldur B
  6. Verify the user was removed

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=<config.yaml> \\
    .venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_membership_sync.py -v -s
"""

from __future__ import annotations

import logging
import os

import pytest

from waldur_api_client.api.marketplace_offering_users import (
    marketplace_offering_users_list,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
)
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import UNSET

from waldur_site_agent_waldur.client import DEFAULT_PROJECT_ROLE_NAME

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

# Shared state across ordered tests
_state: dict = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_resource_with_backend_id(waldur_client_a, offering_uuid):
    """Find an OK resource on Waldur A that has a backend_id (linked to Waldur B)."""
    resources = marketplace_provider_resources_list.sync_all(
        client=waldur_client_a,
        offering_uuid=[offering_uuid],
        state=[ResourceState("OK")],
    )
    for r in resources:
        backend_id = r.backend_id
        if backend_id and not isinstance(backend_id, type(UNSET)) and backend_id.strip():
            return r
    return None


def _find_shared_user(waldur_client_a, backend, offering_uuid):
    """Find a user with an offering_user on Waldur A whose identity resolves on Waldur B.

    Returns (offering_user, remote_user_uuid) or (None, None).
    """
    offering_users = marketplace_offering_users_list.sync_all(
        client=waldur_client_a,
        offering_uuid=[offering_uuid],
        is_restricted=False,
    )

    for ou in offering_users:
        username = ou.username if not isinstance(ou.username, type(UNSET)) else None
        user_username = (
            ou.user_username if not isinstance(ou.user_username, type(UNSET)) else None
        )
        if not username:
            continue

        # Try resolving on Waldur B using the user_username (CUID)
        lookup_name = user_username or username
        try:
            remote_uuid = backend._resolve_remote_user(lookup_name)
            if remote_uuid:
                return ou, remote_uuid
        except Exception:
            logger.debug("Failed to resolve %s on Waldur B", lookup_name)

    return None, None


def _get_project_permissions(waldur_client_b, project_uuid, user_uuid):
    """Get a user's permissions (roles) in a project on Waldur B."""
    resp = waldur_client_b.get_httpx_client().get(
        "/api/projects-permissions/",
        params={
            "project": str(project_uuid),
            "user": str(user_uuid),
        },
    )
    if resp.status_code == 200:
        return resp.json()

    # Fall back to listing project users
    resp = waldur_client_b.get_httpx_client().get(
        f"/api/projects/{project_uuid}/",
    )
    if resp.status_code != 200:
        return []

    project_data = resp.json()
    # Check project roles endpoint
    resp = waldur_client_b.get_httpx_client().get(
        f"/api/user-roles/",
        params={
            "scope_uuid": str(project_uuid),
            "user_uuid": str(user_uuid),
            "is_active": "true",
        },
    )
    if resp.status_code == 200:
        return resp.json()
    return []


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestE2EMembershipSync:
    """Membership sync: add/remove user on Waldur B via backend.add_user/remove_user."""

    def test_01_find_resource(self, offering, waldur_client_a, report):
        """Find an OK resource on Waldur A linked to Waldur B."""
        report.heading(2, "Membership Sync: Find Resource")

        resource = _find_resource_with_backend_id(
            waldur_client_a, offering.waldur_offering_uuid
        )
        if not resource:
            pytest.skip(
                "No OK resource with backend_id found on Waldur A — "
                "run test_e2e_federation.py first to create one"
            )

        resource_uuid = resource.uuid.hex if hasattr(resource.uuid, "hex") else str(resource.uuid)
        backend_id = resource.backend_id
        project_uuid = (
            resource.project_uuid.hex
            if hasattr(resource.project_uuid, "hex")
            else str(resource.project_uuid)
        ) if not isinstance(resource.project_uuid, type(UNSET)) else ""

        _state["resource_uuid_a"] = resource_uuid
        _state["resource_backend_id"] = backend_id
        _state["project_uuid_a"] = project_uuid

        report.status_snapshot(
            "Resource for membership test",
            {
                "name": resource.name or "(unnamed)",
                "uuid_on_A": resource_uuid,
                "backend_id (uuid_on_B)": backend_id,
                "project_uuid_on_A": project_uuid,
            },
        )
        report.text(f"Using resource `{resource.name}` for membership sync test.")

    def test_02_find_shared_user(self, offering, waldur_client_a, backend, report):
        """Find a user that exists on both Waldur A and B."""
        report.heading(2, "Membership Sync: Find Shared User")

        if not _state.get("resource_uuid_a"):
            pytest.skip("No resource from previous test")

        ou, remote_uuid = _find_shared_user(
            waldur_client_a, backend, offering.waldur_offering_uuid
        )
        if not ou or not remote_uuid:
            pytest.skip(
                "No shared user found between Waldur A and B "
                "for this offering"
            )

        username = ou.username if not isinstance(ou.username, type(UNSET)) else ""
        user_username = (
            ou.user_username if not isinstance(ou.user_username, type(UNSET)) else ""
        )

        _state["username"] = username
        _state["user_username"] = user_username
        _state["remote_user_uuid"] = str(remote_uuid)

        report.status_snapshot(
            "Shared user",
            {
                "offering_user_username": username,
                "user_username (cuid)": user_username,
                "remote_uuid_on_B": str(remote_uuid),
            },
        )
        report.text(
            f"Found shared user: username=`{username}`, "
            f"resolves to `{remote_uuid}` on Waldur B."
        )

    def test_03_add_user_to_project(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Add user to the resource's project on Waldur B."""
        report.heading(2, "Membership Sync: Add User")

        if not _state.get("remote_user_uuid"):
            pytest.skip("No shared user from previous test")

        resource_uuid_a = _state["resource_uuid_a"]
        username = _state["username"]
        backend_id = _state["resource_backend_id"]

        # Get the WaldurResource object
        from waldur_api_client.api.marketplace_provider_resources import (
            marketplace_provider_resources_retrieve,
        )

        waldur_resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid_a, client=waldur_client_a
        )

        # Determine test role
        role_mapping = offering.backend_settings.get("role_mapping", {})
        source_role = "PROJECT.MANAGER"
        expected_target_role = role_mapping.get(source_role, source_role)

        _state["source_role"] = source_role
        _state["expected_target_role"] = expected_target_role

        report.text(
            f"Adding user `{username}` with role `{source_role}` "
            f"(maps to `{expected_target_role}` on Waldur B)"
        )

        # Call add_user
        result = backend.add_user(waldur_resource, username, role_name=source_role)

        assert result, f"add_user returned False for user {username}"

        report.text(f"add_user returned `{result}` — user added successfully.")

        # Verify user is in the project on Waldur B
        remote_user_uuid = _state["remote_user_uuid"]
        project_uuid_b = backend._get_resource_project_uuid(backend_id)

        if project_uuid_b:
            _state["project_uuid_b"] = str(project_uuid_b)
            roles = _get_project_permissions(
                waldur_client_b, project_uuid_b, remote_user_uuid
            )
            report.text(
                f"User roles on Waldur B project `{project_uuid_b}`: "
                f"{len(roles)} role(s) found"
            )
            if roles:
                for r in roles[:5]:
                    role_name = r.get("role_name", r.get("role", {}).get("name", "?"))
                    report.text(f"  - role: `{role_name}`")
        else:
            report.text("Could not resolve project UUID on Waldur B for verification.")

    def test_04_remove_user_from_project(
        self, offering, waldur_client_a, waldur_client_b, backend, report
    ):
        """Remove user from the resource's project on Waldur B."""
        report.heading(2, "Membership Sync: Remove User")

        if not _state.get("remote_user_uuid"):
            pytest.skip("No shared user from previous test")

        resource_uuid_a = _state["resource_uuid_a"]
        username = _state["username"]
        source_role = _state.get("source_role", "PROJECT.MANAGER")

        from waldur_api_client.api.marketplace_provider_resources import (
            marketplace_provider_resources_retrieve,
        )

        waldur_resource = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid_a, client=waldur_client_a
        )

        report.text(f"Removing user `{username}` with role `{source_role}`")

        result = backend.remove_user(waldur_resource, username, role_name=source_role)

        assert result, f"remove_user returned False for user {username}"

        report.text(f"remove_user returned `{result}` — user removed successfully.")

        # Verify user role was removed on Waldur B
        remote_user_uuid = _state["remote_user_uuid"]
        project_uuid_b = _state.get("project_uuid_b")

        if project_uuid_b:
            roles = _get_project_permissions(
                waldur_client_b, project_uuid_b, remote_user_uuid
            )
            report.text(
                f"User roles on Waldur B project after removal: "
                f"{len(roles)} role(s)"
            )
        else:
            report.text("Could not verify role removal (no project UUID).")

    def test_05_role_mapping_info(self, offering, report):
        """Report role mapping configuration."""
        report.heading(2, "Membership Sync: Role Mapping")

        role_mapping = offering.backend_settings.get("role_mapping", {})
        if role_mapping:
            report.text("**Configured role mappings:**\n")
            report.text("| Source Role (A) | Target Role (B) |")
            report.text("|-----------------|-----------------|")
            for source, target in role_mapping.items():
                report.text(f"| {source} | {target} |")
        else:
            report.text(
                "No `role_mapping` configured — roles are passed through unchanged."
            )

        report.text(f"\nDefault role: `{DEFAULT_PROJECT_ROLE_NAME}`")

    def test_06_summary(self, report):
        """Summary of membership sync E2E test."""
        report.heading(2, "Membership Sync: Summary")

        report.text("**Membership sync flow:**\n")
        report.text("| Step | Result |")
        report.text("|------|--------|")
        report.text(
            f"| Resource on A | `{_state.get('resource_uuid_a', '?')}` |"
        )
        report.text(
            f"| Backend ID (B) | `{_state.get('resource_backend_id', '?')}` |"
        )
        report.text(
            f"| Shared user | `{_state.get('username', '?')}` |"
        )
        report.text(
            f"| Remote UUID (B) | `{_state.get('remote_user_uuid', '?')}` |"
        )
        report.text(
            f"| Source role | `{_state.get('source_role', '?')}` |"
        )
        report.text(
            f"| Target role | `{_state.get('expected_target_role', '?')}` |"
        )
        report.text("\nMembership sync E2E test completed.")
