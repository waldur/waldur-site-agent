"""Test for race condition: offering user creation vs order processing.

Scenario
--------
A project has 5 users.  When the site agent processes a CREATE order,
only 2 of the 5 users have their OfferingUser usernames set (the celery
task on Waldur hasn't finished yet).  The remaining 3 get their usernames
later via ``username_set`` STOMP messages.

Phase 1 validates the **issue** on code without the fix: after order
processing only the early users are in SLURM, and sending username_set
messages does NOT add the late users (the action is unrecognised).

Phase 2 validates the **fix** (this MR): the same username_set messages
now create the missing SLURM associations.

Uses the real SLURM emulator backend; all Waldur API calls are mocked.
"""

from __future__ import annotations

import importlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest
from waldur_api_client import models
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent.common.structures import Offering
from waldur_site_agent_slurm.backend import SlurmBackend

# ---------------------------------------------------------------------------
# Deferred imports — _add_user_to_resources only exists with the MR applied.
# _process_offering_user_message exists on both main and MR.
# ---------------------------------------------------------------------------


def _get_handler_func(name):
    """Import a function from handlers at call time, or return None."""
    try:
        mod = importlib.import_module("waldur_site_agent.event_processing.handlers")
        return getattr(mod, name, None)
    except Exception:
        return None


HAS_USERNAME_SET_FIX = _get_handler_func("_add_user_to_resources") is not None

requires_fix = pytest.mark.skipif(
    not HAS_USERNAME_SET_FIX,
    reason="_add_user_to_resources not available (MR not applied)",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACCOUNT_NAME = "test_alloc_race"
EARLY_USERS = ["alice", "bob"]
LATE_USERS = ["charlie", "dave", "eve"]
ALL_USERS = EARLY_USERS + LATE_USERS

SLURM_BIN_PATH = ".venv/bin"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _emulator_available() -> bool:
    sacctmgr = Path(SLURM_BIN_PATH) / "sacctmgr"
    return sacctmgr.exists()


pytestmark = pytest.mark.skipif(
    not _emulator_available(),
    reason="SLURM emulator not installed (run uv sync --all-packages)",
)


@pytest.fixture()
def offering() -> Offering:
    return Offering(
        waldur_offering_uuid="d629d5e45567425da9cdbdc1af67b32c",
        name="race-condition-test",
        waldur_api_url="https://waldur.example.com/api/",
        waldur_api_token="fake-token",
        backend_type="slurm",
        order_processing_backend="slurm",
        membership_sync_backend="slurm",
        reporting_backend="slurm",
        backend_settings={
            "default_account": "root",
            "customer_prefix": "hpc_",
            "project_prefix": "hpc_",
            "allocation_prefix": "hpc_",
            "slurm_bin_path": SLURM_BIN_PATH,
            # No cluster_name — the emulator's show-association filter has
            # a bug with cluster= scoping; omitting it uses the default cluster.
            "enable_user_homedir_account_creation": False,
        },
        backend_components={
            "cpu": {
                "limit": 10,
                "measured_unit": "k-Hours",
                "unit_factor": 60000,
                "accounting_type": "limit",
                "label": "CPU",
            },
            "mem": {
                "limit": 10,
                "measured_unit": "gb-Hours",
                "unit_factor": 61440,
                "accounting_type": "usage",
                "label": "RAM",
            },
        },
    )


@pytest.fixture()
def slurm_backend(offering) -> SlurmBackend:
    """Create a SlurmBackend using the emulator, with a clean state."""
    state_file = Path("/tmp/slurm_emulator_db.json")
    if state_file.exists():
        state_file.unlink()

    backend = SlurmBackend(offering.backend_settings, offering.backend_components_dict)
    backend.client.create_resource(ACCOUNT_NAME, "test allocation", "test_org")
    return backend


@pytest.fixture()
def waldur_resource() -> models.Resource:
    """A mock Waldur resource pointing to the emulator account."""
    return models.Resource(
        uuid=uuid.uuid4(),
        name="test-alloc-race",
        backend_id=ACCOUNT_NAME,
        resource_uuid=uuid.uuid4(),
        offering_type="Marketplace.Slurm",
        downscaled=False,
        state=ResourceState.OK,
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        last_sync=datetime(2024, 1, 1, tzinfo=timezone.utc),
        restrict_member_access=False,
        limits=ResourceLimits.from_dict({"cpu": 50, "mem": 200}),
        project_uuid=uuid.uuid4(),
        project_name="Test project",
        project_slug="test-project",
        customer_uuid=uuid.uuid4(),
        customer_name="Test customer",
        customer_slug="test-customer",
    )


def _make_username_set_message(username, resource_backend_ids):
    """Build a minimal OfferingUserMessage with action=username_set."""
    return {
        "offering_user_uuid": uuid.uuid4().hex,
        "user_uuid": uuid.uuid4().hex,
        "username": username,
        "action": "username_set",
        "offering_uuid": "d629d5e45567425da9cdbdc1af67b32c",
        "attributes": {},
        "changed_attributes": [],
        "resource_backend_ids": resource_backend_ids,
    }


# ---------------------------------------------------------------------------
# Phase 1 — Validate the issue (works on main without the fix)
# ---------------------------------------------------------------------------


class TestRaceConditionBug:
    """Demonstrates the race condition: order processing misses late users,
    and on code without the fix, username_set messages do NOT help."""

    def test_order_processing_only_adds_early_users(
        self, slurm_backend, waldur_resource
    ):
        """Only users with usernames at order-processing time are in SLURM."""
        client = slurm_backend.client

        # Simulate _add_users_to_resource (processors.py): it builds
        #   {ou.username for ou in offering_users if ou.state == OK}
        # During the race, only EARLY_USERS have usernames set.
        slurm_backend.add_users_to_resource(waldur_resource, set(EARLY_USERS))

        for username in EARLY_USERS:
            assert client.get_association(username, ACCOUNT_NAME) is not None, (
                f"Early user '{username}' should be in SLURM"
            )
        for username in LATE_USERS:
            assert client.get_association(username, ACCOUNT_NAME) is None, (
                f"Late user '{username}' should NOT be in SLURM yet"
            )

    def test_username_set_messages_are_noop_without_fix(
        self, offering, slurm_backend, waldur_resource, caplog
    ):
        """On main (without the MR), username_set falls through to the
        'Unknown offering user action' branch and does NOT add the users.

        This confirms the race condition is unresolved on main.
        On the MR branch this test still passes — the action IS handled,
        but we mock out the backend call so nothing reaches SLURM,
        keeping the assertion that late users remain absent.
        """
        client = slurm_backend.client

        # Reproduce the race: only early users in SLURM
        slurm_backend.add_users_to_resource(waldur_resource, set(EARLY_USERS))

        # Now simulate what Waldur sends when a late user's username is set.
        # On main: action="username_set" → else: warning("Unknown ...")
        # On MR:   action="username_set" → _add_user_to_resources(...)
        #          but we mock the backend call so SLURM is not touched.
        process_fn = _get_handler_func("_process_offering_user_message")
        assert process_fn is not None, "_process_offering_user_message should exist"

        # Patch get_client and register_event_process_service to avoid real API calls.
        # On MR branch, also patch _add_user_to_resources so it's a no-op.
        stack = [
            mock.patch(
                "waldur_site_agent.event_processing.handlers.common_utils.get_client",
                return_value=mock.Mock(),
            ),
            mock.patch(
                "waldur_site_agent.event_processing.handlers.register_event_process_service",
            ),
        ]

        # If the fix exists, mock it out so the test stays a pure "bug demo"
        if HAS_USERNAME_SET_FIX:
            stack.append(
                mock.patch(
                    "waldur_site_agent.event_processing.handlers._add_user_to_resources",
                )
            )

        with caplog.at_level(logging.WARNING):
            cms = [p.start() for p in stack]
            try:
                for username in LATE_USERS:
                    msg = _make_username_set_message(username, [ACCOUNT_NAME])
                    process_fn(msg, offering, "test-agent")
            finally:
                for p in stack:
                    p.stop()

        # Late users are STILL absent from SLURM — the message didn't help
        for username in LATE_USERS:
            assert client.get_association(username, ACCOUNT_NAME) is None, (
                f"Late user '{username}' should still NOT be in SLURM "
                "(username_set did not create the association)"
            )

        # On main: we expect the "Unknown" warning for each late user
        if not HAS_USERNAME_SET_FIX:
            unknown_warnings = [
                r for r in caplog.records
                if "Unknown offering user action" in r.message
            ]
            assert len(unknown_warnings) == len(LATE_USERS), (
                f"Expected {len(LATE_USERS)} 'Unknown offering user action' warnings, "
                f"got {len(unknown_warnings)}"
            )


# ---------------------------------------------------------------------------
# Phase 2 — Validate the fix (requires MR with _add_user_to_resources)
# ---------------------------------------------------------------------------


@requires_fix
class TestUsernameSetFix:
    """Validates that the username_set handler compensates for the race
    condition by adding late users to SLURM when their usernames are set."""

    def _get_add_user_fn(self):
        fn = _get_handler_func("_add_user_to_resources")
        assert fn is not None
        return fn

    def test_username_set_adds_late_users(
        self, offering, slurm_backend, waldur_resource
    ):
        """After username_set messages, ALL users have SLURM associations."""
        client = slurm_backend.client
        add_user_to_resources = self._get_add_user_fn()

        # Reproduce the race: only early users in SLURM
        slurm_backend.add_users_to_resource(waldur_resource, set(EARLY_USERS))

        # Process username_set for the late users — the real handler function
        mock_waldur_client = mock.Mock()

        with mock.patch(
            "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering",
            return_value=(slurm_backend, "1.0"),
        ), mock.patch(
            "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
        ) as mock_resources_mod:
            mock_resources_mod.sync_all.return_value = [waldur_resource]

            for username in LATE_USERS:
                add_user_to_resources(
                    offering, username, [ACCOUNT_NAME], mock_waldur_client
                )

        # ALL users now have SLURM associations
        for username in ALL_USERS:
            assert client.get_association(username, ACCOUNT_NAME) is not None, (
                f"User '{username}' should be in SLURM after username_set fix"
            )

    def test_username_set_skips_restricted_resource(
        self, offering, slurm_backend, waldur_resource
    ):
        """username_set must respect restrict_member_access."""
        add_user_to_resources = self._get_add_user_fn()

        restricted_resource = models.Resource(
            uuid=waldur_resource.uuid,
            name=waldur_resource.name,
            backend_id=ACCOUNT_NAME,
            resource_uuid=waldur_resource.resource_uuid,
            offering_type="Marketplace.Slurm",
            downscaled=False,
            state=ResourceState.OK,
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_sync=datetime(2024, 1, 1, tzinfo=timezone.utc),
            restrict_member_access=True,
            limits=ResourceLimits.from_dict({"cpu": 50, "mem": 200}),
            project_uuid=waldur_resource.project_uuid,
            project_name="Test project",
            project_slug="test-project",
            customer_uuid=waldur_resource.customer_uuid,
            customer_name="Test customer",
            customer_slug="test-customer",
        )

        with mock.patch(
            "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering",
            return_value=(slurm_backend, "1.0"),
        ), mock.patch(
            "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
        ) as mock_resources_mod:
            mock_resources_mod.sync_all.return_value = [restricted_resource]

            add_user_to_resources(
                offering, "restricted_user", [ACCOUNT_NAME], mock.Mock()
            )

        assert slurm_backend.client.get_association("restricted_user", ACCOUNT_NAME) is None, (
            "User should NOT be added to a restricted resource"
        )

    def test_username_set_idempotent(
        self, offering, slurm_backend, waldur_resource
    ):
        """Calling username_set for an already-present user must not fail."""
        add_user_to_resources = self._get_add_user_fn()
        client = slurm_backend.client

        slurm_backend.add_user(waldur_resource, "idempotent_user")
        assert client.get_association("idempotent_user", ACCOUNT_NAME) is not None

        with mock.patch(
            "waldur_site_agent.event_processing.handlers.common_utils.get_backend_for_offering",
            return_value=(slurm_backend, "1.0"),
        ), mock.patch(
            "waldur_site_agent.event_processing.handlers.marketplace_provider_resources_list"
        ) as mock_resources_mod:
            mock_resources_mod.sync_all.return_value = [waldur_resource]

            add_user_to_resources(
                offering, "idempotent_user", [ACCOUNT_NAME], mock.Mock()
            )

        assert client.get_association("idempotent_user", ACCOUNT_NAME) is not None
