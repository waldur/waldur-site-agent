r"""End-to-end tests: which backend users membership sync removes, and which it keeps.

Runs the real ``OfferingMembershipProcessor`` against Waldur and the SLURM
emulator. Members leave and rejoin through the Waldur API, so their offering
users go through the real lifecycle driven by the Mastermind worker (the
offering has ``offering_user_auto_deletion``), instead of being faked on the
SLURM side the way ``TestStaleUserRemoval`` in ``test_e2e_resources_sync.py`` does.

Regressions guarded here:

  * gh-13 -- a member who left their last project has an offering user in
    "Requested deletion", which drops out of the agent's state-filtered
    offering-user list. They must still be removed, with
    ``preserve_unmanaged_backend_users`` off and on.
  * Service and course accounts are not project-team members. Membership sync
    must not remove active ones (full sync removed and re-added them, cancelling
    their jobs; the event-driven sync removed them for good). CLOSED ones must
    still be removed.
  * ``preserve_unmanaged_backend_users`` keeps accounts Waldur has never seen
    (added by hand with sacctmgr), but still removes every user Waldur knows,
    including restricted offering users.

Tests run in order and share one resource; each builds on the previous state.

Preset fixtures (ci/site_agent_e2e.json): project ``e2eb...0002`` with members
e2euser6 / e2euser7, a restricted offering user for e2euser8 (no project role),
one OK and one CLOSED project service account, and one OK course account.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_MEMBERSHIP_CONFIG=<path-to-config.yaml>   (ci/e2e-ci-config-membership.yaml)

Usage:
    WALDUR_E2E_TESTS=true \
    WALDUR_E2E_MEMBERSHIP_CONFIG=ci/e2e-ci-config-membership.yaml \
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_membership_stale.py -v
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import Unset

from plugins.slurm.tests.e2e.conftest import (
    ReportWriter,
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)
from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.utils import load_configuration

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_MEMBERSHIP_CONFIG_PATH = os.environ.get("WALDUR_E2E_MEMBERSHIP_CONFIG", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="E2E tests disabled")

PROJECT_UUID = "e2eb0000000000000000000000000002"
MEMBER_ROLE = "PROJECT.MEMBER"

# Waldur user UUID -> offering username on the membership offering.
STAYING_USER_UUID = "e2ea0000000000000000000000000007"
STAYING_MEMBER = "e2emember6"
LEAVING_USER_UUID = "e2ea0000000000000000000000000008"
LEAVING_MEMBER = "e2emember7"
RESTRICTED_USER = "e2erestricted8"
SERVICE_ACCOUNT_OK = "e2e-svc-ok"
SERVICE_ACCOUNT_CLOSED = "e2e-svc-closed"
COURSE_ACCOUNT = "e2e-course-01"

# States in which the offering user is gone from the agent's filtered list.
DEPARTED_STATES = {
    OfferingUserState.REQUESTED_DELETION.value,
    OfferingUserState.DELETING.value,
    OfferingUserState.DELETED.value,
}
OFFERING_USER_TIMEOUT = 90


# ---------------------------------------------------------------------------
# Fixtures (override the conftest config/report with this suite's config)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config():
    if not E2E_MEMBERSHIP_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_MEMBERSHIP_CONFIG not set")
    return load_configuration(E2E_MEMBERSHIP_CONFIG_PATH, user_agent_suffix="e2e-membership")


@pytest.fixture(scope="module")
def report(offering) -> Iterator[ReportWriter]:
    rw = ReportWriter()
    rw.heading(1, "E2E Membership Stale-User Test Report")
    rw.text(f"**Date:** {datetime.now(tz=timezone.utc).isoformat()}")
    rw.text(f"**Config:** `{E2E_MEMBERSHIP_CONFIG_PATH}`")
    rw.text(f"**Waldur:** {offering.waldur_api_url}")
    rw.text(f"**Offering:** {offering.waldur_offering_uuid}")
    rw.text("")
    yield rw
    stem = Path(E2E_MEMBERSHIP_CONFIG_PATH).stem or "e2e-membership"
    rw.write(Path(__file__).parent / f"{stem}-report.md")


@pytest.fixture(autouse=True)
def _flag_off_between_tests(offering) -> Iterator[None]:
    """Each test sets the flag it needs; never let one leak into the next."""
    yield
    offering.preserve_unmanaged_backend_users = False


@pytest.fixture(scope="module")
def membership_resource(offering, waldur_client, slurm_backend, report) -> Iterator[dict]:
    """One SLURM allocation in the membership project, after an initial sync."""
    report.heading(2, "Setup: provision the membership-test resource")
    offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
    project_url = get_project_url(waldur_client, PROJECT_UUID)
    order_uuid = create_source_order(
        waldur_client,
        offering_url,
        project_url,
        plan_url,
        limits={"cpu": 100, "ram": 10},
        name=f"membership-stale-{int(time.time())}",
    )
    state = run_processor_until_order_terminal(
        offering, waldur_client, slurm_backend, order_uuid, report=report
    )
    assert state == OrderState.DONE, f"Setup order {order_uuid} did not complete: {state}"

    order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=UUID(order_uuid))
    assert isinstance(order.marketplace_resource_uuid, UUID), order
    resource_uuid = order.marketplace_resource_uuid.hex
    resource = marketplace_provider_resources_retrieve.sync(
        uuid=UUID(resource_uuid), client=waldur_client
    )
    assert resource.backend_id, f"Resource {resource_uuid} has no backend_id"

    _membership_sync(offering, waldur_client, slurm_backend, preserve=False)
    report.text(
        f"Resource `{resource_uuid}` → account `{resource.backend_id}`, "
        f"users {_backend_users(slurm_backend, resource.backend_id)}\n"
    )

    yield {"uuid": resource_uuid, "backend_id": resource.backend_id}

    # Leave Waldur as the preset had it, so a local rerun starts from the same state.
    if _offering_user_state(waldur_client, offering.uuid, LEAVING_USER_UUID) in DEPARTED_STATES:
        _set_project_member(waldur_client, LEAVING_USER_UUID, member=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _backend_users(slurm_backend, backend_id: str) -> set[str]:
    return set(slurm_backend.client.list_resource_users(backend_id))


def _membership_sync(offering, waldur_client, slurm_backend, *, preserve: bool) -> set[str]:
    """Run one full membership sync; return every username it asked the backend to remove.

    The removal spy matters for accounts that full sync removes and re-adds in the
    same pass: the emulator's end state looks unchanged, but their jobs were cancelled.
    """
    offering.preserve_unmanaged_backend_users = preserve
    processor = OfferingMembershipProcessor(offering, waldur_client, resource_backend=slurm_backend)
    with _removal_spy(slurm_backend) as removed:
        processor.process_offering()
    return removed


@contextmanager
def _removal_spy(slurm_backend) -> Iterator[set[str]]:
    """Record every username passed to the backend's remove_users_from_resource."""
    removed: set[str] = set()
    original = slurm_backend.remove_users_from_resource

    def spy(resource: WaldurResource, usernames: set[str], **kwargs: dict) -> list[str]:
        removed.update(usernames)
        return original(resource, usernames, **kwargs)

    with mock.patch.object(slurm_backend, "remove_users_from_resource", side_effect=spy):
        yield removed


def _set_project_member(waldur_client, user_uuid: str, *, member: bool) -> None:
    action = "add_user" if member else "delete_user"
    response = waldur_client.get_httpx_client().post(
        f"/api/projects/{PROJECT_UUID}/{action}/",
        json={"user": user_uuid, "role": MEMBER_ROLE},
    )
    assert response.status_code in (200, 201), (
        f"{action} for {user_uuid} failed: {response.status_code} {response.text}"
    )


def _offering_user_state(waldur_client, offering_uuid: str, user_uuid: str) -> str:
    response = waldur_client.get_httpx_client().get(
        "/api/marketplace-offering-users/",
        params={"offering_uuid": offering_uuid, "user_uuid": user_uuid},
    )
    response.raise_for_status()
    rows = response.json()
    assert len(rows) == 1, f"Expected one offering user for {user_uuid}, got {rows}"
    return rows[0]["state"]


def _wait_offering_user_state(
    waldur_client, offering_uuid: str, user_uuid: str, states: set[str]
) -> str:
    """Poll until the worker has moved the offering user into one of ``states``."""
    deadline = time.monotonic() + OFFERING_USER_TIMEOUT
    state = ""
    while time.monotonic() < deadline:
        state = _offering_user_state(waldur_client, offering_uuid, user_uuid)
        if state in states:
            return state
        time.sleep(2)
    pytest.fail(f"Offering user of {user_uuid} still '{state}', expected one of {states}")


def _resource_state(waldur_client, resource_uuid: str) -> Optional[ResourceState]:
    resource = marketplace_provider_resources_retrieve.sync(
        uuid=UUID(resource_uuid), client=waldur_client
    )
    return None if isinstance(resource.state, Unset) else resource.state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMembershipStaleUsers:
    def test_01_baseline(self, slurm_backend, membership_resource):
        """Members, the OK service account and the OK course account are associated."""
        users = _backend_users(slurm_backend, membership_resource["backend_id"])
        assert users == {STAYING_MEMBER, LEAVING_MEMBER, SERVICE_ACCOUNT_OK, COURSE_ACCOUNT}

    def test_02_full_sync_does_not_remove_active_accounts(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        backend_id = membership_resource["backend_id"]

        removed = _membership_sync(offering, waldur_client, slurm_backend, preserve=False)

        assert not removed & {SERVICE_ACCOUNT_OK, COURSE_ACCOUNT}, (
            f"Full sync removed active accounts (and cancelled their jobs): {removed}"
        )
        assert {SERVICE_ACCOUNT_OK, COURSE_ACCOUNT} <= _backend_users(slurm_backend, backend_id)

    def test_03_event_sync_does_not_remove_active_accounts(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        """The event-driven path has no account sync to re-add a wrongly removed account."""
        processor = OfferingMembershipProcessor(
            offering, waldur_client, resource_backend=slurm_backend
        )

        with _removal_spy(slurm_backend) as removed:
            processor.process_resource_user_sync(membership_resource["uuid"])

        assert not removed & {SERVICE_ACCOUNT_OK, COURSE_ACCOUNT}, removed
        users = _backend_users(slurm_backend, membership_resource["backend_id"])
        assert {SERVICE_ACCOUNT_OK, COURSE_ACCOUNT} <= users

    def test_04_closed_service_account_is_removed(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        backend_id = membership_resource["backend_id"]
        slurm_backend.client.create_association(SERVICE_ACCOUNT_CLOSED, backend_id)

        _membership_sync(offering, waldur_client, slurm_backend, preserve=False)

        assert SERVICE_ACCOUNT_CLOSED not in _backend_users(slurm_backend, backend_id)

    def test_05_flag_off_removes_hand_added_user(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        backend_id = membership_resource["backend_id"]
        slurm_backend.client.create_association("e2e-manual-a", backend_id)

        _membership_sync(offering, waldur_client, slurm_backend, preserve=False)

        assert "e2e-manual-a" not in _backend_users(slurm_backend, backend_id)

    def test_06_flag_on_keeps_hand_added_user(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        backend_id = membership_resource["backend_id"]
        slurm_backend.client.create_association("e2e-manual-b", backend_id)

        for _ in range(2):
            removed = _membership_sync(offering, waldur_client, slurm_backend, preserve=True)
            assert "e2e-manual-b" not in removed

        assert _backend_users(slurm_backend, backend_id) == {
            STAYING_MEMBER,
            LEAVING_MEMBER,
            SERVICE_ACCOUNT_OK,
            COURSE_ACCOUNT,
            "e2e-manual-b",
        }

    def test_07_flag_on_removes_restricted_offering_user(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        """Restricted offering users are filtered from the agent's list but are still known."""
        backend_id = membership_resource["backend_id"]
        slurm_backend.client.create_association(RESTRICTED_USER, backend_id)

        _membership_sync(offering, waldur_client, slurm_backend, preserve=True)

        users = _backend_users(slurm_backend, backend_id)
        assert RESTRICTED_USER not in users
        assert "e2e-manual-b" in users

    def test_08_flag_on_removes_member_who_left(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        """gh-13 with the flag on: the departed member is no longer in the filtered list."""
        backend_id = membership_resource["backend_id"]
        _set_project_member(waldur_client, LEAVING_USER_UUID, member=False)
        _wait_offering_user_state(waldur_client, offering.uuid, LEAVING_USER_UUID, DEPARTED_STATES)

        # Precondition: this is the gh-13 situation, not a user the agent still sees.
        processor = OfferingMembershipProcessor(
            offering, waldur_client, resource_backend=slurm_backend
        )
        visible = {ou.username for ou in processor._get_waldur_offering_users()}
        assert LEAVING_MEMBER not in visible

        _membership_sync(offering, waldur_client, slurm_backend, preserve=True)

        users = _backend_users(slurm_backend, backend_id)
        assert LEAVING_MEMBER not in users
        assert {STAYING_MEMBER, "e2e-manual-b"} <= users

    def test_09_rejoined_member_is_restored(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        """Rejoining reuses the offering user and its username."""
        backend_id = membership_resource["backend_id"]
        _set_project_member(waldur_client, LEAVING_USER_UUID, member=True)
        _wait_offering_user_state(
            waldur_client, offering.uuid, LEAVING_USER_UUID, {OfferingUserState.OK.value}
        )

        removed = _membership_sync(offering, waldur_client, slurm_backend, preserve=True)

        assert LEAVING_MEMBER not in removed
        assert LEAVING_MEMBER in _backend_users(slurm_backend, backend_id)

    def test_10_flag_off_removes_member_who_left(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        """gh-13 with the default setting, through the real offering-user lifecycle."""
        backend_id = membership_resource["backend_id"]
        _set_project_member(waldur_client, LEAVING_USER_UUID, member=False)
        _wait_offering_user_state(waldur_client, offering.uuid, LEAVING_USER_UUID, DEPARTED_STATES)

        _membership_sync(offering, waldur_client, slurm_backend, preserve=False)

        assert _backend_users(slurm_backend, backend_id) == {
            STAYING_MEMBER,
            SERVICE_ACCOUNT_OK,
            COURSE_ACCOUNT,
        }

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "!569: a failed known-username fetch propagates and marks the resource "
            "ERRED; it should skip removals for the pass instead"
        ),
    )
    def test_11_known_username_fetch_failure_skips_removals(
        self, offering, waldur_client, slurm_backend, membership_resource
    ):
        backend_id = membership_resource["backend_id"]
        slurm_backend.client.create_association("e2e-manual-c", backend_id)

        try:
            with mock.patch.object(
                OfferingMembershipProcessor,
                "_get_known_offering_usernames",
                side_effect=RuntimeError("offering-user listing failed"),
            ):
                _membership_sync(offering, waldur_client, slurm_backend, preserve=True)

            assert _resource_state(waldur_client, membership_resource["uuid"]) != (
                ResourceState.ERRED
            )
            assert "e2e-manual-c" in _backend_users(slurm_backend, backend_id)
        finally:
            # A normal pass clears ERRED again (and removes e2e-manual-c).
            _membership_sync(offering, waldur_client, slurm_backend, preserve=False)
