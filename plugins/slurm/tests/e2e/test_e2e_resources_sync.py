"""End-to-end tests: forced offering resources sync after SLURM data loss (WAL-10023).

Scenario: the SLURM database is wiped (fully or partially) while Waldur still
knows all resources. The service provider triggers the new ``sync_resources``
offering action, which makes the agent run
``OfferingMembershipProcessor.process_offering(recreate_missing_resources=True)``.
These tests exercise that forced reconciliation against the SLURM emulator:

  1. Full data loss — every account is removed from the emulator; the forced
     sync must recreate all accounts with their original backend IDs, restore
     user associations and re-apply limits.
  2. Partial data loss — only one account is removed while another survives;
     the forced sync must recreate the missing account and leave the surviving
     one untouched (it must not be recreated from scratch).
  3. Partial association loss — the account survives but a user association
     is lost; the regular membership-sync part of the forced run must re-add
     the missing user.

The STOMP envelope (``on_offering_resources_sync_message_stomp``) is covered
by unit tests; the CI Waldur image does not yet publish the new event type,
so these tests drive the processor API directly.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=e2e-local-config.yaml \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_resources_sync.py -v -s
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import uuid
from pathlib import Path

import pytest
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.resource_state import ResourceState

from plugins.slurm.tests.e2e.conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)
from waldur_site_agent.common.processors import OfferingMembershipProcessor

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="E2E tests disabled")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


EMULATOR_STATE_FILE = Path("/tmp/slurm_emulator_db.json")  # noqa: S108


def _wipe_emulator_state(offering) -> None:
    """Simulate a total SLURM database loss (wiped slurmdbd).

    Deletes the emulator's persisted state file; the next sacctmgr invocation
    re-initializes it with only the default cluster and root account,
    mirroring a freshly re-initialized SLURM accounting database.
    (``sacctmgr cleanup all`` cannot be used: emulator 0.5.x exits 0 on
    unknown commands without wiping anything.)
    """
    EMULATOR_STATE_FILE.unlink(missing_ok=True)
    cluster_name = offering.backend_settings.get("cluster_name")
    if cluster_name:
        slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
        sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
        subprocess.check_output(
            [sacctmgr, "--immediate", "add", "cluster", cluster_name],
            stderr=subprocess.STDOUT,
            timeout=10,
        )


def _set_account_fairshare(offering, account: str, fairshare: int) -> None:
    """Set a marker fairshare on an account directly via sacctmgr.

    Fairshare is used as the tamper-marker for "this account was not
    recreated": the membership sync never touches it, while a recreated
    account gets the default fairshare back. (The account description cannot
    be used — emulator 0.5.x ignores description modifications.)
    """
    slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
    subprocess.check_output(
        [
            sacctmgr,
            "--immediate",
            "modify",
            "account",
            account,
            "set",
            f"fairshare={fairshare}",
        ],
        stderr=subprocess.STDOUT,
        timeout=10,
    )


def _get_account_fairshare(account: str) -> int | None:
    """Read an account's fairshare from the emulator state file.

    Reading the state file directly is the established pattern for emulator
    fields that ``sacctmgr list`` does not expose reliably (see
    ``get_account_qos`` in conftest).
    """
    if not EMULATOR_STATE_FILE.exists():
        return None
    state = json.loads(EMULATOR_STATE_FILE.read_text())
    account_data = (state.get("accounts") or {}).get(account)
    if not account_data:
        return None
    return account_data.get("fairshare")


def _snapshot_backend_resource(slurm_backend, backend_id: str) -> dict:
    """Capture the backend-side state of one account for later comparison.

    Note: with slurm-emulator 0.5.x ``get_resource_limits`` returns {} because
    the emulator does not persist GrpTRESMins; the limits comparison becomes
    meaningful once the emulator 0.6.0 bump (TRES parity) lands.
    """
    return {
        "users": sorted(slurm_backend.client.list_resource_users(backend_id)),
        "limits": slurm_backend.client.get_resource_limits(backend_id),
    }


def _run_forced_sync(offering, waldur_client, slurm_backend) -> None:
    """Run the forced reconciliation, as the sync_resources event handler does."""
    processor = OfferingMembershipProcessor(
        offering,
        waldur_client,
        resource_backend=slurm_backend,
    )
    processor.process_offering(recreate_missing_resources=True)


def _get_waldur_resource(waldur_client, resource_uuid: str):
    return marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=waldur_client)


def _require_baseline_accounts(slurm_backend, provisioned_resources) -> None:
    """Fail fast with a clear cause if a prior test left the backend broken.

    These data-loss tests share module-scoped emulator state and run in
    sequence. If an earlier test (e.g. TestFullDataLoss) failed after wiping
    accounts but before the forced sync restored them, the tests below would
    otherwise fail for a confusing, unrelated reason. This precondition makes
    the inherited-state cause explicit.
    """
    missing = [
        info["backend_id"]
        for info in provisioned_resources
        if slurm_backend.client.get_resource(info["backend_id"]) is None
    ]
    if missing:
        pytest.fail(
            f"Precondition failed: backend accounts missing before test ({missing}); "
            "a previous data-loss test likely left the emulator in a broken state."
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def provisioned_resources(offering, waldur_client, slurm_backend, project_uuid, report):
    """Create two SLURM allocations via orders and snapshot their backend state.

    Returns a list of dicts: {resource_uuid, backend_id, snapshot}.
    """
    report.heading(2, "Setup: provision two resources for data-loss tests")

    offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
    project_url = get_project_url(waldur_client, project_uuid)

    suffix = uuid.uuid4().hex[:6]
    resources = []
    for index in (1, 2):
        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100 * index, "ram": 10 * index},
            name=f"dataloss-{suffix}-{index}",
        )
        final_state = run_processor_until_order_terminal(
            offering,
            waldur_client,
            slurm_backend,
            order_uuid,
            report=report,
        )
        assert final_state == OrderState.DONE, (
            f"Setup order {order_uuid} did not complete: {final_state}"
        )

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        resource_uuid = order.marketplace_resource_uuid.hex
        resource = _get_waldur_resource(waldur_client, resource_uuid)
        assert resource.backend_id, f"Resource {resource_uuid} has no backend_id"
        resources.append({"resource_uuid": resource_uuid, "backend_id": resource.backend_id})
        report.text(f"Provisioned resource `{resource_uuid}` → account `{resource.backend_id}`\n")

    # Run a regular membership sync once so user associations are in place,
    # then snapshot the backend state as the restore baseline.
    processor = OfferingMembershipProcessor(
        offering,
        waldur_client,
        resource_backend=slurm_backend,
    )
    processor.process_offering()

    for info in resources:
        info["snapshot"] = _snapshot_backend_resource(slurm_backend, info["backend_id"])
        report.text(
            f"Baseline for `{info['backend_id']}`: users={info['snapshot']['users']}, "
            f"limits={info['snapshot']['limits']}\n"
        )

    return resources


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullDataLoss:
    """All SLURM accounts are lost; forced sync must restore everything."""

    def test_forced_sync_recreates_all_accounts(
        self, offering, waldur_client, slurm_backend, provisioned_resources, report
    ):
        report.heading(2, "Test: full SLURM data loss, forced sync restores all accounts")

        # --- Wipe the entire emulator state (slurmdbd loss) ---
        _wipe_emulator_state(offering)
        for info in provisioned_resources:
            assert slurm_backend.client.get_resource(info["backend_id"]) is None, (
                f"Account {info['backend_id']} should be gone after the wipe"
            )
        report.text("Emulator state wiped, all accounts gone\n")

        # --- Forced reconciliation (what the sync_resources event triggers) ---
        _run_forced_sync(offering, waldur_client, slurm_backend)

        # --- Accounts recreated with original backend IDs ---
        for info in provisioned_resources:
            backend_id = info["backend_id"]
            account = slurm_backend.client.get_resource(backend_id)
            assert account is not None, f"Account {backend_id} was not recreated"

            restored = _snapshot_backend_resource(slurm_backend, backend_id)
            assert restored["users"] == info["snapshot"]["users"], (
                f"User associations for {backend_id} not restored: "
                f"{restored['users']} != {info['snapshot']['users']}"
            )
            assert restored["limits"] == info["snapshot"]["limits"], (
                f"Limits for {backend_id} not restored: "
                f"{restored['limits']} != {info['snapshot']['limits']}"
            )
            report.text(
                f"Account `{backend_id}` restored: users={restored['users']}, "
                f"limits={restored['limits']}\n"
            )

        # --- Waldur resources stay healthy ---
        for info in provisioned_resources:
            resource = _get_waldur_resource(waldur_client, info["resource_uuid"])
            assert resource.state == ResourceState.OK, (
                f"Resource {info['resource_uuid']} not OK after restore: {resource.state}"
            )


class TestPartialDataLoss:
    """Only some accounts are lost; surviving accounts must not be recreated."""

    @pytest.fixture(autouse=True)
    def _baseline(self, slurm_backend, provisioned_resources):
        _require_baseline_accounts(slurm_backend, provisioned_resources)

    def test_forced_sync_recreates_only_missing_account(
        self, offering, waldur_client, slurm_backend, provisioned_resources, report
    ):
        report.heading(2, "Test: partial data loss, only the missing account is recreated")

        survivor, victim = provisioned_resources[0], provisioned_resources[1]

        # Mark the surviving account: the sync never touches fairshare, while
        # a recreated account would get the default fairshare back, so the
        # marker proves the account was not recreated.
        marker_fairshare = 42
        _set_account_fairshare(offering, survivor["backend_id"], marker_fairshare)
        assert _get_account_fairshare(survivor["backend_id"]) == marker_fairshare

        # Remove only the victim account
        slurm_backend.client.delete_resource(victim["backend_id"])
        assert slurm_backend.client.get_resource(victim["backend_id"]) is None
        assert slurm_backend.client.get_resource(survivor["backend_id"]) is not None
        report.text(
            f"Deleted `{victim['backend_id']}`, kept `{survivor['backend_id']}` "
            f"with marker fairshare\n"
        )

        # --- Forced reconciliation ---
        _run_forced_sync(offering, waldur_client, slurm_backend)

        # --- Victim recreated with users and limits ---
        assert slurm_backend.client.get_resource(victim["backend_id"]) is not None, (
            f"Missing account {victim['backend_id']} was not recreated"
        )
        restored = _snapshot_backend_resource(slurm_backend, victim["backend_id"])
        assert restored["users"] == victim["snapshot"]["users"]
        assert restored["limits"] == victim["snapshot"]["limits"]

        # --- Survivor untouched: marker fairshare still in place ---
        survivor_account = slurm_backend.client.get_resource(survivor["backend_id"])
        assert survivor_account is not None
        survivor_fairshare = _get_account_fairshare(survivor["backend_id"])
        assert survivor_fairshare == marker_fairshare, (
            f"Surviving account {survivor['backend_id']} was recreated "
            f"(fairshare reset to {survivor_fairshare!r})"
        )
        survivor_state = _snapshot_backend_resource(slurm_backend, survivor["backend_id"])
        assert survivor_state["users"] == survivor["snapshot"]["users"]
        assert survivor_state["limits"] == survivor["snapshot"]["limits"]

        # --- Both Waldur resources healthy ---
        for info in provisioned_resources:
            resource = _get_waldur_resource(waldur_client, info["resource_uuid"])
            assert resource.state == ResourceState.OK


class TestPartialAssociationLoss:
    """The account survives but a user association is lost (partial dataset loss)."""

    @pytest.fixture(autouse=True)
    def _baseline(self, slurm_backend, provisioned_resources):
        _require_baseline_accounts(slurm_backend, provisioned_resources)

    def test_forced_sync_restores_lost_user_association(
        self, offering, waldur_client, slurm_backend, provisioned_resources, report
    ):
        report.heading(2, "Test: lost user association is restored by forced sync")

        target = provisioned_resources[0]
        baseline_users = target["snapshot"]["users"]
        if not baseline_users:
            pytest.skip("No user associations provisioned for this offering")

        lost_user = baseline_users[0]
        slurm_backend.client.delete_association(lost_user, target["backend_id"])
        remaining = sorted(slurm_backend.client.list_resource_users(target["backend_id"]))
        assert lost_user not in remaining, "Association should be gone before the sync"
        report.text(f"Removed association `{lost_user}` from `{target['backend_id']}`\n")

        # --- Forced reconciliation ---
        _run_forced_sync(offering, waldur_client, slurm_backend)

        restored_users = sorted(slurm_backend.client.list_resource_users(target["backend_id"]))
        assert restored_users == baseline_users, (
            f"User associations not restored: {restored_users} != {baseline_users}"
        )
        report.text(f"Associations restored: {restored_users}\n")


class TestStaleUserRemoval:
    """A backend user no longer in the Waldur team must be removed by membership sync.

    Reproduces the leak where a user removed from their last project keeps their
    backend association. Such a user is absent from the (state-filtered,
    offering-wide) offering users list, so the sync must derive stale users from
    the backend user list minus the current team — not by intersecting with the
    offering users — or the association leaks and is never cleaned up.
    """

    @pytest.fixture(autouse=True)
    def _baseline(self, slurm_backend, provisioned_resources):
        _require_baseline_accounts(slurm_backend, provisioned_resources)

    def test_membership_sync_removes_departed_user(
        self, offering, waldur_client, slurm_backend, provisioned_resources, report
    ):
        report.heading(2, "Test: departed team member is removed by membership sync")

        target = provisioned_resources[0]
        backend_id = target["backend_id"]
        baseline_users = sorted(slurm_backend.client.list_resource_users(backend_id))

        # Inject a user the backend reports but the Waldur team does not contain and
        # which is not an offering user — a member who has left all of their projects.
        departed_user = f"departed-{uuid.uuid4().hex[:8]}"
        slurm_backend.client.create_association(departed_user, backend_id)
        with_departed = sorted(slurm_backend.client.list_resource_users(backend_id))
        assert departed_user in with_departed, (
            "Precondition: injected association should be present before the sync"
        )
        report.text(f"Injected stale association `{departed_user}` on `{backend_id}`\n")

        # Regular membership sync (no forced recreation): stale users must be removed.
        processor = OfferingMembershipProcessor(
            offering,
            waldur_client,
            resource_backend=slurm_backend,
        )
        processor.process_offering()

        remaining_users = sorted(slurm_backend.client.list_resource_users(backend_id))
        assert departed_user not in remaining_users, (
            f"Departed user `{departed_user}` was not removed: {remaining_users}"
        )
        # Legitimate team members must be left intact.
        assert remaining_users == baseline_users, (
            f"Membership sync changed legitimate users: {remaining_users} != {baseline_users}"
        )
        report.text(f"Stale association removed; users now {remaining_users}\n")
