r"""End-to-end test: offering partitions are applied to Slurm user associations.

Scenario:
  1. Add an `OfferingPartition` record to the test offering via the marketplace
     API (POST /api/marketplace-provider-offerings/<uuid>/add_partition/).
  2. Toggle `enforce_offering_partitions: true` on the in-memory `Offering`
     object so the agent's add_user path takes the partition-aware branch.
  3. Construct an `OfferingOrderProcessor` — its `__init__` re-fetches the
     offering (now carrying the partition) and populates
     `slurm_backend.offering_partitions` on the resource backend.
  4. Create a CREATE order, approve it, and run the processor until DONE so
     the Slurm account exists on the emulator.
  5. Call `slurm_backend.add_user(resource, "<test_user>")` directly to
     exercise the association path that the membership processor would run
     in production. With enforcement on, the agent emits
     `sacctmgr add user … Partitions=<name> Share=parent`.
  6. Shell out to the emulator's `sacctmgr show association format=
     account,user,partition` and assert the user row carries the partition.
  7. Teardown removes the partition via API and restores the in-memory
     `enforce_offering_partitions` flag.

Uses the SLURM emulator backend; orders complete synchronously in one cycle.

Environment variables (same as the rest of the e2e suite):
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path-to-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \\
    WALDUR_E2E_CONFIG=e2e-local-config.yaml \\
    WALDUR_E2E_PROJECT_A_UUID=<uuid> \\
    .venv/bin/python -m pytest \\
        plugins/slurm/tests/e2e/test_e2e_partition_associations.py -v -s
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import pytest
from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_approve_by_provider,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_approve_by_provider_request import (
    OrderApproveByProviderRequest,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from plugins.slurm.tests.e2e.conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)
from waldur_site_agent.common.structures import Offering

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="E2E tests disabled")


PARTITION_NAME = "e2e-zen3"
TEST_USER = "e2e-partition-user"


def _partition_uuid_by_name(httpx_client, offering_uuid: str, name: str) -> str | None:
    """Return the UUID of an OfferingPartition with the given name, or None.

    Goes through the raw HTTP client because the generated SDK's
    `OfferingPartition.from_dict` requires fields that the marketplace
    offering-detail endpoint does not always return, which breaks the
    typed wrappers.
    """
    resp = httpx_client.get(
        f"/api/marketplace-provider-offerings/{offering_uuid}/",
        params={"field": ["partitions"]},
    )
    resp.raise_for_status()
    for entry in resp.json().get("partitions") or []:
        if entry.get("partition_name") == name:
            return entry.get("uuid")
    return None


_OK_STATUSES = (200, 204)
_GONE_STATUS = 404


def _remove_partition(httpx_client, offering_uuid: str, partition_uuid: str) -> None:
    """POST /remove_partition — tolerates 404 for already-removed records."""
    resp = httpx_client.post(
        f"/api/marketplace-provider-offerings/{offering_uuid}/remove_partition/",
        json={"partition_uuid": partition_uuid},
    )
    if resp.status_code in _OK_STATUSES:
        return
    if resp.status_code == _GONE_STATUS:
        logger.info("Partition %s already absent on teardown", partition_uuid)
        return
    msg = (
        f"remove_partition failed: {resp.status_code} {resp.text}"
        f" (offering {offering_uuid}, partition {partition_uuid})"
    )
    raise RuntimeError(msg)


@pytest.fixture
def partition_on_offering(waldur_client, offering: Offering):
    """Add an OfferingPartition to the offering for the duration of the test.

    Yields the created partition's UUID so the test can correlate. Cleans
    up via remove_partition on teardown — even if the assertion fails,
    so subsequent runs start from a clean offering.

    Uses the raw HTTP client (not the generated SDK wrappers) because the
    SDK's `OfferingPartition` model expects fields (`created`, `modified`,
    `offering_name`, …) that aren't present on the 201 response from
    `/add_partition/`. A successful add still completes server-side but
    the wrapper raises `KeyError` parsing the response.
    """
    httpx_client = waldur_client.get_httpx_client()
    offering_uuid = offering.uuid

    # Pre-cleanup: remove a stale partition with the same name (could be
    # left from a previous run that crashed mid-teardown). Bind name
    # uniqueness on (offering, partition_name) makes this necessary.
    existing = _partition_uuid_by_name(httpx_client, offering_uuid, PARTITION_NAME)
    if existing:
        logger.info("Found stale partition %s on offering, removing", existing)
        _remove_partition(httpx_client, offering_uuid, existing)

    resp = httpx_client.post(
        f"/api/marketplace-provider-offerings/{offering_uuid}/add_partition/",
        json={"offering": offering_uuid, "partition_name": PARTITION_NAME},
    )
    resp.raise_for_status()
    partition_uuid = resp.json()["uuid"]
    logger.info(
        "Added partition %s (uuid=%s) to offering %s",
        PARTITION_NAME, partition_uuid, offering_uuid,
    )

    try:
        yield partition_uuid
    finally:
        try:
            _remove_partition(httpx_client, offering_uuid, partition_uuid)
        except Exception:
            logger.exception("Failed to remove partition during teardown")


@pytest.fixture
def enforcement_enabled(offering: Offering, slurm_backend):
    """Flip `enforce_offering_partitions: true` for the duration of the test.

    The flag lives in the agent's local YAML config so it can't be toggled
    via the Waldur API. Two surfaces have to be in sync:

    - `offering.backend_settings` — would be read by any newly-constructed
      `SlurmBackend`.
    - `slurm_backend._enforce_offering_partitions` — cached at __init__
      time on the module-scoped fixture instance the test actually uses;
      mutating the dict above doesn't update this cached bool.

    Restores both on teardown.
    """
    prev_setting = offering.backend_settings.get("enforce_offering_partitions")
    prev_attr = slurm_backend._enforce_offering_partitions
    offering.backend_settings["enforce_offering_partitions"] = True
    slurm_backend._enforce_offering_partitions = True
    try:
        yield
    finally:
        if prev_setting is None:
            offering.backend_settings.pop("enforce_offering_partitions", None)
        else:
            offering.backend_settings["enforce_offering_partitions"] = prev_setting
        slurm_backend._enforce_offering_partitions = prev_attr


def _sacctmgr_path(offering: Offering) -> str:
    bin_dir = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    return str(Path(bin_dir) / "sacctmgr")


def _show_association_partition(
    sacctmgr: str, account: str, user: str
) -> list[str]:
    """Return the list of partition names on (account, user) associations.

    With ``Partitions=p1,p2`` the emulator (>=0.4.0) creates one row per
    partition, so this returns ``["p1", "p2"]``. With no partition
    restriction it returns ``[""]`` (one row, empty partition field).
    """
    output = subprocess.check_output(
        [
            sacctmgr, "--parsable2", "--noheader", "--immediate",
            "show", "association",
            "where", f"user={user}", f"account={account}",
            "format=account,user,partition",
        ],
        stderr=subprocess.STDOUT,
        timeout=10,
    ).decode()
    # account|user|partition| → cells[0..2] are the format columns we asked for.
    expected_columns = 3
    partitions: list[str] = []
    for line in output.splitlines():
        if "|" not in line:
            continue
        cells = line.split("|")
        if len(cells) >= expected_columns and cells[0] == account and cells[1] == user:
            partitions.append(cells[2])
    return partitions


class TestPartitionAssociations:
    """Verify offering partitions reach sacctmgr when enforcement is enabled."""

    def test_partition_applied_to_user_association(
        self,
        offering: Offering,
        waldur_client,
        slurm_backend,
        project_uuid,
        partition_on_offering,  # noqa: ARG002  -- fixture, setup/teardown via yield
        enforcement_enabled,  # noqa: ARG002  -- fixture, setup/teardown via yield
        report,
    ):
        """The offering's partition name shows up on the user's sacctmgr row.

        Steps:
          1. Partition `e2e-zen3` is on the offering (fixture).
          2. enforce_offering_partitions: true (fixture).
          3. Create + approve a CREATE order; processor runs to DONE. The
             processor's __init__ fetches the offering (now carrying the
             partition) and populates `slurm_backend.offering_partitions`.
          4. Call slurm_backend.add_user(resource, TEST_USER) directly.
          5. Query sacctmgr → assert the user has Partition=e2e-zen3.
        """
        report.heading(2, "Test: offering partition applied to Slurm user association")

        # --- Setup: create + approve order ---
        offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
        project_url = get_project_url(waldur_client, project_uuid)

        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100},
            name="partition-e2e-test",
        )
        report.text(f"Created order `{order_uuid}`\n")

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        if state == OrderState.PENDING_PROVIDER:
            marketplace_orders_approve_by_provider.sync_detailed(
                client=waldur_client,
                uuid=order_uuid,
                body=OrderApproveByProviderRequest(),
            )

        # --- Run processor: constructs OfferingOrderProcessor with the
        # fixture backend, fetches the offering (now with our partition),
        # populates slurm_backend.offering_partitions, and drives the
        # order to DONE. ---
        run_processor_until_order_terminal(
            offering, waldur_client, slurm_backend, order_uuid
        )

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        assert order.state == OrderState.DONE, f"Order did not reach DONE: {order.state}"

        # Confirm the processor wired the partition list through to the
        # backend — if this is empty, the assertion below would fail
        # with a misleading error.
        assert PARTITION_NAME in slurm_backend.offering_partitions, (
            f"Expected {PARTITION_NAME} in slurm_backend.offering_partitions, "
            f"got {slurm_backend.offering_partitions}"
        )

        resource = marketplace_provider_resources_retrieve.sync(
            uuid=order.marketplace_resource_uuid, client=waldur_client
        )
        account = resource.backend_id
        assert account, "Resource has no backend_id (Slurm account name)"
        report.text(f"Slurm account: `{account}`\n")

        # --- Action: add a user against the freshly-provisioned account ---
        slurm_backend.add_user(resource, TEST_USER)

        # --- Assertion: emulator stored the partition on the user row ---
        sacctmgr = _sacctmgr_path(offering)
        partitions = _show_association_partition(sacctmgr, account, TEST_USER)
        report.text(
            f"`sacctmgr show association where user={TEST_USER} account={account} "
            f"format=account,user,partition` → partitions: `{partitions}`\n"
        )
        assert partitions == [PARTITION_NAME], (
            f"Expected user row with Partition={PARTITION_NAME!r}, got {partitions!r}. "
            "If the cell is empty, the agent didn't pass Partitions= to sacctmgr "
            "(check enforce_offering_partitions and the offering_partitions wiring)."
        )

    def test_opt_out_default_does_not_apply_partition(
        self,
        offering: Offering,
        waldur_client,
        slurm_backend,
        project_uuid,
        partition_on_offering,  # noqa: ARG002  -- fixture, setup/teardown via yield
        report,
    ):
        """With enforcement disabled (default), the partition does NOT reach sacctmgr.

        Regression test for the opt-in promise: existing deployments that
        populate OfferingPartition records for informational use only must
        not see new sacctmgr Partitions= flags appear after upgrade.
        """
        report.heading(
            2,
            "Test: opt-out default keeps OfferingPartition informational",
        )

        # Important: do NOT request enforcement_enabled here — defaults apply.
        previous = offering.backend_settings.pop("enforce_offering_partitions", None)
        try:
            offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
            project_url = get_project_url(waldur_client, project_uuid)

            order_uuid = create_source_order(
                waldur_client, offering_url, project_url, plan_url,
                limits={"cpu": 100}, name="partition-e2e-optout",
            )
            order = marketplace_orders_retrieve.sync(
                client=waldur_client, uuid=order_uuid
            )
            if order.state == OrderState.PENDING_PROVIDER:
                marketplace_orders_approve_by_provider.sync_detailed(
                    client=waldur_client,
                    uuid=order_uuid,
                    body=OrderApproveByProviderRequest(),
                )

            run_processor_until_order_terminal(
                offering, waldur_client, slurm_backend, order_uuid
            )
            order = marketplace_orders_retrieve.sync(
                client=waldur_client, uuid=order_uuid
            )
            resource = marketplace_provider_resources_retrieve.sync(
                uuid=order.marketplace_resource_uuid, client=waldur_client
            )
            account = resource.backend_id

            # The processor populated offering_partitions because the
            # partition exists on the offering. Enforcement is off, so
            # add_user must IGNORE the populated list and emit no
            # Partitions= flag.
            slurm_backend.add_user(resource, TEST_USER + "-optout")

            sacctmgr = _sacctmgr_path(offering)
            partitions = _show_association_partition(
                sacctmgr, account, TEST_USER + "-optout"
            )
            report.text(f"Partitions on opt-out user row: `{partitions}`\n")
            # Either zero rows (no association if something failed) or
            # one row with an empty partition cell — both mean Slurm got
            # no Partitions= flag.
            assert partitions in ([], [""]), (
                f"Expected no partition on user row (opt-out default), got {partitions!r}"
            )
        finally:
            if previous is not None:
                offering.backend_settings["enforce_offering_partitions"] = previous
