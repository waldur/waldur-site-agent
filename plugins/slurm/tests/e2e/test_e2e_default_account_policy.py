r"""End-to-end test: `default_account_policy` controls the user's DefaultAccount.

Scenario:
  1. Provision a Slurm account once via a CREATE order (processor → DONE).
  2. For each policy value, patch the cached
     `slurm_backend._default_account_policy` and call
     `slurm_backend.add_user(resource, <distinct-user>)` directly — the path the
     membership processor runs in production.
  3. Shell out to the emulator's `sacctmgr show user format=user,defaultaccount`
     and assert the user's `Def Acct` matches the policy:
       - `common`     → the configured `default_account`
       - `individual` → the resource's account (backend_id)
       - `none`       → empty (token omitted)

A distinct username is used per policy because `add_user` skips association
creation when one already exists (`backend.py` `get_association` guard), which
would otherwise no-op the second and third cases.

Note: under `none` the emulator leaves `DefaultAccount` empty; real sacctmgr
auto-assigns a default for new users. The empty-string assertion here is
therefore emulator-specific.

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
        plugins/slurm/tests/e2e/test_e2e_default_account_policy.py -v -s
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


TEST_USER = "e2e-defacct-user"


def _sacctmgr_path(offering: Offering) -> str:
    bin_dir = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    return str(Path(bin_dir) / "sacctmgr")


def _show_user_default_account(sacctmgr: str, user: str) -> str:
    """Return the `Def Acct` cell for `user` from the emulator, or "" if unset/absent.

    `sacctmgr show user format=user,defaultaccount` lists all users (no `where`
    filter in the emulator), so we scan for the matching row. In `--parsable2`
    mode each row is ``user|defacct``.
    """
    output = subprocess.check_output(
        [
            sacctmgr,
            "--parsable2",
            "--noheader",
            "--immediate",
            "show",
            "user",
            "format=user,defaultaccount",
        ],
        stderr=subprocess.STDOUT,
        timeout=10,
    ).decode()
    for line in output.splitlines():
        if "|" not in line:
            continue
        cells = line.split("|")
        if cells and cells[0] == user:
            return cells[1] if len(cells) > 1 else ""
    return ""


class TestDefaultAccountPolicy:
    """Verify each default_account_policy lands the right DefaultAccount in sacctmgr."""

    def test_policy_sets_correct_default_account(
        self,
        offering: Offering,
        waldur_client,
        slurm_backend,
        project_uuid,
        report,
    ):
        """`common`/`individual`/`none` each produce the expected `Def Acct`."""
        report.heading(2, "Test: default_account_policy sets the user's DefaultAccount")

        # --- Setup: provision one account via a CREATE order ---
        offering_url, plan_url = get_offering_info(waldur_client, offering.uuid)
        project_url = get_project_url(waldur_client, project_uuid)

        order_uuid = create_source_order(
            waldur_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100},
            name="defacct-policy-e2e-test",
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

        run_processor_until_order_terminal(offering, waldur_client, slurm_backend, order_uuid)

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        assert order.state == OrderState.DONE, f"Order did not reach DONE: {order.state}"

        resource = marketplace_provider_resources_retrieve.sync(
            uuid=order.marketplace_resource_uuid, client=waldur_client
        )
        account = resource.backend_id
        assert account, "Resource has no backend_id (Slurm account name)"
        report.text(f"Slurm account: `{account}`\n")

        configured_default = offering.backend_settings.get("default_account", "root")
        expected = {
            "common": configured_default,
            "individual": account,
            "none": "",
        }

        sacctmgr = _sacctmgr_path(offering)

        # --- Action + assertion: one distinct user per policy ---
        # The fixture backend is module-scoped and caches the policy at
        # __init__; patch the cached attribute per case and restore it.
        original_policy = slurm_backend._default_account_policy
        try:
            for policy, expected_default in expected.items():
                slurm_backend._default_account_policy = policy
                user = f"{TEST_USER}-{policy}"

                slurm_backend.add_user(resource, user)

                actual = _show_user_default_account(sacctmgr, user)
                report.text(
                    f"policy=`{policy}` → user `{user}` DefaultAccount=`{actual}` "
                    f"(expected `{expected_default}`)\n"
                )
                assert actual == expected_default, (
                    f"policy={policy!r}: expected DefaultAccount={expected_default!r} "
                    f"for user {user!r}, got {actual!r}."
                )
        finally:
            slurm_backend._default_account_policy = original_policy
