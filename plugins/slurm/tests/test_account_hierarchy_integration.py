"""Integration tests for the SLURM account hierarchy against the slurm-emulator.

Unlike the ``tests/e2e/`` suite, these tests do **not** need a running Waldur
instance.  They drive the real :class:`SlurmClient` against the emulator's
``sacctmgr`` / ``sacct`` binaries (installed via the ``slurm-emulator`` dev
dependency), so the full account-creation and association commands actually run.

They pin the decoupling of ``root_account`` (which controls the parent of the
top-tier customer account, i.e. the root of the account tree) from
``default_account`` (which controls only the user's ``DefaultAccount=`` on
associations).  A regression here would re-parent the whole account tree under a
restricted user account.
"""

import os
import shutil
import uuid
from datetime import datetime, timezone

import pytest
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE

_SACCTMGR = shutil.which("sacctmgr")

pytestmark = pytest.mark.skipif(
    _SACCTMGR is None,
    reason="slurm-emulator binaries (sacctmgr) not on PATH; install the slurm plugin dev group",
)

_SLURM_TRES = {
    "cpu": {
        "limit": 10,
        "measured_unit": "k-Hours",
        "unit_factor": 60000,
        "accounting_type": "limit",
        "label": "CPU",
    },
}


@pytest.fixture
def emulator_env(tmp_path, monkeypatch):
    """Point the emulator at a per-test state file so each test starts clean.

    A fresh state file makes the emulator initialise a default tree containing
    the ``root`` account, which is the real cluster root used as ``root_account``.
    """
    monkeypatch.setenv("SLURM_EMULATOR_STATE_FILE", str(tmp_path / "slurm_db.json"))
    monkeypatch.setenv("SLURM_EMULATOR_TIME_FILE", str(tmp_path / "slurm_time.json"))


def _make_backend(extra_settings: dict) -> SlurmBackend:
    settings = {
        "customer_prefix": "hpc_",
        "project_prefix": "hpc_",
        "allocation_prefix": "hpc_",
        "slurm_bin_path": os.path.dirname(_SACCTMGR),
        # Homedir creation shells out to OS-level commands that don't exist in CI.
        "enable_user_homedir_account_creation": False,
        **extra_settings,
    }
    return SlurmBackend(settings, _SLURM_TRES)


def _make_waldur_resource(
    customer_slug: str = "orgb",
    customer_name: str = "Org B",
    project_slug: str = "projecta",
    project_name: str = "Project A",
    backend_id: str = "hpc_test-alloc",
) -> WaldurResource:
    return WaldurResource(
        uuid=uuid.uuid4(),
        name="test-alloc",
        backend_id=backend_id,
        resource_uuid=uuid.uuid4(),
        offering_type=MARKETPLACE_SLURM_OFFERING_TYPE,
        downscaled=False,
        state=ResourceState.OK,
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        last_sync=datetime(2024, 1, 1, tzinfo=timezone.utc),
        restrict_member_access=False,
        limits=ResourceLimits.from_dict({"cpu": 50}),
        project_uuid=uuid.uuid4(),
        project_name=project_name,
        project_slug=project_slug,
        customer_uuid=uuid.uuid4(),
        customer_name=customer_name,
        customer_slug=customer_slug,
    )


def _default_account_of(backend: SlurmBackend, username: str) -> str:
    """Read the user's DefaultAccount straight from the emulator."""
    output = backend.client._execute_command(
        ["show", "user", username, "format=User,DefaultAccount"]
    )
    for line in output.splitlines():
        if "|" in line and line.split("|")[0] == username:
            return line.split("|")[1].strip().strip('"')
    raise AssertionError(f"user {username} not found in emulator output: {output!r}")


@pytest.mark.usefixtures("emulator_env")
class TestAccountHierarchyIntegration:
    """End-to-end account tree creation through real sacctmgr (emulated)."""

    def test_customer_account_rooted_at_root_account_not_default_account(self):
        # Restricted default account for users, but the tree must hang off root.
        backend = _make_backend(
            {"default_account": "restricted_access", "root_account": "root"}
        )

        backend._pre_create_resource(_make_waldur_resource())

        assert backend.client.get_account_parent("hpc_orgb") == "root"
        assert backend.client.get_account_parent("hpc_projecta") == "hpc_orgb"

    def test_root_account_defaults_to_default_account_when_unset(self):
        # Without root_account, the legacy behaviour holds: parent == default_account.
        backend = _make_backend({"default_account": "root"})

        backend._pre_create_resource(_make_waldur_resource())

        assert backend.client.get_account_parent("hpc_orgb") == "root"

    def test_user_default_account_is_default_account_not_root_account(self):
        backend = _make_backend(
            {"default_account": "restricted_access", "root_account": "root"}
        )
        # The DefaultAccount target and the allocation account must exist first.
        backend.client.create_resource(
            name="restricted_access",
            description="restricted",
            organization="restricted_access",
            parent_name="root",
        )
        resource = _make_waldur_resource()
        backend.client.create_resource(
            name=resource.backend_id,
            description="alloc",
            organization=resource.backend_id,
            parent_name="root",
        )

        assert backend.add_user(resource, "alice") is True

        assert _default_account_of(backend, "alice") == "restricted_access"
