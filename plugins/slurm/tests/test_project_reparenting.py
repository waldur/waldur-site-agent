"""Tests for SLURM account reparenting when a Waldur project moves to a new customer."""

import uuid
from datetime import datetime, timezone
from typing import Optional
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from waldur_api_client.models import ResourceState
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_limits import ResourceLimits
from waldur_api_client.types import UNSET
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient

from waldur_site_agent.common import MARKETPLACE_SLURM_OFFERING_TYPE

_SLURM_SETTINGS = {
    "default_account": "root",
    "customer_prefix": "hpc_",
    "project_prefix": "hpc_",
    "allocation_prefix": "hpc_",
}

_SLURM_TRES = {
    "cpu": {
        "limit": 10,
        "measured_unit": "k-Hours",
        "unit_factor": 60000,
        "accounting_type": "limit",
        "label": "CPU",
    },
}


def _make_waldur_resource(
    project_slug: str = "project-alpha",
    customer_slug: str = "org-b",
    customer_name: str = "Org B",
    project_name: str = "Project Alpha",
) -> WaldurResource:
    return WaldurResource(
        uuid=uuid.uuid4(),
        name="test-alloc",
        backend_id="hpc_test-alloc",
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


def _make_backend(extra_settings: Optional[dict] = None) -> SlurmBackend:
    settings = dict(_SLURM_SETTINGS)
    if extra_settings:
        settings.update(extra_settings)
    backend = SlurmBackend(settings, _SLURM_TRES)
    backend.client = MagicMock(spec=SlurmClient)
    return backend


class TestRootAccountDecoupling:
    """The account-tree root (root_account) is independent of the user DefaultAccount.

    ``default_account`` is the ``DefaultAccount=`` placed on user associations, while
    ``root_account`` is the parent of the top-tier customer account. They used to be a
    single setting; these tests pin the decoupled behaviour.
    """

    def test_root_account_defaults_to_default_account(self):
        # No root_account configured: the customer account is parented under default_account.
        backend = _make_backend(extra_settings={"default_account": "restricted_access"})
        assert backend._root_account == "restricted_access"

        waldur_resource = _make_waldur_resource(customer_slug="org-b", customer_name="Org B")
        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend._pre_create_resource(waldur_resource)

        mock_create.assert_any_call("hpc_org-b", "Org B", "hpc_org-b", "restricted_access")

    def test_root_account_overrides_parent_without_touching_user_default(self):
        backend = _make_backend(
            extra_settings={"default_account": "restricted_access", "root_account": "root"}
        )
        assert backend._root_account == "root"

        waldur_resource = _make_waldur_resource(customer_slug="org-b", customer_name="Org B")
        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend._pre_create_resource(waldur_resource)

        # Customer account is rooted at "root", not under the restricted user account.
        mock_create.assert_any_call("hpc_org-b", "Org B", "hpc_org-b", "root")
        mock_create.assert_any_call(
            "hpc_project-alpha", "Project Alpha", "hpc_project-alpha", "hpc_org-b"
        )

    def test_user_association_uses_default_account_not_root_account(self):
        backend = _make_backend(
            extra_settings={"default_account": "restricted_access", "root_account": "root"}
        )
        backend.client.get_association.return_value = None

        waldur_resource = _make_waldur_resource()
        backend.add_user(waldur_resource, "alice")

        # DefaultAccount stays the restricted account regardless of root_account.
        backend.client.create_association.assert_called_once_with(
            "alice", waldur_resource.backend_id, "restricted_access"
        )


class TestGetAccountParent:
    """Unit tests for SlurmClient.get_account_parent()."""

    @pytest.fixture
    def client(self):
        c = SlurmClient({}, slurm_bin_path="")
        with patch.object(c, "execute_command", return_value=""):
            yield c

    def test_returns_parent_from_account_level_association(self, client):
        # Account-level row (empty User) carries the parent; per-user rows do not.
        client.execute_command.return_value = (
            "hpc_project-alpha|hpc_org-a|\nhpc_project-alpha|hpc_org-a|alice\n"
        )
        result = client.get_account_parent("hpc_project-alpha")
        assert result == "hpc_org-a"

    def test_returns_none_when_account_not_in_output(self, client):
        client.execute_command.return_value = "other-account|hpc_org-a|\n"
        result = client.get_account_parent("hpc_project-alpha")
        assert result is None

    def test_returns_none_on_empty_output(self, client):
        client.execute_command.return_value = ""
        result = client.get_account_parent("hpc_project-alpha")
        assert result is None

    def test_ignores_user_rows_when_account_row_has_parent(self, client):
        # A bare per-user row must not be mistaken for the account's parent.
        client.execute_command.return_value = "hpc_project-alpha|hpc_org-a|bob\n"
        result = client.get_account_parent("hpc_project-alpha")
        assert result is None

    def test_command_queries_association_with_parent_format(self, client):
        client.execute_command.return_value = ""
        client.get_account_parent("hpc_project-alpha")
        cmd = client.executed_commands[-1]
        assert "assoc" in cmd
        assert "account=hpc_project-alpha" in cmd
        assert "format=Account,ParentName,User" in cmd


class TestSetAccountParent:
    """Unit tests for SlurmClient.set_account_parent()."""

    @pytest.fixture
    def client(self):
        c = SlurmClient({}, slurm_bin_path="")
        with patch.object(c, "execute_command", return_value=""):
            yield c

    def test_issues_modify_command_with_correct_names(self, client):
        client.set_account_parent("hpc_project-alpha", "hpc_org-b")
        cmd = client.executed_commands[-1]
        assert "modify" in cmd
        assert "hpc_project-alpha" in cmd
        assert "hpc_org-b" in cmd


class TestSyncResourceProject:
    """Tests for SlurmBackend.sync_resource_project()."""

    def test_reparents_when_parent_is_stale(self):
        backend = _make_backend()
        # First call: pre-check returns stale parent; second call: post-verify returns correct.
        backend.client.get_account_parent.side_effect = ["hpc_org-a", "hpc_org-b"]

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
            customer_name="Org B",
        )

        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend.sync_resource_project(waldur_resource)

        assert mock_create.call_count == 2
        mock_create.assert_any_call("hpc_org-b", "Org B", "hpc_org-b", "root")
        mock_create.assert_any_call("hpc_project-alpha", "Project Alpha", "hpc_project-alpha", "hpc_org-b")
        backend.client.set_account_parent.assert_called_once_with("hpc_project-alpha", "hpc_org-b")

    def test_no_reparent_when_parent_already_correct(self):
        backend = _make_backend()
        backend.client.get_account_parent.return_value = "hpc_org-b"

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
        )

        backend.sync_resource_project(waldur_resource)

        backend.client.set_account_parent.assert_not_called()

    def test_creates_accounts_and_sets_parent_when_project_account_missing(self):
        backend = _make_backend()
        # First call: account missing; second call: post-verify confirms correct parent.
        backend.client.get_account_parent.side_effect = [None, "hpc_org-b"]

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
            customer_name="Org B",
        )

        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend.sync_resource_project(waldur_resource)

        assert mock_create.call_count == 2
        mock_create.assert_any_call("hpc_org-b", "Org B", "hpc_org-b", "root")
        mock_create.assert_any_call("hpc_project-alpha", "Project Alpha", "hpc_project-alpha", "hpc_org-b")
        backend.client.set_account_parent.assert_called_once_with("hpc_project-alpha", "hpc_org-b")

    def test_skipped_for_flat_hierarchy(self):
        backend = _make_backend(extra_settings={"parent_account": "flat-parent"})

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
        )

        backend.sync_resource_project(waldur_resource)

        backend.client.get_account_parent.assert_not_called()
        backend.client.set_account_parent.assert_not_called()

    def test_skipped_when_customer_slug_missing(self):
        backend = _make_backend()
        waldur_resource = _make_waldur_resource(customer_slug="", project_slug="project-alpha")

        backend.sync_resource_project(waldur_resource)

        backend.client.get_account_parent.assert_not_called()
        backend.client.set_account_parent.assert_not_called()

    def test_skipped_when_project_slug_missing(self):
        backend = _make_backend()
        waldur_resource = _make_waldur_resource(project_slug="", customer_slug="org-b")

        backend.sync_resource_project(waldur_resource)

        backend.client.get_account_parent.assert_not_called()
        backend.client.set_account_parent.assert_not_called()

    def test_skipped_when_project_and_customer_backend_ids_are_identical(self):
        # customer_prefix == project_prefix AND same slug → identical IDs → self-parenting guard.
        backend = _make_backend()
        waldur_resource = _make_waldur_resource(
            project_slug="acme",
            customer_slug="acme",
        )

        backend.sync_resource_project(waldur_resource)

        backend.client.get_account_parent.assert_not_called()
        backend.client.set_account_parent.assert_not_called()

    def test_backend_error_is_caught_not_propagated(self):
        backend = _make_backend()
        backend.client.get_account_parent.return_value = "hpc_org-a"

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
        )

        with patch.object(
            backend, "_create_backend_resource", side_effect=BackendError("sacctmgr failed")
        ):
            # Must not raise — BackendError is caught internally.
            backend.sync_resource_project(waldur_resource)

        backend.client.set_account_parent.assert_not_called()

    def test_warns_when_parent_mismatch_after_reparent(self):
        backend = _make_backend()
        # Pre-check: stale parent. Post-verify: still wrong (set_account_parent silently no-oped).
        backend.client.get_account_parent.side_effect = ["hpc_org-a", "hpc_org-a"]

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
        )

        with patch.object(backend, "_create_backend_resource"):
            with patch.object(backend, "_get_logger_name", create=True):
                backend.sync_resource_project(waldur_resource)

        # set_account_parent was still attempted even though it didn't take effect.
        backend.client.set_account_parent.assert_called_once_with("hpc_project-alpha", "hpc_org-b")

    def test_uses_backend_id_as_fallback_when_names_are_unset(self):
        backend = _make_backend()
        backend.client.get_account_parent.side_effect = [None, "hpc_org-b"]

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
            customer_name=UNSET,
            project_name=UNSET,
        )

        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend.sync_resource_project(waldur_resource)

        # When names are Unset, backend IDs are used as the description fallback.
        mock_create.assert_any_call("hpc_org-b", "hpc_org-b", "hpc_org-b", "root")
        mock_create.assert_any_call("hpc_project-alpha", "hpc_project-alpha", "hpc_project-alpha", "hpc_org-b")
