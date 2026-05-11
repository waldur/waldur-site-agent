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
        project_name="Project Alpha",
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


class TestGetAccountParent:
    """Unit tests for SlurmClient.get_account_parent()."""

    @pytest.fixture
    def client(self):
        c = SlurmClient({}, slurm_bin_path="")
        with patch.object(c, "execute_command", return_value=""):
            yield c

    def test_returns_parent_when_account_found(self, client):
        client.execute_command.return_value = "hpc_project-alpha|hpc_org-a\n"
        result = client.get_account_parent("hpc_project-alpha")
        assert result == "hpc_org-a"

    def test_returns_none_when_account_not_in_output(self, client):
        client.execute_command.return_value = "other-account|hpc_org-a\n"
        result = client.get_account_parent("hpc_project-alpha")
        assert result is None

    def test_returns_none_on_empty_output(self, client):
        client.execute_command.return_value = ""
        result = client.get_account_parent("hpc_project-alpha")
        assert result is None

    def test_command_references_account_and_format(self, client):
        client.execute_command.return_value = ""
        client.get_account_parent("hpc_project-alpha")
        cmd = client.executed_commands[-1]
        assert "hpc_project-alpha" in cmd
        assert "format=Account,ParentName" in cmd


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
        backend.client.get_account_parent.return_value = "hpc_org-a"

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
            customer_name="Org B",
        )

        with patch.object(backend, "_create_backend_resource") as mock_create:
            backend.sync_resource_project(waldur_resource)

        backend.client.get_account_parent.assert_called_once_with("hpc_project-alpha")
        mock_create.assert_called_once_with("hpc_org-b", "Org B", "hpc_org-b", "root")
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

    def test_no_reparent_when_account_not_found_in_slurm(self):
        backend = _make_backend()
        backend.client.get_account_parent.return_value = None

        waldur_resource = _make_waldur_resource(
            project_slug="project-alpha",
            customer_slug="org-b",
        )

        backend.sync_resource_project(waldur_resource)

        backend.client.set_account_parent.assert_not_called()

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
