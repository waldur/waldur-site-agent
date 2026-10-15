"""Regression tests for gh-23.

The LDAP project group (and the project directory and QoS) is addressed by the
resource's persisted backend ID in every later operation. It used to be named
in ``_pre_create_resource`` from ``waldur_resource.slug``, which differs from
the ID under ``account_name_generation_policy: project_slug`` or after a
collision suffix, so ``add_user`` failed with ``noSuchObject``.

The group, directory and QoS are now created in ``_on_backend_resource_created``,
which runs only once SLURM has accepted the ID, so they carry the persisted ID
and the uniqueness retry leaves nothing behind for the IDs it rejects. Groups
created under the old name are renamed on the next membership operation.
"""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.backend.exceptions import BackendError, DuplicateResourceError
from waldur_site_agent.common.processors import OfferingBaseProcessor

_CPU_COMPONENTS = {
    "cpu": {
        "measured_unit": "minutes",
        "unit_factor": 1,
        "accounting_type": "limit",
        "label": "CPU",
    }
}


def _backend(extra: Optional[dict[str, Any]] = None) -> SlurmBackend:
    settings: dict[str, Any] = {
        "default_account": "root",
        "customer_prefix": "c-",
        "project_prefix": "p-",
        "allocation_prefix": "a-",
    }
    if extra:
        settings.update(extra)
    backend = SlurmBackend(settings, _CPU_COMPONENTS)
    backend.client = MagicMock()
    backend.client.get_resource.return_value = None
    backend.client.get_association.return_value = None
    return backend


def _waldur_resource(slug: str = "alloc1", backend_id: str = "") -> MagicMock:
    """A resource whose slug differs from the resolved backend ID."""
    resource = MagicMock()
    resource.uuid = "uuid-1"
    resource.name = "alloc"
    resource.slug = slug
    resource.backend_id = backend_id
    resource.customer_slug = "cust"
    resource.project_slug = "proj"
    resource.customer_name = "Cust"
    resource.project_name = "Proj"
    resource.limits = None
    resource.created = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    resource.end_date = UNSET
    resource.offering_plugin_options.additional_properties = {}
    return resource


def _ldap_client(backend: SlurmBackend, groups: Optional[set[str]] = None) -> MagicMock:
    """An LDAP client mock whose group_exists answers from ``groups``."""
    existing = groups if groups is not None else set()
    ldap = MagicMock()
    ldap.group_exists.side_effect = lambda name: name in existing
    backend._ldap_client = ldap
    return ldap


def _stub_limits_and_post_create(backend: SlurmBackend) -> None:
    backend._setup_resource_limits = MagicMock(return_value={})
    backend.post_create_resource = MagicMock()


class TestPrerequisitesNamedAfterAcceptedId:
    def test_group_directory_and_qos_use_the_created_id(self):
        backend = _backend(
            {"project_directory": {"enabled": True}, "qos_management": {"enabled": True}}
        )
        ldap = _ldap_client(backend)
        backend._setup_project_directory = MagicMock()
        backend._setup_account_qos = MagicMock()
        backend._create_backend_resource = MagicMock(return_value=True)
        _stub_limits_and_post_create(backend)

        info = backend.create_resource_with_id(_waldur_resource(), "a-proj-0", None)

        assert info.backend_id == "a-proj-0"
        ldap.create_project_group.assert_called_once_with("a-proj-0")
        backend._setup_project_directory.assert_called_once_with("a-proj-0")
        backend._setup_account_qos.assert_called_once_with("a-proj-0")

    def test_pre_create_only_creates_parent_accounts(self):
        backend = _backend(
            {"project_directory": {"enabled": True}, "qos_management": {"enabled": True}}
        )
        ldap = _ldap_client(backend)
        backend._setup_project_directory = MagicMock()
        backend._create_backend_resource = MagicMock(return_value=True)

        backend._pre_create_resource(_waldur_resource())

        created = [c.args[0] for c in backend._create_backend_resource.call_args_list]
        assert created == ["c-cust", "p-proj"]
        ldap.create_project_group.assert_not_called()
        backend._setup_project_directory.assert_not_called()
        backend.client.create_qos.assert_not_called()

    def test_rejected_id_leaves_nothing_behind(self):
        """The account already exists: no group, directory or QoS for its name."""
        backend = _backend(
            {"project_directory": {"enabled": True}, "qos_management": {"enabled": True}}
        )
        ldap = _ldap_client(backend)
        backend._setup_project_directory = MagicMock()
        backend._create_backend_resource = MagicMock(side_effect=[True, True, False])

        with pytest.raises(DuplicateResourceError):
            backend.create_resource_with_id(_waldur_resource(), "a-sibling", None)

        ldap.create_project_group.assert_not_called()
        backend._setup_project_directory.assert_not_called()
        backend.client.create_qos.assert_not_called()
        backend.client.set_account_default_qos.assert_not_called()


class TestUniquenessRetry:
    """Through the processor's retry, with the shipped equal-prefix config."""

    def _run(self, backend: SlurmBackend) -> tuple[Any, MagicMock]:
        accounts: set[str] = set()
        backend.client.get_resource.side_effect = lambda name: (
            {"name": name} if name in accounts else None
        )
        backend.client.create_resource.side_effect = lambda name, **_: accounts.add(name)
        _stub_limits_and_post_create(backend)

        resource = _waldur_resource()
        resource.offering_plugin_options.additional_properties = {
            "account_name_generation_policy": "project_slug"
        }
        processor = SimpleNamespace(
            resource_backend=backend,
            offering=SimpleNamespace(backend_settings={}),
            _check_backend_id_uniqueness=lambda _backend_id: True,
        )
        info = OfferingBaseProcessor._create_resource_with_uniqueness_check(
            processor,  # type: ignore[arg-type]
            resource,
            {},
        )
        return info, resource

    def test_only_the_accepted_id_gets_a_group_directory_and_qos(self):
        # project_prefix == allocation_prefix, so the first candidate is the
        # project account _pre_create_resource just created, and is rejected.
        backend = _backend(
            {
                "project_prefix": "hpc_",
                "allocation_prefix": "hpc_",
                "project_directory": {"enabled": True},
                "qos_management": {"enabled": True},
            }
        )
        ldap = _ldap_client(backend)
        backend._setup_project_directory = MagicMock()
        backend.client.qos_exists.return_value = False

        info, resource = self._run(backend)

        assert info.backend_id == "hpc_proj-0"
        ldap.create_project_group.assert_called_once_with("hpc_proj-0")
        backend._setup_project_directory.assert_called_once_with("hpc_proj-0")
        backend.client.create_qos.assert_called_once()
        assert backend.client.create_qos.call_args.kwargs["name"] == "hpc_proj-0"
        backend.client.set_account_default_qos.assert_called_once_with(
            "hpc_proj-0", "hpc_proj-0"
        )

        # The regression itself: add_user finds the group creation made.
        resource.backend_id = info.backend_id
        backend.add_user(resource, "alice")
        ldap.add_user_to_group.assert_called_once_with("hpc_proj-0", "alice")


class TestHealLegacyProjectGroup:
    """Groups created before the fix are named after the slug; rename them."""

    def test_add_user_renames_slug_named_group(self):
        backend = _backend()
        ldap = _ldap_client(backend, {"a-alloc1"})

        backend.add_user(_waldur_resource(backend_id="a-proj-0"), "alice")

        ldap.rename_group.assert_called_once_with("a-alloc1", "a-proj-0")
        ldap.add_user_to_group.assert_called_once_with("a-proj-0", "alice")

    def test_remove_user_renames_slug_named_group(self):
        backend = _backend()
        ldap = _ldap_client(backend, {"a-alloc1"})
        backend.client.get_association.return_value = None

        backend.remove_user(_waldur_resource(backend_id="a-proj-0"), "alice")

        ldap.rename_group.assert_called_once_with("a-alloc1", "a-proj-0")
        ldap.remove_user_from_group.assert_called_once_with("a-proj-0", "alice")

    def test_delete_renames_before_deleting(self):
        backend = _backend()
        ldap = _ldap_client(backend, {"a-alloc1"})
        backend.client.account_has_users.return_value = False

        backend._pre_delete_resource(_waldur_resource(backend_id="a-proj-0"))

        ldap.rename_group.assert_called_once_with("a-alloc1", "a-proj-0")
        ldap.delete_group.assert_called_once_with("a-proj-0")

    def test_no_rename_when_slug_name_is_a_live_account(self):
        backend = _backend()
        ldap = _ldap_client(backend, {"a-alloc1"})
        backend.client.get_resource.side_effect = lambda name: (
            {"name": name} if name == "a-alloc1" else None
        )

        backend.add_user(_waldur_resource(backend_id="a-proj-0"), "alice")

        ldap.rename_group.assert_not_called()

    @pytest.mark.parametrize(
        ("groups", "backend_id"),
        [
            ({"a-proj-0", "a-alloc1"}, "a-proj-0"),  # the right group already exists
            (set(), "a-proj-0"),  # no slug-named group to rename
            ({"a-alloc1"}, "a-alloc1"),  # names never diverged
        ],
    )
    def test_no_rename_needed(self, groups, backend_id):
        backend = _backend()
        ldap = _ldap_client(backend, groups)

        backend.add_user(_waldur_resource(backend_id=backend_id), "alice")

        ldap.rename_group.assert_not_called()

    def test_rename_failure_does_not_block_add_user(self):
        backend = _backend()
        ldap = _ldap_client(backend, {"a-alloc1"})
        ldap.rename_group.side_effect = BackendError("boom")

        assert backend.add_user(_waldur_resource(backend_id="a-proj-0"), "alice") is True
        ldap.add_user_to_group.assert_called_once_with("a-proj-0", "alice")
