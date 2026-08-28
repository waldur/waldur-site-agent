"""Opt-in qos_management.skip_qos_swap and apply_limits_to_qos.

These flags default off. Existing qos_management.enabled configs must keep
create-and-attach, account-level GrpTRESMins, and classic qos= restore/pause.
"""

from __future__ import annotations

import datetime
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.schemas import SlurmBackendSettingsSchema

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

_SCHEMA_BASE = {
    "default_account": "root",
    "customer_prefix": "c-",
    "project_prefix": "p-",
    "allocation_prefix": "a-",
}

_CPU_COMPONENTS = {
    "cpu": {
        "measured_unit": "minutes",
        "unit_factor": 1,
        "accounting_type": "limit",
        "label": "CPU",
    }
}

_PREPAID_COMPONENTS = {
    "cpu": {
        "measured_unit": "minutes",
        "unit_factor": 1,
        "accounting_type": "one",
        "label": "CPU",
    }
}


def _backend(
    qos_management: Optional[dict[str, Any]] = None,
    extra: Optional[dict[str, Any]] = None,
    components: Optional[dict[str, Any]] = None,
) -> SlurmBackend:
    settings = {"default_account": "root"}
    if qos_management is not None:
        settings["qos_management"] = qos_management
    if extra:
        settings.update(extra)
    backend = SlurmBackend(settings, components or _CPU_COMPONENTS)
    backend.client = MagicMock()
    backend.client.get_association.return_value = None
    backend.client.qos_exists.return_value = False
    backend.client.list_tres.return_value = ["cpu", "billing", "gres/gpu"]
    return backend


def _waldur_resource() -> MagicMock:
    resource = MagicMock()
    resource.uuid = "uuid-1"
    resource.name = "alloc"
    resource.slug = "alloc1"
    resource.backend_id = "a-alloc1"
    resource.customer_slug = "cust"
    resource.project_slug = "proj"
    resource.customer_name = "Cust"
    resource.project_name = "Proj"
    resource.limits = None
    resource.created = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    resource.end_date = UNSET
    return resource


class TestQosManagementSchema:
    def test_enabled_only_with_qos_paused_still_ok(self):
        """Enabled alone must not grow new constraints — existing deploys."""
        schema = SlurmBackendSettingsSchema(
            **_SCHEMA_BASE,
            qos_paused="paused",
            qos_management={"enabled": True},
        )
        assert schema.qos_management.enabled is True
        assert schema.qos_management.skip_qos_swap is False
        assert schema.qos_management.apply_limits_to_qos is False

    def test_skip_qos_swap_without_enabled_ok(self):
        schema = SlurmBackendSettingsSchema(
            **_SCHEMA_BASE,
            qos_management={"skip_qos_swap": True},
        )
        assert schema.qos_management.skip_qos_swap is True
        assert schema.qos_management.enabled is False

    def test_skip_qos_swap_with_qos_paused_rejected(self):
        with pytest.raises(ValidationError, match="skip_qos_swap"):
            SlurmBackendSettingsSchema(
                **_SCHEMA_BASE,
                qos_paused="paused",
                qos_management={"skip_qos_swap": True},
            )

    def test_skip_qos_swap_with_qos_downscaled_rejected(self):
        with pytest.raises(ValidationError, match="skip_qos_swap"):
            SlurmBackendSettingsSchema(
                **_SCHEMA_BASE,
                qos_downscaled="limited",
                qos_management={"enabled": True, "skip_qos_swap": True},
            )

    def test_skip_qos_swap_with_qos_default_rejected(self):
        with pytest.raises(ValidationError, match="qos_default"):
            SlurmBackendSettingsSchema(
                **_SCHEMA_BASE,
                qos_default="efp",
                qos_management={"skip_qos_swap": True},
            )

    def test_apply_limits_requires_enabled(self):
        with pytest.raises(ValidationError, match="apply_limits_to_qos"):
            SlurmBackendSettingsSchema(
                **_SCHEMA_BASE,
                qos_management={"apply_limits_to_qos": True, "skip_qos_swap": True},
            )

    def test_apply_limits_requires_skip_qos_swap(self):
        with pytest.raises(ValidationError, match="apply_limits_to_qos"):
            SlurmBackendSettingsSchema(
                **_SCHEMA_BASE,
                qos_management={"enabled": True, "apply_limits_to_qos": True},
            )

    def test_discoverer_combo_ok(self):
        schema = SlurmBackendSettingsSchema(
            **_SCHEMA_BASE,
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            },
        )
        assert schema.qos_management.apply_limits_to_qos is True


class TestQosManagementRuntimeValidation:
    """Invariants must raise from SlurmBackend.__init__ (schema path is soft-fail)."""

    def test_apply_limits_without_enabled_raises(self):
        with pytest.raises(
            BackendError, match="apply_limits_to_qos requires qos_management.enabled"
        ):
            SlurmBackend(
                {
                    "default_account": "root",
                    "qos_management": {"apply_limits_to_qos": True, "skip_qos_swap": True},
                },
                _CPU_COMPONENTS,
            )

    def test_apply_limits_without_skip_raises(self):
        with pytest.raises(BackendError, match="apply_limits_to_qos requires.*skip_qos_swap"):
            SlurmBackend(
                {
                    "default_account": "root",
                    "qos_management": {"enabled": True, "apply_limits_to_qos": True},
                },
                _CPU_COMPONENTS,
            )

    def test_skip_with_qos_paused_raises(self):
        with pytest.raises(BackendError, match="qos_paused/qos_downscaled"):
            SlurmBackend(
                {
                    "default_account": "root",
                    "qos_paused": "paused",
                    "qos_management": {"skip_qos_swap": True},
                },
                _CPU_COMPONENTS,
            )

    def test_skip_with_qos_default_raises(self):
        with pytest.raises(BackendError, match="qos_default"):
            SlurmBackend(
                {
                    "default_account": "root",
                    "qos_default": "efp",
                    "qos_management": {"skip_qos_swap": True},
                },
                _CPU_COMPONENTS,
            )

    def test_discoverer_combo_constructs(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            }
        )
        assert backend._qos_apply_limits() is True


class TestSkipQosSwap:
    def test_restore_clears_grp_submit_jobs_without_set_qos(self):
        backend = _backend(qos_management={"skip_qos_swap": True})
        assert backend.restore_resource("acct1") is True
        backend.client.set_account_qos.assert_not_called()
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", -1)

    def test_pause_blocks_submission_without_set_qos(self):
        backend = _backend(qos_management={"skip_qos_swap": True})
        assert backend.pause_resource("acct1") is True
        backend.client.set_account_qos.assert_not_called()
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)

    def test_downscale_blocks_submission_without_set_qos(self):
        backend = _backend(qos_management={"skip_qos_swap": True})
        assert backend.downscale_resource("acct1") is True
        backend.client.set_account_qos.assert_not_called()
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)

    def test_skip_qos_swap_does_not_disable_enforcement_pause(self):
        """skip_qos_swap must not sit above qos_enforced and turn pause into a no-op."""
        backend = _backend(
            qos_management={"skip_qos_swap": True},
            extra={"qos_enforcement_enabled": True, "enforce_offering_qos": True},
        )
        # Mirror what the processor would push when the offering opts in.
        backend._qos_enforcement_enabled = True
        backend._enforce_offering_qos = True
        backend.offering_enforce_qos = True
        assert backend.pause_resource("acct1") is True
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)
        backend.client.set_account_qos.assert_not_called()

    def test_restore_still_swaps_when_flag_off(self):
        backend = _backend(
            qos_management={"enabled": True},
            extra={"qos_default": "efp"},
        )
        backend.client.get_current_account_qos.return_value = "limited"
        assert backend.restore_resource("acct1") is True
        backend.client.set_account_qos.assert_called_once_with("acct1", "efp")

    def test_pause_still_swaps_when_flag_off(self):
        backend = _backend(
            qos_management={"enabled": True},
            extra={"qos_paused": "paused"},
        )
        backend.client.get_current_account_qos.return_value = "efp"
        assert backend.pause_resource("acct1") is True
        backend.client.set_account_qos.assert_called_once_with("acct1", "paused")
        backend.client.set_account_grp_submit_jobs.assert_not_called()


class TestQosAttachSequencing:
    def test_enabled_only_still_attaches_during_pre_create(self):
        backend = _backend(
            qos_management={"enabled": True, "additional_qos": ["2cpu-single-host"]},
            extra={
                "customer_prefix": "c-",
                "project_prefix": "p-",
                "allocation_prefix": "a-",
            },
        )
        backend._create_backend_resource = MagicMock(return_value=True)
        backend._pre_create_resource(_waldur_resource())
        backend.client.create_qos.assert_called_once()
        backend.client.add_account_qos.assert_any_call("a-alloc1", "a-alloc1")
        backend.client.set_account_default_qos.assert_called_once_with("a-alloc1", "a-alloc1")
        backend.client.add_account_qos.assert_any_call("a-alloc1", "2cpu-single-host")

    def test_skip_qos_swap_defers_attach_until_post_create(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "additional_qos": ["2cpu-single-host"],
            },
            extra={
                "customer_prefix": "c-",
                "project_prefix": "p-",
                "allocation_prefix": "a-",
            },
        )
        backend._create_backend_resource = MagicMock(return_value=True)
        backend._pre_create_resource(_waldur_resource())
        backend.client.create_qos.assert_called_once()
        backend.client.add_account_qos.assert_not_called()
        backend.client.set_account_default_qos.assert_not_called()

        backend.post_create_resource(
            BackendResourceInfo(backend_id="a-alloc1"), _waldur_resource(), None
        )
        backend.client.add_account_qos.assert_any_call("a-alloc1", "a-alloc1")
        backend.client.set_account_default_qos.assert_called_once_with("a-alloc1", "a-alloc1")
        backend.client.add_account_qos.assert_any_call("a-alloc1", "2cpu-single-host")

    def test_enabled_only_skips_attach_when_qos_already_exists(self):
        backend = _backend(qos_management={"enabled": True})
        backend.client.qos_exists.return_value = True
        backend._setup_account_qos("acct1")
        backend.client.create_qos.assert_not_called()
        backend.client.add_account_qos.assert_not_called()


class TestApplyLimitsToQos:
    def test_set_resource_limits_writes_qos_not_account(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            }
        )
        backend.set_resource_limits("acct1", {"cpu": 6912000})
        backend.client.set_qos_grp_tres_mins.assert_called_once_with("acct1", {"cpu": 6912000})
        backend.client.set_resource_limits.assert_not_called()
        backend.client.reset_qos_raw_usage.assert_not_called()

    def test_set_resource_limits_still_writes_account_when_flag_off(self):
        backend = _backend(qos_management={"enabled": True})
        backend.set_resource_limits("acct1", {"cpu": 100})
        backend.client.set_resource_limits.assert_called_once()
        backend.client.set_qos_grp_tres_mins.assert_not_called()

    def test_setup_resource_limits_resets_qos_raw_usage_when_qos_created(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            },
            extra={
                "customer_prefix": "c-",
                "project_prefix": "p-",
                "allocation_prefix": "a-",
            },
        )
        backend._create_backend_resource = MagicMock(return_value=True)
        resource = _waldur_resource()
        resource.limits = MagicMock()
        resource.limits.to_dict.return_value = {"cpu": 50}
        backend._pre_create_resource(resource)
        assert backend._qos_created_this_cycle is True
        backend._setup_resource_limits("a-alloc1", resource)
        backend.client.set_qos_grp_tres_mins.assert_called_once_with("a-alloc1", {"cpu": 50})
        backend.client.reset_qos_raw_usage.assert_called_once_with("a-alloc1")
        backend.client.set_resource_limits.assert_not_called()
        assert backend._qos_created_this_cycle is False

    def test_setup_resource_limits_skips_raw_usage_reset_when_qos_existed(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            },
            extra={
                "customer_prefix": "c-",
                "project_prefix": "p-",
                "allocation_prefix": "a-",
            },
        )
        backend.client.qos_exists.return_value = True
        backend._create_backend_resource = MagicMock(return_value=True)
        resource = _waldur_resource()
        resource.limits = MagicMock()
        resource.limits.to_dict.return_value = {"cpu": 50}
        backend._pre_create_resource(resource)
        assert backend._qos_created_this_cycle is False
        backend._setup_resource_limits("a-alloc1", resource)
        backend.client.set_qos_grp_tres_mins.assert_called_once_with("a-alloc1", {"cpu": 50})
        backend.client.reset_qos_raw_usage.assert_not_called()
        backend.client.create_qos.assert_not_called()

    def test_get_resource_limits_reads_qos(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            }
        )
        backend.client.get_qos_grp_tres_mins.return_value = {"cpu": 60000, "billing": 1}
        assert backend.get_resource_limits("acct1") == {"cpu": 60000}
        backend.client.get_resource_limits.assert_not_called()

    def test_get_resource_limits_still_reads_account_when_flag_off(self):
        backend = _backend(qos_management={"enabled": True})
        backend.client.get_resource_limits.return_value = {"cpu": 10}
        assert backend.get_resource_limits("acct1") == {"cpu": 10}
        backend.client.get_qos_grp_tres_mins.assert_not_called()

    def test_get_resource_limits_filters_unknown_account_tres(self):
        backend = _backend(qos_management={"enabled": True})
        backend.client.get_resource_limits.return_value = {"cpu": 10, "mem": 99}
        assert backend.get_resource_limits("acct1") == {"cpu": 10}

    def test_mapped_limits_land_on_qos(self):
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 1,
                "accounting_type": "limit",
                "label": "Compute",
                "target_components": {
                    "billing": {"factor": 66.18 * 60},
                    "gres/gpu": {"factor": 60},
                },
            }
        }
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            },
            components=components,
        )
        backend.set_resource_limits("acct1", {"node_hours": 5000})
        written = backend.client.set_qos_grp_tres_mins.call_args[0][1]
        assert written["gres/gpu"] == 5000 * 60
        assert written["billing"] == int(5000 * 66.18 * 60)
        backend.client.set_resource_limits.assert_not_called()

    def test_prepaid_end_date_sync_writes_qos_without_raw_usage_reset(self):
        backend = _backend(
            qos_management={
                "enabled": True,
                "skip_qos_swap": True,
                "apply_limits_to_qos": True,
            },
            components=_PREPAID_COMPONENTS,
        )
        resource = _waldur_resource()
        resource.backend_id = "acct1"
        resource.limits = MagicMock()
        resource.limits.to_dict.return_value = {"cpu": 100}
        resource.end_date = datetime.date(2026, 6, 1)
        backend.sync_resource_end_date(resource, MagicMock())
        backend.client.set_qos_grp_tres_mins.assert_called_once()
        written = backend.client.set_qos_grp_tres_mins.call_args[0][1]
        assert written["cpu"] == 100 * 5  # Jan→Jun: 5 months
        backend.client.reset_qos_raw_usage.assert_not_called()
        backend.client.set_resource_limits.assert_not_called()
