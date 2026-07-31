"""Tests for opt-in QoS enforcement in the SLURM backend.

Covers:
- ``qos_enforced()`` resolution: agent override (None/True/False) wins over the
  per-offering flag.
- ``add_user`` grants the selected QoS on the association when enforcing.
- pause/downscale/restore use the orthogonal GrpSubmitJobs lever when enforcing
  (so a pause never clobbers the per-association grant), and the QoS-swap lever
  otherwise.
- config validation rejects forcing enforcement together with the QoS-swap
  pause settings.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.schemas import SlurmBackendSettingsSchema


def _backend(enforce=None, offering_enforce=False, settings=None, enabled=True):
    base = {"default_account": "root"}
    if settings:
        base.update(settings)
    backend = SlurmBackend(base, {"cpu": {"unit": "minutes"}})
    backend.client = MagicMock()
    backend.client.get_association.return_value = None
    # Patch the opt-in gate + cached agent override + processor-pushed offering flag.
    backend._qos_enforcement_enabled = enabled
    backend._enforce_offering_qos = enforce
    backend.offering_enforce_qos = offering_enforce
    return backend


def _resource(attributes=None):
    resource = MagicMock()
    resource.backend_id = "acct1"
    if attributes is None:
        resource.attributes = None
    else:
        resource.attributes.additional_properties = attributes
    return resource


class TestQoSEnforcedResolution:
    def test_offering_flag_used_when_no_override(self):
        assert _backend(enforce=None, offering_enforce=True).qos_enforced() is True
        assert _backend(enforce=None, offering_enforce=False).qos_enforced() is False

    def test_agent_override_true_wins(self):
        assert _backend(enforce=True, offering_enforce=False).qos_enforced() is True

    def test_agent_override_false_wins(self):
        assert _backend(enforce=False, offering_enforce=True).qos_enforced() is False

    def test_disabled_gate_forces_off_regardless(self):
        # Opt-in: with the gate off, nothing enforces — not the offering flag,
        # not even a forced agent override.
        assert _backend(enforce=True, offering_enforce=True, enabled=False).qos_enforced() is False
        assert _backend(enforce=None, offering_enforce=True, enabled=False).qos_enforced() is False

    def test_gate_off_is_the_default(self):
        # A backend with no QoS settings does not enforce even if the offering asks.
        backend = SlurmBackend({"default_account": "root"}, {"cpu": {"unit": "minutes"}})
        backend.offering_enforce_qos = True
        assert backend.qos_enforced() is False


class TestAddUserGrant:
    def test_grants_selected_qos_when_enforced(self):
        backend = _backend(offering_enforce=True)
        backend.add_user(_resource({"qos": "boost", "partition": "gpu"}), "alice")
        backend.client.create_association_with_qos.assert_called_once()
        args, kwargs = backend.client.create_association_with_qos.call_args
        assert args[0] == "alice"
        assert args[1] == "acct1"
        assert args[2] == ["boost"]
        assert kwargs["default_qos"] == "boost"
        # The user's selected partition scopes the grant.
        assert kwargs["partitions"] == ["gpu"]

    def test_qos_composes_across_offering_partitions(self):
        # No partition selected, but the offering enforces a partition set:
        # the QoS must span all of them rather than silently dropping them.
        backend = _backend(offering_enforce=True)
        backend.offering_partitions = ["cn", "gpu"]
        backend._enforce_offering_partitions = True
        backend.add_user(_resource({"qos": "boost"}), "alice")
        _, kwargs = backend.client.create_association_with_qos.call_args
        assert kwargs["partitions"] == ["cn", "gpu"]

    def test_qos_falls_back_to_default_partition(self):
        backend = _backend(offering_enforce=True)
        backend._default_partition = "batch"
        backend.add_user(_resource({"qos": "boost"}), "alice")
        _, kwargs = backend.client.create_association_with_qos.call_args
        assert kwargs["partitions"] == ["batch"]

    def test_no_grant_when_not_enforced(self):
        backend = _backend(offering_enforce=False)
        backend.add_user(_resource({"qos": "boost"}), "alice")
        backend.client.create_association_with_qos.assert_not_called()
        backend.client.create_association.assert_called_once()

    def test_no_grant_when_no_selected_qos(self):
        backend = _backend(offering_enforce=True)
        backend.add_user(_resource({}), "alice")
        backend.client.create_association_with_qos.assert_not_called()

    def test_no_grant_when_attributes_absent(self):
        backend = _backend(offering_enforce=True)
        backend.add_user(_resource(None), "alice")
        backend.client.create_association_with_qos.assert_not_called()


class TestOrthogonalPauseLever:
    def test_pause_blocks_submission_when_enforced(self):
        backend = _backend(offering_enforce=True)
        assert backend.pause_resource("acct1") is True
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)
        backend.client.set_account_qos.assert_not_called()

    def test_downscale_blocks_submission_when_enforced(self):
        backend = _backend(offering_enforce=True)
        assert backend.downscale_resource("acct1") is True
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", 0)
        backend.client.set_account_qos.assert_not_called()

    def test_restore_clears_lever_when_enforced(self):
        backend = _backend(offering_enforce=True)
        assert backend.restore_resource("acct1") is True
        backend.client.set_account_grp_submit_jobs.assert_called_once_with("acct1", -1)
        backend.client.set_account_qos.assert_not_called()

    def test_pause_uses_qos_swap_when_not_enforced(self):
        backend = _backend(offering_enforce=False, settings={"qos_paused": "paused"})
        backend.client.get_current_account_qos.return_value = "normal"
        assert backend.pause_resource("acct1") is True
        backend.client.set_account_qos.assert_called_once_with("acct1", "paused")
        backend.client.set_account_grp_submit_jobs.assert_not_called()


class TestConfigConflictValidation:
    _base = {
        "default_account": "root",
        "customer_prefix": "c-",
        "project_prefix": "p-",
        "allocation_prefix": "a-",
    }

    def test_force_enforce_with_qos_paused_rejected(self):
        with pytest.raises(ValidationError, match="qos_enforcement_enabled"):
            SlurmBackendSettingsSchema(
                **self._base,
                qos_enforcement_enabled=True,
                enforce_offering_qos=True,
                qos_paused="paused",
            )

    def test_force_enforce_with_qos_downscaled_rejected(self):
        with pytest.raises(ValidationError, match="qos_enforcement_enabled"):
            SlurmBackendSettingsSchema(
                **self._base,
                qos_enforcement_enabled=True,
                enforce_offering_qos=True,
                qos_downscaled="limited",
            )

    def test_force_enforce_without_qos_swap_ok(self):
        schema = SlurmBackendSettingsSchema(
            **self._base, qos_enforcement_enabled=True, enforce_offering_qos=True
        )
        assert schema.enforce_offering_qos is True

    def test_force_enforce_with_swap_but_gate_off_ok(self):
        # Gate off means enforcement is unreachable, so the swap-pause config is
        # not dead — no conflict, even with enforce_offering_qos=True.
        schema = SlurmBackendSettingsSchema(
            **self._base, enforce_offering_qos=True, qos_paused="paused"
        )
        assert schema.qos_enforcement_enabled is False

    def test_qos_swap_without_forced_enforce_ok(self):
        # Per-offering enforcement (override unset) coexists with qos_paused —
        # the conflict is resolved per-offering at runtime, not at config load.
        schema = SlurmBackendSettingsSchema(
            **self._base, qos_enforcement_enabled=True, qos_paused="paused"
        )
        assert schema.enforce_offering_qos is None
