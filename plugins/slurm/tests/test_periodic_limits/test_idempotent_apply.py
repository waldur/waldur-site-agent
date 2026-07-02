"""Tests for idempotent (diff-and-skip) production apply of periodic settings.

The production apply path must diff each requested setting against current
SLURM state and skip unchanged ones, so periodic re-applies with identical
values do not emit redundant ``sacctmgr modify`` commands. Reads are
fail-safe: a failed or unknown read falls back to applying the setting.
"""

from unittest.mock import MagicMock

import pytest
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_slurm.backend import SlurmBackend
from waldur_site_agent_slurm.client import SlurmClient

ACCOUNT = "test-project-123"


def _limits(grp_tres_mins=None, max_tres_mins=None):
    """Build a get_account_limits() return payload with string TRES values."""
    return {
        "GrpTRES": {},
        "GrpTRESMins": grp_tres_mins or {},
        "MaxTRES": {},
        "MaxTRESMins": max_tres_mins or {},
    }


class TestIdempotentProductionApply:
    """Diff-and-skip behaviour of _apply_settings_production."""

    @pytest.fixture
    def backend(self):
        """Backend in production mode with a spec'd, fully mocked client."""
        config = {
            "periodic_limits": {
                "enabled": True,
                "emulator_mode": False,
                "limit_type": "GrpTRESMins",
            }
        }
        backend = SlurmBackend(config, {})
        backend.client = MagicMock(spec=SlurmClient)
        backend.client.executed_commands = []
        return backend

    def test_unchanged_fairshare_and_limits_skip_all(self, backend):
        """Matching fairshare and limits emit no set/modify calls."""
        backend.client.get_account_fairshare.return_value = 666
        backend.client.get_account_limits.return_value = _limits(
            grp_tres_mins={"billing": "119400"}
        )

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 119400},
            "limit_type": "GrpTRESMins",
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_not_called()
        backend.client.set_account_limits.assert_not_called()
        backend.client.reset_raw_usage.assert_not_called()

    def test_changed_fairshare_is_applied(self, backend):
        """A differing fairshare is set; unchanged limits are skipped."""
        backend.client.get_account_fairshare.return_value = 500
        backend.client.get_account_limits.return_value = _limits(
            grp_tres_mins={"billing": "119400"}
        )

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 119400},
            "limit_type": "GrpTRESMins",
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_called_once_with(ACCOUNT, 666)
        backend.client.set_account_limits.assert_not_called()

    def test_only_changed_tres_is_applied(self, backend):
        """Only the TRES whose value differs is set; matching ones are skipped."""
        backend.client.get_account_fairshare.return_value = 666
        backend.client.get_account_limits.return_value = _limits(
            grp_tres_mins={"billing": "119400", "cpu": "1000"}
        )

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 120000, "cpu": 1000},
            "limit_type": "GrpTRESMins",
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_not_called()
        backend.client.set_account_limits.assert_called_once_with(
            ACCOUNT, "GrpTRESMins", {"billing": 120000}
        )

    def test_fairshare_getter_raises_falls_back_to_applying(self, backend):
        """A read failure must not skip the fairshare change."""
        backend.client.get_account_fairshare.side_effect = BackendError("SLURM down")
        backend.client.get_account_limits.return_value = _limits(
            grp_tres_mins={"billing": "119400"}
        )

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 119400},
            "limit_type": "GrpTRESMins",
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_called_once_with(ACCOUNT, 666)

    def test_limits_getter_returns_none_falls_back_to_applying(self, backend):
        """An unknown (None) current limits read applies the full target."""
        backend.client.get_account_fairshare.return_value = 666
        backend.client.get_account_limits.return_value = None

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 119400},
            "limit_type": "GrpTRESMins",
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_not_called()
        backend.client.set_account_limits.assert_called_once_with(
            ACCOUNT, "GrpTRESMins", {"billing": 119400}
        )

    def test_fairshare_getter_returns_none_falls_back_to_applying(self, backend):
        """A None fairshare read applies the setting rather than skipping."""
        backend.client.get_account_fairshare.return_value = None
        backend.client.get_account_limits.return_value = _limits()

        settings = {"fairshare": 666}
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_called_once_with(ACCOUNT, 666)

    def test_reset_raw_usage_applied_even_when_unchanged(self, backend):
        """reset_raw_usage is applied regardless of fairshare/limits diffs."""
        backend.client.get_account_fairshare.return_value = 666
        backend.client.get_account_limits.return_value = _limits(
            grp_tres_mins={"billing": "119400"}
        )

        settings = {
            "fairshare": 666,
            "grp_tres_mins": {"billing": 119400},
            "limit_type": "GrpTRESMins",
            "reset_raw_usage": True,
        }
        result = backend.apply_periodic_settings(ACCOUNT, settings)

        assert result["success"] is True
        backend.client.set_account_fairshare.assert_not_called()
        backend.client.set_account_limits.assert_not_called()
        backend.client.reset_raw_usage.assert_called_once_with(ACCOUNT)
