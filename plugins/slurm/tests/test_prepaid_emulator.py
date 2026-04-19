"""Integration tests for prepaid SLURM limits using slurm-emulator.

Verifies the exact GrpTRESMins values set on SLURM accounts for
different prepaid scenarios: initial creation, renewal (end_date
extension), and limit increase mid-term.

Requires slurm-emulator package (dev dependency).
"""

import datetime
from typing import Optional
from unittest import mock
from unittest.mock import patch

import pytest

try:
    from emulator.commands.sacctmgr import SacctmgrEmulator
    from emulator.core.database import Account, SlurmDatabase
    from emulator.core.time_engine import TimeEngine

    EMULATOR_AVAILABLE = True
except ImportError:
    EMULATOR_AVAILABLE = False

from waldur_api_client.types import UNSET

from waldur_site_agent_slurm.backend import SlurmBackend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def time_engine() -> "TimeEngine":
    """Create a time engine at a known date."""
    engine = TimeEngine()
    engine.set_time(datetime.datetime(2026, 1, 1))
    return engine


@pytest.fixture()
def slurm_database() -> "SlurmDatabase":
    """Create a clean SLURM database with a root account."""
    database = SlurmDatabase()
    root = Account(name="root", description="root", organization="root")
    database.accounts["root"] = root
    return database


@pytest.fixture()
def sacctmgr_emulator(
    slurm_database: "SlurmDatabase", time_engine: "TimeEngine",
) -> "SacctmgrEmulator":
    """Create a sacctmgr emulator instance."""
    return SacctmgrEmulator(slurm_database, time_engine)


@pytest.fixture()
def patched_slurm_client(sacctmgr_emulator: "SacctmgrEmulator") -> None:
    """Patch SlurmClient to use emulator instead of real SLURM commands."""

    def mock_execute_command(
        args: list,
        command_name: str = "sacctmgr",
        immediate: bool = True,
        parsable: bool = True,
        silent: bool = False,
    ) -> str:
        del immediate, parsable, silent
        if command_name == "sacctmgr":
            return sacctmgr_emulator.handle_command(args)
        if command_name == "sinfo":
            return "slurm-emulator 0.1.0"
        return ""

    with patch(
        "waldur_site_agent_slurm.client.SlurmClient._execute_command",
        side_effect=mock_execute_command,
    ):
        yield


_MISSING = object()


def _make_waldur_resource(
    limits: Optional[dict] = None,
    created: object = _MISSING,
    end_date: object = _MISSING,
    backend_id: str = "",
    name: str = "test-resource",
) -> mock.MagicMock:
    """Create a mock WaldurResource."""
    resource = mock.MagicMock()
    resource.backend_id = backend_id
    resource.name = name
    if limits is not None:
        resource.limits.to_dict.return_value = limits
    else:
        resource.limits = None
    resource.created = UNSET if created is _MISSING else created
    resource.end_date = UNSET if end_date is _MISSING else end_date
    return resource


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not EMULATOR_AVAILABLE, reason="slurm-emulator not installed")
class TestPrepaidEmulatorScenarios:
    """Verify exact GrpTRESMins values against slurm-emulator.

    Each test creates a SLURM account via the backend and reads back
    the limits from the emulator's internal database to verify the
    GrpTRESMins values match expectations.
    """

    def _make_backend(
        self, components: dict, settings: Optional[dict] = None,
    ) -> SlurmBackend:
        """Create a SlurmBackend with emulator-patched client."""
        from unittest import mock as _mock  # noqa: PLC0415

        default_settings = {
            "default_account": "root",
            "customer_prefix": "",
            "project_prefix": "",
            "allocation_prefix": "",
        }
        if settings:
            default_settings.update(settings)
        backend = SlurmBackend(default_settings, components)
        # The emulator doesn't register custom TRES types (e.g. node_hours), so
        # list_components() would return only built-in SLURM TRES and cause custom
        # components to be filtered out. Simulate the real-cluster behavior where
        # all configured component keys are registered TRES types.
        backend.list_components = _mock.MagicMock(return_value=list(components.keys()))
        return backend

    def _create_account(self, backend: SlurmBackend, name: str) -> None:
        """Create a SLURM account under root."""
        backend.client.create_resource(name, "test", "test", parent_name="root")

    @staticmethod
    def _get_grp_tres_mins(
        slurm_database: "SlurmDatabase", account_name: str,
    ) -> dict:
        """Read GrpTRESMins from emulator's internal database.

        The emulator stores limits as ``{'GrpTRESMins:cpu': 1800, ...}``.
        This helper extracts and returns ``{'cpu': 1800, ...}``.
        """
        acct = slurm_database.accounts.get(account_name)
        if acct is None:
            return {}
        prefix = "GrpTRESMins:"
        return {
            k[len(prefix):]: int(v)
            for k, v in acct.limits.items()
            if k.startswith(prefix)
        }

    def test_6_node_hours_for_5_months(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """6 node-hours/month for 5 months -> GrpTRESMins=node_hours=1800."""
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "one",
                "label": "Node Hours",
                "is_prepaid": True,
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-5mo")

        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),  # 5 months
            backend_id="prepaid-5mo",
        )
        backend._setup_resource_limits("prepaid-5mo", resource)

        actual = self._get_grp_tres_mins(slurm_database, "prepaid-5mo")
        # 6 hours * 60 min/hour * 5 months = 1800
        expected = 1800
        assert actual.get("node_hours") == expected

    def test_6_node_hours_for_12_months(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """6 node-hours/month for 12 months -> GrpTRESMins=node_hours=4320."""
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "one",
                "label": "Node Hours",
                "is_prepaid": True,
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-12mo")

        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2027, 1, 1),  # 12 months
            backend_id="prepaid-12mo",
        )
        backend._setup_resource_limits("prepaid-12mo", resource)

        actual = self._get_grp_tres_mins(slurm_database, "prepaid-12mo")
        # 6 * 60 * 12 = 4320
        expected = 4320
        assert actual.get("node_hours") == expected

    def test_renewal_extends_budget(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """Renewal from 5 to 8 months: GrpTRESMins 1800 -> 2880."""
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "one",
                "label": "Node Hours",
                "is_prepaid": True,
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-renewal")

        # Initial: 5 months
        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),
            backend_id="prepaid-renewal",
        )
        backend._setup_resource_limits("prepaid-renewal", resource)

        before = self._get_grp_tres_mins(slurm_database, "prepaid-renewal")
        assert before.get("node_hours") == 1800  # noqa: PLR2004

        # Renewal: extend to 8 months (same limits, new end_date)
        resource.end_date = datetime.date(2026, 9, 1)
        backend.sync_resource_end_date(resource, mock.MagicMock())

        after = self._get_grp_tres_mins(slurm_database, "prepaid-renewal")
        # 6 * 60 * 8 = 2880
        expected = 2880
        assert after.get("node_hours") == expected

    def test_limit_increase_midterm(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """Limit increase 6->10 for 5 months: GrpTRESMins 1800 -> 3000."""
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "one",
                "label": "Node Hours",
                "is_prepaid": True,
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-increase")

        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),
            backend_id="prepaid-increase",
        )
        backend._setup_resource_limits("prepaid-increase", resource)
        assert self._get_grp_tres_mins(slurm_database, "prepaid-increase").get("node_hours") == 1800  # noqa: PLR2004

        # Increase limit to 10
        resource.limits.to_dict.return_value = {"node_hours": 10}
        backend._setup_resource_limits("prepaid-increase", resource)

        actual = self._get_grp_tres_mins(slurm_database, "prepaid-increase")
        # 10 * 60 * 5 = 3000
        expected = 3000
        assert actual.get("node_hours") == expected

    def test_multi_component_prepaid(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """Multiple prepaid components: cpu=600, gpu=24 for 6 months."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
                "is_prepaid": True,
            },
            "gpu": {
                "measured_unit": "units",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "GPU",
                "is_prepaid": True,
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-multi")

        resource = _make_waldur_resource(
            limits={"cpu": 100, "gpu": 4},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 7, 1),  # 6 months
            backend_id="prepaid-multi",
        )
        backend._setup_resource_limits("prepaid-multi", resource)

        actual = self._get_grp_tres_mins(slurm_database, "prepaid-multi")
        # cpu: 100*1*6=600, gpu: 4*1*6=24
        expected_cpu = 600
        expected_gpu = 24
        assert actual.get("cpu") == expected_cpu
        assert actual.get("gpu") == expected_gpu

    def test_non_prepaid_not_multiplied(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """Non-prepaid limit: GrpTRESMins=storage=500 (no duration mult)."""
        components = {
            "storage": {
                "measured_unit": "GB",
                "unit_factor": 1,
                "accounting_type": "limit",
                "label": "Storage",
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "regular-limit")

        resource = _make_waldur_resource(
            limits={"storage": 500},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),
            backend_id="regular-limit",
        )
        backend._setup_resource_limits("regular-limit", resource)

        actual = self._get_grp_tres_mins(slurm_database, "regular-limit")
        # No multiplication: 500 * 1 = 500
        expected = 500
        assert actual.get("storage") == expected

    def test_no_end_date_then_end_date_set_later(
        self,
        patched_slurm_client: None,
        slurm_database: "SlurmDatabase",
    ) -> None:
        """Resource created without end_date, then end_date set later.

        1. Create with no end_date: GrpTRESMins = limit * unit_factor (no months)
        2. Set end_date: sync_resource_end_date recalculates with duration
        """
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,
                "accounting_type": "one",
                "label": "Node Hours",
            },
        }
        backend = self._make_backend(components)
        self._create_account(backend, "prepaid-no-end")

        # Step 1: Create without end_date — plain limit, no duration
        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            backend_id="prepaid-no-end",
            # end_date omitted → UNSET
        )
        backend._setup_resource_limits("prepaid-no-end", resource)

        before = self._get_grp_tres_mins(slurm_database, "prepaid-no-end")
        # No duration multiplication: 6 * 60 = 360
        expected_no_duration = 360
        assert before.get("node_hours") == expected_no_duration

        # Step 2: Staff sets end_date on resource → sync picks it up
        resource.end_date = datetime.date(2026, 6, 1)  # 5 months from created
        backend.sync_resource_end_date(resource, mock.MagicMock())

        after = self._get_grp_tres_mins(slurm_database, "prepaid-no-end")
        # Now with duration: 6 * 60 * 5 = 1800
        expected_with_duration = 1800
        assert after.get("node_hours") == expected_with_duration
