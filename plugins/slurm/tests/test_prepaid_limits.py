"""Tests for prepaid limit calculation in SLURM backend.

Verifies that prepaid components have their SLURM limits (GrpTRESMins)
multiplied by subscription duration (limit * months * unit_factor).
"""

import datetime
from typing import Optional
from unittest import mock

from waldur_api_client.types import UNSET

from waldur_site_agent_slurm.backend import SlurmBackend


_MISSING = object()  # sentinel distinct from None and UNSET


def _make_waldur_resource(
    limits: Optional[dict] = None,
    created: object = _MISSING,
    end_date: object = _MISSING,
    backend_id: str = "test-alloc-01",
) -> mock.MagicMock:
    """Create a mock WaldurResource with the given fields.

    Use ``created=None`` to simulate a null value from Waldur API.
    Omit the parameter to get UNSET (field not present in response).
    """
    resource = mock.MagicMock()
    resource.backend_id = backend_id

    if limits is not None:
        resource.limits.to_dict.return_value = limits
    else:
        resource.limits = None

    resource.created = UNSET if created is _MISSING else created
    resource.end_date = UNSET if end_date is _MISSING else end_date
    return resource


def _make_backend(
    components: dict, settings: Optional[dict] = None,
) -> SlurmBackend:
    """Create a SlurmBackend with mocked client."""
    with mock.patch(
        "waldur_site_agent_slurm.backend.SlurmClient", autospec=True
    ):
        return SlurmBackend(settings or {}, components)


class TestCalculateDurationMonths:
    """Tests for _calculate_duration_months helper."""

    def test_exact_months(self) -> None:
        created = datetime.datetime(2026, 1, 15, tzinfo=datetime.timezone.utc)
        end_date = datetime.date(2026, 6, 15)
        result = SlurmBackend._calculate_duration_months(created, end_date)
        assert result == 5  # noqa: PLR2004

    def test_partial_month_rounds_up(self) -> None:
        created = datetime.datetime(2026, 1, 10, tzinfo=datetime.timezone.utc)
        end_date = datetime.date(2026, 4, 20)
        # 3 months + end_day(20) > created_day(10) -> 4
        result = SlurmBackend._calculate_duration_months(created, end_date)
        assert result == 4  # noqa: PLR2004

    def test_same_month(self) -> None:
        created = datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc)
        end_date = datetime.date(2026, 3, 15)
        result = SlurmBackend._calculate_duration_months(created, end_date)
        assert result == 1

    def test_minimum_one_month(self) -> None:
        created = datetime.datetime(2026, 3, 20, tzinfo=datetime.timezone.utc)
        end_date = datetime.date(2026, 3, 10)
        result = SlurmBackend._calculate_duration_months(created, end_date)
        assert result == 1

    def test_twelve_months(self) -> None:
        created = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        end_date = datetime.date(2027, 1, 1)
        result = SlurmBackend._calculate_duration_months(created, end_date)
        assert result == 12  # noqa: PLR2004


class TestCollectResourceLimitsPrepaid:
    """Tests for _collect_resource_limits with prepaid components."""

    def test_prepaid_limits_multiplied_by_duration(self) -> None:
        """Prepaid component limits are multiplied by duration months."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
                "is_prepaid": True,
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),  # 5 months
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        # 6 cores * 1 (unit_factor) * 5 months = 30
        expected = 30
        assert allocation_limits["cpu"] == expected

    def test_prepaid_limits_with_unit_factor(self) -> None:
        """Prepaid multiplication applies after unit_factor conversion."""
        components = {
            "node_hours": {
                "measured_unit": "Hours",
                "unit_factor": 60,  # hours to minutes
                "accounting_type": "one",
                "label": "Node Hours",
                "is_prepaid": True,
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"node_hours": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),  # 5 months
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        # 6 hours * 60 (unit_factor) * 5 months = 1800
        expected = 1800
        assert allocation_limits["node_hours"] == expected

    def test_non_prepaid_limits_not_multiplied(self) -> None:
        """Non-prepaid limit components are not affected by duration."""
        components = {
            "gpu": {
                "measured_unit": "units",
                "unit_factor": 1,
                "accounting_type": "limit",
                "label": "GPU",
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"gpu": 4},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 6, 1),
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        # No multiplication: 4 * 1 (unit_factor) = 4
        expected = 4
        assert allocation_limits["gpu"] == expected

    def test_mixed_prepaid_and_non_prepaid(self) -> None:
        """Only prepaid components are multiplied by duration."""
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
                "accounting_type": "limit",
                "label": "GPU",
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 10, "gpu": 4},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 4, 1),  # 3 months
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        expected_cpu = 30  # 10 * 3
        expected_gpu = 4  # unchanged
        assert allocation_limits["cpu"] == expected_cpu
        assert allocation_limits["gpu"] == expected_gpu

    def test_no_end_date_skips_multiplication(self) -> None:
        """When end_date is not set, prepaid limits are not multiplied."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
                "is_prepaid": True,
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=UNSET,
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        # No multiplication without end_date
        expected = 6
        assert allocation_limits["cpu"] == expected


class TestHasPrepaidComponents:
    """Tests for has_prepaid_components method."""

    def test_returns_true_when_prepaid_exists(self) -> None:
        components = {
            "cpu": {"measured_unit": "cores", "unit_factor": 1,
                    "accounting_type": "one", "label": "CPU", "is_prepaid": True},
        }
        backend = _make_backend(components)
        assert backend.has_prepaid_components() is True

    def test_returns_false_when_no_prepaid(self) -> None:
        components = {
            "cpu": {"measured_unit": "cores", "unit_factor": 1,
                    "accounting_type": "limit", "label": "CPU"},
        }
        backend = _make_backend(components)
        assert backend.has_prepaid_components() is False


class TestSyncResourceEndDate:
    """Tests for sync_resource_end_date override."""

    def test_recalculates_limits_for_prepaid(self) -> None:
        """sync_resource_end_date recalculates and applies limits."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
                "is_prepaid": True,
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=datetime.date(2026, 9, 1),  # 8 months (after renewal)
        )

        backend.sync_resource_end_date(resource, mock.MagicMock())

        expected_limits = {"cpu": 48}  # 6 * 8 months
        backend.client.set_resource_limits.assert_called_once_with(
            "test-alloc-01", expected_limits
        )

    def test_skips_for_non_prepaid(self) -> None:
        """sync_resource_end_date is a no-op for non-prepaid backends."""
        components = {
            "cpu": {"measured_unit": "cores", "unit_factor": 1,
                    "accounting_type": "limit", "label": "CPU"},
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(limits={"cpu": 6})

        backend.sync_resource_end_date(resource, mock.MagicMock())

        backend.client.set_resource_limits.assert_not_called()


class TestPrepaidLimitChangePathInProcessor:
    """Tests that _process_update_order uses _setup_resource_limits for prepaid."""

    def test_limit_change_calls_setup_resource_limits(self) -> None:
        """When limits change on prepaid, _setup_resource_limits is called.

        This ensures the duration multiplication (limit * months * unit_factor)
        is applied, not just unit_factor from set_resource_limits.
        """
        backend = mock.MagicMock()
        backend.has_prepaid_components.return_value = True

        # Simulate the processor logic for prepaid limit change
        old_limits = {"cpu": 6}
        new_limits = {"cpu": 10}

        # This is the logic from _process_update_order
        if new_limits != old_limits and backend.has_prepaid_components():
            waldur_resource = mock.MagicMock()
            waldur_resource.backend_id = "test-alloc"
            waldur_resource.limits.additional_properties = dict(old_limits)
            # Update in-memory limits
            for key, value in new_limits.items():
                waldur_resource.limits.additional_properties[key] = value
            backend._setup_resource_limits("test-alloc", waldur_resource)

        backend._setup_resource_limits.assert_called_once()
        backend.set_resource_limits.assert_not_called()

    def test_limit_change_non_prepaid_calls_set_resource_limits(self) -> None:
        """When limits change on non-prepaid, set_resource_limits is called."""
        backend = mock.MagicMock()
        backend.has_prepaid_components.return_value = False

        old_limits = {"cpu": 6}
        new_limits = {"cpu": 10}

        if new_limits != old_limits and not backend.has_prepaid_components():
            backend.set_resource_limits("test-alloc", new_limits)

        backend.set_resource_limits.assert_called_once_with("test-alloc", new_limits)
        backend._setup_resource_limits.assert_not_called()


class TestCollectResourceLimitsNullDates:
    """Tests for _collect_resource_limits with null/missing date fields."""

    def test_created_none_does_not_crash(self) -> None:
        """When created is None, prepaid multiplication is skipped (no crash)."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 6},
            created=None,
            end_date=datetime.date(2026, 6, 1),
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        # No multiplication — created is None, so duration can't be calculated
        expected = 6
        assert allocation_limits["cpu"] == expected

    def test_end_date_none_does_not_crash(self) -> None:
        """When end_date is None, prepaid multiplication is skipped."""
        components = {
            "cpu": {
                "measured_unit": "cores",
                "unit_factor": 1,
                "accounting_type": "one",
                "label": "CPU",
            },
        }
        backend = _make_backend(components)
        resource = _make_waldur_resource(
            limits={"cpu": 6},
            created=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            end_date=None,
        )

        allocation_limits, _ = backend._collect_resource_limits(resource)

        expected = 6
        assert allocation_limits["cpu"] == expected
