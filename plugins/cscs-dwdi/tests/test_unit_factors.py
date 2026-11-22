"""Unit factors and component mapping of the CSCS-DWDI reporting backends.

Every backend reports ``DWDI amount / factor``, the direction the rest of the agent
uses; the expected values here are literals, not computed from the configuration.
"""

import logging
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_cscs_dwdi.backend import (
    CSCSDWDIComputeBackend,
    CSCSDWDIInferenceBackend,
    CSCSDWDIStorageBackend,
)

SETTINGS = {
    "cscs_dwdi_api_url": "https://api.example.com",
    "cscs_dwdi_client_id": "test_client",
    "cscs_dwdi_client_secret": "test_secret",
    "cscs_dwdi_oidc_token_url": "https://oidc.example.com/token",
    "storage_filesystem": "lustre",
    "storage_data_type": "projects",
}
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
BACKENDS = {
    "cscs-dwdi-compute": CSCSDWDIComputeBackend,
    "cscs-dwdi-storage": CSCSDWDIStorageBackend,
    "cscs-dwdi-inference": CSCSDWDIInferenceBackend,
}


def component(
    accounting_type: str = "usage",
    unit_factor: Any = 1,
    unit_factor_reporting: Any = None,
    dwdi_field: Optional[str] = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "measured_unit": "units",
        "unit_factor": unit_factor,
        "accounting_type": accounting_type,
        "label": "Component",
    }
    if unit_factor_reporting is not None:
        result["unit_factor_reporting"] = unit_factor_reporting
    if dwdi_field is not None:
        result["dwdi_field"] = dwdi_field
    return result


def compute_report(components: dict[str, dict[str, Any]]) -> dict[str, Any]:
    backend = CSCSDWDIComputeBackend(SETTINGS, components)
    return backend._process_api_response(
        {
            "compute": [
                {
                    "account": "acc",
                    "totalNodeHours": 100.0,
                    "users": [
                        {"username": "alice", "nodeHours": 60.0},
                        {"username": "bob", "nodeHours": 40.0},
                    ],
                }
            ]
        }
    )["acc"]


def storage_report(components: dict[str, dict[str, Any]]) -> dict[str, float]:
    backend = CSCSDWDIStorageBackend(SETTINGS, components)
    with patch.object(backend.cscs_client, "get_storage_usage_for_month") as mock_get:
        mock_get.return_value = {
            "storage": [
                {"path": "/store/p1", "spaceUsed": 5_000_000_000, "inodesUsed": 1500}
            ]
        }
        report = backend.get_usage_report_for_period(["/store/p1"], 2024, 6)
    return report["/store/p1"]["TOTAL_ACCOUNT_USAGE"]


def inference_report(components: dict[str, dict[str, Any]]) -> dict[str, float]:
    backend = CSCSDWDIInferenceBackend(SETTINGS, components)
    with patch.object(backend.cscs_client, "get_inference_cost_for_month") as mock_get:
        mock_get.return_value = {
            "inference": [{"waldurResourceUuid": "r1", "totalCost": 12.5}]
        }
        report = backend._get_usage_report(["r1"])
    return report["r1"]["TOTAL_ACCOUNT_USAGE"]


class TestFactorDirection:
    def test_compute_divides_by_unit_factor(self) -> None:
        report = compute_report({"nodeHours": component(unit_factor=4)})
        assert report["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 25.0}
        assert report["alice"] == {"nodeHours": 15.0}
        assert report["bob"] == {"nodeHours": 10.0}

    def test_compute_reporting_factor_overrides_unit_factor(self) -> None:
        # 4-GPU nodes reported as GPU-hours: node-hours / 0.25.
        report = compute_report(
            {"gpu_node_hours": component(unit_factor=1, unit_factor_reporting=0.25)}
        )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"gpu_node_hours": 400.0}
        assert report["alice"] == {"gpu_node_hours": 240.0}

    def test_explicit_null_reporting_factor_falls_back_to_unit_factor(self) -> None:
        components = {"nodeHours": component(unit_factor=2)}
        components["nodeHours"]["unit_factor_reporting"] = None
        report = compute_report(components)
        assert report["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 50.0}

    def test_storage_divides_bytes_into_gigabytes(self) -> None:
        report = storage_report(
            {
                "storage_space": component(unit_factor=1_000_000_000),
                "storage_inodes": component(unit_factor=1),
            }
        )
        assert report == {"storage_space": 5.0, "storage_inodes": 1500.0}

    def test_inference_divides_cost(self) -> None:
        assert inference_report({"token_cost": component(unit_factor=1)}) == {
            "token_cost": 12.5
        }
        assert inference_report({"token_cost": component(unit_factor_reporting=0.5)}) == {
            "token_cost": 25.0
        }


class TestInvalidFactor:
    @pytest.mark.parametrize("factor", [0, -2, float("inf"), "x"])
    def test_component_with_unusable_factor_is_not_reported(
        self, factor: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.ERROR):
            report = compute_report(
                {
                    "nodeHours": component(unit_factor=factor),
                    "gpu_node_hours": component(unit_factor=2),
                }
            )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"gpu_node_hours": 50.0}
        assert "nodeHours" in caplog.text
        assert "positive number" in caplog.text

    def test_zero_reporting_factor_is_refused_even_with_valid_unit_factor(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.ERROR):
            report = storage_report(
                {"storage_space": component(unit_factor=1, unit_factor_reporting=0)}
            )
        assert report == {}
        assert "storage_space" in caplog.text


class TestComponentMapping:
    def test_unfilled_usage_component_is_logged_as_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.ERROR):
            report = compute_report(
                {"nodeHours": component(), "cpuHours": component(accounting_type="usage")}
            )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 100.0}
        assert "cpuHours" in caplog.text
        assert "will not be reported" in caplog.text

    def test_limit_components_of_another_backend_are_skipped_quietly(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # A SLURM offering reporting through DWDI passes its limit components too.
        with caplog.at_level(logging.ERROR):
            report = compute_report(
                {
                    "cpu": component(accounting_type="limit", unit_factor=60000),
                    "mem": component(accounting_type="limit", unit_factor=61440),
                    "node": component(accounting_type="usage", unit_factor=1),
                }
            )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"node": 100.0}
        assert caplog.text == ""

    def test_dwdi_field_fills_a_component_of_any_name(self) -> None:
        report = compute_report({"gpu": component(dwdi_field="nodeHours", unit_factor=1)})
        assert report["TOTAL_ACCOUNT_USAGE"] == {"gpu": 100.0}
        assert report["alice"] == {"gpu": 60.0}

    def test_unknown_dwdi_field_is_logged_and_not_reported(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.ERROR):
            report = compute_report(
                {"nodeHours": component(), "cpu": component(dwdi_field="cpuHours")}
            )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 100.0}
        assert "cpuHours" in caplog.text

    def test_component_names_match_case_sensitively(self) -> None:
        # As before: a "Nodes" limit or a "gpuNodeHours" component is not filled
        # with node hours, so it cannot be billed them a second time.
        report = compute_report(
            {
                "nodeHours": component(),
                "gpuNodeHours": component(accounting_type="limit"),
                "Nodes": component(accounting_type="limit"),
            }
        )
        assert report["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 100.0}


class TestMissingAmounts:
    """An amount DWDI leaves out raises rather than reading as 0 or as "no usage".

    Either would let the caller lower recorded usage: the historical loader with
    --reconcile-stale zeroes a period whose report has no amount.
    """

    def test_missing_account_total_raises(self) -> None:
        backend = CSCSDWDIComputeBackend(SETTINGS, {"nodeHours": component()})
        with pytest.raises(BackendError, match="totalNodeHours"):
            backend._process_api_response(
                {"compute": [{"account": "acc", "users": [{"username": "x", "nodeHours": 5.0}]}]}
            )

    def test_missing_account_total_fails_the_historical_period(self) -> None:
        backend = CSCSDWDIComputeBackend(SETTINGS, {"nodeHours": component()})
        with patch.object(backend.cscs_client, "get_usage_for_month") as mock_get:
            mock_get.return_value = {"compute": [{"account": "acc", "users": []}]}
            with pytest.raises(BackendError):
                backend.get_usage_report_for_period(["acc"], 2026, 10)

    def test_missing_user_amount_skips_only_that_user(self) -> None:
        backend = CSCSDWDIComputeBackend(SETTINGS, {"nodeHours": component()})
        report = backend._process_api_response(
            {
                "compute": [
                    {
                        "account": "acc",
                        "totalNodeHours": 5.0,
                        "users": [{"username": "x"}, {"username": "y", "nodeHours": 5.0}],
                    }
                ]
            }
        )
        assert report["acc"]["TOTAL_ACCOUNT_USAGE"] == {"nodeHours": 5.0}
        assert report["acc"]["x"] == {}
        assert report["acc"]["y"] == {"nodeHours": 5.0}

    @pytest.mark.parametrize("entry", [{"path": "/store/p1"}, {"path": "/store/p1", "spaceUsed": None}])
    def test_missing_storage_amount_raises(self, entry: dict[str, Any]) -> None:
        backend = CSCSDWDIStorageBackend(SETTINGS, {"storage_space": component()})
        with patch.object(backend.cscs_client, "get_storage_usage_for_month") as mock_get:
            mock_get.return_value = {"storage": [entry]}
            with pytest.raises(BackendError, match="spaceUsed"):
                backend.get_usage_report_for_period(["/store/p1"], 2024, 6)

    def test_amount_of_an_unmapped_field_may_be_missing(self) -> None:
        # Only space is configured, so DWDI leaving out inodes changes nothing.
        backend = CSCSDWDIStorageBackend(SETTINGS, {"storage_space": component()})
        with patch.object(backend.cscs_client, "get_storage_usage_for_month") as mock_get:
            mock_get.return_value = {"storage": [{"path": "/store/p1", "spaceUsed": 7}]}
            report = backend.get_usage_report_for_period(["/store/p1"], 2024, 6)
        assert report["/store/p1"]["TOTAL_ACCOUNT_USAGE"] == {"storage_space": 7.0}

    def test_null_inference_cost_raises(self) -> None:
        backend = CSCSDWDIInferenceBackend(SETTINGS, {"token_cost": component()})
        with patch.object(backend.cscs_client, "get_inference_cost_for_month") as mock_get:
            mock_get.return_value = {
                "inference": [{"waldurResourceUuid": "r1", "totalCost": None}]
            }
            with pytest.raises(BackendError, match="totalCost"):
                backend._get_usage_report(["r1"])


class TestNoDataForResource:
    """With no DWDI row, only the components this backend fills are reported as 0."""

    COMPONENTS = {
        "nodeHours": component(),
        "cpuHours": component(),  # usage, nothing fills it
        "bad_node": component(unit_factor=0),
        "cpu": component(accounting_type="limit", unit_factor=60000),
    }

    def test_compute(self) -> None:
        backend = CSCSDWDIComputeBackend(SETTINGS, self.COMPONENTS)
        with patch.object(backend.cscs_client, "get_usage_for_month") as mock_get:
            mock_get.return_value = {"compute": []}
            info = backend._pull_backend_resource(
                "acc", MagicMock(offering_backend_id="alps")
            )
        assert info.usage == {"TOTAL_ACCOUNT_USAGE": {"nodeHours": 0.0}}

    def test_inference(self) -> None:
        backend = CSCSDWDIInferenceBackend(
            SETTINGS, {"token_cost": component(), "other": component()}
        )
        with patch.object(backend.cscs_client, "get_inference_cost_for_month") as mock_get:
            mock_get.return_value = {"inference": []}
            info = backend._pull_backend_resource("r1")
        assert info.usage == {"TOTAL_ACCOUNT_USAGE": {"token_cost": 0.0}}


def example_offerings() -> list[dict[str, Any]]:
    offerings = []
    for path in sorted(EXAMPLES.glob("*.yaml")):
        offerings.extend(yaml.safe_load(path.read_text())["offerings"])
    return offerings


@pytest.mark.parametrize(
    "offering", example_offerings(), ids=lambda offering: offering["name"]
)
def test_every_example_component_is_filled(offering: dict[str, Any]) -> None:
    backend_class = BACKENDS[offering["reporting_backend"]]
    backend = backend_class(
        {**SETTINGS, **offering["backend_settings"]}, offering["backend_components"]
    )
    assert set(backend.component_fields) == set(offering["backend_components"])
