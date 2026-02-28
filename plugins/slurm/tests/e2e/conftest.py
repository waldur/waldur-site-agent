"""Fixtures for E2E API optimization tests using SLURM emulator backend.

These tests exercise the core OfferingOrderProcessor / OfferingMembershipProcessor /
OfferingReportProcessor pipelines against a real Waldur instance with a SLURM
emulator backend (synchronous, no remote Waldur B needed).

The SLURM emulator provides CLI entry points (sacctmgr, sacct, scancel)
that replace real SLURM commands.  State is persisted in
``/tmp/slurm_emulator_db.json``.

Environment variables:
    WALDUR_E2E_TESTS=true               - Gate: skip all if not set
    WALDUR_E2E_CONFIG=<path>            - Path to agent config YAML
    WALDUR_E2E_PROJECT_A_UUID=<uuid>    - Project UUID on Waldur A
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_create,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import (
    OrderCreateRequestLimits,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent_slurm.backend import SlurmBackend

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------


class ReportWriter:
    """Accumulates markdown sections for the E2E test report."""

    def __init__(self):
        self._sections: list[str] = []
        self._api_log: list[dict] = []
        self.total_calls: int = 0
        self.total_response_bytes: int = 0
        self._test_calls: dict[str, int] = {}
        self._test_bytes: dict[str, int] = {}

    def heading(self, level: int, text: str):
        self._sections.append(f"\n{'#' * level} {text}\n")

    def text(self, text: str):
        self._sections.append(text)

    def api_call(self, method: str, url: str, status_code: int, response_bytes: int = 0):
        self._api_log.append({
            "method": method, "url": url, "status": status_code, "bytes": response_bytes,
        })
        self.total_calls += 1
        self.total_response_bytes += response_bytes

    def record_test_calls(self, test_id: str, count: int, response_bytes: int = 0):
        self._test_calls[test_id] = count
        self._test_bytes[test_id] = response_bytes

    def flush_api_log(self, heading: str = "API calls"):
        """Render accumulated API calls as a markdown table, then clear."""
        if not self._api_log:
            return
        self._sections.append(f"\n**{heading}:**\n")
        self._sections.append("| Method | URL | Status | Bytes |")
        self._sections.append("|--------|-----|--------|-------|")
        for e in self._api_log:
            url = e["url"]
            if "://" in url:
                url = "/" + url.split("/", 3)[-1]
            self._sections.append(
                f"| {e['method']} | `{url}` | {e['status']} | {_fmt_bytes(e['bytes'])} |"
            )
        self._sections.append("")
        self._api_log.clear()

    def status_snapshot(self, label: str, items: dict):
        """Render a key-value snapshot as a markdown table."""
        self._sections.append(f"\n**{label}:**\n")
        self._sections.append("| Field | Value |")
        self._sections.append("|-------|-------|")
        for k, v in items.items():
            self._sections.append(f"| {k} | `{v}` |")
        self._sections.append("")

    def write(self, path: str | Path):
        path = Path(path)
        # Append summary table
        self._sections.append("\n## API Call Summary\n")
        self._sections.append("| Test | API Calls | Response Size |")
        self._sections.append("|------|-----------|---------------|")
        for test_id in self._test_calls:
            count = self._test_calls[test_id]
            size = _fmt_bytes(self._test_bytes.get(test_id, 0))
            self._sections.append(f"| {test_id} | {count} | {size} |")
        self._sections.append(
            f"| **TOTAL** | **{self.total_calls}** "
            f"| **{_fmt_bytes(self.total_response_bytes)}** |"
        )
        self._sections.append("")

        with open(path, "w") as f:
            f.write("\n".join(self._sections))

        # Write machine-readable JSON counts
        json_path = path.with_suffix(".json")
        data = {
            "date": datetime.now(tz=timezone.utc).isoformat(),
            "total_calls": self.total_calls,
            "total_response_bytes": self.total_response_bytes,
            "tests": {
                tid: {
                    "calls": self._test_calls[tid],
                    "response_bytes": self._test_bytes.get(tid, 0),
                }
                for tid in self._test_calls
            },
        }
        with open(json_path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("API call counts written to %s", json_path)


def _fmt_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 / 1024:.1f} MB"


def _make_response_hook(report: ReportWriter):
    """Create an httpx response event hook that logs to the report."""

    def hook(response):
        response.read()
        body_size = len(response.content)
        report.api_call(
            method=response.request.method,
            url=str(response.request.url),
            status_code=response.status_code,
            response_bytes=body_size,
        )

    return hook


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------


def snapshot_order(report: ReportWriter, client, order_uuid: str, label: str):
    """Fetch order and write status snapshot to the report."""
    order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
    state = order.state if not isinstance(order.state, type(UNSET)) else "UNSET"
    type_ = order.type_ if not isinstance(order.type_, type(UNSET)) else "UNSET"
    resource_uuid = "UNSET"
    if not isinstance(order.marketplace_resource_uuid, type(UNSET)):
        resource_uuid = (
            order.marketplace_resource_uuid.hex
            if hasattr(order.marketplace_resource_uuid, "hex")
            else str(order.marketplace_resource_uuid)
        )
    report.status_snapshot(
        label,
        {
            "uuid": order_uuid,
            "state": state,
            "type": type_,
            "backend_id": order.backend_id or "(empty)",
            "resource_uuid": resource_uuid,
            "error_message": order.error_message or "(none)",
        },
    )
    return order


def snapshot_resource(report: ReportWriter, client, resource_uuid: str, label: str):
    """Fetch resource and write status snapshot to the report."""
    res = marketplace_provider_resources_retrieve.sync(
        uuid=resource_uuid, client=client
    )
    limits_str = (
        str(dict(res.limits.additional_properties))
        if not isinstance(res.limits, type(UNSET))
        else "{}"
    )
    report.status_snapshot(
        label,
        {
            "uuid": resource_uuid,
            "name": res.name or "(empty)",
            "state": str(res.state),
            "backend_id": res.backend_id or "(empty)",
            "limits": limits_str,
            "offering": res.offering_name or "?",
            "project": res.project_name or "?",
        },
    )
    return res


# ---------------------------------------------------------------------------
# Order helpers
# ---------------------------------------------------------------------------


def get_offering_info(client, offering_uuid: str) -> tuple[str, str]:
    """Get the public offering URL and first plan URL for order creation.

    Returns:
        (offering_url, plan_url)
    """
    resp = client.get_httpx_client().get(
        f"/api/marketplace-public-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    data = resp.json()
    offering_url = data["url"]
    plans = data.get("plans", [])
    if not plans:
        msg = f"No plans found for offering {offering_uuid}"
        raise RuntimeError(msg)
    plan_url = plans[0]["url"]
    return offering_url, plan_url


def get_project_url(client, project_uuid: str) -> str:
    """Get the full URL for a project."""
    from waldur_api_client.api.projects import projects_retrieve

    proj = projects_retrieve.sync(client=client, uuid=project_uuid)
    return proj.url


def create_source_order(
    client,
    offering_url: str,
    project_url: str,
    plan_url: str,
    limits: dict[str, int],
    name: str = "",
) -> str:
    """Create a CREATE order on Waldur, return order UUID hex."""
    order_limits = OrderCreateRequestLimits()
    for key, value in limits.items():
        order_limits[key] = value

    attrs = GenericOrderAttributes()
    if name:
        attrs["name"] = name

    body = OrderCreateRequest(
        offering=offering_url,
        project=project_url,
        plan=plan_url,
        limits=order_limits,
        attributes=attrs,
        type_=RequestTypes.CREATE,
    )

    order = marketplace_orders_create.sync(client=client, body=body)
    order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
    logger.info("Created source order %s", order_uuid)
    return order_uuid


def run_processor_until_order_terminal(
    offering,
    waldur_client,
    backend,
    order_uuid: str,
    max_cycles: int = 10,
    cycle_delay: int = 2,
    report=None,
) -> OrderState:
    """Run process_offering() in a loop until the order reaches a terminal state.

    Returns the final OrderState (DONE or ERRED).
    With SLURM emulator, orders complete synchronously (1 cycle).
    """
    processor = OfferingOrderProcessor(
        offering=offering,
        waldur_rest_client=waldur_client,
        resource_backend=backend,
    )

    for cycle in range(max_cycles):
        logger.info("--- Processor cycle %d ---", cycle + 1)
        if report:
            report.heading(4, f"Processor cycle {cycle + 1}")

        processor.process_offering()

        if report:
            report.flush_api_log(f"Cycle {cycle + 1} API calls")
            snapshot_order(
                report,
                waldur_client,
                order_uuid,
                f"Order state after cycle {cycle + 1}",
            )
            report.flush_api_log()

        order = marketplace_orders_retrieve.sync(client=waldur_client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        logger.info("Order %s state: %s (cycle %d)", order_uuid, state, cycle + 1)

        if state == OrderState.DONE:
            return OrderState.DONE
        if state == OrderState.ERRED:
            error_msg = getattr(order, "error_message", "unknown")
            logger.warning(
                "Order %s ERRED: %s (may be server-side set_state_done bug)",
                order_uuid,
                error_msg,
            )
            return OrderState.ERRED

        time.sleep(cycle_delay)

    pytest.fail(f"Order {order_uuid} not terminal after {max_cycles} cycles")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config():
    """Load agent configuration from YAML file."""
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-test")


@pytest.fixture(scope="module")
def offering(config):
    """First offering from the config."""
    if not config.offerings:
        pytest.skip("No offerings in config")
    return config.offerings[0]


@pytest.fixture(scope="module")
def report(request, offering):
    """Module-scoped ReportWriter; writes markdown file at teardown."""
    rw = ReportWriter()
    rw.heading(1, "E2E API Optimization Test Report")
    rw.text(f"**Date:** {datetime.now(tz=timezone.utc).isoformat()}")
    rw.text(f"**Config:** `{E2E_CONFIG_PATH}`")
    rw.text(f"**Waldur:** {offering.waldur_api_url}")
    rw.text(f"**Backend:** SLURM emulator")
    rw.text(f"**Offering:** {offering.waldur_offering_uuid}")
    components_str = ", ".join(
        f"{k} (factor={getattr(v, 'unit_factor', 1)})"
        for k, v in offering.backend_components.items()
    )
    rw.text(f"**Components:** {components_str}")
    rw.text("")

    def finalizer():
        stem = Path(E2E_CONFIG_PATH).stem if E2E_CONFIG_PATH else "e2e"
        path = Path(__file__).parent / f"{stem}-report.md"
        rw.write(path)
        logger.info("E2E report written to %s", path)

    request.addfinalizer(finalizer)
    return rw


@pytest.fixture(scope="module")
def waldur_client(offering, report):
    """AuthenticatedClient for Waldur, with response logging."""
    client = get_client(offering.waldur_api_url, offering.waldur_api_token)
    httpx_client = client.get_httpx_client()
    httpx_client.event_hooks["response"].append(_make_response_hook(report))
    return client


@pytest.fixture(scope="module")
def _emulator_cleanup(offering):
    """Reset slurm-emulator state before the test module runs."""
    slurm_bin_path = offering.backend_settings.get("slurm_bin_path", ".venv/bin")
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")

    try:
        subprocess.check_output(
            [sacctmgr, "cleanup", "all"],
            stderr=subprocess.STDOUT,
            timeout=10,
        )
        logger.info("Emulator state reset via 'sacctmgr cleanup all'")
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning("sacctmgr cleanup failed (%s), deleting state file", exc)
        state_file = Path("/tmp/slurm_emulator_db.json")
        if state_file.exists():
            state_file.unlink()
            logger.info("Deleted emulator state file %s", state_file)


@pytest.fixture(scope="module")
def slurm_backend(offering, _emulator_cleanup):
    """SlurmBackend using the slurm-emulator CLI entry points.

    Orders complete synchronously (supports_async_orders=False).
    """
    settings = offering.backend_settings
    components = offering.backend_components_dict
    return SlurmBackend(settings, components)


@pytest.fixture(scope="module")
def project_uuid():
    """UUID of the project on Waldur to create orders in."""
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(autouse=True)
def _track_api_calls(request, report):
    """Track API call count and response bytes per test automatically."""
    start_calls = report.total_calls
    start_bytes = report.total_response_bytes
    yield
    count = report.total_calls - start_calls
    resp_bytes = report.total_response_bytes - start_bytes
    test_id = request.node.nodeid.split("::")[-1]
    class_name = request.node.cls.__name__ if request.node.cls else ""
    label = f"{class_name}::{test_id}" if class_name else test_id
    report.record_test_calls(label, count, resp_bytes)
