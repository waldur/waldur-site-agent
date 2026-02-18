"""Fixtures for end-to-end federation tests against two Waldur instances.

These tests exercise the full OfferingOrderProcessor pipeline with
non-blocking order creation:
  1. Test creates an order on Waldur A (source, Marketplace.Slurm offering)
  2. OfferingOrderProcessor.process_offering() picks it up from Waldur A
  3. WaldurBackend submits order on Waldur B (target, non-blocking)
  4. check_pending_order() tracks completion on subsequent cycles
  5. Order completes on B, processor marks it done on A

Environment variables:
    WALDUR_E2E_TESTS=true               - Gate: skip all if not set
    WALDUR_E2E_CONFIG=<path>            - Path to agent config YAML
                                          (same format as production config)
    WALDUR_E2E_PROJECT_A_UUID=<uuid>    - Project UUID on Waldur A
                                          (must have access to the source offering)
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
import urllib3.util

from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from plugins.waldur.tests.integration_helpers import (
    AutoApproveWaldurClient,
    WaldurTestSetup,
)
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent_waldur.backend import TERMINAL_ERROR_STATES, WaldurBackend

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

    def heading(self, level: int, text: str):
        self._sections.append(f"\n{'#' * level} {text}\n")

    def text(self, text: str):
        self._sections.append(text)

    def api_call(self, method: str, url: str, status_code: int):
        self._api_log.append({"method": method, "url": url, "status": status_code})

    def flush_api_log(self, heading: str = "API calls"):
        """Render accumulated API calls as a markdown table, then clear."""
        if not self._api_log:
            return
        self._sections.append(f"\n**{heading}:**\n")
        self._sections.append("| Method | URL | Status |")
        self._sections.append("|--------|-----|--------|")
        for e in self._api_log:
            url = e["url"]
            if "://" in url:
                url = "/" + url.split("/", 3)[-1]
            self._sections.append(f"| {e['method']} | `{url}` | {e['status']} |")
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
        with open(path, "w") as f:
            f.write("\n".join(self._sections))


def _make_response_hook(report: ReportWriter):
    """Create an httpx response event hook that logs to the report."""

    def hook(response):
        report.api_call(
            method=response.request.method,
            url=str(response.request.url),
            status_code=response.status_code,
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
# STOMP helpers
# ---------------------------------------------------------------------------


class MessageCapture:
    """Thread-safe STOMP message capture for E2E tests."""

    def __init__(self):
        self._messages: list[dict] = []
        self._lock = threading.Lock()
        self._waiters: dict[str, threading.Event] = {}

    def make_handler(self, delegate=None):
        """Return a STOMP handler that captures messages and optionally delegates.

        The returned function has the standard STOMP handler signature:
        ``(frame, offering, user_agent) -> None``
        """

        def handler(frame, offering, user_agent):
            message = json.loads(frame.body)
            with self._lock:
                self._messages.append(message)
                # Signal any waiters whose key:value matches this message
                for waiter_id, evt in list(self._waiters.items()):
                    if ":" in waiter_id:
                        k, v = waiter_id.split(":", 1)
                        if str(message.get(k, "")) == v:
                            evt.set()
            if delegate:
                delegate(frame, offering, user_agent)

        return handler

    def wait_for_order_event(self, order_uuid: str, timeout: float = 60) -> dict | None:
        """Wait for an ORDER event matching the given UUID. Returns message or None."""
        return self.wait_for_event("order_uuid", order_uuid, timeout)

    def wait_for_event(
        self, key: str, value: str, timeout: float = 60
    ) -> dict | None:
        """Wait for any event where message[key] == value. Returns message or None."""
        waiter_id = f"{key}:{value}"
        with self._lock:
            for msg in self._messages:
                if msg.get(key) == value:
                    return msg
            event = threading.Event()
            self._waiters[waiter_id] = event

        if event.wait(timeout=timeout):
            with self._lock:
                for msg in reversed(self._messages):
                    if msg.get(key) == value:
                        return msg
        return None

    def wait_for_any(self, timeout: float = 30) -> dict | None:
        """Wait for any message to arrive. Returns the latest or None on timeout."""
        initial_count = len(self.messages)
        deadline = __import__("time").time() + timeout
        while __import__("time").time() < deadline:
            msgs = self.messages
            if len(msgs) > initial_count:
                return msgs[-1]
            __import__("time").sleep(0.5)
        return None

    @property
    def messages(self) -> list[dict]:
        with self._lock:
            return list(self._messages)


def check_stomp_available(api_url: str) -> bool:
    """Check if /rmqws-stomp WebSocket endpoint is available.

    A plain HTTP GET to the WebSocket endpoint should return HTTP 426
    (Upgrade Required), confirming the endpoint exists.
    """
    import httpx as httpx_lib

    parsed = urllib3.util.parse_url(api_url)
    scheme = "https" if "https" in api_url else "http"
    try:
        resp = httpx_lib.get(
            f"{scheme}://{parsed.host}/rmqws-stomp",
            timeout=5,
            follow_redirects=False,
        )
        return resp.status_code == 426
    except Exception:
        return False


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
    """First offering from the config (the source offering on Waldur A)."""
    if not config.offerings:
        pytest.skip("No offerings in config")
    return config.offerings[0]


@pytest.fixture(scope="module")
def report(request, offering):
    """Module-scoped ReportWriter; writes markdown file at teardown."""
    rw = ReportWriter()
    rw.heading(1, "E2E Federation Test Report")
    rw.text(f"**Date:** {datetime.now(tz=timezone.utc).isoformat()}")
    rw.text(f"**Config:** `{E2E_CONFIG_PATH}`")
    rw.text(f"**Waldur A:** {offering.waldur_api_url}")
    settings = offering.backend_settings
    rw.text(f"**Waldur B:** {settings['target_api_url']}")
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
def waldur_client_a(offering, report):
    """AuthenticatedClient for Waldur A (source), with response logging."""
    client = get_client(offering.waldur_api_url, offering.waldur_api_token)
    httpx_client = client.get_httpx_client()
    httpx_client.event_hooks["response"].append(_make_response_hook(report))
    return client


@pytest.fixture(scope="module")
def waldur_client_b(offering, report):
    """AuthenticatedClient for Waldur B (target), with response logging."""
    settings = offering.backend_settings
    client = get_client(settings["target_api_url"], settings["target_api_token"])
    httpx_client = client.get_httpx_client()
    httpx_client.event_hooks["response"].append(_make_response_hook(report))
    return client


class AutoApproveWaldurBackend(WaldurBackend):
    """WaldurBackend that auto-approves pending orders on Waldur B.

    In production, Waldur B's own backend processor approves
    Marketplace.Basic orders. In E2E tests there is no such processor,
    so we approve them ourselves in check_pending_order().
    """

    def check_pending_order(self, order_backend_id: str) -> bool:
        """Check and auto-approve pending target orders on Waldur B."""
        order_uuid = UUID(order_backend_id)
        target_order = self.client.get_order(order_uuid)
        state = (
            target_order.state
            if not isinstance(target_order.state, type(UNSET))
            else None
        )

        if state == OrderState.DONE:
            logger.info("Target order %s completed", order_backend_id)
            return True

        if state in TERMINAL_ERROR_STATES:
            msg = f"Target order {order_backend_id} failed: {state}"
            raise BackendError(msg)

        if state == OrderState.PENDING_PROVIDER:
            logger.info("Auto-approving target order %s", order_backend_id)
            WaldurTestSetup.approve_order(self.client._api_client, order_uuid)

        return False


@pytest.fixture(scope="module")
def backend(offering):
    """WaldurBackend with auto-approve for Waldur B orders.

    In production, Waldur B has its own backend processor that approves
    Marketplace.Basic orders. In tests, we auto-approve them ourselves.
    """
    settings = offering.backend_settings
    components = offering.backend_components_dict
    backend = AutoApproveWaldurBackend(settings, components)
    backend.client = AutoApproveWaldurClient(
        api_url=settings["target_api_url"],
        api_token=settings["target_api_token"],
        offering_uuid=settings["target_offering_uuid"],
    )
    return backend


@pytest.fixture(scope="module")
def project_a_uuid():
    """UUID of the project on Waldur A to create orders in."""
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID
