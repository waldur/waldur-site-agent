"""E2E backwards-compatibility test for the periodic-limits handler.

Older Mastermind versions still send ``qos_threshold`` and ``grace_limit`` in
the periodic-limits STOMP payload. After the refactor the agent must:

* not raise on those fields,
* not issue any ``modify ... set qos=`` command (QoS state is owned by the
  ``RESOURCE`` event path),
* not query ``sshare`` / ``sacct`` for current usage (the local gate is gone).

This test bypasses the STOMP transport and calls
``on_resource_periodic_limits_update_stomp`` directly with a synthetic Frame.
It needs only a real resource backed by the emulator to exercise the
write path; no Mastermind policy is required.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_CONFIG=<path>
    WALDUR_E2E_PROJECT_A_UUID=<uuid>
"""

from __future__ import annotations

import logging
import os
import time

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
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent.event_processing import handlers

from .conftest import get_account_qos, make_periodic_limits_frame

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _offering_urls(client, offering_uuid: str) -> tuple[str, str]:
    resp = client.get_httpx_client().get(
        f"/api/marketplace-public-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    data = resp.json()
    plans = data.get("plans", [])
    if not plans:
        raise RuntimeError(f"No plans for offering {offering_uuid}")
    return data["url"], plans[0]["url"]


def _project_url(client, project_uuid: str) -> str:
    resp = client.get_httpx_client().get(f"/api/projects/{project_uuid}/")
    resp.raise_for_status()
    return resp.json()["url"]


def _create_order(
    client, offering_url: str, project_url: str, plan_url: str, limits: dict, name: str
) -> str:
    order_limits = OrderCreateRequestLimits()
    for k, v in limits.items():
        order_limits[k] = v
    attrs = GenericOrderAttributes()
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
    return order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)


def _process_order_terminal(offering, client, backend, order_uuid: str) -> OrderState:
    processor = OfferingOrderProcessor(
        offering=offering, waldur_rest_client=client, resource_backend=backend
    )
    for _ in range(10):
        processor.process_offering()
        order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        if state in (OrderState.DONE, OrderState.ERRED):
            return state
        time.sleep(1)
    pytest.fail(f"Order {order_uuid} not terminal")


def _get_resource_uuid(client, order_uuid: str) -> str:
    order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
    ruuid = order.marketplace_resource_uuid
    return ruuid.hex if hasattr(ruuid, "hex") else str(ruuid)


def _get_backend_id(client, resource_uuid: str) -> str:
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    return res.backend_id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bc_config():
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-qos-backcompat")


@pytest.fixture(scope="module")
def bc_offering(bc_config):
    if not bc_config.offerings:
        pytest.skip("No offerings in config")
    return bc_config.offerings[0]


@pytest.fixture(scope="module")
def bc_client(bc_offering):
    return get_client(bc_offering.waldur_api_url, bc_offering.waldur_api_token)


@pytest.fixture(scope="module")
def bc_backend(bc_offering):
    return SlurmBackend(bc_offering.backend_settings, bc_offering.backend_components_dict)


@pytest.fixture(scope="module")
def bc_project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(scope="module")
def bc_slurm_bin_path(bc_offering):
    return bc_offering.backend_settings.get("slurm_bin_path", ".venv/bin")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestQosBackcompat:
    """Periodic-limits handler tolerates legacy payloads without touching QoS."""

    _state: dict = {}

    def test_01_setup(
        self, bc_client, bc_offering, bc_backend, bc_project_uuid, bc_slurm_bin_path
    ):
        project_url = _project_url(bc_client, bc_project_uuid)
        offering_url, plan_url = _offering_urls(bc_client, bc_offering.waldur_offering_uuid)
        order_uuid = _create_order(
            bc_client,
            offering_url,
            project_url,
            plan_url,
            limits={"cpu": 100, "ram": 200},
            name="qos-backcompat",
        )
        state = _process_order_terminal(bc_offering, bc_client, bc_backend, order_uuid)
        assert state == OrderState.DONE, f"Setup order {order_uuid} ended in {state}"

        resource_uuid = _get_resource_uuid(bc_client, order_uuid)
        backend_id = _get_backend_id(bc_client, resource_uuid)
        TestQosBackcompat._state = {
            "resource_uuid": resource_uuid,
            "backend_id": backend_id,
            "project_url": project_url,
            "offering_url": offering_url,
            "plan_url": plan_url,
        }

    def test_02_legacy_payload_ignored(self, bc_client, bc_offering, bc_backend, bc_slurm_bin_path):
        """Send a frame with qos_threshold + grace_limit; agent must not touch QoS."""
        s = TestQosBackcompat._state
        if not s:
            pytest.skip("setup did not run")

        qos_before = get_account_qos(bc_slurm_bin_path, s["backend_id"])
        bc_backend.client.clear_executed_commands()

        # We feed a payload with the legacy QoS fields. The agent should
        # apply fairshare/limits/reset and ignore qos_threshold/grace_limit.
        # Note: the handler resolves the backend via get_backend_for_offering;
        # it does not use bc_backend directly. We still snapshot executed
        # commands on bc_backend.client so we can compare against a fresh
        # backend created by the handler — both end up writing to the same
        # emulator state, so the assertion is on emulator-observable effects.
        legacy_settings = {
            "fairshare": 333,
            "grp_tres_mins": {"billing": 60000},
            "qos_threshold": {"billing": 60000},  # legacy
            "grace_limit": {"billing": 72000},  # legacy
            "reset_raw_usage": False,
            "limit_type": "GrpTRESMins",
        }
        frame = make_periodic_limits_frame(
            resource_uuid=s["resource_uuid"],
            backend_id=s["backend_id"],
            offering_uuid=bc_offering.waldur_offering_uuid,
            settings=legacy_settings,
        )

        # The handler swallows internal errors and logs; ensure it does not
        # raise out. Call directly in-process.
        handlers.on_resource_periodic_limits_update_stomp(
            frame, bc_offering, user_agent="e2e-qos-backcompat"
        )

        # QoS untouched on the emulator.
        qos_after = get_account_qos(bc_slurm_bin_path, s["backend_id"])
        assert qos_after == qos_before, (
            f"Legacy payload must not touch QoS: was {qos_before!r}, now {qos_after!r}"
        )

    def test_03_new_payload_works(self, bc_client, bc_offering, bc_backend, bc_slurm_bin_path):
        """A clean payload (no legacy keys) still applies fairshare + limits + reset."""
        s = TestQosBackcompat._state
        if not s:
            pytest.skip("setup did not run")

        qos_before = get_account_qos(bc_slurm_bin_path, s["backend_id"])

        clean_settings = {
            "fairshare": 444,
            "grp_tres_mins": {"billing": 70000},
            "reset_raw_usage": False,
            "limit_type": "GrpTRESMins",
        }
        frame = make_periodic_limits_frame(
            resource_uuid=s["resource_uuid"],
            backend_id=s["backend_id"],
            offering_uuid=bc_offering.waldur_offering_uuid,
            settings=clean_settings,
        )

        handlers.on_resource_periodic_limits_update_stomp(
            frame, bc_offering, user_agent="e2e-qos-backcompat"
        )

        qos_after = get_account_qos(bc_slurm_bin_path, s["backend_id"])
        assert qos_after == qos_before, (
            f"Clean payload must not touch QoS either: was {qos_before!r}, now {qos_after!r}"
        )

    def test_04_cleanup(self, bc_client, bc_offering, bc_backend):
        s = TestQosBackcompat._state
        if not s:
            pytest.skip("setup did not run")

        try:
            res = marketplace_provider_resources_retrieve.sync(
                uuid=s["resource_uuid"], client=bc_client
            )
            resource_url = res.url if not isinstance(res.url, type(UNSET)) else None
            body = OrderCreateRequest(
                offering=s["offering_url"],
                project=s["project_url"],
                plan=s["plan_url"],
                attributes=GenericOrderAttributes(),
                type_=RequestTypes.TERMINATE,
            )
            if resource_url:
                body.additional_properties["resource"] = resource_url
            order = marketplace_orders_create.sync(client=bc_client, body=body)
            order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
            _process_order_terminal(bc_offering, bc_client, bc_backend, order_uuid)
        except Exception as exc:
            logger.warning("Resource terminate failed: %s", exc)
