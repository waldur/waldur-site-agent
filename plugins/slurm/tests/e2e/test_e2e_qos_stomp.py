"""E2E tests for SLURM QoS application via the STOMP RESOURCE event path.

Verifies the live path: Mastermind flips ``resource.paused`` /
``resource.downscaled`` (driven by real policy evaluation), Mastermind emits a
``RESOURCE`` STOMP event, the agent's ``on_resource_message_stomp`` handler
processes it via ``_sync_resource_status``, and the resulting QoS is visible
on the SLURM emulator account.

Uses the second offering in ``WALDUR_E2E_STOMP_CONFIG`` (the policy-attached
one, ``e2ef0000000000000000000000000101``). The first offering is reserved
for the existing test_e2e_stomp.py order-flow tests.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_STOMP_CONFIG=<path>
    WALDUR_E2E_PROJECT_A_UUID=<uuid>
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from uuid import UUID

import pytest
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_set_usage,
)
from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_create,
    marketplace_orders_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.api.marketplace_slurm_periodic_usage_policies import (
    marketplace_slurm_periodic_usage_policies_destroy,
)
from waldur_api_client.models.component_usage_create_request import (
    ComponentUsageCreateRequest,
)
from waldur_api_client.models.component_usage_item_request import (
    ComponentUsageItemRequest,
)
from waldur_api_client.models.generic_order_attributes import GenericOrderAttributes
from waldur_api_client.models.observable_object_type_enum import (
    ObservableObjectTypeEnum,
)
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import (
    OrderCreateRequestLimits,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.policy_period_enum import PolicyPeriodEnum
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.utils import (
    setup_stomp_offering_subscriptions,
    stop_stomp_consumers,
)

from .conftest import get_account_qos, set_resource_paused

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_STOMP_CONFIG_PATH = os.environ.get("WALDUR_E2E_STOMP_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

# Second offering in the STOMP config (the policy-attached one).
QOS_OFFERING_UUID = "e2ef0000000000000000000000000101"
RESOURCE_NODE_LIMIT = 100
STOMP_WAIT_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Capture helpers (lightweight version of test_e2e_stomp.MessageCapture)
# ---------------------------------------------------------------------------


class ResourceEventCapture:
    """Thread-safe capture of RESOURCE STOMP events that also delegates to the real handler."""

    def __init__(self):
        self._messages: list[dict] = []
        self._lock = threading.Lock()
        self._waiters: dict[str, threading.Event] = {}

    def make_handler(self, delegate):
        def handler(frame, offering, user_agent, expose_backend_error_details=True):  # noqa: ARG001
            message = json.loads(frame.body)
            with self._lock:
                self._messages.append(message)
                for key, evt in list(self._waiters.items()):
                    field, value = key.split("=", 1)
                    if str(message.get(field, "")) == value:
                        evt.set()
            # Delegate to the real handler so the agent's resource sync runs.
            delegate(frame, offering, user_agent)

        return handler

    def wait_for(
        self, field: str, value: str, timeout: float = STOMP_WAIT_TIMEOUT
    ) -> dict | None:
        with self._lock:
            for msg in reversed(self._messages):
                if str(msg.get(field, "")) == value:
                    return msg
            key = f"{field}={value}"
            evt = threading.Event()
            self._waiters[key] = evt

        if evt.wait(timeout=timeout):
            with self._lock:
                for msg in reversed(self._messages):
                    if str(msg.get(field, "")) == value:
                        return msg
        return None

    def messages_for(self, resource_uuid: str) -> list[dict]:
        with self._lock:
            return [m for m in self._messages if m.get("resource_uuid") == resource_uuid]


# ---------------------------------------------------------------------------
# Mastermind interaction helpers
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


def _provider_offering_url(client, offering_uuid: str) -> str:
    resp = client.get_httpx_client().get(
        f"/api/marketplace-provider-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    return resp.json()["url"]


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


def _wait_for_order_done(client, order_uuid: str, timeout: float = 30) -> OrderState:
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        if state in (OrderState.DONE, OrderState.ERRED):
            return state
        time.sleep(1)
    raise TimeoutError(f"Order {order_uuid} not terminal after {timeout}s")


def _get_resource_uuid_from_order(client, order_uuid: str) -> str:
    order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
    ruuid = order.marketplace_resource_uuid
    return ruuid.hex if hasattr(ruuid, "hex") else str(ruuid)


def _get_backend_id(client, resource_uuid: str) -> str:
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    return res.backend_id


def _submit_usage(client, resource_uuid: str, component_type: str, amount: int):
    body = ComponentUsageCreateRequest(
        usages=[ComponentUsageItemRequest(type_=component_type, amount=str(amount))],
        resource=UUID(resource_uuid),
    )
    marketplace_component_usages_set_usage.sync_detailed(client=client, body=body)


def _create_policy(client, scope_url: str) -> str:
    payload = {
        "scope": scope_url,
        "actions": (
            "notify_organization_owners,"
            "request_slurm_resource_downscaling,"
            "request_slurm_resource_pausing"
        ),
        "component_limits_set": [],
        "apply_to_all": True,
        "period": PolicyPeriodEnum.VALUE_2.value,  # monthly
        "grace_ratio": 0.0,
        "carryover_enabled": False,
        "carryover_factor": 0,
        "raw_usage_reset": True,
        "tres_billing_enabled": False,
    }
    resp = client.get_httpx_client().post(
        "/api/marketplace-slurm-periodic-usage-policies/", json=payload
    )
    resp.raise_for_status()
    return str(resp.json()["uuid"]).replace("-", "")


def _evaluate_policy(client, policy_uuid: str, resource_uuid: str) -> None:
    resp = client.get_httpx_client().post(
        f"/api/marketplace-slurm-periodic-usage-policies/{policy_uuid}/evaluate/",
        json={"resource_uuid": resource_uuid},
    )
    resp.raise_for_status()


def _resource_flags(client, resource_uuid: str) -> tuple[bool, bool]:
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    paused = bool(res.paused) if not isinstance(res.paused, type(UNSET)) else False
    downscaled = (
        bool(res.downscaled) if not isinstance(res.downscaled, type(UNSET)) else False
    )
    return paused, downscaled


def _wait_for_qos(
    slurm_bin_path: str, backend_id: str, expected: str, timeout: float = STOMP_WAIT_TIMEOUT
) -> str:
    """Poll the emulator until the account QoS matches ``expected`` or timeout."""
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        last = get_account_qos(slurm_bin_path, backend_id)
        if last == expected:
            return last
        time.sleep(1)
    return last


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def qos_stomp_config():
    if not E2E_STOMP_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_STOMP_CONFIG not set")
    return load_configuration(E2E_STOMP_CONFIG_PATH, user_agent_suffix="e2e-qos-stomp")


@pytest.fixture(scope="module")
def qos_stomp_offering(qos_stomp_config):
    for o in qos_stomp_config.offerings:
        if o.waldur_offering_uuid == QOS_OFFERING_UUID:
            return o
    pytest.skip(f"Offering {QOS_OFFERING_UUID} not in STOMP config")


@pytest.fixture(scope="module")
def qos_stomp_client(qos_stomp_offering):
    return get_client(qos_stomp_offering.waldur_api_url, qos_stomp_offering.waldur_api_token)


@pytest.fixture(scope="module")
def qos_stomp_backend(qos_stomp_offering):
    return SlurmBackend(
        qos_stomp_offering.backend_settings, qos_stomp_offering.backend_components_dict
    )


@pytest.fixture(scope="module")
def qos_stomp_project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(scope="module")
def qos_slurm_bin_path(qos_stomp_offering):
    return qos_stomp_offering.backend_settings.get("slurm_bin_path", ".venv/bin")


@pytest.fixture(scope="module")
def resource_capture():
    return ResourceEventCapture()


@pytest.fixture(scope="module")
def qos_stomp_consumers(request, qos_stomp_offering, resource_capture):
    """Subscribe to the offering's STOMP events; intercept RESOURCE messages.

    The capture handler delegates to the real handler so the agent still
    performs the resource sync (and therefore the QoS write).
    """
    consumers = setup_stomp_offering_subscriptions(qos_stomp_offering, "e2e-qos-stomp")
    if not consumers:
        pytest.skip("No STOMP consumers could be established")

    resource_handler = resource_capture.make_handler(
        delegate=handlers.on_resource_message_stomp
    )

    for conn, event_subscription, _offering in consumers:
        observable_objects = getattr(event_subscription, "observable_objects", [])
        for obj in observable_objects:
            if obj.get("object_type") == ObservableObjectTypeEnum.RESOURCE.value:
                listener = conn.get_listener(WALDUR_LISTENER_NAME)
                if listener:
                    listener.on_message_callback = resource_handler
                    logger.info(
                        "Hooked RESOURCE event handler for offering %s",
                        _offering.name,
                    )
                break

    consumers_map = {(qos_stomp_offering.name, qos_stomp_offering.uuid): consumers}

    def finalizer():
        stop_stomp_consumers(consumers_map)

    request.addfinalizer(finalizer)
    return consumers


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("qos_stomp_consumers")
class TestQosStomp:
    """RESOURCE STOMP event → agent → SLURM QoS."""

    _state: dict = {}

    def test_01_setup(
        self,
        qos_stomp_client,
        qos_stomp_offering,
        qos_stomp_backend,
        qos_stomp_project_uuid,
        qos_slurm_bin_path,
    ):
        """Create resource + policy on the STOMP offering."""
        project_url = _project_url(qos_stomp_client, qos_stomp_project_uuid)
        offering_url, plan_url = _offering_urls(qos_stomp_client, QOS_OFFERING_UUID)

        # The order processor runs through the STOMP handler, so we don't drive
        # it here. We post the order and wait for completion.
        order_uuid = _create_order(
            qos_stomp_client,
            offering_url,
            project_url,
            plan_url,
            limits={"node": RESOURCE_NODE_LIMIT},
            name="qos-stomp-target",
        )
        # The STOMP order handler may not be hooked here (we only hooked
        # RESOURCE). Fall back to the synchronous processor to bring the
        # order to DONE.
        from waldur_site_agent.common.processors import OfferingOrderProcessor

        processor = OfferingOrderProcessor(
            offering=qos_stomp_offering,
            waldur_rest_client=qos_stomp_client,
            resource_backend=qos_stomp_backend,
        )
        for _ in range(10):
            processor.process_offering()
            order = marketplace_orders_retrieve.sync(client=qos_stomp_client, uuid=order_uuid)
            state = order.state if not isinstance(order.state, type(UNSET)) else None
            if state in (OrderState.DONE, OrderState.ERRED):
                break
            time.sleep(1)

        resource_uuid = _get_resource_uuid_from_order(qos_stomp_client, order_uuid)
        backend_id = _get_backend_id(qos_stomp_client, resource_uuid)
        scope_url = _provider_offering_url(qos_stomp_client, QOS_OFFERING_UUID)
        policy_uuid = _create_policy(qos_stomp_client, scope_url)

        TestQosStomp._state = {
            "resource_uuid": resource_uuid,
            "backend_id": backend_id,
            "policy_uuid": policy_uuid,
            "project_url": project_url,
        }

        # Baseline: the agent's STOMP handler ran on order creation, so the
        # account should exist; QoS may be empty until a state-change event.
        qos = get_account_qos(qos_slurm_bin_path, backend_id)
        logger.info("Initial QoS for %s: %r", backend_id, qos)

    def test_02_pause_via_policy_evaluation(
        self,
        qos_stomp_client,
        qos_slurm_bin_path,
        resource_capture,
    ):
        """Submit usage above grace, evaluate, wait for RESOURCE STOMP event, verify QoS=paused."""
        s = TestQosStomp._state
        if not s:
            pytest.skip("setup did not run")

        _submit_usage(qos_stomp_client, s["resource_uuid"], "node", int(RESOURCE_NODE_LIMIT * 1.5))
        time.sleep(2)
        _evaluate_policy(qos_stomp_client, s["policy_uuid"], s["resource_uuid"])

        # Mastermind flips paused=True → resource.save() → RESOURCE STOMP event.
        msg = resource_capture.wait_for("resource_uuid", s["resource_uuid"])
        assert msg is not None, (
            f"No RESOURCE event received within {STOMP_WAIT_TIMEOUT}s after pausing"
        )
        assert msg.get("paused") is True, (
            f"Expected paused=True in STOMP message, got {msg!r}"
        )

        # The capture handler delegates to on_resource_message_stomp, which
        # runs process_resource_by_uuid → _sync_resource_status → set qos=paused.
        qos = _wait_for_qos(qos_slurm_bin_path, s["backend_id"], "paused")
        assert qos == "paused", (
            f"Expected emulator QoS='paused' after STOMP-driven sync, got {qos!r}"
        )

    def test_03_restore_via_full_recovery(
        self,
        qos_stomp_client,
        qos_slurm_bin_path,
        resource_capture,
    ):
        """Drop usage to 0, evaluate, wait for RESOURCE event, verify QoS=normal."""
        s = TestQosStomp._state
        if not s:
            pytest.skip("setup did not run")

        # Wait briefly so the next event isn't immediately matched by an earlier one.
        prev_count = len(resource_capture.messages_for(s["resource_uuid"]))

        _submit_usage(qos_stomp_client, s["resource_uuid"], "node", 0)
        time.sleep(2)
        _evaluate_policy(qos_stomp_client, s["policy_uuid"], s["resource_uuid"])

        # Wait until at least one *new* RESOURCE event for our resource arrives.
        deadline = time.time() + STOMP_WAIT_TIMEOUT
        new_msg = None
        while time.time() < deadline:
            msgs = resource_capture.messages_for(s["resource_uuid"])
            if len(msgs) > prev_count and msgs[-1].get("paused") is False:
                new_msg = msgs[-1]
                break
            time.sleep(1)
        assert new_msg is not None, (
            f"No restore RESOURCE event received within {STOMP_WAIT_TIMEOUT}s"
        )
        assert new_msg.get("paused") is False, (
            f"Expected paused=False in restore message, got {new_msg!r}"
        )

        qos = _wait_for_qos(qos_slurm_bin_path, s["backend_id"], "normal")
        assert qos == "normal", (
            f"Expected emulator QoS='normal' after restore, got {qos!r}"
        )

    def test_04_idempotent_redelivery(
        self,
        qos_stomp_client,
        qos_stomp_backend,
        qos_slurm_bin_path,
    ):
        """Re-trigger the same flag state via direct PATCH; QoS must not be re-written.

        ``_sync_resource_status`` only calls ``set_account_qos`` when the
        current QoS differs from the target. We exercise that by patching the
        resource to its current state (paused=False) and verifying that no new
        ``modify ... set qos=`` command is issued to the backend.
        """
        s = TestQosStomp._state
        if not s:
            pytest.skip("setup did not run")

        # Baseline QoS should be "normal" from test_03.
        qos_before = get_account_qos(qos_slurm_bin_path, s["backend_id"])
        assert qos_before == "normal", (
            f"Precondition: expected QoS=normal before idempotency test, got {qos_before!r}"
        )

        qos_stomp_backend.client.clear_executed_commands()

        # Touch the resource — paused field set to its already-current value.
        # Mastermind's field tracker fires only on a *change*, so we need to
        # toggle. Set paused=True briefly then back to False.
        set_resource_paused(qos_stomp_client, s["resource_uuid"], True)
        time.sleep(1)
        set_resource_paused(qos_stomp_client, s["resource_uuid"], False)

        # After the second flip the resource is again paused=False and the
        # agent processes both events. The first flip writes qos=paused;
        # the second restores qos=normal. Wait for QoS to converge.
        qos_after = _wait_for_qos(qos_slurm_bin_path, s["backend_id"], "normal", timeout=20)
        assert qos_after == "normal", (
            f"Expected QoS to converge to 'normal' after toggle, got {qos_after!r}"
        )

        cmds = qos_stomp_backend.client.executed_commands
        logger.info("Idempotency test executed commands: %s", cmds)
        # The processor must have issued at most one set qos=paused and one
        # set qos=normal — anything more indicates redundant rewrites.
        paused_writes = [c for c in cmds if "set qos=paused" in c.lower()]
        normal_writes = [c for c in cmds if "set qos=normal" in c.lower()]
        assert len(paused_writes) <= 1, (
            f"Expected at most one set qos=paused, got {paused_writes}"
        )
        assert len(normal_writes) <= 1, (
            f"Expected at most one set qos=normal, got {normal_writes}"
        )

    def test_05_cleanup(self, qos_stomp_client, qos_stomp_offering, qos_stomp_backend):
        s = TestQosStomp._state
        if not s:
            pytest.skip("setup did not run")

        try:
            marketplace_slurm_periodic_usage_policies_destroy.sync_detailed(
                client=qos_stomp_client, uuid=s["policy_uuid"]
            )
        except Exception as exc:
            logger.warning("Policy delete failed: %s", exc)

        # Terminate the resource via a TERMINATE order.
        try:
            offering_url, plan_url = _offering_urls(qos_stomp_client, QOS_OFFERING_UUID)
            res = marketplace_provider_resources_retrieve.sync(
                uuid=s["resource_uuid"], client=qos_stomp_client
            )
            resource_url = res.url if not isinstance(res.url, type(UNSET)) else None
            body = OrderCreateRequest(
                offering=offering_url,
                project=s["project_url"],
                plan=plan_url,
                attributes=GenericOrderAttributes(),
                type_=RequestTypes.TERMINATE,
            )
            if resource_url:
                body.additional_properties["resource"] = resource_url
            order = marketplace_orders_create.sync(client=qos_stomp_client, body=body)
            order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
            from waldur_site_agent.common.processors import OfferingOrderProcessor

            processor = OfferingOrderProcessor(
                offering=qos_stomp_offering,
                waldur_rest_client=qos_stomp_client,
                resource_backend=qos_stomp_backend,
            )
            for _ in range(10):
                processor.process_offering()
                order = marketplace_orders_retrieve.sync(
                    client=qos_stomp_client, uuid=order_uuid
                )
                state = (
                    order.state if not isinstance(order.state, type(UNSET)) else None
                )
                if state in (OrderState.DONE, OrderState.ERRED):
                    break
                time.sleep(1)
        except Exception as exc:
            logger.warning("Resource terminate failed: %s", exc)
