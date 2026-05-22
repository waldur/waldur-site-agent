"""E2E tests for SLURM QoS application via the polling path.

Verifies that the agent translates ``resource.paused`` / ``resource.downscaled``
flags into the correct SLURM QoS by running the offering membership processor
synchronously after Mastermind flag transitions.

The single chosen offering uses the "month g0 reset" policy config (grace=0%,
no carryover) so the threshold geometry is simple: 100% = downscale = pause.

Also asserts that ``apply_periodic_settings`` is now QoS-free — no
``modify account set qos=`` commands are issued, even when the inbound
settings dict deliberately omits them.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_POLICY_CONFIG=<path>
    WALDUR_E2E_PROJECT_A_UUID=<uuid>
"""

from __future__ import annotations

import logging
import os
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
from waldur_api_client.models.order_create_request import OrderCreateRequest
from waldur_api_client.models.order_create_request_limits import (
    OrderCreateRequestLimits,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.models.policy_period_enum import PolicyPeriodEnum
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.utils import get_client, load_configuration

from .conftest import get_account_qos

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_POLICY_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

# Offering used for the focused single-offering tests. Picked from the 11
# policy offerings in the demo preset: "month g0 reset" is the simplest
# geometry (grace=0% so downscale and pause both trigger at 100% usage).
TARGET_OFFERING_UUID = "e2ef0000000000000000000000000101"
RESOURCE_NODE_LIMIT = 100
PROCESSOR_CYCLE_DELAY = 1
PROCESSOR_MAX_CYCLES = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _offering_url(client, offering_uuid: str) -> tuple[str, str]:
    """Return (offering_url, plan_url) for the public offering."""
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


def _process_order_terminal(offering, client, backend, order_uuid: str) -> OrderState:
    from waldur_site_agent.common.processors import OfferingOrderProcessor

    processor = OfferingOrderProcessor(
        offering=offering, waldur_rest_client=client, resource_backend=backend
    )
    for _ in range(PROCESSOR_MAX_CYCLES):
        processor.process_offering()
        order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        if state in (OrderState.DONE, OrderState.ERRED):
            return state
        time.sleep(PROCESSOR_CYCLE_DELAY)
    pytest.fail(f"Order {order_uuid} not terminal")


def _get_resource_uuid(client, order_uuid: str) -> str:
    order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
    if isinstance(order.marketplace_resource_uuid, type(UNSET)):
        pytest.fail(f"Order {order_uuid} has no resource UUID")
    ruuid = order.marketplace_resource_uuid
    return ruuid.hex if hasattr(ruuid, "hex") else str(ruuid)


def _get_resource_backend_id(client, resource_uuid: str) -> str:
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    backend_id = res.backend_id
    if isinstance(backend_id, type(UNSET)) or not backend_id:
        pytest.fail(f"Resource {resource_uuid} has no backend_id")
    return backend_id


def _submit_usage(client, resource_uuid: str, component_type: str, amount: int):
    body = ComponentUsageCreateRequest(
        usages=[ComponentUsageItemRequest(type_=component_type, amount=str(amount))],
        resource=UUID(resource_uuid),
    )
    marketplace_component_usages_set_usage.sync_detailed(client=client, body=body)


def _create_policy(client, scope_url: str) -> str:
    """Create a SLURM periodic policy with downscaling + pausing actions.

    Returns policy UUID hex.
    """
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
    """Trigger synchronous policy evaluation; flips paused/downscaled if usage crosses."""
    resp = client.get_httpx_client().post(
        f"/api/marketplace-slurm-periodic-usage-policies/{policy_uuid}/evaluate/",
        json={"resource_uuid": resource_uuid},
    )
    resp.raise_for_status()


def _resource_flags(client, resource_uuid: str) -> tuple[bool, bool]:
    """Return (paused, downscaled) flags from the current resource state."""
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    paused = bool(res.paused) if not isinstance(res.paused, type(UNSET)) else False
    downscaled = (
        bool(res.downscaled) if not isinstance(res.downscaled, type(UNSET)) else False
    )
    return paused, downscaled


def _run_sync(offering, client, backend, resource_uuid: str) -> None:
    """Run the membership processor for one resource (drives _sync_resource_status).

    The backend is injected via the constructor so the processor's init
    can set ``service_provider_uuid`` and ``offering_partitions`` on the
    exact same backend instance the test will later inspect.
    """
    processor = OfferingMembershipProcessor(
        offering=offering, waldur_rest_client=client, resource_backend=backend
    )
    processor.process_resource_by_uuid(resource_uuid)


def _terminate(offering, client, backend, resource_uuid: str, project_url: str):
    offering_url, plan_url = _offering_url(client, offering.waldur_offering_uuid)
    res = marketplace_provider_resources_retrieve.sync(uuid=resource_uuid, client=client)
    resource_url = res.url if not isinstance(res.url, type(UNSET)) else None
    body = OrderCreateRequest(
        offering=offering_url,
        project=project_url,
        plan=plan_url,
        attributes=GenericOrderAttributes(),
        type_=RequestTypes.TERMINATE,
    )
    if resource_url:
        body.additional_properties["resource"] = resource_url
    order = marketplace_orders_create.sync(client=client, body=body)
    order_uuid = order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
    _process_order_terminal(offering, client, backend, order_uuid)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def qos_config():
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_POLICY_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-qos-polling")


@pytest.fixture(scope="module")
def qos_offering(qos_config):
    for o in qos_config.offerings:
        if o.waldur_offering_uuid == TARGET_OFFERING_UUID:
            return o
    pytest.skip(f"Offering {TARGET_OFFERING_UUID} not in config")


@pytest.fixture(scope="module")
def qos_client(qos_offering):
    return get_client(qos_offering.waldur_api_url, qos_offering.waldur_api_token)


@pytest.fixture(scope="module")
def qos_backend(qos_offering):
    return SlurmBackend(
        qos_offering.backend_settings, qos_offering.backend_components_dict
    )


@pytest.fixture(scope="module")
def qos_project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(scope="module")
def slurm_bin_path(qos_offering):
    return qos_offering.backend_settings.get("slurm_bin_path", ".venv/bin")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestQosPolling:
    """QoS application via process_resource_by_uuid (polling path)."""

    _state: dict = {}

    def test_01_setup(
        self, qos_client, qos_offering, qos_backend, qos_project_uuid, slurm_bin_path
    ):
        """Create a resource on the target offering and attach a SLURM periodic policy."""
        project_url = _project_url(qos_client, qos_project_uuid)
        offering_url, plan_url = _offering_url(qos_client, TARGET_OFFERING_UUID)

        order_uuid = _create_order(
            qos_client,
            offering_url,
            project_url,
            plan_url,
            limits={"node": RESOURCE_NODE_LIMIT},
            name="qos-polling-target",
        )
        state = _process_order_terminal(qos_offering, qos_client, qos_backend, order_uuid)
        assert state == OrderState.DONE, f"Setup order {order_uuid} ended in {state}"

        resource_uuid = _get_resource_uuid(qos_client, order_uuid)
        backend_id = _get_resource_backend_id(qos_client, resource_uuid)
        scope_url = _provider_offering_url(qos_client, TARGET_OFFERING_UUID)
        policy_uuid = _create_policy(qos_client, scope_url)

        TestQosPolling._state = {
            "resource_uuid": resource_uuid,
            "backend_id": backend_id,
            "policy_uuid": policy_uuid,
            "project_url": project_url,
        }

        # Fresh resource hasn't been synced yet — QoS may be empty.
        # Run one sync to establish the baseline (paused=False, downscaled=False).
        _run_sync(qos_offering, qos_client, qos_backend, resource_uuid)
        qos = get_account_qos(slurm_bin_path, backend_id)
        logger.info("Initial QoS for %s: %r", backend_id, qos)
        assert qos in ("", "normal"), (
            f"Expected baseline QoS to be empty or 'normal', got {qos!r}"
        )

    def test_02_default_qos_when_no_threshold_crossed(
        self, qos_client, qos_offering, qos_backend, slurm_bin_path
    ):
        """Zero usage → evaluate → sync → emulator account is on qos_default."""
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        _evaluate_policy(qos_client, s["policy_uuid"], s["resource_uuid"])
        paused, downscaled = _resource_flags(qos_client, s["resource_uuid"])
        assert paused is False and downscaled is False, (
            f"Expected both flags False at 0% usage, got paused={paused}, "
            f"downscaled={downscaled}"
        )

        _run_sync(qos_offering, qos_client, qos_backend, s["resource_uuid"])
        qos = get_account_qos(slurm_bin_path, s["backend_id"])
        # restore_resource intentionally skips writing when the account
        # currently has no QoS assigned (a fresh account in the emulator).
        # Both "" and "normal" are semantically default; what matters is
        # that nothing escalating (limited/paused) is set.
        assert qos in ("", "normal"), (
            f"Expected default-equivalent QoS, got {qos!r}"
        )

    def test_03_downscaled_qos_at_100pct(
        self, qos_client, qos_offering, qos_backend, slurm_bin_path
    ):
        """100% usage → flag flips to downscaled → emulator QoS = qos_downscaled."""
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        _submit_usage(qos_client, s["resource_uuid"], "node", RESOURCE_NODE_LIMIT)
        time.sleep(2)
        _evaluate_policy(qos_client, s["policy_uuid"], s["resource_uuid"])
        paused, downscaled = _resource_flags(qos_client, s["resource_uuid"])
        # For grace=0% the same threshold trips both flags. The polling sync
        # then prefers paused (pause_resource is the first branch in
        # _sync_resource_status), so the emulator QoS will be qos_paused.
        # That is correct behaviour for grace=0% configs and matches what
        # the existing test_e2e_policy.py asserts at this phase.
        assert downscaled is True, (
            f"Expected downscaled=True at 100% usage, got {downscaled}"
        )

        _run_sync(qos_offering, qos_client, qos_backend, s["resource_uuid"])
        qos = get_account_qos(slurm_bin_path, s["backend_id"])
        # paused takes precedence over downscaled in _sync_resource_status.
        expected = "paused" if paused else "limited"
        assert qos == expected, (
            f"Expected QoS={expected!r} (paused={paused}, downscaled={downscaled}), "
            f"got {qos!r}"
        )

    def test_04_paused_qos_above_grace(
        self, qos_client, qos_offering, qos_backend, slurm_bin_path
    ):
        """Usage well above the grace limit → paused flag → emulator QoS = qos_paused.

        For this config grace=0% so any usage above the threshold already pauses;
        this test confirms that further increases do not change the QoS state.
        """
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        _submit_usage(qos_client, s["resource_uuid"], "node", int(RESOURCE_NODE_LIMIT * 1.5))
        time.sleep(2)
        _evaluate_policy(qos_client, s["policy_uuid"], s["resource_uuid"])
        paused, downscaled = _resource_flags(qos_client, s["resource_uuid"])
        assert paused is True, f"Expected paused=True above grace, got {paused}"

        _run_sync(qos_offering, qos_client, qos_backend, s["resource_uuid"])
        qos = get_account_qos(slurm_bin_path, s["backend_id"])
        assert qos == "paused", f"Expected QoS='paused', got {qos!r}"

    def test_05_full_recovery_restores_default_qos(
        self, qos_client, qos_offering, qos_backend, slurm_bin_path
    ):
        """0% usage → flags clear → emulator QoS = qos_default."""
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        _submit_usage(qos_client, s["resource_uuid"], "node", 0)
        time.sleep(2)
        _evaluate_policy(qos_client, s["policy_uuid"], s["resource_uuid"])
        paused, downscaled = _resource_flags(qos_client, s["resource_uuid"])
        assert paused is False and downscaled is False, (
            f"Expected both flags False after recovery, "
            f"got paused={paused}, downscaled={downscaled}"
        )

        _run_sync(qos_offering, qos_client, qos_backend, s["resource_uuid"])
        qos = get_account_qos(slurm_bin_path, s["backend_id"])
        assert qos == "normal", f"Expected restored QoS='normal', got {qos!r}"

    def test_06_apply_periodic_settings_is_qos_free(self, qos_backend, slurm_bin_path):
        """apply_periodic_settings emits fairshare / limits / reset — no QoS commands."""
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        qos_before = get_account_qos(slurm_bin_path, s["backend_id"])

        qos_backend.client.clear_executed_commands()
        result = qos_backend.apply_periodic_settings(
            s["backend_id"],
            {
                "fairshare": 333,
                "grp_tres_mins": {"billing": 60000},
                "reset_raw_usage": True,
                "limit_type": "GrpTRESMins",
            },
        )
        assert result.get("success") is True, f"apply_periodic_settings failed: {result}"

        cmds = qos_backend.client.executed_commands
        logger.info("apply_periodic_settings executed: %s", cmds)
        # No QoS commands at all
        assert not any("set qos=" in c.lower() for c in cmds), (
            f"Expected no 'set qos=' command, got: {cmds}"
        )
        # No sshare queries (removed with get_current_usage)
        assert not any("sshare" in c.lower() for c in cmds), (
            f"Expected no sshare query, got: {cmds}"
        )
        # Standard write commands are present
        assert any("fairshare=" in c.lower() for c in cmds), (
            f"Expected a fairshare modify, got: {cmds}"
        )
        assert any("grptresmins=" in c.lower() for c in cmds), (
            f"Expected a GrpTRESMins modify, got: {cmds}"
        )
        assert any("rawusage=0" in c.lower() for c in cmds), (
            f"Expected a RawUsage reset, got: {cmds}"
        )

        qos_after = get_account_qos(slurm_bin_path, s["backend_id"])
        assert qos_after == qos_before, (
            f"apply_periodic_settings must not change QoS: was {qos_before!r}, "
            f"now {qos_after!r}"
        )

    def test_07_cleanup(self, qos_client, qos_offering, qos_backend):
        """Delete policy, terminate resource."""
        s = TestQosPolling._state
        if not s:
            pytest.skip("setup did not run")

        try:
            marketplace_slurm_periodic_usage_policies_destroy.sync_detailed(
                client=qos_client, uuid=s["policy_uuid"]
            )
        except Exception as exc:
            logger.warning("Policy delete failed: %s", exc)

        try:
            _terminate(
                qos_offering, qos_client, qos_backend, s["resource_uuid"], s["project_url"]
            )
        except Exception as exc:
            logger.warning("Resource terminate failed: %s", exc)
