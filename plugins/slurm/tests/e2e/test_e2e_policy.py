"""E2E tests for SLURM periodic usage policy evaluation.

Tests the full policy lifecycle against a real Waldur instance:
  1. Create resources on 11 distinct policy offerings
  2. Create policies via the provider API (one per offering)
  3. Submit usage at various levels
  4. Trigger policy evaluation via staff API
  5. Verify evaluate response: usage percentages, actions taken, state changes

The 11 policy configurations mirror the distinct setups found in the
staging cluster, covering:
  - Period types: monthly, quarterly, total
  - Grace ratios: 0%, 15%, 30%
  - Carryover: enabled (15%) vs disabled
  - Raw usage reset: on vs off

Environment variables:
    WALDUR_E2E_TESTS=true                   - Gate: skip all if not set
    WALDUR_E2E_POLICY_CONFIG=<path>         - Path to policy agent config YAML
    WALDUR_E2E_PROJECT_A_UUID=<uuid>        - Project UUID on Waldur
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
from waldur_api_client.models.qos_strategy_enum import QosStrategyEnum
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.models.slurm_periodic_usage_policy_request import (
    SlurmPeriodicUsagePolicyRequest,
)
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.common.utils import get_client, load_configuration
from waldur_site_agent_slurm.backend import SlurmBackend

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_POLICY_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

# Resource limit used when creating orders (node hours).
RESOURCE_NODE_LIMIT = 100

# ---------------------------------------------------------------------------
# Policy config definitions — mirrors the 11 distinct staging configs
# ---------------------------------------------------------------------------

# PolicyPeriodEnum: VALUE_1=TOTAL, VALUE_2=MONTH_1, VALUE_3=MONTH_3
PERIOD_MONTH = PolicyPeriodEnum.VALUE_2
PERIOD_QUARTER = PolicyPeriodEnum.VALUE_3
PERIOD_TOTAL = PolicyPeriodEnum.VALUE_1


@dataclass
class PolicyConfig:
    """Definition of a policy configuration to test."""

    id: str
    offering_uuid: str
    period: PolicyPeriodEnum
    grace_ratio: float
    carryover_enabled: bool
    carryover_factor: int
    raw_usage_reset: bool
    description: str

    def effective_limit(self, base_limit: int) -> float:
        """Return the effective limit accounting for carryover.

        When carryover is enabled and the previous period had zero usage,
        the full carryover amount (carryover_factor% of the base limit)
        is added to the current period's effective limit.

        Total-period configs have no "previous period", so carryover
        does not increase the effective limit for them.
        """
        if (
            self.carryover_enabled
            and self.carryover_factor > 0
            and self.period != PERIOD_TOTAL
        ):
            return base_limit * (1 + self.carryover_factor / 100)
        return float(base_limit)


POLICY_CONFIGS: list[PolicyConfig] = [
    PolicyConfig(
        id="m-g0-r",
        offering_uuid="e2ef0000000000000000000000000101",
        period=PERIOD_MONTH,
        grace_ratio=0.0,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="monthly, grace=0%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="m-g0",
        offering_uuid="e2ef0000000000000000000000000102",
        period=PERIOD_MONTH,
        grace_ratio=0.0,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=False,
        description="monthly, grace=0%, no carryover, raw_reset=false",
    ),
    PolicyConfig(
        id="m-g15",
        offering_uuid="e2ef0000000000000000000000000103",
        period=PERIOD_MONTH,
        grace_ratio=0.15,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=False,
        description="monthly, grace=15%, no carryover, raw_reset=false",
    ),
    PolicyConfig(
        id="m-g15-r",
        offering_uuid="e2ef0000000000000000000000000104",
        period=PERIOD_MONTH,
        grace_ratio=0.15,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="monthly, grace=15%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="m-g15-c15-r",
        offering_uuid="e2ef0000000000000000000000000105",
        period=PERIOD_MONTH,
        grace_ratio=0.15,
        carryover_enabled=True,
        carryover_factor=15,
        raw_usage_reset=True,
        description="monthly, grace=15%, carryover=15%, raw_reset=true",
    ),
    PolicyConfig(
        id="q-g0-r",
        offering_uuid="e2ef0000000000000000000000000106",
        period=PERIOD_QUARTER,
        grace_ratio=0.0,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="quarterly, grace=0%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="q-g15",
        offering_uuid="e2ef0000000000000000000000000107",
        period=PERIOD_QUARTER,
        grace_ratio=0.15,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=False,
        description="quarterly, grace=15%, no carryover, raw_reset=false",
    ),
    PolicyConfig(
        id="q-g15-r",
        offering_uuid="e2ef0000000000000000000000000108",
        period=PERIOD_QUARTER,
        grace_ratio=0.15,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="quarterly, grace=15%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="q-g30-r",
        offering_uuid="e2ef0000000000000000000000000109",
        period=PERIOD_QUARTER,
        grace_ratio=0.3,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="quarterly, grace=30%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="t-g0-r",
        offering_uuid="e2ef000000000000000000000000010a",
        period=PERIOD_TOTAL,
        grace_ratio=0.0,
        carryover_enabled=False,
        carryover_factor=0,
        raw_usage_reset=True,
        description="total, grace=0%, no carryover, raw_reset=true",
    ),
    PolicyConfig(
        id="t-g15-c15",
        offering_uuid="e2ef000000000000000000000000010b",
        period=PERIOD_TOTAL,
        grace_ratio=0.15,
        carryover_enabled=True,
        carryover_factor=15,
        raw_usage_reset=False,
        description="total, grace=15%, carryover=15%, raw_reset=false",
    ),
]

ACTIONS = (
    "notify_organization_owners,"
    "request_slurm_resource_downscaling,"
    "request_slurm_resource_pausing"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_offering_and_plan_urls(client, offering_uuid: str) -> tuple[str, str]:
    """Return (offering_url, plan_url) for a public offering."""
    resp = client.get_httpx_client().get(
        f"/api/marketplace-public-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    data = resp.json()
    plans = data.get("plans", [])
    if not plans:
        raise RuntimeError(f"No plans for offering {offering_uuid}")
    return data["url"], plans[0]["url"]


def _get_provider_offering_url(client, offering_uuid: str) -> str:
    """Return the provider offering URL (used as policy scope)."""
    resp = client.get_httpx_client().get(
        f"/api/marketplace-provider-offerings/{offering_uuid}/"
    )
    resp.raise_for_status()
    return resp.json()["url"]


def _get_project_url(client, project_uuid: str) -> str:
    resp = client.get_httpx_client().get(f"/api/projects/{project_uuid}/")
    resp.raise_for_status()
    return resp.json()["url"]


def _create_order(
    client, offering_url: str, project_url: str, plan_url: str, limits: dict, name: str
) -> str:
    """Create a CREATE order, return order UUID hex."""
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


def _process_order(offering, client, backend, order_uuid, max_cycles=10):
    """Run processor until order reaches terminal state. Return OrderState."""
    processor = OfferingOrderProcessor(
        offering=offering,
        waldur_rest_client=client,
        resource_backend=backend,
    )
    for cycle in range(max_cycles):
        processor.process_offering()
        order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
        state = order.state if not isinstance(order.state, type(UNSET)) else None
        if state in (OrderState.DONE, OrderState.ERRED):
            return state
        time.sleep(1)
    pytest.fail(f"Order {order_uuid} not terminal after {max_cycles} cycles")


def _get_resource_uuid(client, order_uuid: str) -> str:
    """Get resource UUID from a completed order."""
    order = marketplace_orders_retrieve.sync(client=client, uuid=order_uuid)
    if isinstance(order.marketplace_resource_uuid, type(UNSET)):
        pytest.fail(f"Order {order_uuid} has no resource UUID")
    ruuid = order.marketplace_resource_uuid
    return ruuid.hex if hasattr(ruuid, "hex") else str(ruuid)


def _submit_usage(client, resource_uuid: str, component_type: str, amount: int):
    """Submit component usage for a resource."""
    body = ComponentUsageCreateRequest(
        usages=[
            ComponentUsageItemRequest(type_=component_type, amount=str(amount)),
        ],
        resource=UUID(resource_uuid),
    )
    marketplace_component_usages_set_usage.sync_detailed(client=client, body=body)


def _create_policy(
    client, scope_url: str, cfg: PolicyConfig
) -> str:
    """Create a SLURM periodic usage policy, return policy UUID hex."""
    body = SlurmPeriodicUsagePolicyRequest(
        scope=scope_url,
        actions=ACTIONS,
        component_limits_set=[],
        apply_to_all=True,
        period=cfg.period,
        grace_ratio=cfg.grace_ratio,
        carryover_enabled=cfg.carryover_enabled,
        carryover_factor=cfg.carryover_factor,
        raw_usage_reset=cfg.raw_usage_reset,
        tres_billing_enabled=False,
        qos_strategy=QosStrategyEnum("threshold"),
    )
    # Use raw httpx client to avoid SlurmPeriodicUsagePolicy.from_dict()
    # crash on nullable date fields (dateutil isoparser TypeError).
    payload = body.to_dict()
    resp = client.get_httpx_client().post(
        "/api/marketplace-slurm-periodic-usage-policies/",
        json=payload,
    )
    if resp.is_error:
        logger.error(
            "[%s] policy create failed: status=%s response=%s payload=%s",
            cfg.id,
            resp.status_code,
            resp.text,
            payload,
        )
    resp.raise_for_status()
    return str(resp.json()["uuid"]).replace("-", "")


def _evaluate_policy(client, policy_uuid: str, resource_uuid: str | None = None):
    """Trigger synchronous policy evaluation, return response.

    Uses raw httpx to avoid potential from_dict parsing issues
    in the generated client. Returns a SimpleNamespace tree so
    tests can use attribute access (e.g. result.resources[0].usage_percentage).
    """
    payload: dict = {}
    if resource_uuid:
        payload["resource_uuid"] = resource_uuid
    resp = client.get_httpx_client().post(
        f"/api/marketplace-slurm-periodic-usage-policies/{policy_uuid}/evaluate/",
        json=payload,
    )
    resp.raise_for_status()
    data = resp.json()

    # Build lightweight attribute-access wrappers.
    resources = []
    for r in data.get("resources", []):
        prev_state = types.SimpleNamespace(
            additional_properties=r.get("previous_state", {})
        )
        new_state = types.SimpleNamespace(
            additional_properties=r.get("new_state", {})
        )
        resources.append(types.SimpleNamespace(
            resource_uuid=r.get("resource_uuid"),
            resource_name=r.get("resource_name"),
            usage_percentage=r.get("usage_percentage", 0.0),
            actions_taken=r.get("actions_taken", []),
            previous_state=prev_state,
            new_state=new_state,
        ))
    return types.SimpleNamespace(
        policy_uuid=data.get("policy_uuid"),
        billing_period=data.get("billing_period"),
        resources=resources,
    )


def _delete_policy(client, policy_uuid: str):
    """Delete a policy."""
    marketplace_slurm_periodic_usage_policies_destroy.sync_detailed(
        client=client, uuid=policy_uuid
    )


def _terminate_resource(client, resource_uuid: str, offering_url, project_url, plan_url, offering, backend):
    """Create TERMINATE order and process it."""
    res = marketplace_provider_resources_retrieve.sync(
        uuid=resource_uuid, client=client
    )
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
    _process_order(offering, client, backend, order_uuid, max_cycles=10)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config():
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_POLICY_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-policy-test")


@pytest.fixture(scope="module")
def offerings_map(config):
    """Map offering UUID → offering config object."""
    return {o.waldur_offering_uuid: o for o in config.offerings}


@pytest.fixture(scope="module")
def waldur_client(config):
    """Authenticated client for Waldur."""
    first = config.offerings[0]
    return get_client(first.waldur_api_url, first.waldur_api_token)


@pytest.fixture(scope="module")
def _emulator_cleanup(config):
    """Reset slurm-emulator state."""
    first = config.offerings[0]
    slurm_bin_path = first.backend_settings.get("slurm_bin_path", ".venv/bin")
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
    try:
        subprocess.check_output(
            [sacctmgr, "cleanup", "all"], stderr=subprocess.STDOUT, timeout=10
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        state_file = Path("/tmp/slurm_emulator_db.json")
        if state_file.exists():
            state_file.unlink()


@pytest.fixture(scope="module")
def backends(offerings_map, _emulator_cleanup):
    """Map offering UUID → SlurmBackend."""
    result = {}
    for uuid, offering in offerings_map.items():
        result[uuid] = SlurmBackend(
            offering.backend_settings, offering.backend_components_dict
        )
    return result


@pytest.fixture(scope="module")
def project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSlurmPolicyE2E:
    """End-to-end SLURM periodic usage policy tests.

    Tests run in order. Shared state is stored in ``_state``.
    """

    _state: dict = {}

    # -- Phase 1: Create resources on all 11 offerings -----------------------

    def test_01_create_resources(
        self, waldur_client, offerings_map, backends, project_uuid
    ):
        """Create one resource per policy offering via order processing."""
        project_url = _get_project_url(waldur_client, project_uuid)
        resources = {}

        for cfg in POLICY_CONFIGS:
            offering = offerings_map.get(cfg.offering_uuid)
            if not offering:
                pytest.skip(f"Offering {cfg.offering_uuid} not in config")

            offering_url, plan_url = _get_offering_and_plan_urls(
                waldur_client, cfg.offering_uuid
            )
            backend = backends[cfg.offering_uuid]

            order_uuid = _create_order(
                waldur_client,
                offering_url,
                project_url,
                plan_url,
                limits={"node": RESOURCE_NODE_LIMIT},
                name=f"pol-{cfg.id}",
            )
            logger.info("[%s] Created order %s", cfg.id, order_uuid)

            state = _process_order(
                offering, waldur_client, backend, order_uuid, max_cycles=10
            )
            assert state == OrderState.DONE, (
                f"[{cfg.id}] Order {order_uuid} ended in {state}"
            )

            resource_uuid = _get_resource_uuid(waldur_client, order_uuid)
            resources[cfg.id] = {
                "resource_uuid": resource_uuid,
                "order_uuid": order_uuid,
                "offering_url": offering_url,
                "plan_url": plan_url,
            }
            logger.info("[%s] Resource %s created", cfg.id, resource_uuid)

        TestSlurmPolicyE2E._state["resources"] = resources
        TestSlurmPolicyE2E._state["project_url"] = project_url
        assert len(resources) == len(POLICY_CONFIGS)

    # -- Phase 2: Create policies via API ------------------------------------

    def test_02_create_policies(self, waldur_client):
        """Create a policy on each offering via the provider API."""
        resources = TestSlurmPolicyE2E._state.get("resources")
        if not resources:
            pytest.skip("No resources from test_01")

        policies = {}
        for cfg in POLICY_CONFIGS:
            scope_url = _get_provider_offering_url(
                waldur_client, cfg.offering_uuid
            )
            policy_uuid = _create_policy(waldur_client, scope_url, cfg)
            policies[cfg.id] = policy_uuid
            logger.info(
                "[%s] Created policy %s (%s)", cfg.id, policy_uuid, cfg.description
            )

        TestSlurmPolicyE2E._state["policies"] = policies
        assert len(policies) == len(POLICY_CONFIGS)

    # -- Phase 3: Evaluate with no usage → no actions ------------------------

    def test_03_evaluate_no_usage(self, waldur_client):
        """Evaluate all policies with zero usage — expect no actions."""
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]

            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)
            assert result is not None, f"[{cfg.id}] Evaluate returned None"

            if result.resources:
                res = result.resources[0]
                logger.info(
                    "[%s] usage=%.1f%% actions=%s",
                    cfg.id,
                    res.usage_percentage,
                    res.actions_taken,
                )
                assert res.usage_percentage == 0.0, (
                    f"[{cfg.id}] Expected 0% usage, got {res.usage_percentage}%"
                )
                assert res.actions_taken == [], (
                    f"[{cfg.id}] Expected no actions, got {res.actions_taken}"
                )

    # -- Phase 4: Submit usage at 50% → still no downscale/pause -------------

    def test_04_evaluate_below_threshold(self, waldur_client):
        """Submit 50% usage on all resources — expect no downscale/pause."""
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        usage_amount = RESOURCE_NODE_LIMIT // 2  # 50%

        for cfg in POLICY_CONFIGS:
            resource_uuid = resources[cfg.id]["resource_uuid"]
            _submit_usage(waldur_client, resource_uuid, "node", usage_amount)

        # Small delay for usage to be recorded
        time.sleep(2)

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]
            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)

            if result and result.resources:
                res = result.resources[0]
                logger.info(
                    "[%s] usage=%.1f%% actions=%s",
                    cfg.id,
                    res.usage_percentage,
                    res.actions_taken,
                )
                # At 50%, no policy should trigger downscale or pause
                downscale_or_pause = [
                    a for a in res.actions_taken
                    if "downscal" in a.lower() or "paus" in a.lower()
                ]
                assert downscale_or_pause == [], (
                    f"[{cfg.id}] Unexpected actions at 50%: {res.actions_taken}"
                )

    # -- Phase 5: Submit usage above 100% → expect downscaling ---------------

    def test_05_evaluate_above_threshold(self, waldur_client):
        """Submit usage above 100% of effective limit — expect downscaling."""
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        for cfg in POLICY_CONFIGS:
            # Use 110% of effective limit (accounts for carryover).
            eff = cfg.effective_limit(RESOURCE_NODE_LIMIT)
            usage_amount = int(eff * 1.1)
            resource_uuid = resources[cfg.id]["resource_uuid"]
            _submit_usage(waldur_client, resource_uuid, "node", usage_amount)

        time.sleep(2)

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]
            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)

            if result and result.resources:
                res = result.resources[0]
                logger.info(
                    "[%s] usage=%.1f%% actions=%s",
                    cfg.id,
                    res.usage_percentage,
                    res.actions_taken,
                )
                # Above 100% of effective limit → downscaling should trigger
                assert res.usage_percentage >= 100.0, (
                    f"[{cfg.id}] Expected >=100% usage, got {res.usage_percentage}%"
                )
                has_downscale = any(
                    "downscal" in a.lower() for a in res.actions_taken
                )
                assert has_downscale, (
                    f"[{cfg.id}] Expected downscaling at 110%, got {res.actions_taken}"
                )

    # -- Phase 6: Submit usage above grace → expect pausing ------------------

    def test_06_evaluate_above_grace(self, waldur_client):
        """Submit usage above grace limit — expect pausing for grace>0 configs.

        For grace=0% configs, pausing triggers at 100% (same as downscaling).
        For grace=15% configs, pausing triggers at 115%.
        For grace=30% configs, pausing triggers at 130%.
        """
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        for cfg in POLICY_CONFIGS:
            # Submit usage that exceeds grace limit relative to effective limit.
            eff = cfg.effective_limit(RESOURCE_NODE_LIMIT)
            pause_threshold = 1.0 + cfg.grace_ratio
            usage_amount = int(eff * (pause_threshold + 0.1))

            resource_uuid = resources[cfg.id]["resource_uuid"]
            _submit_usage(waldur_client, resource_uuid, "node", usage_amount)

        time.sleep(2)

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]
            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)

            if result and result.resources:
                res = result.resources[0]
                logger.info(
                    "[%s] grace=%.0f%% usage=%.1f%% actions=%s",
                    cfg.id,
                    cfg.grace_ratio * 100,
                    res.usage_percentage,
                    res.actions_taken,
                )
                # Both downscaling and pausing should be active
                has_downscale = any(
                    "downscal" in a.lower() for a in res.actions_taken
                )
                has_pause = any(
                    "paus" in a.lower() for a in res.actions_taken
                )
                assert has_downscale, (
                    f"[{cfg.id}] Expected downscaling above grace, "
                    f"got {res.actions_taken}"
                )
                assert has_pause, (
                    f"[{cfg.id}] Expected pausing above grace "
                    f"(grace={cfg.grace_ratio}), got {res.actions_taken}"
                )

    # -- Phase 7: Partial recovery — drop from paused to downscaled-only -----

    def test_07_partial_recovery(self, waldur_client):
        """After test_06 paused all resources, drop usage to 105%.

        For configs with grace>=15%, the resource should recover from paused
        (usage is below grace limit) but remain downscaled (still above 100%).
        For grace=0% configs, 105% is still above the grace limit (100%),
        so they stay paused and downscaled.
        """
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        for cfg in POLICY_CONFIGS:
            # 105% of effective limit — between 100% and grace for grace>=15%.
            eff = cfg.effective_limit(RESOURCE_NODE_LIMIT)
            usage_amount = int(eff * 1.05)
            resource_uuid = resources[cfg.id]["resource_uuid"]
            _submit_usage(waldur_client, resource_uuid, "node", usage_amount)

        time.sleep(2)

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]
            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)

            if result and result.resources:
                res = result.resources[0]
                prev = res.previous_state.additional_properties
                new = res.new_state.additional_properties
                logger.info(
                    "[%s] partial recovery: usage=%.1f%% actions=%s "
                    "prev=%s new=%s",
                    cfg.id,
                    res.usage_percentage,
                    res.actions_taken,
                    prev,
                    new,
                )

                if cfg.grace_ratio > 0:
                    # Grace > 0%: 105% is below grace limit → no pause
                    assert new.get("paused") is False, (
                        f"[{cfg.id}] Expected new_state paused=False "
                        f"(105% is below grace limit "
                        f"{(1 + cfg.grace_ratio) * 100}%), got {new}"
                    )
                    # Still above 100% → downscale remains
                    assert new.get("downscaled") is True, (
                        f"[{cfg.id}] Expected new_state downscaled=True "
                        f"(still at 105%), got {new}"
                    )
                    # Actions should include downscale but NOT pause
                    has_pause = any(
                        "paus" in a.lower() for a in res.actions_taken
                    )
                    assert not has_pause, (
                        f"[{cfg.id}] Should NOT pause at 105% with "
                        f"grace={cfg.grace_ratio}, got {res.actions_taken}"
                    )
                else:
                    # Grace = 0%: 105% is above grace limit (100%) → still paused
                    assert new.get("paused") is True, (
                        f"[{cfg.id}] Grace=0%: expected paused=True at 105%, "
                        f"got {new}"
                    )
                    assert new.get("downscaled") is True, (
                        f"[{cfg.id}] Grace=0%: expected downscaled=True at "
                        f"105%, got {new}"
                    )

    # -- Phase 8: Full recovery — drop usage to zero -------------------------

    def test_08_full_recovery(self, waldur_client):
        """Drop usage to 0% — all resources should fully recover.

        Both paused and downscaled flags should be cleared for every config.
        """
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        if not policies:
            pytest.skip("No policies from test_02")

        for cfg in POLICY_CONFIGS:
            resource_uuid = resources[cfg.id]["resource_uuid"]
            _submit_usage(waldur_client, resource_uuid, "node", 0)

        time.sleep(2)

        for cfg in POLICY_CONFIGS:
            policy_uuid = policies[cfg.id]
            resource_uuid = resources[cfg.id]["resource_uuid"]
            result = _evaluate_policy(waldur_client, policy_uuid, resource_uuid)

            if result and result.resources:
                res = result.resources[0]
                prev = res.previous_state.additional_properties
                new = res.new_state.additional_properties
                logger.info(
                    "[%s] full recovery: usage=%.1f%% actions=%s "
                    "prev=%s new=%s",
                    cfg.id,
                    res.usage_percentage,
                    res.actions_taken,
                    prev,
                    new,
                )

                assert res.usage_percentage == 0.0, (
                    f"[{cfg.id}] Expected 0% usage after reset, "
                    f"got {res.usage_percentage}%"
                )
                assert new.get("paused") is False, (
                    f"[{cfg.id}] Expected paused=False after full recovery, "
                    f"got {new}"
                )
                assert new.get("downscaled") is False, (
                    f"[{cfg.id}] Expected downscaled=False after full "
                    f"recovery, got {new}"
                )
                assert res.actions_taken == [], (
                    f"[{cfg.id}] Expected no actions at 0% usage, "
                    f"got {res.actions_taken}"
                )

    # -- Phase 9: Cleanup ----------------------------------------------------

    def test_09_cleanup(
        self, waldur_client, offerings_map, backends
    ):
        """Delete policies and terminate resources."""
        policies = TestSlurmPolicyE2E._state.get("policies", {})
        resources = TestSlurmPolicyE2E._state.get("resources", {})
        project_url = TestSlurmPolicyE2E._state.get("project_url", "")

        # Delete policies first
        for cfg_id, policy_uuid in policies.items():
            try:
                _delete_policy(waldur_client, policy_uuid)
                logger.info("[%s] Deleted policy %s", cfg_id, policy_uuid)
            except Exception as exc:
                logger.warning("[%s] Policy delete failed: %s", cfg_id, exc)

        # Terminate resources
        for cfg in POLICY_CONFIGS:
            info = resources.get(cfg.id)
            if not info:
                continue
            offering = offerings_map.get(cfg.offering_uuid)
            backend = backends.get(cfg.offering_uuid)
            if not offering or not backend:
                continue
            try:
                _terminate_resource(
                    waldur_client,
                    info["resource_uuid"],
                    info["offering_url"],
                    project_url,
                    info["plan_url"],
                    offering,
                    backend,
                )
                logger.info("[%s] Terminated resource %s", cfg.id, info["resource_uuid"])
            except Exception as exc:
                logger.warning("[%s] Terminate failed: %s", cfg.id, exc)
