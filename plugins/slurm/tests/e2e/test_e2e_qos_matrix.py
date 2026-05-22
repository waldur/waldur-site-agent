"""E2E QoS-application sweep across all 11 distinct policy configurations.

Complements ``test_e2e_policy.py``, which asserts the Mastermind-side state
transitions (``resource.paused`` / ``resource.downscaled``). This file is the
agent-side complement: after each Mastermind transition, it runs the
membership processor for the resource and verifies that the SLURM emulator
account has the QoS the agent should have written.

Defense-in-depth: every policy config should produce the same agent-side
behaviour (QoS keyed off the two boolean flags). The loop guards against
accidental coupling between policy configuration and agent QoS application.

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_POLICY_CONFIG=<path>
    WALDUR_E2E_PROJECT_A_UUID=<uuid>
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass
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
from waldur_api_client.models.request_types import RequestTypes
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingOrderProcessor,
)
from waldur_site_agent.common.utils import get_client, load_configuration

from .conftest import get_account_qos

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_CONFIG_PATH = os.environ.get("WALDUR_E2E_POLICY_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")

pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

RESOURCE_NODE_LIMIT = 100
PERIOD_MONTH = PolicyPeriodEnum.VALUE_2
PERIOD_QUARTER = PolicyPeriodEnum.VALUE_3
PERIOD_TOTAL = PolicyPeriodEnum.VALUE_1


# ---------------------------------------------------------------------------
# Policy configs (mirror those in test_e2e_policy.py)
# ---------------------------------------------------------------------------


@dataclass
class PolicyConfig:
    id: str
    offering_uuid: str
    period: PolicyPeriodEnum
    grace_ratio: float
    carryover_enabled: bool
    carryover_factor: int
    raw_usage_reset: bool

    def effective_limit(self, base_limit: int) -> float:
        if (
            self.carryover_enabled
            and self.carryover_factor > 0
            and self.period != PERIOD_TOTAL
        ):
            return base_limit * (1 + self.carryover_factor / 100)
        return float(base_limit)


POLICY_CONFIGS: list[PolicyConfig] = [
    PolicyConfig("m-g0-r", "e2ef0000000000000000000000000101", PERIOD_MONTH, 0.0, False, 0, True),
    PolicyConfig("m-g0", "e2ef0000000000000000000000000102", PERIOD_MONTH, 0.0, False, 0, False),
    PolicyConfig("m-g15", "e2ef0000000000000000000000000103", PERIOD_MONTH, 0.15, False, 0, False),
    PolicyConfig("m-g15-r", "e2ef0000000000000000000000000104", PERIOD_MONTH, 0.15, False, 0, True),
    PolicyConfig("m-g15-c15-r", "e2ef0000000000000000000000000105", PERIOD_MONTH, 0.15, True, 15, True),
    PolicyConfig("q-g0-r", "e2ef0000000000000000000000000106", PERIOD_QUARTER, 0.0, False, 0, True),
    PolicyConfig("q-g15", "e2ef0000000000000000000000000107", PERIOD_QUARTER, 0.15, False, 0, False),
    PolicyConfig("q-g15-r", "e2ef0000000000000000000000000108", PERIOD_QUARTER, 0.15, False, 0, True),
    PolicyConfig("q-g30-r", "e2ef0000000000000000000000000109", PERIOD_QUARTER, 0.30, False, 0, True),
    PolicyConfig("t-g0-r", "e2ef000000000000000000000000010a", PERIOD_TOTAL, 0.0, False, 0, True),
    PolicyConfig("t-g15-c15", "e2ef000000000000000000000000010b", PERIOD_TOTAL, 0.15, True, 15, False),
]

ACTIONS = (
    "notify_organization_owners,"
    "request_slurm_resource_downscaling,"
    "request_slurm_resource_pausing"
)


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


def _submit_usage(client, resource_uuid: str, component_type: str, amount: int):
    body = ComponentUsageCreateRequest(
        usages=[ComponentUsageItemRequest(type_=component_type, amount=str(amount))],
        resource=UUID(resource_uuid),
    )
    marketplace_component_usages_set_usage.sync_detailed(client=client, body=body)


def _create_policy(client, scope_url: str, cfg: PolicyConfig) -> str:
    payload = {
        "scope": scope_url,
        "actions": ACTIONS,
        "component_limits_set": [],
        "apply_to_all": True,
        "period": cfg.period.value,
        "grace_ratio": cfg.grace_ratio,
        "carryover_enabled": cfg.carryover_enabled,
        "carryover_factor": cfg.carryover_factor,
        "raw_usage_reset": cfg.raw_usage_reset,
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


def _run_sync(offering, client, backend, resource_uuid: str) -> None:
    processor = OfferingMembershipProcessor(
        offering=offering, waldur_rest_client=client, resource_backend=backend
    )
    processor.process_resource_by_uuid(resource_uuid)


def _acceptable_qos_for_flags(paused: bool, downscaled: bool) -> set[str]:
    """Mirror _sync_resource_status dispatch order: pause > downscale > restore.

    For the default (both flags False) state the agent's ``restore_resource``
    intentionally does NOT write ``qos_default`` when the account currently
    has no QoS assigned (a fresh-account safeguard). So a fresh account stays
    at empty QoS, which is semantically equivalent to ``qos_default``. Once
    the account has been escalated and then restored, the QoS becomes
    ``normal`` explicitly. Both forms are acceptable for the default state.
    """
    if paused:
        return {"paused"}
    if downscaled:
        return {"limited"}
    return {"", "normal"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def matrix_config():
    if not E2E_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_POLICY_CONFIG not set")
    return load_configuration(E2E_CONFIG_PATH, user_agent_suffix="e2e-qos-matrix")


@pytest.fixture(scope="module")
def offerings_map(matrix_config):
    return {o.waldur_offering_uuid: o for o in matrix_config.offerings}


@pytest.fixture(scope="module")
def matrix_client(matrix_config):
    first = matrix_config.offerings[0]
    return get_client(first.waldur_api_url, first.waldur_api_token)


@pytest.fixture(scope="module")
def _matrix_emulator_cleanup(matrix_config):
    first = matrix_config.offerings[0]
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
def matrix_backends(offerings_map, _matrix_emulator_cleanup):
    return {
        uuid: SlurmBackend(o.backend_settings, o.backend_components_dict)
        for uuid, o in offerings_map.items()
    }


@pytest.fixture(scope="module")
def matrix_project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


@pytest.fixture(scope="module")
def matrix_slurm_bin_path(matrix_config):
    return matrix_config.offerings[0].backend_settings.get("slurm_bin_path", ".venv/bin")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestQosMatrix:
    """Loop over 11 configs and verify agent QoS application."""

    _state: dict = {}

    def test_01_create_resources_and_policies(
        self,
        matrix_client,
        offerings_map,
        matrix_backends,
        matrix_project_uuid,
        matrix_slurm_bin_path,
    ):
        project_url = _project_url(matrix_client, matrix_project_uuid)
        resources = {}
        policies = {}

        for cfg in POLICY_CONFIGS:
            offering = offerings_map.get(cfg.offering_uuid)
            if not offering:
                pytest.skip(f"Offering {cfg.offering_uuid} not in config")
            offering_url, plan_url = _offering_urls(matrix_client, cfg.offering_uuid)
            backend = matrix_backends[cfg.offering_uuid]

            order_uuid = _create_order(
                matrix_client,
                offering_url,
                project_url,
                plan_url,
                limits={"node": RESOURCE_NODE_LIMIT},
                name=f"qmx-{cfg.id}",
            )
            state = _process_order_terminal(offering, matrix_client, backend, order_uuid)
            assert state == OrderState.DONE, (
                f"[{cfg.id}] Setup order {order_uuid} ended in {state}"
            )

            resource_uuid = _get_resource_uuid(matrix_client, order_uuid)
            backend_id = _get_backend_id(matrix_client, resource_uuid)
            scope_url = _provider_offering_url(matrix_client, cfg.offering_uuid)
            policy_uuid = _create_policy(matrix_client, scope_url, cfg)

            resources[cfg.id] = {
                "resource_uuid": resource_uuid,
                "backend_id": backend_id,
                "offering_url": offering_url,
                "plan_url": plan_url,
            }
            policies[cfg.id] = policy_uuid

            # Establish baseline QoS via a sync.
            _run_sync(offering, matrix_client, backend, resource_uuid)

        TestQosMatrix._state = {
            "resources": resources,
            "policies": policies,
            "project_url": project_url,
        }
        assert len(resources) == len(POLICY_CONFIGS)
        assert len(policies) == len(POLICY_CONFIGS)

    def test_02_zero_usage_qos_default(
        self, matrix_client, offerings_map, matrix_backends, matrix_slurm_bin_path
    ):
        """0% usage on every config → every emulator account at qos_default."""
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        failures = []
        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            policy_uuid = s["policies"][cfg.id]
            offering = offerings_map[cfg.offering_uuid]
            backend = matrix_backends[cfg.offering_uuid]

            _evaluate_policy(matrix_client, policy_uuid, info["resource_uuid"])
            paused, downscaled = _resource_flags(matrix_client, info["resource_uuid"])
            _run_sync(offering, matrix_client, backend, info["resource_uuid"])
            qos = get_account_qos(matrix_slurm_bin_path, info["backend_id"])
            acceptable = _acceptable_qos_for_flags(paused, downscaled)
            if qos not in acceptable:
                failures.append(
                    f"[{cfg.id}] paused={paused} downscaled={downscaled} "
                    f"acceptable={acceptable} actual={qos!r}"
                )
        assert not failures, "QoS mismatch at 0% usage:\n  " + "\n  ".join(failures)

    def test_03_above_threshold_qos(
        self, matrix_client, offerings_map, matrix_backends, matrix_slurm_bin_path
    ):
        """110% of effective_limit → expected: downscaled. (grace=0% configs: paused.)"""
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            _submit_usage(
                matrix_client,
                info["resource_uuid"],
                "node",
                int(cfg.effective_limit(RESOURCE_NODE_LIMIT) * 1.1),
            )
        time.sleep(2)

        failures = []
        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            policy_uuid = s["policies"][cfg.id]
            offering = offerings_map[cfg.offering_uuid]
            backend = matrix_backends[cfg.offering_uuid]

            _evaluate_policy(matrix_client, policy_uuid, info["resource_uuid"])
            paused, downscaled = _resource_flags(matrix_client, info["resource_uuid"])
            _run_sync(offering, matrix_client, backend, info["resource_uuid"])
            qos = get_account_qos(matrix_slurm_bin_path, info["backend_id"])
            acceptable = _acceptable_qos_for_flags(paused, downscaled)
            if qos not in acceptable:
                failures.append(
                    f"[{cfg.id}] grace={cfg.grace_ratio} paused={paused} "
                    f"downscaled={downscaled} acceptable={acceptable} actual={qos!r}"
                )
        assert not failures, "QoS mismatch at 110%:\n  " + "\n  ".join(failures)

    def test_04_above_grace_qos_paused(
        self, matrix_client, offerings_map, matrix_backends, matrix_slurm_bin_path
    ):
        """Usage above grace limit → expected: paused for every config."""
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            eff = cfg.effective_limit(RESOURCE_NODE_LIMIT)
            usage = int(eff * (1.0 + cfg.grace_ratio + 0.1))
            _submit_usage(matrix_client, info["resource_uuid"], "node", usage)
        time.sleep(2)

        failures = []
        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            policy_uuid = s["policies"][cfg.id]
            offering = offerings_map[cfg.offering_uuid]
            backend = matrix_backends[cfg.offering_uuid]

            _evaluate_policy(matrix_client, policy_uuid, info["resource_uuid"])
            paused, downscaled = _resource_flags(matrix_client, info["resource_uuid"])
            _run_sync(offering, matrix_client, backend, info["resource_uuid"])
            qos = get_account_qos(matrix_slurm_bin_path, info["backend_id"])
            acceptable = _acceptable_qos_for_flags(paused, downscaled)
            if qos not in acceptable:
                failures.append(
                    f"[{cfg.id}] grace={cfg.grace_ratio} paused={paused} "
                    f"downscaled={downscaled} acceptable={acceptable} actual={qos!r}"
                )
            elif not paused:
                failures.append(
                    f"[{cfg.id}] grace={cfg.grace_ratio} expected paused=True "
                    f"above grace, got paused={paused}"
                )
        assert not failures, "QoS mismatch above grace:\n  " + "\n  ".join(failures)

    def test_05_partial_recovery_qos(
        self, matrix_client, offerings_map, matrix_backends, matrix_slurm_bin_path
    ):
        """Drop to 105% → grace>0% configs: downscaled. grace=0% configs: still paused."""
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            usage = int(cfg.effective_limit(RESOURCE_NODE_LIMIT) * 1.05)
            _submit_usage(matrix_client, info["resource_uuid"], "node", usage)
        time.sleep(2)

        failures = []
        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            policy_uuid = s["policies"][cfg.id]
            offering = offerings_map[cfg.offering_uuid]
            backend = matrix_backends[cfg.offering_uuid]

            _evaluate_policy(matrix_client, policy_uuid, info["resource_uuid"])
            paused, downscaled = _resource_flags(matrix_client, info["resource_uuid"])
            _run_sync(offering, matrix_client, backend, info["resource_uuid"])
            qos = get_account_qos(matrix_slurm_bin_path, info["backend_id"])
            acceptable = _acceptable_qos_for_flags(paused, downscaled)
            if qos not in acceptable:
                failures.append(
                    f"[{cfg.id}] grace={cfg.grace_ratio} paused={paused} "
                    f"downscaled={downscaled} acceptable={acceptable} actual={qos!r}"
                )
        assert not failures, "QoS mismatch at 105%:\n  " + "\n  ".join(failures)

    def test_06_full_recovery_qos_default(
        self, matrix_client, offerings_map, matrix_backends, matrix_slurm_bin_path
    ):
        """Drop to 0% on every config → emulator QoS = normal for all."""
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            _submit_usage(matrix_client, info["resource_uuid"], "node", 0)
        time.sleep(2)

        failures = []
        for cfg in POLICY_CONFIGS:
            info = s["resources"][cfg.id]
            policy_uuid = s["policies"][cfg.id]
            offering = offerings_map[cfg.offering_uuid]
            backend = matrix_backends[cfg.offering_uuid]

            _evaluate_policy(matrix_client, policy_uuid, info["resource_uuid"])
            paused, downscaled = _resource_flags(matrix_client, info["resource_uuid"])
            _run_sync(offering, matrix_client, backend, info["resource_uuid"])
            qos = get_account_qos(matrix_slurm_bin_path, info["backend_id"])
            if qos != "normal":
                failures.append(
                    f"[{cfg.id}] expected QoS=normal after recovery, got {qos!r} "
                    f"(paused={paused}, downscaled={downscaled})"
                )
        assert not failures, "Recovery QoS mismatch:\n  " + "\n  ".join(failures)

    def test_07_cleanup(self, matrix_client, offerings_map, matrix_backends):
        s = TestQosMatrix._state
        if not s:
            pytest.skip("setup did not run")

        for cfg_id, policy_uuid in s["policies"].items():
            try:
                marketplace_slurm_periodic_usage_policies_destroy.sync_detailed(
                    client=matrix_client, uuid=policy_uuid
                )
            except Exception as exc:
                logger.warning("[%s] policy delete failed: %s", cfg_id, exc)

        for cfg in POLICY_CONFIGS:
            info = s["resources"].get(cfg.id)
            if not info:
                continue
            offering = offerings_map.get(cfg.offering_uuid)
            backend = matrix_backends.get(cfg.offering_uuid)
            if not offering or not backend:
                continue
            try:
                offering_url, plan_url = _offering_urls(matrix_client, cfg.offering_uuid)
                res = marketplace_provider_resources_retrieve.sync(
                    uuid=info["resource_uuid"], client=matrix_client
                )
                resource_url = (
                    res.url if not isinstance(res.url, type(UNSET)) else None
                )
                body = OrderCreateRequest(
                    offering=offering_url,
                    project=s["project_url"],
                    plan=plan_url,
                    attributes=GenericOrderAttributes(),
                    type_=RequestTypes.TERMINATE,
                )
                if resource_url:
                    body.additional_properties["resource"] = resource_url
                order = marketplace_orders_create.sync(client=matrix_client, body=body)
                order_uuid = (
                    order.uuid.hex if hasattr(order.uuid, "hex") else str(order.uuid)
                )
                _process_order_terminal(
                    offering, matrix_client, backend, order_uuid
                )
            except Exception as exc:
                logger.warning("[%s] resource terminate failed: %s", cfg.id, exc)
