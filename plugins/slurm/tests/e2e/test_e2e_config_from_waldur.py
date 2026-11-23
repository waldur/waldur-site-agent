"""E2E: an offering whose agent configuration comes from Waldur (waldur/waldur-site-agent#80).

Against a real Waldur and the SLURM emulator:

1. the test writes the agent configuration of its own offering through the API
   (``update_integration``) and gives its ``cpu`` component a ``unit_factor`` other
   than 1 and a limit (``update_offering_component``);
2. it loads an agent configuration file that holds only the bootstrap fields
   (URL, token, offering UUID, ``configuration_source: waldur``) and checks that
   the loaded offering is what Waldur serves;
3. it runs an order through the agent and checks that the SLURM account limit is
   the component limit multiplied by the factor;
4. it injects usage into the emulator, reports it, and checks that Waldur records
   the backend usage divided by the factor;
5. it changes a backend setting in Waldur, loads again and checks the new value.

The offering (``E2E SLURM Config from Waldur``, ci/site_agent_e2e.json) is used by
no other suite, so rewriting its configuration and components cannot change what
the other files of the job see. Only the Waldur URL and token are taken from
``WALDUR_E2E_CONFIG``, as is the emulator's ``slurm_bin_path``.

Needs a Waldur with ``GET marketplace-provider-offerings/{uuid}/site_agent_config/``
(waldur/waldur-mastermind#1287).

Environment variables:
    WALDUR_E2E_TESTS=true               - Gate: skip all if not set
    WALDUR_E2E_CONFIG=<path>            - Agent config YAML (Waldur URL, token, slurm_bin_path)
    WALDUR_E2E_PROJECT_A_UUID=<uuid>    - Project to create the order in
"""

from __future__ import annotations

import copy
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.api.marketplace_provider_offerings import (
    marketplace_provider_offerings_retrieve,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_retrieve,
)
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET
from waldur_site_agent_slurm.backend import SlurmBackend

from waldur_site_agent.common.processors import OfferingReportProcessor
from waldur_site_agent.common.utils import get_client, load_configuration

from .conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
pytestmark = pytest.mark.skipif(not E2E_TESTS, reason="WALDUR_E2E_TESTS not set")

OFFERING_UUID = "e2ef0000000000000000000000000009"
COMPONENT = "cpu"
UNIT_FACTOR = 60  # Waldur Hours -> SLURM CPU-minutes
COMPONENT_LIMIT = 100
CHANGED_SETTING = ("qos_downscaled", "e2e_cfg_limited")

_state: dict = {}


def _agent_configuration(slurm_bin_path: str) -> dict:
    """The agent configuration the test writes to Waldur."""
    return {
        "order_processing_backend": "slurm",
        "membership_sync_backend": "slurm",
        "reporting_backend": "slurm",
        "backend_settings": {
            "slurm_bin_path": slurm_bin_path,
            "default_account": "root",
            # Distinct from the other suites' prefixes: their accounts live in the
            # same emulator.
            "customer_prefix": "e2e_cfgc_",
            "project_prefix": "e2e_cfgp_",
            "allocation_prefix": "e2e_cfga_",
            "qos_downscaled": "limited",
            "qos_paused": "paused",
            "qos_default": "normal",
            "enable_user_homedir_account_creation": False,
            "check_backend_id_uniqueness": True,
        },
        "event_settings": {"stomp_enabled": False},
    }


def _offering_path(action: str = "") -> str:
    return f"/api/marketplace-provider-offerings/{OFFERING_UUID}/{action}"


def _write_agent_configuration(client, configuration: dict) -> None:
    response = client.get_httpx_client().post(
        _offering_path("update_integration/"), json={"agent_configuration": configuration}
    )
    assert response.status_code in (200, 201), (
        f"update_integration answered {response.status_code}: {response.text}"
    )


def _served(client) -> dict:
    response = client.get_httpx_client().get(_offering_path("site_agent_config/"))
    assert response.status_code == 200, (
        f"site_agent_config answered {response.status_code}: {response.text}"
    )
    return response.json()


def _load(bootstrap_file: Path):
    configuration = load_configuration(str(bootstrap_file), user_agent_suffix="e2e-config-from-waldur")
    assert len(configuration.offerings) == 1
    return configuration.offerings[0]


@pytest.fixture(scope="module")
def admin_client(offering):
    """Client with the staff token of the shared E2E configuration."""
    return get_client(offering.waldur_api_url, offering.waldur_api_token)


@pytest.fixture(scope="module")
def written_configuration(offering, admin_client):
    """Write the offering's agent configuration and component factor to Waldur."""
    configuration = _agent_configuration(offering.backend_settings.get("slurm_bin_path", ".venv/bin"))
    httpx_client = admin_client.get_httpx_client()

    details = httpx_client.get(_offering_path())
    details.raise_for_status()
    components = {c["type"]: c for c in details.json().get("components", [])}
    assert COMPONENT in components, f"offering {OFFERING_UUID} has no {COMPONENT} component"
    response = httpx_client.post(
        _offering_path("update_offering_component/"),
        json={
            "uuid": components[COMPONENT]["uuid"],
            "unit_factor": UNIT_FACTOR,
            "limit_amount": COMPONENT_LIMIT,
        },
    )
    assert response.status_code in (200, 201), (
        f"update_offering_component answered {response.status_code}: {response.text}"
    )

    _write_agent_configuration(admin_client, configuration)
    yield configuration
    # Leave the offering as the next run expects it, whatever step 5 changed.
    _write_agent_configuration(admin_client, configuration)


@pytest.fixture(scope="module")
def bootstrap_file(offering, tmp_path_factory) -> Path:
    """An agent configuration file holding only what is needed to reach Waldur."""
    path = tmp_path_factory.mktemp("config-from-waldur") / "bootstrap.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "timezone": "UTC",
                "offerings": [
                    {
                        "configuration_source": "waldur",
                        "waldur_api_url": offering.waldur_api_url,
                        "waldur_api_token": offering.waldur_api_token,
                        "waldur_offering_uuid": OFFERING_UUID,
                    }
                ],
            }
        )
    )
    return path


class TestConfigurationFromWaldur:
    """The five steps of the acceptance criterion, in order."""

    def test_01_bootstrap_file_loads_what_waldur_serves(
        self, written_configuration, admin_client, bootstrap_file
    ):
        loaded = _load(bootstrap_file)
        served = _served(admin_client)
        data = served["offering"]

        assert loaded.configuration_source == "waldur"
        assert loaded.waldur_offering_uuid == OFFERING_UUID
        assert loaded.configuration_version == served["version"]
        assert loaded.name == data["name"]
        for role in (
            "backend_type",
            "order_processing_backend",
            "membership_sync_backend",
            "reporting_backend",
        ):
            assert getattr(loaded, role) == data[role] == "slurm", role
        assert loaded.stomp_enabled is False
        # No secret is configured here, so the settings are exactly Waldur's.
        assert loaded.backend_settings == data["backend_settings"]
        for key, value in written_configuration["backend_settings"].items():
            assert loaded.backend_settings[key] == value, key
        assert loaded.backend_components_dict == data["backend_components"]
        assert loaded.policy_settings == data.get("policy_settings")

        component = loaded.backend_components[COMPONENT]
        assert component.unit_factor == UNIT_FACTOR
        assert component.limit == COMPONENT_LIMIT
        assert component.accounting_type.value == "usage"

        _state["offering"] = loaded
        _state["version"] = served["version"]

    def test_02_order_sets_limit_times_factor(
        self, admin_client, project_uuid, _emulator_cleanup
    ):
        loaded = _state.get("offering")
        if loaded is None:
            pytest.skip("the configuration did not load in test_01")
        backend = SlurmBackend(loaded.backend_settings, loaded.backend_components_dict)
        _state["backend"] = backend

        offering_url, plan_url = get_offering_info(admin_client, OFFERING_UUID)
        order_uuid = create_source_order(
            admin_client,
            offering_url,
            get_project_url(admin_client, project_uuid),
            plan_url,
            limits={},
            name=f"e2e-cfg-{uuid.uuid4().hex[:6]}",
        )
        final_state = run_processor_until_order_terminal(
            loaded, admin_client, backend, order_uuid
        )
        assert final_state == OrderState.DONE

        order = marketplace_orders_retrieve.sync(client=admin_client, uuid=order_uuid)
        resource_uuid = order.marketplace_resource_uuid.hex
        resource = marketplace_provider_resources_retrieve.sync(
            client=admin_client, uuid=resource_uuid
        )
        backend_id = resource.backend_id
        assert backend_id and not isinstance(backend_id, type(UNSET))
        _state["resource_uuid"] = resource_uuid
        _state["backend_id"] = backend_id

        limits = backend.client.get_resource_limits(backend_id)
        assert limits.get(COMPONENT) == COMPONENT_LIMIT * UNIT_FACTOR, limits

    def test_03_reported_usage_is_backend_usage_over_factor(self, admin_client):
        loaded, backend = _state.get("offering"), _state.get("backend")
        resource_uuid, backend_id = _state.get("resource_uuid"), _state.get("backend_id")
        if not resource_uuid:
            pytest.skip("no resource was created in test_02")

        from emulator.core.database import SlurmDatabase  # noqa: PLC0415
        from emulator.core.time_engine import TimeEngine  # noqa: PLC0415
        from emulator.core.usage_simulator import UsageSimulator  # noqa: PLC0415

        # A run ending now, kept inside the current month (sacct --truncate).
        now = datetime.now().replace(microsecond=0)
        hours_into_month = (now - now.replace(day=1, hour=0, minute=0, second=0)).total_seconds() / 3600
        node_hours = min(0.5, round(hours_into_month / 2, 4))
        database = SlurmDatabase()
        database.load_state()
        UsageSimulator(TimeEngine(), database).inject_usage(
            backend_id, "e2euser1", node_hours, at_time=now
        )

        lines = backend.client.get_usage_report([backend_id], timezone="UTC")
        backend_usage = sum(line.tres_usage.get(COMPONENT, 0) for line in lines)
        assert backend_usage > 0, lines

        processor = OfferingReportProcessor(
            loaded, admin_client, timezone="UTC", resource_backend=backend
        )
        processor._process_resource_with_retries(
            marketplace_provider_resources_retrieve.sync(client=admin_client, uuid=resource_uuid),
            marketplace_provider_offerings_retrieve.sync(client=admin_client, uuid=OFFERING_UUID),
        )

        response = admin_client.get_httpx_client().get(
            "/api/marketplace-component-usages/", params={"resource_uuid": resource_uuid}
        )
        response.raise_for_status()
        period = now.strftime("%Y-%m")
        recorded = [
            float(usage["usage"])
            for usage in response.json()
            if usage.get("type") == COMPONENT and str(usage.get("billing_period", "")).startswith(period)
        ]
        assert recorded, response.json()
        expected = backend_usage / UNIT_FACTOR
        assert abs(recorded[0] - expected) < 0.01, (recorded, backend_usage)

    def test_04_setting_changed_in_waldur_applies_on_the_next_load(
        self, written_configuration, admin_client, bootstrap_file
    ):
        key, value = CHANGED_SETTING
        changed = copy.deepcopy(written_configuration)
        assert changed["backend_settings"][key] != value
        changed["backend_settings"][key] = value
        _write_agent_configuration(admin_client, changed)

        loaded = _load(bootstrap_file)
        assert loaded.backend_settings[key] == value
        assert loaded.configuration_version
        if "version" in _state:
            assert loaded.configuration_version != _state["version"]
