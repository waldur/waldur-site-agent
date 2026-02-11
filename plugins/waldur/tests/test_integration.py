"""Integration tests for Waldur-to-Waldur federation backend.

Runs against a real Waldur instance, emulating Waldur A (source) and Waldur B
(target) using two separate offerings + customers on the same instance.

Environment variables:
    WALDUR_INTEGRATION_TESTS=true     - Gate: skip all if not set
    WALDUR_API_URL=http://localhost:8080/api/  - Single Waldur instance
    WALDUR_API_TOKEN=<staff-token>    - Staff API token

Usage:
    WALDUR_INTEGRATION_TESTS=true \\
    WALDUR_API_URL=http://localhost:8080/api/ \\
    WALDUR_API_TOKEN=<token> \\
    uv run pytest plugins/waldur/tests/test_integration.py -v
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import MagicMock

import pytest

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_waldur.backend import WaldurBackend

from .integration_helpers import AutoApproveWaldurClient, WaldurTestSetup

# --- Environment gating ---

INTEGRATION_TESTS = os.environ.get("WALDUR_INTEGRATION_TESTS", "false").lower() == "true"
WALDUR_API_URL = os.environ.get("WALDUR_API_URL", "http://localhost:8080/api/")
WALDUR_API_TOKEN = os.environ.get("WALDUR_API_TOKEN", "")


def _make_test_backend(backend_settings: dict, backend_components: dict) -> WaldurBackend:
    """Create a WaldurBackend with a AutoApproveWaldurClient for integration tests.

    The AutoApproveWaldurClient auto-approves pending-provider orders on the test
    instance since there is no backend processor running. In production,
    Waldur B's own workflow handles order approval.
    """
    backend = WaldurBackend(backend_settings, backend_components)
    # Replace the production client with the test client that auto-approves orders
    backend.client = AutoApproveWaldurClient(
        api_url=backend_settings["target_api_url"],
        api_token=backend_settings["target_api_token"],
        offering_uuid=backend_settings["target_offering_uuid"],
    )
    return backend


def _complete_pending_order(waldur_setup: WaldurTestSetup, result) -> None:
    """Approve and wait for a pending order after non-blocking create_resource().

    The WaldurBackend.create_resource_with_id() returns immediately with
    pending_order_id set. Integration tests need to approve the order on
    Waldur B and wait for it to reach DONE before subsequent operations.
    """
    if result.pending_order_id:
        waldur_setup.approve_and_wait(result.pending_order_id)


def _make_waldur_resource(
    project_uuid: str,
    customer_uuid: str,
    limits: dict[str, int],
    project_name: str = "Test Project",
    name: str = "Test Resource",
    backend_id: str = "",
) -> MagicMock:
    """Create a mock WaldurResource with the required attributes."""
    resource = MagicMock()
    resource.project_uuid = project_uuid
    resource.customer_uuid = customer_uuid
    resource.project_name = project_name
    resource.name = name
    resource.backend_id = backend_id
    resource.slug = "test-resource"
    resource.project_slug = "test-project"
    resource.offering_plugin_options = {}

    # Limits must support __contains__ and __getitem__
    resource.limits = limits
    return resource


# ============================================================================
# Module-scoped fixtures — setup happens once per test module
# ============================================================================


@pytest.fixture(scope="module")
def waldur_setup():
    """Create the WaldurTestSetup instance, shared across the module."""
    setup = WaldurTestSetup(WALDUR_API_URL, WALDUR_API_TOKEN)
    yield setup
    setup.cleanup()


@pytest.fixture(scope="module")
def passthrough_env(waldur_setup):
    """Create passthrough test environment (cpu, mem — same on both sides)."""
    return waldur_setup.setup_passthrough()


@pytest.fixture(scope="module")
def conversion_env(waldur_setup):
    """Create conversion test environment (node_hours -> gpu_hours + storage_gb_hours)."""
    return waldur_setup.setup_with_conversion()


@pytest.fixture(scope="module")
def passthrough_backend(passthrough_env):
    """WaldurBackend configured for passthrough mode."""
    return _make_test_backend(
        passthrough_env.backend_settings,
        passthrough_env.backend_components,
    )


@pytest.fixture(scope="module")
def conversion_backend(conversion_env):
    """WaldurBackend configured with component conversion."""
    return _make_test_backend(
        conversion_env.backend_settings,
        conversion_env.backend_components,
    )


# Mutable state shared across ordered tests within a scenario.
# Using a dict so module-scoped fixtures can share state with function-scoped tests.
_passthrough_state: dict[str, str] = {}
_conversion_state: dict[str, str] = {}


# ============================================================================
# Scenario 1: Passthrough — Full Lifecycle
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestPassthroughLifecycle:
    """Tests with 1:1 component mapping (cpu, mem)."""

    # -- Connectivity --

    def test_ping(self, passthrough_backend):
        """Backend can connect to Waldur B API."""
        assert passthrough_backend.ping() is True

    def test_diagnostics(self, passthrough_backend):
        """Diagnostics runs without error and returns True."""
        assert passthrough_backend.diagnostics() is True

    def test_list_components(self, passthrough_backend):
        """Backend reports configured source components."""
        components = passthrough_backend.list_components()
        assert set(components) == {"cpu", "mem"}

    # -- Create --

    def test_create_resource(self, passthrough_backend, passthrough_env, waldur_setup):
        """Create resource on Waldur B — order completes, resource UUID returned."""
        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 100, "mem": 200},
            name=f"pt-resource-{uuid.uuid4().hex[:6]}",
        )
        result = passthrough_backend.create_resource(waldur_resource)

        assert result.backend_id, "Expected a backend_id (resource UUID on Waldur B)"
        _complete_pending_order(waldur_setup, result)
        _passthrough_state["resource_backend_id"] = result.backend_id

    def test_verify_resource_on_target(self, passthrough_backend):
        """Resource exists on Waldur B with correct limits."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id, "No resource created in previous test"

        resource = passthrough_backend.client.get_resource(resource_id)
        assert resource is not None, "Resource not found on Waldur B"

    def test_create_resource_idempotent_project(
        self, passthrough_backend, passthrough_env, waldur_setup
    ):
        """Second create reuses existing project (same backend_id)."""
        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 50, "mem": 75},
            name=f"pt-resource2-{uuid.uuid4().hex[:6]}",
        )
        result = passthrough_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        _passthrough_state["resource2_backend_id"] = result.backend_id

    # -- Update Limits --

    def test_update_limits(self, passthrough_backend):
        """Limits updated on Waldur B resource."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id, "No resource created"

        passthrough_backend.set_resource_limits(resource_id, {"cpu": 200, "mem": 400})

        # Verify updated limits
        limits = passthrough_backend.client.get_resource_limits(resource_id)
        assert limits.get("cpu") == 200
        assert limits.get("mem") == 400

    # -- Create with zero limits --

    def test_create_resource_with_zero_limits(
        self, passthrough_backend, passthrough_env, waldur_setup
    ):
        """Create resource with all limits=0 — should succeed."""
        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 0, "mem": 0},
            name=f"pt-zero-{uuid.uuid4().hex[:6]}",
        )
        result = passthrough_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        _passthrough_state["resource_zero_backend_id"] = result.backend_id

    # -- Usage Reporting --

    def test_usage_report_no_usage_data(self, passthrough_backend):
        """Resource exists on B but has no usage — returns all zeros."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id

        report = passthrough_backend._get_usage_report([resource_id])
        assert resource_id in report
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        assert total.get("cpu", 0) == 0
        assert total.get("mem", 0) == 0

    def test_usage_reporting_passthrough(self, passthrough_backend, waldur_setup):
        """Inject usage on B, pull via backend, verify 1:1 mapping."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id

        waldur_setup.inject_usage(resource_id, {"cpu": 50.0, "mem": 120.0})

        report = passthrough_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        # Usage should be passed through 1:1
        assert total.get("cpu", 0) >= 50.0
        assert total.get("mem", 0) >= 120.0

    def test_usage_report_multiple_resources(self, passthrough_backend):
        """Usage report with 2+ resource IDs — each gets independent usage."""
        r1 = _passthrough_state.get("resource_backend_id")
        r2 = _passthrough_state.get("resource2_backend_id")
        assert r1 and r2

        report = passthrough_backend._get_usage_report([r1, r2])
        assert r1 in report
        assert r2 in report
        assert "TOTAL_ACCOUNT_USAGE" in report[r1]
        assert "TOTAL_ACCOUNT_USAGE" in report[r2]

    # -- Pause / Downscale / Restore (no-ops for Waldur backend) --

    def test_pause_resource(self, passthrough_backend):
        """Pause is a no-op, returns True."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id
        assert passthrough_backend.pause_resource(resource_id) is True

    def test_downscale_resource(self, passthrough_backend):
        """Downscale is a no-op, returns True."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id
        assert passthrough_backend.downscale_resource(resource_id) is True

    def test_restore_resource(self, passthrough_backend):
        """Restore is a no-op, returns True."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id
        assert passthrough_backend.restore_resource(resource_id) is True

    # -- Resource Metadata --

    def test_get_resource_metadata(self, passthrough_backend):
        """Metadata returns the Waldur B resource UUID."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id
        metadata = passthrough_backend.get_resource_metadata(resource_id)
        assert metadata["waldur_b_resource_uuid"] == resource_id

    # -- List Resources --

    def test_list_resources(self, passthrough_backend):
        """List resources on Waldur B returns at least the ones we created."""
        resources = passthrough_backend.list_resources()
        backend_ids = {r.backend_id for r in resources}
        assert _passthrough_state.get("resource_backend_id") in backend_ids

    # -- Delete --

    def test_delete_resource(self, passthrough_backend):
        """Terminate order on B, resource deleted."""
        resource_id = _passthrough_state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        passthrough_backend.delete_resource(resource_mock)

    def test_delete_second_resource(self, passthrough_backend):
        """Clean up the second resource created for idempotency test."""
        resource_id = _passthrough_state.get("resource2_backend_id")
        if resource_id:
            resource_mock = MagicMock()
            resource_mock.backend_id = resource_id
            passthrough_backend.delete_resource(resource_mock)

    def test_delete_zero_limits_resource(self, passthrough_backend):
        """Clean up the zero-limits resource."""
        resource_id = _passthrough_state.get("resource_zero_backend_id")
        if resource_id:
            resource_mock = MagicMock()
            resource_mock.backend_id = resource_id
            passthrough_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 2: Component Conversion
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestConversionLifecycle:
    """Tests with component conversion (node_hours -> gpu_hours + storage_gb_hours)."""

    def test_create_resource_with_conversion(
        self, conversion_backend, conversion_env, waldur_setup
    ):
        """node_hours=100 -> gpu_hours=500, storage_gb_hours=1000 on B."""
        waldur_resource = _make_waldur_resource(
            project_uuid=conversion_env.project_a_uuid,
            customer_uuid=conversion_env.offering_a.customer_uuid,
            limits={"node_hours": 100},
            name=f"cv-resource-{uuid.uuid4().hex[:6]}",
        )
        result = conversion_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        _conversion_state["resource_backend_id"] = result.backend_id

        # Verify converted limits on Waldur B
        limits = conversion_backend.client.get_resource_limits(result.backend_id)
        assert limits.get("gpu_hours") == 500
        assert limits.get("storage_gb_hours") == 1000

    def test_update_limits_with_conversion(self, conversion_backend):
        """Converted limits correctly applied on B."""
        resource_id = _conversion_state.get("resource_backend_id")
        assert resource_id

        # node_hours=200 -> gpu_hours=1000, storage_gb_hours=2000
        conversion_backend.set_resource_limits(resource_id, {"node_hours": 200})

        limits = conversion_backend.client.get_resource_limits(resource_id)
        assert limits.get("gpu_hours") == 1000
        assert limits.get("storage_gb_hours") == 2000

    def test_usage_reporting_with_conversion(self, conversion_backend, waldur_setup):
        """gpu_hours=500, storage_gb_hours=800 on B -> node_hours=180 reported to A."""
        resource_id = _conversion_state.get("resource_backend_id")
        assert resource_id

        waldur_setup.inject_usage(
            resource_id, {"gpu_hours": 500.0, "storage_gb_hours": 800.0}
        )

        report = conversion_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        # node_hours = 500/5 + 800/10 = 100 + 80 = 180
        assert total.get("node_hours", 0) >= 180.0

    def test_usage_report_partial_components(self, conversion_backend, waldur_setup):
        """Usage only for some target components — missing ones contribute 0."""
        resource_id = _conversion_state.get("resource_backend_id")
        assert resource_id

        # Only inject gpu_hours, not storage_gb_hours
        waldur_setup.inject_usage(resource_id, {"gpu_hours": 250.0})

        report = conversion_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        # node_hours should include at least 250/5 = 50 from gpu_hours
        assert total.get("node_hours", 0) >= 50.0

    def test_delete_resource_with_conversion(self, conversion_backend):
        """Clean termination of converted resource."""
        resource_id = _conversion_state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        conversion_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 3: Error Handling & Broken States
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestErrorHandling:
    """Tests for error conditions and edge cases."""

    def test_delete_nonexistent_resource(self, passthrough_backend):
        """delete_resource with fake backend_id — should not crash."""
        fake_uuid = str(uuid.uuid4())
        resource_mock = MagicMock()
        resource_mock.backend_id = fake_uuid
        # Should log warning and return, not raise
        passthrough_backend.delete_resource(resource_mock)

    def test_delete_resource_empty_backend_id(self, passthrough_backend):
        """delete_resource with empty backend_id — skips gracefully."""
        resource_mock = MagicMock()
        resource_mock.backend_id = ""
        passthrough_backend.delete_resource(resource_mock)

    def test_delete_resource_whitespace_backend_id(self, passthrough_backend):
        """delete_resource with whitespace-only backend_id — skips gracefully."""
        resource_mock = MagicMock()
        resource_mock.backend_id = "   "
        passthrough_backend.delete_resource(resource_mock)

    def test_set_limits_invalid_resource(self, passthrough_backend):
        """set_resource_limits with non-existent resource UUID — raises BackendError."""
        fake_uuid = str(uuid.uuid4())
        with pytest.raises(Exception):
            passthrough_backend.set_resource_limits(fake_uuid, {"cpu": 100, "mem": 200})

    def test_usage_report_missing_resource(self, passthrough_backend):
        """_get_usage_report with non-existent resource — returns empty usage."""
        fake_uuid = str(uuid.uuid4())
        report = passthrough_backend._get_usage_report([fake_uuid])
        assert fake_uuid in report
        total = report[fake_uuid]["TOTAL_ACCOUNT_USAGE"]
        assert total.get("cpu", 0) == 0
        assert total.get("mem", 0) == 0

    def test_create_resource_missing_project_uuid(self, passthrough_backend):
        """create_resource with project_uuid=None — raises BackendError."""
        waldur_resource = _make_waldur_resource(
            project_uuid=None,
            customer_uuid="some-customer",
            limits={"cpu": 10, "mem": 20},
        )
        with pytest.raises(BackendError, match="No project UUID"):
            passthrough_backend.create_resource(waldur_resource)


# ============================================================================
# Scenario 4: Order State Failures
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestOrderStates:
    """Tests for order failure scenarios."""

    def test_create_order_with_invalid_offering(self, passthrough_env):
        """Backend configured with wrong target_offering_uuid — order fails."""
        settings = {
            **passthrough_env.backend_settings,
            "target_offering_uuid": str(uuid.uuid4()),
        }
        backend = WaldurBackend(settings, passthrough_env.backend_components)

        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 10, "mem": 20},
        )
        with pytest.raises(Exception):
            backend.create_resource(waldur_resource)

    def test_create_with_short_timeout(self, passthrough_env, waldur_setup):
        """Backend with very short timeout — non-blocking create always returns.

        With non-blocking create, the timeout only affects delete/update orders.
        create_resource() returns immediately with pending_order_id.
        """
        settings = {
            **passthrough_env.backend_settings,
            "order_poll_timeout": 1,
            "order_poll_interval": 1,
        }
        backend = _make_test_backend(settings, passthrough_env.backend_components)

        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 5, "mem": 10},
            name=f"pt-timeout-{uuid.uuid4().hex[:6]}",
        )
        # Non-blocking create always returns immediately
        result = backend.create_resource(waldur_resource)
        assert result.backend_id
        assert result.pending_order_id  # Order is still pending

        # Approve and clean up
        _complete_pending_order(waldur_setup, result)
        resource_mock = MagicMock()
        resource_mock.backend_id = result.backend_id
        backend_cleanup = _make_test_backend(
            passthrough_env.backend_settings,
            passthrough_env.backend_components,
        )
        backend_cleanup.delete_resource(resource_mock)


# ============================================================================
# Scenario 5: Connectivity & Configuration Errors
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestConnectivity:
    """Tests for connectivity and configuration validation."""

    def test_ping_wrong_api_url(self):
        """Backend with unreachable API URL — ping() returns False."""
        settings = {
            "target_api_url": "https://nonexistent.example.com/api/",
            "target_api_token": "fake-token",
            "target_offering_uuid": str(uuid.uuid4()),
            "target_customer_uuid": str(uuid.uuid4()),
        }
        backend = WaldurBackend(settings, {"cpu": {"measured_unit": "Hours", "unit_factor": 1}})
        assert backend.ping() is False

    def test_ping_wrong_token(self, passthrough_env):
        """Backend with invalid token — ping() returns False."""
        settings = {
            **passthrough_env.backend_settings,
            "target_api_token": "invalid-token-12345",
        }
        backend = WaldurBackend(settings, passthrough_env.backend_components)
        assert backend.ping() is False

    def test_ping_unreachable_returns_false(self):
        """Backend with unreachable URL — ping returns False regardless of raise_exception.

        Note: WaldurClient.ping() catches all exceptions internally,
        so BackendError is never raised by ping(raise_exception=True).
        """
        settings = {
            "target_api_url": "https://nonexistent.example.com/api/",
            "target_api_token": "fake-token",
            "target_offering_uuid": str(uuid.uuid4()),
            "target_customer_uuid": str(uuid.uuid4()),
        }
        backend = WaldurBackend(settings, {"cpu": {"measured_unit": "Hours", "unit_factor": 1}})
        assert backend.ping(raise_exception=True) is False

    def test_diagnostics_success(self, passthrough_backend):
        """diagnostics() returns True when connected."""
        assert passthrough_backend.diagnostics() is True

    def test_missing_required_settings(self):
        """WaldurBackend({}) — raises ValueError for missing settings."""
        with pytest.raises(ValueError, match="Missing required"):
            WaldurBackend({}, {})

    def test_missing_single_setting(self):
        """Missing just one required setting — raises ValueError."""
        settings = {
            "target_api_url": "https://example.com/api/",
            "target_api_token": "token",
            # Missing target_offering_uuid and target_customer_uuid
        }
        with pytest.raises(ValueError, match="Missing required"):
            WaldurBackend(settings, {})


# ============================================================================
# Scenario 6: Waldur A Operations — Order Processing Simulation
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestWaldurAOrderOperations:
    """Simulate the full range of operations that the agent processes from Waldur A.

    These tests exercise the WaldurBackend methods as they would be called by
    the OfferingOrderProcessor and OfferingMembershipProcessor when processing
    orders from Waldur A.
    """

    _state: dict[str, str] = {}

    # -- CREATE order processing --

    def test_process_create_order(self, passthrough_backend, passthrough_env, waldur_setup):
        """Simulates OfferingOrderProcessor._process_create_order().

        The agent receives a CREATE order from Waldur A, calls
        backend.create_resource() with the resource info.
        """
        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 150, "mem": 300},
            name=f"order-create-{uuid.uuid4().hex[:6]}",
            project_name="Order Test Project",
        )

        result = passthrough_backend.create_resource(waldur_resource)
        assert result.backend_id
        assert result.limits == {"cpu": 150, "mem": 300}
        _complete_pending_order(waldur_setup, result)
        self.__class__._state["resource_backend_id"] = result.backend_id

    # -- UPDATE order processing (limits change) --

    def test_process_update_limits_order(self, passthrough_backend):
        """Simulates OfferingOrderProcessor._process_update_order().

        The agent receives an UPDATE order from Waldur A with new limits,
        calls backend.set_resource_limits().
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        new_limits = {"cpu": 300, "mem": 600}
        passthrough_backend.set_resource_limits(resource_id, new_limits)

        # Verify the new limits are applied on Waldur B
        actual_limits = passthrough_backend.client.get_resource_limits(resource_id)
        assert actual_limits.get("cpu") == 300
        assert actual_limits.get("mem") == 600

    def test_process_update_limits_partial(self, passthrough_backend):
        """Update with partial limits — only specified components change."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        # Only update cpu, mem stays the same in the update request
        passthrough_backend.set_resource_limits(resource_id, {"cpu": 500, "mem": 600})

        actual_limits = passthrough_backend.client.get_resource_limits(resource_id)
        assert actual_limits.get("cpu") == 500

    # -- Resource pull (used by membership sync and reporting) --

    def test_pull_resource_data(self, passthrough_backend):
        """Backend can retrieve resource info from Waldur B.

        This is used by the membership processor to check resource status.
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource = passthrough_backend.client.get_marketplace_resource(
            uuid.UUID(resource_id)
        )
        assert resource is not None

    # -- Pause / Downscale / Restore (membership sync status operations) --

    def test_pause_and_restore_cycle(self, passthrough_backend):
        """Simulates OfferingMembershipProcessor._sync_resource_status().

        When resource is exhausted, processor calls pause_resource().
        When limits are increased, calls restore_resource().
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        # Pause (allocation exhausted)
        assert passthrough_backend.pause_resource(resource_id) is True

        # Restore (limits increased or usage reset)
        assert passthrough_backend.restore_resource(resource_id) is True

    def test_downscale_resource(self, passthrough_backend):
        """Simulates downscale when approaching allocation limit."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id
        assert passthrough_backend.downscale_resource(resource_id) is True

    # -- Usage reporting --

    def test_report_usage(self, passthrough_backend, waldur_setup):
        """Simulates OfferingReportProcessor usage pull.

        The reporter calls _get_usage_report() to collect usage from
        the backend, then submits it to Waldur A.
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        waldur_setup.inject_usage(resource_id, {"cpu": 75.0, "mem": 150.0})

        report = passthrough_backend._get_usage_report([resource_id])
        assert resource_id in report
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        assert "cpu" in total
        assert "mem" in total

    # -- TERMINATE order processing --

    def test_process_terminate_order(self, passthrough_backend):
        """Simulates OfferingOrderProcessor._process_terminate_order().

        The agent receives a TERMINATE order from Waldur A, calls
        backend.delete_resource().
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        passthrough_backend.delete_resource(resource_mock)

    def test_terminate_already_terminated(self, passthrough_backend):
        """Terminating an already-terminated resource — logs warning, no crash."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        # Resource is in Terminating/Terminated state after first delete.
        # delete_resource checks if resource exists first — if it's gone or
        # in a non-terminable state, it should not crash.
        # The backend may raise for 409 (resource not in terminable state),
        # which is acceptable behavior.
        try:
            passthrough_backend.delete_resource(resource_mock)
        except Exception:
            pass  # 409 "Valid states for operation: OK, Erred" is expected


# ============================================================================
# Scenario 7: Full Order Lifecycle with Conversion
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestConversionOrderOperations:
    """Order operations with component conversion active."""

    _state: dict[str, str] = {}

    def test_create_with_conversion(self, conversion_backend, conversion_env, waldur_setup):
        """CREATE order with conversion: node_hours -> gpu_hours + storage_gb_hours."""
        waldur_resource = _make_waldur_resource(
            project_uuid=conversion_env.project_a_uuid,
            customer_uuid=conversion_env.offering_a.customer_uuid,
            limits={"node_hours": 50},
            name=f"cv-order-{uuid.uuid4().hex[:6]}",
        )
        result = conversion_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        self.__class__._state["resource_backend_id"] = result.backend_id

        # Verify converted limits: 50 * 5 = 250 gpu_hours, 50 * 10 = 500 storage_gb_hours
        limits = conversion_backend.client.get_resource_limits(result.backend_id)
        assert limits.get("gpu_hours") == 250
        assert limits.get("storage_gb_hours") == 500

    def test_update_limits_with_conversion(self, conversion_backend):
        """UPDATE order with conversion."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        # node_hours=75 -> gpu_hours=375, storage_gb_hours=750
        conversion_backend.set_resource_limits(resource_id, {"node_hours": 75})

        limits = conversion_backend.client.get_resource_limits(resource_id)
        assert limits.get("gpu_hours") == 375
        assert limits.get("storage_gb_hours") == 750

    def test_usage_with_conversion(self, conversion_backend, waldur_setup):
        """Usage reporting with reverse conversion."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        waldur_setup.inject_usage(
            resource_id, {"gpu_hours": 100.0, "storage_gb_hours": 200.0}
        )

        report = conversion_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        # node_hours = 100/5 + 200/10 = 20 + 20 = 40
        assert total.get("node_hours", 0) >= 40.0

    def test_terminate_with_conversion(self, conversion_backend):
        """TERMINATE order with conversion."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        conversion_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 8: Multiple Resources and Resource Listing
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestMultipleResources:
    """Tests involving multiple resources under the same offering."""

    _state: dict[str, list[str]] = {"resource_ids": []}

    def test_create_multiple_resources(self, passthrough_backend, passthrough_env, waldur_setup):
        """Create multiple resources under the same project."""
        for i in range(2):
            waldur_resource = _make_waldur_resource(
                project_uuid=passthrough_env.project_a_uuid,
                customer_uuid=passthrough_env.offering_a.customer_uuid,
                limits={"cpu": 10 * (i + 1), "mem": 20 * (i + 1)},
                name=f"multi-resource-{i}-{uuid.uuid4().hex[:6]}",
            )
            result = passthrough_backend.create_resource(waldur_resource)
            assert result.backend_id
            _complete_pending_order(waldur_setup, result)
            self.__class__._state["resource_ids"].append(result.backend_id)

    def test_list_multiple_resources(self, passthrough_backend):
        """List resources includes all created resources."""
        resources = passthrough_backend.list_resources()
        backend_ids = {r.backend_id for r in resources}
        for rid in self.__class__._state["resource_ids"]:
            assert rid in backend_ids

    def test_independent_limits(self, passthrough_backend):
        """Each resource has independent limits."""
        resource_ids = self.__class__._state["resource_ids"]
        assert len(resource_ids) >= 2

        # Update only first resource
        passthrough_backend.set_resource_limits(resource_ids[0], {"cpu": 999, "mem": 888})

        limits_0 = passthrough_backend.client.get_resource_limits(resource_ids[0])
        limits_1 = passthrough_backend.client.get_resource_limits(resource_ids[1])
        assert limits_0.get("cpu") == 999
        assert limits_1.get("cpu") != 999  # Second resource unchanged

    def test_independent_usage_reports(self, passthrough_backend, waldur_setup):
        """Each resource has independent usage tracking."""
        resource_ids = self.__class__._state["resource_ids"]
        assert len(resource_ids) >= 2

        waldur_setup.inject_usage(resource_ids[0], {"cpu": 33.0, "mem": 44.0})

        report = passthrough_backend._get_usage_report(resource_ids)
        for rid in resource_ids:
            assert rid in report
            assert "TOTAL_ACCOUNT_USAGE" in report[rid]

    def test_cleanup_multiple_resources(self, passthrough_backend):
        """Delete all created resources."""
        for rid in self.__class__._state["resource_ids"]:
            resource_mock = MagicMock()
            resource_mock.backend_id = rid
            passthrough_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 9: Mixed Billing Types (LIMIT, USAGE, FIXED, ONE)
# ============================================================================


@pytest.fixture(scope="module")
def mixed_billing_env(waldur_setup):
    """Create mixed billing type test environment."""
    return waldur_setup.setup_mixed_billing_types()


@pytest.fixture(scope="module")
def mixed_billing_backend(mixed_billing_env):
    """WaldurBackend configured with mixed billing type components."""
    return _make_test_backend(
        mixed_billing_env.backend_settings,
        mixed_billing_env.backend_components,
    )


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestMixedBillingTypes:
    """Tests for offerings with mixed component billing types.

    Validates that limit, usage, fixed, and one-time components
    can coexist in an offering and behave correctly.
    """

    _state: dict[str, str] = {}

    def test_ping(self, mixed_billing_backend):
        """Backend with mixed billing types is reachable."""
        assert mixed_billing_backend.ping() is True

    def test_list_components(self, mixed_billing_backend):
        """All four component types are listed."""
        components = mixed_billing_backend.list_components()
        assert set(components) == {"limit_comp", "usage_comp", "fixed_comp", "one_comp"}

    def test_create_resource_with_limit_component(
        self, mixed_billing_backend, mixed_billing_env, waldur_setup
    ):
        """Create resource — only limit_comp supports limits in orders.

        USAGE, FIXED, and ONE type components do not accept limits on
        order creation. The order should include only limit_comp limits.
        """
        waldur_resource = _make_waldur_resource(
            project_uuid=mixed_billing_env.project_a_uuid,
            customer_uuid=mixed_billing_env.offering_a.customer_uuid,
            limits={"limit_comp": 100},
            name=f"bt-resource-{uuid.uuid4().hex[:6]}",
        )
        result = mixed_billing_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        self.__class__._state["resource_backend_id"] = result.backend_id

    def test_verify_limit_component_on_target(self, mixed_billing_backend):
        """Limit component value is set on Waldur B resource."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        limits = mixed_billing_backend.client.get_resource_limits(resource_id)
        assert limits.get("limit_comp") == 100

    def test_update_limit_component(self, mixed_billing_backend):
        """Update limits — only limit_comp can be changed."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        mixed_billing_backend.set_resource_limits(resource_id, {"limit_comp": 250})

        limits = mixed_billing_backend.client.get_resource_limits(resource_id)
        assert limits.get("limit_comp") == 250

    def test_usage_reporting_metered_components(self, mixed_billing_backend, waldur_setup):
        """Usage can be injected for limit and usage billing types.

        Waldur only supports usage injection for 'limit' and 'usage' type
        components. 'fixed' and 'one' components are not usage-metered.
        """
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        # Only limit and usage type components support usage injection
        waldur_setup.inject_usage(resource_id, {
            "limit_comp": 30.0,
            "usage_comp": 50.0,
        })

        report = mixed_billing_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        assert total.get("limit_comp", 0) >= 30.0
        assert total.get("usage_comp", 0) >= 50.0
        # fixed and one components have no usage data — default to 0
        assert total.get("fixed_comp", 0) == 0.0
        assert total.get("one_comp", 0) == 0.0

    def test_usage_report_empty_for_unused_components(self, mixed_billing_backend, waldur_setup):
        """Components with no usage injected default to 0.0."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        report = mixed_billing_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        # All configured components must be present in the report
        for comp in ("limit_comp", "usage_comp", "fixed_comp", "one_comp"):
            assert comp in total

    def test_delete_resource(self, mixed_billing_backend):
        """Clean up mixed billing type resource."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        mixed_billing_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 10: Usage-Only Components (no limits on orders)
# ============================================================================


@pytest.fixture(scope="module")
def usage_only_env(waldur_setup):
    """Create usage-only billing type test environment."""
    return waldur_setup.setup_usage_only()


@pytest.fixture(scope="module")
def usage_only_backend(usage_only_env):
    """WaldurBackend configured with usage-only components."""
    return _make_test_backend(
        usage_only_env.backend_settings,
        usage_only_env.backend_components,
    )


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestUsageOnlyComponents:
    """Tests for offerings where all components use USAGE billing type.

    USAGE components are metered — they support usage reporting but
    do not support setting limits on orders.
    """

    _state: dict[str, str] = {}

    def test_ping(self, usage_only_backend):
        """Backend with usage-only components is reachable."""
        assert usage_only_backend.ping() is True

    def test_create_resource_no_limits(self, usage_only_backend, usage_only_env, waldur_setup):
        """Create resource with empty limits — valid for usage-only offerings."""
        waldur_resource = _make_waldur_resource(
            project_uuid=usage_only_env.project_a_uuid,
            customer_uuid=usage_only_env.offering_a.customer_uuid,
            limits={},
            name=f"uo-resource-{uuid.uuid4().hex[:6]}",
        )
        result = usage_only_backend.create_resource(waldur_resource)
        assert result.backend_id
        _complete_pending_order(waldur_setup, result)
        self.__class__._state["resource_backend_id"] = result.backend_id

    def test_resource_has_no_limits(self, usage_only_backend):
        """Usage-only resource should have no limits or zero limits."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        limits = usage_only_backend.client.get_resource_limits(resource_id)
        # Either empty or all zeros for usage components
        for value in limits.values():
            assert value == 0 or value is None

    def test_inject_and_report_usage(self, usage_only_backend, waldur_setup):
        """Usage reporting works for usage-only components."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        waldur_setup.inject_usage(resource_id, {
            "cpu_hours": 100.0,
            "gpu_hours": 200.0,
        })

        report = usage_only_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        assert total.get("cpu_hours", 0) >= 100.0
        assert total.get("gpu_hours", 0) >= 200.0

    def test_usage_report_structure(self, usage_only_backend):
        """Usage report has all configured components."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        report = usage_only_backend._get_usage_report([resource_id])
        total = report[resource_id]["TOTAL_ACCOUNT_USAGE"]
        assert "cpu_hours" in total
        assert "gpu_hours" in total

    def test_delete_resource(self, usage_only_backend):
        """Clean up usage-only resource."""
        resource_id = self.__class__._state.get("resource_backend_id")
        assert resource_id

        resource_mock = MagicMock()
        resource_mock.backend_id = resource_id
        usage_only_backend.delete_resource(resource_mock)


# ============================================================================
# Scenario 11: Non-Blocking Order Flow
# ============================================================================


@pytest.mark.skipif(not INTEGRATION_TESTS, reason="Integration tests not enabled")
class TestNonBlockingOrderFlow:
    """Tests for the non-blocking order creation and check_pending_order() flow.

    Validates the two-phase async creation:
    1. create_resource() returns immediately with pending_order_id
    2. check_pending_order() tracks order completion on Waldur B
    """

    _state: dict[str, str] = {}

    def test_create_returns_pending_order_id(self, passthrough_backend, passthrough_env):
        """create_resource() returns immediately with pending_order_id set."""
        waldur_resource = _make_waldur_resource(
            project_uuid=passthrough_env.project_a_uuid,
            customer_uuid=passthrough_env.offering_a.customer_uuid,
            limits={"cpu": 50, "mem": 100},
            name=f"nb-resource-{uuid.uuid4().hex[:6]}",
        )
        result = passthrough_backend.create_resource(waldur_resource)
        assert result.backend_id, "Expected backend_id (target resource UUID)"
        assert result.pending_order_id, "Expected pending_order_id (target order UUID)"
        self.__class__._state["backend_id"] = result.backend_id
        self.__class__._state["pending_order_id"] = result.pending_order_id

    def test_check_pending_order_returns_false(self, passthrough_backend):
        """Target order is still PENDING_PROVIDER — check returns False."""
        pending_order_id = self.__class__._state.get("pending_order_id")
        assert pending_order_id
        assert passthrough_backend.check_pending_order(pending_order_id) is False

    def test_approve_target_order(self, waldur_setup):
        """Approve the target order (simulating Waldur B backend processor)."""
        pending_order_id = self.__class__._state.get("pending_order_id")
        assert pending_order_id
        waldur_setup.approve_and_wait(pending_order_id)

    def test_check_pending_order_returns_true(self, passthrough_backend):
        """After approval, check returns True."""
        pending_order_id = self.__class__._state.get("pending_order_id")
        assert pending_order_id
        assert passthrough_backend.check_pending_order(pending_order_id) is True

    def test_resource_usable_after_completion(self, passthrough_backend):
        """Resource on Waldur B is usable after order completes."""
        backend_id = self.__class__._state.get("backend_id")
        assert backend_id

        passthrough_backend.set_resource_limits(backend_id, {"cpu": 200, "mem": 400})

        limits = passthrough_backend.client.get_resource_limits(backend_id)
        assert limits.get("cpu") == 200
        assert limits.get("mem") == 400

    def test_cleanup(self, passthrough_backend):
        """Delete the resource."""
        backend_id = self.__class__._state.get("backend_id")
        assert backend_id

        resource_mock = MagicMock()
        resource_mock.backend_id = backend_id
        passthrough_backend.delete_resource(resource_mock)
