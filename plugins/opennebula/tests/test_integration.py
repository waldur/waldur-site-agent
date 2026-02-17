"""Integration tests for OpenNebula plugin against a real OpenNebula instance.

These tests exercise the full VDC lifecycle including user creation,
quota enforcement, and VM instantiation. They require a running
OpenNebula instance and are gated by environment variables.

Environment variables:
    OPENNEBULA_INTEGRATION_TESTS=true   - Gate: skip all if not set
    OPENNEBULA_API_URL                  - XML-RPC endpoint (e.g. http://host:2633/RPC2)
    OPENNEBULA_CREDENTIALS              - Admin credentials (e.g. oneadmin:password)
    OPENNEBULA_CLUSTER_IDS              - Comma-separated cluster IDs (e.g. "0,100")
    OPENNEBULA_VM_TEMPLATE_ID           - VM template ID for instantiation tests
"""

from __future__ import annotations

import logging
import os
import secrets
import time

import pyone
import pytest

from waldur_site_agent_opennebula.client import OpenNebulaClient

logger = logging.getLogger(__name__)

INTEGRATION_TESTS = (
    os.environ.get("OPENNEBULA_INTEGRATION_TESTS", "false").lower() == "true"
)

pytestmark = pytest.mark.skipif(
    not INTEGRATION_TESTS,
    reason="OPENNEBULA_INTEGRATION_TESTS not set",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_url():
    return os.environ.get("OPENNEBULA_API_URL", "")


@pytest.fixture(scope="module")
def credentials():
    return os.environ.get("OPENNEBULA_CREDENTIALS", "")


@pytest.fixture(scope="module")
def cluster_ids():
    raw = os.environ.get("OPENNEBULA_CLUSTER_IDS", "")
    if not raw:
        return []
    return [int(x.strip()) for x in raw.split(",")]


@pytest.fixture(scope="module")
def vm_template_id():
    raw = os.environ.get("OPENNEBULA_VM_TEMPLATE_ID", "")
    if not raw:
        pytest.skip("OPENNEBULA_VM_TEMPLATE_ID not set")
    return int(raw)


@pytest.fixture(scope="module")
def client(api_url, credentials, cluster_ids):
    if not api_url or not credentials:
        pytest.skip("OPENNEBULA_API_URL or OPENNEBULA_CREDENTIALS not set")
    return OpenNebulaClient(
        api_url=api_url,
        credentials=credentials,
        cluster_ids=cluster_ids,
    )


@pytest.fixture(scope="module")
def admin_one(api_url, credentials):
    """Raw pyone admin connection for verification."""
    if not api_url or not credentials:
        pytest.skip("OPENNEBULA_API_URL or OPENNEBULA_CREDENTIALS not set")
    return pyone.OneServer(api_url, session=credentials)


@pytest.fixture(scope="module")
def vdc_name():
    """Unique VDC name for this test run."""
    suffix = secrets.token_hex(4)
    return f"test_integ_{suffix}"


@pytest.fixture(scope="module")
def vdc_state():
    """Module-scoped mutable state shared between ordered tests."""
    return {}


# ---------------------------------------------------------------------------
# Tests — ordered within class, module-scoped fixtures ensure shared state
# ---------------------------------------------------------------------------


class TestVDCLifecycle:
    """Full VDC lifecycle: create -> quotas -> user -> VM -> cleanup."""

    def test_01_connectivity(self, client):
        """Admin can connect to OpenNebula."""
        version = client.one.system.version()
        assert version  # non-empty string

    def test_02_create_vdc(self, client, vdc_name, vdc_state):
        """Create VDC with group and clusters."""
        backend_id = client.create_resource(
            name=vdc_name,
            description="Integration test VDC",
            organization="test-org",
        )
        assert backend_id == vdc_name
        vdc_state["backend_id"] = backend_id

    def test_03_vdc_exists(self, client, vdc_name):
        """VDC and group are visible in the pool."""
        vdc = client._get_vdc_by_name(vdc_name)
        assert vdc is not None
        group = client._get_group_by_name(vdc_name)
        assert group is not None

    def test_04_set_quotas(self, client, vdc_name):
        """Set quotas that allow VM instantiation."""
        client.set_resource_limits(vdc_name, {"cpu": 4, "ram": 4096, "storage": 20480})

    def test_05_verify_quotas(self, client, vdc_name):
        """Quotas are reflected in group info."""
        limits = client.get_resource_limits(vdc_name)
        assert limits["cpu"] == 4
        assert limits["ram"] == 4096
        assert limits["storage"] == 20480

    def test_06_create_user(self, client, vdc_name, vdc_state):
        """Create VDC user in the VDC group."""
        username = f"{vdc_name}_admin"
        password = secrets.token_urlsafe(16)
        vdc_state["username"] = username
        vdc_state["password"] = password

        user_id = client.create_user(username, password, vdc_name)
        assert isinstance(user_id, int)
        assert user_id > 0
        vdc_state["user_id"] = user_id

    def test_07_user_credentials_stored(self, client, vdc_state):
        """Password is stored in user TEMPLATE."""
        creds = client.get_user_credentials(vdc_state["username"])
        assert creds is not None
        assert creds["opennebula_username"] == vdc_state["username"]
        assert creds["opennebula_password"] == vdc_state["password"]

    def test_08_user_can_authenticate(self, api_url, vdc_state):
        """VDC user can authenticate via XML-RPC."""
        user_one = pyone.OneServer(
            api_url,
            session=f"{vdc_state['username']}:{vdc_state['password']}",
        )
        version = user_one.system.version()
        assert version  # non-empty string

    def test_09_user_in_correct_group(self, admin_one, vdc_name, vdc_state):
        """User's primary group matches the VDC group."""
        user_info = admin_one.user.info(vdc_state["user_id"])
        group = None
        pool = admin_one.grouppool.info()
        for g in pool.GROUP:
            if g.NAME == vdc_name:
                group = g
                break
        assert group is not None
        assert user_info.GID == group.ID

    def test_10_user_sees_datastores(self, api_url, vdc_state):
        """VDC user can see datastores (via cluster membership)."""
        user_one = pyone.OneServer(
            api_url,
            session=f"{vdc_state['username']}:{vdc_state['password']}",
        )
        dspool = user_one.datastorepool.info()
        datastores = (
            dspool.DATASTORE
            if hasattr(dspool, "DATASTORE") and dspool.DATASTORE
            else []
        )
        assert len(datastores) > 0, "VDC user should see at least one datastore"

    def test_11_instantiate_vm(self, api_url, vm_template_id, vdc_state):
        """VDC user can instantiate a VM from a shared template."""
        user_one = pyone.OneServer(
            api_url,
            session=f"{vdc_state['username']}:{vdc_state['password']}",
        )
        vm_name = f"{vdc_state['username']}_test_vm"
        vm_id = user_one.template.instantiate(vm_template_id, vm_name, False, "")
        assert isinstance(vm_id, int)
        assert vm_id > 0
        vdc_state["vm_id"] = vm_id
        logger.info("Instantiated VM %d from template %d", vm_id, vm_template_id)

    def test_12_vm_reaches_active(self, api_url, vdc_state):
        """VM reaches ACTIVE state within timeout."""
        if "vm_id" not in vdc_state:
            pytest.skip("No VM was created")

        user_one = pyone.OneServer(
            api_url,
            session=f"{vdc_state['username']}:{vdc_state['password']}",
        )
        vm_id = vdc_state["vm_id"]

        # Poll for up to 60 seconds
        deadline = time.time() + 60
        while time.time() < deadline:
            vm = user_one.vm.info(vm_id)
            # STATE 3 = ACTIVE
            if vm.STATE == 3:
                logger.info("VM %d is ACTIVE (lcm_state=%d)", vm_id, vm.LCM_STATE)
                return
            time.sleep(3)

        vm = user_one.vm.info(vm_id)
        pytest.fail(f"VM {vm_id} did not reach ACTIVE state: state={vm.STATE}")

    def test_13_usage_reported(self, client, vdc_name, vdc_state):
        """Usage report reflects the running VM."""
        if "vm_id" not in vdc_state:
            pytest.skip("No VM was created")

        results = client.get_usage_report([vdc_name])
        assert len(results) == 1
        usage = results[0]["usage"]
        # At least CPU or RAM should be non-zero with a running VM
        assert usage.get("cpu", 0) > 0 or usage.get("ram", 0) > 0, (
            f"Expected non-zero usage with running VM, got: {usage}"
        )

    def test_14_reset_password(self, client, vdc_state, api_url):
        """Password reset updates auth and TEMPLATE."""
        new_password = secrets.token_urlsafe(16)
        client.reset_user_password(vdc_state["username"], new_password)
        vdc_state["password"] = new_password

        # Verify new password works
        user_one = pyone.OneServer(
            api_url,
            session=f"{vdc_state['username']}:{new_password}",
        )
        version = user_one.system.version()
        assert version

        # Verify TEMPLATE updated
        creds = client.get_user_credentials(vdc_state["username"])
        assert creds["opennebula_password"] == new_password

    def test_15_cleanup_vm(self, admin_one, vdc_state):
        """Terminate VM before VDC deletion."""
        if "vm_id" not in vdc_state:
            pytest.skip("No VM was created")

        vm_id = vdc_state["vm_id"]
        try:
            admin_one.vm.action("terminate-hard", vm_id)
            logger.info("Terminated VM %d", vm_id)
            # Wait for VM to reach DONE state
            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    vm = admin_one.vm.info(vm_id)
                    if vm.STATE == 6:  # DONE
                        break
                except pyone.OneException:
                    break
                time.sleep(2)
        except pyone.OneException as e:
            logger.warning("Failed to terminate VM %d: %s", vm_id, e)

    def test_16_delete_user(self, client, vdc_state):
        """Delete VDC user."""
        client.delete_user(vdc_state["username"])

        # Verify user is gone
        creds = client.get_user_credentials(vdc_state["username"])
        assert creds is None

    def test_17_delete_vdc(self, client, vdc_name):
        """Delete VDC and group."""
        client.delete_resource(vdc_name)

    def test_18_vdc_gone(self, client, vdc_name):
        """VDC and group no longer exist."""
        assert client._get_vdc_by_name(vdc_name) is None
        assert client._get_group_by_name(vdc_name) is None


class TestIdempotentCreate:
    """Verify idempotent VDC and user creation."""

    @pytest.fixture(autouse=True)
    def _setup_teardown(self, client):
        """Ensure cleanup after each test."""
        self._names_to_cleanup = []
        self._users_to_cleanup = []
        yield
        for username in self._users_to_cleanup:
            try:
                client.delete_user(username)
            except Exception:
                pass
        for name in self._names_to_cleanup:
            try:
                client.delete_resource(name)
            except Exception:
                pass

    def test_create_vdc_twice(self, client):
        """Creating the same VDC twice returns the same resource."""
        name = f"test_idempotent_{secrets.token_hex(4)}"
        self._names_to_cleanup.append(name)

        result1 = client.create_resource(name, "", "")
        result2 = client.create_resource(name, "", "")
        assert result1 == result2

    def test_create_user_twice(self, client):
        """Creating the same user twice returns the same ID."""
        name = f"test_idemp_user_{secrets.token_hex(4)}"
        self._names_to_cleanup.append(name)
        client.create_resource(name, "", "")

        username = f"{name}_admin"
        self._users_to_cleanup.append(username)
        password = secrets.token_urlsafe(16)

        id1 = client.create_user(username, password, name)
        id2 = client.create_user(username, password, name)
        assert id1 == id2
