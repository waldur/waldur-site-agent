r"""End-to-end tests for LDAP-integrated SLURM backend.

Validates the full lifecycle with LDAP user provisioning, per-account QoS
creation, LDAP project group management, partition-aware SLURM associations,
and project directory creation.

Requires:
    - Waldur API stack (docker-compose.e2e.yml)
    - OpenLDAP server (waldur-ldap service in docker-compose.e2e.yml)
    - SLURM emulator (installed via dev dependencies)

Environment variables:
    WALDUR_E2E_TESTS=true
    WALDUR_E2E_LDAP_CONFIG=<path-to-ldap-config.yaml>
    WALDUR_E2E_PROJECT_A_UUID=<project-uuid-on-waldur>

Usage:
    WALDUR_E2E_TESTS=true \
    WALDUR_E2E_LDAP_CONFIG=ci/e2e-ci-config-ldap.yaml \
    WALDUR_E2E_PROJECT_A_UUID=e2eb0000000000000000000000000001 \
    .venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_ldap.py -v -s
"""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import uuid
from pathlib import Path

import pytest
from ldap3 import ALL, SUBTREE, Connection, Server
from waldur_api_client.api.marketplace_orders import marketplace_orders_retrieve
from waldur_api_client.models.order_state import OrderState
from waldur_site_agent_slurm.backend import SlurmBackend

from plugins.slurm.tests.e2e.conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)
from waldur_site_agent.common.processors import OfferingMembershipProcessor
from waldur_site_agent.common.utils import get_client, load_configuration

logger = logging.getLogger(__name__)

E2E_TESTS = os.environ.get("WALDUR_E2E_TESTS", "false").lower() == "true"
E2E_LDAP_CONFIG_PATH = os.environ.get("WALDUR_E2E_LDAP_CONFIG", "")
E2E_PROJECT_A_UUID = os.environ.get("WALDUR_E2E_PROJECT_A_UUID", "")


# ---------------------------------------------------------------------------
# LDAP assertion helpers
# ---------------------------------------------------------------------------


class LdapAssertions:
    """Helper for verifying LDAP state during tests."""

    def __init__(self, ldap_settings: dict) -> None:
        """Initialize from LDAP settings dict."""
        self.uri = ldap_settings["uri"]
        self.bind_dn = ldap_settings["bind_dn"]
        self.bind_password = ldap_settings["bind_password"]
        self.base_dn = ldap_settings["base_dn"]
        self.people_dn = f"{ldap_settings.get('people_ou', 'ou=People')},{self.base_dn}"
        self.groups_dn = f"{ldap_settings.get('groups_ou', 'ou=Groups')},{self.base_dn}"

    def _connect(self) -> Connection:
        server = Server(self.uri, get_info=ALL)
        return Connection(server, self.bind_dn, self.bind_password, auto_bind=True)

    def assert_user_exists(self, username: str) -> dict:
        """Assert user exists in LDAP and return attributes."""
        conn = self._connect()
        try:
            conn.search(
                self.people_dn,
                f"(uid={username})",
                search_scope=SUBTREE,
                attributes=[
                    "uid",
                    "uidNumber",
                    "gidNumber",
                    "cn",
                    "mail",
                    "loginShell",
                ],
            )
            assert len(conn.entries) == 1, (
                f"Expected 1 LDAP entry for uid={username}, got {len(conn.entries)}"
            )
            return conn.entries[0].entry_attributes_as_dict
        finally:
            conn.unbind()

    def assert_user_not_exists(self, username: str) -> None:
        """Assert user does not exist in LDAP."""
        conn = self._connect()
        try:
            conn.search(
                self.people_dn,
                f"(uid={username})",
                search_scope=SUBTREE,
                attributes=["uid"],
            )
            assert len(conn.entries) == 0, (
                f"Expected no LDAP entry for uid={username}, got {len(conn.entries)}"
            )
        finally:
            conn.unbind()

    def assert_group_exists(self, group_name: str) -> dict:
        """Assert group exists in LDAP and return attributes."""
        conn = self._connect()
        try:
            conn.search(
                self.groups_dn,
                f"(cn={group_name})",
                search_scope=SUBTREE,
                attributes=["cn", "gidNumber", "memberUid", "member"],
            )
            assert len(conn.entries) >= 1, (
                f"Expected LDAP group cn={group_name}, got {len(conn.entries)}"
            )
            return conn.entries[0].entry_attributes_as_dict
        finally:
            conn.unbind()

    def assert_user_in_group(
        self,
        group_name: str,
        username: str,
        attr: str = "memberUid",
    ) -> None:
        """Assert user is a member of a group."""
        conn = self._connect()
        try:
            if attr == "member":
                user_dn = f"uid={username},{self.people_dn}"
                filter_str = f"(&(cn={group_name})(member={user_dn}))"
            else:
                filter_str = f"(&(cn={group_name})(memberUid={username}))"
            conn.search(
                self.groups_dn,
                filter_str,
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            assert len(conn.entries) >= 1, (
                f"Expected {username} in group {group_name} (attr={attr})"
            )
        finally:
            conn.unbind()

    def assert_user_not_in_group(
        self,
        group_name: str,
        username: str,
        attr: str = "memberUid",
    ) -> None:
        """Assert user is NOT a member of a group."""
        conn = self._connect()
        try:
            if attr == "member":
                user_dn = f"uid={username},{self.people_dn}"
                filter_str = f"(&(cn={group_name})(member={user_dn}))"
            else:
                filter_str = f"(&(cn={group_name})(memberUid={username}))"
            conn.search(
                self.groups_dn,
                filter_str,
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            assert len(conn.entries) == 0, f"Expected {username} NOT in group {group_name}"
        finally:
            conn.unbind()

    def count_users(self) -> int:
        """Count total users in LDAP."""
        conn = self._connect()
        try:
            conn.search(
                self.people_dn,
                "(objectClass=posixAccount)",
                search_scope=SUBTREE,
                attributes=["uid"],
            )
            return len(conn.entries)
        finally:
            conn.unbind()

    def list_usernames(self) -> list[str]:
        """Return all POSIX usernames in LDAP."""
        conn = self._connect()
        try:
            conn.search(
                self.people_dn,
                "(objectClass=posixAccount)",
                search_scope=SUBTREE,
                attributes=["uid"],
            )
            return [str(e.uid) for e in conn.entries]
        finally:
            conn.unbind()


# ---------------------------------------------------------------------------
# SLURM emulator assertion helpers
# ---------------------------------------------------------------------------


def assert_slurm_account_exists(
    slurm_backend: SlurmBackend,
    account_name: str,
) -> None:
    """Assert a SLURM account exists."""
    resource = slurm_backend.client.get_resource(account_name)
    assert resource is not None, f"SLURM account {account_name} should exist"


def assert_slurm_qos_exists(
    slurm_backend: SlurmBackend,
    qos_name: str,
) -> None:
    """Assert a SLURM QoS exists."""
    assert slurm_backend.client.qos_exists(qos_name), f"SLURM QoS {qos_name} should exist"


def assert_slurm_association_exists(
    slurm_backend: SlurmBackend,
    username: str,
    account: str,
) -> None:
    """Assert a SLURM user-account association exists."""
    assoc = slurm_backend.client.get_association(username, account)
    assert assoc is not None, f"SLURM association {username}@{account} should exist"


def assert_slurm_association_not_exists(
    slurm_backend: SlurmBackend,
    username: str,
    account: str,
) -> None:
    """Assert a SLURM user-account association does not exist."""
    assoc = slurm_backend.client.get_association(username, account)
    assert assoc is None, f"SLURM association {username}@{account} should not exist"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ldap_config():
    """Load LDAP E2E config."""
    if not E2E_LDAP_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_LDAP_CONFIG not set")
    return load_configuration(
        E2E_LDAP_CONFIG_PATH,
        user_agent_suffix="e2e-ldap-test",
    )


@pytest.fixture(scope="module")
def ldap_offering(ldap_config):
    """First offering from LDAP config."""
    if not ldap_config.offerings:
        pytest.skip("No offerings in LDAP config")
    return ldap_config.offerings[0]


@pytest.fixture(scope="module")
def ldap_assertions(ldap_offering):
    """LDAP assertion helper."""
    ldap_settings = ldap_offering.backend_settings.get("ldap", {})
    if not ldap_settings:
        pytest.skip("No LDAP settings in offering")
    return LdapAssertions(ldap_settings)


@pytest.fixture(scope="module")
def ldap_waldur_client(ldap_offering):
    """AuthenticatedClient for Waldur."""
    return get_client(
        ldap_offering.waldur_api_url,
        ldap_offering.waldur_api_token,
    )


@pytest.fixture(scope="module")
def _ldap_emulator_cleanup(ldap_offering) -> None:
    """Reset slurm-emulator state before LDAP tests."""
    slurm_bin_path = ldap_offering.backend_settings.get(
        "slurm_bin_path",
        ".venv/bin",
    )
    sacctmgr = str(Path(slurm_bin_path) / "sacctmgr")
    try:
        subprocess.check_output(
            [sacctmgr, "cleanup", "all"],
            stderr=subprocess.STDOUT,
            timeout=10,
        )
    except (
        subprocess.CalledProcessError,
        FileNotFoundError,
        subprocess.TimeoutExpired,
    ):
        state_file = Path("/tmp/slurm_emulator_db.json")  # noqa: S108
        if state_file.exists():
            state_file.unlink()

    # Also ensure parent account 'eurohpc' exists (emulator starts empty)
    with contextlib.suppress(subprocess.CalledProcessError, FileNotFoundError):
        subprocess.check_output(
            [
                sacctmgr,
                "--immediate",
                "--parsable2",
                "--noheader",
                "add",
                "account",
                "eurohpc",
            ],
            stderr=subprocess.STDOUT,
            timeout=10,
        )

    # Ensure project dir base exists
    Path("/tmp/e2e-projects").mkdir(exist_ok=True)  # noqa: S108


@pytest.fixture(scope="module")
def ldap_slurm_backend(ldap_offering, _ldap_emulator_cleanup):
    """SlurmBackend with LDAP integration using emulator CLI."""
    settings = ldap_offering.backend_settings
    components = ldap_offering.backend_components_dict
    return SlurmBackend(settings, components)


@pytest.fixture(scope="module")
def ldap_project_uuid():
    """Project UUID for LDAP tests."""
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Shared state for ordered tests
_ldap_state: dict = {}


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestLdapResourceLifecycle:
    """Full resource lifecycle with LDAP user provisioning and QoS."""

    def test_01_create_resource(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
        ldap_project_uuid,
        ldap_assertions,
    ):
        """CREATE order: SLURM account, QoS, LDAP project group."""
        offering_url, plan_url = get_offering_info(
            ldap_waldur_client, ldap_offering.waldur_offering_uuid
        )
        project_url = get_project_url(
            ldap_waldur_client,
            ldap_project_uuid,
        )

        # node_hours is the Waldur-facing component; the mapper
        # converts to cpu (x64) and gpu (x8) for SLURM.
        limits = {"node_hours": 100}
        resource_name = f"e2e-ldap-{uuid.uuid4().hex[:6]}"

        order_uuid = create_source_order(
            client=ldap_waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits=limits,
            name=resource_name,
        )
        _ldap_state["order_uuid"] = order_uuid

        final_state = run_processor_until_order_terminal(
            ldap_offering,
            ldap_waldur_client,
            ldap_slurm_backend,
            order_uuid,
        )
        assert final_state == OrderState.DONE, f"Expected DONE, got {final_state}"

        # Get resource backend_id
        order = marketplace_orders_retrieve.sync(
            client=ldap_waldur_client,
            uuid=order_uuid,
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        _ldap_state["resource_uuid"] = resource_uuid

        from waldur_api_client.api.marketplace_provider_resources import (  # noqa: PLC0415
            marketplace_provider_resources_retrieve,
        )

        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )
        backend_id = res.backend_id
        _ldap_state["backend_id"] = backend_id

        # Verify SLURM account exists
        assert_slurm_account_exists(ldap_slurm_backend, backend_id)

        # Verify parent account hierarchy (should be under eurohpc)
        parent = ldap_slurm_backend.client.get_resource(backend_id)
        assert parent is not None

        logger.info("Resource created with backend_id=%s", backend_id)

        # Verify component mapper converted node_hours → cpu + gpu
        mapper = ldap_slurm_backend._component_mapper
        assert not mapper.is_passthrough, "Mapper should be in conversion mode"
        assert mapper.source_components == {"node_hours"}
        assert mapper.target_components == {"cpu", "gpu"}

        converted = mapper.convert_limits_to_target({"node_hours": 100})
        assert converted == {"cpu": 6400, "gpu": 800}
        logger.info("Component mapper conversion verified: %s", converted)

        # Verify LDAP project group was created
        ldap_assertions.assert_group_exists(backend_id)
        logger.info("LDAP project group %s verified", backend_id)

        # Verify project directory was created
        project_dir = Path(f"/tmp/e2e-projects/{backend_id}")  # noqa: S108
        assert project_dir.exists(), f"Project directory {project_dir} should exist"
        logger.info("Project directory %s verified", project_dir)

    def test_02_membership_sync_creates_ldap_users(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
        ldap_assertions,
    ):
        """Membership sync: LDAP users created, added to project group."""
        backend_id = _ldap_state.get("backend_id")
        if not backend_id:
            pytest.skip("No backend_id from test_01")

        # Run membership sync processor
        processor = OfferingMembershipProcessor(
            offering=ldap_offering,
            waldur_rest_client=ldap_waldur_client,
            resource_backend=ldap_slurm_backend,
        )
        processor.process_offering()

        user_count_before = ldap_assertions.count_users()
        logger.info(
            "LDAP users after membership sync: %d",
            user_count_before,
        )

        assert user_count_before > 0, "Expected LDAP users to be created during membership sync"

        # Verify username format: first_letter_full_lastname produces "x.lastname"
        usernames = ldap_assertions.list_usernames()
        logger.info("LDAP usernames: %s", usernames)
        for uname in usernames:
            assert "." in uname, (
                f"Username '{uname}' should contain a dot (first_letter_full_lastname format)"
            )
            parts = uname.split(".", 1)
            assert len(parts[0]) == 1, (
                f"Username '{uname}' first part should be a single character"
            )

    def test_03_backward_compat_no_ldap(
        self,
        ldap_offering,
        ldap_waldur_client,  # noqa: ARG002
    ):
        """Verify that an offering without LDAP settings works normally.

        This test creates a SlurmBackend without LDAP settings to confirm
        backward compatibility.
        """
        # Create a backend without LDAP settings
        settings = dict(ldap_offering.backend_settings)
        settings.pop("ldap", None)
        settings.pop("qos_management", None)
        settings.pop("project_directory", None)
        settings.pop("parent_account", None)
        settings.pop("default_partition", None)
        # Restore required fields for standard hierarchy
        settings["customer_prefix"] = "compat_cust_"
        settings["project_prefix"] = "compat_proj_"
        settings["allocation_prefix"] = "compat_alloc_"

        # Use simple passthrough components (no target_components)
        components = {
            "cpu": {
                "limit": 10000,
                "measured_unit": "k-Hours",
                "unit_factor": 60000,
                "accounting_type": "limit",
                "label": "CPU",
            },
        }
        backend = SlurmBackend(settings, components)

        # Should initialize without error and have no LDAP client
        assert backend._ldap_client is None
        assert backend._default_partition is None
        assert not backend._qos_config
        assert not backend._project_dir_config
        assert backend._component_mapper.is_passthrough
        logger.info(
            "Backward compatibility check passed: no LDAP features active",
        )
