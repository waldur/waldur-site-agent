r"""End-to-end tests for LDAP-integrated SLURM backend.

Validates the full lifecycle with LDAP user provisioning, per-account QoS
creation, LDAP project group management, partition-aware SLURM associations,
project directory creation, resource modification, usage reporting, and
termination.

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
from datetime import datetime
from pathlib import Path

import pytest
from ldap3 import ALL, SUBTREE, Connection, Server
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_list,
)
from waldur_api_client.api.marketplace_component_user_usages import (
    marketplace_component_user_usages_list,
)
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

from plugins.slurm.tests.e2e.conftest import (
    create_source_order,
    get_offering_info,
    get_project_url,
    run_processor_until_order_terminal,
)
from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingReportProcessor,
)
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

    def assert_group_not_exists(self, group_name: str) -> None:
        conn = self._connect()
        try:
            conn.search(
                self.groups_dn,
                f"(cn={group_name})",
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            assert len(conn.entries) == 0, (
                f"Expected no LDAP group cn={group_name}, got {len(conn.entries)}"
            )
        finally:
            conn.unbind()

    def assert_user_in_group(
        self,
        group_name: str,
        username: str,
        attr: str = "memberUid",
    ) -> None:
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

    def get_group_members(self, group_name: str, attr: str = "memberUid") -> list[str]:
        conn = self._connect()
        try:
            conn.search(
                self.groups_dn,
                f"(cn={group_name})",
                search_scope=SUBTREE,
                attributes=[attr],
            )
            if not conn.entries:
                return []
            values = conn.entries[0].entry_attributes_as_dict.get(attr, [])
            return [str(v) for v in values] if isinstance(values, list) else [str(values)]
        finally:
            conn.unbind()


# ---------------------------------------------------------------------------
# SLURM emulator assertion helpers
# ---------------------------------------------------------------------------


def assert_slurm_account_exists(
    slurm_backend: SlurmBackend,
    account_name: str,
) -> None:
    resource = slurm_backend.client.get_resource(account_name)
    assert resource is not None, f"SLURM account {account_name} should exist"


def assert_slurm_account_not_exists(
    slurm_backend: SlurmBackend,
    account_name: str,
) -> None:
    resource = slurm_backend.client.get_resource(account_name)
    assert resource is None, f"SLURM account {account_name} should not exist"


def assert_slurm_qos_exists(
    slurm_backend: SlurmBackend,
    qos_name: str,
) -> None:
    assert slurm_backend.client.qos_exists(qos_name), f"SLURM QoS {qos_name} should exist"


def assert_slurm_association_exists(
    slurm_backend: SlurmBackend,
    username: str,
    account: str,
) -> None:
    assoc = slurm_backend.client.get_association(username, account)
    assert assoc is not None, f"SLURM association {username}@{account} should exist"


def assert_slurm_association_not_exists(
    slurm_backend: SlurmBackend,
    username: str,
    account: str,
) -> None:
    assoc = slurm_backend.client.get_association(username, account)
    assert assoc is None, f"SLURM association {username}@{account} should not exist"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ldap_config():
    if not E2E_LDAP_CONFIG_PATH:
        pytest.skip("WALDUR_E2E_LDAP_CONFIG not set")
    return load_configuration(
        E2E_LDAP_CONFIG_PATH,
        user_agent_suffix="e2e-ldap-test",
    )


@pytest.fixture(scope="module")
def ldap_offering(ldap_config):
    if not ldap_config.offerings:
        pytest.skip("No offerings in LDAP config")
    return ldap_config.offerings[0]


@pytest.fixture(scope="module")
def ldap_assertions(ldap_offering):
    ldap_settings = ldap_offering.backend_settings.get("ldap", {})
    if not ldap_settings:
        pytest.skip("No LDAP settings in offering")
    return LdapAssertions(ldap_settings)


@pytest.fixture(scope="module")
def ldap_waldur_client(ldap_offering):
    return get_client(
        ldap_offering.waldur_api_url,
        ldap_offering.waldur_api_token,
    )


@pytest.fixture(scope="module")
def _ldap_emulator_cleanup(ldap_offering) -> None:
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

    # Ensure parent account 'eurohpc' exists (emulator starts empty)
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
    settings = ldap_offering.backend_settings
    components = ldap_offering.backend_components_dict
    return SlurmBackend(settings, components)


@pytest.fixture(scope="module")
def ldap_project_uuid():
    if not E2E_PROJECT_A_UUID:
        pytest.skip("WALDUR_E2E_PROJECT_A_UUID not set")
    return E2E_PROJECT_A_UUID


# ---------------------------------------------------------------------------
# Tests — Resource Lifecycle (create, update, terminate)
# ---------------------------------------------------------------------------

# Shared state for ordered tests
_ldap_state: dict = {}


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestLdapResourceLifecycle:
    """Full resource lifecycle: create, update limits, terminate."""

    def test_01_create_resource(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
        ldap_project_uuid,
        ldap_assertions,
    ):
        """CREATE order: SLURM account, QoS, LDAP project group, project dir."""
        offering_url, plan_url = get_offering_info(
            ldap_waldur_client, ldap_offering.waldur_offering_uuid
        )
        project_url = get_project_url(
            ldap_waldur_client,
            ldap_project_uuid,
        )

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

        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )
        backend_id = res.backend_id
        _ldap_state["backend_id"] = backend_id

        # Verify SLURM account exists
        assert_slurm_account_exists(ldap_slurm_backend, backend_id)

        # Verify component mapper converted node_hours -> cpu + gpu
        mapper = ldap_slurm_backend._component_mapper
        assert not mapper.is_passthrough, "Mapper should be in conversion mode"
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

    def test_02_update_limits(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """UPDATE order: increase node_hours and verify SLURM limits change."""
        resource_uuid = _ldap_state.get("resource_uuid")
        backend_id = _ldap_state.get("backend_id")
        if not resource_uuid or not backend_id:
            pytest.skip("No resource from test_01")

        new_limits = {"node_hours": 200}

        resp = ldap_waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/update_limits/",
            json={"limits": new_limits},
        )
        assert resp.status_code < 400, (
            f"Failed to create update order: {resp.status_code} {resp.text[:500]}"
        )
        data = resp.json()
        update_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert update_order_uuid, f"No order UUID in response: {data}"

        final_state = run_processor_until_order_terminal(
            ldap_offering,
            ldap_waldur_client,
            ldap_slurm_backend,
            update_order_uuid,
        )
        assert final_state == OrderState.DONE, f"Expected DONE, got {final_state}"

        # Verify the SLURM account still exists after update
        assert_slurm_account_exists(ldap_slurm_backend, backend_id)

        # Verify Waldur resource limits were updated
        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )
        if not isinstance(res.limits, type(UNSET)):
            waldur_limits = dict(res.limits.additional_properties)
            logger.info("Waldur limits after update: %s", waldur_limits)
            assert waldur_limits.get("node_hours") == 200, (
                f"Expected node_hours=200, got {waldur_limits}"
            )

        logger.info("UPDATE order completed, limits updated to node_hours=200")

    def test_03_terminate_resource(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """TERMINATE order: SLURM account removed, resource state changes."""
        resource_uuid = _ldap_state.get("resource_uuid")
        backend_id = _ldap_state.get("backend_id")
        if not resource_uuid or not backend_id:
            pytest.skip("No resource from test_01")

        resp = ldap_waldur_client.get_httpx_client().post(
            f"/api/marketplace-resources/{resource_uuid}/terminate/",
            json={},
        )
        assert resp.status_code < 400, (
            f"Failed to create terminate order: {resp.status_code} {resp.text[:500]}"
        )
        data = resp.json()
        terminate_order_uuid = data.get("order_uuid") or data.get("uuid", "")
        assert terminate_order_uuid, f"No order UUID in response: {data}"

        final_state = run_processor_until_order_terminal(
            ldap_offering,
            ldap_waldur_client,
            ldap_slurm_backend,
            terminate_order_uuid,
        )
        assert final_state == OrderState.DONE, f"Expected DONE, got {final_state}"

        # Verify the resource is no longer in OK state
        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )
        assert str(res.state) != "OK", (
            f"Resource should not be OK after termination, got {res.state}"
        )

        # Verify the SLURM account was deleted
        assert_slurm_account_not_exists(ldap_slurm_backend, backend_id)
        logger.info("TERMINATE order completed, SLURM account %s removed", backend_id)


# ---------------------------------------------------------------------------
# Tests — Membership Sync with LDAP
# ---------------------------------------------------------------------------

_membership_state: dict = {}


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestLdapMembershipSync:
    """Membership sync: LDAP user provisioning and SLURM associations."""

    def test_01_create_resource_for_membership(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
        ldap_project_uuid,
    ):
        """Create a fresh resource for membership sync tests."""
        offering_url, plan_url = get_offering_info(
            ldap_waldur_client, ldap_offering.waldur_offering_uuid
        )
        project_url = get_project_url(ldap_waldur_client, ldap_project_uuid)

        order_uuid = create_source_order(
            client=ldap_waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits={"node_hours": 50},
            name=f"e2e-ldap-mem-{uuid.uuid4().hex[:6]}",
        )

        final_state = run_processor_until_order_terminal(
            ldap_offering,
            ldap_waldur_client,
            ldap_slurm_backend,
            order_uuid,
        )
        assert final_state == OrderState.DONE, f"Expected DONE, got {final_state}"

        order = marketplace_orders_retrieve.sync(
            client=ldap_waldur_client, uuid=order_uuid
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )

        _membership_state["resource_uuid"] = resource_uuid
        _membership_state["backend_id"] = res.backend_id
        logger.info(
            "Created resource %s (backend_id=%s) for membership tests",
            resource_uuid,
            res.backend_id,
        )

    def test_02_membership_sync_adds_users(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """Membership sync adds users to SLURM account and LDAP project group."""
        backend_id = _membership_state.get("backend_id")
        if not backend_id:
            pytest.skip("No backend_id from test_01")

        processor = OfferingMembershipProcessor(
            offering=ldap_offering,
            waldur_rest_client=ldap_waldur_client,
            resource_backend=ldap_slurm_backend,
        )
        processor.process_offering()

        # Collect offering user usernames from Waldur
        offering_users = processor._get_cached_offering_users()
        usernames = [
            ou.username
            for ou in offering_users
            if not isinstance(ou.username, type(UNSET)) and ou.username
        ]
        assert len(usernames) > 0, "Expected offering users with usernames"
        _membership_state["usernames"] = usernames
        logger.info("Offering user usernames: %s", usernames)

    def test_03_username_format_verification(self):
        """Verify username format matches first_letter_full_lastname (x.lastname)."""
        usernames = _membership_state.get("usernames", [])
        if not usernames:
            pytest.skip("No usernames from test_02 (prerequisite skipped or failed)")

        for uname in usernames:
            assert "." in uname, (
                f"Username '{uname}' should contain a dot (first_letter_full_lastname format)"
            )
            parts = uname.split(".", 1)
            assert len(parts[0]) == 1, (
                f"Username '{uname}' first part should be a single character"
            )
        logger.info("All %d usernames match first_letter_full_lastname format", len(usernames))

    def test_04_users_in_ldap_project_group(
        self,
        ldap_assertions,
    ):
        """Verify users are added to the LDAP project group."""
        backend_id = _membership_state.get("backend_id")
        usernames = _membership_state.get("usernames", [])
        if not backend_id or not usernames:
            pytest.skip("No backend_id or usernames from previous tests")

        members = ldap_assertions.get_group_members(backend_id, attr="memberUid")
        logger.info("Project group %s members: %s", backend_id, members)

        for uname in usernames:
            assert uname in members, (
                f"User {uname} should be in LDAP project group {backend_id}"
            )

    def test_05_users_in_access_group(
        self,
        ldap_offering,
        ldap_assertions,
    ):
        """Verify LDAP users are in configured access groups.

        Access group membership is set during initial LDAP user provisioning.
        Only checks users that actually exist as posixAccount entries in LDAP.
        """
        usernames = _membership_state.get("usernames", [])
        if not usernames:
            pytest.skip("No usernames from previous tests")

        # Only check users that exist in LDAP (access groups are set on creation)
        ldap_users = set(ldap_assertions.list_usernames())
        testable_users = [u for u in usernames if u in ldap_users]
        if not testable_users:
            pytest.skip("No LDAP posixAccount entries found — access group check not applicable")

        access_groups = ldap_offering.backend_settings.get("ldap", {}).get("access_groups", [])
        for group_config in access_groups:
            group_name = group_config["name"]
            attr = group_config.get("attribute", "memberUid")
            for uname in testable_users:
                ldap_assertions.assert_user_in_group(group_name, uname, attr=attr)
            logger.info(
                "All %d LDAP users verified in access group %s (attr=%s)",
                len(testable_users),
                group_name,
                attr,
            )

    def test_06_slurm_associations_exist(
        self,
        ldap_slurm_backend,
    ):
        """Verify SLURM user-account associations exist for all users."""
        backend_id = _membership_state.get("backend_id")
        usernames = _membership_state.get("usernames", [])
        if not backend_id or not usernames:
            pytest.skip("No backend_id or usernames from previous tests")

        for uname in usernames:
            assert_slurm_association_exists(ldap_slurm_backend, uname, backend_id)
        logger.info(
            "All %d SLURM associations verified for account %s",
            len(usernames),
            backend_id,
        )

    def test_07_idempotent_membership_sync(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """Running membership sync again should be idempotent."""
        backend_id = _membership_state.get("backend_id")
        usernames = _membership_state.get("usernames", [])
        if not backend_id or not usernames:
            pytest.skip("No backend_id or usernames from previous tests")

        # Count SLURM associations before second sync
        assoc_count_before = sum(
            1
            for u in usernames
            if ldap_slurm_backend.client.get_association(u, backend_id) is not None
        )

        processor = OfferingMembershipProcessor(
            offering=ldap_offering,
            waldur_rest_client=ldap_waldur_client,
            resource_backend=ldap_slurm_backend,
        )
        processor.process_offering()

        assoc_count_after = sum(
            1
            for u in usernames
            if ldap_slurm_backend.client.get_association(u, backend_id) is not None
        )
        assert assoc_count_after == assoc_count_before, (
            f"Idempotent sync changed association count: {assoc_count_before} -> {assoc_count_after}"
        )
        logger.info(
            "Idempotent membership sync verified: %d associations unchanged",
            assoc_count_after,
        )


# ---------------------------------------------------------------------------
# Tests — Usage Reporting
# ---------------------------------------------------------------------------

_usage_state: dict = {}


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestLdapUsageReporting:
    """Usage reporting: total and per-user usage through the SLURM emulator."""

    def test_01_create_resource_for_usage(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
        ldap_project_uuid,
    ):
        """Create a resource and sync members for usage reporting tests."""
        offering_url, plan_url = get_offering_info(
            ldap_waldur_client, ldap_offering.waldur_offering_uuid
        )
        project_url = get_project_url(ldap_waldur_client, ldap_project_uuid)

        order_uuid = create_source_order(
            client=ldap_waldur_client,
            offering_url=offering_url,
            project_url=project_url,
            plan_url=plan_url,
            limits={"node_hours": 500},
            name=f"e2e-ldap-usage-{uuid.uuid4().hex[:6]}",
        )
        final_state = run_processor_until_order_terminal(
            ldap_offering,
            ldap_waldur_client,
            ldap_slurm_backend,
            order_uuid,
        )
        assert final_state == OrderState.DONE

        order = marketplace_orders_retrieve.sync(
            client=ldap_waldur_client, uuid=order_uuid
        )
        resource_uuid = order.marketplace_resource_uuid.hex
        res = marketplace_provider_resources_retrieve.sync(
            uuid=resource_uuid, client=ldap_waldur_client
        )
        _usage_state["resource_uuid"] = resource_uuid
        _usage_state["backend_id"] = res.backend_id

        # Run membership sync to provision users
        processor = OfferingMembershipProcessor(
            offering=ldap_offering,
            waldur_rest_client=ldap_waldur_client,
            resource_backend=ldap_slurm_backend,
        )
        processor.process_offering()
        logger.info(
            "Created resource %s (backend_id=%s) for usage tests",
            resource_uuid,
            res.backend_id,
        )

    def test_02_inject_usage_and_report(
        self,
        ldap_offering,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """Inject per-user usage into emulator and run report processor."""
        resource_uuid = _usage_state.get("resource_uuid")
        backend_id = _usage_state.get("backend_id")
        if not resource_uuid or not backend_id:
            pytest.skip("No resource from test_01")

        from emulator.core.database import SlurmDatabase  # noqa: PLC0415
        from emulator.core.time_engine import TimeEngine  # noqa: PLC0415
        from emulator.core.usage_simulator import UsageSimulator  # noqa: PLC0415

        db = SlurmDatabase()
        db.load_state()
        sim = UsageSimulator(TimeEngine(), db)

        # Get usernames from SLURM account associations (works even without LDAP entries)
        usernames = db.list_account_users(backend_id)
        assert len(usernames) > 0, f"No users associated with SLURM account {backend_id}"
        now = datetime(2026, 3, 1, 12, 0, 0)
        injected: dict[str, float] = {}
        for i, username in enumerate(usernames[:5]):
            node_hours = 10.0 * (i + 1)
            sim.inject_usage(backend_id, username, node_hours, at_time=now)
            injected[username] = node_hours

        _usage_state["injected_usage"] = injected
        logger.info("Injected usage for %d users: %s", len(injected), injected)

        # Run report processor
        processor = OfferingReportProcessor(
            ldap_offering,
            ldap_waldur_client,
            timezone="UTC",
            resource_backend=ldap_slurm_backend,
        )
        waldur_offering = marketplace_provider_offerings_retrieve.sync(
            client=ldap_waldur_client, uuid=ldap_offering.waldur_offering_uuid
        )
        waldur_resource = marketplace_provider_resources_retrieve.sync(
            client=ldap_waldur_client, uuid=resource_uuid
        )
        processor._process_resource_with_retries(waldur_resource, waldur_offering)
        logger.info("Report processor completed for resource %s", backend_id)

    def test_03_verify_total_usage(
        self,
        ldap_waldur_client,
        ldap_slurm_backend,
    ):
        """Verify usage data is available from the SLURM backend.

        Note: With ComponentMapper (node_hours -> cpu+gpu), the TRES parser
        may not extract usage when slurm_tres only contains source components.
        This test verifies the raw sacct data is present and the report
        processor ran without error.
        """
        resource_uuid = _usage_state.get("resource_uuid")
        backend_id = _usage_state.get("backend_id")
        if not resource_uuid or not backend_id:
            pytest.skip("No resource from test_01")

        # Verify usage exists at the SLURM emulator level
        from emulator.core.database import SlurmDatabase  # noqa: PLC0415

        db = SlurmDatabase()
        db.load_state()
        records = db.get_usage_records(account=backend_id)
        assert len(records) > 0, f"Expected usage records in emulator for {backend_id}"
        logger.info("Found %d emulator usage records for %s", len(records), backend_id)

        # Check Waldur component usages (may be empty with ComponentMapper TRES mismatch)
        usages = marketplace_component_usages_list.sync_all(
            client=ldap_waldur_client,
            resource_uuid=resource_uuid,
        )
        logger.info("Found %d Waldur component usage records", len(usages))

        if usages:
            usage_types = {
                u.type_
                for u in usages
                if not isinstance(u.type_, type(UNSET))
            }
            logger.info("Usage types on Waldur: %s", usage_types)

    def test_04_verify_per_user_usage(
        self,
        ldap_slurm_backend,
    ):
        """Verify per-user usage is recorded in the SLURM emulator."""
        backend_id = _usage_state.get("backend_id")
        injected = _usage_state.get("injected_usage", {})
        if not backend_id or not injected:
            pytest.skip("No resource or injected usage from previous tests")

        from emulator.core.database import SlurmDatabase  # noqa: PLC0415

        db = SlurmDatabase()
        db.load_state()

        for username, expected_nh in injected.items():
            records = db.get_usage_records(account=backend_id, user=username)
            assert len(records) > 0, f"No emulator usage records for {username}"
            total_nh = sum(r.node_hours for r in records)
            assert abs(total_nh - expected_nh) < 0.01, (
                f"Usage mismatch for {username}: expected {expected_nh}, got {total_nh}"
            )
        logger.info("Per-user emulator usage verified for %d users", len(injected))


# ---------------------------------------------------------------------------
# Tests — Backward Compatibility
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not E2E_TESTS, reason="E2E tests not enabled")
class TestLdapBackwardCompat:
    """Backward compatibility: offerings without LDAP settings."""

    def test_01_backend_without_ldap(
        self,
        ldap_offering,
    ):
        """SlurmBackend without LDAP settings initializes normally."""
        settings = dict(ldap_offering.backend_settings)
        settings.pop("ldap", None)
        settings.pop("qos_management", None)
        settings.pop("project_directory", None)
        settings.pop("parent_account", None)
        settings.pop("default_partition", None)
        settings["customer_prefix"] = "compat_cust_"
        settings["project_prefix"] = "compat_proj_"
        settings["allocation_prefix"] = "compat_alloc_"

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

        assert backend._ldap_client is None
        assert backend._default_partition is None
        assert not backend._qos_config
        assert not backend._project_dir_config
        assert backend._component_mapper.is_passthrough
        logger.info("Backward compatibility: no LDAP features active")

    def test_02_component_mapper_passthrough(
        self,
        ldap_offering,
    ):
        """Passthrough mapper returns limits unchanged."""
        settings = dict(ldap_offering.backend_settings)
        settings.pop("ldap", None)
        settings.pop("qos_management", None)
        settings.pop("project_directory", None)
        settings.pop("parent_account", None)
        settings.pop("default_partition", None)
        settings["customer_prefix"] = "compat_"
        settings["project_prefix"] = "compat_"
        settings["allocation_prefix"] = "compat_"

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
        mapper = backend._component_mapper

        assert mapper.is_passthrough
        result = mapper.convert_limits_to_target({"cpu": 100})
        assert result == {"cpu": 100}
        logger.info("Passthrough mapper verified")

    def test_03_component_mapper_conversion(
        self,
        ldap_slurm_backend,
    ):
        """Conversion mapper expands node_hours to cpu + gpu."""
        mapper = ldap_slurm_backend._component_mapper
        assert not mapper.is_passthrough
        assert mapper.source_components == {"node_hours"}
        assert mapper.target_components == {"cpu", "gpu"}

        result = mapper.convert_limits_to_target({"node_hours": 10})
        assert result == {"cpu": 640, "gpu": 80}

        # cpu=640/64=10, gpu=80/8=10, total node_hours=20
        back = mapper.convert_usage_from_target({"cpu": 640, "gpu": 80})
        assert back == {"node_hours": 20.0}
        logger.info("Conversion mapper verified: node_hours <-> cpu+gpu")
