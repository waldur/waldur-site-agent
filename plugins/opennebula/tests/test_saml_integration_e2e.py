"""Integration tests for OpenNebula Keycloak SAML integration.

These tests exercise the full VDC lifecycle with Keycloak group management,
SAML mapping file generation, and user membership operations against real
OpenNebula and Keycloak instances. They follow the same pattern as
``test_integration.py``.

Environment variables:
    OPENNEBULA_INTEGRATION_TESTS=true   - Gate: skip all if not set
    OPENNEBULA_API_URL                  - XML-RPC endpoint
    OPENNEBULA_CREDENTIALS              - Admin credentials
    OPENNEBULA_CLUSTER_IDS              - Comma-separated cluster IDs
    KEYCLOAK_URL                        - Keycloak base URL (e.g. https://host/keycloak/)
    KEYCLOAK_REALM                      - Target realm (e.g. opennebula)
    KEYCLOAK_ADMIN_USERNAME             - Admin username (in master realm)
    KEYCLOAK_ADMIN_PASSWORD             - Admin password
    KEYCLOAK_TEST_USERNAME              - Existing Keycloak user to test add/remove
"""

from __future__ import annotations

import logging
import os
import secrets
import tempfile

import pytest

from waldur_site_agent.backend.exceptions import BackendError

from waldur_site_agent_opennebula.backend import OpenNebulaBackend
from waldur_site_agent_opennebula.client import OpenNebulaClient

logger = logging.getLogger(__name__)

INTEGRATION_TESTS = (
    os.environ.get("OPENNEBULA_INTEGRATION_TESTS", "false").lower() == "true"
)

KEYCLOAK_CONFIGURED = bool(os.environ.get("KEYCLOAK_URL", ""))

pytestmark = pytest.mark.skipif(
    not INTEGRATION_TESTS or not KEYCLOAK_CONFIGURED,
    reason="OPENNEBULA_INTEGRATION_TESTS or KEYCLOAK_URL not set",
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
def keycloak_settings():
    return {
        "keycloak_url": os.environ.get("KEYCLOAK_URL", ""),
        "keycloak_realm": os.environ.get("KEYCLOAK_REALM", "opennebula"),
        "keycloak_user_realm": "master",
        "client_id": "admin-cli",
        "keycloak_username": os.environ.get("KEYCLOAK_ADMIN_USERNAME", "admin"),
        "keycloak_password": os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin"),
        "keycloak_ssl_verify": True,
    }


@pytest.fixture(scope="module")
def test_username():
    username = os.environ.get("KEYCLOAK_TEST_USERNAME", "")
    if not username:
        pytest.skip("KEYCLOAK_TEST_USERNAME not set")
    return username


@pytest.fixture(scope="module")
def mapping_file(tmp_path_factory):
    """Temporary SAML mapping file for this test run."""
    return str(tmp_path_factory.mktemp("saml") / "keycloak_groups.yaml")


@pytest.fixture(scope="module")
def vdc_name():
    """Unique VDC name for this test run."""
    suffix = secrets.token_hex(4)
    return f"saml_integ_{suffix}"


@pytest.fixture(scope="module")
def backend(api_url, credentials, cluster_ids, keycloak_settings, mapping_file):
    """Create an OpenNebulaBackend with Keycloak enabled."""
    if not api_url or not credentials:
        pytest.skip("OPENNEBULA_API_URL or OPENNEBULA_CREDENTIALS not set")

    settings = {
        "api_url": api_url,
        "credentials": credentials,
        "zone_id": 0,
        "cluster_ids": cluster_ids,
        "resource_type": "vdc",
        "keycloak_enabled": True,
        "keycloak": keycloak_settings,
        "saml_mapping_file": mapping_file,
    }
    components = {
        "cpu": {"limit": 100, "measured_unit": "cores", "unit_factor": 1},
        "ram": {"limit": 4096, "measured_unit": "MB", "unit_factor": 1},
        "storage": {"limit": 10240, "measured_unit": "MB", "unit_factor": 1},
    }
    return OpenNebulaBackend(settings, components)


@pytest.fixture(scope="module")
def client(backend):
    return backend.client


@pytest.fixture(scope="module")
def vdc_state():
    """Module-scoped mutable state shared between ordered tests."""
    return {}


def _make_mock_resource(backend_id, name="Test VDC"):
    """Build a minimal mock WaldurResource."""
    from unittest.mock import MagicMock

    from waldur_api_client.models.resource import Resource as WaldurResource

    resource = MagicMock(spec=WaldurResource)
    resource.backend_id = backend_id
    resource.name = name
    resource.offering_plugin_options = {}
    resource.attributes = {}
    resource.limits = {}
    return resource


# ---------------------------------------------------------------------------
# Tests — ordered lifecycle
# ---------------------------------------------------------------------------


class TestSAMLVDCLifecycle:
    """Full VDC lifecycle with Keycloak SAML integration.

    create VDC → verify KC groups → verify ONE groups → add user →
    verify user → remove user → delete VDC → verify cleanup
    """

    def test_01_connectivity(self, backend):
        """Both OpenNebula and Keycloak are reachable."""
        assert backend.ping() is True

    def test_02_keycloak_client_initialized(self, backend):
        """Keycloak client was successfully created."""
        assert backend.keycloak_client is not None

    def test_03_create_vdc_with_keycloak(self, backend, client, vdc_name, vdc_state):
        """Create VDC — also creates Keycloak groups and ONE SAML groups."""
        resource = _make_mock_resource(vdc_name, f"SAML Test VDC {vdc_name}")
        backend._current_waldur_resource = resource

        result = backend._create_vdc_resource(vdc_name, "SAML Test", "test-org")
        assert result is True
        vdc_state["backend_id"] = vdc_name

    def test_04_vdc_exists(self, client, vdc_name):
        """VDC and base group are visible in ONE."""
        vdc = client._get_vdc_by_name(vdc_name)
        assert vdc is not None
        group = client._get_group_by_name(vdc_name)
        assert group is not None

    def test_05_keycloak_parent_group_exists(self, backend, vdc_name, vdc_state):
        """Keycloak parent group ``vdc_{slug}`` was created."""
        parent = backend.keycloak_client.get_group_by_name(f"vdc_{vdc_name}")
        assert parent is not None
        vdc_state["kc_parent_id"] = parent["id"]
        logger.info("Keycloak parent group: %s (id=%s)", parent["name"], parent["id"])

    def test_06_keycloak_child_groups_exist(self, backend, vdc_state):
        """Child groups admin, user, cloud exist under the parent."""
        parent_id = vdc_state["kc_parent_id"]
        children = backend.keycloak_client.keycloak_admin.get_group_children(parent_id)
        child_names = {c["name"] for c in children}
        assert child_names == {"admin", "user", "cloud"}
        vdc_state["kc_children"] = {c["name"]: c["id"] for c in children}
        logger.info("Keycloak child groups: %s", vdc_state["kc_children"])

    def test_07_one_saml_groups_exist(self, client, vdc_name, vdc_state):
        """ONE groups ``{slug}-admins``, ``{slug}-users``, ``{slug}-cloud`` exist."""
        for suffix in ("admins", "users", "cloud"):
            group_name = f"{vdc_name}-{suffix}"
            group = client._get_group_by_name(group_name)
            assert group is not None, f"ONE group '{group_name}' not found"
            vdc_state[f"one_group_{suffix}"] = getattr(group, "ID")
            logger.info("ONE group '%s' ID=%d", group_name, getattr(group, "ID"))

    def test_08_one_groups_have_saml_template(self, client, vdc_name, vdc_state):
        """SAML_GROUP and FIREEDGE attributes are set on ONE groups."""
        for suffix, expected_path in (
            ("admins", f"/vdc_{vdc_name}/admin"),
            ("users", f"/vdc_{vdc_name}/user"),
            ("cloud", f"/vdc_{vdc_name}/cloud"),
        ):
            group_id = vdc_state[f"one_group_{suffix}"]
            group_info = client._get_group_info(group_id)
            # pyone returns TEMPLATE as an OrderedDict
            template = group_info.TEMPLATE
            saml_group = template.get("SAML_GROUP") if isinstance(template, dict) else getattr(template, "SAML_GROUP", None)
            assert saml_group == expected_path, (
                f"Expected SAML_GROUP='{expected_path}', got '{saml_group}' "
                f"for group {vdc_name}-{suffix}"
            )
            fireedge = template.get("FIREEDGE") if isinstance(template, dict) else getattr(template, "FIREEDGE", None)
            assert fireedge is not None, f"FIREEDGE not set on {vdc_name}-{suffix}"
            logger.info("Group %s-%s: SAML_GROUP=%s", vdc_name, suffix, saml_group)

    def test_09_one_groups_in_vdc(self, client, vdc_name, vdc_state):
        """All SAML groups are assigned to the VDC."""
        vdc = client._get_vdc_by_name(vdc_name)
        assert vdc is not None
        vdc_group_ids = set()
        if hasattr(vdc, "GROUPS") and hasattr(vdc.GROUPS, "ID"):
            ids = vdc.GROUPS.ID
            if isinstance(ids, (list, tuple)):
                vdc_group_ids = set(ids)
            else:
                vdc_group_ids = {ids}

        for suffix in ("admins", "users", "cloud"):
            group_id = vdc_state[f"one_group_{suffix}"]
            assert group_id in vdc_group_ids, (
                f"ONE group {vdc_name}-{suffix} (ID={group_id}) not in VDC groups "
                f"{vdc_group_ids}"
            )

    def test_10_saml_mapping_file_created(self, backend, vdc_name):
        """SAML mapping file contains entries for this VDC."""
        import yaml

        with open(backend.saml_mapping_file) as fh:
            mappings = yaml.safe_load(fh)

        assert isinstance(mappings, dict)
        assert f"/vdc_{vdc_name}/admin" in mappings
        assert f"/vdc_{vdc_name}/user" in mappings
        assert f"/vdc_{vdc_name}/cloud" in mappings
        logger.info("SAML mappings: %s", mappings)

    def test_11_set_quotas(self, client, vdc_name):
        """Set quotas on the base VDC group."""
        client.set_resource_limits(vdc_name, {"cpu": 4, "ram": 4096, "storage": 10240})

    def test_12_add_user_with_user_role(self, backend, vdc_name, test_username, vdc_state):
        """Add an existing Keycloak user to the VDC with 'user' role."""
        resource = _make_mock_resource(vdc_name)
        result = backend.add_user(resource, test_username, role="user")
        assert result is True

        # Verify user is in the correct Keycloak group
        user = backend.keycloak_client.find_user_by_username(test_username)
        assert user is not None
        user_groups = backend.keycloak_client.get_user_groups(user["id"])
        group_paths = [g["path"] for g in user_groups]
        expected_path = f"/vdc_{vdc_name}/user"
        assert expected_path in group_paths, (
            f"User {test_username} not in {expected_path}. Groups: {group_paths}"
        )
        vdc_state["test_user_id"] = user["id"]
        logger.info("User %s added to %s", test_username, expected_path)

    def test_13_add_user_with_admin_role(self, backend, vdc_name, test_username):
        """Adding the same user with admin role adds to admin group."""
        resource = _make_mock_resource(vdc_name)
        result = backend.add_user(resource, test_username, role="admin")
        assert result is True

        user = backend.keycloak_client.find_user_by_username(test_username)
        user_groups = backend.keycloak_client.get_user_groups(user["id"])
        group_paths = [g["path"] for g in user_groups]
        assert f"/vdc_{vdc_name}/admin" in group_paths
        logger.info("User %s also in admin group", test_username)

    def test_14_remove_user(self, backend, vdc_name, test_username, vdc_state):
        """Remove user from all VDC role groups."""
        resource = _make_mock_resource(vdc_name)
        result = backend.remove_user(resource, test_username)
        assert result is True

        # Verify user is no longer in any of this VDC's groups
        user_groups = backend.keycloak_client.get_user_groups(vdc_state["test_user_id"])
        group_paths = [g["path"] for g in user_groups]
        for role in ("admin", "user", "cloud"):
            path = f"/vdc_{vdc_name}/{role}"
            assert path not in group_paths, (
                f"User still in {path} after removal. Groups: {group_paths}"
            )
        logger.info("User %s removed from all VDC groups", test_username)

    def test_15_add_user_not_found_raises(self, backend, vdc_name):
        """Adding a non-existent user raises BackendError."""
        resource = _make_mock_resource(vdc_name)
        with pytest.raises(BackendError, match="not found"):
            backend.add_user(resource, "nonexistent_user_xyz_12345")

    def test_16_delete_vdc_with_cleanup(self, backend, vdc_name, vdc_state):
        """Deleting the VDC cleans up Keycloak groups, ONE groups, and mappings."""
        resource = _make_mock_resource(vdc_name)
        backend._pre_delete_resource(resource)
        backend.client.delete_resource(vdc_name)

    def test_17_keycloak_groups_cleaned_up(self, backend, vdc_name):
        """Keycloak parent group no longer exists."""
        parent = backend.keycloak_client.get_group_by_name(f"vdc_{vdc_name}")
        assert parent is None, f"Keycloak parent group vdc_{vdc_name} still exists"

    def test_18_one_saml_groups_cleaned_up(self, client, vdc_name):
        """ONE SAML groups no longer exist."""
        for suffix in ("admins", "users", "cloud"):
            group = client._get_group_by_name(f"{vdc_name}-{suffix}")
            assert group is None, f"ONE group {vdc_name}-{suffix} still exists"

    def test_19_saml_mapping_entries_removed(self, backend, vdc_name):
        """SAML mapping file no longer contains entries for this VDC."""
        import yaml

        with open(backend.saml_mapping_file) as fh:
            mappings = yaml.safe_load(fh)

        if mappings:
            for role in ("admin", "user", "cloud"):
                path = f"/vdc_{vdc_name}/{role}"
                assert path not in mappings, f"Mapping entry {path} still in file"

    def test_20_vdc_gone(self, client, vdc_name):
        """VDC and base group no longer exist."""
        assert client._get_vdc_by_name(vdc_name) is None
        assert client._get_group_by_name(vdc_name) is None


class TestSAMLIdempotent:
    """Verify idempotent VDC creation with Keycloak groups."""

    @pytest.fixture(autouse=True)
    def _setup_teardown(self, backend, client):
        """Ensure cleanup after each test."""
        self._vdc_names = []
        self._backend = backend
        self._client = client
        yield
        for name in self._vdc_names:
            try:
                resource = _make_mock_resource(name)
                backend._pre_delete_resource(resource)
            except Exception:
                pass
            try:
                client.delete_resource(name)
            except Exception:
                pass

    def test_create_vdc_twice_reuses_keycloak_groups(self, backend, client):
        """Creating the same VDC twice reuses existing Keycloak groups."""
        name = f"saml_idemp_{secrets.token_hex(4)}"
        self._vdc_names.append(name)

        resource = _make_mock_resource(name)
        backend._current_waldur_resource = resource

        result1 = backend._create_vdc_resource(name, "Test", "org")
        assert result1 is True

        # Second create should detect existing VDC
        result2 = backend._create_vdc_resource(name, "Test", "org")
        assert result2 is False  # Already exists

        # Keycloak groups should still be there (only one set)
        parent = backend.keycloak_client.get_group_by_name(f"vdc_{name}")
        assert parent is not None
        children = backend.keycloak_client.keycloak_admin.get_group_children(
            parent["id"]
        )
        assert len(children) == 3
