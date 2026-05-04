"""Integration test for the backend's full pull_resource → CR → operator path.

Mocks the Waldur SDK responses (ResourceProjects + UserRoles) but uses
the REAL CrdClient against a real Kubernetes cluster (the operator
running in that cluster does the actual reconciliation against
Rancher + Keycloak).

Skipped unless K8S_CRD_TEST=1 — the test needs a working kubeconfig
pointing at a cluster that has the ManagedRancherProject CRD installed
and a running rancher-keycloak-operator. See
``rancher-keycloak-operator/docs/tier-1-runbook.md`` for setup.

The companion operator test
``rancher-keycloak-operator/tests/test_integration.py::TestResourceQuota``
verifies the operator side; this test verifies the SITE-AGENT side
of the same chain — namely that ResourceProject.limits modifications
made via Waldur (here mocked) propagate through the plugin into
updated CR specs.
"""

import contextlib
import os
import time
import uuid as uuid_lib
from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent_rancher_kc_crd.backend import RancherKcCrdBackend

pytestmark = pytest.mark.skipif(
    os.environ.get("K8S_CRD_TEST") != "1",
    reason="set K8S_CRD_TEST=1 to run against a real K8s cluster",
)


# --- helpers ---


def _make_resource_project(uid: str, name: str, limits: dict) -> MagicMock:
    rp = MagicMock()
    rp.uuid = uuid_lib.UUID(uid)
    rp.name = name
    rp.description = f"test rp {name}"
    # Mimic the SDK's ResourceProject.limits attribute (has to_dict).
    rp.limits = MagicMock()
    rp.limits.to_dict.return_value = limits
    return rp


def _make_resource(uid: str, slug: str) -> MagicMock:
    r = MagicMock()
    r.uuid = uuid_lib.UUID(uid)
    r.slug = slug
    return r


def _make_settings() -> dict:
    return {
        "cluster_id": os.environ.get("RANCHER_CLUSTER_ID", "c-m-test"),
        "namespace": os.environ.get("CRD_NAMESPACE", "waldur-system"),
        "kubeconfig_path": os.environ.get("KUBECONFIG"),
        "context": os.environ.get("KUBE_CONTEXT"),
        # Keycloak group naming irrelevant for quota-only tests.
        "parent_group_name": "rkc-plugin-test",
        "group_name_template": "rkc-plugin-test-${role_name}",
        "role_map": {},
        # No waldur_api_url/token — we mock the SDK calls below.
    }


def _wait_for_quota(backend, cr_name: str, timeout: int = 30) -> dict:
    """Poll the CR until the operator marks it Ready (or timeout)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cr = backend.crd.get(cr_name)
        if cr and cr.get("status", {}).get("phase") == "Ready":
            return cr
        time.sleep(2)
    return backend.crd.get(cr_name)


def _wait_for_gone(backend, cr_name: str, timeout: int = 30) -> bool:
    """Poll until a CR is either fully gone or unambiguously being deleted.

    The operator's kopf delete handler runs async — between
    ``self.crd.delete(name)`` and ``self.crd.get(name) is None`` there's
    a finalizer-removal window where the CR is still returned with
    ``metadata.deletionTimestamp`` set. Treat that state as "gone for
    our purposes" so the test isn't tied to the operator's cleanup
    latency.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cr = backend.crd.get(cr_name)
        if cr is None:
            return True
        if (cr.get("metadata") or {}).get("deletionTimestamp"):
            return True
        time.sleep(1)
    return False


# --- the tests ---


class TestPullResourceQuotaPropagation:
    """Full chain: mocked Waldur RP limits → CR → operator → namespace quota."""

    def setup_method(self, method) -> None:  # noqa: ARG002
        self.backend = RancherKcCrdBackend(_make_settings(), {})
        # Stub a configured client so pull_resource doesn't early-return.
        self.backend.waldur_client = MagicMock()
        # Fresh UUIDs per test so the operator's finalizer-based
        # cleanup of the previous CR doesn't race with this one's
        # apply().
        slug = f"rkc-plugin-{uuid_lib.uuid4().hex[:8]}"
        resource_uuid_str = str(uuid_lib.uuid4())
        rp_uuid_str = str(uuid_lib.uuid4())
        self.resource = _make_resource(resource_uuid_str, slug)
        self.rp_uuid = rp_uuid_str
        rp_prefix = rp_uuid_str.replace("-", "")[:8]
        self.cr_name = f"{slug}-{rp_prefix}"

    def teardown_method(self, method) -> None:  # noqa: ARG002
        with contextlib.suppress(Exception):
            self.backend.crd.delete(self.cr_name)

    def test_initial_limits_propagate(self):
        """First pull_resource creates a CR with the ResourceProject's limits."""
        rp = _make_resource_project(
            self.rp_uuid, "Plugin Test RP", {"cpu": 500, "memory": "256Mi"}
        )
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        cr = _wait_for_quota(self.backend, self.cr_name, timeout=30)
        assert cr is not None
        assert cr["spec"]["resourceQuotas"] == {"cpu": 500, "memory": "256Mi"}

    def test_limits_modification_propagates(self):
        """Reconcile A → reconcile B with different limits → CR updated."""
        rp_a = _make_resource_project(self.rp_uuid, "Plugin Test RP", {"cpu": 500})
        rp_b = _make_resource_project(
            self.rp_uuid, "Plugin Test RP", {"cpu": 1500, "memory": "512Mi"}
        )

        # Round 1: limits = {cpu: 500}
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_a]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        cr_a = self.backend.crd.get(self.cr_name)
        assert cr_a is not None
        assert cr_a["spec"]["resourceQuotas"] == {"cpu": 500}

        # Round 2: admin bumps the limits in Waldur → cpu doubled, memory added
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_b]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        cr_b = self.backend.crd.get(self.cr_name)
        assert cr_b["spec"]["resourceQuotas"] == {"cpu": 1500, "memory": "512Mi"}

    def test_limits_removal_propagates(self):
        """Removing a limit from Waldur removes it from the CR spec.

        Pairs with the operator-side test
        TestResourceQuota::test_quota_remove_key_propagates which proves
        the same removal lands in the downstream namespace's
        ResourceQuota.spec.hard.
        """
        rp_a = _make_resource_project(
            self.rp_uuid, "Plugin Test RP", {"cpu": 500, "memory": "256Mi"}
        )
        rp_b = _make_resource_project(self.rp_uuid, "Plugin Test RP", {"cpu": 500})

        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_a]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_b]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        cr = self.backend.crd.get(self.cr_name)
        assert cr["spec"]["resourceQuotas"] == {"cpu": 500}
        assert "memory" not in cr["spec"]["resourceQuotas"]


class TestPullResourceWithoutWaldurClient:
    """When waldur_api_* settings are missing, pull_resource is a no-op."""

    def test_returns_none_without_client(self):
        backend = RancherKcCrdBackend(_make_settings(), {})
        # Default constructor leaves waldur_client = None.
        assert backend.waldur_client is None

        resource = _make_resource(
            "11111111-1111-1111-1111-111111111111", "noop-test"
        )
        result = backend.pull_resource(resource)
        assert result is None


class TestOrphanPruning:
    """ResourceProject deleted in Waldur → its CR is pruned on next sync."""

    def setup_method(self, method) -> None:  # noqa: ARG002
        self.backend = RancherKcCrdBackend(_make_settings(), {})
        self.backend.waldur_client = MagicMock()
        slug = f"rkc-prune-{uuid_lib.uuid4().hex[:8]}"
        self.resource_uuid = str(uuid_lib.uuid4())
        self.resource = _make_resource(self.resource_uuid, slug)
        # Two RPs that we'll register under one resource, then drop one.
        self.rp_a_uuid = str(uuid_lib.uuid4())
        self.rp_b_uuid = str(uuid_lib.uuid4())
        self.cr_name_a = f"{slug}-{self.rp_a_uuid.replace('-', '')[:8]}"
        self.cr_name_b = f"{slug}-{self.rp_b_uuid.replace('-', '')[:8]}"

    def teardown_method(self, method) -> None:  # noqa: ARG002
        for name in (self.cr_name_a, self.cr_name_b):
            with contextlib.suppress(Exception):
                self.backend.crd.delete(name)

    def test_removed_rp_prunes_its_cr(self):
        """Sync 1 creates CRs A+B. Sync 2 (only B in Waldur) prunes A."""
        rp_a = _make_resource_project(self.rp_a_uuid, "RP A", {})
        rp_b = _make_resource_project(self.rp_b_uuid, "RP B", {})

        # Sync 1: Waldur reports both RPs.
        with (
            patch.object(
                self.backend, "_fetch_resource_projects", return_value=[rp_a, rp_b]
            ),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        assert self.backend.crd.get(self.cr_name_a) is not None
        assert self.backend.crd.get(self.cr_name_b) is not None

        # Sync 2: admin removed RP A in Waldur. Its CR should be deleted
        # (operator's kopf delete handler then runs the actual cascading
        # Rancher + Keycloak cleanup -- not asserted here).
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_b]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        assert _wait_for_gone(self.backend, self.cr_name_a, timeout=30), (
            "Orphan CR for removed RP A should have been pruned (or marked for deletion)"
        )
        cr_b = self.backend.crd.get(self.cr_name_b)
        assert cr_b is not None and not (cr_b.get("metadata") or {}).get(
            "deletionTimestamp"
        ), "CR for still-present RP B must NOT be pruned"

    def test_zero_rps_prunes_all_label_matching_crs(self):
        """If Waldur reports 0 RPs, every label-matching CR is pruned."""
        rp_a = _make_resource_project(self.rp_a_uuid, "RP A", {})

        # Sync 1: one RP -> one CR.
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[rp_a]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        assert self.backend.crd.get(self.cr_name_a) is not None

        # Sync 2: Waldur reports an empty list (resource has no RPs).
        with (
            patch.object(self.backend, "_fetch_resource_projects", return_value=[]),
            patch.object(self.backend, "_fetch_resource_project_users", return_value=[]),
        ):
            self.backend.pull_resource(self.resource)

        assert _wait_for_gone(self.backend, self.cr_name_a, timeout=30)
