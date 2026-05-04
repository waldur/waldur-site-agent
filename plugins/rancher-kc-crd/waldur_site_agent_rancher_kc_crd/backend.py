"""Backend that drives Rancher + Keycloak via ManagedRancherProject CRDs.

Membership-sync only. Plugged into the existing site-agent
membership-sync loop via the ``waldur_site_agent.backends`` entry point.
"""

import logging
import uuid as uuid_lib
from typing import Any, Optional

from kubernetes import client as k8s
from waldur_api_client.api.marketplace_provider_resource_projects import (
    marketplace_provider_resource_projects_list,
    marketplace_provider_resource_projects_list_users_list,
    marketplace_provider_resource_projects_set_state_erred,
    marketplace_provider_resource_projects_set_state_ok,
)
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_project import ResourceProject
from waldur_api_client.models.resource_project_error_message_request import (
    ResourceProjectErrorMessageRequest,
)

from waldur_site_agent.backend import backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .crd_client import CrdClient
from .status_reader import (
    build_backend_metadata,
    extract_synced_users,
    is_terminal_failure,
)
from .translator import build_cr_spec, cr_name

# CR phase strings from operator 0.3.0+ (status.phase enum).
_CR_PHASE_READY = "Ready"
_CR_PHASE_ERROR = "Error"

# Waldur ResourceProject FSM string labels (from
# marketplace_mastermind/marketplace/models.py:ResourceStates). The SDK
# returns these as the human-readable display value, not the integer.
_RP_STATES_PROGRESSING = frozenset({"Creating", "Updating", "Erred"})

logger = logging.getLogger(__name__)


class RancherKcCrdBackend(backends.BaseBackend):
    """Site-agent backend that writes ManagedRancherProject CRs.

    The loop is: every membership-sync cycle, the orchestrator calls
    ``pull_resource(waldur_resource)``. We translate that resource +
    its ResourceProjects + UserRoles into one CR per ResourceProject,
    apply them server-side, then read ``status.*`` back to populate
    ``BackendResourceInfo`` and ``backend_metadata``.

    ``add_user`` / ``remove_user`` are routed through the same apply
    path — they don't poke Keycloak directly. The operator owns the
    actual Keycloak/Rancher mutations.
    """

    def __init__(
        self,
        backend_settings: dict,
        backend_components: dict[str, dict],
    ) -> None:
        """Initialize the K8s CRD client.

        Each Waldur Resource is 1:1 with a Rancher downstream cluster
        and the translator reads ``resource.backend_id`` as the
        cluster ID at CR-build time. There is no offering-level
        ``cluster_id`` setting -- the per-resource source is the only
        path.
        """
        super().__init__(backend_settings, backend_components)
        self.backend_type = "rancher-kc-crd"

        self.namespace: str = backend_settings.get("namespace", "waldur-system")
        self.role_map: dict[str, str] = backend_settings.get("role_map", {})

        self.crd = CrdClient(
            namespace=self.namespace,
            kubeconfig_path=backend_settings.get("kubeconfig_path"),
            context=backend_settings.get("context"),
        )

        # Waldur SDK client for fetching ResourceProjects + UserRoles.
        # The base BaseBackend doesn't get a client at construction
        # time, so we build our own from backend_settings (matching
        # the pattern used by other plugins that need API access).
        # Optional: when not configured, the backend can still write
        # CRs from external callers (e.g. tests injecting via
        # apply_resource_project) but pull_resource will be a no-op.
        api_url = backend_settings.get("waldur_api_url")
        api_token = backend_settings.get("waldur_api_token")
        self.waldur_client: Optional[AuthenticatedClient] = None
        if api_url and api_token:
            # SDK paths already start with "/api/" (see e.g.
            # waldur_api_client/api/marketplace_provider_resource_projects/
            # marketplace_provider_resource_projects_list.py:41), so the
            # base_url must be the host root without "/api". Users
            # configure the canonical "https://host/api/" URL; strip the
            # trailing "/api" precisely. (The core helper in
            # waldur_site_agent/common/utils.py:206 uses .rstrip("/api")
            # which strips the *character set* — same end result for
            # normal hosts, but a footgun for a host ending in any of
            # /, a, p, i.)
            self.waldur_client = AuthenticatedClient(
                base_url=api_url.rstrip("/").removesuffix("/api"),
                token=api_token,
                prefix="Token",
                verify_ssl=backend_settings.get("waldur_verify_ssl", True),
            )

        logger.info(
            "rancher-kc-crd backend initialized: namespace=%s "
            "role_map_keys=%s waldur_client=%s "
            "(cluster_id comes from each resource's backend_id)",
            self.namespace,
            sorted(self.role_map),
            "configured" if self.waldur_client else "not configured",
        )

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Hit the API server to confirm kubeconfig + RBAC are good."""
        try:
            k8s.VersionApi().get_code()
        except Exception as exc:
            logger.warning("rancher-kc-crd ping failed: %s", exc)
            if raise_exception:
                raise BackendError(str(exc)) from exc
            return False
        return True

    def diagnostics(self) -> bool:
        """Return True if the backend can reach the K8s API server."""
        return self.ping()

    def list_components(self) -> list[str]:
        """Return component names — quotas are per-RP, no offering-level components."""
        return []

    # Stubs for BaseBackend's abstract methods that this plugin doesn't
    # use (membership-only mode — order processing happens elsewhere).

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:  # noqa: ARG002
        """No usage reporting — the operator owns the cluster."""
        return {}

    def _collect_resource_limits(
        self,
        waldur_resource: WaldurResource,  # noqa: ARG002
    ) -> tuple[dict[str, int], dict[str, int]]:
        """No order-level limit collection in membership-only mode."""
        return {}, {}

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """No-op: cluster is provisioned externally, not by this plugin."""

    # ------------------------------------------------------------------
    # Resource lifecycle (membership-only — no provisioning here)
    # ------------------------------------------------------------------

    def pull_resource(
        self,
        waldur_resource: WaldurResource,
    ) -> Optional[BackendResourceInfo]:
        """Reconcile every ResourceProject of this resource into a CR.

        Sequence per cycle:
          1. Fetch ResourceProjects for this Resource via the SDK.
          2. For each RP, fetch its UserRoles.
          3. Translate (resource, RP, users) → ManagedRancherProject CR.
          4. Server-side apply the CR.
          5. Read CR status back; aggregate ``syncedMembers`` into
             ``BackendResourceInfo.users``.

        Returns ``None`` if no Waldur client is configured (treated as
        "missing in backend" by the membership processor, which is
        intentional — the loop has nothing to do without access to
        the source of truth).
        """
        if self.waldur_client is None:
            logger.warning(
                "rancher-kc-crd: waldur_client not configured; "
                "pull_resource is a no-op for %s",
                waldur_resource.uuid,
            )
            return None

        resource_dict = self._waldur_resource_to_dict(waldur_resource)
        rps = self._fetch_resource_projects(waldur_resource.uuid)
        synced_users: set[str] = set()
        expected_cr_names: set[str] = set()

        for rp in rps:
            user_roles = self._fetch_resource_project_users(rp.uuid)
            rp_dict = self._resource_project_to_dict(rp)
            ur_dicts = [self._user_role_to_dict(u) for u in user_roles]

            body = build_cr_spec(
                resource=resource_dict,
                resource_project=rp_dict,
                user_roles=ur_dicts,
                backend_settings=self.backend_settings,
            )
            self.crd.apply(body)
            expected_cr_names.add(body["metadata"]["name"])

            cr = self.crd.get(body["metadata"]["name"]) or {}
            cr_status = cr.get("status") or {}
            synced_users.update(extract_synced_users(cr_status))
            self._sync_rp_state_from_cr(rp, cr_status)

        # Prune CRs whose backing ResourceProject no longer exists in
        # Waldur. List by `waldur.io/resource-uuid` label (set by the
        # translator) so we never touch CRs we don't own; intersect with
        # the names we just applied to find the strict set of orphans.
        # This is the only place CRs get deleted on the membership-sync
        # path -- the operator's own kopf delete handler then runs the
        # cascading Rancher + Keycloak cleanup.
        for cr in self.crd.list_for_resource(resource_dict["uuid"]):
            name = (cr.get("metadata") or {}).get("name")
            if not name or name in expected_cr_names:
                continue
            logger.info(
                "Pruning orphan CR %s (resource_uuid=%s — RP no longer in Waldur)",
                name,
                resource_dict["uuid"],
            )
            try:
                self.crd.delete(name)
            except Exception as exc:
                logger.warning("Failed to delete orphan CR %s: %s", name, exc)

        return BackendResourceInfo(users=sorted(synced_users), usage={})

    # ------------------------------------------------------------------
    # SDK helpers
    # ------------------------------------------------------------------

    def _fetch_resource_projects(self, resource_uuid: uuid_lib.UUID) -> list:
        """All ResourceProjects belonging to a given Resource."""
        return (
            marketplace_provider_resource_projects_list.sync(
                client=self.waldur_client,
                resource_uuid=resource_uuid,
            )
            or []
        )

    def _fetch_resource_project_users(self, rp_uuid: uuid_lib.UUID) -> list:
        """UserRoles assigned on a given ResourceProject."""
        return (
            marketplace_provider_resource_projects_list_users_list.sync(
                client=self.waldur_client,
                uuid=rp_uuid,
            )
            or []
        )

    # ------------------------------------------------------------------
    # CR phase -> RP state transitions
    # ------------------------------------------------------------------

    def _sync_rp_state_from_cr(self, rp: ResourceProject, cr_status: dict) -> None:
        """Drive Waldur RP.state from operator CR phase.

        - CR phase=Ready and RP currently Creating/Updating/Erred ->
          set_state_ok (lets the homeport UI flip from "Creating" to
          green, and lets a previously-failed RP recover automatically
          once the operator catches up).
        - CR phase=Error -> set_state_erred with aggregated condition
          messages so the failure surfaces on the resource detail page.
        - Anything else (in-flight, terminating, no CR yet) -> leave
          the FSM alone; the next pull cycle will retry.
        """
        phase = cr_status.get("phase")
        if phase == _CR_PHASE_READY:
            if str(rp.state) in _RP_STATES_PROGRESSING:
                self._call_set_rp_state_ok(rp)
        elif phase == _CR_PHASE_ERROR:
            message = self._collect_error_messages(cr_status)
            self._call_set_rp_state_erred(rp, message)

    def _call_set_rp_state_ok(self, rp: ResourceProject) -> None:
        try:
            marketplace_provider_resource_projects_set_state_ok.sync(
                uuid=rp.uuid,
                client=self.waldur_client,
            )
            logger.info(
                "ResourceProject %s -> OK (cr.status.phase=Ready)", rp.uuid.hex
            )
        except Exception as exc:
            logger.warning(
                "Failed to set ResourceProject %s state to OK: %s", rp.uuid.hex, exc
            )

    def _call_set_rp_state_erred(self, rp: ResourceProject, error_message: str) -> None:
        body = ResourceProjectErrorMessageRequest(error_message=error_message)
        try:
            marketplace_provider_resource_projects_set_state_erred.sync(
                uuid=rp.uuid,
                client=self.waldur_client,
                body=body,
            )
            logger.warning(
                "ResourceProject %s -> Erred (cr.status.phase=Error): %s",
                rp.uuid.hex,
                error_message or "<no condition message>",
            )
        except Exception as exc:
            logger.warning(
                "Failed to set ResourceProject %s state to Erred: %s",
                rp.uuid.hex,
                exc,
            )

    @staticmethod
    def _collect_error_messages(cr_status: dict) -> str:
        """Flatten failing conditions into a single ; -separated string."""
        msgs = [
            c["message"]
            for c in (cr_status.get("conditions") or [])
            if c.get("status") not in ("True", True) and c.get("message")
        ]
        return "; ".join(msgs)

    @staticmethod
    def _waldur_resource_to_dict(r: WaldurResource) -> dict:
        return {
            "uuid": r.uuid.hex,
            "slug": r.slug,
            # backend_id IS the Rancher cluster ID for a Rancher-flavoured
            # offering (each resource = one downstream cluster). The
            # translator uses it as spec.clusterId.
            "backend_id": getattr(r, "backend_id", "") or "",
        }

    @staticmethod
    def _resource_project_to_dict(rp: Any) -> dict:  # noqa: ANN401
        limits = (
            rp.limits.to_dict()
            if hasattr(rp.limits, "to_dict")
            else (rp.limits or {})
        )
        return {
            "uuid": rp.uuid.hex,
            "name": rp.name,
            "limits": limits,
            "description": getattr(rp, "description", None),
        }

    @staticmethod
    def _user_role_to_dict(u: Any) -> dict:  # noqa: ANN401
        return {
            "role_name": getattr(u, "role_name", None),
            "user_uuid": u.user_uuid.hex if getattr(u, "user_uuid", None) else None,
            "user_username": getattr(u, "user_username", None),
        }

    # ------------------------------------------------------------------
    # Per-user mutations — routed through CR apply
    # ------------------------------------------------------------------

    def add_user(
        self,
        waldur_resource: WaldurResource,
        username: str,
        **kwargs: Any,  # noqa: ANN401
    ) -> bool:
        """Re-apply the affected CR after a role grant.

        We don't mutate the Keycloak group directly; the operator owns
        that. Easiest way to push the new state through is to re-run
        ``pull_resource``, which fetches the current ResourceProject
        users (now including this grant) and re-applies the CR.
        """
        logger.info(
            "add_user(rancher-kc-crd): user=%s resource=%s role=%s — re-syncing CR",
            username,
            waldur_resource.uuid,
            kwargs.get("role_name"),
        )
        try:
            self.pull_resource(waldur_resource)
        except Exception as exc:
            logger.warning("add_user CR re-sync failed: %s", exc)
            return False
        return True

    def remove_user(
        self,
        waldur_resource: WaldurResource,
        username: str,
        **kwargs: Any,  # noqa: ANN401
    ) -> bool:
        """Re-apply the affected CR after a role revoke."""
        logger.info(
            "remove_user(rancher-kc-crd): user=%s resource=%s role=%s — re-syncing CR",
            username,
            waldur_resource.uuid,
            kwargs.get("role_name"),
        )
        try:
            self.pull_resource(waldur_resource)
        except Exception as exc:
            logger.warning("remove_user CR re-sync failed: %s", exc)
            return False
        return True

    # ------------------------------------------------------------------
    # State changes — no-op in v1 (operator owns the cluster)
    # ------------------------------------------------------------------

    def downscale_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op: cluster lifecycle is owned by the operator, not this plugin."""
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op: cluster lifecycle is owned by the operator, not this plugin."""
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op: cluster lifecycle is owned by the operator, not this plugin."""
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:  # noqa: ARG002
        """Aggregate CR statuses for this Resource into backend_metadata.

        v1 stub returns empty; the wiring depends on the same v1 work
        as ``pull_resource``.
        """
        return {}

    # ------------------------------------------------------------------
    # Helpers used by tests (and v1 wiring)
    # ------------------------------------------------------------------

    def apply_resource_project(
        self,
        resource: dict,
        resource_project: dict,
        user_roles: list[dict],
    ) -> dict[str, Any]:
        """Translate and apply a single ResourceProject CR.

        Returns the applied object. Surface for unit tests and the v1
        wiring.
        """
        body = build_cr_spec(
            resource=resource,
            resource_project=resource_project,
            user_roles=user_roles,
            backend_settings=self.backend_settings,
        )
        return self.crd.apply(body)

    def read_resource_project_status(
        self,
        resource_slug: str,
        resource_project_uuid: str,
    ) -> dict[str, Any]:
        """Fetch a CR and return synced users + backend metadata + terminal error."""
        name = cr_name(resource_slug, resource_project_uuid)
        cr = self.crd.get(name)
        status = (cr or {}).get("status") or {}
        return {
            "synced_users": extract_synced_users(status),
            "backend_metadata": build_backend_metadata(status),
            "terminal_error": is_terminal_failure(status),
        }
