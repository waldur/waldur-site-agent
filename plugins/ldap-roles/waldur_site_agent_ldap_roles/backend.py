"""Backend that drives LDAP group memberships from Waldur Resource + ResourceProject roles.

Membership-sync only. Plugged into the existing site-agent
membership-sync loop via the ``waldur_site_agent.backends`` entry point.

Sibling of waldur-site-agent-rancher-kc-crd: same data fetch shape,
same scope split (Resource vs ResourceProject), but the destination
is an LDAP directory instead of a Kubernetes CRD.
"""

from __future__ import annotations

import logging
import uuid as uuid_lib
from typing import Any, Optional

from waldur_api_client.api.marketplace_provider_resource_projects import (
    marketplace_provider_resource_projects_list,
    marketplace_provider_resource_projects_list_users_list,
)
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list_users_list,
)
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_site_agent_ldap_client import LdapClient

from waldur_site_agent.backend import backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .translator import DesiredGroup, Grant, build_grants, diff_members, group_grants

logger = logging.getLogger(__name__)

# Per-grant sync states, in the vocabulary of Waldur's
# set_membership_sync_statuses endpoint. LDAP writes apply within the
# cycle, so "pending" is never reported.
SYNCED = "synced"
MISSING_IN_IDP = "missing_in_idp"
ERROR = "error"

# A member's outcome in one group: (state, message).
Outcome = tuple[str, str]


class LdapRolesBackend(backends.BaseBackend):
    """Site-agent backend that reconciles LDAP group memberships.

    Each membership-sync cycle, the orchestrator calls
    ``pull_resource(waldur_resource)``. We:

    1. Fetch Resource-scope user roles for this Resource.
    2. Fetch ResourceProjects of this Resource and per-RP user roles.
    3. Translate to a desired set of (group_name, members) tuples,
       using the configured templates and role maps.
    4. For each desired group: ensure the group exists in LDAP and is
       owned by this resource, then diff its current members against
       the desired set and add / remove as needed.
    5. Empty every group this resource owns that is no longer desired.

    ``add_user`` / ``remove_user`` re-run ``pull_resource`` so that
    role changes on the Waldur side propagate without bookkeeping in
    the agent itself.
    """

    #: Membership is scoped to Resource and ResourceProject UserRoles and
    #: fully reconciled by pull_resource. Diffing the reported members
    #: against the flat Waldur Project team would flag every
    #: ResourceProject-only member as stale and every team member without
    #: a mapped role as new -- and each of those becomes an
    #: add_user/remove_user call, i.e. one full re-sync per user per cycle.
    skip_resource_team_diff = True

    def __init__(
        self,
        backend_settings: dict,
        backend_components: dict[str, dict],
    ) -> None:
        """Initialize the LDAP client and Waldur SDK client."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "ldap-roles"

        ldap_settings = backend_settings.get("ldap")
        if not ldap_settings:
            msg = (
                "ldap-roles backend requires an 'ldap' section in backend_settings "
                "with at least uri / bind_dn / bind_password / base_dn."
            )
            raise BackendError(msg)

        self.membership_type: str = backend_settings.get("membership_type", "memberUid")
        ldap_settings = self._with_group_object_classes(ldap_settings, self.membership_type)
        # Kept apart from ``self.client``: BaseBackend types that as a
        # BaseClient and calls resource-level methods on it (get_resource
        # and friends) that a directory client has no meaning for.
        self.ldap_client = LdapClient(ldap_settings)

        self.managed_by_tag: str = backend_settings.get("managed_by_tag", "waldur-site-agent")
        self.lookup_by_user_uuid: bool = backend_settings.get("lookup_by_user_uuid", False)
        # Per-grant report of the last pull_resource, by resource UUID hex;
        # the membership processor posts it via get_membership_sync_report.
        self._membership_sync_reports: dict[str, list[dict]] = {}

        api_url = backend_settings.get("waldur_api_url")
        api_token = backend_settings.get("waldur_api_token")
        self.waldur_client: Optional[AuthenticatedClient] = None
        if api_url and api_token:
            # SDK paths already start with "/api/"; base_url is the
            # host root without "/api". Mirrors rancher-kc-crd's
            # construction.
            self.waldur_client = AuthenticatedClient(
                base_url=api_url.rstrip("/").removesuffix("/api"),
                token=api_token,
                prefix="Token",
                verify_ssl=backend_settings.get("waldur_verify_ssl", True),
            )

        logger.info(
            "ldap-roles backend initialized: membership_type=%s "
            "resource_role_map_keys=%s rp_role_map_keys=%s waldur_client=%s",
            self.membership_type,
            sorted(backend_settings.get("resource_role_map") or {}),
            sorted(backend_settings.get("resource_project_role_map") or {}),
            "configured" if self.waldur_client else "not configured",
        )

    @staticmethod
    def _with_group_object_classes(ldap_settings: dict, membership_type: str) -> dict:
        """Default and check the object classes groups are created with.

        The membership attribute has to be one the group's classes allow:
        ``memberUid`` comes with ``posixGroup`` (the client's default),
        ``member`` with ``groupOfNames``. ``member`` mode therefore defaults to
        a plain ``groupOfNames``; directories where ``posixGroup`` is auxiliary
        (rfc2307bis, 389-DS) can list both to get a GID as well.
        """
        required = "groupOfNames" if membership_type == "member" else "posixGroup"
        if membership_type == "member" and "project_group_object_classes" not in ldap_settings:
            ldap_settings = {**ldap_settings, "project_group_object_classes": [required, "top"]}
        classes = ldap_settings.get("project_group_object_classes") or ["posixGroup", "top"]
        if required.lower() not in {c.lower() for c in classes}:
            msg = (
                f"membership_type {membership_type!r} needs {required} in "
                f"ldap.project_group_object_classes, got {classes}"
            )
            raise BackendError(msg)
        return ldap_settings

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Bind to the LDAP server to confirm credentials + reachability."""
        ok = self.ldap_client.ping()
        if not ok and raise_exception:
            msg = "LDAP ping failed"
            raise BackendError(msg)
        return ok

    def diagnostics(self) -> bool:
        """Return True if the backend can bind to LDAP."""
        return self.ping()

    def list_components(self) -> list[str]:
        """No components — LDAP doesn't track usage or quotas."""
        return []

    # Stubs for BaseBackend's abstract methods that this plugin doesn't
    # use (membership-only mode).

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:  # noqa: ARG002
        """No usage reporting."""
        return {}

    def _collect_resource_limits(
        self,
        waldur_resource: WaldurResource,  # noqa: ARG002
    ) -> tuple[dict[str, int], dict[str, int]]:
        """No limit collection in membership-only mode."""
        return {}, {}

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """No-op: groups are created lazily inside pull_resource."""

    def downscale_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op."""
        return True

    def pause_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op."""
        return True

    def restore_resource(self, resource_backend_id: str) -> bool:  # noqa: ARG002
        """No-op."""
        return True

    def get_resource_metadata(self, resource_backend_id: str) -> dict:  # noqa: ARG002
        """No backend metadata to surface."""
        return {}

    # ------------------------------------------------------------------
    # Order processing: create and terminate
    # ------------------------------------------------------------------
    #
    # Offerings use ldap-roles as their order-processing backend too.
    # Membership sync only visits resources that have a backend_id and are
    # OK or ERRED, so create has to set one, and terminate is the last
    # point at which a resource's grants can be revoked.

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Register the resource under ``resource_backend_id``; no LDAP writes yet.

        The processor stores the returned ID on the Waldur resource, which is
        what makes membership sync pick it up. Groups are created by that
        sync, once there are grants to put in them.
        """
        del waldur_resource, user_context
        return BackendResourceInfo(backend_id=resource_backend_id)

    def delete_resource(
        self,
        waldur_resource: WaldurResource,
        **kwargs: str,
    ) -> Optional[str]:
        """Empty every group the resource owns.

        Groups are emptied rather than deleted, for the GID-reuse reason in
        ``_revoke_undesired_groups``. Any LDAP failure raises, so the
        terminate order errs and is retried instead of succeeding with the
        grants still in place.
        """
        del kwargs
        self._revoke_undesired_groups(self._owner_marker(waldur_resource), set(), strict=True)
        self._membership_sync_reports.pop(waldur_resource.uuid.hex, None)
        return None

    # ------------------------------------------------------------------
    # Resource lifecycle (membership-only)
    # ------------------------------------------------------------------

    def pull_resource(
        self,
        waldur_resource: WaldurResource,
    ) -> Optional[BackendResourceInfo]:
        """Reconcile LDAP groups for one Resource and its ResourceProjects.

        Returns ``None`` if no Waldur client is configured (no source
        of truth means nothing to sync). A failed Waldur fetch raises
        before any group is touched, so an API outage never reads as
        "every grant was revoked".
        """
        # A pull that fails must not leave the previous cycle's report to be posted.
        self._membership_sync_reports.pop(waldur_resource.uuid.hex, None)
        if self.waldur_client is None:
            logger.warning(
                "ldap-roles: waldur_client not configured; pull_resource is a no-op for %s",
                waldur_resource.uuid,
            )
            return None

        resource_dict = self._waldur_resource_to_dict(waldur_resource)

        resource_user_roles_dicts: list[dict] = []
        if self.backend_settings.get("resource_role_map"):
            resource_user_roles_dicts = [
                self._user_role_to_dict(u)
                for u in self._fetch_resource_users(waldur_resource.uuid)
            ]

        rp_pairs: list[tuple[dict, list[dict]]] = []
        if self.backend_settings.get("resource_project_role_map"):
            rp_pairs.extend(
                (
                    self._resource_project_to_dict(rp),
                    [
                        self._user_role_to_dict(u)
                        for u in self._fetch_resource_project_users(rp.uuid)
                    ],
                )
                for rp in self._fetch_resource_projects(waldur_resource.uuid)
            )

        grants = build_grants(
            resource=resource_dict,
            resource_user_roles=resource_user_roles_dicts,
            resource_project_user_roles=rp_pairs,
            settings=self.backend_settings,
        )
        desired = group_grants(grants)

        marker = self._owner_marker(waldur_resource)
        outcomes = {group.name: self._sync_group(group, marker) for group in desired}
        self._revoke_undesired_groups(marker, {group.name for group in desired})
        self._membership_sync_reports[waldur_resource.uuid.hex] = self._sync_report(
            grants, outcomes
        )

        synced_users = {
            member
            for group_outcomes in outcomes.values()
            for member, (state, _) in group_outcomes.items()
            if state == SYNCED
        }
        return BackendResourceInfo(users=sorted(synced_users), usage={})

    def get_membership_sync_report(self, waldur_resource: WaldurResource) -> Optional[list[dict]]:
        """Return the per-grant states derived during the last pull_resource."""
        return self._membership_sync_reports.get(waldur_resource.uuid.hex)

    def _sync_report(
        self, grants: list[Grant], outcomes: dict[str, dict[str, Outcome]]
    ) -> list[dict]:
        """One entry per mapped grant, shaped for set_membership_sync_statuses.

        Unmapped roles never become grants, so they stay unreported — no
        state, rather than a misleading one.
        """
        identity_key = "user_uuid" if self.lookup_by_user_uuid else "username"
        report = []
        for grant in grants:
            state, message = outcomes.get(grant.group, {}).get(
                grant.member, (ERROR, f"LDAP group {grant.group} was not reconciled")
            )
            entry = {
                identity_key: grant.member,
                "scope_type": grant.scope_type,
                "role_name": grant.role_name,
                "state": state,
                "message": message,
            }
            if grant.resource_project_uuid:
                entry["resource_project_uuid"] = grant.resource_project_uuid
            report.append(entry)
        return report

    def _owner_marker(self, waldur_resource: WaldurResource) -> str:
        """Description value that marks a group as owned by this resource."""
        return f"managed_by={self.managed_by_tag};resource={waldur_resource.uuid.hex}"

    def _sync_group(self, group: DesiredGroup, marker: str) -> dict[str, Outcome]:
        """Ensure one owned group holds the desired members; return each member's outcome.

        Outcomes use the per-grant sync report's states: ``synced`` once
        the member is in the group, ``missing_in_idp`` when the user has
        no LDAP entry, ``error`` when the group or the write failed.
        """
        try:
            owned = self._ensure_owned_group(group.name, marker)
        except BackendError as exc:
            logger.warning("ldap-roles: failed to ensure group %s: %s", group.name, exc)
            return dict.fromkeys(
                group.members, (ERROR, f"Could not create LDAP group {group.name}: {exc}")
            )
        if not owned:
            return dict.fromkeys(
                group.members,
                (
                    ERROR,
                    f"LDAP group {group.name} already exists and is not managed by the agent "
                    "for this resource",
                ),
            )

        try:
            current = self.ldap_client.list_group_members(group.name, self.membership_type)
        except BackendError as exc:
            logger.warning("ldap-roles: failed to list members of %s: %s", group.name, exc)
            return dict.fromkeys(
                group.members, (ERROR, f"Could not read LDAP group {group.name}: {exc}")
            )

        # Diff against desired identifiers as-is. When membership_type
        # is "memberUid" the LDAP attribute already stores usernames;
        # when "member" the client's list_group_members has stripped
        # the DN to its uid RDN, so the two spaces match.
        _, to_remove = diff_members(current, group.members)

        outcomes: dict[str, Outcome] = {}
        for username in sorted(group.members):
            try:
                exists = self._user_exists(username)
            except BackendError as exc:
                logger.warning("ldap-roles: user lookup failed for %s: %s", username, exc)
                outcomes[username] = (ERROR, f"Could not look up the user in LDAP: {exc}")
                continue
            if not exists:
                logger.warning(
                    "ldap-roles: user %s not present in LDAP; skipping add to group %s",
                    username,
                    group.name,
                )
                outcomes[username] = (
                    MISSING_IN_IDP,
                    "No LDAP entry for this user; access applies once it exists",
                )
                continue
            if username in current:
                outcomes[username] = (SYNCED, "")
                continue
            try:
                self.ldap_client.add_user_to_group(group.name, username, self.membership_type)
                outcomes[username] = (SYNCED, "")
            except BackendError as exc:
                logger.warning(
                    "ldap-roles: failed to add %s to %s: %s", username, group.name, exc
                )
                outcomes[username] = (ERROR, f"Could not add to LDAP group {group.name}: {exc}")

        for username in to_remove:
            try:
                self.ldap_client.remove_user_from_group(
                    group.name, username, self.membership_type
                )
            except BackendError as exc:
                logger.warning(
                    "ldap-roles: failed to remove %s from %s: %s",
                    username,
                    group.name,
                    exc,
                )

        return outcomes

    def _ensure_owned_group(self, group_name: str, marker: str) -> bool:
        """Create the group if absent; return whether this resource owns it.

        Ownership is ``marker`` among the group's description values. A
        group that exists without it was not created for this resource —
        by another resource whose template renders the same name, by the
        ldap plugin, or by an administrator — and reconciling it would
        strip every member Waldur does not know about. Such a group is
        left alone; adding the marker to it by hand hands it over.

        ``create_project_group`` uses ``project_group_object_classes``,
        checked against ``membership_type`` at construction.
        """
        descriptions = self.ldap_client.get_group_descriptions(group_name)
        if descriptions is None:
            # The marker goes in with the group's own add: written separately,
            # a failure in between would leave the agent's group unmarked, and
            # every later cycle would refuse it as someone else's.
            self.ldap_client.create_project_group(
                group_name, extra_attributes={"description": marker}
            )
            return True
        if marker in descriptions:
            return True
        logger.warning(
            "ldap-roles: group %s already exists and is not managed for this resource; "
            "leaving it untouched. Add the description value %r to the group to let "
            "the agent manage it.",
            group_name,
            marker,
        )
        return False

    def _revoke_undesired_groups(
        self, marker: str, desired_names: set[str], strict: bool = False
    ) -> None:
        """Empty the groups this resource owns that are no longer desired.

        A group drops out of the desired state when the last holder of its
        role is revoked, when its ResourceProject is deleted, or when its
        role leaves the role map — the translator emits nothing for any of
        those, so without this pass their members keep the grant forever.

        The entry itself is kept, not deleted: ``get_next_gid`` hands out
        the lowest free GID, so a deleted group's GID — and with it every
        file still owned by that group — would pass to the next group
        created.

        With ``strict``, any LDAP failure raises instead of being logged: on
        termination nothing revisits the resource, so a revocation that
        silently failed would never be retried.
        """
        failures: list[str] = []
        try:
            owned = self.ldap_client.find_groups_by_description(marker)
        except BackendError as exc:
            if strict:
                raise
            logger.warning("ldap-roles: failed to list groups owned via %s: %s", marker, exc)
            return

        for group_name in sorted(set(owned) - desired_names):
            try:
                members = self.ldap_client.list_group_members(group_name, self.membership_type)
            except BackendError as exc:
                logger.warning("ldap-roles: failed to list members of %s: %s", group_name, exc)
                failures.append(f"{group_name}: {exc}")
                continue
            for username in members:
                try:
                    self.ldap_client.remove_user_from_group(
                        group_name, username, self.membership_type
                    )
                except BackendError as exc:
                    logger.warning(
                        "ldap-roles: failed to revoke %s from %s: %s", username, group_name, exc
                    )
                    failures.append(f"{username} from {group_name}: {exc}")
            if members:
                logger.info(
                    "ldap-roles: group %s is no longer granted in Waldur; revoked %s",
                    group_name,
                    ", ".join(members),
                )
        if strict and failures:
            msg = "Could not empty the LDAP groups: " + "; ".join(failures)
            raise BackendError(msg)

    def _user_exists(self, username: str) -> bool:
        """Cache-free user existence check.

        We don't cache between groups because a single sync cycle is
        already short and the cost of a stale negative would be a
        skipped grant that the user notices immediately. When
        ``lookup_by_user_uuid`` is on, the identifier is a UUID stored
        in some LDAP attribute (deployment-specific) and we currently
        don't have a generic resolver; treat as present so the
        downstream add_user_to_group either succeeds or surfaces a
        clearer error.
        """
        if self.lookup_by_user_uuid:
            return True
        # A failed lookup raises rather than reading as "no entry": the
        # caller reports it as an error, not as a user missing from LDAP.
        return self.ldap_client.user_exists(username)

    # ------------------------------------------------------------------
    # SDK helpers
    # ------------------------------------------------------------------
    #
    # All three endpoints are paginated. ``sync_all`` follows every page:
    # a first-page-only read would never grant the roles beyond it, and
    # the revocation pass would then empty their groups each cycle.

    def _fetch_resource_projects(self, resource_uuid: uuid_lib.UUID) -> list:
        """All ResourceProjects belonging to a given Resource."""
        return marketplace_provider_resource_projects_list.sync_all(
            client=self.waldur_client,
            resource_uuid=resource_uuid,
        )

    def _fetch_resource_project_users(self, rp_uuid: uuid_lib.UUID) -> list:
        """UserRoles assigned on a given ResourceProject."""
        return marketplace_provider_resource_projects_list_users_list.sync_all(
            client=self.waldur_client,
            uuid=rp_uuid,
        )

    def _fetch_resource_users(self, resource_uuid: uuid_lib.UUID) -> list:
        """UserRoles assigned on the Resource itself."""
        return marketplace_provider_resources_list_users_list.sync_all(
            client=self.waldur_client,
            uuid=resource_uuid,
        )

    @staticmethod
    def _waldur_resource_to_dict(r: WaldurResource) -> dict:
        return {
            "uuid": r.uuid.hex,
            "slug": r.slug,
            "customer_slug": getattr(r, "customer_slug", "") or "",
            "project_slug": getattr(r, "project_slug", "") or "",
        }

    @staticmethod
    def _resource_project_to_dict(rp: Any) -> dict:  # noqa: ANN401
        return {
            "uuid": rp.uuid.hex,
            "name": rp.name,
        }

    @staticmethod
    def _user_role_to_dict(u: Any) -> dict:  # noqa: ANN401
        return {
            "role_name": getattr(u, "role_name", None),
            "user_uuid": u.user_uuid.hex if getattr(u, "user_uuid", None) else None,
            "user_username": getattr(u, "user_username", None),
        }

    # ------------------------------------------------------------------
    # Per-user mutations — re-run pull_resource to pick up the change
    # ------------------------------------------------------------------
    #
    # With skip_resource_team_diff the membership processor never asks for
    # these; they remain for the direct role-change event path, which calls
    # them from a Waldur role grant/revoke. The username is ignored — the
    # re-pull reads the true Resource/ResourceProject-scoped membership.

    def add_user(
        self,
        waldur_resource: WaldurResource,
        username: str,
        **kwargs: Any,  # noqa: ANN401, ARG002
    ) -> bool:
        """Re-sync after a role grant — Waldur is the source of truth."""
        logger.info(
            "add_user(ldap-roles): user=%s resource=%s — re-syncing groups",
            username,
            waldur_resource.uuid,
        )
        try:
            self.pull_resource(waldur_resource)
        except Exception as exc:
            logger.warning("ldap-roles add_user re-sync failed: %s", exc)
            return False
        return True

    def remove_user(
        self,
        waldur_resource: WaldurResource,
        username: str,
        **kwargs: Any,  # noqa: ANN401, ARG002
    ) -> bool:
        """Re-sync after a role revoke."""
        logger.info(
            "remove_user(ldap-roles): user=%s resource=%s — re-syncing groups",
            username,
            waldur_resource.uuid,
        )
        try:
            self.pull_resource(waldur_resource)
        except Exception as exc:
            logger.warning("ldap-roles remove_user re-sync failed: %s", exc)
            return False
        return True
