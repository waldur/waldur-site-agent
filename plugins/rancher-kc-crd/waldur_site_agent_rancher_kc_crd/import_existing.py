"""Import the access that already exists in Rancher and Keycloak into Waldur.

Brownfield onboarding for the rancher-kc-crd plugin. The plugin only
pushes Waldur's view into ManagedRancherProject CRs; nothing reads the
role bindings a cluster already has back into Waldur. Run this once per
offering, before the agent's first sync, so Waldur already holds the
access the clusters grant today:

``inventory``
    For every Resource of the offering, reads the Rancher cluster named
    by ``Resource.backend_id``: its projects, each project's role
    template bindings (PRTBs) and the cluster role template bindings
    (CRTBs). User principals are resolved to Keycloak users; group
    principals are expanded to the group's direct members. Writes a JSON
    file to review (and edit) before anything changes.

``apply``
    Maps the inventory onto Waldur: a ResourceProject per Rancher
    project, named exactly like it so the operator adopts the project
    instead of creating a second one, and a user role for each binding
    whose role template ``role_map`` (project scope) or
    ``cluster_role_map`` (cluster scope) maps back to a Waldur role.
    Prints the plan; changes Waldur only with ``--execute``.

Nothing is ever written to Rancher or Keycloak. Credentials come from the
same environment variables the operator uses (``RANCHER_URL``,
``RANCHER_BEARER_TOKEN``, ``KEYCLOAK_URL``, ``KEYCLOAK_REALM``,
``KEYCLOAK_USERNAME``, ``KEYCLOAK_PASSWORD``, ...). The Waldur token is
``WALDUR_API_TOKEN`` or ``--waldur-token``, falling back to the
offering's token from the agent config. It has to be a staff token: the
import lists the offering's resources as provider, records backend IDs
through the provider API and looks users up across organizations,
which neither the agent's offering-manager token nor an organization
owner's token can do.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Any, Optional

import httpx
import yaml

from .translator import IdentitySettings, mask_civil_code, resolve_identity_settings

logger = logging.getLogger(__name__)

# 2: projects carry their Rancher resource quota.
INVENTORY_VERSION = 2

# Same exclusion as the operator's discovery scan: adopting these would
# put Rancher's own projects under a CR, and deleting that CR deletes them.
SYSTEM_PROJECT_NAMES = frozenset({"Default", "System"})

KEYCLOAK_USER_PRINCIPAL = "keycloakoidc_user://"
KEYCLOAK_GROUP_PRINCIPAL = "keycloakoidc_group://"

INACTIVE_RESOURCE_STATES = frozenset({"Terminating", "Terminated"})

WALDUR_PAGE_SIZE = 100
KEYCLOAK_PAGE_SIZE = 100
HTTP_NOT_FOUND = 404
HTTP_UNAUTHORIZED = 401


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def reverse_role_map(role_map: dict[str, str]) -> dict[str, str]:
    """Invert a ``role_map``: Rancher role template ID -> Waldur role name.

    When several Waldur roles map to the same template, the first in
    sorted order wins: the binding cannot say which one it came from.
    """
    reverse: dict[str, str] = {}
    for waldur_role in sorted(role_map):
        template = role_map[waldur_role]
        if template in reverse:
            logger.warning(
                "Role template %s is mapped from both %s and %s; importing it as %s",
                template,
                reverse[template],
                waldur_role,
                reverse[template],
            )
            continue
        reverse[template] = waldur_role
    return reverse


def parse_principal(principal: str) -> tuple[str, str]:
    """Split a Rancher principal ID into ``(kind, value)``.

    ``kind`` is ``user`` or ``group`` for Keycloak OIDC principals and
    ``other`` for anything else (local users, other auth providers),
    which cannot be matched to a Keycloak user.
    """
    if principal.startswith(KEYCLOAK_USER_PRINCIPAL):
        return "user", principal[len(KEYCLOAK_USER_PRINCIPAL) :]
    if principal.startswith(KEYCLOAK_GROUP_PRINCIPAL):
        return "group", principal[len(KEYCLOAK_GROUP_PRINCIPAL) :]
    return "other", principal


def reverse_identity_template(
    template: str, identifier: str, ignore_case: bool = False
) -> Optional[str]:
    """Undo ``keycloak_user_identity_template``: ``EE${value}`` + ``ee3800`` -> ``3800``.

    Returns ``None`` when ``identifier`` does not have the template's shape.
    """
    marker = "\x00"
    head, *rest = Template(template).safe_substitute(value=marker).split(marker)
    if not rest:
        return None
    pattern = re.escape(head) + "(?P<v>.+)" + "(?P=v)".join(re.escape(part) for part in rest)
    match = re.fullmatch(pattern, identifier, flags=re.IGNORECASE if ignore_case else 0)
    return match.group("v") if match else None


def keycloak_identifier(kc_user: dict, identity: IdentitySettings) -> tuple[Optional[str], str]:
    """What the operator would match this Keycloak user on, or ``(None, reason)``."""
    if identity.lookup == "id":
        return kc_user.get("id") or None, "Keycloak user has no id"
    if identity.lookup == "username":
        return kc_user.get("username") or None, "Keycloak user has no username"
    values = (kc_user.get("attributes") or {}).get(identity.attribute or "") or []
    if len(values) != 1:
        return None, f"Keycloak user has {len(values)} values of attribute {identity.attribute}"
    return values[0], ""


def waldur_identity_value(kc_user: dict, identity: IdentitySettings) -> tuple[Optional[str], str]:
    """The Waldur field value (username, UUID or civil code) a Keycloak user maps back to."""
    ident, reason = keycloak_identifier(kc_user, identity)
    if not ident:
        return None, reason
    value = reverse_identity_template(identity.template, ident, ignore_case=identity.lowercase)
    if value is None:
        return (
            None,
            f"identifier does not match keycloak_user_identity_template {identity.template!r}",
        )
    return value, ""


def parse_cpu_millicores(value: str) -> Optional[int]:
    """Kubernetes CPU quantity -> millicores: ``8000m`` -> 8000, ``2`` / ``0.5`` -> 2000 / 500."""
    text = str(value).strip()
    try:
        if text.endswith("m"):
            return int(text[:-1])
        return round(float(text) * 1000)
    except ValueError:
        return None


def quota_to_limits(quota_limit: dict) -> tuple[dict, list[str]]:
    """A Rancher project's ``resourceQuota.limit`` as ResourceProject ``limits``.

    The inverse of the operator's translation (``cpu`` millicores ->
    ``limitsCpu``, ``memory`` -> ``limitsMemory``, ``storage`` ->
    ``requestsStorage``). Returns the limits and the quota keys that have
    no Waldur equivalent: the operator replaces the whole quota from the
    CR, so those are removed on the first sync.
    """
    limits: dict[str, Any] = {}
    dropped: list[str] = []
    for key, value in (quota_limit or {}).items():
        if value in (None, ""):
            continue
        if key == "limitsCpu":
            cpu = parse_cpu_millicores(value)
            if cpu is None:
                dropped.append(key)
            else:
                limits["cpu"] = cpu
        elif key == "limitsMemory":
            limits["memory"] = str(value)
        elif key == "requestsStorage":
            limits["storage"] = str(value)
        else:
            dropped.append(key)
    return limits, sorted(dropped)


def binding_principal(binding: dict) -> str:
    """The principal a PRTB/CRTB grants to: user principal, else group principal."""
    return binding.get("userPrincipalId") or binding.get("groupPrincipalId") or ""


def normalize_uuid(value: str) -> str:
    """Undashed, lowercase UUID."""
    return value.replace("-", "").lower()


def project_matches(project: dict, projects: Optional[list[str]]) -> bool:
    """Whether a Rancher project passes ``--project``: name, full ID or short ID.

    Inventory entries (``rancher_project_id``) and raw Rancher projects
    (``id``) are both accepted.
    """
    if not projects:
        return True
    project_id = project.get("rancher_project_id") or project.get("id") or ""
    short_id = project_id.split(":", 1)[-1]
    return bool({project.get("name"), project_id, short_id} & set(projects))


def resource_matches(
    resource_uuid: str,
    resource_name: str,
    cluster_id: str,
    clusters: Optional[list[str]],
    resources: Optional[list[str]],
) -> bool:
    """Whether a Waldur resource passes ``--cluster`` and ``--resource`` (UUID or name)."""
    if clusters and cluster_id not in clusters:
        return False
    if resources:
        wanted = {normalize_uuid(r) for r in resources} | set(resources)
        if not {normalize_uuid(resource_uuid), resource_name} & wanted:
            return False
    return True


def waldur_api_root(url: str) -> str:
    """``https://waldur.example.com`` or ``.../api/`` -> ``https://waldur.example.com/api``."""
    url = url.rstrip("/")
    return url if url.endswith("/api") else f"{url}/api"


# ---------------------------------------------------------------------
# API clients (synchronous, read-mostly)
# ---------------------------------------------------------------------


class RancherApi:
    """Read-only Rancher v3 API access."""

    def __init__(self, url: str, token: str, verify: bool = True) -> None:
        """Client for the Rancher management server at ``url``."""
        self.client = httpx.Client(
            base_url=f"{url.rstrip('/')}/v3",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            verify=verify,
            timeout=60.0,
        )

    def _list(self, path: str, params: dict[str, str]) -> list[dict]:
        items: list[dict] = []
        url: Optional[str] = path
        query: Optional[dict[str, str]] = {**params, "limit": "-1"}
        while url:
            resp = self.client.get(url, params=query)
            resp.raise_for_status()
            body = resp.json()
            items.extend(body.get("data", []))
            url = (body.get("pagination") or {}).get("next")
            query = None  # the next link carries its own query string
        return items

    def projects(self, cluster_id: str) -> list[dict]:
        """All projects of a cluster."""
        return self._list("/projects", {"clusterId": cluster_id})

    def project_bindings(self, project_id: str) -> list[dict]:
        """PRTBs of a project."""
        return self._list("/projectroletemplatebindings", {"projectId": project_id})

    def cluster_bindings(self, cluster_id: str) -> list[dict]:
        """CRTBs of a cluster."""
        return self._list("/clusterroletemplatebindings", {"clusterId": cluster_id})

    def user_principal_ids(self, user_id: str) -> list[str]:
        """Principals linked to a Rancher user (local and external)."""
        resp = self.client.get(f"/users/{user_id}")
        if resp.status_code == HTTP_NOT_FOUND:
            return []
        resp.raise_for_status()
        return resp.json().get("principalIds") or []


class KeycloakApi:
    """Read-only Keycloak admin API access."""

    def __init__(
        self,
        url: str,
        realm: str,
        username: str,
        password: str,
        user_realm: str = "master",
        client_id: str = "admin-cli",
        verify: bool = True,
    ) -> None:
        """Admin client for ``realm``, logging in through ``user_realm``."""
        self.url = url.rstrip("/")
        self.realm = realm
        self.username = username
        self.password = password
        self.user_realm = user_realm
        self.client_id = client_id
        self.client = httpx.Client(verify=verify, timeout=60.0)
        self._token: Optional[str] = None

    def _refresh_token(self) -> None:
        resp = self.client.post(
            f"{self.url}/realms/{self.user_realm}/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": self.client_id,
                "username": self.username,
                "password": self.password,
            },
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]

    def _get(self, path: str, params: Optional[dict[str, str]] = None) -> httpx.Response:
        if not self._token:
            self._refresh_token()
        url = f"{self.url}/admin/realms/{self.realm}{path}"
        resp = self.client.get(
            url, params=params, headers={"Authorization": f"Bearer {self._token}"}
        )
        if resp.status_code == HTTP_UNAUTHORIZED:
            self._refresh_token()
            resp = self.client.get(
                url, params=params, headers={"Authorization": f"Bearer {self._token}"}
            )
        return resp

    def user(self, user_id: str) -> Optional[dict]:
        """A Keycloak user by ID, or None."""
        resp = self._get(f"/users/{user_id}")
        if resp.status_code == HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        return resp.json()

    def groups_named(self, name: str) -> list[dict]:
        """Every group called ``name``, at any depth; a ``/a/b`` name is a full path."""
        if name.startswith("/"):
            resp = self._get(f"/group-by-path/{name.lstrip('/')}")
            if resp.status_code == HTTP_NOT_FOUND:
                return []
            resp.raise_for_status()
            return [resp.json()]
        resp = self._get("/groups", {"search": name, "exact": "true", "max": "-1"})
        resp.raise_for_status()
        found: list[dict] = []

        def walk(groups: list[dict]) -> None:
            for group in groups:
                if group.get("name") == name:
                    found.append(group)
                walk(group.get("subGroups") or [])

        walk(resp.json())
        return found

    def group_members(self, group_id: str) -> list[dict]:
        """Direct members of a group, with attributes, paged."""
        members: list[dict] = []
        first = 0
        while True:
            resp = self._get(
                f"/groups/{group_id}/members",
                {
                    "first": str(first),
                    "max": str(KEYCLOAK_PAGE_SIZE),
                    "briefRepresentation": "false",
                },
            )
            resp.raise_for_status()
            page = resp.json()
            members.extend(page)
            if len(page) < KEYCLOAK_PAGE_SIZE:
                return members
            first += KEYCLOAK_PAGE_SIZE


class WaldurApi:
    """The handful of Waldur endpoints the import needs."""

    def __init__(self, url: str, token: str, verify: bool = True) -> None:
        """Client for the Waldur API at ``url`` (``/api`` is appended if missing)."""
        self.client = httpx.Client(
            base_url=waldur_api_root(url),
            headers={"Authorization": f"Token {token}", "Accept": "application/json"},
            verify=verify,
            timeout=60.0,
        )

    def _list(self, path: str, params: Optional[dict[str, str]] = None) -> list[dict]:
        items: list[dict] = []
        page = 1
        while True:
            resp = self.client.get(
                path,
                params={**(params or {}), "page": str(page), "page_size": str(WALDUR_PAGE_SIZE)},
            )
            resp.raise_for_status()
            batch = resp.json()
            items.extend(batch)
            if len(batch) < WALDUR_PAGE_SIZE:
                return items
            page += 1

    def resources(self, offering_uuid: str) -> list[dict]:
        """Resources of an offering."""
        return self._list("/marketplace-provider-resources/", {"offering_uuid": offering_uuid})

    def offering_roles(self, offering_uuid: str) -> list[dict]:
        """Roles defined for an offering's resources and resource projects."""
        return self._list("/marketplace-offering-roles/", {"offering_uuid": offering_uuid})

    def resource_projects(self, resource_uuid: str) -> list[dict]:
        """ResourceProjects of a resource."""
        return self._list("/marketplace-resource-projects/", {"resource_uuid": resource_uuid})

    def scope_users(self, scope_path: str, scope_uuid: str) -> list[dict]:
        """User roles held in a resource or resource project."""
        return self._list(f"/{scope_path}/{scope_uuid}/list_users/")

    def user_by_uuid(self, user_uuid: str) -> Optional[dict]:
        """A Waldur user by UUID, or None."""
        resp = self.client.get(f"/users/{normalize_uuid(user_uuid)}/")
        if resp.status_code == HTTP_NOT_FOUND:
            return None
        resp.raise_for_status()
        return resp.json()

    def users(self, params: dict[str, str]) -> list[dict]:
        """Waldur users matching the filter params."""
        return self._list("/users/", params)

    def create_resource_project(
        self, resource_uuid: str, name: str, description: str, limits: Optional[dict] = None
    ) -> dict:
        """Create a ResourceProject."""
        body: dict[str, Any] = {"resource": resource_uuid, "name": name, "description": description}
        if limits:
            body["limits"] = limits
        resp = self.client.post("/marketplace-resource-projects/", json=body)
        _raise_with_body(resp)
        return resp.json()

    def set_resource_project_backend_id(self, rp_uuid: str, backend_id: str) -> None:
        """Record the Rancher project ID on a ResourceProject."""
        resp = self.client.post(
            f"/marketplace-provider-resource-projects/{rp_uuid}/set_backend_id/",
            json={"backend_id": backend_id},
        )
        _raise_with_body(resp)

    def add_user(self, scope_path: str, scope_uuid: str, user_uuid: str, role_uuid: str) -> None:
        """Grant a role in a resource or resource project."""
        resp = self.client.post(
            f"/{scope_path}/{scope_uuid}/add_user/",
            json={"user": user_uuid, "role": role_uuid},
        )
        _raise_with_body(resp)


def _raise_with_body(resp: httpx.Response) -> None:
    if resp.is_error:
        raise httpx.HTTPStatusError(
            f"{resp.request.method} {resp.request.url} -> {resp.status_code}: {resp.text[:500]}",
            request=resp.request,
            response=resp,
        )


# ---------------------------------------------------------------------
# Step 1: inventory
# ---------------------------------------------------------------------


class InventoryCollector:
    """Reads clusters, bindings and the Keycloak users behind them."""

    def __init__(
        self, rancher: RancherApi, keycloak: KeycloakApi, identity: IdentitySettings
    ) -> None:
        """Collect with caches: users and groups recur across bindings."""
        self.rancher = rancher
        self.keycloak = keycloak
        self.identity = identity
        self._users: dict[str, dict] = {}
        self._groups: dict[str, tuple[list[dict], str]] = {}
        self._principals: dict[str, list[str]] = {}

    def _user_record(self, kc_user: dict) -> dict:
        value, reason = waldur_identity_value(kc_user, self.identity)
        record = {
            "keycloak_id": kc_user.get("id"),
            "keycloak_username": kc_user.get("username"),
            "identity_value": value,
        }
        if reason and not value:
            record["error"] = reason
        return record

    def _keycloak_user(self, user_id: str) -> dict:
        if user_id not in self._users:
            kc_user = self.keycloak.user(user_id)
            self._users[user_id] = (
                self._user_record(kc_user)
                if kc_user
                else {"keycloak_id": user_id, "error": "user not found in Keycloak"}
            )
        return self._users[user_id]

    def _group_members(self, name: str) -> tuple[list[dict], str]:
        if name not in self._groups:
            groups = self.keycloak.groups_named(name)
            if not groups:
                self._groups[name] = ([], "group not found in Keycloak")
            elif len(groups) > 1:
                paths = ", ".join(g.get("path", "?") for g in groups)
                self._groups[name] = ([], f"group name is ambiguous in Keycloak: {paths}")
            else:
                members = self.keycloak.group_members(groups[0]["id"])
                self._groups[name] = ([self._user_record(m) for m in members], "")
        return self._groups[name]

    def binding(self, raw: dict) -> dict:
        """One PRTB/CRTB as an inventory entry, with the Keycloak users it grants to."""
        principal = binding_principal(raw)
        if raw.get("userId") and not principal.startswith(KEYCLOAK_USER_PRINCIPAL):
            # Rancher 2.15 records a user binding under the user's local
            # principal (local://u-...) even for a Keycloak user, and older
            # bindings may carry no principal at all. The user's principal
            # list says whether it is a Keycloak user.
            if raw["userId"] not in self._principals:
                self._principals[raw["userId"]] = self.rancher.user_principal_ids(raw["userId"])
            ids = self._principals[raw["userId"]]
            keycloak_ids = [p for p in ids if p.startswith(KEYCLOAK_USER_PRINCIPAL)]
            if keycloak_ids:
                principal = keycloak_ids[0]
            elif not principal:
                principal = f"rancher-user://{raw['userId']}"
        entry: dict[str, Any] = {
            "rancher_binding_id": raw.get("id"),
            "rancher_role": raw.get("roleTemplateId"),
            "principal": principal,
            "users": [],
        }
        kind, value = parse_principal(principal)
        if kind == "user":
            entry["users"] = [self._keycloak_user(value)]
        elif kind == "group":
            entry["users"], error = self._group_members(value)
            if error:
                entry["error"] = error
        else:
            entry["error"] = "principal is not a Keycloak OIDC user or group"
        return entry

    def cluster(self, resource: dict, projects_filter: Optional[list[str]] = None) -> dict:
        """Inventory entry for the cluster behind a Waldur resource.

        With ``projects_filter`` only the matching projects are read, and
        the cluster-scope bindings are left out: they are not part of any
        one project.
        """
        cluster_id = resource["backend_id"]
        projects = self.rancher.projects(cluster_id)
        # Counted over every project: a duplicate outside the filter still
        # makes adoption by name ambiguous.
        name_counts: dict[str, int] = {}
        for project in projects:
            name = project.get("name", "")
            name_counts[name] = name_counts.get(name, 0) + 1
        selected = [p for p in projects if project_matches(p, projects_filter)]
        if projects_filter and not selected:
            logger.warning(
                "No project on cluster %s matches %s", cluster_id, ", ".join(projects_filter)
            )
        project_entries = []
        for project in sorted(selected, key=lambda p: p.get("name", "")):
            name = project.get("name", "")
            annotations = project.get("annotations") or {}
            project_entries.append(
                {
                    "rancher_project_id": project["id"],
                    "name": name,
                    "description": project.get("description") or "",
                    "system": name in SYSTEM_PROJECT_NAMES,
                    "duplicate_name": name_counts[name] > 1,
                    "waldur_managed": annotations.get("waldur/managed") == "true",
                    "resource_quota": (project.get("resourceQuota") or {}).get("limit") or {},
                    "bindings": [
                        self.binding(b) for b in self.rancher.project_bindings(project["id"])
                    ],
                }
            )
        return {
            "cluster_id": cluster_id,
            "resource_uuid": normalize_uuid(resource["uuid"]),
            "resource_name": resource.get("name", ""),
            "projects": project_entries,
            "cluster_bindings": []
            if projects_filter
            else [self.binding(b) for b in self.rancher.cluster_bindings(cluster_id)],
        }


def select_resources(
    resources: list[dict],
    clusters: Optional[list[str]] = None,
    resource_filters: Optional[list[str]] = None,
) -> list[dict]:
    """Resources to import: active, with a cluster ID, one per cluster."""
    selected: list[dict] = []
    seen: dict[str, str] = {}
    for resource in resources:
        cluster_id = resource.get("backend_id") or ""
        if resource.get("state") in INACTIVE_RESOURCE_STATES:
            continue
        if not cluster_id:
            logger.warning("Resource %s has no backend_id; skipping", resource.get("name"))
            continue
        if not resource_matches(
            resource.get("uuid", ""),
            resource.get("name", ""),
            cluster_id,
            clusters,
            resource_filters,
        ):
            continue
        if cluster_id in seen:
            logger.warning(
                "Resources %s and %s both point at cluster %s; skipping the second",
                seen[cluster_id],
                resource.get("name"),
                cluster_id,
            )
            continue
        seen[cluster_id] = resource.get("name", "")
        selected.append(resource)
    return selected


# ---------------------------------------------------------------------
# Step 2: plan and apply
# ---------------------------------------------------------------------

SCOPE_PATHS = {
    "resource_project": "marketplace-resource-projects",
    "resource": "marketplace-resources",
}


class Importer:
    """Turns an inventory into ResourceProjects and user roles in Waldur."""

    def __init__(
        self,
        waldur: WaldurApi,
        offering_uuid: str,
        role_map: dict[str, str],
        cluster_role_map: dict[str, str],
        identity: IdentitySettings,
        execute: bool = False,
        include_system: bool = False,
    ) -> None:
        """Resolve the offering's role UUIDs up front."""
        self.waldur = waldur
        self.identity = identity
        self.execute = execute
        self.include_system = include_system
        self.project_roles = reverse_role_map(role_map)
        self.cluster_roles = reverse_role_map(cluster_role_map)
        self.role_uuids = self._role_uuids(offering_uuid)
        self._waldur_users: dict[str, tuple[Optional[dict], str]] = {}
        self.actions: list[dict] = []

    def _role_uuids(self, offering_uuid: str) -> dict[tuple[str, str], str]:
        """``(content_type, role name) -> role UUID`` for the offering's roles.

        Role names are only unique per offering, so ``add_user`` gets the
        UUID: by name it can resolve to another offering's role.
        """
        return {
            (role.get("content_type") or "", role["name"]): role["uuid"]
            for role in self.waldur.offering_roles(offering_uuid)
        }

    def _record(self, **action: Any) -> dict:  # noqa: ANN401
        self.actions.append(action)
        return action

    def display_user(self, user: dict) -> str:
        """How an inventory user is shown; civil codes are masked."""
        name = user.get("keycloak_username") or user.get("keycloak_id") or "?"
        return mask_civil_code(name) if self.identity.source == "civil_number" else name

    def resolve_user(self, value: str) -> tuple[Optional[dict], str]:
        """Find the Waldur user a Keycloak user maps back to."""
        if value in self._waldur_users:
            return self._waldur_users[value]
        source = self.identity.source
        candidates: list[dict]
        if source == "uuid":
            found = self.waldur.user_by_uuid(value)
            candidates = [found] if found else []
        elif source == "username":
            candidates = self.waldur.users({"username": value})
            if not candidates and self.identity.lowercase:
                # The identifier was lowercased on the way out; the
                # Waldur username may not be.
                candidates = [
                    u
                    for u in self.waldur.users({"query": value})
                    if (u.get("username") or "").lower() == value.lower()
                ]
        else:
            candidates = [
                u
                for u in self.waldur.users({"query": value})
                if (u.get("civil_number") or "").lower() == value.lower()
            ]
        # Deactivated accounts are dropped before counting: an old account
        # sharing the identifier must not block the current one.
        active = [u for u in candidates if u.get("is_active", True)]
        if not candidates:
            result: tuple[Optional[dict], str] = (None, f"no Waldur user with this {source}")
        elif not active:
            result = (None, "Waldur user is deactivated")
        elif len(active) > 1:
            result = (None, f"{len(active)} active Waldur users share this {source}")
        else:
            result = (active[0], "")
        self._waldur_users[value] = result
        return result

    def _grants(
        self, bindings: list[dict], reverse_map: dict[str, str], content_type: str, where: str
    ) -> dict[tuple[str, str], dict]:
        """Waldur ``(user UUID, role name)`` grants the bindings translate to."""
        grants: dict[tuple[str, str], dict] = {}
        for binding in bindings:
            rancher_role = binding.get("rancher_role") or ""
            waldur_role = reverse_map.get(rancher_role)
            base = {"where": where, "rancher_role": rancher_role, "principal": binding["principal"]}
            if binding.get("error"):
                self._record(op="skip", reason=binding["error"], **base)
                continue
            if not waldur_role:
                self._record(op="skip", reason="role template not in the role map", **base)
                continue
            if (content_type, waldur_role) not in self.role_uuids:
                self._record(
                    op="skip",
                    reason=f"offering has no {content_type} role {waldur_role!r}",
                    **base,
                )
                continue
            for user in binding["users"]:
                who = {**base, "user": self.display_user(user), "waldur_role": waldur_role}
                if user.get("error") or not user.get("identity_value"):
                    self._record(op="skip", reason=user.get("error", "no identifier"), **who)
                    continue
                waldur_user, reason = self.resolve_user(user["identity_value"])
                if not waldur_user:
                    self._record(op="skip", reason=reason, **who)
                    continue
                key = (normalize_uuid(waldur_user["uuid"]), waldur_role)
                grants.setdefault(key, {**who, "user": waldur_user.get("username", who["user"])})
        return grants

    def _apply_grants(
        self,
        grants: dict[tuple[str, str], dict],
        content_type: str,
        scope_uuid: Optional[str],
    ) -> None:
        scope_path = SCOPE_PATHS[content_type]
        # Compared by role UUID: a Resource's list_users also returns the
        # roles held in its ResourceProjects, and an offering may use one
        # role name at both scopes.
        existing: set[tuple[str, str]] = set()
        if scope_uuid:
            existing = {
                (normalize_uuid(u["user_uuid"]), normalize_uuid(u.get("role_uuid") or ""))
                for u in self.waldur.scope_users(scope_path, scope_uuid)
            }
        for (user_uuid, waldur_role), info in sorted(grants.items(), key=lambda kv: kv[1]["user"]):
            if self.execute and not scope_uuid:
                self._record(op="skip", reason="resource project was not created", **info)
                continue
            role_uuid = self.role_uuids[(content_type, waldur_role)]
            if (user_uuid, normalize_uuid(role_uuid)) in existing:
                self._record(op="exists", **info)
                continue
            action = self._record(op="add_user", **info)
            if not self.execute or not scope_uuid:
                continue
            try:
                self.waldur.add_user(scope_path, scope_uuid, user_uuid, role_uuid)
                action["done"] = True
            except httpx.HTTPError as exc:
                action["error"] = str(exc)

    def import_cluster(self, cluster: dict, projects_filter: Optional[list[str]] = None) -> None:
        """Plan (and with execute, apply) one inventory cluster.

        With ``projects_filter`` only the matching projects are imported,
        and the cluster-scope grants are left out.
        """
        resource_uuid = cluster["resource_uuid"]
        label = f"{cluster['resource_name']} ({cluster['cluster_id']})"
        rps = self.waldur.resource_projects(resource_uuid)
        by_name = {rp["name"]: rp for rp in rps}
        by_backend_id = {rp["backend_id"]: rp for rp in rps if rp.get("backend_id")}
        for project in cluster["projects"]:
            if not project_matches(project, projects_filter):
                continue
            where = f"{label} / {project['name']}"
            if project["system"] and not self.include_system:
                self._record(op="skip", where=where, reason="Rancher system project")
                continue
            if project["duplicate_name"]:
                self._record(
                    op="skip",
                    where=where,
                    reason="several Rancher projects share this name; the operator "
                    "would adopt an arbitrary one -- rename them first",
                )
                continue
            rp = by_backend_id.get(project["rancher_project_id"]) or by_name.get(project["name"])
            if rp and rp["name"] != project["name"]:
                # The operator adopts by name: a second ResourceProject would
                # put two CRs on one Rancher project.
                self._record(
                    op="skip",
                    where=where,
                    reason=f"already linked to resource project {rp['name']!r}; "
                    "rename one of them so the names match",
                )
                continue
            limits, dropped = quota_to_limits(project.get("resource_quota") or {})
            if dropped:
                self._record(
                    op="warn",
                    where=where,
                    reason=f"quota {', '.join(dropped)} has no Waldur limit; "
                    "the operator removes it on the first sync",
                )
            if rp:
                existing_uuid = normalize_uuid(rp["uuid"])
                rp_uuid: Optional[str] = existing_uuid
                self._record(op="exists", where=where, what="resource project")
                if limits and not rp.get("limits"):
                    self._record(
                        op="warn",
                        where=where,
                        reason="the resource project has no limits, so the operator "
                        "clears this project's Rancher quota on the first sync",
                    )
                if not rp.get("backend_id"):
                    self._record_backend_id(existing_uuid, project, where)
            else:
                rp_uuid = self._create_project(resource_uuid, project, limits, where)
            grants = self._grants(
                project["bindings"], self.project_roles, "resource_project", where
            )
            self._apply_grants(grants, "resource_project", rp_uuid)

        if self.cluster_roles and not projects_filter:
            grants = self._grants(
                cluster["cluster_bindings"], self.cluster_roles, "resource", f"{label} / cluster"
            )
            self._apply_grants(grants, "resource", resource_uuid)

    def _create_project(
        self, resource_uuid: str, project: dict, limits: dict, where: str
    ) -> Optional[str]:
        what = "resource project"
        if limits:
            what += " with limits " + ", ".join(f"{k}={v}" for k, v in sorted(limits.items()))
        action = self._record(op="create_project", where=where, what=what)
        if not self.execute:
            return None
        try:
            rp = self.waldur.create_resource_project(
                resource_uuid, project["name"], project["description"], limits
            )
        except httpx.HTTPError as exc:
            action["error"] = str(exc)
            return None
        rp_uuid = normalize_uuid(rp["uuid"])
        action["done"] = True
        self._record_backend_id(rp_uuid, project, where)
        return rp_uuid

    def _record_backend_id(self, rp_uuid: str, project: dict, where: str) -> None:
        """Store the Rancher project ID as the ResourceProject's backend_id."""
        action = self._record(
            op="set_backend_id", where=where, what=f"backend_id {project['rancher_project_id']}"
        )
        if not self.execute:
            return
        try:
            self.waldur.set_resource_project_backend_id(rp_uuid, project["rancher_project_id"])
            action["done"] = True
        except httpx.HTTPError as exc:
            action["error"] = str(exc)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def load_offering(config_path: str, offering: Optional[str]) -> dict:
    """The agent config's offering entry, by name or UUID (or the only one)."""
    with Path(config_path).open() as f:
        config = yaml.safe_load(f) or {}
    offerings = [
        o
        for o in config.get("offerings") or []
        if (o.get("backend_type") == "rancher-kc-crd")
        or (o.get("membership_sync_backend") == "rancher-kc-crd")
    ]
    if offering:
        offerings = [
            o
            for o in offerings
            if offering in (o.get("name"), o.get("waldur_offering_uuid"))
            or normalize_uuid(offering) == normalize_uuid(o.get("waldur_offering_uuid") or "")
        ]
    if len(offerings) != 1:
        msg = (
            f"{len(offerings)} rancher-kc-crd offerings match in {config_path}; "
            "pick one with --offering"
        )
        raise SystemExit(msg)
    return offerings[0]


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "true").lower() == "true"


def _require_env(*names: str) -> dict[str, str]:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise SystemExit(f"Missing environment variable(s): {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


def _waldur_client(offering: dict, token: Optional[str]) -> WaldurApi:
    settings = offering.get("backend_settings") or {}
    url = settings.get("waldur_api_url") or offering["waldur_api_url"]
    token = (
        token
        or os.environ.get("WALDUR_API_TOKEN")
        or settings.get("waldur_api_token")
        or offering.get("waldur_api_token")
    )
    if not token:
        msg = "No Waldur token: pass --waldur-token or set WALDUR_API_TOKEN"
        raise SystemExit(msg)
    verify = settings.get("waldur_verify_ssl", offering.get("verify_ssl", True))
    return WaldurApi(url, token, verify=bool(verify))


def _out(line: str = "") -> None:
    sys.stdout.write(line + "\n")


def cmd_inventory(args: argparse.Namespace) -> int:
    """Step 1: read clusters, bindings and Keycloak users into a JSON file."""
    offering = load_offering(args.config, args.offering)
    identity = resolve_identity_settings(offering.get("backend_settings") or {})
    waldur = _waldur_client(offering, args.waldur_token)
    rancher_env = _require_env("RANCHER_URL", "RANCHER_BEARER_TOKEN")
    kc_env = _require_env(
        "KEYCLOAK_URL", "KEYCLOAK_REALM", "KEYCLOAK_USERNAME", "KEYCLOAK_PASSWORD"
    )
    collector = InventoryCollector(
        RancherApi(
            rancher_env["RANCHER_URL"],
            rancher_env["RANCHER_BEARER_TOKEN"],
            verify=_env_flag("RANCHER_VERIFY_SSL"),
        ),
        KeycloakApi(
            kc_env["KEYCLOAK_URL"],
            kc_env["KEYCLOAK_REALM"],
            kc_env["KEYCLOAK_USERNAME"],
            kc_env["KEYCLOAK_PASSWORD"],
            user_realm=os.environ.get("KEYCLOAK_USER_REALM", "master"),
            client_id=os.environ.get("KEYCLOAK_CLIENT_ID", "admin-cli"),
            verify=_env_flag("KEYCLOAK_VERIFY_SSL"),
        ),
        identity,
    )
    offering_uuid = normalize_uuid(offering["waldur_offering_uuid"])
    all_resources = waldur.resources(offering_uuid)
    resources = select_resources(all_resources, args.cluster, args.resource)
    if not resources:
        available = ", ".join(
            f"{r.get('backend_id')} ({r.get('name')})" for r in select_resources(all_resources)
        )
        msg = f"No resource of the offering matches the filters. Available: {available or 'none'}"
        raise SystemExit(msg)
    clusters = []
    for resource in resources:
        _out(f"Reading cluster {resource['backend_id']} ({resource.get('name')})")
        clusters.append(collector.cluster(resource, args.project))
    inventory = {
        "version": INVENTORY_VERSION,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "offering_uuid": offering_uuid,
        "identity_source": identity.source,
        "clusters": clusters,
    }
    output = Path(args.output)
    # The inventory names users and, in civil_number mode, holds civil codes.
    output.touch(mode=0o600, exist_ok=True)
    output.chmod(0o600)
    output.write_text(json.dumps(inventory, indent=2) + "\n")
    projects = sum(len(c["projects"]) for c in clusters)
    bindings = sum(len(p["bindings"]) for c in clusters for p in c["projects"])
    bindings += sum(len(c["cluster_bindings"]) for c in clusters)
    _out(
        f"Wrote {output}: {len(clusters)} cluster(s), {projects} project(s), {bindings} binding(s)"
    )
    return 0


def print_plan(actions: list[dict], execute: bool) -> None:
    """Print actions grouped by project, then a summary."""
    labels = {
        "create_project": "created" if execute else "create",
        "add_user": "granted" if execute else "grant",
        "set_backend_id": "recorded" if execute else "record",
        "exists": "exists",
        "skip": "skip",
        "warn": "WARN",
    }
    current = None
    for action in actions:
        if action["where"] != current:
            current = action["where"]
            _out(f"\n{current}")
        label = labels[action["op"]]
        if action.get("error"):
            label = "FAILED"
        detail = action.get("what") or ""
        if action.get("user"):
            detail = f"{action['user']} as {action['waldur_role']}"
        elif action.get("rancher_role"):
            detail = f"{action['principal']} ({action['rancher_role']})"
        line = f"  {label:8} {detail}"
        if action.get("reason"):
            line += f" -- {action['reason']}"
        if action.get("error"):
            line += f" -- {action['error']}"
        _out(line)

    counts: dict[str, int] = {}
    for action in actions:
        key = "failed" if action.get("error") else action["op"]
        counts[key] = counts.get(key, 0) + 1
    summary = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
    _out(f"\nSummary: {summary}")
    if not execute:
        _out("Dry run: nothing was changed. Re-run with --execute to apply.")


def cmd_apply(args: argparse.Namespace) -> int:
    """Step 2: create ResourceProjects and user roles in Waldur from an inventory."""
    offering = load_offering(args.config, args.offering)
    settings = offering.get("backend_settings") or {}
    identity = resolve_identity_settings(settings)
    inventory = json.loads(Path(args.inventory).read_text())
    if inventory.get("version") != INVENTORY_VERSION:
        raise SystemExit(f"Unsupported inventory version {inventory.get('version')}")
    offering_uuid = normalize_uuid(offering["waldur_offering_uuid"])
    if inventory.get("offering_uuid") != offering_uuid:
        msg = f"Inventory is for offering {inventory.get('offering_uuid')}, not {offering_uuid}"
        raise SystemExit(msg)
    if inventory.get("identity_source") != identity.source:
        msg = (
            f"Inventory was collected with identity source {inventory.get('identity_source')!r}"
            f" but the config now says {identity.source!r}; re-run inventory"
        )
        raise SystemExit(msg)
    importer = Importer(
        _waldur_client(offering, args.waldur_token),
        offering_uuid,
        settings.get("role_map") or {},
        settings.get("cluster_role_map") or {},
        identity,
        execute=args.execute,
        include_system=args.include_system,
    )
    for cluster in inventory["clusters"]:
        if not resource_matches(
            cluster["resource_uuid"],
            cluster["resource_name"],
            cluster["cluster_id"],
            args.cluster,
            args.resource,
        ):
            continue
        importer.import_cluster(cluster, args.project)
    if not importer.actions:
        _out("Nothing in the inventory matches the filters.")
        return 0
    print_plan(importer.actions, args.execute)
    if args.report:
        Path(args.report).write_text(json.dumps(importer.actions, indent=2) + "\n")
    return 1 if any(a.get("error") for a in importer.actions) else 0


def build_parser() -> argparse.ArgumentParser:
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        prog="waldur-rancher-kc-crd-import",
        description="Import existing Rancher project and cluster access into Waldur.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("-c", "--config", required=True, help="site-agent config file")
        p.add_argument("--offering", help="offering name or UUID, if the config has several")
        p.add_argument(
            "--cluster",
            action="append",
            help="only this Rancher cluster ID (repeatable)",
        )
        p.add_argument(
            "--resource",
            action="append",
            help="only this Waldur resource, by UUID or name (repeatable)",
        )
        p.add_argument(
            "--project",
            action="append",
            help="only this Rancher project, by name or ID (repeatable); "
            "skips cluster-scope bindings",
        )
        p.add_argument("--waldur-token", help="Waldur staff API token (default: WALDUR_API_TOKEN)")

    inv = sub.add_parser("inventory", help="read clusters, bindings and Keycloak users")
    common(inv)
    inv.add_argument("-o", "--output", default="rancher-access-inventory.json")
    inv.set_defaults(func=cmd_inventory)

    apply = sub.add_parser("apply", help="create resource projects and roles in Waldur")
    common(apply)
    apply.add_argument("-i", "--inventory", default="rancher-access-inventory.json")
    apply.add_argument("--execute", action="store_true", help="change Waldur (default: dry run)")
    apply.add_argument(
        "--include-system",
        action="store_true",
        help="also import the Default and System projects (the operator would then own them)",
    )
    apply.add_argument("--report", help="write the actions taken as JSON to this file")
    apply.set_defaults(func=cmd_apply)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """Entry point for ``waldur-rancher-kc-crd-import``."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
