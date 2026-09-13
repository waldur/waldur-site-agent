"""Translate Waldur (Resource, ResourceProject, [UserRole]) into LDAP group memberships.

Pure functions only — no I/O. The backend module wires this into the
Waldur SDK and the LdapClient.
"""

from __future__ import annotations

from string import Template
from typing import NamedTuple, Optional


class DesiredGroup(NamedTuple):
    """A single LDAP group that should exist with a given member set.

    ``members`` are user identifiers as the backend will resolve them
    against LDAP — usernames by default, UUIDs when
    ``lookup_by_user_uuid`` is enabled. The backend itself decides how
    to translate the identifier to an LDAP attribute value
    (``memberUid`` vs ``member``).
    """

    name: str
    members: frozenset[str]


class Grant(NamedTuple):
    """One mapped role grant and the LDAP group it lands in.

    ``member`` is the identifier resolved against LDAP (see
    ``DesiredGroup``); ``role_name`` is the Waldur role name, which the
    per-grant sync report is keyed by. ``resource_project_uuid`` is set
    only for ResourceProject-scope grants.
    """

    scope_type: str  # "resource" or "resource_project"
    resource_project_uuid: Optional[str]
    role_name: str
    member: str
    group: str


def render_group_name(
    template: str,
    *,
    role_name: str,
    resource_slug: str = "",
    rp_uuid: str = "",
    rp_uuid_short: str = "",
    project_name: str = "",
    customer_slug: str = "",
    project_slug: str = "",
) -> str:
    """Render a group name from a string.Template.

    Variables (all optional, missing ones substitute to empty):

    - ``${role_name}`` — output role token (post-mapping).
    - ``${resource_slug}`` — Waldur Resource slug.
    - ``${rp_uuid}`` — Waldur ResourceProject UUID, full 32-char hex.
    - ``${rp_uuid_short}`` — first 8 hex chars of ``rp_uuid``.
    - ``${customer_slug}`` — Waldur Customer (organization) slug.
    - ``${project_slug}`` — slug of the parent Waldur Project that
      owns the Resource (NOT the ResourceProject — those have no slug).
    - ``${project_name}`` — human-readable ResourceProject name.

    For ResourceProject-scope groups the template should include some
    per-RP discriminator (``${rp_uuid_short}`` is the recommended
    short form) so each (resource x resource-project x role) triple
    gets its own group; otherwise N RPs collapse onto one group.
    """
    return Template(template).safe_substitute(
        role_name=role_name,
        resource_slug=resource_slug,
        rp_uuid=rp_uuid,
        rp_uuid_short=rp_uuid_short,
        project_name=project_name,
        customer_slug=customer_slug,
        project_slug=project_slug,
    )


def build_groups(
    user_roles: list[dict],
    *,
    template: str,
    role_map: dict[str, str],
    lookup_by_user_uuid: bool = False,
    resource_slug: str = "",
    rp_uuid: str = "",
    rp_uuid_short: str = "",
    project_name: str = "",
    customer_slug: str = "",
    project_slug: str = "",
) -> list[DesiredGroup]:
    """Group UserRoles by role name and emit one DesiredGroup per role.

    ``user_roles`` is a list of dicts with at minimum ``role_name``
    plus a user identifier (``user_uuid`` or ``user_username``).
    Roles absent from ``role_map`` are dropped silently — the caller
    is expected to log if it cares.

    A role with no resolvable members produces no group at all (no
    point creating an empty group on the LDAP side just to delete it
    later).
    """
    by_role: dict[str, list[dict]] = {}
    for ur in user_roles:
        role = ur.get("role_name")
        if not role or role not in role_map:
            continue
        by_role.setdefault(role, []).append(ur)

    groups: list[DesiredGroup] = []
    for role_name in sorted(by_role):
        members: set[str] = set()
        for ur in by_role[role_name]:
            ident = ur.get("user_uuid") if lookup_by_user_uuid else ur.get("user_username")
            if ident:
                members.add(ident)
        if not members:
            continue
        rendered = render_group_name(
            template,
            role_name=role_map[role_name],
            resource_slug=resource_slug,
            rp_uuid=rp_uuid,
            rp_uuid_short=rp_uuid_short,
            project_name=project_name,
            customer_slug=customer_slug,
            project_slug=project_slug,
        )
        groups.append(DesiredGroup(name=rendered, members=frozenset(members)))
    return groups


def build_desired_state(
    *,
    resource: dict,
    resource_user_roles: list[dict],
    resource_project_user_roles: list[tuple[dict, list[dict]]],
    settings: dict,
) -> list[DesiredGroup]:
    """Combine Resource-scope and ResourceProject-scope groups into one set.

    ``resource`` is a dict with ``slug``, ``customer_slug``,
    ``project_slug``.

    ``resource_user_roles`` are the role assignments on the Resource
    itself (from ``marketplace_provider_resources_list_users_list``).

    ``resource_project_user_roles`` is a list of
    ``(resource_project_dict, user_roles)`` tuples — one per RP owned
    by this Resource.

    The two scopes use independent templates and role maps. Groups
    with the same rendered name across scopes are merged (members
    union); the caller's choice of templates determines whether that
    happens.
    """
    return group_grants(
        build_grants(
            resource=resource,
            resource_user_roles=resource_user_roles,
            resource_project_user_roles=resource_project_user_roles,
            settings=settings,
        )
    )


def build_grants(
    *,
    resource: dict,
    resource_user_roles: list[dict],
    resource_project_user_roles: list[tuple[dict, list[dict]]],
    settings: dict,
) -> list[Grant]:
    """Resolve every mapped role grant to the group it belongs in.

    Same inputs as ``build_desired_state``. Grants whose role is absent
    from its scope's role map, or that carry no user identifier, are
    dropped — they reach no group and are not reported.
    """
    lookup_by_user_uuid = settings.get("lookup_by_user_uuid", False)
    names = {
        "resource_slug": resource.get("slug") or "",
        "customer_slug": resource.get("customer_slug") or "",
        "project_slug": resource.get("project_slug") or "",
    }
    grants: list[Grant] = []

    resource_role_map = settings.get("resource_role_map") or {}
    if resource_role_map:
        grants.extend(
            _scope_grants(
                resource_user_roles,
                template=settings.get("resource_group_template", "${resource_slug}_${role_name}"),
                role_map=resource_role_map,
                lookup_by_user_uuid=lookup_by_user_uuid,
                scope_type="resource",
                resource_project_uuid=None,
                names=names,
            )
        )

    rp_role_map = settings.get("resource_project_role_map") or {}
    if rp_role_map:
        rp_template = settings.get(
            "resource_project_group_template",
            "${resource_slug}_${rp_uuid_short}_${role_name}",
        )
        for rp, urs in resource_project_user_roles:
            rp_uuid = rp.get("uuid") or ""
            grants.extend(
                _scope_grants(
                    urs,
                    template=rp_template,
                    role_map=rp_role_map,
                    lookup_by_user_uuid=lookup_by_user_uuid,
                    scope_type="resource_project",
                    resource_project_uuid=rp_uuid,
                    names={
                        **names,
                        "rp_uuid": rp_uuid,
                        "rp_uuid_short": rp_uuid.replace("-", "")[:8],
                        "project_name": rp.get("name") or "",
                    },
                )
            )
    return grants


def _scope_grants(
    user_roles: list[dict],
    *,
    template: str,
    role_map: dict[str, str],
    lookup_by_user_uuid: bool,
    scope_type: str,
    resource_project_uuid: Optional[str],
    names: dict[str, str],
) -> list[Grant]:
    grants = []
    for ur in user_roles:
        role = ur.get("role_name")
        if not role or role not in role_map:
            continue
        ident = ur.get("user_uuid") if lookup_by_user_uuid else ur.get("user_username")
        if not ident:
            continue
        group = render_group_name(template, role_name=role_map[role], **names)
        grants.append(Grant(scope_type, resource_project_uuid, role, ident, group))
    return grants


def group_grants(grants: list[Grant]) -> list[DesiredGroup]:
    """Collapse grants into one DesiredGroup per group name, sorted by name.

    Grants from both scopes that render the same name share a group.
    """
    members: dict[str, set[str]] = {}
    for grant in grants:
        members.setdefault(grant.group, set()).add(grant.member)
    return [DesiredGroup(name=name, members=frozenset(m)) for name, m in sorted(members.items())]


def diff_members(
    current: list[str],
    desired: frozenset[str],
) -> tuple[list[str], list[str]]:
    """Compute the (to_add, to_remove) sets for one group.

    Pure set diff. The backend handles user-not-in-LDAP errors and
    membership-type translation — this function only deals with the
    identifier strings.
    """
    current_set = set(current)
    to_add = sorted(desired - current_set)
    to_remove = sorted(current_set - desired)
    return to_add, to_remove


def cluster_user_roles_dicts(  # pragma: no cover - trivial passthrough
    raw: Optional[list[dict]],
) -> list[dict]:
    """Defensive coercion: list[dict] | None -> list[dict].

    Mirrors the rancher-kc-crd convention so backend code doesn't have
    to special-case None.
    """
    return list(raw) if raw else []
