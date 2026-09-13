# waldur-site-agent-ldap-roles

Membership-sync backend that maps **Waldur Resource and ResourceProject
roles** to **LDAP group memberships**.

Sibling of `waldur-site-agent-rancher-kc-crd` — same data model
(Resource + ResourceProject as separate aggregations), same
role-mapping shape, same per-cycle reconciliation, but the destination
is OpenLDAP instead of Kubernetes CRDs.

For each Waldur Resource owned by the offering:

- Resource-level role assignments produce one LDAP group per
  `(role, resource)` pair (template:
  `${resource_slug}_${role_name}`).
- ResourceProject-level role assignments produce one LDAP group per
  `(role, resource_project)` pair (template:
  `${resource_slug}_${rp_uuid_short}_${role_name}`).

Group naming templates are configurable per offering. Roles outside the
configured `role_map` are ignored. Users not present in LDAP are skipped
with a warning — sync continues.

## Resource lifecycle

`ldap-roles` must also be the offering's `order_processing_backend`:

- **Create** records the resource's backend ID (its slug) and writes nothing
  to LDAP yet. Membership sync only visits resources that have a backend ID,
  so without this a new resource is never synced.
- **Terminate** empties every group the resource owns. After termination
  membership sync no longer visits the resource, so this is the last point
  at which its grants can be revoked. Any LDAP failure fails the terminate
  order, so it can be retried instead of succeeding with access still in
  place.

## Which roles

The role-map keys are names of **offering roles**: roles a service provider
defines on the offering with `Resource` or `Resource project` scope
(`POST /api/marketplace-offering-roles/`, `content_type_input` `resource` or
`resource_project`). Waldur only grants a role on a scope of its own type, so
project or offering roles such as `PROJECT.ADMIN` or `OFFERING.MANAGER` never
appear on a Resource or ResourceProject — a map keyed by them silently matches
nothing. ResourceProjects also need `enable_resource_projects: true` in the
offering's `plugin_options`.

## Group ownership and revocation

Every group the backend creates carries an ownership marker in its
`description`: `managed_by=<managed_by_tag>;resource=<resource uuid>`.

- **Only marked groups are touched.** If a group with the rendered name
  already exists without this resource's marker — created by an
  administrator, the `ldap` plugin, or another resource whose template
  renders the same name — it is left alone and a warning is logged.
  Adding the marker value to the group's `description` by hand hands it
  over to the agent.
- **Revocation.** Each cycle, every group marked for the resource that is
  no longer in the desired state is emptied: the last holder of its role
  was revoked, its ResourceProject was deleted, or its role was removed
  from the role map.
- **Groups are emptied, never deleted.** GIDs are allocated as the lowest
  free number in range, so a deleted group's GID — and every file still
  owned by it — would pass to the next group created.
- A failed Waldur API read aborts the cycle before any group is changed.

## Per-grant sync status

With `enable_membership_sync_status: true` in the offering's `plugin_options`,
the agent reports after each sync whether every mapped grant actually landed,
and Waldur shows it next to the grant in the resource's team list:

| State | When |
|---|---|
| `synced` | The user is a member of the grant's group. |
| `missing_in_idp` | The user has no LDAP entry, so the grant is skipped. |
| `error` | The group is not the agent's (see below), or an LDAP write failed; the message says which. |

Roles absent from the role maps reach no group and are not reported. `pending`
is never used: LDAP changes apply within the sync.

## Membership types

`membership_type` picks the attribute a group lists its members in, and the
group's object classes must allow it:

| `membership_type` | Attribute value | Needs in `ldap.project_group_object_classes` | Default classes |
|---|---|---|---|
| `memberUid` | `alice` | `posixGroup` | `posixGroup`, `top` |
| `member` | `uid=alice,<people_ou>,<base_dn>` | `groupOfNames` | `groupOfNames`, `top` |

A mismatch is refused at startup rather than failing on every group.

- **A plain `groupOfNames` has no GID.** On directories where `posixGroup` is
  auxiliary (rfc2307bis, 389-DS), list both classes to get one. With the stock
  nis schema `posixGroup` is structural and the two cannot be combined.
- **`groupOfNames` must always have a member**, so these groups are created
  with a stand-in, `ldap.empty_group_member_dn` (default
  `cn=nobody,<base_dn>`). It keeps a group valid while no user holds its role,
  and it replaces the last member when that member is revoked from a group that
  does not have it yet (for example, one created by hand). It is not a user: it
  is never listed as a member or removed, and it must not be a `uid=` DN.

## Configuration

```yaml
backend_type: ldap-roles
order_processing_backend: ldap-roles   # required, see "Resource lifecycle"
membership_sync_backend: ldap-roles
backend_settings:
  waldur_api_url: https://waldur.example.com/api/
  waldur_api_token: <token>

  # Group naming
  resource_group_template: "${resource_slug}_${role_name}"
  resource_project_group_template: "${resource_slug}_${rp_uuid_short}_${role_name}"

  # Waldur role name -> output role token (used as ${role_name} in templates).
  # Roles absent from these maps are ignored. See "Which roles" below.
  resource_role_map:
    RESOURCE.ADMIN: admin
  resource_project_role_map:
    RESOURCE_PROJECT.ADMIN: admin
    RESOURCE_PROJECT.MEMBER: member

  # LDAP membership attribute: "memberUid" (POSIX) or "member" (DN-based).
  membership_type: memberUid

  # Tag in the ownership marker written to the groups this backend creates
  # (see "Group ownership and revocation").
  managed_by_tag: "waldur-site-agent"

  ldap:
    uri: ldaps://ldap.example.com
    bind_dn: cn=admin,dc=example,dc=com
    bind_password: <secret>
    base_dn: dc=example,dc=com
    people_ou: ou=People
    groups_ou: ou=Groups
```

## What this plugin does NOT do

- Does not provision or update LDAP **user** entries — pair this plugin
  with `waldur-site-agent-ldap` (or another username-management
  backend) if user provisioning is needed.
- Does not enforce quotas, run usage reports, or manage resource state
  transitions — it is membership-sync only.
