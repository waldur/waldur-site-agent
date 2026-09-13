# waldur-site-agent-ldap-client

Shared LDAP client for Waldur Site Agent plugins. Wraps `ldap3` with
helpers for POSIX user/group management (search, ID allocation,
add/remove group members, group lifecycle).

Used by:

- `waldur-site-agent-ldap` — username provisioning
- `waldur-site-agent-ldap-roles` — Resource/ResourceProject role-driven
  group membership sync

This package is a utility library; it does not register a backend
entry point.
