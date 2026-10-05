# LDAP client for Waldur Site Agent plugins

A shared library, not a backend: it registers no entry point. It wraps `ldap3`
with the POSIX user and group operations the LDAP-aware plugins share — search,
UID/GID allocation, group lifecycle, adding and removing group members.

Used by:

- [`waldur-site-agent-ldap`](../ldap/README.md) — username provisioning
- [`waldur-site-agent-ldap-roles`](../ldap-roles/README.md) — group membership
  driven by Waldur resource and resource-project roles
- [`waldur-site-agent-slurm`](../slurm/README.md) — the SLURM backend's LDAP
  integration, installed with the `ldap` extra (`waldur-site-agent-slurm[ldap]`)

The connection settings live in each plugin's `ldap:` block; the
[LDAP plugin README](../ldap/README.md#configuration) describes them.

## Tests

```bash
cd plugins/ldap-client && uv run pytest tests/
```

The live tests need an LDAP server and are skipped without one.
