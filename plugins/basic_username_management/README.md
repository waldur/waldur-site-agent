# Basic username management plugin for Waldur Site Agent

The default username management backend. It generates no usernames and looks
none up: offering users keep whatever username Waldur already holds for them.
Use it when usernames are assigned elsewhere (by Waldur, or by the service
provider by hand) and the agent only needs to read them.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `base` | `waldur_site_agent.username_management_backends` | username management |

| Operation | Behaviour |
|---|---|
| Generate a username | **No-op** — returns an empty string, so no username is set |
| Look up an existing username | **No-op** — returns `None` |

`base` is the value `username_management_backend` takes when an offering does
not set it, so this package must be installed alongside the agent even when no
other username backend is used. For generated usernames see the
[LDAP plugin](../ldap/README.md); for Waldur-to-Waldur federation see the
[Waldur plugin](../waldur/README.md) (`waldur-identity-bridge`).

## Configuration

Nothing to configure. To select it explicitly:

<!-- docs-check: skip -->

```yaml
offerings:
  - name: "..."
    username_management_backend: "base"
```
