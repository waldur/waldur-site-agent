# Keycloak client for Waldur Site Agent plugins

A shared library, not a backend: it registers no entry point. It wraps
`python-keycloak` with the group and membership operations the rancher,
k8s-ut-namespace and opennebula plugins need when they manage access through
Keycloak groups.

## Settings

The plugins read these keys from a nested `keycloak:` block of their
`backend_settings`, and turn the integration on with `keycloak_enabled: true`.
The block is validated by
`waldur_site_agent_keycloak_client.schemas.KeycloakSettingsSchema`, so a
misspelt key is logged as a warning when the agent loads its configuration.

| Setting | Default | Description |
|---|---|---|
| `keycloak_url` | `https://localhost/auth/` | Keycloak base URL |
| `keycloak_realm` | `waldur` | Realm the groups are managed in |
| `keycloak_user_realm` | `master` | Realm the admin user authenticates against |
| `client_id` | `admin-cli` | Client used for the admin login |
| `keycloak_username` | empty | Admin username |
| `keycloak_password` | empty | Admin password |
| `keycloak_ssl_verify` | `true` | Verify Keycloak's TLS certificate; a path names a CA bundle |

<!-- docs-check: skip -->

```yaml
backend_settings:
  keycloak_enabled: true
  keycloak:
    keycloak_url: "https://keycloak.example.com/auth/"
    keycloak_realm: "waldur"
    keycloak_user_realm: "master"
    keycloak_username: "admin"
    keycloak_password: "<password>"
```

Used by: [rancher](../rancher/README.md),
[k8s-ut-namespace](../k8s-ut-namespace/README.md),
[opennebula](../opennebula/README.md).

## Tests

```bash
cd plugins/keycloak-client && uv run pytest tests/
```
