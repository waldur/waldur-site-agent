# Configuration Reference

This document provides a complete reference for configuring Waldur Site Agent. It's a reference,
not a tutorial — if this is your first setup, start with the [Quickstart](quickstart.md) instead
and come back here once something needs a field this page covers but the Quickstart didn't.

**Required in every offering**, regardless of backend: [`name`](#name),
[`waldur_api_url`](#waldur_api_url), [`waldur_offering_uuid`](#waldur_offering_uuid),
[`backend_type`](#backend_type), credentials — either [`waldur_api_token`](#waldur_api_token) or the
three
[OIDC client-credential settings](#oidc-client-credentials-oidc_token_url-oidc_client_id-oidc_client_secret)
— and a `*_backend` setting for each process you run (e.g. `order_processing_backend`). An
offering without any `*_backend` loads without error but is not served: `order_process` skips it,
`membership_sync` logs `Unable to create backend` for it, and `report` stops with that error. Most backends also need
at least one entry under [`backend_components`](#backend-components) to provision limits or
report usage. Everything else on this page — global settings, event processing, resource
management, backend-specific `backend_settings`, and the optional component fields — has a
working default and can be added when you actually need it.

Unknown keys are **ignored silently**, at the top level and inside an offering, so a misspelt
optional setting quietly keeps its default. See [Configuration Validation](configuration-validation.md)
for what is checked and how errors are reported.

## Configuration File Structure

The agent reads a YAML configuration file (default `waldur-site-agent-config.yaml` in the
working directory; set another with `-c`) with the following structure:

```yaml
timezone: "UTC"
log_level: "INFO"
offerings:
  - name: "Example Offering"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<token>"
    waldur_offering_uuid: "<offering UUID>"
    backend_type: "slurm"
    order_processing_backend: "slurm"
    # ... backend_settings, backend_components, other offering settings
```

## Global Settings

Top-level keys of the configuration file.
Generated from the code by `scripts/generate_reference_docs.py`; each key has its own
section below.

<!-- BEGIN GENERATED: global-settings -->
<!-- pyml disable-num-lines 10 line-length -->
| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| [`sentry_dsn`](#sentry_dsn) | `str` | no | — | Sentry DSN for error reporting (URL) |
| [`elastic_apm_server_url`](#elastic_apm_server_url) | `str` | no | — | Elastic APM server URL (enables APM when set) |
| [`timezone`](#timezone) | `str` | no | `UTC` | Timezone for billing calculations |
| [`global_proxy`](#global_proxy) | `str` | no | `""` | Global proxy URL for API connections |
| [`log_level`](#log_level) | `str` | no | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| [`reporting_periods`](#reporting_periods) | `int` | no | `2` | Number of billing periods to report (1=current, 2=current+previous) |
| [`expose_backend_error_details`](#expose_backend_error_details) | `bool` | no | `true` | If True (default), the agent forwards exception messages and tracebacks to Waldur error details when marking objects as ERRED (current behaviour). If False, only BackendError messages are exposed and tracebacks are kept in site-agent logs. |
| [`log_shipping`](#log_shipping) | `LogShippingConfig` | no | see below | Configuration for shipping agent logs to Waldur |
<!-- END GENERATED: global-settings -->

### `sentry_dsn`

- **Type**: String
- **Description**: Data Source Name for Sentry error tracking
- **Default**: Empty (disabled)
- **Example**: `"https://key@sentry.io/project"`

### `elastic_apm_server_url`

- **Type**: String
- **Description**: Elastic APM server URL. When set, enables Elastic APM monitoring with automatic
  instrumentation.
- **Default**: Empty (disabled)
- **Example**: `"https://apm-server.example.com:8200"`

### `timezone`

- **Type**: String (IANA zone name)
- **Description**: Timezone for billing period calculations
- **Default**: `"UTC"`
- **Examples**: `"UTC"`, `"Europe/Tallinn"`, `"America/New_York"`
- **Validation**: An unknown zone name fails configuration loading.

**Note**: Set it to the zone Waldur bills in when agent and Waldur run in different timezones,
otherwise usage near a month boundary can be filed against the wrong billing period.

### `log_level`

- **Type**: String
- **Values**: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (case-insensitive)
- **Default**: `"INFO"`
- **Description**: Level of the agent's own log output.

### `reporting_periods`

- **Type**: Integer, 1–12
- **Default**: `2`
- **Description**: Number of billing periods `report` mode sends usage for, counting back from
  the current one: `1` reports only the current month, `2` also re-sends the previous month so late
  accounting data lands in the right period.

### `expose_backend_error_details`

- **Type**: Boolean
- **Default**: `true`
- **Description**: When the agent marks an order or resource as ERRED, it forwards the exception
  message and traceback to Waldur's error details. With `false`, Waldur gets only the message of a
  `BackendError` raised by the plugin; any other exception is reported as "Internal backend error.
  Please contact the service provider." and tracebacks stay in the agent's own log.

### `log_shipping`

Ships the agent's own log entries to Waldur (`POST /api/marketplace-site-agent-logs/`) so they can
be read without shell access to the agent host. Off by default.

```yaml
log_shipping:
  enabled: true              # default false
  ship_interval_seconds: 60  # >= 10
  buffer_size_mb: 1          # in-memory buffer, >= 1
  log_level: "WARNING"       # minimum level shipped
```

Keys of `log_shipping` (generated from the code):

<!-- BEGIN GENERATED: log-shipping-settings -->
<!-- pyml disable-num-lines 6 line-length -->
| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | `bool` | no | `false` | Enable log shipping to Waldur |
| `ship_interval_seconds` | `int` | no | `60` | Interval between shipments in seconds |
| `buffer_size_mb` | `int` | no | `1` | Maximum in-memory buffer size in megabytes |
| `log_level` | `str` | no | `WARNING` | Minimum log level to ship (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
<!-- END GENERATED: log-shipping-settings -->

Entries below `log_level` are not shipped, so with the default `WARNING` a quiet agent ships
nothing. Shipping uses the same credentials as the offering (static token or OIDC).

### `global_proxy`

- **Type**: String
- **Description**: Proxy for every Waldur API connection the agent makes — polling and event
  mode, the OIDC token request, Waldur-to-Waldur federation calls and the event-mode STOMP
  WebSocket. Supports `http://`, `https://`, `socks5://` and `socks5h://` proxies, with optional
  `user:password@`; any other scheme is a configuration error.
- **Default**: Empty (no proxy)
- **Example**: `"socks5://localhost:12345"`

**STOMP WebSocket**: in event mode the broker WebSocket uses the same proxy for `http://`
(HTTP CONNECT) and `socks5://` / `socks5h://` proxies. As for REST calls, the proxy resolves the
broker's host name and `NO_PROXY` does not bypass it. websocket-client cannot use an `https://`
proxy: with one, REST calls go through it, the agent logs a warning at start-up, and the
WebSocket connects as without `global_proxy` (the `http_proxy` / `https_proxy` environment
variables still apply). Without `global_proxy` the WebSocket honours those environment variables
as before.

## Offering Configuration

Each offering in the `offerings` array represents a separate service offering.

Keys of an offering.
Generated from the code by `scripts/generate_reference_docs.py`; each key has its own
section below.

<!-- BEGIN GENERATED: offering-settings -->
<!-- pyml disable-num-lines 27 line-length -->
| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| [`name`](#name) | `str` | yes | — | Human-readable name for the offering |
| [`waldur_api_url`](#waldur_api_url) | `str` | yes | — | Base URL for the Waldur API endpoint |
| [`waldur_api_token`](#waldur_api_token) | `str` | no | `""` | Authentication token for Waldur API |
| [`waldur_offering_uuid`](#waldur_offering_uuid) | `str` | yes | — | UUID of the offering in Waldur |
| [`oidc_token_url`](#oidc-client-credentials-oidc_token_url-oidc_client_id-oidc_client_secret) | `str` | no | — | OIDC token endpoint URL |
| [`oidc_client_id`](#oidc-client-credentials-oidc_token_url-oidc_client_id-oidc_client_secret) | `str` | no | — | OIDC client ID for token requests |
| [`oidc_client_secret`](#oidc-client-credentials-oidc_token_url-oidc_client_id-oidc_client_secret) | `str` | no | — | OIDC client secret for obtaining access tokens |
| [`backend_type`](#backend_type) | `str` | yes | — | Backend type identifier |
| [`backend_settings`](#backend-specific-settings) | `dict[str, Any]` | no | `{}` | Backend-specific settings |
| [`backend_components`](#backend-components) | `dict[str, BackendComponent]` | no | `{}` | Component definitions |
| [`websocket_use_tls`](#websocket_use_tls) | `bool` | no | `true` | Use TLS for websocket connections |
| [`stomp_enabled`](#stomp_enabled) | `bool` | no | `false` | Enable STOMP event processing |
| [`stomp_membership_sync_enabled`](#stomp_membership_sync_enabled) | `bool` | no | — | Use STOMP for membership sync; defaults to stomp_enabled. Set to false to keep HTTP polling for membership sync even when stomp_enabled=true. |
| [`stomp_ws_host`](#stomp_ws_host-stomp_ws_port-stomp_ws_path) | `str` | no | — | STOMP WebSocket host |
| [`stomp_ws_port`](#stomp_ws_host-stomp_ws_port-stomp_ws_path) | `int` | no | — | STOMP WebSocket port |
| [`stomp_ws_path`](#stomp_ws_host-stomp_ws_port-stomp_ws_path) | `str` | no | — | STOMP WebSocket path |
| [`order_processing_backend`](#backend-selection) | `str` | no | `""` | Backend for order processing |
| [`membership_sync_backend`](#backend-selection) | `str` | no | `""` | Backend for membership sync |
| [`reporting_backend`](#backend-selection) | `str` | no | `""` | Backend for usage reporting |
| [`username_management_backend`](#backend-selection) | `str` | no | `base` | Backend for username management |
| [`resource_import_enabled`](#resource_import_enabled) | `bool` | no | `false` | Enable resource import |
| [`username_reconciliation_enabled`](#username_reconciliation_enabled) | `bool` | no | `false` | Enable periodic username reconciliation from target backend |
| [`preserve_unmanaged_backend_users`](#preserve_unmanaged_backend_users) | `bool` | no | `false` | If False (default), membership sync removes any backend user who is not on the Waldur resource team. If True, users Waldur has ever known as offering users of this offering (any state, including DELETED and restricted) are removed once they leave the team; accounts Waldur has never seen are kept. Applies to local-username backends; ignored for identity-bridge / federation. |
| [`verify_ssl`](#verify_ssl) | `bool` | no | `true` | Verify SSL certificates |
| [`omit_anomalous_usage_components`](#omit_anomalous_usage_components) | `bool` | no | `false` | If False (default), a decreasing component blocks the whole set_usage payload. If True, only the decreasing components are omitted and the rest are still reported. Use True for backends whose meters are independent (e.g. Waldur-to-Waldur). |
<!-- END GENERATED: offering-settings -->

### Basic Settings

#### `name`

- **Type**: String
- **Required**: Yes
- **Description**: Human-readable name for the offering

#### `waldur_api_url`

- **Type**: String
- **Required**: Yes
- **Description**: URL of Waldur API endpoint
- **Example**: `"http://localhost:8081/api/"`

#### `waldur_api_token`

- **Type**: String
- **Required**: Yes, unless OIDC client credentials are configured (see below)
- **Description**: Token for Waldur API authentication
- **Permissions**: The token user must have **OFFERING.MANAGER** role on the offering specified by
  `waldur_offering_uuid`. This grants the permissions needed for order processing, usage reporting,
  membership sync, and event subscriptions.
- **Security**: Keep this secret and secure

#### OIDC client credentials: `oidc_token_url`, `oidc_client_id`, `oidc_client_secret`

- **Type**: String (all three)
- **Required**: Only as an alternative to `waldur_api_token`. Set all three or none; a partial set
  fails configuration validation. When `waldur_api_token` is also set, it takes precedence.
- **Description**: Instead of a static token, the agent can obtain a short-lived JWT from an OIDC
  provider with the client-credentials grant and send it as `Authorization: Bearer <jwt>`. Tokens
  are cached per `(oidc_token_url, oidc_client_id)` and refreshed shortly before they expire; the
  header is resolved on every request, so long polling cycles, CLI runs and log shipping keep
  working after the token they started with has expired. Waldur
  validates the JWT through token introspection (`OIDC_INTROSPECTION_URL`, `OIDC_CLIENT_ID`,
  `OIDC_CLIENT_SECRET` and `OIDC_USER_FIELD` in Waldur's settings); the user it resolves to needs
  the same role as a token user. `global_proxy` and `verify_ssl` also apply to the token request.
- **Limitation**: OIDC-only offerings cannot use STOMP event processing. RabbitMQ authenticates the
  STOMP session with the static API token, so `stomp_enabled: true` without `waldur_api_token`
  fails configuration validation. Use polling mode, or keep a static token for event processing.

```yaml
offerings:
  - name: "OIDC-authenticated offering"
    waldur_api_url: "https://waldur.example.com/api/"
    oidc_token_url: "https://idp.example.com/realms/waldur/protocol/openid-connect/token"
    oidc_client_id: "site-agent"
    oidc_client_secret: "change-me"
    waldur_offering_uuid: "<offering UUID>"
    backend_type: "slurm"
    order_processing_backend: "slurm"
```

#### `verify_ssl`

- **Type**: Boolean
- **Default**: `true`
- **Description**: Whether to verify SSL certificates for Waldur API

#### `waldur_offering_uuid`

- **Type**: String
- **Required**: Yes
- **Description**: UUID of the offering in Waldur
- **Note**: Found in Waldur UI under Integration -> Credentials
- **Supported offering types**: Waldur accepts an agent identity only for an offering whose type
  is `Waldur site agent` (`Marketplace.Slurm`), `Script` (`Marketplace.Script`), `Basic`
  (`Marketplace.Basic`) or `OpenStack tenant` (`OpenStack.Tenant`). Point an agent at any other
  type — a service desk offering, say — and identity registration is refused with a misleading
  `400 Object with uuid=... does not exist`, even though the offering is there. The agent logs a
  warning and carries on syncing without agent telemetry; see
  [Agent Identity Registration Is Refused](deployment.md#agent-identity-registration-is-refused).
  The set of accepted types is a property of the Waldur server, so it can differ between Waldur
  versions.

### Backend Configuration

#### `backend_type`

- **Type**: String
- **Required**: Yes — configuration loading fails without it
- **Description**: Names the plugin whose schemas validate this offering's `backend_settings` and
  `backend_components` (for example `"slurm"`, `"waldur"`, `"litellm"`). Lowercased on load.
  Usually the same name as the `*_backend` settings below. Only `backend_type` selects the schema,
  so in an offering that combines backends (say `litellm` for orders and `litellm-usage` for
  reporting) the settings are validated against the `backend_type` plugin only.

#### Backend Selection

Configure which backends to use for different operations:

```yaml
order_processing_backend: "slurm"    # Backend for order processing
membership_sync_backend: "slurm"     # Backend for membership syncing
reporting_backend: "slurm"           # Backend for usage reporting
username_management_backend: "base"  # Backend for username management (default "base")
```

**Processing backends** (entry point group `waldur_site_agent.backends`; names as installed by the
plugins in this repository):

<!-- pyml disable-num-lines 20 line-length -->
| Name | Plugin package | Purpose |
| ---- | -------------- | ------- |
| `slurm` | `waldur-site-agent-slurm` | SLURM accounts, limits, usage |
| `moab` | `waldur-site-agent-moab` | MOAB Accounting Manager |
| `mup` | `waldur-site-agent-mup` | MUP portal |
| `waldur` | `waldur-site-agent-waldur` | Waldur-to-Waldur federation |
| `rancher` | `waldur-site-agent-rancher` | Rancher projects via the Rancher API |
| `rancher-kc-crd` | `waldur-site-agent-rancher-kc-crd` | Rancher + Keycloak via `ManagedRancherProject` CRDs |
| `k8s-ut-namespace` | `waldur-site-agent-k8s-ut-namespace` | Kubernetes UT `ManagedNamespace` resources |
| `okd` | `waldur-site-agent-okd` | OKD / OpenShift projects |
| `opennebula` | `waldur-site-agent-opennebula` | OpenNebula VDCs and VMs |
| `azure` | `waldur-site-agent-azure` | Azure virtual machines |
| `digitalocean` | `waldur-site-agent-digitalocean` | DigitalOcean droplets |
| `harbor` | `waldur-site-agent-harbor` | Harbor registry projects |
| `nextcloud` | `waldur-site-agent-nextcloud` | Nextcloud |
| `ceph_s3`, `croit_usage` | `waldur-site-agent-ceph-s3` | Ceph S3 users and buckets; croit usage reporting |
| `litellm`, `litellm-usage` | `waldur-site-agent-litellm` | LiteLLM virtual keys; usage reporting |
| `envoy`, `envoy-usage` | `waldur-site-agent-envoy-ai-gateway` | Envoy AI Gateway API keys; usage reporting |
| `cscs-dwdi-compute`, `cscs-dwdi-storage`, `cscs-dwdi-inference` | `waldur-site-agent-cscs-dwdi` | CSCS DWDI usage reporting |
| `ldap-roles` | `waldur-site-agent-ldap-roles` | LDAP group membership driven by Waldur roles |

**Username management backends** (`waldur_site_agent.username_management_backends`): `base`
(`waldur-site-agent-basic-username-management`), `ldap` (`waldur-site-agent-ldap`) and
`waldur-identity-bridge` (`waldur-site-agent-waldur`).

A backend is available only when its plugin package is installed alongside the core package;
`pip install waldur-site-agent` alone installs none. Third-party plugins register under the same
entry point groups — see the [Plugin Development Guide](plugin-development-guide.md).

**Note**: If a `*_backend` setting is omitted, `order_process` skips the offering
(`Order processing is disabled for offering …`), `membership_sync` logs `Unable to create backend
for <offering>` for it, and `report` stops the whole process with the same error. In
`event_process`, the matching event subscriptions are not created.

### Event Processing

#### `stomp_enabled`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Enable STOMP-based event processing
- **Requires**: `waldur_api_token` (OIDC-only offerings cannot authenticate the STOMP session)

#### `stomp_membership_sync_enabled`

- **Type**: Boolean or null
- **Default**: `null` (inherits `stomp_enabled`)
- **Description**: Controls whether membership sync uses STOMP events or HTTP
  polling.  When `stomp_enabled` is `true` this defaults to `true` as well.
  Set to `false` to keep HTTP polling for membership sync while using STOMP for
  order processing.
- **Note**: Setting this to `true` while `stomp_enabled` is `false` leaves
  membership sync with no runner at all — the polling agent skips it (assuming
  STOMP owns it) and the STOMP consumers never start. The agent logs a
  `MISCONFIGURATION` warning on startup if it sees this combination.

#### `websocket_use_tls`

- **Type**: Boolean
- **Default**: `true`
- **Description**: Use TLS for websocket connections

#### `stomp_ws_host`, `stomp_ws_port`, `stomp_ws_path`

- **Type**: String / Integer / String
- **Defaults**: the host of `waldur_api_url`; port `443` when `verify_ssl` is true, otherwise `80`;
  path `/rmqws-stomp`
- **Description**: Where the agent opens the STOMP WebSocket. Override them when RabbitMQ's
  web-STOMP endpoint is not served behind the Waldur API host — for example a development broker
  on `localhost:15674` with path `/ws`. Set `websocket_use_tls` to match the endpoint; the default
  port follows `verify_ssl`, not `websocket_use_tls`.

#### `username_reconciliation_enabled`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Pull usernames that the backend assigns (for example Waldur B's offering users
  in a federation) back into Waldur at the start of each membership pass, and — in `event_process`
  mode — on the reconciliation timer.

### Resource Management

#### `omit_anomalous_usage_components`

- **Type**: Boolean
- **Default**: `false`
- **Description**: A component whose reported usage is lower than what Waldur already holds for
  the period is treated as a data-collection error. With `false` that one component blocks the
  whole usage submission for the resource; with `true` only the decreasing components are left out
  and the rest are reported. Use `true` for backends whose meters are independent of each other
  (for example Waldur-to-Waldur federation).

#### `resource_import_enabled`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Whether to expose importable resources to Waldur

#### `preserve_unmanaged_backend_users`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Controls how membership sync treats backend users who are
  not on the Waldur resource team. When `false` (default), any such user is
  removed. When `true`, users Waldur has ever known as offering users of this
  offering (any state, including `DELETED` and restricted) are removed once
  they leave the team; accounts Waldur has never seen — for example people
  the service provider added locally because Waldur validation blocked their
  offering user — are kept. Applies to every local-username backend
  (SLURM, MOAB, MUP, OKD, Harbor, …). Ignored for identity-bridge /
  Waldur-to-Waldur federation. If the unfiltered offering-user list cannot
  be fetched, sync falls back to removing only users still present in the
  filtered offering-user list for that pass; departed or restricted users
  stay on the backend until the unfiltered list is reachable again, and the
  agent logs which removals were deferred.

## Common Backend Settings

These settings can be used in `backend_settings` for any backend type.

### `check_backend_id_uniqueness`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Enable checking that the generated backend ID is unique
  across offering history before creating a resource. When enabled, the agent
  queries Waldur to verify uniqueness and retries with a new ID on collision.

### `check_all_offerings`

- **Type**: Boolean
- **Default**: `false`
- **Description**: When `check_backend_id_uniqueness` is enabled, check
  uniqueness across all customer offerings instead of only the current offering.

### `backend_id_max_retries`

- **Type**: Integer
- **Default**: `50`
- **Description**: Maximum number of retry attempts when generating a unique
  backend ID. Applies when `check_backend_id_uniqueness` is enabled or the
  `project_slug` account name generation policy is used. Set to a lower value
  if collisions are rare or a higher value for large deployments.

### `soft_delete`

- **Type**: Boolean
- **Default**: `false`
- **Description**: On a terminate order, set every component limit of the backend resource to 0
  instead of deleting it. The account and its data stay on the backend.

### Home directory settings

Backends that create POSIX home directories (SLURM, and the standalone
`waldur_site_create_homedirs` command) read `enable_user_homedir_account_creation` (default
`true`), `default_homedir_umask` (default `"0077"`), `homedir_base_path` (where home directories
live; when unset, the path comes from the system passwd database) and `homedir_quota` (filesystem
quota for each new home directory). `homedir_quota` and the quota providers are described in
[SLURM Storage Quotas](slurm-storage-quotas.md).

### Account name generation vs. resource slug templates

The offering's `account_name_generation_policy` plugin option (set in Waldur,
not in the agent config) controls how the agent derives a resource's backend ID
(e.g. the SLURM account name):

- **Unset (default)** — the agent uses the resource's slug verbatim:
  `{allocation_prefix}{resource_slug}`. If the offering also defines a
  `resource_slug_template` (e.g. `{project_slug}-{counter}`), the slug is already
  unique and is used as-is, with **no extra suffix**.
- **`project_slug`** — the agent **ignores the resource slug** and instead
  derives the backend ID from the *project* slug, appending an incrementing
  `-{counter}` on each collision to disambiguate multiple resources in the same
  project.

> **Warning:** `account_name_generation_policy: project_slug` and
> `resource_slug_template` are two mutually exclusive ways to make backend IDs
> unique. If you set both, the `project_slug` policy wins and appends its own
> counter on top of (and ignoring) your template — producing IDs like
> `prefix-test-prj-01-2-31`. If you use a `resource_slug_template`, leave
> `account_name_generation_policy` **unset** so the unique slug is used directly.

## Backend-Specific Settings

Each plugin's README holds its full settings reference; the blocks below show the common keys.

### SLURM Backend Settings

See the [SLURM plugin README](../plugins/slurm/README.md) for every key, including the REST
execution mode, QoS management and periodic limits.

```yaml
backend_settings:
  default_account: "root"                              # DefaultAccount= on user associations
  # root_account: "root"                               # Parent of top-tier customer account
  # default_account_policy: "common"                   # common (default) | individual | none
  customer_prefix: "hpc_"                              # Prefix for customer accounts
  project_prefix: "hpc_"                               # Prefix for project accounts
  allocation_prefix: "hpc_"                            # Prefix for allocation accounts
  qos_downscaled: "limited"                           # QoS for downscaled accounts
  qos_paused: "paused"                                # QoS for paused accounts
  qos_default: "normal"                               # Default QoS
  enable_user_homedir_account_creation: true         # Create home directories
  default_homedir_umask: "0077"                              # Umask for home directories
```

### MOAB Backend Settings

```yaml
backend_settings:
  default_account: "root"
  customer_prefix: "c_"
  project_prefix: "p_"
  allocation_prefix: "a_"
  enable_user_homedir_account_creation: true
```

### MUP Backend Settings

```yaml
backend_settings:
  api_url: "https://mup.example.com/api/"   # required
  username: "agent-user"                    # required
  password: "secret"                        # required
  project_prefix: "waldur_"                 # default "waldur_"
  allocation_prefix: "alloc_"               # default "alloc_"
  default_research_field: 1                 # default 1
  default_agency: "FCT"                     # default "FCT"
```

The agent refuses to start the MUP backend when `api_url`, `username` or `password` is missing.
The remaining optional keys (default user profile fields and others) are listed in the
[MUP plugin README](../plugins/mup/README.md).

### Waldur Federation Backend Settings

The `target_api_token` user must be a **customer owner** (can be a non-SP customer
separate from the offering's service provider) and an **ISD identity manager**
(`is_identity_manager: true` with `managed_isds` set). Access to the target
offering's users is granted via ISD overlap, not via OFFERING.MANAGER.

```yaml
backend_settings:
  target_api_url: "https://waldur-b.example.com/api/"
  target_api_token: "token-for-waldur-b"  # customer owner + ISD manager
  target_offering_uuid: "offering-uuid-on-waldur-b"
  target_customer_uuid: "customer-uuid-on-waldur-b"
  user_match_field: "cuid"                   # cuid | email | username
  order_poll_timeout: 300                    # Max seconds for sync order completion
  order_poll_interval: 5                     # Seconds between sync order polls
  user_not_found_action: "warn"              # warn | fail
  identity_bridge_source: "isd:efp"          # ISD source for identity bridge
  user_resolve_method: "identity_bridge"     # identity_bridge | remote_eduteams | user_field
  role_mapping:                              # Optional: translate role names A -> B
    PROJECT.ADMIN: PROJECT.ADMIN
    PROJECT.MANAGER: PROJECT.MANAGER
  end_date_sync_direction: "bidirectional"   # a_to_b | b_to_a | bidirectional | disabled
  limit_sync_direction: "b_to_a"             # b_to_a (default) | disabled -- limit sync
  passthrough_attributes: []                 # Offering attribute keys forwarded verbatim to B
  fetch_consented_users_only: false          # Only sync users with data-sharing consent
  # Optional: target STOMP for instant async order completion
  # Requires target_offering_uuid to be a Marketplace.Slurm offering
  target_stomp_enabled: false
```

## Backend Components

Define computing components tracked by the backend:

```yaml
backend_components:
  cpu:
    measured_unit: "k-Hours"           # Waldur measured unit
    unit_factor: 60000                 # Conversion factor
    accounting_type: "usage"           # "usage", "limit", or "one"
    label: "CPU"                       # Display label in Waldur
  mem:
    limit: 10                          # Fixed limit amount
    measured_unit: "gb-Hours"
    unit_factor: 61440                 # 60 * 1024
    accounting_type: "usage"
    label: "RAM"
```

### Component Settings

Keys of a component under `backend_components`.
Generated from the code by `scripts/generate_reference_docs.py`; each key has its own
section below.

<!-- BEGIN GENERATED: component-settings -->
<!-- pyml disable-num-lines 23 line-length -->
| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| [`measured_unit`](#measured_unit) | `str` | yes | — | Unit of measurement (e.g., 'Hours', 'GB') |
| [`unit_factor`](#unit_factor) | `float` | no | `1.0` | Factor for conversion to backend units |
| [`unit_factor_reporting`](#unit_factor_reporting) | `float` | no | — | Factor for unit conversion in reporting mode. Falls back to unit_factor if not set. |
| [`accounting_type`](#accounting_type) | `usage \| limit \| one` | yes | — | Component accounting type |
| [`label`](#label) | `str` | yes | — | Human-readable label for display |
| [`limit`](#limit) | `float` | no | — | Component limit value |
| [`description`](#description) | `str` | no | — | Description of the component |
| [`min_value`](#min_value) | `int` | no | — | Minimum allowed value |
| [`max_value`](#max_value) | `int` | no | — | Maximum allowed value |
| [`max_available_limit`](#max_available_limit) | `int` | no | — | Maximum available limit |
| [`default_limit`](#default_limit) | `int` | no | — | Default limit value |
| [`limit_period`](#limit_period) | `str` | no | — | Limit period: annual, month, quarterly, total |
| [`article_code`](#article_code) | `str` | no | — | Article code for billing |
| [`is_boolean`](#is_boolean) | `bool` | no | — | Whether the component is boolean |
| [`is_prepaid`](#is_prepaid) | `bool` | no | — | Whether the component is prepaid |
| [`min_prepaid_duration`](#min_prepaid_duration) | `int` | no | — | Minimum prepaid duration in months |
| [`max_prepaid_duration`](#max_prepaid_duration) | `int` | no | — | Maximum prepaid duration in months |
| [`prepaid_duration_step`](#prepaid_duration_step) | `int` | no | — | Step size in months for initial prepaid duration |
| [`min_renewal_duration`](#min_renewal_duration) | `int` | no | — | Minimum renewal duration in months |
| [`max_renewal_duration`](#max_renewal_duration) | `int` | no | — | Maximum renewal duration in months |
| [`renewal_duration_step`](#renewal_duration_step) | `int` | no | — | Step size in months for renewal duration |
<!-- END GENERATED: component-settings -->

#### `measured_unit`

- **Type**: String
- **Description**: Unit displayed in Waldur
- **Examples**: `"k-Hours"`, `"gb-Hours"`, `"EUR"`

#### `unit_factor`

- **Type**: Number
- **Default**: `1.0`
- **Description**: Factor for conversion from Waldur units to backend units
- **Examples**:
  - `60000` for CPU (60 * 1000, converts k-Hours to CPU-minutes)
  - `61440` for memory (60 * 1024, converts gb-Hours to MB-minutes)

#### `unit_factor_reporting`

- **Type**: Number
- **Optional**: Yes
- **Description**: Factor used instead of `unit_factor` when converting backend usage back to
  Waldur units, falling back to `unit_factor` when unset. Only the CSCS DWDI reporting backends
  read it; every other backend uses `unit_factor` for both directions.

#### `accounting_type`

- **Type**: String
- **Values**: `"usage"`, `"limit"`, or `"one"`
- **Description**: Controls billing type and backend behavior.
  `"usage"` for usage-based tracking, `"limit"` for fixed
  allocation caps, `"one"` for prepaid ONE_TIME billing
  (automatically sets `is_prepaid: true` in Waldur).

#### `label`

- **Type**: String
- **Description**: Human-readable label displayed in Waldur

#### `limit`

- **Type**: Number
- **Optional**: Yes
- **Description**: Fixed limit amount for limit-type components

#### `description`

- **Type**: String
- **Optional**: Yes
- **Description**: Description of the component shown in Waldur

#### `min_value`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Minimum allowed value for the component

#### `max_value`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Maximum allowed value for the component

#### `max_available_limit`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Maximum available limit for the component

#### `default_limit`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Default limit value applied when creating a resource

#### `limit_period`

- **Type**: String
- **Optional**: Yes
- **Values**: `"annual"`, `"month"`, `"quarterly"`, `"total"`
- **Description**: Billing period for limit enforcement

#### `article_code`

- **Type**: String
- **Optional**: Yes
- **Description**: Article code for billing system integration

#### `is_boolean`

- **Type**: Boolean
- **Optional**: Yes
- **Description**: Whether the component represents a boolean (on/off) option

#### `is_prepaid`

- **Type**: Boolean
- **Optional**: Yes
- **Description**: Whether the component requires prepaid billing.
  Automatically set to `true` when `accounting_type: "one"`.

#### `min_prepaid_duration`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Minimum initial prepaid duration in months. Only applies when `accounting_type: "one"`.

#### `max_prepaid_duration`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Maximum initial prepaid duration in months. Only applies when `accounting_type: "one"`.

#### `prepaid_duration_step`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Step size in months for initial duration.
  If set, only multiples of this value
  (starting from `min_prepaid_duration`) are valid.
  For example, `min_prepaid_duration: 3` and
  `prepaid_duration_step: 3` allows 3, 6, 9, 12 months.

#### `min_renewal_duration`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Minimum renewal duration in months.

#### `max_renewal_duration`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Maximum renewal duration in months.

#### `renewal_duration_step`

- **Type**: Integer
- **Optional**: Yes
- **Description**: Step size in months for renewal.
  Only multiples of this value
  (starting from `min_renewal_duration`) are valid.

### Prepaid Billing

Prepaid billing allows customers to pay upfront for a fixed
capacity over a specified duration.
Prepaid components use `accounting_type: "one"` which maps
to Waldur's ONE_TIME billing type and automatically sets
`is_prepaid: true`.

When a component has `accounting_type: "one"`,
the following flow applies:

1. **Ordering**: Customer orders a resource with limits
   and an `end_date`. Waldur validates the duration
   against component constraints.
2. **Upfront billing**: Waldur creates a single invoice
   item for the full subscription period
   (limit × price × months).
3. **Backend enforcement**: The site agent calculates
   `GrpTRESMins = limit × duration_months × unit_factor`
   and sets it on the SLURM account. This gives SLURM
   a cumulative budget cap for the subscription period.
4. **Limit changes**: Customer can request more capacity.
   Waldur creates supplementary invoice items.
   The agent recalculates GrpTRESMins with the new
   limits and remaining duration.
5. **Renewal**: Customer extends the subscription.
   The agent detects the new `end_date` and
   recalculates GrpTRESMins with the extended duration.
6. **Termination**: When `end_date` is reached,
   Waldur automatically creates a TERMINATE order.

### Backend-Specific Component Notes

**SLURM**: Supports `cpu`, `mem`, and other custom components

**MOAB**: Only supports `deposit` component

```yaml
backend_components:
  deposit:
    measured_unit: "EUR"
    accounting_type: "limit"
    label: "Deposit (EUR)"
```

## Environment Variables

These are read from the environment, not from the configuration file. All are optional.

<!-- pyml disable-num-lines 11 line-length -->
| Variable | Default | Used by | Meaning |
| -------- | ------- | ------- | ------- |
| `WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES` | `5` | `order_process` | Minutes between order-processing cycles. Accepts a fraction (`0.5`). |
| `WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES` | `5` | `membership_sync` | Minutes between membership cycles. Whole number. |
| `WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` | `30` | `report` | Minutes between reporting cycles. Whole number. |
| `WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES` | `60` | `event_process` | Minutes between the periodic reconciliation passes (see [Architecture](architecture.md#periodic-reconciliation)). Whole number. |
| `WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES` | `15` | `event_process` | How long a STOMP consumer may stay down before the agent stops touching its liveness heartbeat (see [Architecture](architecture.md#stomp-connection-watchdog-and-liveness)). Accepts a fraction. |
| `WALDUR_SITE_AGENT_STOMP_HANDLER_STUCK_AFTER_MINUTES` | `30` | `event_process` | A message handler running longer than this counts as a stuck queue and, past the STOMP unhealthy threshold, withholds the liveness heartbeat (see [Architecture](architecture.md#message-handling-and-acknowledgement)). Accepts a fraction. |
| `WALDUR_SITE_AGENT_RESOURCE_STATUS_RECONCILIATION_MINUTES` | `60` | `event_process` | Minutes between passes that re-apply every resource's paused/downscaled status (see [Architecture](architecture.md#resource-status-reconciliation)). `0` disables it. Accepts a fraction. |
| `WALDUR_SITE_AGENT_HEARTBEAT_PATH` | `/tmp/waldur-site-agent-heartbeat` | all modes, `waldur_site_healthz` | File the agent touches as its liveness heartbeat and the probe reads. Give each agent process sharing a `/tmp` its own path. |
| `SENTRY_ENVIRONMENT` | — | Sentry SDK | Environment tag on Sentry events; read by the Sentry SDK itself when `sentry_dsn` is set. |

A value that is not a number makes the agent fail at start-up; the three "whole number" variables
also reject a fraction such as `2.5`.

## Example Configurations

### SLURM Cluster

```yaml
sentry_dsn: ""
timezone: "UTC"
offerings:
  - name: "HPC SLURM Cluster"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your-api-token"
    verify_ssl: true
    waldur_offering_uuid: "uuid-from-waldur"

    backend_type: "slurm"
    order_processing_backend: "slurm"
    membership_sync_backend: "slurm"
    reporting_backend: "slurm"
    username_management_backend: "base"

    resource_import_enabled: true
    stomp_enabled: false

    backend_settings:
      default_account: "root"
      customer_prefix: "hpc_"
      project_prefix: "hpc_"
      allocation_prefix: "hpc_"
      qos_default: "normal"
      enable_user_homedir_account_creation: true
      default_homedir_umask: "0077"

    backend_components:
      cpu:
        measured_unit: "k-Hours"
        unit_factor: 60000
        accounting_type: "usage"
        label: "CPU"
      mem:
        measured_unit: "gb-Hours"
        unit_factor: 61440
        accounting_type: "usage"
        label: "RAM"
```

### MOAB Cluster

```yaml
offerings:
  - name: "MOAB Cluster"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your-api-token"
    waldur_offering_uuid: "uuid-from-waldur"

    backend_type: "moab"
    order_processing_backend: "moab"
    membership_sync_backend: "moab"
    reporting_backend: "moab"
    username_management_backend: "base"

    backend_settings:
      default_account: "root"
      customer_prefix: "c_"
      project_prefix: "p_"
      allocation_prefix: "a_"

    backend_components:
      deposit:
        measured_unit: "EUR"
        accounting_type: "limit"
        label: "Deposit (EUR)"
```

### Event-Based Processing

Run with `-m event_process`. Each `*_backend` set here decides which events the agent subscribes
to: orders need `order_processing_backend`, and role, resource, offering-user and account events
need `membership_sync_backend`. Leaving `membership_sync_backend` out does not move membership to
polling — it switches membership sync off for the offering. To keep membership on HTTP polling
while orders arrive over STOMP, set `stomp_membership_sync_enabled: false` and run a separate
`membership_sync` agent for the offering.

```yaml
offerings:
  - name: "Event-Driven SLURM"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your-api-token"   # STOMP needs a static token, not OIDC
    waldur_offering_uuid: "uuid-from-waldur"
    backend_type: "slurm"

    stomp_enabled: true
    websocket_use_tls: true

    order_processing_backend: "slurm"
    membership_sync_backend: "slurm"
    reporting_backend: "slurm"            # reporting still needs a separate `report` agent

    backend_settings:
      default_account: "root"
      customer_prefix: "hpc_"
      project_prefix: "hpc_"
      allocation_prefix: "hpc_"

    backend_components:
      cpu:
        measured_unit: "k-Hours"
        unit_factor: 60000
        accounting_type: "usage"
        label: "CPU"
```

### Waldur-to-Waldur Federation

```yaml
offerings:
  - name: "Federated HPC Access"
    waldur_api_url: "https://waldur-a.example.com/api/"
    waldur_api_token: "token-for-waldur-a"
    waldur_offering_uuid: "offering-uuid-on-waldur-a"
    backend_type: "waldur"
    order_processing_backend: "waldur"
    membership_sync_backend: "waldur"
    reporting_backend: "waldur"

    # Optional: STOMP event processing
    stomp_enabled: true
    websocket_use_tls: true

    backend_settings:
      target_api_url: "https://waldur-b.example.com/api/"
      target_api_token: "token-for-waldur-b"  # customer owner + ISD manager
      target_offering_uuid: "offering-uuid-on-waldur-b"
      target_customer_uuid: "customer-uuid-on-waldur-b"
      user_match_field: "cuid"
      order_poll_timeout: 300
      order_poll_interval: 5
      user_not_found_action: "warn"
      target_stomp_enabled: true

    backend_components:
      node_hours:
        measured_unit: "Node-hours"
        unit_factor: 1.0
        accounting_type: "limit"
        label: "Node Hours"
        target_components:
          cpu_k_hours:
            factor: 128.0
      tb_hours:
        measured_unit: "TB-hours"
        unit_factor: 1.0
        accounting_type: "limit"
        label: "TB Hours"
        target_components:
          gb_k_hours:
            factor: 1.0
```

## Validation

Validate your configuration:

```bash
# Load the file, query Waldur with each offering's credentials, and run the diagnostics of the
# offering's order processing backend (for SLURM: the binaries and `sinfo -V`).
waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml

# Create the offering's components in Waldur from backend_components
waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```

`waldur_site_diagnostics` is designed to exit 1 when an order processing backend's own diagnostics
fail or its `cluster_name` does not match the offering's `backend_id` in Waldur. Waldur API errors
are logged as errors; when the offering itself cannot be fetched (a rejected token, an unknown
offering UUID) the command then stops with a Python traceback, which also exits non-zero. A failure
fetching orders is only logged. Read the output, not just the exit status. Membership and
reporting backends are not checked.

Loading alone does not catch everything: unknown keys are ignored and a plugin settings schema
failure only logs a warning. [Configuration Validation](configuration-validation.md) explains which
errors stop the agent and which do not.
