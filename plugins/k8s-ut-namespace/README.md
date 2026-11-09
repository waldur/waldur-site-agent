# Kubernetes ManagedNamespace plugin for Waldur Site Agent

This plugin integrates Waldur Site Agent with Kubernetes clusters by managing `ManagedNamespace`
custom resources (CRD: `provisioning.hpc.ut.ee/v1`). A Waldur resource becomes a namespace with a
`ResourceQuota`, optionally with Keycloak RBAC groups, optionally with a Pod Security level, and
optionally billed by the consumption Prometheus measured over the billing period.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `k8s-ut-namespace` | `waldur_site_agent.backends` | order processing, membership sync, reporting |
| `k8s-namespace` | `waldur_site_agent.backends` | the same backend under a deployment-neutral name |

**Modes:** `order_process`, `membership_sync`, `report`, and `event_process`, which delivers orders and
membership changes as STOMP events to the same processors. `report` collects consumption only when a
`prometheus:` block is configured; without one it reports nothing and the offering bills by limits (see
[Usage reporting](#usage-reporting)).

| Operation | Behaviour |
|---|---|
| Create resource | Keycloak groups (when enabled) and a `ManagedNamespace` CR with quotas and optional `spec.security` |
| Terminate resource | Deletes the CR and the Keycloak groups |
| Update limits | Patches the quota quantities in the CR spec; the security level is not re-applied |
| Add / remove members | Keycloak group membership; optionally user lists in the CR (`sync_users_to_cr`) |
| Downscale | Quota set to cpu=1, memory=1Gi, storage=1Gi |
| Pause | Quota set to zero |
| Restore | **No-op** — returns `True`; limits come back with the next limit update |
| Usage reporting | Period consumption per namespace when a `prometheus:` block is configured, **No-op** otherwise |

### Backend names

The plugin registers a backend under both `k8s-ut-namespace` and `k8s-namespace`, and registers one
settings schema (`K8sUtNamespaceBackendSettingsSchema`) under both names in
`waldur_site_agent.backend_settings_schemas`. `K8sNamespaceBackend` is a subclass of
`K8sUtNamespaceBackend` whose only change is `backend_type`, so logs and diagnostics name the backend the
offering was configured with. Settings, components and behaviour are the parent's, unchanged.

The ManagedNamespace operator is not specific to one deployment, so an offering need not call itself "UT" to
use it. Use `k8s-namespace` in new configurations; existing ones keep `k8s-ut-namespace` and need no change.
Either name works in `backend_type` as well, which is what selects the settings schema — `backend_type` does not
choose the backend for a mode, the `*_backend` keys do.

## Features

- **ManagedNamespace Lifecycle**: Creates, updates, and deletes `ManagedNamespace` custom resources
- **Resource Quotas**: Sets CPU, memory, storage, and GPU limits as namespace quotas, with an optional
  per-resource switch to record the entitlement without enforcing it
- **Pod Security**: Applies a Pod Security Admission level per namespace, either from the offering or from the order
- **Role-Based Access Control**: Creates 3 Keycloak groups per namespace (admin, readwrite, readonly)
- **Waldur Role Mapping**: Maps Waldur roles to namespace access levels automatically
- **User Management**: Adds/removes users from Keycloak groups, reconciles role changes
- **Namespace Labels & Annotations**: Configurable labels and annotations propagated to created namespaces
- **Status Monitoring**: Parses operator Ready condition and exposes readiness in Waldur metadata
- **Configurable User Identity**: Choose which user attribute (email, civil_number, etc.) populates CR user fields
- **Namespace Name Validation**: Validates generated names against RFC 1123 before CR creation
- **Consumption Billing**: Reads core-hours, GB-hours and GPU-hours for each namespace from Prometheus
- **Status Operations**: Supports downscale (minimal quota), pause (zero quota), and restore

## Architecture

The plugin follows the Waldur Site Agent plugin architecture and consists of:

- **K8sUtNamespaceBackend**: Main backend implementation that orchestrates namespace and user management
  (`K8sNamespaceBackend` is the same class under the second name)
- **K8sUtNamespaceClient**: Handles Kubernetes API operations for `ManagedNamespace` CRs
- **PrometheusClient**: Reads period consumption from the Prometheus HTTP API
- **KeycloakClient**: Manages Keycloak groups and user memberships (shared package)

### Role Mapping

Waldur roles are mapped to namespace access levels. The default mapping is:

| Waldur Role | Namespace Role |
|-------------|----------------|
| `manager`   | `admin`        |
| `admin`     | `admin`        |
| `member`    | `readwrite`    |

This mapping is configurable via the `role_mapping` setting in `backend_settings`.
Custom entries are merged with the defaults, so you only need to specify overrides or additions:

```yaml
backend_settings:
  role_mapping:
    observer: "readonly"
    member: "readonly"  # override the default
```

Users whose Waldur role is not in the mapping fall back to `default_role` (default: `readwrite`).

### Component Mapping

Waldur component keys are mapped to Kubernetes quota fields. The default mapping is:

| Waldur Component | K8s Quota Field | Unit Format |
|------------------|-----------------|-------------|
| `cpu`            | `cpu`           | Integer     |
| `ram`            | `memory`        | `{value}Gi` |
| `storage`        | `storage`       | `{value}Gi` |
| `gpu`            | `gpu`           | Integer     |

This mapping is configurable via the `component_quota_mapping` setting in `backend_settings`.
Custom entries are merged with the defaults:

```yaml
backend_settings:
  component_quota_mapping:
    vram: "nvidia.com/vram"
```

Each component's `type` selects the quota field and, when billing is configured, the Prometheus query;
the component's key is the name Waldur sees. A component named `vCPU hours` with `type: "cpu"` is metered by
the CPU query and reported under `vCPU hours`.

## Installation

Install the plugin using uv:

```bash
uv sync --all-packages
```

The plugin will be automatically discovered via Python entry points.

## Setup Requirements

### Kubernetes Cluster Setup

1. **Kubernetes Cluster**: Accessible cluster with the `ManagedNamespace` CRD installed
   (`provisioning.hpc.ut.ee/v1`)
2. **Access Method**: Either a kubeconfig file or in-cluster service account
3. **CR Namespace**: A namespace where `ManagedNamespace` CRs will be created
   (default: `waldur-system`)

### Keycloak Setup (Optional)

Required for RBAC group integration:

1. **Keycloak Server**: Accessible Keycloak instance
2. **Target Realm**: Where user accounts and groups will be managed
3. **Service User**: User with group management permissions

#### Creating Keycloak Service User

1. Login to Keycloak Admin Console
2. Select Target Realm
3. Create User:
   - **Username**: `waldur-site-agent-k8s`
   - **Email Verified**: Yes
   - **Enabled**: Yes
4. **Set Password**: In Credentials tab (temporary: No)
5. **Assign Roles**: In Role Mappings tab
   - **Client Roles** -> `realm-management`
   - **Add**: `manage-users` (sufficient for group operations)

### Prometheus Setup (Optional)

Required only for consumption billing:

1. **Prometheus**: Reachable over HTTP with the query API enabled and the cluster's cAdvisor and
   kube-state-metrics series retained for the whole reporting window
2. **Credential**: A bearer token, or a username and password, if the API is authenticated
3. **Retention**: At least as long as the agent's `reporting_periods` (default 2 months), because a closed
   month is measured again after it ends

### Waldur Marketplace Setup

1. **Marketplace Offering**: Created with appropriate type (e.g., `Marketplace.Basic`)
2. **Components**: Configured via `waldur_site_load_components`
3. **Offering State**: Must be `Active` for order processing

## Configuration

### Minimal Configuration (K8s Only)

```yaml
offerings:
  - name: "k8s-namespaces"
    waldur_api_url: "https://your-waldur.com/"
    waldur_api_token: "your-waldur-api-token"
    waldur_offering_uuid: "your-offering-uuid"

    backend_type: "k8s-namespace"
    order_processing_backend: "k8s-namespace"
    membership_sync_backend: "k8s-namespace"
    reporting_backend: "k8s-namespace"

    backend_settings:
      kubeconfig_path: "/path/to/kubeconfig"
      cr_namespace: "waldur-system"
      namespace_prefix: "waldur-"
      keycloak_enabled: false
      sync_users_to_cr: true
      cr_user_identity_field: "email"
      namespace_labels:
        tenant: "waldur"
      namespace_annotations:
        description: "Managed by Waldur"

    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "cores"
        accounting_type: "limit"
        label: "CPU Cores"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB"
        accounting_type: "limit"
        label: "Memory (GB)"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB"
        accounting_type: "limit"
        label: "Storage (GB)"
        unit_factor: 1
```

### Full Configuration (with Keycloak)

```yaml
offerings:
  - name: "k8s-namespaces"
    waldur_api_url: "https://your-waldur.com/"
    waldur_api_token: "your-waldur-api-token"
    waldur_offering_uuid: "your-offering-uuid"

    backend_type: "k8s-namespace"
    order_processing_backend: "k8s-namespace"
    membership_sync_backend: "k8s-namespace"
    reporting_backend: "k8s-namespace"

    backend_settings:
      kubeconfig_path: "/path/to/kubeconfig"
      cr_namespace: "waldur-system"
      namespace_prefix: "waldur-"
      default_role: "readwrite"

      # Quotas are managed unless the order says otherwise
      quota_enabled: true
      # Pod Security level for new namespaces; unset leaves it to the operator
      default_security_level: "baseline"

      keycloak_enabled: true
      keycloak_use_user_id: true
      keycloak:
        keycloak_url: "https://your-keycloak.com/"
        keycloak_realm: "your-realm"
        keycloak_user_realm: "your-realm"
        keycloak_username: "waldur-site-agent-k8s"
        keycloak_password: "your-keycloak-password"
        keycloak_ssl_verify: true

    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "cores"
        accounting_type: "limit"
        label: "CPU Cores"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB"
        accounting_type: "limit"
        label: "Memory (GB)"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB"
        accounting_type: "limit"
        label: "Storage (GB)"
        unit_factor: 1
      gpu:
        type: "gpu"
        measured_unit: "units"
        accounting_type: "limit"
        label: "GPU"
        unit_factor: 1
```

### Full Configuration (with Keycloak and Prometheus billing)

```yaml
offerings:
  - name: "k8s-namespaces"
    waldur_api_url: "https://your-waldur.com/"
    waldur_api_token: "your-waldur-api-token"
    waldur_offering_uuid: "your-offering-uuid"

    backend_type: "k8s-namespace"
    order_processing_backend: "k8s-namespace"
    membership_sync_backend: "k8s-namespace"
    reporting_backend: "k8s-namespace"

    backend_settings:
      kubeconfig_path: "/path/to/kubeconfig"
      cr_namespace: "waldur-system"
      namespace_prefix: "waldur-"

      prometheus:
        url: "https://prometheus.example.org"
        bearer_token: "your-prometheus-token"
        ssl_verify: true
        timeout: 30
        step: "1h"
        queries:
          storage: >-
            avg_over_time((sum by (namespace)
            (kubelet_volume_stats_used_bytes{namespace=~"{namespaces}"}))[{window}:{step}])
            * {window_seconds} / 1073741824 / 3600

    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "core-hours"
        accounting_type: "usage"
        label: "CPU core-hours"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB-hours"
        accounting_type: "usage"
        label: "Memory GB-hours"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB-hours"
        accounting_type: "usage"
        label: "Storage GB-hours"
        unit_factor: 1
      gpu:
        type: "gpu"
        measured_unit: "GPU-hours"
        accounting_type: "usage"
        label: "GPU-hours"
        unit_factor: 1
```

## Configuration Reference

### Backend Settings

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `kubeconfig_path` | string | No | - | Path to kubeconfig file (omit for in-cluster config) |
| `cr_namespace` | string | No | `waldur-system` | Namespace where ManagedNamespace CRs are created |
| `namespace_prefix` | string | No | `waldur-` | Prefix for created namespace names |
| `default_role` | string | No | `readwrite` | Default namespace role for users without explicit role |
| `role_mapping` | object | No | See Role Mapping | Custom Waldur role to namespace role mapping (merged with defaults) |
| `component_quota_mapping` | object | No | See Component Mapping | Custom component to K8s quota field mapping |
| `quota_enabled` | boolean | No | `true` | Manage the namespace's `ResourceQuota` unless the order says otherwise |
| `default_security_level` | string | No | - | Pod Security level unless the order names one |
| `keycloak_use_user_id` | boolean | No | `true` | Use Keycloak user ID for lookup (false = use username) |
| `sync_users_to_cr` | boolean | No | `false` | Sync user identities to CR `adminUsers`/`rwUsers`/`roUsers` fields |
| `cr_user_identity_field` | string | No | `email` | User attribute for CR user fields |
| `cr_user_identity_lowercase` | bool | No | `false` | Lowercase the identity value before writing to CR |
| `namespace_labels` | object | No | `{}` | Labels to set on created namespaces (e.g., `tenant: waldur`) |
| `namespace_annotations` | object | No | `{}` | Annotations to set on created namespaces |
| `prometheus` | object | No | - | Consumption billing; usage is not reported when unset (see below) |

### Prometheus Settings (Optional)

The block is validated by `PrometheusSettingsSchema`, which forbids unknown keys: a misspelt key
such as `bearer_tokn` is reported against the `prometheus` block when the agent loads its configuration.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | string | Yes | - | Prometheus base URL; queries are posted to `{url}/api/v1/query` |
| `bearer_token` | string | No | - | Sent as `Authorization: Bearer …`; wins over basic auth when both are set |
| `username` | string | No | - | HTTP basic auth user; `password` must be set with it |
| `password` | string | No | - | HTTP basic auth password; `username` must be set with it |
| `ssl_verify` | boolean | No | `true` | Verify Prometheus's TLS certificate; only an explicit `false` disables it |
| `timeout` | number | No | `30` | Seconds per query request |
| `step` | string or number | No | `1h` | Resolution of the period averages, as a PromQL duration or seconds |
| `queries` | object | No | built-in | PromQL per component type, merged over the built-in queries |

An empty value (`ssl_verify:`, `timeout:`, `step:`, `queries:`) is read as unset and keeps the default;
only `ssl_verify: false` turns certificate verification off.

### Keycloak Settings (Optional)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `keycloak_enabled` | boolean | No | `false` | Enable Keycloak RBAC integration |
| `keycloak.keycloak_url` | string | No | `https://localhost/auth/` | Keycloak server URL |
| `keycloak.keycloak_realm` | string | No | `waldur` | Realm the groups are managed in |
| `keycloak.keycloak_user_realm` | string | No | `master` | Realm the admin user authenticates against |
| `keycloak.client_id` | string | No | `admin-cli` | Client used for the admin login |
| `keycloak.keycloak_username` | string | No | empty | Keycloak admin username |
| `keycloak.keycloak_password` | string | No | empty | Keycloak admin password |
| `keycloak.keycloak_ssl_verify` | boolean or path | No | `true` | Verify TLS; a path names a CA bundle |

The settings are validated by
`waldur_site_agent_k8s_ut_namespace.schemas.K8sUtNamespaceBackendSettingsSchema` (the
`keycloak:` block by the [keycloak-client](../keycloak-client/README.md) schema); a
misspelt key is logged as a warning when the agent loads its configuration. Both backend names
register this one schema class, so either name in `backend_type` validates the same keys.

## Quota management (`quota_enabled`)

The CRD's `spec.quota.enabled` decides whether the operator enforces the namespace's `ResourceQuota`.
The plugin resolves it per resource, in this order:

1. The resource's `quota_enabled` order attribute, when the order form recorded one
2. The offering's `quota_enabled` setting
3. `true`

The attribute is what makes the switch a per-order choice; the setting is the offering's default.
A `null` attribute — an untouched form field — falls back to the setting rather than reading as "off",
and an empty `quota_enabled:` in the YAML keeps the default of `true`.

Booleans are accepted as `true`/`false`, `0`/`1`, or the strings `true`, `false`, `yes`, `no`, `on`,
`off` (case-insensitive, surrounding whitespace ignored). Anything else — `maybe`, `sometimes` — fails the
order with a `BackendError` naming the source and the value, before any Keycloak group or CR is written.

`enabled` is always sent explicitly, because the CRD defaults it to `true` and an omitted field would
silently re-enable a quota the customer turned off. The quantities are sent even when it is off: they are the
entitlement, and the switch only decides whether it is enforced, so re-enabling it later needs no second write.

The switch is read at creation only. An update order patches the quota quantities and leaves `enabled` as it is.

## Pod Security level

`spec.security.level` sets the namespace's Pod Security Admission level. The plugin accepts the three levels
the CRD accepts, `privileged`, `baseline` and `restricted`, and resolves one per resource:

1. The resource's `security_level` order attribute, when the order form recorded a non-empty one
2. The offering's `default_security_level` setting
3. Unset

The value is trimmed and lowercased before it is sent, so `"  Restricted "` is applied as `restricted`. A
value outside the three levels fails the order with a `BackendError` that lists them — before any group or CR is
written — instead of surfacing an admission error from the API server.

When neither the order nor the offering names a level, `spec.security` is left out entirely and the operator
applies the installation's own default. The agent does not guess that default, which is why the level actually in force
is read back from `status.securityLevel` and published as `security_level` in the resource's `backend_metadata`
(the key is absent while the operator has not set it).

**The level is a creation-time choice.** An update order patches `spec.quota` only, so changing `security_level` on an
existing resource does not change its level; changing `default_security_level` likewise only affects namespaces created
afterwards. Changing the level of an existing namespace means editing the CR (or recreating the namespace).

## Consumption billing from Prometheus

With a `prometheus:` block the plugin reports what each namespace consumed in the billing period instead of
leaving the offering to bill by limits. Without the block, nothing is queried and nothing is reported.

The figures are in the period's **native units** — core-hours, GB-hours, GPU-hours — because that is what a price
list is written against. Each component's `unit_factor` converts from there into the unit Waldur shows, and the
result is rounded to two decimals. Leave `unit_factor: 1` and set `measured_unit` to the native unit, as in the
example above, unless the price list is denominated differently.

### What each component type measures

One query per component type, keyed on the component's `type` (which defaults to its key). The built-in queries are:

| Type | Metric | Unit | Notes |
|---|---|---|---|
| `cpu` | `container_cpu_usage_seconds_total` | core-hours | `increase` over the window, ÷ 3600 |
| `ram` | `container_memory_working_set_bytes` | GB-hours | period average × window seconds, ÷ 1024³ ÷ 3600 |
| `storage` | `kubelet_volume_stats_used_bytes` | GB-hours | stored bytes, not reserved capacity |
| `gpu` | `kube_pod_container_resource_requests` | GPU-hours | Requested `nvidia_com_gpu`, as the scheduler counts them |

The cpu and ram queries drop cAdvisor's pod- and namespace-level rollup rows (`container!=""`), which repeat
the same seconds and would otherwise be billed twice. The ram, storage and gpu queries average the per-namespace sum
over the period, so a container that starts mid-period pulls the mean down instead of being credited for the whole
period.

A component type with no query at all is reported as `0` with a warning; an offering whose component is measured
differently overrides just that type in `queries`.

### Query templates

`queries` maps a component type to PromQL. Templates are filled by literal placeholder replacement, so PromQL's
own braces survive and a query can be pasted from the Prometheus UI as-is:

| Placeholder | Filled with |
|---|---|
| `{namespaces}` | The backend ids being measured, as one RE2 alternation (`waldur-a\|waldur-b`) |
| `{window}` | The period length as a PromQL duration (`2419200s`) |
| `{window_seconds}` | The same as a bare number, for arithmetic |
| `{step}` | The `step` setting, clamped to the window |

### Which period is measured

The current period is measured from the month's start in the agent's `timezone` to the reporting instant. A
closed month is measured again with both ends pinned to the month's half-open bounds — its end is the first instant
of the next month — so a month is complete only after it has ended, and a month an agent outage left half-reported is
repaired on the next pass. Prometheus must therefore still hold that month's series: the agent reports the current month
and, with the default `reporting_periods: 2`, the previous one too.

A period that has only just opened is measured over a window of at least 60 seconds: Prometheus refuses a zero-length
range selector, and a month with a minute behind it really did consume close to nothing.

### Attribution and failures

Every reported series is attributed to the namespace in its `namespace` label. A series without one is
attributed to the single namespace requested, which is the shape of an override written as a plain
`sum(...)`; when more than one namespace was requested it is dropped with a warning, because there is no
honest way to say which one it belongs to.

A namespace with no series at all is reported as `0` for every component, not skipped: an idle namespace consumed nothing.

A query that fails — refused, unreachable, or answered with something unreadable — drops every requested namespace from
that pass's report rather than reporting zeros, and logs a warning naming the component types. Reporting nothing is not
the same as reporting zero: the namespace is left out of the pass, and the next pass retries. A failure of one component
type never aborts the others.

**The core pulls one resource at a time.** `_get_usage_report` and `get_usage_report_for_period` accept a
list of backend ids and fold them into one selector, but the reporting processor calls `pull_resource` once
per resource, so in `report` mode each call measures a single namespace: a pass costs one query per component
type per resource.

`membership_sync` pulls the same resources but never reads usage, so the backend skips the report for those
pulls and does not query Prometheus at all.

### Usage that decreases within a period

The ram, storage and gpu figures are averages over a window that starts at the period's start and ends at
the reporting instant, so a namespace that frees memory or storage — or gives up a GPU — can report a lower
figure on a later pass than on an earlier one. The core treats a decrease within a period as a data-collection
error and, by default, blocks the whole usage update for that resource for the period. Set
`omit_anomalous_usage_components: true` on such an offering to submit the components that did not decrease
instead (see
[Resource Management](../../docs/configuration.md#omit_anomalous_usage_components)).

### Offering-side billing

The plugin reports a figure for every component in `backend_components`, and the core submits only those
Waldur's offering declares. Waldur prices a component whose `accounting_type` (Waldur's `billing_type`) is
`usage` from that figure, and a `limit` component from its limit. One component is one or the other, so
reporting usage for a limit component does not by itself add a charge.

What does charge twice is an offering that keeps a limit-priced component beside the usage-priced one that
meters the same dimension: `cpu` billed by cores allocated plus `cpu-hours` billed by consumption bills the
same cores twice. Switch the component over — `accounting_type: "usage"`, and a `measured_unit` such as
`core-hours` — rather than adding a second one.

Switching an offering to consumption billing is an offering change, not a plugin change: set
`accounting_type: "usage"` on the components and run `waldur_site_load_components` to update them in Waldur.
The `prometheus:` block above is the configuration for such an offering; the same offering with the components
below still at `accounting_type: "limit"` would bill by limits alone.

```yaml
    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "core-hours"
        accounting_type: "usage"
        label: "CPU core-hours"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB-hours"
        accounting_type: "usage"
        label: "Memory GB-hours"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB-hours"
        accounting_type: "usage"
        label: "Storage GB-hours"
        unit_factor: 1
      gpu:
        type: "gpu"
        measured_unit: "GPU-hours"
        accounting_type: "usage"
        label: "GPU-hours"
        unit_factor: 1
```

## Usage

### Running the Agent

Start the agent with your configuration file:

```bash
uv run waldur_site_agent -c k8s-namespace-config.yaml -m order_process
```

### Diagnostics

Run diagnostics to check connectivity:

```bash
uv run waldur_site_diagnostics -c k8s-namespace-config.yaml
```

Diagnostics prints whether Prometheus billing is configured and its URL, and pings it: an unreachable or
rejecting Prometheus fails the diagnostics. The probe is a `vector(1)` query against `/api/v1/query` rather than
`/-/healthy`, which is unauthenticated and would report a bad token as healthy.

### Supported Agent Modes

- **order_process**: Creates and manages ManagedNamespace CRs based on Waldur resource orders
- **membership_sync**: Synchronizes user memberships between Waldur and Keycloak groups
- **report**: Reports each namespace's period consumption when a `prometheus:` block is configured, and
  reports nothing otherwise (see [Usage reporting](#usage-reporting))
- **event_process**: Runs the same order and membership processors from STOMP events instead of polling

## Resource Lifecycle

### Namespace Creation

When a Waldur resource order is processed:

1. The `quota_enabled` and `security_level` order attributes are resolved, and an invalid value fails the
   order before anything is created
2. Resource slug is validated (required for naming)
3. With Keycloak enabled, three groups are created: `ns_{slug}_admin`, `ns_{slug}_readwrite`,
   `ns_{slug}_readonly`
4. A `ManagedNamespace` CR is created with quota and group references in the spec, and `spec.security` when a
   level was resolved
5. The namespace name is `{namespace_prefix}{slug}` (e.g., `waldur-my-project`)
6. If CR creation fails, Keycloak groups are cleaned up (compensating transaction)

### Namespace Deletion

When a Waldur resource termination order is processed:

1. The `ManagedNamespace` CR is deleted
2. All 3 Keycloak groups are deleted

### Limit Updates

When resource limits are updated in Waldur:

1. Limits are converted to K8s resource quantities
2. The CR's `spec.quota` is patched with the new values

The patch is a merge patch of `spec.quota`, so `enabled` and the namespace's security level are left as they
are; neither `quota_enabled` nor `security_level` is re-read from a later order.

### User Management

When users are added to a Waldur resource:

1. Each user's Waldur role is mapped to a namespace role (admin/readwrite/readonly)
2. User is looked up in Keycloak
3. User is removed from any incorrect role groups (role reconciliation)
4. User is added to the correct role group

#### Direct CR User Sync

When `sync_users_to_cr` is enabled, user identities from Waldur are written directly to the
ManagedNamespace CR's `adminUsers`, `rwUsers`, and `roUsers` fields.
The managed-namespace-operator then creates RoleBindings with these identities as
User subjects (optionally prefixed with `CONTROLLER_USER_PREFIX` on the operator side).

The `cr_user_identity_field` setting controls which user attribute is used as the
identity value. The default is `email`, but any attribute exposed by the offering's
user attribute config can be used (e.g., `civil_number`, `username`).

Each user's Waldur role is mapped to a namespace role using the same
`role_mapping` configuration (see [Role Mapping](#role-mapping)), and the
identity value is placed in the corresponding CR field:

| Namespace Role | CR Field |
|---|---|
| `admin` | `adminUsers` |
| `readwrite` | `rwUsers` |
| `readonly` | `roUsers` |

On each membership sync cycle, the **full current set** of team members from
Waldur is written to the CR. Users removed from the Waldur project team are
automatically removed from the CR on the next sync, because empty lists are
sent for roles with no members.

This can be used **alongside** Keycloak groups (both mechanisms populate the
same RoleBindings) or **without** Keycloak (`keycloak_enabled: false`) for
deployments that rely solely on OIDC-based authentication.

```yaml
backend_settings:
  sync_users_to_cr: true
  cr_user_identity_field: "civil_number"  # or "email", "username", etc.
  cr_user_identity_lowercase: true        # optional, lowercase the value
  keycloak_enabled: false  # optional, can also be true for dual mode
```

The chosen field must be enabled in the offering's user attribute config
(`expose_civil_number: true`) in Waldur. Users missing the configured
attribute are skipped with a warning log.

When `cr_user_identity_lowercase` is enabled, the identity value is
lowercased before writing to the CR (e.g., `EE12345678901` becomes
`ee12345678901`). This is useful when OIDC subject matching is
case-sensitive and the identity source has mixed case.

When users are removed:

1. User is removed from all 3 Keycloak groups

### Usage Reporting

Reporting is opt-in and per offering. With a `prometheus:` block, `_get_usage_report` returns the current
period's consumption per namespace and `get_usage_report_for_period` recomputes a closed month, both as
`TOTAL_ACCOUNT_USAGE` only — the plugin does not attribute consumption to individual users.

Without the block the plugin reports **nothing**: `_get_usage_report` and `get_usage_report_for_period` return `{}`,
`ping` does not probe anything, and offerings using it bill by limits (allocation), as they always have. An
offering that never configured billing keeps behaving exactly as before.

A block that is present but unusable — no `url` — raises `PrometheusError` when the backend is constructed, rather
than degrading to "no billing": reporting nothing is the one failure mode that looks exactly like an idle month.

## Namespace Labels & Annotations

Labels and annotations configured in `backend_settings` are included in the
ManagedNamespace CR spec. The operator propagates them to the actual namespace.
This is useful for cluster policies (e.g., Kyverno requiring a `tenant` label):

```yaml
backend_settings:
  namespace_labels:
    tenant: "waldur"
    cost-center: "HPC-001"
  namespace_annotations:
    description: "Managed by Waldur Site Agent"
```

## Status Operations

| Operation | Effect |
|-----------|--------|
| Downscale | Quota set to minimal: cpu=1, memory=1Gi, storage=1Gi |
| Pause | Quota set to zero: cpu=0, memory=0Gi, storage=0Gi |
| Restore | **No-op** (limits come back with the next limit update) |

Both patch `spec.quota` only, so a namespace whose quota management is switched off (`quota_enabled: false`)
keeps running: there is no `ResourceQuota` for the quantities to restrict.

## Error Handling

- Kubernetes connectivity issues are logged and raised as `BackendError`
- Keycloak initialization failure logs a warning; user management operations become no-ops
- CR creation failure triggers automatic Keycloak group cleanup
- Missing users in Keycloak are logged as warnings and skipped
- Missing backend ID on deletion is logged and skipped gracefully
- An invalid `quota_enabled` or `security_level` value fails the order and leaves no Keycloak groups behind
- A Prometheus query failure omits the affected namespaces from that pass's report and never aborts it

## Development

### Running Tests

```bash
# From the plugin directory, so its entry points resolve
cd plugins/k8s-ut-namespace && uv run pytest tests/
```

### Code Quality

```bash
uvx prek run --all-files
```
