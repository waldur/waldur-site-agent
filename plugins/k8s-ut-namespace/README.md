# Waldur Site Agent - K8s UT Namespace Plugin

This plugin enables integration between Waldur Site Agent and Kubernetes clusters for managing
`ManagedNamespace` custom resources (CRD: `provisioning.hpc.ut.ee/v1`) with optional Keycloak
RBAC group integration, and optionally bills each namespace by the consumption Prometheus
measured over the billing period.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `k8s-namespace` | `waldur_site_agent.backends` | order processing, membership sync, reporting (preferred name) |
| `k8s-ut-namespace` | `waldur_site_agent.backends` | the same backend under its former name |

**Modes:** `order_process`, `membership_sync`, `event_process`, and `report`, which reports
consumption only when a `prometheus:` block is configured; without one it reports nothing and the
offering bills by limits (see [Usage reporting](#usage-reporting)).

| Operation | Behaviour |
|---|---|
| Create resource | Keycloak groups (when enabled) and a `ManagedNamespace` CR with quotas and optional `spec.security` |
| Terminate resource | Deletes the CR and the Keycloak groups |
| Update limits | Patches the quota quantities; `spec.quota.enabled` and the security level are left as they are |
| Add / remove members | Keycloak group membership; optionally user lists in the CR (`sync_users_to_cr`) |
| Downscale | Quota set to cpu=1, memory=1Gi, storage=1Gi, and enforced |
| Pause | Quota set to zero, and enforced |
| Restore | Puts back the whole quota (quantities and `enabled`) that pause or downscale recorded |
| Usage reporting | Consumption per namespace from Prometheus with a `prometheus:` block; **no-op** otherwise |

### Backend names

The plugin registers one backend under two names, `k8s-namespace` and `k8s-ut-namespace`, in both
`waldur_site_agent.backends` and `waldur_site_agent.backend_settings_schemas`. **Use `k8s-namespace` for new
offerings**: the ManagedNamespace operator is not specific to one deployment. Existing configurations that use
`k8s-ut-namespace` keep working and need no change.

Both names share one settings schema (`K8sUtNamespaceBackendSettingsSchema`), so settings are validated the same
way under either, and only once when an offering mixes the two names. `k8s-namespace` loads
`K8sNamespaceBackend`, a subclass of `K8sUtNamespaceBackend` whose only change is `backend_type`. Settings,
components and behaviour are identical. The two names differ only in how the backend identifies itself:

- the raw processor class path the agent registers in Waldur is
  `waldur_site_agent_k8s_ut_namespace.backend.K8sNamespaceBackend` or `...K8sUtNamespaceBackend`;
- `backend.backend_type` is `k8s-namespace` or `k8s-ut-namespace`, which is the name the core's generic log
  lines use for the backend;
- the plugin catalogue (`plugin_catalogue.build_catalogue()`) lists `k8s-namespace` and `k8s-ut-namespace` as two
  separate backends with identical roles and settings. Pick `k8s-namespace`.

Waldur's own backend name for the offering does **not** change: anything Waldur derives from the reported class path
(for example the limit-precision advisory) reads only the plugin package, so it is `k8s-ut-namespace` under either
name. Neither name is written into resource metadata. The Python package and the PyPI distribution keep the
`k8s-ut-namespace` name.

## Features

- **ManagedNamespace Lifecycle**: Creates, updates, and deletes `ManagedNamespace` custom resources
- **Resource Quotas**: Sets CPU, memory, storage, and GPU limits as namespace quotas, with an offering
  switch (and, where the offering allows it, a per-order one) to record them without enforcing them
- **Pod Security**: Applies a Pod Security Admission level per namespace, from the offering or, within the
  levels the offering allows, from the order
- **Role-Based Access Control**: Creates 3 Keycloak groups per namespace (admin, readwrite, readonly)
- **Waldur Role Mapping**: Maps Waldur roles to namespace access levels automatically
- **User Management**: Adds/removes users from Keycloak groups, reconciles role changes
- **Namespace Labels & Annotations**: Configurable labels and annotations propagated to created namespaces
- **Status Monitoring**: Parses operator Ready condition and exposes readiness in Waldur metadata
- **Configurable User Identity**: Choose which user attribute (email, civil_number, etc.) populates CR user fields
- **Namespace Name Validation**: Validates generated names against RFC 1123 before CR creation
- **Consumption Billing**: Reads core-hours, GiB-hours and GPU-hours per namespace from Prometheus
- **Status Operations**: Supports downscale (minimal quota), pause (zero quota), and restore

## Architecture

The plugin follows the Waldur Site Agent plugin architecture and consists of:

- **K8sUtNamespaceBackend**: Main backend implementation that orchestrates namespace and user management
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

When billing is configured, each component's `type` also selects the Prometheus query; the
component's key is the name Waldur sees. A component named `vCPU hours` with `type: "cpu"` is
metered by the CPU query and reported under `vCPU hours`.

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

1. **Prometheus**: reachable over HTTP with the query API, holding cAdvisor
   (`container_cpu_usage_seconds_total`, `container_memory_working_set_bytes` from the kubelet's
   `/metrics/cadvisor`), kubelet volume stats (`kubelet_volume_stats_used_bytes` from `/metrics`) and
   kube-state-metrics (`kube_pod_container_resource_requests`, `kube_pod_init_container_resource_requests`,
   `kube_pod_status_phase`) series. The default `selectors` match kube-prometheus-stack's job labels.
2. **Trust**: tenants must not be able to write series with labels of their choosing into it; see
   [Security](#security)
3. **Credential**: a bearer token, or a username and password, if the API is authenticated
4. **Retention**: at least 31 days, and N × 31 days with `reporting_periods: N`; see [Retention](#retention)
5. **Waldur**: `INVOICE_FINALIZATION_GRACE_PERIOD_HOURS` above 0 and at least `closed_period_hours`,
   or the last pass before midnight is final; see [Closed months](#closed-months)

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

      # Enforce the ResourceQuota; orders may not change it (see Quota enforcement)
      quota_enabled: true
      allow_quota_override: false
      # Pod Security level of new namespaces; orders may not pick another (see Pod Security level)
      default_security_level: "restricted"
      allowed_security_levels: []

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

### Configuration with Prometheus billing

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
      quota_enabled: false  # billed by consumption: no cap

      prometheus:
        url: "https://prometheus.example.org"
        bearer_token: "your-prometheus-token"
        ssl_verify: "/etc/ssl/certs/internal-ca.pem"  # or true / false
        timeout: 30
        step: "1m"
        # retention: read from Prometheus when unset; set it for Thanos or Mimir
        closed_period_hours: 24
        selectors:
          # Only the sources that differ from kube-prometheus-stack's defaults
          kube_state_metrics: 'job="kube-state-metrics", cluster="hpc"'

    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "core-hours"
        accounting_type: "usage"
        label: "CPU core-hours"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GiB-hours"
        accounting_type: "usage"
        label: "Memory GiB-hours"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GiB-hours"
        accounting_type: "usage"
        label: "Storage GiB-hours"
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
| `quota_enabled` | boolean | No | `true` | Enforce the namespace's `ResourceQuota` (`spec.quota.enabled`) |
| `allow_quota_override` | boolean | No | `false` | Let the order's `quota_enabled` attribute differ from the setting |
| `default_security_level` | string | No | - | Pod Security level of new namespaces; unset leaves it to the operator |
| `allowed_security_levels` | list | No | `[]` | Levels the order's `security_level` attribute may choose |
| `keycloak_use_user_id` | boolean | No | `true` | Use Keycloak user ID for lookup (false = use username) |
| `sync_users_to_cr` | boolean | No | `false` | Sync user identities to CR `adminUsers`/`rwUsers`/`roUsers` fields |
| `cr_user_identity_field` | string | No | `email` | User attribute for CR user fields |
| `cr_user_identity_lowercase` | bool | No | `false` | Lowercase the identity value before writing to CR |
| `namespace_labels` | object | No | `{}` | Labels to set on created namespaces (e.g., `tenant: waldur`) |
| `namespace_annotations` | object | No | `{}` | Annotations to set on created namespaces |
| `prometheus` | object | No | - | Consumption billing; usage is not reported when unset (see below) |

### Prometheus Settings (Optional)

The block is validated by `PrometheusSettingsSchema`, which forbids unknown keys: a misspelt key
such as `bearer_tokn` is reported against the `prometheus` block when the agent loads its
configuration.

<!-- pyml disable-num-lines 14 line-length -->
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | string | Yes | - | Prometheus base URL; queries are posted to `{url}/api/v1/query`. No credentials, query string or fragment |
| `bearer_token` | string | No | - | Sent as `Authorization: Bearer …`; not together with basic auth |
| `username` | string | No | - | HTTP basic auth user; `password` must be set with it |
| `password` | string | No | - | HTTP basic auth password; `username` must be set with it |
| `ssl_verify` | boolean or path | No | `true` | Verify Prometheus's TLS certificate; a path names a CA bundle; only an explicit `false` disables it |
| `timeout` | number | No | `30` | Seconds per request, in total; Prometheus is given the same evaluation timeout |
| `step` | string or number | No | `1m` | Resolution of the gauge integrals, as a PromQL duration or seconds; at most `5m` |
| `retention` | string or number | No | read from Prometheus | How long Prometheus keeps samples; see [Retention](#retention) |
| `closed_period_hours` | number | No | `24` | Hours after a month ends during which it is measured again; see [Closed months](#closed-months) |
| `selectors` | object | No | see below | Label matchers that pin each metric's source, merged over the defaults |
| `queries` | object | No | built-in | PromQL per component type, merged over the built-in queries |

The default `selectors`, as kube-prometheus-stack labels its scrape jobs:

```yaml
selectors:
  cadvisor: 'job="kubelet", metrics_path="/metrics/cadvisor"'
  kubelet: 'job="kubelet", metrics_path="/metrics"'
  kube_state_metrics: 'job="kube-state-metrics"'
  prometheus: ''   # any source; set it to Prometheus's own scrape job, see Security
```

Each becomes the placeholder `{<name>_selector}`; an empty one matches any source. Further entries
can be added for overrides, such as `dcgm: 'job="dcgm-exporter"'` used as `{dcgm_selector}`.

An empty value (`ssl_verify:`, `timeout:`, `step:`, `retention:`, `queries:`) is read as unset and keeps
the default; only `ssl_verify: false` turns certificate verification off. An empty block
(`prometheus: {}`), a block without `url`, credentials in `url`, both a token and basic auth, a `step`
over `5m` or a duration that does not parse all raise when the backend is built, rather than silently
turning billing off.

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
misspelt key is logged as a warning when the agent loads its configuration.

## Quota enforcement (`quota_enabled`)

The CRD's `spec.quota.enabled` decides whether the operator enforces the namespace's `ResourceQuota`.
An offering that bills by consumption may not want a cap: a quota sized to the limits blocks rolling
updates, which need headroom above steady state.

This requires ManagedNamespace operator 1.4.3 or later, the first version with `spec.quota.enabled`. An
older operator's CRD does not declare the field and prunes it on admission, so the quota stays enforced
without any error; on those versions the setting has to be made through namespace labels instead. The plugin
does not manage those labels, so pause and downscale cannot cap a namespace whose quota was turned off that
way: they force `spec.quota.enabled`, which such an operator ignores.

The plugin resolves the switch per resource when it is created:

1. When the order carries a `quota_enabled` attribute equal to the offering's `quota_enabled` setting, that
   value is used.
2. When the attribute differs from the setting, it is used if the offering sets `allow_quota_override: true`;
   otherwise the order **fails** with a `BackendError` that names the attribute and says the offering does not
   allow it. The request is never silently dropped.
3. When the attribute is absent (or `null`, or a whitespace-only string), the offering's `quota_enabled`
   setting is used, which defaults to `true`.

A refused order is refused before any Keycloak group or CR is created, so nothing is left behind.

Values are accepted as Waldur's own boolean fields accept them: a JSON boolean, `0`/`1` (also `0.0`/`1.0`),
or the strings `true`/`t`/`yes`/`y`/`on`/`1` and `false`/`f`/`no`/`n`/`off`/`0` (case-insensitive,
surrounding whitespace ignored). Anything else fails the order with a message naming the attribute or setting
and the rejected value. An empty `quota_enabled:` in the YAML keeps the default of `true`.

`enabled` is always sent explicitly, because the CRD defaults it to `true` and an omitted field would
silently re-enable a quota that was turned off. The quantities are sent even when it is off: they are the
entitlement, and the switch only decides whether it is enforced, so turning it back on later needs no
second write.

The attribute is resolved at creation only. A limit update is a merge patch of the quota quantities, so it
leaves `enabled` as it is. Changing the offering's settings later does not touch namespaces that already
exist either; see [Why the gate exists](#why-the-gate-exists) for finding and re-capping them.

### Pause, downscale and restore

Pause and downscale work by shrinking the quota, which restricts nothing while the quota is not enforced. So
both also set `spec.quota.enabled: true`. A namespace with `quota_enabled: false` cannot escape a pause that
way. The whole `spec.quota` in force before (quantities and `enabled`) is recorded as JSON in the CR annotation
`waldur.io/quota-before-pause`; a second pause or downscale keeps the first snapshot, so downscale followed by
pause still restores the original quota.

Restore writes the snapshot back and removes the annotation; without the annotation it is a no-op. Quantities
the namespace did not have before the pause are removed again, so a namespace without quantities has none
after a restore. The quantities have to come back from the snapshot: an offering billed by usage never sends a
limit update that would restore them. A limit update that arrives while the namespace is paused or
downscaled is written into the snapshot, not the spec, so it neither lifts the pause nor gets undone by the
restore. The recorded `enabled` is re-checked against the offering's settings as
they are at restore time: if the offering no longer allows orders to differ from its default
(`allow_quota_override` turned off since), the offering's current `quota_enabled` is restored instead, with a
warning. Likewise, re-creating a resource resolves its attributes afresh against the current settings.

A namespace paused by an agent that wrote the earlier `waldur.io/quota-enabled-before-pause` annotation (which
recorded only `enabled`) gets `enabled` back on restore; its pre-pause quantities were not recorded and come
back with the next limit update.

### Why the quota switch is gated

Waldur stores order attributes that the offering's order form does not declare: it validates the declared
options and passes every other key through. Leaving `quota_enabled` out of the form therefore does not stop
an orderer from sending it through the API. Without `allow_quota_override`, any orderer could get an
uncapped namespace on a limit-billed offering, so the plugin honours the attribute only where the offering
says so, and refuses the order otherwise rather than silently ignoring the request.

Turning `allow_quota_override` off stops new uncapped orders; it does not re-cap namespaces created while it
was on. List them and re-enable their quota by hand where needed:

```bash
kubectl get managednamespaces -n waldur-system \
  -o jsonpath='{range .items[?(@.spec.quota.enabled==false)]}{.metadata.name}{"\n"}{end}'
kubectl patch managednamespace <name> -n waldur-system --type merge -p '{"spec":{"quota":{"enabled":true}}}'
```

### Quota order-form option

To let customers choose, set `allow_quota_override: true` and declare the option on the offering
(`options` in the offering's order form, set through the offering editor or the API):

```json
{
  "order": ["quota_enabled"],
  "options": {
    "quota_enabled": {
      "type": "select_string",
      "label": "Enforce resource quota",
      "help_text": "Leave empty for the offering default",
      "required": false,
      "choices": ["true", "false"]
    }
  }
}
```

A `select_string` records the choice as a string in either direction, which the plugin parses. Waldur
validates the declared choices, and an unanswered optional field is left out of the order entirely, which
keeps the offering default.

## Pod Security level

`spec.security.level` sets the namespace's Pod Security Admission level, one of `privileged`, `baseline` and
`restricted`. This requires ManagedNamespace operator 1.4.3 or later, the first version that supports
`spec.security.level` and reports the applied level in `status.securityLevel`. On older operators the level
has to be set through namespace labels instead (see [Namespace Labels & Annotations](#namespace-labels--annotations)).
Remove such `pod-security.kubernetes.io/*` labels when moving to `spec.security.level`: which of the two wins
is up to the operator, not the agent.

The plugin resolves the level per resource when it is created:

1. When the order carries a `security_level` attribute that equals `default_security_level` or is listed in
   `allowed_security_levels`, that level is used.
2. When the attribute names any other level, the order **fails** with a `BackendError` naming the attribute
   and the levels the offering allows. The request is never silently dropped.
3. When the attribute is absent (or `null`, or a whitespace-only string), the offering's
   `default_security_level` is used.
4. When there is no default either, `spec.security` is left out and the operator applies the installation's
   own default, which the agent does not know and does not guess.

The allow-list is empty by default, so an order cannot choose a level unless the offering says so, and
`privileged` is never implied: an offering that wants to offer it lists it. `default_security_level` is the
offering's own choice and needs no allow-list entry. Values are matched case-insensitively after trimming
whitespace. An offering with an allow-list should also set `default_security_level`: otherwise an order that
leaves the attribute out gets the operator's default, which may be looser than anything on the list.

A refused order is refused before any Keycloak group or CR is created. A `default_security_level` or
`allowed_security_levels` entry that is not one of the three levels fails the order with a message naming
the setting (the agent also warns about it when it loads its configuration; case and surrounding whitespace
are normalised first, so `Restricted` is fine). A blank `default_security_level` means unset.

After creating the CR the plugin checks the stored object. If the cluster's CRD dropped a requested
`spec.security.level`, or `spec.quota.enabled: false`, the operator is older than 1.4.3: the plugin deletes the
CR and the Keycloak groups it just created and fails the order with an error saying the operator does not
support the field. (`enabled: true` is not checked, since a CRD without the field enforces the quota anyway.)
If removing the CR fails too, the error says so and names the CR to delete by hand.

That check only sees the CRD. A CRD upgraded ahead of its controller stores `spec.security.level`, but the old
controller ignores it; the agent then logs a warning for every ready namespace that requests a level and has
no `status.securityLevel`.

The level actually in force is read back from the CR's `status.securityLevel` and published as
`security_level` in the resource's `backend_metadata`; the key is absent while the operator has not set it.
When it differs from a requested `spec.security.level`, the agent logs a warning.

The level is a creation-time choice. A limit update patches `spec.quota` only, and changing
`default_security_level` affects only namespaces created afterwards.

### Why the security level is gated

Waldur stores order attributes that the offering's order form does not declare, so leaving `security_level`
out of the form does not stop an orderer from sending it through the API. An ungated
`security_level: privileged` would let any orderer run privileged pods, hostPath mounts and host networking in
their namespace, which is effectively node-level access to a shared cluster. The plugin therefore honours the
attribute only for the levels the offering lists, and refuses the order otherwise.

### Security level order-form option

To let customers choose, list the levels in `allowed_security_levels` and declare the option on the offering
with `choices` limited to those levels and the default. Waldur validates the declared choices, so the form
cannot submit any other level; the plugin's check covers orders that bypass the form. For example, to let
orders loosen the default `restricted` to `baseline`:

```yaml
    backend_settings:
      default_security_level: "restricted"
      allowed_security_levels: ["baseline"]
```

and the matching order-form option:

```json
{
  "order": ["security_level"],
  "options": {
    "security_level": {
      "type": "select_string",
      "label": "Pod Security level",
      "help_text": "Leave empty for the offering default (restricted)",
      "required": false,
      "choices": ["restricted", "baseline"]
    }
  }
}
```

## Consumption billing from Prometheus

With a `prometheus:` block the plugin reports what each namespace consumed in the billing period.
Without the block nothing is queried and nothing is reported.

> **Tenants must not be able to write labelled series into the billing Prometheus.** The bill is
> computed from every series that carries the billed namespace's label. If a tenant can choose the
> labels of what Prometheus ingests — a `ServiceMonitor` or `PodMonitor` with `honorLabels: true` or
> with `relabelings` that set `job` or `namespace`, a Pushgateway, or remote-write — it can pose as
> the kubelet or kube-state-metrics in another tenant's namespace and inflate that bill, or keep a
> finished GPU pod billable. Check this before enabling billing; see [Security](#security) for the
> setups that rule it out, and for the one setting that looks right but switches billing off.

Only components with `accounting_type: "usage"` are measured and reported. A limit-billed
component is priced from its limit, and a consumption figure under its name — 1488 core-hours
against a two-core limit — would only mislead.

The figures are in **native units** — core-hours, GiB-hours, GPU-hours — because that is what a
price list is written against. Each component's `unit_factor` divides them into the unit Waldur
shows, and the result is rounded to two decimals.

### Built-in queries

One query per component type, keyed on the component's `type` (which defaults to its key):

<!-- pyml disable-num-lines 7 line-length -->
| Type | Metrics | Source | Unit | How |
|---|---|---|---|---|
| `cpu` | `container_cpu_usage_seconds_total` | `{cadvisor_selector}` | core-hours | `increase` over the period, ÷ 3600 |
| `ram` | `container_memory_working_set_bytes` | `{cadvisor_selector}` | GiB-hours | integrated over the period |
| `storage` | `kubelet_volume_stats_used_bytes` | `{kubelet_selector}` | GiB-hours | integrated; bytes stored, each claim once |
| `gpu` | `kube_pod_container_resource_requests`, `kube_pod_init_container_resource_requests`, `kube_pod_status_phase` | `{kube_state_metrics_selector}` | GPU-hours | integrated; per pod, while Running |

```promql
# cpu
sum by (namespace) (clamp_min(increase(container_cpu_usage_seconds_total{
  namespace=~"{namespaces}", {cadvisor_selector}, container!="", image!=""}[{window}]), 0)) / 3600

# ram
sum_over_time((sum by (namespace) (clamp_min(container_memory_working_set_bytes{
  namespace=~"{namespaces}", {cadvisor_selector}, container!="", image!=""}, 0)))
  [{window}:{step}] offset {step}) * {step_seconds} / 3600 / 1073741824

# storage
sum_over_time((sum by (namespace) (max by (namespace, persistentvolumeclaim) (clamp_min(
  kubelet_volume_stats_used_bytes{namespace=~"{namespaces}", {kubelet_selector}}, 0))))
  [{window}:{step}] offset {step}) * {step_seconds} / 3600 / 1073741824

# gpu
sum_over_time((sum by (namespace) (max by (namespace, pod) (
    label_replace(sum by (namespace, pod) (max by (namespace, pod, container) (clamp_min(
      kube_pod_container_resource_requests{
        namespace=~"{namespaces}", {kube_state_metrics_selector}, resource="nvidia_com_gpu"}, 0))),
      "requests", "containers", "", "")
    or
    label_replace(max by (namespace, pod) (clamp_min(
      kube_pod_init_container_resource_requests{
        namespace=~"{namespaces}", {kube_state_metrics_selector}, resource="nvidia_com_gpu"}, 0)),
      "requests", "init", "", "")
  ) * on (namespace, pod) group_left () (max by (namespace, pod) (kube_pod_status_phase{
    namespace=~"{namespaces}", {kube_state_metrics_selector}, phase="Running"}) == 1)))
  [{window}:{step}] offset {step}) * {step_seconds} / 3600
```

- **Sources are pinned.** Each query selects only the scrape job that produces its metric, through
  the `selectors` setting (defaults below). This keeps out a tenant's own exporter publishing the
  same metric names, and the kubelet's `/metrics/resource` endpoint, which repeats cAdvisor's
  containers and would bill them twice. See [Security](#security).
- **Every input series is clamped at zero** before it is summed, and a negative answer is dropped
  with a warning, so no series can lower a bill.
- **cpu, ram:** `container!=""` drops cAdvisor's pod- and namespace-level rollup rows, which repeat
  the same usage; `image!=""` drops the pause container and image-less rows. `increase()` needs two
  samples of a series inside the window, so a container that lives shorter than two scrape intervals
  is not counted.
- **storage** bills what is stored, not the capacity granted. A ReadWriteMany claim mounted on
  several nodes is reported by each node's kubelet, so each claim counts once, at its largest
  reading. The kubelet reports only mounted filesystem volumes: an unmounted claim and a `Block`-mode
  volume have no `kubelet_volume_stats_*` series and are not billed. To bill reserved capacity
  instead, override the query with kube-state-metrics' request:

  ```yaml
  queries:
    storage: >-
      sum_over_time((sum by (namespace) (max by (namespace, persistentvolumeclaim) (clamp_min(
      kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace=~"{namespaces}",
      {kube_state_metrics_selector}}, 0)))) [{window}:{step}] offset {step})
      * {step_seconds} / 3600 / 1073741824
  ```

- **gpu** bills what the scheduler holds for a pod — the larger of its largest init-container
  request and the sum of its containers' requests — and only while the pod is `Running`. A `Pending`
  pod holds no GPU yet, and a `Succeeded` or `Failed` pod keeps its request in kube-state-metrics
  until it is deleted. A container reported by two kube-state-metrics replicas counts once.

A component type with no query at all is reported as `0` with a warning.

### Query templates

`queries` maps a component type to PromQL, merged over the built-in queries. Templates are filled by
literal placeholder replacement, so PromQL's own braces survive:

<!-- pyml disable-num-lines 9 line-length -->
| Placeholder | Filled with |
|---|---|
| `{namespaces}` | The backend ids being measured, as one regex alternation (`waldur-a\|waldur-b`), escaped for RE2 and for the PromQL string |
| `{window}` | The period's length as a PromQL duration (`2678400s`) |
| `{window_seconds}` | The same as a bare number, for arithmetic |
| `{step}` | The `step` setting as a PromQL duration |
| `{step_seconds}` | The same as a bare number |
| `{<name>_selector}` | Each entry of `selectors`: `{cadvisor_selector}`, `{kubelet_selector}`, `{kube_state_metrics_selector}`, and any added |

A template must use `{namespaces}`, and one that names an unknown placeholder is refused when the
backend is built. A query must return one series per namespace with a `namespace` label
(`sum by (namespace)`); a series without one is dropped with a warning, because there is no way to
tell whose it is. Only backend ids that are valid namespace names (RFC 1123 labels) are queried.

### Step and accuracy

A step point measures the state at one instant, and each point bills the step after it. Whatever
runs between two points is not seen, and whatever covers a point is billed for the whole step: with
`step: 1h`, a job running :10–:40 every hour bills nothing and one running :55–:05 bills six times
its usage. The default is therefore `1m`, and a step longer than `5m` — Prometheus's default
lookback — is refused.

At a minute, the error is about one step per pod start or stop. A series that stops is marked stale
at the next scrape. One that stops without a staleness marker — backfilled data, a Prometheus
restart — stays visible for the lookback (5 minutes) and is billed for it. GPU figures are not
affected: the pod's phase changes from `Running` when it finishes.

A 31-day month at `1m` is 44,640 points per series, evaluated in one query per component type. On
a large cluster, watch Prometheus's query time (`--query.timeout`) against the agent's `timeout`, or
point the overrides at recording rules.

### Which period is measured

Each query is evaluated at the period's end. The current period runs from the start of the month in
the agent's `timezone` to the reporting instant, and counts only completed steps: the figure lags by
up to one step and never decreases from one pass to the next.

Consecutive months tile without overlap. `increase` covers the samples in `(start, end]`, and
`offset {step}` places the gauges' step points on `[start, end)`, so a namespace created at midnight
on the 1st is not billed a step to the month before. Ranges are left-open from Prometheus 3 on; on
Prometheus 2 the point on a boundary falls into both months. A month that crosses a daylight-saving
change is measured in real seconds.

### Closed months

The current-period figure stops at the last pass before midnight, so a month is measured once more
after it ends — but only within `closed_period_hours` (default 24) of its end. Waldur takes usage for
a past month only while that month's invoice is still open, which lasts
`INVOICE_FINALIZATION_GRACE_PERIOD_HOURS` and is **0 by default**: with the default, Waldur refuses
the re-measured month and the last pass before midnight is final, so up to one reporting interval
at the end of each month goes unbilled. Set the grace period above 0 and at least as long as
`closed_period_hours`. Outside the window the month is not queried, and the processor skips it.

`waldur_site_load_historical_usage` gets an error rather than an empty report for a month outside the
window — an empty report with `--reconcile-stale` would zero the month — and measures months in the
agent's `timezone`, as the processor does. To load an older month, raise `closed_period_hours` in
the configuration the loader reads.

### Retention

A period that Prometheus does not hold in full is **not reported**: measured from the part that is
left, it would come out low, and for the current month a retention shorter than the month turns the
window into a sliding one that the anomaly guard then freezes at whatever the retention covers.

- With `retention` unset, the plugin reads the server's time-based retention
  (`/api/v1/status/runtimeinfo`) and its oldest sample (`/api/v1/status/tsdb/blocks`, or the
  server's own `prometheus_tsdb_lowest_timestamp_seconds`), once per cycle, and uses the later of
  the two. That also catches a size-based retention and a server younger than its retention. A
  server that exposes neither is assumed to keep Prometheus's default 15 days, with a warning.
- With `retention` set, the value is trusted as is. Set it for Thanos, Mimir and other stores whose
  status endpoints describe only the local TSDB, not the data they serve.

The current month needs at least 31 days of data; with `reporting_periods: N`, keep N × 31 days. An
uncovered current month is logged as an error and reported with an empty total, which changes
nothing in Waldur, and `waldur_site_diagnostics` fails. An uncovered closed month raises: the
processor logs it and skips the month, and the historical loader stops for it.

### No data and failures

A namespace that **no** query returned a series for is left out of the report: Prometheus holds
nothing for it in that period. For a closed month the processor then skips the period rather than
storing zeros; for the current one it is reported with an empty total. A namespace that some query
found series for is reported with every usage-billed component, and a component nothing was found
for is a real `0`: a namespace whose pods request no GPU used no GPU.

- A query Prometheus refuses (`400`, `422`) reports nothing for that pass — a partial report would
  contradict the figures Waldur holds for the other components. The other component types are still
  asked for, so the log names every broken query.
- A query Prometheus gives up on (`503` with `errorType: timeout`) fails that resource's pass only,
  and its other component types are not asked for. Usually the query is the cause, not the server:
  a namespace with many series makes its own month-to-date subqueries slow, and that must not stop
  every other namespace's billing. Three such timeouts in a row count as the server being down.
- A transport error, a `timeout` the agent itself runs out, a rejected credential or any other `5xx`
  ends the pass at once, and Prometheus is not asked again for the rest of the reporting cycle:
  every further query would wait out the same timeout. The current period is then reported with an
  empty total; a closed month raises.
- Each request is bounded by `timeout` in total, and an answer larger than 16 MiB is refused.
  Prometheus is told 80 % of `timeout`, so a slow query comes back as its timeout error before the
  agent stops waiting.

The reporting processor calls `pull_resource` once per resource, so a pass costs one query per
component type per resource and period. `membership_sync` pulls the same resources but never reads
usage: `pull_resource` asks `include_usage_requested()` and makes no Prometheus query for those
pulls.

### Security

A bill is only as trustworthy as the series it is computed from. Pinning the scrape job and clamping
at zero keep out a tenant's exporter that publishes the billed metric names under its own job — to
cancel its own bill with negative values, say. They do not help when a tenant can set the labels of
what the billing Prometheus ingests: a `ServiceMonitor` or `PodMonitor` with `honorLabels: true`, or
with `relabelings` that set `job`, `metrics_path` or `namespace`, a Pushgateway, or remote-write. A
tenant could then publish `job="kubelet"` series in another tenant's namespace and inflate that bill,
or a `kube_pod_status_phase` that keeps a finished GPU pod billable.

So the Prometheus the agent reads must not pick up tenant monitors, or must confine them:

- **A system-only Prometheus** (the strongest): it scrapes the kubelet, cAdvisor, kube-state-metrics
  and itself, and nothing a tenant defines — with the Prometheus Operator,
  `serviceMonitorNamespaceSelector` and `podMonitorNamespaceSelector` limited to the system
  namespaces — and has no Pushgateway or remote-write receiver. Point `url` at it.
- **Confined tenant monitors**: with the Prometheus Operator, `enforcedNamespaceLabel: namespace`,
  with the kubelet and kube-state-metrics monitors listed in `excludedFromEnforcement`. A tenant
  monitor's series are then labelled with the tenant's own namespace, so it can at most raise its own
  bill. Check on the running Prometheus that a tenant monitor's series do end up that way.

Do **not** use `overrideHonorLabels: true` for this. It applies to every `ServiceMonitor` and
`PodMonitor`, the system ones included, and the kubelet and kube-state-metrics monitors need
`honorLabels: true` to keep the workload's `namespace`: their series would carry it as
`exported_namespace`, every namespace would look empty, and nothing would be billed — logged only as
"no samples". `enforcedNamespaceLabel` without the `excludedFromEnforcement` entries does the same.
Narrower `selectors` are no substitute either: a tenant that sets labels can set whatever a selector
pins. Behind Thanos or Mimir, an external label does not identify the source, because a series that
already carries the label keeps its own value; query the system Prometheus directly, or make sure no
Prometheus that tenants can write into feeds the same store.

The coverage check reads the oldest sample from `/api/v1/status/tsdb/blocks`. Where that endpoint is
missing (Prometheus before 2.15, or a store that does not serve it), it falls back to the scraped
`prometheus_tsdb_lowest_timestamp_seconds`, narrowed by `selectors.prometheus`, and takes the smallest
value. A forged value lower than the real one only disables the check. One higher than every real
one — possible when the selector is left open and nothing real matches — makes the current month look
uncovered and stops its reporting. Set `selectors.prometheus` to Prometheus's own scrape job, or set
`retention`.

Credentials go in `bearer_token` or `username`/`password`, which are secret settings, never in
`url`. Sending them over `http://`, and turning `ssl_verify` off, are logged as warnings.

### Offering-side billing

Waldur prices a component whose `accounting_type` (Waldur's `billing_type`) is `usage` from the
reported figure, and a `limit` component from its limit. Switching an offering to consumption billing
is an offering change: set `accounting_type: "usage"` on the components and run
`waldur_site_load_components`. Switch a component over rather than adding a usage-billed one beside
it: `cpu` billed by cores allocated plus `cpu-hours` billed by consumption bills the same cores
twice.

Usage-billed components carry no limits — Waldur sends a limit only for a limit-billed component —
so with every component usage-billed the CR's `spec.quota` holds no quantities, and the operator's
own default decides whether a `ResourceQuota` is enforced. An offering billed by consumption
normally wants no quota at all: set `quota_enabled: false` (see
[Quota enforcement](#quota-enforcement-quota_enabled)).

### Known limitations

- Usage between the last report and a namespace's termination is not billed: the processor does
  not report usage before it processes a termination order. That is a core follow-up.
- Usage is reported per namespace only, not per user.
- The default queries can be under-reported on purpose, and a provider that bills strangers should
  weigh these:
  - `cpu` misses containers shorter than two scrape intervals and each container's usage before its
    first scrape, so work split into many short pods largely escapes it.
  - `storage` bills bytes in mounted volumes: a claim whose pods are scaled to zero, or a `Block`-mode
    volume, costs nothing. The capacity-based override above bills what is reserved instead.
  - `gpu` bills GPUs requested. Where the NVIDIA container runtime is the default and accepts
    `NVIDIA_VISIBLE_DEVICES` from unprivileged pods (its default), a pod can reach GPUs without
    requesting any; set `accept-nvidia-visible-devices-envvar-when-unprivileged = false`.

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

Diagnostics prints whether Prometheus billing is configured, its URL, and from when Prometheus holds
data and why. It pings the query API — a `vector(1)` query rather than `/-/healthy`, which is
unauthenticated and would report a rejected token as healthy — and fails when Prometheus is
unreachable or rejects the credential, or when it does not hold all of the current month. The probe
is diagnostics only: the backend does not hold orders back while Prometheus is down.

### Supported Agent Modes

- **order_process**: Creates and manages ManagedNamespace CRs based on Waldur resource orders
- **membership_sync**: Synchronizes user memberships between Waldur and Keycloak groups
- **report**: Reports each namespace's period consumption when a `prometheus:` block is
  configured, and nothing otherwise (see [Usage reporting](#usage-reporting))

## Resource Lifecycle

### Namespace Creation

When a Waldur resource order is processed:

1. Resource slug is validated (required for naming)
2. The `quota_enabled` and `security_level` order attributes are resolved against the offering; an invalid
   or disallowed value fails the order before anything is created
3. Three Keycloak groups are created: `ns_{slug}_admin`, `ns_{slug}_readwrite`, `ns_{slug}_readonly`
4. A `ManagedNamespace` CR is created with quota (including `enabled`) and group references in the spec,
   and `spec.security` when a level was resolved
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

The patch is a merge patch, so `spec.quota.enabled` keeps the value it was created with and
`spec.security` is not touched.

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

Reporting is opt-in per offering. With a `prometheus:` block, `_get_usage_report` returns the
current period's consumption per namespace and `get_usage_report_for_period` re-measures a closed
month, both as `TOTAL_ACCOUNT_USAGE` only: namespaces are billed as a whole, not per user. See
[Consumption billing from Prometheus](#consumption-billing-from-prometheus).

Without the block the plugin reports **nothing**: both return `{}`, and offerings using it bill by
limits (allocation), as before.

### Namespace Labels & Annotations

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

### Status Operations

| Operation | Effect |
|-----------|--------|
| Downscale | Quota set to minimal: cpu=1, memory=1Gi, storage=1Gi; `spec.quota.enabled` forced to `true` |
| Pause | Quota set to zero: cpu=0, memory=0Gi, storage=0Gi; `spec.quota.enabled` forced to `true` |
| Restore | Writes back the quota recorded by pause/downscale (quantities and `enabled`); no-op without it |

The quota in force before pause or downscale is kept in the `waldur.io/quota-before-pause` annotation on the CR
(see [Pause, downscale and restore](#pause-downscale-and-restore)).

## Error Handling

- Kubernetes connectivity issues are logged and raised as `BackendError`
- Keycloak initialization failure logs a warning; user management operations become no-ops
- CR creation failure triggers automatic Keycloak group cleanup
- Missing users in Keycloak are logged as warnings and skipped
- Missing backend ID on deletion is logged and skipped gracefully
- A failed Prometheus query leaves every namespace of that pass out of the report, and names the
  component type in the log; an unreachable Prometheus is not asked again for the rest of the cycle
- An invalid `quota_enabled` or `security_level` value, or one the offering does not allow the order to set,
  fails the order and leaves no Keycloak groups or CR behind

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
