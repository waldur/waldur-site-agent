# Waldur Site Agent - K8s UT Namespace Plugin

This plugin enables integration between Waldur Site Agent and Kubernetes clusters for managing
`ManagedNamespace` custom resources (CRD: `provisioning.hpc.ut.ee/v1`) with optional Keycloak
RBAC group integration.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `k8s-namespace` | `waldur_site_agent.backends` | order processing, membership sync (preferred name) |
| `k8s-ut-namespace` | `waldur_site_agent.backends` | the same backend under its former name |

**Modes:** `order_process`, `membership_sync`, `event_process`. `report` runs but reports
nothing (see [Usage reporting](#usage-reporting)).

| Operation | Behaviour |
|---|---|
| Create resource | Three Keycloak groups (when enabled) and a `ManagedNamespace` CR with quotas (`spec.quota.enabled`) |
| Terminate resource | Deletes the CR and the Keycloak groups |
| Update limits | Patches the quota quantities in the CR spec; `spec.quota.enabled` is left as it is |
| Add / remove members | Keycloak group membership; optionally user lists in the CR (`sync_users_to_cr`) |
| Downscale | Quota set to cpu=1, memory=1Gi, storage=1Gi, and enforced |
| Pause | Quota set to zero, and enforced |
| Restore | Puts back the whole quota (quantities and `enabled`) that pause or downscale recorded |
| Usage reporting | **No-op** — reports nothing; billing is by limits |

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
- **Role-Based Access Control**: Creates 3 Keycloak groups per namespace (admin, readwrite, readonly)
- **Waldur Role Mapping**: Maps Waldur roles to namespace access levels automatically
- **User Management**: Adds/removes users from Keycloak groups, reconciles role changes
- **Namespace Labels & Annotations**: Configurable labels and annotations propagated to created namespaces
- **Status Monitoring**: Parses operator Ready condition and exposes readiness in Waldur metadata
- **Configurable User Identity**: Choose which user attribute (email, civil_number, etc.) populates CR user fields
- **Namespace Name Validation**: Validates generated names against RFC 1123 before CR creation
- **Status Operations**: Supports downscale (minimal quota), pause (zero quota), and restore

## Architecture

The plugin follows the Waldur Site Agent plugin architecture and consists of:

- **K8sUtNamespaceBackend**: Main backend implementation that orchestrates namespace and user management
- **K8sUtNamespaceClient**: Handles Kubernetes API operations for `ManagedNamespace` CRs
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
| `keycloak_use_user_id` | boolean | No | `true` | Use Keycloak user ID for lookup (false = use username) |
| `sync_users_to_cr` | boolean | No | `false` | Sync user identities to CR `adminUsers`/`rwUsers`/`roUsers` fields |
| `cr_user_identity_field` | string | No | `email` | User attribute for CR user fields |
| `cr_user_identity_lowercase` | bool | No | `false` | Lowercase the identity value before writing to CR |
| `namespace_labels` | object | No | `{}` | Labels to set on created namespaces (e.g., `tenant: waldur`) |
| `namespace_annotations` | object | No | `{}` | Annotations to set on created namespaces |

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

### Why the gate exists

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

### Order-form option

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

### Supported Agent Modes

- **order_process**: Creates and manages ManagedNamespace CRs based on Waldur resource orders
- **membership_sync**: Synchronizes user memberships between Waldur and Keycloak groups
- **report**: Runs, but reports no usage (see [Usage reporting](#usage-reporting))

## Resource Lifecycle

### Namespace Creation

When a Waldur resource order is processed:

1. Resource slug is validated (required for naming)
2. The `quota_enabled` order attribute is resolved against the offering; an invalid or disallowed value
   fails the order before anything is created
3. Three Keycloak groups are created: `ns_{slug}_admin`, `ns_{slug}_readwrite`, `ns_{slug}_readonly`
4. A `ManagedNamespace` CR is created with quota (including `enabled`) and group references in the spec
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

The patch is a merge patch, so `spec.quota.enabled` keeps the value it was created with.

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

The plugin reports **no usage**: `_get_usage_report` returns an empty report, and
offerings using it bill by limits (allocation), not consumption. Reporting actual
consumption from `ResourceQuota.status.used` is tracked in
[waldur-site-agent#6](https://code.opennodecloud.com/waldur/waldur-site-agent/-/work_items/6).

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
- An invalid `quota_enabled` value, or one the offering does not allow the order to set, fails the order
  and leaves no Keycloak groups or CR behind

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
