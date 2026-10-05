# Waldur Site Agent - K8s UT Namespace Plugin

This plugin enables integration between Waldur Site Agent and Kubernetes clusters for managing
`ManagedNamespace` custom resources (CRD: `provisioning.hpc.ut.ee/v1`) with optional Keycloak
RBAC group integration.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `k8s-ut-namespace` | `waldur_site_agent.backends` | order processing, membership sync |

**Modes:** `order_process`, `membership_sync`, `event_process`. `report` runs but reports
nothing (see [Usage reporting](#usage-reporting)).

| Operation | Behaviour |
|---|---|
| Create resource | Three Keycloak groups (when enabled) and a `ManagedNamespace` CR with quotas |
| Terminate resource | Deletes the CR and the Keycloak groups |
| Update limits | Updates the quotas in the CR spec |
| Add / remove members | Keycloak group membership; optionally user lists in the CR (`sync_users_to_cr`) |
| Downscale | Quota set to cpu=1, memory=1Gi, storage=1Gi |
| Pause | Quota set to zero |
| Restore | **No-op** — returns `True`; limits come back with the next limit update |
| Usage reporting | **No-op** — reports nothing; billing is by limits |

## Features

- **ManagedNamespace Lifecycle**: Creates, updates, and deletes `ManagedNamespace` custom resources
- **Resource Quotas**: Sets CPU, memory, storage, and GPU limits as namespace quotas
- **Role-Based Access Control**: Creates 3 Keycloak groups per namespace (admin, readwrite, readonly)
- **Waldur Role Mapping**: Maps Waldur roles to namespace access levels automatically
- **User Management**: Adds/removes users from Keycloak groups, reconciles role changes
- **Usage Reporting**: Meters cpu/ram/gpu from live Running pod requests, storage from the namespace's own quota, both × elapsed time (see "Usage Reporting" below)
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

    backend_type: "k8s-ut-namespace"
    order_processing_backend: "k8s-ut-namespace"
    membership_sync_backend: "k8s-ut-namespace"
    reporting_backend: "k8s-ut-namespace"

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
        accounting_type: "usage"
        label: "CPU Cores"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB"
        accounting_type: "usage"
        label: "Memory (GB)"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB"
        accounting_type: "usage"
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

    backend_type: "k8s-ut-namespace"
    order_processing_backend: "k8s-ut-namespace"
    membership_sync_backend: "k8s-ut-namespace"
    reporting_backend: "k8s-ut-namespace"

    backend_settings:
      kubeconfig_path: "/path/to/kubeconfig"
      cr_namespace: "waldur-system"
      namespace_prefix: "waldur-"
      default_role: "readwrite"

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
        accounting_type: "usage"
        label: "CPU Cores"
        unit_factor: 1
      ram:
        type: "ram"
        measured_unit: "GB"
        accounting_type: "usage"
        label: "Memory (GB)"
        unit_factor: 1
      storage:
        type: "storage"
        measured_unit: "GB"
        accounting_type: "usage"
        label: "Storage (GB)"
        unit_factor: 1
      gpu:
        type: "gpu"
        measured_unit: "units"
        accounting_type: "usage"
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
2. Three Keycloak groups are created: `ns_{slug}_admin`, `ns_{slug}_readwrite`, `ns_{slug}_readonly`
3. A `ManagedNamespace` CR is created with quota and group references in the spec
4. The namespace name is `{namespace_prefix}{slug}` (e.g., `waldur-my-project`)
5. If CR creation fails, Keycloak groups are cleaned up (compensating transaction)

### Namespace Deletion

When a Waldur resource termination order is processed:

1. The `ManagedNamespace` CR is deleted
2. All 3 Keycloak groups are deleted

### Limit Updates

When resource limits are updated in Waldur:

1. Limits are converted to K8s resource quantities
2. The CR's `spec.quota` is patched with the new values

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

Two different sources, depending on the component — in both cases **metered × elapsed
time**, the same convention SLURM's own `ReqTRES × Elapsed` already uses in this
system, accumulated into a running month-to-date total per component:

- **cpu/ram/gpu**: summed from every *Running* pod's container resource requests in
  the namespace (`spec.cpu`/`memory`/`nvidia.com/gpu`), sampled on every report call.
  This reflects what's actually scheduled right now, not a standing allocation — a
  namespace with its quota untouched but nothing running in it reports zero for these.
- **storage**: a PVC isn't a per-pod resource request and persists independent of any
  pod, so this one component stays metered from the `ManagedNamespace`'s own
  `spec.quota` — a point-in-time snapshot, with no history of what it was a moment ago
  (there is no per-namespace consumption log the way `sacct` provides one for SLURM).

Usage values are in Waldur component units, using the reverse of the component quota
mapping (e.g., K8s `limits.memory: 4Gi` → Waldur `ram: 4`) for storage, or the pod
request's own quantity for cpu/ram/gpu (e.g. a pod requesting `cpu: 500m` → `0.5`).

Sampling, not a continuous watch: a pod that starts and finishes between two report
calls is missed entirely, the same class of imprecision SLURM's own `ReqTRES ×
Elapsed` already accepts (bills *requested*, not measured utilization) — not a new
one introduced here.

The running total is persisted as a JSON-encoded annotation on the CR itself
(`provisioning.hpc.ut.ee/usage-accumulator`), not in local site-agent state, so it
survives an agent restart or reschedule. It resets to zero at the start of each
calendar month, matching `sacct`'s own month-to-date convention.

**Requires `accounting_type: "usage"`** on the components you want metered this way
(see the example configs below) — Waldur bills `accounting_type: "limit"` components
from their allocated limit directly and never looks at what this method reports,
regardless of its contents. Use `"limit"` instead for a component you want billed by
allocation size rather than measured usage.

**Requires pods to actually exist in a real Kubernetes `Namespace`** matching the CR's
`spec.name` — this plugin only ever creates the `ManagedNamespace` CR itself; turning
that into a real `Namespace` (and binding RBAC/quota to it) is a separate, external
operator/controller's job. Without one, `cpu`/`ram`/`gpu` usage is always zero (no real
namespace means no real pods), while `storage` still reads normally from the CR's own
quota.
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
| Downscale | Quota set to minimal: cpu=1, memory=1Gi, storage=1Gi |
| Pause | Quota set to zero: cpu=0, memory=0Gi, storage=0Gi |
| Restore | **No-op** (limits come back with the next limit update) |

## Error Handling

- Kubernetes connectivity issues are logged and raised as `BackendError`
- Keycloak initialization failure logs a warning; user management operations become no-ops
- CR creation failure triggers automatic Keycloak group cleanup
- Missing users in Keycloak are logged as warnings and skipped
- Missing backend ID on deletion is logged and skipped gracefully

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
