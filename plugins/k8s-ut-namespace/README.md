# Waldur Site Agent - K8s UT Namespace Plugin

This plugin enables integration between Waldur Site Agent and Kubernetes clusters for managing
`ManagedNamespace` custom resources (CRD: `provisioning.hpc.ut.ee/v1`) with optional Keycloak
RBAC group integration.

## Features

- **ManagedNamespace Lifecycle**: Creates, updates, and deletes `ManagedNamespace` custom resources
- **Resource Quotas**: Sets CPU, memory, storage, and GPU limits as namespace quotas
- **Role-Based Access Control**: Creates 3 Keycloak groups per namespace (admin, readwrite, readonly)
- **Waldur Role Mapping**: Maps Waldur roles to namespace access levels automatically
- **User Management**: Adds/removes users from Keycloak groups, reconciles role changes
- **Usage Reporting**: Reports namespace quota allocations back to Waldur
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
| `keycloak_use_user_id` | boolean | No | `true` | Use Keycloak user ID for lookup (false = use username) |

### Keycloak Settings (Optional)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `keycloak_enabled` | boolean | No | `false` | Enable Keycloak RBAC integration |
| `keycloak.keycloak_url` | string | Conditional | - | Keycloak server URL |
| `keycloak.keycloak_realm` | string | Conditional | - | Keycloak realm name |
| `keycloak.keycloak_user_realm` | string | Conditional | - | Keycloak user realm for auth |
| `keycloak.keycloak_username` | string | Conditional | - | Keycloak admin username |
| `keycloak.keycloak_password` | string | Conditional | - | Keycloak admin password |
| `keycloak.keycloak_ssl_verify` | boolean | No | `true` | Whether to verify SSL certificates |

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
- **report**: Reports namespace quota allocations to Waldur

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

When users are removed:

1. User is removed from all 3 Keycloak groups

### Status Operations

| Operation | Effect |
|-----------|--------|
| Downscale | Quota set to minimal: cpu=1, memory=1Gi, storage=1Gi |
| Pause | Quota set to zero: cpu=0, memory=0Gi, storage=0Gi |
| Restore | No-op (limits should be re-set via a separate update order) |

## Error Handling

- Kubernetes connectivity issues are logged and raised as `BackendError`
- Keycloak initialization failure logs a warning; user management operations become no-ops
- CR creation failure triggers automatic Keycloak group cleanup
- Missing users in Keycloak are logged as warnings and skipped
- Missing backend ID on deletion is logged and skipped gracefully

## Development

### Running Tests

```bash
.venv/bin/python -m pytest plugins/k8s-ut-namespace/tests/
```

### Code Quality

```bash
pre-commit run --all-files
```
