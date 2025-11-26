# Waldur Site Agent - Rancher Plugin

This plugin enables integration between Waldur Site Agent and Rancher for Kubernetes project management with optional
Keycloak user group integration.

## Features

- **Rancher Project Management**: Creates and manages Rancher projects with resource-specific naming
- **OIDC Group Integration**: Creates hierarchical Keycloak groups that map to Rancher project roles via OIDC
- **Automatic User Management**: Adds/removes users from Keycloak groups based on Waldur project membership
- **Resource Quotas**: Sets CPU and memory limits as Rancher project quotas
- **Usage Reporting**: Reports actual allocated resources (CPU, memory, storage) from Kubernetes
- **Complete Lifecycle**: Creates groups, binds to projects, manages users, cleans up empty groups
- **Enhanced Descriptions**: Project descriptions include customer and project names for clarity

## Architecture

The plugin follows the Waldur Site Agent plugin architecture and consists of:

- **RancherBackend**: Main backend implementation that orchestrates project and user management
- **RancherClient**: Handles Rancher API operations for project management
- **KeycloakClient**: Manages Keycloak groups and user memberships

### Key Architecture Features

- **Resource-Specific Naming**: Rancher projects named after resource slugs for better identification
- **OIDC-Based Access**: No direct user-to-Rancher assignments; all access via Keycloak groups
- **Enhanced Backend Interface**: Full `WaldurResource` context available to all backend methods
- **Automatic Cleanup**: Empty groups and role bindings automatically removed
- **Real-World Validated**: Tested with actual Rancher and Keycloak instances

## Installation

1. Install the plugin using uv:

```bash
uv sync --all-packages
```

1. The plugin will be automatically discovered via Python entry points.

## Setup Requirements

### Rancher Server Setup

#### Required Rancher Credentials

1. **Rancher Server**: Accessible Rancher instance
2. **API Access**: Unscoped API token with cluster access
3. **Cluster ID**: Target cluster ID (format: `c-xxxxx`, not `c-xxxxx:p-xxxxx`)

#### Creating Rancher API Tokens

1. Login to Rancher UI
2. Navigate to: User Profile → API & Keys
3. Create Token:
   - **Name**: `waldur-site-agent`
   - **Scope**: `No Scope` (unscoped for full access)
   - **Expires**: Set appropriate expiration
4. **Save**: Access Key and Secret Key
5. **Find Cluster ID**: In Rancher UI, cluster URL shows cluster ID (e.g., `c-j8276`)

### Keycloak Setup (Optional)

#### Required for OIDC Group Integration

1. **Keycloak Server**: Accessible Keycloak instance
2. **Target Realm**: Where user accounts and groups will be managed
3. **Service User**: User with group management permissions

#### Creating Keycloak Service User

1. Login to Keycloak Admin Console
2. Select Target Realm: (e.g., `your-realm`)
3. Create User:
   - **Username**: `waldur-site-agent-rancher`
   - **Email Verified**: Yes
   - **Enabled**: Yes
4. **Set Password**: In Credentials tab (temporary: No)
5. **Assign Roles**: In Role Mappings tab
   - **Client Roles** → `realm-management`
   - **Add**: `manage-users` (sufficient for group operations)

### Waldur Marketplace Setup

#### Required Waldur Configuration

1. **Marketplace Offering**: Created with type `Marketplace.Slurm`
2. **Components**: Configured via `waldur_site_load_components`
3. **Offering State**: Must be `Active` for order processing

#### Setting Up Offering Components

1. **Create configuration file** with component definitions
2. **Run component loader**:

   ```bash
   uv run waldur_site_load_components -c your-config.yaml
   ```

3. **Activate offering** in Waldur Admin UI (change from Draft to Active)

## Complete Setup Example

### Step 1: Create Configuration File

```yaml
# rancher-offering-config.yaml
offerings:
  - name: "your-rancher-offering"

    # Waldur API configuration
    waldur_api_url: "https://your-waldur.com/"
    waldur_api_token: "your-waldur-api-token"
    waldur_offering_uuid: "your-offering-uuid"

    # Backend configuration
    backend_type: "rancher"
    order_processing_backend: "rancher"
    membership_sync_backend: "rancher"
    reporting_backend: "rancher"

    backend_settings:
      # Rancher configuration
      backend_url: "https://your-rancher.com"
      username: "token-xxxxx"  # Rancher access key
      password: "your-secret-key"  # Rancher secret key
      cluster_id: "c-xxxxx"  # Cluster ID only
      verify_cert: true
      project_prefix: "waldur-"
      default_role: "project-member"

      # Keycloak integration (optional)
      keycloak_enabled: true
      keycloak_role_name: "project-member"
      keycloak_use_user_id: true  # Use Waldur username as Keycloak user ID
      keycloak_url: "https://your-keycloak.com/"
      keycloak_realm: "your-realm"
      keycloak_user_realm: "your-realm"
      keycloak_username: "waldur-site-agent-rancher"
      keycloak_password: "your-keycloak-password"
      keycloak_ssl_verify: true

    # Component definitions
    backend_components:
      cpu:
        type: "cpu"
        measured_unit: "cores"
        accounting_type: "limit"
        label: "CPU Cores"
        unit_factor: 1
      memory:
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

### Step 2: Load Components

```bash
uv run waldur_site_load_components -c rancher-offering-config.yaml
```

### Step 3: Activate Offering

1. Login to Waldur Admin UI
2. Navigate to: Marketplace → Provider Offerings
3. Find your offering and change state from `Draft` to `Active`

### Step 4: Start Order Processing

```bash
uv run waldur_site_agent -c rancher-offering-config.yaml -m order_process
```

### Step 5: Verify Setup

```bash
uv run waldur_site_diagnostics -c rancher-offering-config.yaml
```

## Configuration

### Basic Configuration (Rancher only)

```yaml
waldur:
  api_url: "https://waldur.example.com/api/"
  token: "your-waldur-api-token-here"

offerings:
  - name: "rancher-projects"
    uuid: "12345678-1234-5678-9abc-123456789012"
    backend_type: "rancher"

    backend:
      backend_url: "https://rancher.example.com"
      username: "your-rancher-access-key"
      password: "your-rancher-secret-key"
      cluster_id: "c-m-1234abcd:p-5678efgh"
      verify_cert: true
      project_prefix: "waldur-"
      default_role: "project-member"
      keycloak_enabled: false

    components:
      cpu:
        type: "cpu"
        name: "CPU"
        measured_unit: "cores"
        billing_type: "fixed"
```

### Full Configuration (with Keycloak)

```yaml
waldur:
  api_url: "https://waldur.example.com/api/"
  token: "your-waldur-api-token-here"

offerings:
  - name: "rancher-kubernetes"
    uuid: "12345678-1234-5678-9abc-123456789012"
    backend_type: "rancher"

    backend:
      backend_url: "https://rancher.example.com"
      username: "your-rancher-access-key"
      password: "your-rancher-secret-key"
      cluster_id: "c-m-1234abcd:p-5678efgh"
      verify_cert: true
      project_prefix: "waldur-"
      default_role: "project-member"

      keycloak_enabled: true
      keycloak_role_name: "project-member"
      keycloak_url: "https://keycloak.example.com/auth/"
      keycloak_realm: "waldur"
      keycloak_user_realm: "master"
      keycloak_username: "keycloak-admin"
      keycloak_password: "your-keycloak-admin-password"
      keycloak_ssl_verify: true
      keycloak_sync_frequency: 15

    components:
      cpu:
        type: "cpu"
        name: "CPU"
        measured_unit: "cores"
        billing_type: "fixed"
      memory:
        type: "ram"
        name: "RAM"
        measured_unit: "GB"
        billing_type: "fixed"
      storage:
        type: "storage"
        name: "Storage"
        measured_unit: "GB"
        billing_type: "fixed"
      pods:
        type: "pods"
        name: "Pods"
        measured_unit: "pods"
        billing_type: "fixed"
```

## Configuration Reference

### Rancher Settings (matching waldur-mastermind format)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `backend_url` | string | Yes | - | Rancher server URL (e.g., <https://rancher.example.com>) |
| `username` | string | Yes | - | Rancher access key (called username in waldur-mastermind) |
| `password` | string | Yes | - | Rancher secret key |
| `cluster_id` | string | Yes | - | Rancher cluster ID (e.g., c-m-1234abcd, not c-m-1234abcd:p-xxxxx) |
| `verify_cert` | boolean | No | true | Whether to verify SSL certificates |
| `project_prefix` | string | No | "waldur-" | Prefix for created Rancher project names |
| `default_role` | string | No | "project-member" | Default role assigned to users in Rancher |
| `keycloak_role_name` | string | No | "project-member" | Role name used in Keycloak group naming |
| `keycloak_use_user_id` | boolean | No | true | Use Keycloak user ID for lookup (false = use username) |

### Keycloak Settings (optional, matching waldur-mastermind format)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `keycloak_enabled` | boolean | No | false | Enable Keycloak integration |
| `keycloak_url` | string | Conditional | - | Keycloak server URL |
| `keycloak_realm` | string | Conditional | "waldur" | Keycloak realm name |
| `keycloak_user_realm` | string | Conditional | "master" | Keycloak user realm for auth |
| `keycloak_username` | string | Conditional | - | Keycloak admin username |
| `keycloak_password` | string | Conditional | - | Keycloak admin password |
| `keycloak_ssl_verify` | boolean | No | true | Whether to verify SSL certificates |

## Usage

### Running the Agent

Start the agent with your configuration file:

```bash
uv run waldur_site_agent -c rancher-config.yaml -m order_process
```

### Diagnostics

Run diagnostics to check connectivity:

```bash
uv run waldur_site_diagnostics -c rancher-config.yaml
```

### Supported Agent Modes

- **order_process**: Creates and manages Rancher projects based on Waldur resource orders
- **membership_sync**: Synchronizes user memberships between Waldur and Rancher/Keycloak
- **report**: Reports resource usage from Rancher projects to Waldur

## Project Management

### Project Creation

When a Waldur resource (representing project access) is created:

1. A Rancher project is created with the name `{project_prefix}{waldur_project_slug}`
2. If Keycloak is enabled, hierarchical groups are created:
   - **Parent Group**: `c_{cluster_uuid_hex}` (cluster-level access)
   - **Child Group**: `project_{project_uuid_hex}_{role_name}` (project + role access)
3. Resource quotas are applied to the Rancher project
4. OIDC binds the Keycloak groups to Rancher project roles

### User Management

When users are added to a Waldur resource:

1. User is added to the Rancher project with the configured role
2. If Keycloak is enabled, user is added to the child group (`project_{project_uuid_hex}_{role_name}`)
3. OIDC automatically grants the user access to the Rancher project based on group membership

When users are removed:

1. User is removed from the Rancher project
2. If Keycloak is enabled, user is removed from the project role group

### Naming Convention

The plugin follows the waldur-mastermind Rancher plugin naming patterns:

- **Rancher Project Name**: `{project_prefix}{waldur_resource_slug}` (configurable prefix)
- **Keycloak Parent Group**: `c_{cluster_uuid_hex}` (cluster access)
- **Keycloak Child Group**: `project_{project_uuid_hex}_{role_name}` (project + role access)

Where:

- `{project_prefix}` is configurable (default: `waldur-`)
- `{waldur_resource_slug}` is the Waldur resource slug (more specific than project slug)
- `{cluster_uuid_hex}` is the cluster UUID in hex format
- `{project_uuid_hex}` is the Waldur project UUID in hex format (for permissions)
- `{role_name}` is configurable (default: `project-member`)

## Supported Components and Accounting Model

The plugin supports the following resource components (all with `billing_type: "limit"`):

- **CPU**: Measured in cores
- **Memory**: Measured in GB
- **Storage**: Measured in GB

### Accounting Model

**Project Limits (Quotas)**:

- Only **CPU and memory limits** are set as Rancher project quotas
- Storage is not enforced as quotas (reported only)

**Usage Reporting** (for all components):

All components report **actual allocated resources**:

- **CPU**: Sum of all container CPU requests in the project
- **Memory**: Sum of all container memory requests in the project
- **Storage**: Sum of all persistent volume claims in the project

### Accounting Flow

1. **Project Creation**: CPU and memory limits → Rancher project quotas
2. **Usage Reporting**: All components → actual allocated resources from Kubernetes

## Complete Workflow

The plugin provides end-to-end automation for Rancher project and user management:

### Order Processing

1. **Order Detection**: Monitors Waldur for new resource orders
2. **Project Creation**: Creates Rancher project named `{prefix}{resource_slug}`
3. **Enhanced Descriptions**: Includes customer and project context
4. **Quota Management**: Sets CPU and memory limits if specified
5. **OIDC Setup**: Creates and binds Keycloak groups to project roles

### Membership Sync

1. **User Detection**: Monitors Waldur for user membership changes
2. **Group Management**: Creates missing Keycloak groups if needed
3. **User Addition**: Adds users to appropriate Keycloak groups
4. **User Removal**: Removes users when removed from Waldur projects
5. **Cleanup**: Removes empty groups and their Rancher role bindings

### OIDC Integration Flow

1. **Keycloak Groups**: `c_{cluster_hex}` (parent) → `project_{project_slug}_{role}` (child)
2. **Group Binding**: `keycloakoidc_group://{group_name}` bound to Rancher project role
3. **User Management**: Users added to Keycloak groups only (not directly to Rancher)
4. **Automatic Access**: OIDC grants Rancher project access based on group membership

## Error Handling

- Rancher connectivity issues will be logged and retried
- Keycloak failures will be logged but won't stop Rancher operations
- Invalid configurations will be detected during diagnostics
- Missing users in Keycloak will be logged as warnings

## Development

### Running Tests

```bash
uv run pytest plugins/rancher/tests/
```

### Code Quality

```bash
pre-commit run --all-files
```

## Troubleshooting

### Common Issues

#### 1. Order Processing Disabled

```text
Order processing is disabled for offering X, skipping it
```

**Solution**: Add backend configuration to your offering:

```yaml
order_processing_backend: "rancher"
membership_sync_backend: "rancher"
reporting_backend: "rancher"
```

#### 2. Rancher Authentication Fails (401 Unauthorized)

```text
401 Client Error: Unauthorized for url: https://rancher.../v3
```

**Solutions**:

- Verify access key and secret key are correct
- Ensure token is **unscoped** (not cluster-specific)
- Check token hasn't expired
- Verify API URL format: `https://your-rancher.com` (without `/v3`)

#### 3. Keycloak Connection Fails (404)

```text
404: "Unable to find matching target resource method"
```

**Solutions**:

- Verify Keycloak URL (try with/without `/auth/` suffix)
- Check realm name is correct
- Ensure user exists in the specified realm

#### 4. Keycloak Group Creation Fails (403 Forbidden)

```text
403: "HTTP 403 Forbidden"
```

**Solution**: Grant user `manage-users` role:

- **Realm**: Select target realm
- **Users** → Your service user
- **Role Mappings** → **Client Roles** → `realm-management`
- **Add**: `manage-users`

#### 5. Cluster ID Format Error

```text
Cluster not found or invalid cluster ID
```

**Solution**: Use correct format:

- ✅ **Correct**: `c-j8276` (cluster ID only)
- ❌ **Incorrect**: `c-j8276:p-xxxxx` (project reference)

#### 6. Component Loading Fails

```text
KeyError: 'accounting_type'
```

**Solution**: Use correct component format:

```yaml
backend_components:
  cpu:
    type: "cpu"
    measured_unit: "cores"
    accounting_type: "limit"  # Not billing_type
    label: "CPU Cores"
    unit_factor: 1
```

### Logging

Enable debug logging to see detailed operation logs:

```yaml
logging:
  level: DEBUG
```

### Diagnostic Commands

Run comprehensive diagnostics:

```bash
uv run waldur_site_diagnostics -c your-config.yaml
```

This will test:

- Rancher API connectivity and authentication
- Keycloak connectivity and permissions (if enabled)
- Project listing capabilities
- Backend discovery and initialization
- Component configuration validity

### Verification Commands

Test individual components:

```bash
# Test Rancher connection
curl -u "token-xxxxx:secret-key" "https://your-rancher.com/v3"

# Test Keycloak realm access
curl "https://your-keycloak.com/auth/admin/realms/your-realm" \
  -H "Authorization: Bearer $(get-keycloak-token)"

# List Rancher projects in cluster
curl -u "token-xxxxx:secret-key" \
  "https://your-rancher.com/v3/projects?clusterId=c-xxxxx"
```
