# Configuration Reference

This document provides a complete reference for configuring Waldur Site Agent.

## Configuration File Structure

The agent uses a YAML configuration file (`waldur-site-agent-config.yaml`) with the following structure:

```yaml
sentry_dsn: ""
timezone: "UTC"
offerings:
  - name: "Example Offering"
    # Offering-specific configuration...
```

## Global Settings

### `sentry_dsn`

- **Type**: String
- **Description**: Data Source Name for Sentry error tracking
- **Default**: Empty (disabled)
- **Example**: `"https://key@sentry.io/project"`

### `timezone`

- **Type**: String
- **Description**: Timezone for billing period calculations
- **Default**: System timezone
- **Recommended**: `"UTC"`
- **Examples**: `"UTC"`, `"Europe/Tallinn"`, `"America/New_York"`

**Note**: Important when agent and Waldur are deployed in different timezones to prevent billing period
mismatches at month boundaries.

## Offering Configuration

Each offering in the `offerings` array represents a separate service offering.

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
- **Required**: Yes
- **Description**: Token for Waldur API authentication
- **Security**: Keep this secret and secure

#### `verify_ssl`

- **Type**: Boolean
- **Default**: `true`
- **Description**: Whether to verify SSL certificates for Waldur API

#### `waldur_offering_uuid`

- **Type**: String
- **Required**: Yes
- **Description**: UUID of the offering in Waldur
- **Note**: Found in Waldur UI under Integration -> Credentials

### Backend Configuration

#### `backend_type`

- **Type**: String
- **Required**: Yes for legacy configurations
- **Values**: `"slurm"`, `"moab"`, `"mup"`
- **Description**: Type of backend (legacy setting, use specific backend settings instead)

#### Backend Selection

Configure which backends to use for different operations:

```yaml
order_processing_backend: "slurm"    # Backend for order processing
membership_sync_backend: "slurm"     # Backend for membership syncing
reporting_backend: "slurm"           # Backend for usage reporting
username_management_backend: "base"  # Backend for username management
```

**Available backends** (via entry points):

- `"slurm"`: SLURM cluster management
- `"moab"`: MOAB cluster management
- `"mup"`: MUP portal integration
- `"waldur"`: Waldur-to-Waldur federation
- `"base"`: Basic username management
- Custom backends via plugins

**Note**: If a backend setting is omitted, that process won't start for the offering.

### Event Processing

#### `stomp_enabled`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Enable STOMP-based event processing

#### `websocket_use_tls`

- **Type**: Boolean
- **Default**: `true`
- **Description**: Use TLS for websocket connections

### Resource Management

#### `resource_import_enabled`

- **Type**: Boolean
- **Default**: `false`
- **Description**: Whether to expose importable resources to Waldur

## Backend-Specific Settings

### SLURM Backend Settings

```yaml
backend_settings:
  default_account: "root"                              # Default parent account
  customer_prefix: "hpc_"                              # Prefix for customer accounts
  project_prefix: "hpc_"                               # Prefix for project accounts
  allocation_prefix: "hpc_"                            # Prefix for allocation accounts
  qos_downscaled: "limited"                           # QoS for downscaled accounts
  qos_paused: "paused"                                # QoS for paused accounts
  qos_default: "normal"                               # Default QoS
  enable_user_homedir_account_creation: true         # Create home directories
  homedir_umask: "0700"                              # Umask for home directories
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
  # MUP-specific settings
  api_url: "https://mup.example.com/api/"
  api_token: "your-api-token"
  # Other MUP-specific configuration
```

### Waldur Federation Backend Settings

```yaml
backend_settings:
  target_api_url: "https://waldur-b.example.com/api/"
  target_api_token: "service-account-token"
  target_offering_uuid: "offering-uuid-on-waldur-b"
  target_customer_uuid: "customer-uuid-on-waldur-b"
  user_match_field: "cuid"                   # cuid | email | username
  order_poll_timeout: 300                    # Max seconds for sync order completion
  order_poll_interval: 5                     # Seconds between sync order polls
  user_not_found_action: "warn"              # warn | fail
  # Optional: target STOMP for instant async order completion
  target_stomp_enabled: false
  target_stomp_offering_uuid: ""             # Marketplace.Slurm offering on B
```

## Backend Components

Define computing components tracked by the backend:

```yaml
backend_components:
  cpu:
    measured_unit: "k-Hours"           # Waldur measured unit
    unit_factor: 60000                 # Conversion factor
    accounting_type: "usage"           # "usage" or "limit"
    label: "CPU"                       # Display label in Waldur
  mem:
    limit: 10                          # Fixed limit amount
    measured_unit: "gb-Hours"
    unit_factor: 61440                 # 60 * 1024
    accounting_type: "usage"
    label: "RAM"
```

### Component Settings

#### `measured_unit`

- **Type**: String
- **Description**: Unit displayed in Waldur
- **Examples**: `"k-Hours"`, `"gb-Hours"`, `"EUR"`

#### `unit_factor`

- **Type**: Number
- **Description**: Factor for conversion from Waldur units to backend units
- **Examples**:
  - `60000` for CPU (60 * 1000, converts k-Hours to CPU-minutes)
  - `61440` for memory (60 * 1024, converts gb-Hours to MB-minutes)

#### `accounting_type`

- **Type**: String
- **Values**: `"usage"` or `"limit"`
- **Description**: Whether component tracks usage or limits

#### `label`

- **Type**: String
- **Description**: Human-readable label displayed in Waldur

#### `limit`

- **Type**: Number
- **Optional**: Yes
- **Description**: Fixed limit amount for limit-type components

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

Override configuration values using environment variables:

### Agent Timing

- `WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES`: Order processing period (default: 5)
- `WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES`: Reporting period (default: 30)
- `WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES`: Membership sync period (default: 5)

### Monitoring

- `SENTRY_ENVIRONMENT`: Environment name for Sentry

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
      homedir_umask: "0700"

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

    order_processing_backend: "moab"
    membership_sync_backend: "moab"
    reporting_backend: "moab"
    username_management_backend: "base"

    backend_settings:
      default_account: "root"
      customer_prefix: "c_"
      project_prefix: "p_"
      allocation_prefix: "a_"
      enable_user_homedir_account_creation: true

    backend_components:
      deposit:
        measured_unit: "EUR"
        accounting_type: "limit"
        label: "Deposit (EUR)"
```

### Event-Based Processing

```yaml
offerings:
  - name: "Event-Driven SLURM"
    # ... basic settings ...

    stomp_enabled: true
    websocket_use_tls: true

    order_processing_backend: "slurm"
    reporting_backend: "slurm"
    # Note: membership_sync_backend omitted for event processing
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
      target_api_token: "service-account-token-for-waldur-b"
      target_offering_uuid: "offering-uuid-on-waldur-b"
      target_customer_uuid: "customer-uuid-on-waldur-b"
      user_match_field: "cuid"
      order_poll_timeout: 300
      order_poll_interval: 5
      user_not_found_action: "warn"
      target_stomp_enabled: true
      target_stomp_offering_uuid: "agent-offering-uuid-on-waldur-b"

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
# Test configuration syntax
waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml

# Load components (validates backend configuration)
waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```
