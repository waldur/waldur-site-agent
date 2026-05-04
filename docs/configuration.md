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

### `elastic_apm_server_url`

- **Type**: String
- **Description**: Elastic APM server URL. When set, enables Elastic APM monitoring with automatic
  instrumentation.
- **Default**: Empty (disabled)
- **Example**: `"https://apm-server.example.com:8200"`

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
- **Permissions**: The token user must have **OFFERING.MANAGER** role on the offering specified by
  `waldur_offering_uuid`. This grants the permissions needed for order processing, usage reporting,
  membership sync, and event subscriptions.
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
- `"rancher"`: Direct Rancher REST API integration (single offering = one cluster)
- `"rancher-kc-crd"`: CRD-driven Rancher + Keycloak management via the
  [`rancher-keycloak-operator`](https://github.com/waldur/rancher-keycloak-operator).
  Membership-sync only; targets multiple clusters per offering by reading
  `cluster_id` from each Resource's `backend_id`. See
  [`plugins/rancher-kc-crd/README.md`](../plugins/rancher-kc-crd/README.md).
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
  # MUP-specific settings
  api_url: "https://mup.example.com/api/"
  api_token: "your-api-token"
  # Other MUP-specific configuration
```

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
# Test configuration syntax
waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml

# Load components (validates backend configuration)
waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```
