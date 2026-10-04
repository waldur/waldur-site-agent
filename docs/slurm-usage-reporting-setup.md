# SLURM Usage Reporting Setup Guide

This guide explains how to set up a single Waldur Site Agent instance for usage reporting with SLURM backend.
This configuration is ideal when you only need to collect and report usage data from your SLURM cluster to
Waldur Mastermind.

## Overview

The usage reporting agent (`report` mode) collects CPU, memory, and other resource usage data from SLURM
accounting records and sends it to Waldur Mastermind. It runs in a continuous loop, fetching usage data for
the current billing period and reporting it at regular intervals.

## Prerequisites

### System Requirements

- Linux system with access to SLURM cluster head node
- Python 3.9.2 or higher
- `uv` package manager installed
- An OS account that can read every user's job records (root, `SlurmUser`, or
  a user with `AdminLevel=Operator` or higher when `PrivateData` hides jobs)
- Network access to Waldur Mastermind API

### SLURM Requirements

- SLURM accounting enabled (`sacct` and `sacctmgr` commands available)
- Access to SLURM accounting database
- Required SLURM commands:
  - `sacct` - for usage reporting
  - `sacctmgr` - for account management
  - `sinfo` - for cluster diagnostics

## Installation

### 1. Clone and Install the Application

```bash
# Clone the repository
git clone https://github.com/waldur/waldur-site-agent.git
cd waldur-site-agent

# Install dependencies with SLURM plugin
uv sync --package waldur-site-agent-slurm
```

### 2. Create Configuration Directory

```bash
sudo mkdir -p /etc/waldur
```

## Configuration

### 1. Create Configuration File

Create `/etc/waldur/waldur-site-agent-config.yaml` with the following configuration:

```yaml
sentry_dsn: ""  # Optional: Sentry DSN for error tracking
timezone: "UTC"  # Timezone for billing period calculations

offerings:
  - name: "SLURM Usage Reporting"
    waldur_api_url: "https://your-waldur-instance.com/api/"
    waldur_api_token: "your-api-token-here"
    waldur_offering_uuid: "your-offering-uuid-here"

    # Backend configuration for usage reporting only
    username_management_backend: "base"  # Not used in report mode
    order_processing_backend: "slurm"   # Not used in report mode
    membership_sync_backend: "slurm"    # Not used in report mode
    reporting_backend: "slurm"          # This is what matters for reporting

    # Event processing (not needed for usage reporting)
    stomp_enabled: false

    backend_type: "slurm"
    backend_settings:
      default_account: "root"           # DefaultAccount= on user associations
      customer_prefix: "hpc_"           # Prefix for customer accounts
      project_prefix: "hpc_"            # Prefix for project accounts
      allocation_prefix: "hpc_"         # Prefix for allocation accounts

      # Not used by report mode; shown because the same file usually also
      # drives order_process / membership_sync. Optional.
      qos_downscaled: "limited"
      qos_paused: "paused"
      qos_default: "normal"
      enable_user_homedir_account_creation: false

    # Define components for usage reporting
    backend_components:
      cpu:
        limit: 10                       # Not used in usage reporting
        measured_unit: "k-Hours"        # Waldur unit for CPU usage
        unit_factor: 60000              # Convert CPU-minutes to k-Hours (60 * 1000)
        accounting_type: "usage"        # Report actual usage
        label: "CPU"

      mem:
        limit: 10                       # Not used in usage reporting
        measured_unit: "gb-Hours"       # Waldur unit for memory usage
        unit_factor: 61440              # Convert MB-minutes to gb-Hours (60 * 1024)
        accounting_type: "usage"        # Report actual usage
        label: "RAM"
```

### 2. Configuration Parameters Explained

#### Waldur Connection

- `waldur_api_url`: URL to your Waldur Mastermind API endpoint
- `waldur_api_token`: API token for authentication (create in Waldur admin)
- `waldur_offering_uuid`: UUID of the SLURM offering in Waldur

#### Backend Settings

- `default_account`: `DefaultAccount=` set on user associations in the SLURM cluster
- Prefixes: Used to name the accounts the agent creates. Report mode does not
  use them to find accounts — see [How It Works](#how-it-works).

#### Backend Components

- `cpu`: CPU usage tracking in CPU-minutes (SLURM native unit)
- `mem`: Memory usage tracking in MB-minutes (SLURM native unit)
- `unit_factor`: Conversion factor from SLURM units to Waldur units
- `accounting_type: "usage"`: Report actual usage (not limits)

## Deployment

### Option 1: Systemd Service (Recommended)

1. **Copy service file:**

```bash
sudo cp systemd-conf/agent-report/agent.service /etc/systemd/system/waldur-site-agent-report.service
```

1. **Reload systemd and enable service:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable waldur-site-agent-report.service
sudo systemctl start waldur-site-agent-report.service
```

1. **Check service status:**

```bash
sudo systemctl status waldur-site-agent-report.service
```

### Option 2: Manual Execution

For testing or one-time runs:

```bash
# Run directly
uv run waldur_site_agent -m report -c /etc/waldur/waldur-site-agent-config.yaml

# Or with installed package
waldur_site_agent -m report -c /etc/waldur/waldur-site-agent-config.yaml
```

## Operation

### How It Works

1. **Initialization**: the agent loads the configuration and registers with Waldur.
2. **Resource discovery**: it lists the offering's resources in Waldur; each
   resource's `backend_id` is the SLURM account to report on. Accounts that are
   not Waldur resources are never reported, whatever their name.
3. **Usage collection**:
   - runs `sacct --truncate --allocations --allusers` for the current month
     (in REST mode, `GET /slurmdb/{version}/jobs/` instead — see the
     [SLURM plugin README](../plugins/slurm/README.md#rest-api-execution-mode));
   - aggregates usage per account and per user;
   - converts SLURM units to Waldur units with each component's `unit_factor`.
4. **Reporting**: sends the usage to Waldur.
5. **Sleep**: waits for the reporting interval (default 30 minutes), then
   repeats from step 2.

### Timing Configuration

Control reporting frequency with environment variable:

```bash
# Report every 15 minutes instead of default 30
export WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES=15
```

### Logging

#### Systemd Service Logs

```bash
# View service logs
sudo journalctl -u waldur-site-agent-report.service -f

# View logs for specific time period
sudo journalctl -u waldur-site-agent-report.service --since "1 hour ago"
```

#### Manual Execution Logs

Logs are written to stdout/stderr when running manually.

## Monitoring and Troubleshooting

### Health Checks

Check the configuration, the connection to Waldur and the SLURM tools in one
step (exits non-zero on failure):

```bash
uv run waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
```

The agent has no dry-run mode; the historical loader does (`--dry-run`, below).

### Common Issues

#### SLURM Commands Not Found

- Ensure SLURM tools are in PATH
- Verify `sacct` and `sacctmgr` are executable
- Check SLURM accounting is enabled

#### Authentication Errors

- Verify Waldur API token is valid
- Check network connectivity to Waldur Mastermind
- Ensure offering UUID exists in Waldur

#### No Usage Data

- Verify the Waldur resources have `backend_id` set, and that those SLURM
  accounts exist
- Check SLURM accounting database has recent data
- Ensure users have submitted jobs in the current billing period

#### Permission Errors

- `sacct --allusers` only returns other users' jobs to root, `SlurmUser` or a
  sufficiently privileged user (see Prerequisites)
- Check file permissions on configuration file

### Debugging

Enable debug logging by setting `log_level` in the agent configuration file:

```yaml
log_level: DEBUG
```

## Data Flow

```text
SLURM Cluster → sacct command → Usage aggregation → Unit conversion → Waldur API
     ↓              ↓                    ↓                ↓              ↓
- Job records  - CPU-minutes      - Per-account    - k-Hours     - POST usage
- Resource     - MB-minutes       - Per-user       - gb-Hours      data
  usage        - Account data     - Totals         - Converted
                                                    values
```

## Security Considerations

1. **API Token Security**: Store Waldur API token securely, restrict file permissions
2. **Root Access**: Agent needs root for SLURM commands - run in controlled environment
3. **Network**: Ensure secure connection to Waldur Mastermind (HTTPS)
4. **Logging**: Avoid logging sensitive data, configure log rotation

## Historical Usage Loading

In addition to regular usage reporting, the SLURM plugin supports loading historical usage data into Waldur.
This is useful for:

- Migrating existing SLURM usage data when first deploying Waldur
- Backfilling missing usage data due to outages or configuration issues
- Reconciling billing periods with historical SLURM accounting records

### Prerequisites for Historical Loading

**Token requirements:**

- By default the loader checks that `--user-token` belongs to a **staff** user.
- A service-provider token works with `--no-staff-check`, except for
  usage-based components in **past** billing periods: Mastermind only lets staff
  backfill those and rejects the submission otherwise.

**Data Requirements:**

- SLURM accounting database must contain historical data for the requested periods
- Resources must already exist in Waldur (historical loading cannot create resources)
- Offering users must be configured in Waldur for user-level usage attribution

### Historical Usage Command

```bash
# Load usage for specific date range
waldur_site_load_historical_usage \
  --config /etc/waldur/waldur-site-agent-config.yaml \
  --offering-uuid 12345678-1234-1234-1234-123456789abc \
  --user-token staff-user-api-token-here \
  --start-date 2024-01-01 \
  --end-date 2024-03-31
```

#### Command Parameters

- `--config`: Path to agent configuration file (same as regular usage reporting)
- `--offering-uuid`: UUID of the Waldur offering to load data for
- `--user-token`: **Staff user API token** (not the offering's regular API token)
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format
- `--skip-user-usage`: Submit resource-level totals only
- `--no-staff-check`: Skip the staff check (service-provider tokens)
- `--dry-run`: Log what would be submitted; send nothing
- `--reconcile-stale`: Zero Waldur usage records the backend no longer reports
  (for example usage previously attributed to the wrong month)
- `--resource-backend-id ID`: Process only this resource; repeatable

#### Processing Behavior

**Monthly Processing:**

- Historical usage is always processed **monthly** to align with Waldur's billing model
- Date ranges are automatically split into monthly billing periods
- Each month is processed independently for reliability and progress tracking

**Data Attribution:**

- Usage data is attributed to the first day of each billing month
- User usage includes both username and offering user URL when available
- Resource-level usage totals are calculated and submitted separately

**Error Handling:**

- Failed months are logged but don't stop processing of other months
- Individual user usage failures don't affect resource-level usage submission
- Progress is displayed: "Processing month 3/12: 2024-03"

### Usage Examples

#### Load Full Year of Data

```bash
# Load all of 2024
waldur_site_load_historical_usage \
  --config /etc/waldur/waldur-site-agent-config.yaml \
  --offering-uuid 12345678-1234-1234-1234-123456789abc \
  --user-token your-staff-token \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

#### Load Specific Quarter

```bash
# Load Q1 2024
waldur_site_load_historical_usage \
  --config /etc/waldur/waldur-site-agent-config.yaml \
  --offering-uuid 12345678-1234-1234-1234-123456789abc \
  --user-token your-staff-token \
  --start-date 2024-01-01 \
  --end-date 2024-03-31
```

#### Load Single Month

```bash
# Load just January 2024
waldur_site_load_historical_usage \
  --config /etc/waldur/waldur-site-agent-config.yaml \
  --offering-uuid 12345678-1234-1234-1234-123456789abc \
  --user-token your-staff-token \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### Monitoring Historical Loads

#### Progress Tracking

The command logs its progress, for example:

```text
Starting historical usage loading
Will process 12 months of data
Processing month 1/12: 2024-01 for offering 'SLURM Usage Reporting' (<uuid>)
Found 5 active resources to process
...
Historical usage loading completed successfully!
Processed 12 months from 2024-01-01 to 2024-12-31
```

Use `--dry-run` first to see what would be submitted.

#### Log Files

For production use, redirect output to log files:

```bash
waldur_site_load_historical_usage \
  --config /etc/waldur/waldur-site-agent-config.yaml \
  --offering-uuid 12345678-1234-1234-1234-123456789abc \
  --user-token your-staff-token \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  > historical_load_2024.log 2>&1
```

### Troubleshooting Historical Loads

#### Error Messages and Solutions

**No Staff Privileges:**

```text
Historical usage loading requires staff user privileges
```

- Solution: use a staff token, or `--no-staff-check` with a service-provider
  token (not enough for past periods of usage-based components)

**No Resources Found:**

```text
No active resources found for offering, skipping month
```

- Solution: Ensure resources exist in Waldur and have `backend_id` values set

**No Usage Data:**

```text
No usage data found for 2024-01
```

- Solution: Check SLURM accounting database has data for that period
- Verify SLURM account names match Waldur resource `backend_id` values

#### Performance Considerations

**Large Date Ranges:**

- Historical loads can take hours for multi-year ranges
- Each month requires multiple API calls to Waldur
- SLURM database queries may be slow for old data

**Rate Limiting:**

- Waldur may rate limit API calls during bulk submission
- Consider adding delays between months if encountering 429 errors

**Database Impact:**

- Large historical queries may impact SLURM cluster performance
- Consider running during maintenance windows for multi-year loads

#### Validation and Verification

**Verify Data in Waldur:**

1. Check resource usage in Waldur marketplace
2. Verify billing calculations include historical periods
3. Confirm user-level usage attribution is correct

**Cross-Reference with SLURM:**

```bash
# The query the agent runs (CLI mode) for January 2024
sacct --noconvert --truncate --allocations --allusers \
      --accounts=project1_allocation \
      --starttime=2024-01-01T00:00:00 \
      --endtime=2024-01-31T23:59:59 \
      --format=Account,ReqTRES,Elapsed,User
```

### Integration Notes

This setup is designed for **usage reporting only**. For a complete Waldur Site Agent deployment that includes:

- Order processing (resource creation/deletion)
- Membership synchronization
- Event processing

You would need additional agent instances or a multi-mode configuration with different service files for each mode.

**Historical Loading Integration:**

- Historical loading is a separate command, not part of regular agent operation
- Run historical loads **before** starting regular usage reporting to avoid conflicts
- Backfilling past periods of usage-based components needs a staff token; regular reporting uses the offering's token
