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
- Python 3.11 or higher
- `uv` package manager installed
- Root access (required for SLURM commands)
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
    mqtt_enabled: false
    stomp_enabled: false

    backend_type: "slurm"
    backend_settings:
      default_account: "root"           # Root account in SLURM
      customer_prefix: "hpc_"           # Prefix for customer accounts
      project_prefix: "hpc_"            # Prefix for project accounts
      allocation_prefix: "hpc_"         # Prefix for allocation accounts

      # QoS settings (not used in report mode but required)
      qos_downscaled: "limited"
      qos_paused: "paused"
      qos_default: "normal"

      # Home directory settings (not used in report mode)
      enable_user_homedir_account_creation: false
      homedir_umask: "0700"

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

- `default_account`: Root account in SLURM cluster
- Prefixes: Used to identify accounts created by the agent (for filtering)

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

1. **Initialization**: Agent loads configuration and connects to SLURM cluster
2. **Account Discovery**: Identifies accounts matching configured prefixes
3. **Usage Collection**:
   - Runs `sacct` to collect usage data for current billing period
   - Aggregates CPU and memory usage per account and user
   - Converts SLURM units to Waldur units using configured factors
4. **Reporting**: Sends usage data to Waldur Mastermind API
5. **Sleep**: Waits for configured interval (default: 30 minutes)
6. **Repeat**: Returns to step 3

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

1. **Test SLURM connectivity:**

```bash
uv run waldur_site_diagnostics
```

1. **Verify configuration:**

```bash
# Check if configuration is valid
uv run waldur_site_agent -m report -c /etc/waldur/waldur-site-agent-config.yaml --dry-run
```

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

- Verify accounts exist in SLURM with configured prefixes
- Check SLURM accounting database has recent data
- Ensure users have submitted jobs in the current billing period

#### Permission Errors

- Agent typically needs root access for SLURM commands
- Verify service runs as root user
- Check file permissions on configuration file

### Debugging

Enable debug logging by setting environment variable:

```bash
export WALDUR_SITE_AGENT_LOG_LEVEL=DEBUG
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

**Staff User Requirements:**

- Historical usage loading requires a **staff user API token**
- Regular offering API tokens cannot submit historical data
- The staff user must have appropriate permissions in Waldur

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

The command provides detailed progress information:

```text
🚀 Starting historical usage loading
📊 Will process 12 months of data
📅 Processing month 1/12: 2024-01
📋 Found 5 active resources to process
📊 Processing usage data for 5 accounts
📤 Submitted usage for resource project1_allocation: {'cpu': 15000, 'mem': 25000}
✅ Completed processing 2024-01 (5 resources)
📅 Processing month 2/12: 2024-02
...
🎉 Historical usage loading completed successfully!
Processed 12 months from 2024-01-01 to 2024-12-31
```

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
❌ Historical usage loading requires staff user privileges
```

- Solution: Use an API token from a user with `is_staff=True` in Waldur

**No Resources Found:**

```text
ℹ️ No active resources found for offering, skipping month
```

- Solution: Ensure resources exist in Waldur and have `backend_id` values set

**No Usage Data:**

```text
ℹ️ No usage data found for 2024-01
```

- Solution: Check SLURM accounting database has data for that period
- Verify SLURM account names match Waldur resource `backend_id` values

**Backend Not Supported:**

```text
❌ Backend does not support historical usage reporting
```

- Solution: Ensure you're using the SLURM backend and have updated code

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
# Verify SLURM usage data matches what was submitted
sacct --accounts=project1_allocation \
      --starttime=2024-01-01 \
      --endtime=2024-01-31 \
      --allocations \
      --allusers \
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
- Historical data submission requires staff tokens, regular reporting uses offering tokens
