# CSCS-DWDI Plugin for Waldur Site Agent

This plugin provides integration with the CSCS Data Warehouse Data Intelligence (DWDI) system to report both
computational and storage usage data to Waldur.

## Overview

The plugin implements two separate backends to handle different types of accounting data:

- **Compute Backend** (`cscs-dwdi-compute`): Reports CPU and node hour usage from HPC clusters
- **Storage Backend** (`cscs-dwdi-storage`): Reports storage space and inode usage from filesystems

## Backend Types

### Compute Backend

The compute backend queries the DWDI API for computational resource usage and reports:

- Node hours consumed by accounts and users
- CPU hours consumed by accounts and users
- Account-level and user-level usage aggregation

**API Endpoints Used:**

- `/api/v1/compute/usage-month/account` - Monthly usage data
- `/api/v1/compute/usage-day/account` - Daily usage data

### Storage Backend

The storage backend queries the DWDI API for storage resource usage and reports:

- Storage space used (converted from bytes to configured units)
- Inode (file count) usage
- Path-based resource identification

**API Endpoints Used:**

- `/api/v1/storage/usage-month/filesystem_name/data_type` - Monthly storage usage
- `/api/v1/storage/usage-day/filesystem_name/data_type` - Daily storage usage

## Configuration

### Compute Backend Configuration

```yaml
backend_type: "cscs-dwdi-compute"

backend_settings:
  cscs_dwdi_api_url: "https://dwdi.cscs.ch"
  cscs_dwdi_client_id: "your_oidc_client_id"
  cscs_dwdi_client_secret: "your_oidc_client_secret"
  cscs_dwdi_oidc_token_url: "https://auth.cscs.ch/realms/cscs/protocol/openid-connect/token"
  cscs_dwdi_oidc_scope: "openid"  # Optional

backend_components:
  nodeHours:
    measured_unit: "node-hours"
    unit_factor: 1
    accounting_type: "usage"
    label: "Node Hours"

  cpuHours:
    measured_unit: "cpu-hours"
    unit_factor: 1
    accounting_type: "usage"
    label: "CPU Hours"
```

### Storage Backend Configuration

```yaml
backend_type: "cscs-dwdi-storage"

backend_settings:
  cscs_dwdi_api_url: "https://dwdi.cscs.ch"
  cscs_dwdi_client_id: "your_oidc_client_id"
  cscs_dwdi_client_secret: "your_oidc_client_secret"
  cscs_dwdi_oidc_token_url: "https://auth.cscs.ch/realms/cscs/protocol/openid-connect/token"

  # Storage-specific settings
  storage_filesystem: "lustre"
  storage_data_type: "projects"
  storage_tenant: "cscs"  # Optional

  # Map Waldur resource IDs to storage paths
  storage_path_mapping:
    "project_123": "/store/projects/proj123"
    "project_456": "/store/projects/proj456"

backend_components:
  storage_space:
    measured_unit: "GB"
    unit_factor: 0.000000001  # Convert bytes to GB
    accounting_type: "usage"
    label: "Storage Space (GB)"

  storage_inodes:
    measured_unit: "count"
    unit_factor: 1
    accounting_type: "usage"
    label: "File Count"
```

## Authentication

Both backends use OIDC client credentials flow for authentication with the DWDI API. You need:

- `cscs_dwdi_client_id`: OIDC client identifier
- `cscs_dwdi_client_secret`: OIDC client secret
- `cscs_dwdi_oidc_token_url`: OIDC token endpoint URL
- `cscs_dwdi_oidc_scope`: OIDC scope (optional, defaults to "openid")

## SOCKS Proxy Support

Both backends support SOCKS proxy for network connectivity. This is useful when the DWDI API is only accessible
through a proxy or jump host.

### SOCKS Proxy Configuration

Add the SOCKS proxy setting to your backend configuration:

```yaml
backend_settings:
  # ... other settings ...
  socks_proxy: "socks5://localhost:12345"  # SOCKS5 proxy URL
```

### Supported Proxy Types

- **SOCKS5**: `socks5://hostname:port`
- **SOCKS4**: `socks4://hostname:port`
- **HTTP**: `http://hostname:port`

### Usage Examples

**SSH Tunnel with SOCKS5:**

```bash
# Create SSH tunnel to jump host
ssh -D 12345 -N user@jumphost.cscs.ch

# Configure backend to use tunnel
backend_settings:
  socks_proxy: "socks5://localhost:12345"
```

**HTTP Proxy:**

```yaml
backend_settings:
  socks_proxy: "http://proxy.cscs.ch:8080"
```

## Resource Identification

### Compute Resources

For compute resources, the system uses account names as returned by the DWDI API. The Waldur resource
`backend_id` should match the account name in the cluster accounting system.

### Storage Resources

For storage resources, there are two options:

1. **Direct Path Usage**: Set the Waldur resource `backend_id` to the actual filesystem path
2. **Path Mapping**: Use the `storage_path_mapping` setting to map resource IDs to paths

## Usage Reporting

Both backends are read-only and designed for usage reporting. They implement the `_get_usage_report()` method
but do not support:

- Account creation/deletion
- Resource management
- User management
- Limit setting

## Example Configurations

See the `examples/` directory for complete configuration examples:

- `cscs-dwdi-compute-config.yaml` - Compute backend only
- `cscs-dwdi-storage-config.yaml` - Storage backend only
- `cscs-dwdi-combined-config.yaml` - Both backends in one configuration

## Installation

The plugin is automatically discovered when the waldur-site-agent-cscs-dwdi package is installed alongside waldur-site-agent.

```bash
# Install all workspace packages including cscs-dwdi plugin
uv sync --all-packages
```

## Testing

Run the test suite:

```bash
uv run pytest plugins/cscs-dwdi/tests/
```

## API Compatibility

This plugin is compatible with DWDI API version 1 (`/api/v1/`). It requires the following API endpoints to be available:

**Compute API:**

- `/api/v1/compute/usage-month/account`
- `/api/v1/compute/usage-day/account`

**Storage API:**

- `/api/v1/storage/usage-month/filesystem_name/data_type`
- `/api/v1/storage/usage-day/filesystem_name/data_type`

## Troubleshooting

### Authentication Issues

- Verify OIDC client credentials are correct
- Check that the token endpoint URL is accessible
- Ensure the client has appropriate scopes

### Storage Backend Issues

- Verify `storage_filesystem` and `storage_data_type` match available values in DWDI
- Check `storage_path_mapping` if using custom resource IDs
- Ensure storage paths exist in the DWDI system

### Connection Issues

- Use the `ping()` method to test API connectivity
- Check network connectivity to the DWDI API endpoint
- Verify SSL/TLS configuration
- If behind a firewall, configure SOCKS proxy (`socks_proxy` setting)

### Proxy Issues

- Verify proxy server is running and accessible
- Check proxy authentication if required
- Test proxy connectivity manually: `curl --proxy socks5://localhost:12345 https://dwdi.cscs.ch`
- Ensure proxy supports the required protocol (SOCKS4/5, HTTP)

## Development

### Project Structure

```text
plugins/cscs-dwdi/
├── pyproject.toml                           # Plugin configuration
├── README.md                               # This documentation
├── examples/                               # Configuration examples
├── waldur_site_agent_cscs_dwdi/
│   ├── __init__.py                         # Package init
│   ├── backend.py                          # Backend implementations
│   └── client.py                          # CSCS-DWDI API client
└── tests/
    └── test_cscs_dwdi.py                  # Plugin tests
```

### Key Classes

- **`CSCSDWDIComputeBackend`**: Compute usage reporting backend
- **`CSCSDWDIStorageBackend`**: Storage usage reporting backend
- **`CSCSDWDIClient`**: HTTP client for CSCS-DWDI API communication

### Extension Points

To extend the plugin:

1. **Additional Endpoints**: Modify `CSCSDWDIClient` to support more API endpoints
2. **Authentication Methods**: Update authentication logic in `client.py`
3. **Data Processing**: Enhance response processing methods for additional data formats
