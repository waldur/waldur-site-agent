# CSCS-DWDI Plugin for Waldur Site Agent

This plugin provides reporting functionality for Waldur Site Agent by integrating with the CSCS-DWDI
(Data Warehouse and Data Intelligence) API.

## Overview

The CSCS-DWDI plugin is a **reporting-only backend** that fetches compute usage data from the CSCS-DWDI
service and reports it to Waldur. It supports node-hour usage tracking for multiple accounts and users.

## Features

- **Monthly Usage Reporting**: Fetches usage data for the current month
- **Multi-Account Support**: Reports usage for multiple accounts in a single API call
- **Per-User Usage**: Breaks down usage by individual users within each account
- **OIDC Authentication**: Uses OAuth2/OIDC for secure API access
- **Automatic Aggregation**: Combines usage across different clusters and time periods

## Configuration

Add the following configuration to your Waldur Site Agent offering:

```yaml
offerings:
  - name: "CSCS HPC Offering"
    reporting_backend: "cscs-dwdi"
    backend_settings:
      cscs_dwdi_api_url: "https://dwdi-api.cscs.ch"
      cscs_dwdi_client_id: "your-oidc-client-id"
      cscs_dwdi_client_secret: "your-oidc-client-secret"
      # Optional OIDC configuration (for production use)
      cscs_dwdi_oidc_token_url: "https://identity.cscs.ch/realms/cscs/protocol/openid-connect/token"
      cscs_dwdi_oidc_scope: "cscs-dwdi:read"

    backend_components:
      nodeHours:
        measured_unit: "node-hours"
        unit_factor: 1
        accounting_type: "usage"
        label: "Node Hours"
      storage:
        measured_unit: "TB"
        unit_factor: 1
        accounting_type: "usage"
        label: "Storage Usage"
```

### Configuration Parameters

#### Backend Settings

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cscs_dwdi_api_url` | Yes | Base URL for the CSCS-DWDI API service |
| `cscs_dwdi_client_id` | Yes | OIDC client ID for authentication |
| `cscs_dwdi_client_secret` | Yes | OIDC client secret for authentication |
| `cscs_dwdi_oidc_token_url` | Yes | OIDC token endpoint URL (required for authentication) |
| `cscs_dwdi_oidc_scope` | No | OIDC scope to request (defaults to "openid") |

#### Backend Components

Components must match the field names returned by the CSCS-DWDI API. For example:

- `nodeHours` - Maps to the `nodeHours` field in API responses
- `storage` - Maps to the `storage` field in API responses (if available)
- `gpuHours` - Maps to the `gpuHours` field in API responses (if available)

Each component supports:

| Parameter | Description |
|-----------|-------------|
| `measured_unit` | Unit for display in Waldur (e.g., "node-hours", "TB") |
| `unit_factor` | Conversion factor from API units to measured units |
| `accounting_type` | Either "usage" for actual usage or "limit" for quotas |
| `label` | Display label in Waldur interface |

## Usage Data Format

The plugin reports usage for all configured components:

- **Component Types**: Configurable (e.g., `nodeHours`, `storage`, `gpuHours`)
- **Units**: Based on API response and `unit_factor` configuration
- **Granularity**: Monthly reporting with current month data
- **User Attribution**: Individual user usage within each account
- **Aggregation**: Automatically aggregates across clusters and time periods

## API Integration

The plugin uses the CSCS-DWDI API endpoints:

- `GET /api/v1/compute/usage-month-multiaccount` - Primary endpoint for monthly usage data
- Authentication via OIDC Bearer tokens

### Authentication

The plugin uses OAuth2/OIDC authentication with the following requirements:

- Requires `cscs_dwdi_oidc_token_url` in backend settings
- Uses OAuth2 `client_credentials` grant flow
- Automatically handles token caching and renewal
- Includes 5-minute safety margin for token expiry
- Fails with proper error logging if OIDC configuration is missing

### Data Processing

1. **Account Filtering**: Only reports on accounts that match Waldur resource backend IDs
2. **User Aggregation**: Combines usage for the same user across different dates and clusters
3. **Time Range**: Automatically queries from the first day of the current month to today
4. **Precision**: Rounds node-hours to 2 decimal places

## Installation

This plugin is part of the Waldur Site Agent workspace. To install:

```bash
# Install all workspace packages including cscs-dwdi plugin
uv sync --all-packages

# Install specific plugin for development
uv sync --extra cscs-dwdi
```

## Testing

Run the plugin tests:

```bash
# Run CSCS-DWDI plugin tests
uv run pytest plugins/cscs-dwdi/tests/

# Run with coverage
uv run pytest plugins/cscs-dwdi/tests/ --cov=waldur_site_agent_cscs_dwdi
```

## Limitations

This is a **reporting-only backend** that does not support:

- Account creation or deletion
- User management
- Resource limit management
- Order processing
- Membership synchronization

For these operations, use a different backend (e.g., SLURM) in combination with the CSCS-DWDI reporting backend:

```yaml
offerings:
  - name: "Mixed Backend Offering"
    order_processing_backend: "slurm"       # Use SLURM for orders
    reporting_backend: "cscs-dwdi"          # Use CSCS-DWDI for reporting
    membership_sync_backend: "slurm"        # Use SLURM for membership
```

## Error Handling

The plugin includes comprehensive error handling:

- **API Connectivity**: Ping checks verify API availability
- **Authentication**: Token refresh and error handling
- **Data Validation**: Validates API responses and filters invalid data
- **Retry Logic**: Uses the framework's built-in retry mechanisms

## Development

### Project Structure

```text
plugins/cscs-dwdi/
├── pyproject.toml                           # Plugin configuration
├── README.md                               # This documentation
├── waldur_site_agent_cscs_dwdi/
│   ├── __init__.py                         # Package init
│   ├── backend.py                          # Main backend implementation
│   └── client.py                          # CSCS-DWDI API client
└── tests/
    └── test_cscs_dwdi.py                  # Plugin tests
```

### Key Classes

- **`CSCSDWDIBackend`**: Main backend class implementing reporting functionality
- **`CSCSDWDIClient`**: HTTP client for CSCS-DWDI API communication

### Extension Points

To extend the plugin:

1. **Additional Endpoints**: Modify `CSCSDWDIClient` to support more API endpoints
2. **Authentication Methods**: Update authentication logic in `client.py`
3. **Data Processing**: Enhance `_process_api_response()` for additional data formats

## Troubleshooting

### Common Issues

#### Authentication Failures

- Verify OIDC client credentials
- Check API URL configuration
- Ensure proper token scopes

#### Missing Usage Data

- Verify account names match between Waldur and CSCS-DWDI
- Check date ranges and API response format
- Review API rate limits and quotas

#### Network Connectivity

- Test API connectivity with ping functionality
- Verify network access from agent deployment environment
- Check firewall and proxy settings

### Debugging

Enable debug logging for detailed API interactions:

```python
import logging
logging.getLogger('waldur_site_agent_cscs_dwdi').setLevel(logging.DEBUG)
```

## Support

For issues and questions:

- Check the [Waldur Site Agent documentation](../../docs/)
- Review plugin test cases for usage examples
- Create issues in the project repository
