# Waldur Site Agent - Thales Luna HSM Plugin

This plugin enables Waldur Site Agent to collect crypto operation metrics from Thales Luna Network-Attached
HSMs (Hardware Security Modules) for usage reporting.

## Features

- **Crypto Operation Reporting**: Collects total crypto operations (ENCRYPT, DECRYPT, SIGN, VERIFY,
  KEY_GENERATION, KEY_DERIVATION) from HSM partitions
- **Monthly Statistics**: Automatically tracks operations from the beginning of each month
- **Counter Reset Handling**: Properly handles HSM counter resets by accumulating previous totals
- **Session Management**: Handles Luna HSM's two-step authentication process with automatic session renewal
- **Multi-Partition Support**: Can monitor multiple HSM partitions simultaneously

## Architecture

The plugin consists of three main components:

- **LunaHsmBackend**: Main backend class inheriting from `BaseBackend`
- **LunaHsmClient**: API client handling Luna HSM authentication and metrics collection
- **MonthlyStatsStorage**: Local storage for persistent monthly operation counts

## Configuration

Add the Luna HSM backend to your Waldur Site Agent configuration:

```yaml
offerings:
  - name: "Luna HSM Service"
    backend_type: "luna_hsm"
    backend_settings:
      # HSM API connection
      api_base_url: "https://your-hsm-host:8443"
      hsm_id: "657242"
      verify_ssl: false  # Use true for production with valid certificates

      # Session authentication (step 1)
      admin_username: "admin"
      admin_password: "your_admin_password"

      # HSM authentication (step 2)
      hsm_role: "so"  # Security Officer
      hsm_password: "your_so_password"
      hsm_ped: "0"    # PED configuration

      # Optional settings
      session_timeout: 3600  # Session lifetime in seconds
      stats_storage_path: "/var/lib/waldur-site-agent/luna-hsm-stats.json"

    backend_components:
      operations:
        name: "Crypto Operations"
        unit: "operations"
        unit_factor: 1
```

## Authentication Flow

The plugin implements Luna HSM's two-step authentication:

1. **Session Authentication**: Basic auth to `/auth/session` endpoint
2. **HSM Login**: Role-based login to specific HSM partition
3. **Metrics Collection**: Authenticated requests to `/metrics` endpoint

All authentication is handled automatically with session management and automatic renewal.

## Usage Reporting

The plugin reports total crypto operations for each HSM partition from the beginning of the current month. This includes:

- All operation types: ENCRYPT, DECRYPT, SIGN, VERIFY, KEY_GENERATION, KEY_DERIVATION
- Automatic handling of counter resets (accumulates previous totals)
- Persistent storage to survive agent restarts

### Resource Configuration in Waldur

1. Create HSM resources in Waldur Mastermind
2. Set the `backend_id` field to match the HSM `partitionId`
3. The agent will automatically start collecting metrics for configured partitions

## File Structure

```text
plugins/luna-hsm/
├── pyproject.toml                    # Plugin configuration and dependencies
├── README.md                         # This documentation
├── waldur_site_agent_luna_hsm/
│   ├── __init__.py                   # Package initialization
│   ├── backend.py                    # LunaHsmBackend implementation
│   ├── client.py                     # Luna HSM API client
│   └── storage.py                    # Monthly statistics persistence
└── tests/
    ├── test_backend.py               # Backend tests
    ├── test_client.py                # Client tests
    └── test_storage.py               # Storage tests
```

## Installation

1. Install the plugin in the UV workspace:

   ```bash
   uv sync --extra luna-hsm
   ```

2. Configure the offering in your agent configuration file

3. Run the agent:

   ```bash
   uv run waldur_site_agent -m report -c your-config.yaml
   ```

## Agent Commands

### Running Reports

```bash
uv run waldur_site_agent -m report -c config.yaml
```

### Diagnostics

```bash
uv run waldur_site_diagnostics
```

### Testing Connection

```bash
python -c "
from waldur_site_agent_luna_hsm.backend import LunaHsmBackend
backend = LunaHsmBackend(your_settings, {'operations': {}})
print('Connection:', backend.ping())
"
```

## Troubleshooting

### Common Issues

#### Authentication Failures

- Verify admin credentials and HSM role credentials
- Check HSM ID matches your actual HSM
- Ensure network connectivity to HSM

#### SSL Certificate Issues

- Set `verify_ssl: false` for self-signed certificates
- Or install proper CA certificates for production

#### Missing Statistics

- Check file permissions for stats storage path
- Verify HSM partitions exist and are accessible
- Check agent logs for detailed error messages

### Logs

The plugin logs extensively. Check the agent logs for:
- Authentication success/failure
- Metrics collection status
- Statistics calculations
- Counter reset detections

## Security Considerations

- Store HSM credentials securely
- Use proper SSL certificates in production
- Restrict file system permissions on statistics storage
- Monitor for unauthorized access attempts
- Regularly rotate HSM passwords

## Limitations

- HSM partitions cannot be created/deleted via the agent (they must pre-exist)
- User management is not applicable for HSM partitions
- Resource limits are not configurable for HSM partitions
- Only supports Security Officer (SO) role authentication

## Development

For development and testing:

```bash
# Run tests
uv run pytest plugins/luna-hsm/tests/

# Type checking
uv run mypy plugins/luna-hsm/

# Linting
uv run ruff plugins/luna-hsm/
```
