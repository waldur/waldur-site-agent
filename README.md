# Waldur Site Agent

A stateless Python application that synchronizes data between Waldur Mastermind and service provider
backends. Manages account creation, usage reporting, and membership synchronization across different cluster
management systems.

## Architecture

The agent uses a **uv workspace architecture** with pluggable backends:

- **Core Package**: `waldur-site-agent` (base classes, common utilities)
- **Plugin Packages**: Standalone backend implementations
  - `waldur-site-agent-slurm`: SLURM clusters
  - `waldur-site-agent-moab`: MOAB clusters
  - `waldur-site-agent-mup`: MUP portal
  - `waldur-site-agent-okd`: OpenShift/OKD platforms
  - `waldur-site-agent-harbor`: Harbor registries
  - `waldur-site-agent-croit-s3`: Croit S3 storage
  - `waldur-site-agent-cscs-dwdi`: CSCS DWDI accounting
  - `waldur-site-agent-basic-username-management`: Username management

### Agent Modes

- `order_process`: Fetches orders from Waldur and manages backend resources
- `report`: Reports usage data from backend to Waldur
- `membership_sync`: Synchronizes user memberships
- `event_process`: Event-based processing using MQTT/STOMP

## Usage

```bash
waldur_site_agent -m <mode> -c <config-file>
```

### CLI Arguments

- `-m`, `--mode`: Agent mode (`order_process`, `report`, `membership_sync`, `event_process`)
- `-c`, `--config-file`: Path to configuration file

### Environment Variables

- `WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES`: Order processing period (default: 5)
- `WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES`: Reporting period (default: 30)
- `WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES`: Membership sync period (default: 5)
- `SENTRY_ENVIRONMENT`: Sentry environment name

## Development

```bash
# Install dependencies
uv sync --all-packages

# Run tests
uv run pytest

# Format and lint code
pre-commit run --all-files

# Load components into Waldur
waldur_site_load_components -c <config-file>
```

## Documentation

- [Architecture & Plugin Development](docs/architecture.md)
- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)
- [Deployment Guide](docs/deployment.md)
- [Username Management](docs/offering-users.md)
- [SLURM Usage Reporting Setup](docs/slurm-usage-reporting-setup.md)

## Plugin Documentation

### Compute & HPC Plugins

- [SLURM Plugin](plugins/slurm/README.md) - SLURM cluster management
- [MOAB Plugin](plugins/moab/README.md) - MOAB cluster management
- [MUP Plugin](plugins/mup/README.md) - MUP portal integration

### Container & Cloud Plugins

- [OpenShift/OKD Plugin](plugins/okd/README.md) - OpenShift and OKD container platform management
- [Harbor Plugin](plugins/harbor/README.md) - Harbor container registry management

### Storage Plugins

- [Croit S3 Plugin](plugins/croit-s3/README.md) - Croit S3 storage management

### Accounting Plugins

- [CSCS DWDI Plugin](plugins/cscs-dwdi/README.md) - CSCS DWDI accounting integration

### Utility Plugins

- [Basic Username Management Plugin](plugins/basic_username_management/README.md) - Username generation and management

## License

MIT License - see [LICENCE](./LICENCE.md) file for details.
