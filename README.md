# Waldur Site Agent

A stateless Python application that synchronizes data between Waldur Mastermind and service provider
backends. Manages account creation, usage reporting, and membership synchronization across different cluster
management systems.

## Architecture

The agent uses a **uv workspace architecture** with pluggable backends:

- **Core Package**: `waldur-site-agent` (base classes, common utilities)
- **Plugin Packages**: Standalone backend implementations under `plugins/` (see table below)

### Agent Modes

- `order_process`: Fetches orders from Waldur and manages backend resources
- `report`: Reports usage data from backend to Waldur
- `membership_sync`: Synchronizes user memberships
- `event_process`: Event-based processing using STOMP

## Usage

```bash
waldur_site_agent -m <mode> -c <config-file>
```

## Logging

The agent emits structured logs in JSON format to stdout. This applies to both the core
agent and CLI tools.

Example log entry:

```json
{"event": "Running agent in order_process mode", "level": "info", "logger": "waldur_site_agent.backend", "timestamp": "2026-02-03T14:02:35.551020+00:00"}
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
uvx prek run --all-files

# Load components into Waldur
waldur_site_load_components -c <config-file>
```

## Releasing

```bash
./scripts/release.sh 0.10.0
# Review the commit, then push:
git push origin main --tags
```

See the [Releasing Guide](docs/releasing.md) for details on
version bumping, changelog generation, and what CI does after
you push.

## Documentation

- [Architecture & Plugin Development](docs/architecture.md)
- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)
- [Deployment Guide](docs/deployment.md)
- [Username Management](docs/offering-users.md)
- [SLURM Usage Reporting Setup](docs/slurm-usage-reporting-setup.md)
- [Releasing Guide](docs/releasing.md)

## Plugins

<!-- BEGIN PLUGIN TABLE -->
| Plugin | Description |
| ------ | ----------- |
| [basic_username_management](plugins/basic_username_management/README.md) | Basic username management plugin |
| [croit-s3](plugins/croit-s3/README.md) | Croit S3 storage plugin |
| [cscs-dwdi](plugins/cscs-dwdi/README.md) | CSCS-DWDI reporting plugin |
| [digitalocean](plugins/digitalocean/README.md) | DigitalOcean plugin |
| [harbor](plugins/harbor/README.md) | Harbor container registry plugin |
| [k8s-ut-namespace](plugins/k8s-ut-namespace/README.md) | Kubernetes UT ManagedNamespace plugin |
| keycloak-client | Shared Keycloak client for Waldur Site Agent plugins |
| [ldap](plugins/ldap/README.md) | LDAP plugin |
| [moab](plugins/moab/README.md) | MOAB plugin |
| [mup](plugins/mup/README.md) | MUP plugin |
| [okd](plugins/okd/README.md) | OKD/OpenShift plugin |
| [opennebula](plugins/opennebula/README.md) | OpenNebula VDC plugin |
| [rancher](plugins/rancher/README.md) | Rancher plugin |
| [rancher-kc-crd](plugins/rancher-kc-crd/README.md) | Rancher + Keycloak CRD-driven plugin |
| [slurm](plugins/slurm/README.md) | SLURM plugin |
| [waldur](plugins/waldur/README.md) | Waldur-to-Waldur federation plugin |
<!-- END PLUGIN TABLE -->

## License

MIT License - see [LICENCE](./LICENCE.md) file for details.
