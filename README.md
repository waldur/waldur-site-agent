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

## Installation

The core agent and every plugin are published to PyPI. Install the core package
plus the plugins you need — all packages share the same version number, so keep
them in sync:

```bash
pip install waldur-site-agent waldur-site-agent-slurm
```

See the [plugin table](#plugins) below for the full list of published packages.

For Kubernetes, released Helm charts are published to a chart repository hosted
on GitHub Pages:

```bash
helm repo add waldur https://waldur.github.io/waldur-site-agent
helm repo update
helm search repo waldur/waldur-site-agent --versions
helm install waldur-site-agent waldur/waldur-site-agent
```

Release candidates are pre-release versions — add `--devel` to see and install
them. Configurable values are documented in the
[chart README](https://github.com/waldur/waldur-site-agent/blob/main/helm/waldur-site-agent/README.md).

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

For operators deploying the agent:

- [Quickstart](docs/quickstart.md) - the fastest path from a fresh install to a running, verified agent
- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)
- [Configuration Validation](docs/configuration-validation.md) - how config errors are reported, and how to read them
- [Deployment Guide](docs/deployment.md)
- [Upgrading Guide](docs/upgrading.md)
- [Username Management](docs/offering-users.md)
- [SLURM Usage Reporting Setup](docs/slurm-usage-reporting-setup.md)

For contributors:

- [Architecture & Plugin Development](docs/architecture.md)
- [Releasing Guide](docs/releasing.md)

## Plugins

<!-- BEGIN PLUGIN TABLE -->
<!-- pyml disable-num-lines 21 line-length -->
| Plugin | PyPI package | Description |
| ------ | ------------ | ----------- |
| [basic_username_management](plugins/basic_username_management/README.md) | [`waldur-site-agent-basic-username-management`](https://pypi.org/project/waldur-site-agent-basic-username-management/) | Basic username management plugin |
| [ceph-s3](plugins/ceph-s3/README.md) | [`waldur-site-agent-ceph-s3`](https://pypi.org/project/waldur-site-agent-ceph-s3/) | Ceph S3 storage plugin (croit and RadosGW flavours) |
| [cscs-dwdi](plugins/cscs-dwdi/README.md) | [`waldur-site-agent-cscs-dwdi`](https://pypi.org/project/waldur-site-agent-cscs-dwdi/) | CSCS-DWDI reporting plugin |
| [digitalocean](plugins/digitalocean/README.md) | [`waldur-site-agent-digitalocean`](https://pypi.org/project/waldur-site-agent-digitalocean/) | DigitalOcean plugin |
| [envoy-ai-gateway](plugins/envoy-ai-gateway/README.md) | [`waldur-site-agent-envoy-ai-gateway`](https://pypi.org/project/waldur-site-agent-envoy-ai-gateway/) | Envoy AI Gateway (API keys + usage reporting) plugin |
| [harbor](plugins/harbor/README.md) | [`waldur-site-agent-harbor`](https://pypi.org/project/waldur-site-agent-harbor/) | Harbor container registry plugin |
| [k8s-ut-namespace](plugins/k8s-ut-namespace/README.md) | [`waldur-site-agent-k8s-ut-namespace`](https://pypi.org/project/waldur-site-agent-k8s-ut-namespace/) | Kubernetes UT ManagedNamespace plugin |
| [keycloak-client](plugins/keycloak-client/) | [`waldur-site-agent-keycloak-client`](https://pypi.org/project/waldur-site-agent-keycloak-client/) | Shared Keycloak client for Waldur Site Agent plugins |
| [ldap](plugins/ldap/README.md) | [`waldur-site-agent-ldap`](https://pypi.org/project/waldur-site-agent-ldap/) | LDAP plugin |
| [litellm](plugins/litellm/README.md) | [`waldur-site-agent-litellm`](https://pypi.org/project/waldur-site-agent-litellm/) | LiteLLM (virtual key lifecycle + usage reporting) plugin |
| [moab](plugins/moab/README.md) | [`waldur-site-agent-moab`](https://pypi.org/project/waldur-site-agent-moab/) | MOAB plugin |
| [mup](plugins/mup/README.md) | [`waldur-site-agent-mup`](https://pypi.org/project/waldur-site-agent-mup/) | MUP plugin |
| [nextcloud](plugins/nextcloud/README.md) | [`waldur-site-agent-nextcloud`](https://pypi.org/project/waldur-site-agent-nextcloud/) | Nextcloud plugin |
| [okd](plugins/okd/README.md) | [`waldur-site-agent-okd`](https://pypi.org/project/waldur-site-agent-okd/) | OKD/OpenShift plugin |
| [opennebula](plugins/opennebula/README.md) | [`waldur-site-agent-opennebula`](https://pypi.org/project/waldur-site-agent-opennebula/) | OpenNebula VDC plugin |
| [rancher](plugins/rancher/README.md) | [`waldur-site-agent-rancher`](https://pypi.org/project/waldur-site-agent-rancher/) | Rancher plugin |
| [rancher-kc-crd](plugins/rancher-kc-crd/README.md) | [`waldur-site-agent-rancher-kc-crd`](https://pypi.org/project/waldur-site-agent-rancher-kc-crd/) | Rancher + Keycloak CRD-driven plugin |
| [slurm](plugins/slurm/README.md) | [`waldur-site-agent-slurm`](https://pypi.org/project/waldur-site-agent-slurm/) | SLURM plugin |
| [waldur](plugins/waldur/README.md) | [`waldur-site-agent-waldur`](https://pypi.org/project/waldur-site-agent-waldur/) | Waldur-to-Waldur federation plugin |
<!-- END PLUGIN TABLE -->

## License

MIT License - see [LICENCE](./LICENCE.md) file for details.
