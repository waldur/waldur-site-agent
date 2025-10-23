# Waldur Site Agent Helm Chart

This Helm chart deploys the Waldur Site Agent, a stateless application that synchronizes data between Waldur Mastermind and
service provider backends (SLURM, MOAB, MUP clusters).

## Installation

### Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

### Installing the Chart

To install the chart with the release name `my-waldur-site-agent`:

```bash
helm install my-waldur-site-agent ./helm/waldur-site-agent
```

### Uninstalling the Chart

To uninstall/delete the `my-waldur-site-agent` deployment:

```bash
helm delete my-waldur-site-agent
```

## Configuration

The following table lists the configurable parameters of the Waldur Site Agent chart and their default values.

### Global Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.registry` | Container registry (optional) | `""` |
| `image.repository` | Container image repository | `opennode/waldur-site-agent` |
| `image.tag` | Container image tag | `latest` |
| `image.pullPolicy` | Container image pull policy | `IfNotPresent` |
| `nameOverride` | Override the name of the chart | `""` |
| `fullnameOverride` | Override the full name of the chart | `""` |

### Agent Deployment Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `agents.orderProcess.enabled` | Deploy order processing agent | `false` |
| `agents.orderProcess.replicas` | Number of order-process replicas | `1` |
| `agents.report.enabled` | Deploy reporting agent | `true` |
| `agents.report.replicas` | Number of report replicas | `1` |
| `agents.membershipSync.enabled` | Deploy membership sync agent | `false` |
| `agents.membershipSync.replicas` | Number of membership-sync replicas | `1` |
| `agents.eventProcess.enabled` | Deploy event processing agent | `true` |
| `agents.eventProcess.replicas` | Number of event-process replicas | `1` |

### Secret Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `secret.create` | Create Secret for agent configuration | `true` |
| `secret.name` | Secret name (generated if empty) | `""` |
| `secret.data.config.yaml` | Complete agent configuration | See values.yaml |

### Resources & Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `512Mi` |
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.requests.memory` | Memory request | `128Mi` |
| `securityContext.runAsUser` | User ID to run container | `1000` |
| `securityContext.runAsNonRoot` | Run as non-root user | `true` |

### Deployment Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `healthCheck.enabled` | Enable health checks using diagnostics | `true` |
| `healthCheck.initialDelaySeconds` | Initial delay for health checks | `30` |
| `healthCheck.periodSeconds` | Health check interval | `60` |
| `healthCheck.timeoutSeconds` | Health check timeout | `10` |
| `healthCheck.failureThreshold` | Failed checks before restart | `3` |
| `healthCheck.successThreshold` | Successful checks to be considered healthy | `1` |

## Usage Examples

### Combination 1: Event-based Processing (Default)

```yaml
# values.yaml
agents:
  orderProcess:
    enabled: false

  report:
    enabled: true
    replicas: 1

  membershipSync:
    enabled: false

  eventProcess:
    enabled: true
    replicas: 1

secret:
  data:
    config.yaml: |
      offerings:
        - name: "My SLURM Cluster"
          waldur_api_url: "https://my-waldur.example.com/api/"
          waldur_api_token: "your-api-token-here"
          waldur_offering_uuid: "your-offering-uuid"
          mqtt_enabled: true
          backend_type: "slurm"
          backend_settings:
            default_account: "root"
            customer_prefix: "customer_"
            project_prefix: "project_"
            allocation_prefix: "alloc_"
          backend_components:
            cpu:
              limit: 1000
              measured_unit: "k-Hours"
              unit_factor: 60000
              accounting_type: "usage"
              label: "CPU"
```

### Combination 2: Polling-based Processing

```yaml
# values.yaml
agents:
  orderProcess:
    enabled: true
    replicas: 1

  report:
    enabled: true
    replicas: 1

  membershipSync:
    enabled: true
    replicas: 1

  eventProcess:
    enabled: false

secret:
  data:
    config.yaml: |
      offerings:
        - name: "My SLURM Cluster"
          waldur_api_url: "https://my-waldur.example.com/api/"
          waldur_api_token: "your-api-token-here"
          waldur_offering_uuid: "your-offering-uuid"
          mqtt_enabled: false
          backend_type: "slurm"
          # ... backend configuration
```

### Multiple Backend Configuration

```yaml
# values.yaml
secret:
  data:
    config.yaml: |
      offerings:
        - name: "SLURM Cluster"
          waldur_api_url: "https://waldur.example.com/api/"
          waldur_api_token: "token1"
          waldur_offering_uuid: "uuid1"
          backend_type: "slurm"
          # ... SLURM settings
        - name: "MOAB Cluster"
          waldur_api_url: "https://waldur.example.com/api/"
          waldur_api_token: "token2"
          waldur_offering_uuid: "uuid2"
          backend_type: "moab"
          # ... MOAB settings
```

## Agent Architecture

The Waldur Site Agent is designed to run as **4 separate deployments**, each handling a specific responsibility:

### Agent Modes

- **`order-process`**: Polls for orders from Waldur and manages backend resources
- **`report`**: Reports usage data from backend to Waldur on schedule
- **`membership-sync`**: Synchronizes user memberships between Waldur and backend
- **`event-process`**: Event-based processing using MQTT/STOMP (alternative to order-process + membership-sync)

### Valid Deployment Combinations

The chart supports two valid combinations as per the
[official documentation](https://github.com/waldur/waldur-site-agent):

1. **Event-based** (default): `event-process` + `report`
2. **Polling-based**: `order-process` + `membership-sync` + `report`

**Important**: Each mode runs as a separate long-running deployment with built-in scheduling.
The agents are not designed as batch jobs.

## Security Considerations

- All sensitive configuration (API tokens, URLs) should be stored in the Secret
- The agent runs as a non-root user (UID 1000)
- Read-only root filesystem is enforced
- No privileged escalation is allowed

## Troubleshooting

### Check Agent Logs

```bash
kubectl logs deployment/my-waldur-site-agent
```

### Validate Configuration

```bash
# Check if secret is created properly
kubectl get secret my-waldur-site-agent-secret -o yaml

# Check rendered configuration
kubectl exec deployment/my-waldur-site-agent -- cat /etc/waldur-site-agent/config.yaml
```

### Test Connectivity

```bash
# Run a one-time test
kubectl run waldur-agent-test --rm -i --tty --image=opennode/waldur-site-agent:latest -- waldur_site_agent --help
```

## Contributing

For issues and feature requests, please visit the [Waldur Site Agent repository](https://github.com/waldur/waldur-site-agent).
