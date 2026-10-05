# Waldur Site Agent Helm Chart

This Helm chart deploys the Waldur Site Agent, a stateless application that synchronizes data between Waldur Mastermind and
service provider backends. The image is built with the core agent and every plugin in the repository at that release,
so any of those backends can be configured without building a custom image.

## Installation

### Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+

### Adding the Chart Repository

Released charts are published to a Helm repository hosted on GitHub Pages:

```bash
helm repo add waldur https://waldur.github.io/waldur-site-agent
helm repo update
```

List the available versions:

```bash
helm search repo waldur/waldur-site-agent --versions
```

Release candidates (for example `1.0.6-rc.19`) are pre-release versions and are
hidden unless you pass `--devel`:

```bash
helm search repo waldur/waldur-site-agent --versions --devel
```

The chart is also indexed on Artifact Hub, which tracks the same repository:
<https://artifacthub.io/packages/helm/waldur-site-agent/waldur-site-agent>

### Installing the Chart

To install the chart with the release name `my-waldur-site-agent`:

```bash
helm install my-waldur-site-agent waldur/waldur-site-agent
```

Pin an explicit version (recommended for production). Add `--devel` when
installing a release candidate:

```bash
helm install my-waldur-site-agent waldur/waldur-site-agent --version <VERSION>
```

The chart version mirrors the agent release version, and the chart's default
`image.tag` is set to the matching agent image at release time.

To install from a checkout of this repository instead — useful when developing
the chart itself:

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
| `imagePullSecrets` | Image pull secrets for the agent pods | `[]` |
| `nameOverride` | Override the name of the chart | `""` |
| `fullnameOverride` | Override the full name of the chart | `""` |

Published charts set `image.tag` to the matching agent release; an empty tag falls back to the chart's `appVersion`.

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

### ServiceAccount Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create a ServiceAccount (named after the chart fullname if `name` is empty) | `false` |
| `serviceAccount.name` | ServiceAccount to run agent pods as (`default` if empty and `create` is false) | `""` |
| `serviceAccount.annotations` | Annotations for the created ServiceAccount | `{}` |

Backends that call the Kubernetes API (e.g. `envoy`) need a ServiceAccount with matching RBAC.
Set `serviceAccount.name` to a pre-created ServiceAccount, or set `serviceAccount.create: true`
and bind the required Role to it.

### Resources & Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `1024Mi` |
| `resources.requests.cpu` | CPU request | `200m` |
| `resources.requests.memory` | Memory request | `256Mi` |
| `podSecurityContext` | Pod security context | `{fsGroup: 1000}` |
| `securityContext` | Container security context | UID 1000, read-only root, no escalation, no capabilities |
| `podAnnotations` | Annotations for the agent pods | `{}` |
| `nodeSelector` | Node selector for the agent pods | `{}` |
| `tolerations` | Tolerations for the agent pods | `[]` |
| `affinity` | Affinity rules for the agent pods | `{}` |

### Extending the Pods

| Parameter | Description | Default |
|-----------|-------------|---------|
| `extraEnv` | Extra environment variables for every agent container (e.g. `https_proxy`) | `[]` |
| `extraVolumes` | Extra volumes for every agent pod (e.g. munge key, `slurm.conf`) | `[]` |
| `extraVolumeMounts` | Extra volume mounts for every agent container | `[]` |
| `hostAliases` | `/etc/hosts` entries for every agent pod (e.g. SLURM node hostnames) | `[]` |
| `strategy.type` | Deployment strategy | `Recreate` |

`Recreate` keeps two pods of the same mode from processing the same offering during a rollout.

### Deployment Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `healthCheck.enabled` | Enable liveness, readiness and startup probes | `true` |
| `healthCheck.initialDelaySeconds` | Initial delay for liveness and readiness | `30` |
| `healthCheck.periodSeconds` | Liveness and readiness interval | `60` |
| `healthCheck.timeoutSeconds` | Probe timeout, shared by all three probes | `10` |
| `healthCheck.failureThreshold` | Failed liveness checks before restart | `3` |
| `healthCheck.successThreshold` | Successful checks to be considered healthy | `1` |
| `healthCheck.startupProbe.enabled` | Give the agent a startup grace period | `true` |
| `healthCheck.startupProbe.periodSeconds` | Startup probe interval | `10` |
| `healthCheck.startupProbe.failureThreshold` | Startup attempts before giving up | `30` |

Liveness only reads the heartbeat file the agent's main loop writes; readiness
additionally calls `GET /api/users/me/` on Waldur. The startup probe covers the
cold start, during which the agent loads its config and contacts Waldur once
before the first heartbeat is written -- without it that time counts against
liveness and a slow Waldur restarts the pod before it ever runs. Raise
`startupProbe.failureThreshold` (attempts, `periodSeconds` apart) for a site
whose Waldur is slow to answer.

In event mode the main loop also withholds the heartbeat while a STOMP consumer
stays disconnected longer than `WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES`
(default 15, set it through `extraEnv`), so liveness restarts an agent that has
stopped receiving events. The probe fails once the heartbeat is older than its
maximum age (300 s), so expect the restart roughly threshold + 5 min +
`periodSeconds` × `failureThreshold` after the outage starts. Federation target
consumers and setups refused with a 4xx are logged, not counted.

The heartbeat file defaults to `/tmp/waldur-site-agent-heartbeat`; each pod has
its own `/tmp`, so the chart needs no change. Elsewhere set
`WALDUR_SITE_AGENT_HEARTBEAT_PATH` (and `waldur_site_healthz --heartbeat-path`)
so processes sharing a filesystem do not share a heartbeat.

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
          stomp_enabled: true
          backend_type: "slurm"
          order_processing_backend: "slurm"
          membership_sync_backend: "slurm"
          reporting_backend: "slurm"
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
          stomp_enabled: false
          backend_type: "slurm"
          order_processing_backend: "slurm"
          membership_sync_backend: "slurm"
          reporting_backend: "slurm"
          # ... backend_settings and backend_components
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
          order_processing_backend: "slurm"
          membership_sync_backend: "slurm"
          reporting_backend: "slurm"
          # ... SLURM settings
        - name: "MOAB Cluster"
          waldur_api_url: "https://waldur.example.com/api/"
          waldur_api_token: "token2"
          waldur_offering_uuid: "uuid2"
          backend_type: "moab"
          order_processing_backend: "moab"
          membership_sync_backend: "moab"
          reporting_backend: "moab"
          # ... MOAB settings
```

## Agent Architecture

The Waldur Site Agent is designed to run as **4 separate deployments**, each handling a specific responsibility:

### Agent Modes

- **`order-process`**: Polls for orders from Waldur and manages backend resources
- **`report`**: Reports usage data from backend to Waldur on schedule
- **`membership-sync`**: Synchronizes user memberships between Waldur and backend
- **`event-process`**: Event-based processing using STOMP (alternative to order-process + membership-sync)

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

Every offering needs its `*_backend` keys: without `order_processing_backend` the agent skips
order processing for it, and without `membership_sync_backend` or `reporting_backend` those modes
fail for it. Leave a key out only if you do not run that mode for the offering, or the backend has
nothing to do in it (Azure, for example, has no backend membership to sync).

### Check Agent Logs

Each enabled mode is its own Deployment, named `<fullname>-<mode>`. The fullname is the release
name when it already contains `waldur-site-agent` (as `my-waldur-site-agent` below), otherwise
`<release>-waldur-site-agent` (release `foo` gives `foo-waldur-site-agent-report`), unless
`fullnameOverride` is set:

```bash
kubectl get deployments -l app.kubernetes.io/instance=my-waldur-site-agent
kubectl logs deployment/my-waldur-site-agent-event-process
kubectl logs deployment/my-waldur-site-agent-report
```

### Validate Configuration

```bash
# Check if secret is created properly
kubectl get secret my-waldur-site-agent-secret -o yaml

# Check rendered configuration
kubectl exec deployment/my-waldur-site-agent-report -- cat /etc/waldur-site-agent/config.yaml

# Run the agent's own checks against that configuration
kubectl exec deployment/my-waldur-site-agent-report -- \
  waldur_site_diagnostics -c /etc/waldur-site-agent/config.yaml
```

### Test Connectivity

```bash
# Run a one-time test
kubectl run waldur-agent-test --rm -i --tty --image=opennode/waldur-site-agent:latest -- waldur_site_agent --help
```

## Contributing

For issues and feature requests, please visit the [Waldur Site Agent repository](https://github.com/waldur/waldur-site-agent).
