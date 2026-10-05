# Deployment Guide

This guide covers production deployment of Waldur Site Agent using systemd services.

> **Deploying on Kubernetes?** Use the published Helm chart instead:
>
> ```bash
> helm repo add waldur https://waldur.github.io/waldur-site-agent
> helm repo update
> helm install waldur-site-agent waldur/waldur-site-agent
> ```
>
> The chart runs the same agent modes described below as separate deployments.
> See the
> [chart README](https://github.com/waldur/waldur-site-agent/blob/main/helm/waldur-site-agent/README.md)
> for available versions and configurable values.

## Deployment Overview

The agent can run in 4 different modes, deployed as separate systemd services:

1. **agent-order-process**: Processes orders from Waldur
2. **agent-report**: Reports usage data to Waldur
3. **agent-membership-sync**: Synchronizes memberships
4. **agent-event-process**: Event-based processing (alternative to #1 and #3)

## Service Combinations

**Option 1: Polling-based** (traditional)

- agent-order-process
- agent-membership-sync
- agent-report

**Option 2: Event-based** (requires STOMP)

- agent-event-process
- agent-report

**Note**: Only one combination can be active at a time.

## Systemd Service Setup

### Download Service Files

```bash
# Order processing service
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-order-process/agent.service \
  -o /etc/systemd/system/waldur-agent-order-process.service

# Reporting service
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-report/agent.service \
  -o /etc/systemd/system/waldur-agent-report.service

# Membership sync service
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-membership-sync/agent.service \
  -o /etc/systemd/system/waldur-agent-membership-sync.service

# Event processing service
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-event-process/agent.service \
  -o /etc/systemd/system/waldur-agent-event-process.service
```

The units run `waldur_site_agent` as root and find it on systemd's search path, which includes
`/usr/local/bin` where the [Installation Guide](installation.md) puts it (systemd 239 or newer is
needed for a command name without a path; Ubuntu 24.04 ships 255, Rocky Linux 9 ships 252). If
the agent lives elsewhere, put the absolute path in `ExecStart=` with a drop-in
(`systemctl edit <unit>`). The units restart the agent 30 seconds after it exits with an error.

### Logging to files instead of the journal

Each mode also has an `agent-file-logging.service` variant that appends output to
`/var/log/waldur-site-agent-<mode>.log` and `-error.log` instead of the journal. It uses
`StandardOutput=append:`, which needs **systemd 240 or newer**. Rotate the files with logrotate
(`copytruncate`); `journalctl` then shows only systemd's own start and stop messages for these
units, not the agent's output.

```bash
base=https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf
sudo curl -L "$base/agent-order-process/agent-file-logging.service" \
  -o /etc/systemd/system/waldur-agent-order-process.service

# Repeat for the other modes you run
```

### Enable and Start Services

#### Option 1: Polling-based Deployment

```bash
systemctl daemon-reload

# Start and enable services
systemctl start waldur-agent-order-process.service
systemctl enable waldur-agent-order-process.service

systemctl start waldur-agent-report.service
systemctl enable waldur-agent-report.service

systemctl start waldur-agent-membership-sync.service
systemctl enable waldur-agent-membership-sync.service
```

#### Option 2: Event-based Deployment

```bash
systemctl daemon-reload

# Start and enable services
systemctl start waldur-agent-event-process.service
systemctl enable waldur-agent-event-process.service

systemctl start waldur-agent-report.service
systemctl enable waldur-agent-report.service
```

### Heartbeat file per service

Each agent process writes a liveness heartbeat file, checked by
`waldur_site_healthz --liveness-only`. Several services on one host must not share it, or any of
them touching it keeps a stalled one looking alive, so each unit sets its own path with
`WALDUR_SITE_AGENT_HEARTBEAT_PATH` in its own runtime directory:
`/run/waldur-site-agent-<mode>/heartbeat`, for example
`/run/waldur-site-agent-event-process/heartbeat`. Without the variable the agent uses
`/tmp/waldur-site-agent-heartbeat`.

Check a unit's liveness with:

```bash
sudo /usr/local/bin/waldur_site_healthz --liveness-only \
  --heartbeat-path /run/waldur-site-agent-event-process/heartbeat
```

It exits 0 while the heartbeat is younger than 300 seconds (`--max-age`). In event mode the
agent stops refreshing it while a STOMP consumer stays disconnected for longer than
`WALDUR_SITE_AGENT_STOMP_UNHEALTHY_AFTER_MINUTES` (default 15), so the check fails about that
long plus 300 seconds into a broker outage.

## Service Management

### Check Service Status

```bash
# Check individual service
systemctl status waldur-agent-order-process.service

# Check all waldur services
systemctl status 'waldur-agent-*'
```

### View Logs

```bash
# Follow logs for a service
journalctl -u waldur-agent-order-process.service -f

# View recent logs
journalctl -u waldur-agent-order-process.service --since "1 hour ago"

# View logs for all agents
journalctl -u 'waldur-agent-*' -f
```

### Restart Services

```bash
# Restart individual service
systemctl restart waldur-agent-order-process.service

# Restart all agent services
systemctl restart 'waldur-agent-*'
```

## Configuration Management

### Configuration File Location

The default configuration file location is `/etc/waldur/waldur-site-agent-config.yaml`.

### Update Configuration

1. Edit configuration file:

   ```bash
   sudo nano /etc/waldur/waldur-site-agent-config.yaml
   ```

2. Validate configuration:

   ```bash
   sudo /usr/local/bin/waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
   ```

3. Restart services:

   ```bash
   systemctl restart 'waldur-agent-*'
   ```

## Event-Based Processing Setup

### STOMP Configuration

For STOMP-based event processing, enable it per offering and keep the backends the event
handlers need:

```yaml
offerings:
  - name: "Your Offering"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your-token"          # required: OIDC-only offerings cannot use STOMP
    waldur_offering_uuid: "your-offering-uuid"
    backend_type: "slurm"
    order_processing_backend: "slurm"
    membership_sync_backend: "slurm"         # without it, membership events are not subscribed
    reporting_backend: "slurm"
    stomp_enabled: true
    websocket_use_tls: true
    # Optional overrides; by default the agent connects to the host of waldur_api_url,
    # path /rmqws-stomp, port 443 (80 when websocket_use_tls is false)
    # stomp_ws_host: "waldur.example.com"
    # stomp_ws_port: 443
    # stomp_ws_path: "/rmqws-stomp"
```

- The STOMP login uses the offering's static `waldur_api_token`. A configuration that combines
  `stomp_enabled: true` with OIDC-only authentication fails validation, and the agent exits.
- `global_proxy` does not apply to the STOMP WebSocket yet. If the broker is only reachable
  through a proxy, set `https_proxy` (or `http_proxy`) in the unit's environment.
- `waldur_site_agent -m event_process` still needs `agent-report` for usage reporting.

## Monitoring and Alerting

### Health Checks

A unit can be active while its agent is stuck, so check both the unit and its heartbeat:

```bash
#!/bin/bash
# /usr/local/bin/check-waldur-agent.sh
# List the modes you run: polling = order-process membership-sync report,
# event-based = event-process report.
MODES=(order-process membership-sync report)

for mode in "${MODES[@]}"; do
    unit="waldur-agent-$mode.service"
    if ! systemctl is-active --quiet "$unit"; then
        echo "CRITICAL: $unit is not running"
        exit 2
    fi
    if ! /usr/local/bin/waldur_site_healthz --liveness-only \
            --heartbeat-path "/run/waldur-site-agent-$mode/heartbeat" >/dev/null 2>&1; then
        echo "CRITICAL: $unit has not written a heartbeat for 5 minutes"
        exit 2
    fi
done

echo "OK: all Waldur agent services are running and alive"
exit 0
```

### Log Rotation

Systemd handles log rotation automatically via journald. Configure retention:

```bash
# Edit journald configuration
sudo nano /etc/systemd/journald.conf

# Add or modify:
SystemMaxUse=1G
MaxRetentionSec=1month
```

### Sentry Integration

Add Sentry DSN to configuration for error tracking:

```yaml
sentry_dsn: "https://your-dsn@sentry.io/project"
```

Set environment in systemd service files:

```ini
[Service]
Environment=SENTRY_ENVIRONMENT=production
```

## Security Considerations

### File Permissions

```bash
# Secure configuration file
sudo chmod 600 /etc/waldur/waldur-site-agent-config.yaml
sudo chown root:root /etc/waldur/waldur-site-agent-config.yaml
```

### API Token Security

- For the source offering: the token user needs **OFFERING.MANAGER** role on the offering
- For Waldur federation (`waldur` backend): the target token user needs **customer owner**
  (can be a non-SP customer) and **ISD identity manager** (`managed_isds` set)
- Use dedicated service accounts in Waldur
- Rotate API tokens regularly
- Store tokens securely (consider using systemd credentials)

### Network Security

- Restrict outbound connections to Waldur API endpoints
- Use TLS for all connections
- Configure firewall rules appropriately

## Troubleshooting

Before digging into a specific symptom below, run:

```bash
sudo /usr/local/bin/waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
```

This checks the Waldur side of the setup — API reachability, token auth, offering state, loaded
components — and the backend: it calls each offering's backend diagnostics (for SLURM, the SLURM
tools and accounting database), and exits non-zero if either side fails. For SLURM, follow up
with `waldur_site_diagnose_slurm_account <allocation-account> -c /etc/waldur/waldur-site-agent-config.yaml`
(the resource's backend ID in Waldur) for a deeper check that compares actual SLURM account state against what Waldur expects.

### Common Issues

#### Service Won't Start

1. Check configuration syntax:

   ```bash
   sudo /usr/local/bin/waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
   ```

2. Check service logs:

   ```bash
   journalctl -u waldur-agent-order-process.service -n 50
   ```

#### Backend Connection Issues

1. Test backend connectivity:

   ```bash
   # For SLURM
   sacct --help
   sacctmgr --help

   # For MOAB (as root)
   mam-list-accounts
   ```

2. Check permissions and PATH

#### Agent Identity Registration Is Refused

Symptom — every cycle, for the same offering:

```text
Registering a new identity for offering my-offering with name agent-<uuid>
Unable to register the identity agent-<uuid> for the offering my-offering:
Unexpected status code: 400 ... {"offering":["Object with uuid=<uuid> does not exist."]}
Continuing without agent telemetry.
```

The offering does exist. Waldur registers an agent identity only for the offering types listed
under [`waldur_offering_uuid`](configuration.md#waldur_offering_uuid), and reports any other type
as a missing object rather than as an unsupported one.

The agent keeps processing the offering: the identity, its service and its processors are
telemetry, and the agent's actual work — orders, membership sync, usage reporting — goes through
the marketplace API and does not touch them. What you lose until the offering type is accepted:

- the agent does not appear in Waldur's agent monitoring view, so there is no version, uptime,
  dependency or processor information for it;
- log shipping never starts. A shipper is keyed by the agent identity's UUID, so without an
  identity there is nothing to attach a batch to — and the endpoint that receives the batches
  applies the same offering-type restriction, so it would refuse them anyway. Agent logs stay in
  the service's own output (`journalctl -u waldur-agent-*.service`).

What to do:

1. Confirm the offering's type, using the agent's own token:

   ```bash
   curl -s -H "Authorization: Token your-token" \
     https://waldur.example.com/api/marketplace-provider-offerings/<offering-uuid>/ \
     | jq '{name, type, state}'
   ```

2. If it comes back `404`, the UUID in `waldur_offering_uuid` is wrong or belongs to another
   Waldur instance — the offering name in the log line comes from your configuration file, not
   from the API, so a stale UUID looks identical to this symptom.
3. If the type is not one of the supported ones, either move the agent to an offering of a
   supported type, or ask your Waldur operator to widen the accepted types on the server.

#### Waldur API Issues

1. Test API connectivity:

   ```bash
   curl -H "Authorization: Token your-token" https://waldur.example.com/api/
   ```

2. Verify SSL certificates if using HTTPS

### Debug Mode

Enable debug logging by setting `log_level` in the agent configuration file:

```yaml
log_level: DEBUG
```

## Performance Tuning

### Environment variables

Set these in a unit drop-in (`systemctl edit <unit>`):

All names start with `WALDUR_SITE_AGENT_`:

| Variable | Default | Effect |
|---|---|---|
| `…ORDER_PROCESS_PERIOD_MINUTES` | `5` | `order_process` polling interval (fractions allowed) |
| `…MEMBERSHIP_SYNC_PERIOD_MINUTES` | `5` | `membership_sync` polling interval (whole minutes) |
| `…REPORT_PERIOD_MINUTES` | `30` | `report` interval (whole minutes) |
| `…RECONCILIATION_PERIOD_MINUTES` | `60` | Event mode: periodic reconciliation (whole minutes) |
| `…STOMP_UNHEALTHY_AFTER_MINUTES` | `15` | Event mode: STOMP downtime before liveness fails |
| `…HEARTBEAT_PATH` | `/tmp/waldur-site-agent-heartbeat` | Liveness heartbeat file (set per unit) |

In event mode the reconciliation covers orders, resource API keys, offering users and the
project hierarchy.

A fractional value for a "whole minutes" variable stops the agent at startup.

```ini
[Service]
# Poll for orders every 10 minutes instead of 5
Environment=WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES=10
```

### Resource Limits

Add resource limits to service files:

```ini
[Service]
MemoryMax=512M
CPUQuota=50%
```

## Backup and Recovery

### Configuration Backup

```bash
# Backup configuration
sudo cp /etc/waldur/waldur-site-agent-config.yaml /etc/waldur/waldur-site-agent-config.yaml.backup

# Version control (optional)
sudo git init /etc/waldur
sudo git add waldur-site-agent-config.yaml
sudo git commit -m "Initial configuration"
```

### Service State

The agent is stateless, but consider backing up:

- Configuration files
- Custom systemd service modifications
- Log files (if needed for auditing)

## Scaling Considerations

### Multiple Backend Support

The agent supports multiple offerings in a single configuration file. Each offering can use different backends:

```yaml
offerings:
  - name: "SLURM Cluster A"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "token-a"
    waldur_offering_uuid: "uuid-a"
    backend_type: "slurm"
    order_processing_backend: "slurm"
    membership_sync_backend: "slurm"
    reporting_backend: "slurm"
    # ... SLURM backend_settings and backend_components ...

  - name: "MOAB Cluster B"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "token-b"
    waldur_offering_uuid: "uuid-b"
    backend_type: "moab"
    order_processing_backend: "moab"
    membership_sync_backend: "moab"
    reporting_backend: "moab"
    # ... MOAB backend_settings and backend_components ...
```

Each backend's plugin must be installed in the agent's environment
(see [Installation](installation.md#what-gets-installed)).

### High Availability

For HA deployment:

- Run one set of services per offering configuration; two agents processing the same offering
  race each other on orders
- Restart is automatic (`Restart=on-failure`); use the per-unit liveness check above for
  monitoring
- Implement cluster-level monitoring
- Consider using configuration management tools (Ansible, Puppet, etc.)
