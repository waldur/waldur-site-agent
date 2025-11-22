# Deployment Guide

This guide covers production deployment of Waldur Site Agent using systemd services.

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

**Option 2: Event-based** (requires MQTT/STOMP)

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

### Legacy Systemd Support

For systemd versions older than 240:

```bash
# Use legacy service files instead
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-order-process/agent-legacy.service \
  -o /etc/systemd/system/waldur-agent-order-process.service

# Repeat for other services with -legacy.service files
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

## Service Management

### Check Service Status

```bash
# Check individual service
systemctl status waldur-agent-order-process.service

# Check all waldur services
systemctl status waldur-agent-*
```

### View Logs

```bash
# Follow logs for a service
journalctl -u waldur-agent-order-process.service -f

# View recent logs
journalctl -u waldur-agent-order-process.service --since "1 hour ago"

# View logs for all agents
journalctl -u waldur-agent-* -f
```

### Restart Services

```bash
# Restart individual service
systemctl restart waldur-agent-order-process.service

# Restart all agent services
systemctl restart waldur-agent-*
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
   waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
   ```

3. Restart services:

   ```bash
   systemctl restart waldur-agent-*
   ```

## Event-Based Processing Setup

### MQTT Configuration

For MQTT-based event processing, add to your offering configuration:

```yaml
offerings:
  - name: "Your Offering"
    # ... other settings ...
    mqtt_enabled: true
    websocket_use_tls: true
```

### STOMP Configuration

For STOMP-based event processing:

```yaml
offerings:
  - name: "Your Offering"
    # ... other settings ...
    stomp_enabled: true
    websocket_use_tls: true
```

**Important**: Configure the event bus settings in Waldur to match your agent configuration.

## Monitoring and Alerting

### Health Checks

Create a monitoring script:

```bash
#!/bin/bash
# /usr/local/bin/check-waldur-agent.sh

SERVICES=("waldur-agent-order-process" "waldur-agent-report" "waldur-agent-membership-sync")

for service in "${SERVICES[@]}"; do
    if ! systemctl is-active --quiet "$service"; then
        echo "CRITICAL: $service is not running"
        exit 2
    fi
done

echo "OK: All Waldur agent services are running"
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

- Use dedicated service account in Waldur
- Rotate API tokens regularly
- Store tokens securely (consider using systemd credentials)

### Network Security

- Restrict outbound connections to Waldur API endpoints
- Use TLS for all connections
- Configure firewall rules appropriately

## Troubleshooting

### Common Issues

#### Service Won't Start

1. Check configuration syntax:

   ```bash
   waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
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

#### Waldur API Issues

1. Test API connectivity:

   ```bash
   curl -H "Authorization: Token your-token" https://waldur.example.com/api/
   ```

2. Verify SSL certificates if using HTTPS

### Debug Mode

Enable debug logging by modifying service files:

```ini
[Service]
Environment=WALDUR_SITE_AGENT_LOG_LEVEL=DEBUG
```

## Performance Tuning

### Adjust Processing Periods

Modify environment variables in systemd service files:

```ini
[Service]
# Reduce order processing frequency for high-load systems
Environment=WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES=10

# Increase reporting frequency for better accuracy
Environment=WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES=15
```

### Resource Limits

Add resource limits to service files:

```ini
[Service]
MemoryLimit=512M
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
    order_processing_backend: "slurm"
    # ... SLURM-specific settings ...

  - name: "MOAB Cluster B"
    order_processing_backend: "moab"
    # ... MOAB-specific settings ...
```

### High Availability

For HA deployment:

- Run agents on multiple nodes
- Use external load balancer for MQTT/STOMP connections
- Implement cluster-level monitoring
- Consider using configuration management tools (Ansible, Puppet, etc.)
