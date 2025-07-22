# Luna HSM Reporting Agent Setup Guide

This guide provides step-by-step instructions for setting up the Waldur Site Agent to collect crypto operation
metrics from Thales Luna Network-Attached HSMs.

## Prerequisites

### System Requirements

- Python 3.9+ with uv package manager
- Network access to Luna HSM appliance (typically port 8443)
- Access to Waldur Mastermind API
- Linux system with systemd (for service deployment)

### Luna HSM Requirements

- Luna Network-Attached HSM with firmware 7.x+
- Admin account access to HSM web interface
- Security Officer (SO) credentials for target partitions
- HSM must have API access enabled

## Step 1: Install the Agent

### 1.1 Clone and Install

```bash
# Clone the repository
git clone <waldur-site-agent-repo>
cd waldur-site-agent

# Install with Luna HSM plugin
uv sync --extra luna-hsm

# Verify installation
uv run waldur_site_agent --help
```

### 1.2 Create System Directories

```bash
# Create configuration directory
sudo mkdir -p /etc/waldur-site-agent

# Create data directory
sudo mkdir -p /var/lib/waldur-site-agent

# Create log directory
sudo mkdir -p /var/log/waldur-site-agent

# Set appropriate permissions
sudo chown -R $(whoami):$(whoami) /etc/waldur-site-agent
sudo chown -R $(whoami):$(whoami) /var/lib/waldur-site-agent
sudo chown -R $(whoami):$(whoami) /var/log/waldur-site-agent
```

## Step 2: Configure Luna HSM Access

### 2.1 Gather HSM Information

You need to collect the following information from your Luna HSM:

1. **HSM Host/IP**: The hostname or IP address of your HSM
2. **HSM ID**: Found in HSM web interface under System Information
3. **Admin Credentials**: Username/password for HSM admin access
4. **SO Credentials**: Security Officer username/password
5. **Partition IDs**: ID numbers of partitions you want to monitor

### 2.2 Test HSM API Access

```bash
# Test basic connectivity (replace with your HSM details)
curl -k -v https://your-hsm-host:8443/api/status

# Test authentication (this will show the auth flow)
curl -s -k -X POST https://your-hsm-host:8443/auth/session \
  -H "Content-Type: application/vnd.safenetinc.lunasa+json;version=" \
  -u 'admin:admin-password' \
  -d '{}' -c /tmp/cookies.txt

# Test HSM login
curl -s -k -X POST \
  -d '{"ped":"0","password":"so-password","role":"so"}' \
  -b /tmp/cookies.txt \
  -H "Content-Type: application/vnd.safenetinc.lunasa+json;version=" \
  https://your-hsm-host:8443/api/lunasa/hsms/YOUR_HSM_ID/login

# Test metrics collection
curl -s -k --cookie /tmp/cookies.txt \
  -H "Content-Type: application/vnd.safenetinc.lunasa+json;version=" \
  https://your-hsm-host:8443/api/lunasa/hsms/YOUR_HSM_ID/metrics | jq '.'
```

## Step 3: Create Configuration File

### 3.1 Copy Example Configuration

```bash
# Copy the example configuration
cp examples/luna-hsm-config.yaml /etc/waldur-site-agent/luna-hsm-config.yaml

# Secure the configuration file
chmod 600 /etc/waldur-site-agent/luna-hsm-config.yaml
```

### 3.2 Edit Configuration

Edit `/etc/waldur-site-agent/luna-hsm-config.yaml` with your specific values:

```yaml
offerings:
  - name: "Production Luna HSM Service"
    waldur_api_url: "https://your-waldur.example.com/api/"
    waldur_api_token: "your-actual-api-token"
    waldur_offering_uuid: "your-offering-uuid"
    backend_type: "luna_hsm"
    backend_settings:
      api_base_url: "https://your-actual-hsm-host:8443"
      hsm_id: "your-actual-hsm-id"
      admin_username: "admin"
      admin_password: "your-actual-admin-password"
      hsm_password: "your-actual-so-password"
      # ... other settings
```

## Step 4: Test Reporting

### 4.1 Test Reporting

```bash
# Test reporting
uv run waldur_site_agent -c /etc/waldur-site-agent/luna-hsm-config.yaml -m report

# Check logs for successful metric collection
tail -f /var/log/waldur-site-agent/luna-hsm.log
```

## Step 5: Set Up Scheduled Reporting

### 5.1 Create Systemd Service

```bash
# Copy service file
sudo cp systemd-conf/waldur-site-agent-luna-hsm.service /etc/systemd/system/

# Edit service file with correct paths
sudo nano /etc/systemd/system/waldur-site-agent-luna-hsm.service
```

### 5.2 Create Systemd Timer

```bash
# Copy timer file
sudo cp systemd-conf/waldur-site-agent-luna-hsm.timer /etc/systemd/system/

# Enable and start timer
sudo systemctl daemon-reload
sudo systemctl enable waldur-site-agent-luna-hsm.timer
sudo systemctl start waldur-site-agent-luna-hsm.timer

# Check timer status
sudo systemctl status waldur-site-agent-luna-hsm.timer
sudo systemctl list-timers | grep waldur
```

### 5.3 Alternative: Cron Setup

If you prefer cron over systemd timers:

```bash
# Edit crontab
crontab -e

# Add entry to run every 15 minutes
*/15 * * * * cd /path/to/waldur-site-agent && \
  uv run waldur_site_agent \
    -c /etc/waldur-site-agent/luna-hsm-config.yaml -m report \
  >> /var/log/waldur-site-agent/cron.log 2>&1
```

## Step 6: Monitoring and Maintenance

### 6.1 Log Monitoring

```bash
# Monitor real-time logs
tail -f /var/log/waldur-site-agent/luna-hsm.log

# Check for errors
grep -i error /var/log/waldur-site-agent/luna-hsm.log

# Check service status
sudo systemctl status waldur-site-agent-luna-hsm
```

### 6.2 Statistics Files

```bash
# View current statistics
cat /var/lib/waldur-site-agent/luna-hsm-stats.json | jq '.'

# Check file permissions
ls -la /var/lib/waldur-site-agent/luna-hsm-stats.json
```
