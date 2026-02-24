# Rocky Linux 9 Installation Guide

This guide provides step-by-step instructions for installing Waldur Site Agent on Rocky Linux 9.

## Prerequisites

- Fresh Rocky Linux 9 installation
- SSH access with sudo privileges
- Internet connectivity

## System Preparation

### 1. Update System

```bash
sudo dnf update -y
```

### 2. Install Required System Packages

```bash
# Install development tools and dependencies
sudo dnf groupinstall "Development Tools" -y
sudo dnf install -y git curl wget openssl-devel libffi-devel bzip2-devel sqlite-devel
```

### 3. Install Python 3.13

Rocky 9 comes with Python 3.9 by default. For optimal compatibility, install Python 3.13 from EPEL:

```bash
# Enable EPEL repository
sudo dnf install -y epel-release

# Install Python 3.13
sudo dnf install -y python3.13 python3.13-pip

# Verify installation
python3.13 --version
```

### 4. Install UV Package Manager

UV is the recommended package manager for Waldur Site Agent:

```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add UV to PATH for current session
source ~/.bashrc

# Verify installation
uv --version
```

## Waldur Site Agent Installation

### Installation Method Options

Rocky Linux 9 supports two installation approaches:

1. **Python 3.13 Installation** (Recommended) - Latest Python from EPEL with native packages
2. **Full Development Installation** (Advanced) - Using UV with complete development environment

### Method 1: Python 3.13 Installation (Recommended)

This method uses the latest Python 3.13 from EPEL with native package management.

#### 1. Install Python 3.13 and Dependencies

```bash
# Install EPEL repository and Python 3.13
sudo dnf install -y epel-release
sudo dnf install -y python3.13 python3.13-pip

# Verify installation
python3.13 --version
python3.13 -m pip --version
```

#### 2. Create Service User

```bash
# Create dedicated user for the agent
sudo useradd -r -s /bin/bash -d /opt/waldur-agent -m waldur-agent

# Create configuration directory
sudo mkdir -p /etc/waldur
sudo chown waldur-agent:waldur-agent /etc/waldur
```

#### 3. Install Core Agent

```bash
# Install waldur-site-agent with Python 3.13 (as regular user first)
python3.13 -m pip install --user waldur-site-agent

# Verify installation
~/.local/bin/waldur_site_agent --help
```

#### 4. Install for Service User

```bash
# Install for service user
sudo -u waldur-agent python3.13 -m pip install --user waldur-site-agent

# Verify service user installation
sudo -u waldur-agent /opt/waldur-agent/.local/bin/waldur_site_agent --help
```

### Method 2: Full Development Installation (Advanced)

Use this method if you need full development tools or prefer UV package manager.

#### 1. Create Service User

```bash
# Create dedicated user for the agent
sudo useradd -r -s /bin/bash -d /opt/waldur-agent -m waldur-agent

# Create configuration directory
sudo mkdir -p /etc/waldur
sudo chown waldur-agent:waldur-agent /etc/waldur
```

#### 2. Install Agent Using UV

```bash
# Switch to service user
sudo -u waldur-agent bash

# Install waldur-site-agent
uv tool install waldur-site-agent

# Add UV tools to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Verify installation
waldur_site_agent --help
```

## Plugin Installation

Waldur Site Agent uses a modular plugin architecture. Install plugins based on your backend requirements.

### Available Plugins

- **waldur-site-agent-slurm**: SLURM cluster management
- **waldur-site-agent-moab**: MOAB cluster management
- **waldur-site-agent-mup**: MUP portal integration
- **waldur-site-agent-okd**: OpenShift/OKD container platform management
- **waldur-site-agent-harbor**: Harbor container registry management
- **waldur-site-agent-croit-s3**: Croit S3 storage management
- **waldur-site-agent-cscs-dwdi**: CSCS DWDI integration
- **waldur-site-agent-basic-username-management**: Username management

### Plugin Installation Methods

#### Method 1: With Python 3.13 (Recommended)

```bash
# Install SLURM plugin
python3.13 -m pip install --user waldur-site-agent-slurm

# Install MOAB plugin
python3.13 -m pip install --user waldur-site-agent-moab

# Install MUP plugin
python3.13 -m pip install --user waldur-site-agent-mup

# Install OpenShift/OKD plugin
python3.13 -m pip install --user waldur-site-agent-okd

# Install Harbor plugin
python3.13 -m pip install --user waldur-site-agent-harbor

# Install Croit S3 plugin
python3.13 -m pip install --user waldur-site-agent-croit-s3

# Install CSCS DWDI plugin
python3.13 -m pip install --user waldur-site-agent-cscs-dwdi

# Install username management plugin
python3.13 -m pip install --user waldur-site-agent-basic-username-management

# Install for service user (example with SLURM)
sudo -u waldur-agent python3.13 -m pip install --user waldur-site-agent-slurm
```

#### Method 2: With UV

```bash
# Install plugins with UV (development)
uv tool install waldur-site-agent-slurm
uv tool install waldur-site-agent-moab
uv tool install waldur-site-agent-mup
uv tool install waldur-site-agent-okd
uv tool install waldur-site-agent-harbor
uv tool install waldur-site-agent-croit-s3
uv tool install waldur-site-agent-cscs-dwdi
uv tool install waldur-site-agent-basic-username-management
```

### Plugin Verification

```bash
# Verify plugin installation
python3.13 -c "import waldur_site_agent_slurm; print('SLURM plugin installed')"
python3.13 -c "import waldur_site_agent_moab; print('MOAB plugin installed')"
python3.13 -c "import waldur_site_agent_mup; print('MUP plugin installed')"
python3.13 -c "import waldur_site_agent_okd; print('OKD plugin installed')"
python3.13 -c "import waldur_site_agent_harbor; print('Harbor plugin installed')"

# Check available backends (as service user)
sudo -u waldur-agent /opt/waldur-agent/.local/bin/waldur_site_diagnostics --help
```

### Backend-Specific Plugin Requirements

#### SLURM Plugin (waldur-site-agent-slurm)

**Required for**: SLURM cluster management

**Additional system requirements**:

```bash
# Install SLURM client tools
sudo dnf install -y slurm slurm-slurmd slurm-slurmctld

# Verify SLURM tools
sacct --help
sacctmgr --help
```

**Configuration**: Set `order_processing_backend: "slurm"` in your config file.

#### MOAB Plugin (waldur-site-agent-moab)

**Required for**: MOAB cluster management

**Additional system requirements**:

```bash
# Install MOAB client tools (adjust based on your MOAB distribution)
# Consult your MOAB documentation for Rocky Linux packages
sudo dnf install -y moab-client

# Verify MOAB tools (requires root access)
sudo mam-list-accounts --help
```

**Configuration**: Set `order_processing_backend: "moab"` in your config file.

#### MUP Plugin (waldur-site-agent-mup)

**Required for**: MUP portal integration

**No additional system requirements** - uses API calls only.

**Configuration**: Set `order_processing_backend: "mup"` in your config file.

#### OpenShift/OKD Plugin (waldur-site-agent-okd)

**Required for**: OpenShift and OKD container platform management

**Additional system requirements**:

```bash
# Install OpenShift CLI tools
sudo dnf install -y origin-clients

# Or install oc client manually
curl -LO https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/openshift-client-linux.tar.gz
tar -xzf openshift-client-linux.tar.gz
sudo mv oc /usr/local/bin/

# Verify OpenShift tools
oc version
```

**Configuration**: Set `order_processing_backend: "okd"` in your config file.

#### Harbor Plugin (waldur-site-agent-harbor)

**Required for**: Harbor container registry management

**No additional system requirements** - uses Harbor API calls only.

**Configuration**: Set `order_processing_backend: "harbor"` in your config file.

#### Croit S3 Plugin (waldur-site-agent-croit-s3)

**Required for**: Croit S3 storage management

**No additional system requirements** - uses S3-compatible API calls only.

**Configuration**: Set `order_processing_backend: "croit-s3"` in your config file.

#### CSCS DWDI Plugin (waldur-site-agent-cscs-dwdi)

**Required for**: CSCS DWDI integration

**No additional system requirements** - uses API calls only.

**Configuration**: Set `order_processing_backend: "cscs-dwdi"` in your config file.

#### Username Management Plugin (waldur-site-agent-basic-username-management)

**Required for**: Custom username generation and management

**No additional system requirements**.

**Configuration**: Set `username_management_backend: "base"` in your config file.

### 3. Alternative: Install from Source (Development)

For development or custom modifications:

```bash
# Switch to service user
sudo -u waldur-agent bash
cd ~

# Clone repository
git clone https://github.com/waldur/waldur-site-agent.git
cd waldur-site-agent

# Install using UV workspace
uv sync --all-packages

# Create wrapper script
cat > ~/.local/bin/waldur_site_agent << 'EOF'
#!/bin/bash
cd /opt/waldur-agent/waldur-site-agent
exec uv run waldur_site_agent "$@"
EOF

chmod +x ~/.local/bin/waldur_site_agent

# Add to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## Configuration Setup

### 1. Download Configuration Template

```bash
# Download configuration template
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/examples/waldur-site-agent-config.yaml.example \
  -o /etc/waldur/waldur-site-agent-config.yaml

# Set proper ownership
sudo chown waldur-agent:waldur-agent /etc/waldur/waldur-site-agent-config.yaml
sudo chmod 600 /etc/waldur/waldur-site-agent-config.yaml
```

### 2. Edit Configuration

```bash
# Edit configuration file
sudo -u waldur-agent nano /etc/waldur/waldur-site-agent-config.yaml
```

Update the following required fields:

- `waldur_api_url`: Your Waldur API endpoint
- `waldur_api_token`: Your Waldur API token
- `waldur_offering_uuid`: UUID from your Waldur offering
- Backend-specific settings as needed

### 3. Load Components into Waldur

```bash
# Load components (as waldur-agent user)
sudo -u waldur-agent waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```

## SLURM Backend Setup (if applicable)

If you're using SLURM backend, install SLURM tools:

```bash
# Install SLURM client tools
sudo dnf install -y slurm slurm-slurmd slurm-slurmctld

# Verify SLURM tools are available
sacct --help
sacctmgr --help
```

## MOAB Backend Setup (if applicable)

For MOAB backend (requires root access):

```bash
# Install MOAB client tools (adjust repository/package names as needed)
# This depends on your MOAB installation source
sudo dnf install -y moab-client

# Verify MOAB tools are available
sudo mam-list-accounts --help
```

## Systemd Service Setup

### 1. Download Service Files

```bash
# Create systemd service directory
sudo mkdir -p /etc/systemd/system

# Download service files
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-order-process/agent.service \
  -o /etc/systemd/system/waldur-agent-order-process.service

sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-report/agent.service \
  -o /etc/systemd/system/waldur-agent-report.service

sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/systemd-conf/agent-membership-sync/agent.service \
  -o /etc/systemd/system/waldur-agent-membership-sync.service
```

### 2. Modify Service Files for Rocky 9

The executable path depends on your installation method:

#### For Python 3.13 Installation (Method 1)

```bash
# Set the correct path for pip-based installation
AGENT_PATH="/opt/waldur-agent/.local/bin/waldur_site_agent"
```

#### For UV Installation (Method 2)

```bash
# Set the correct path for UV-based installation
AGENT_PATH="/opt/waldur-agent/.local/bin/waldur_site_agent"
```

Update the service files:

```bash
# Function to update service file
update_service_file() {
    local service_file="$1"
    local mode="$2"
    local agent_path="${3:-/opt/waldur-agent/.local/bin/waldur_site_agent}"

    sudo sed -i "s|^User=.*|User=waldur-agent|" "$service_file"
    sudo sed -i "s|^Group=.*|Group=waldur-agent|" "$service_file"
    sudo sed -i "s|^ExecStart=.*|ExecStart=${agent_path} -m ${mode} -c /etc/waldur/waldur-site-agent-config.yaml|" "$service_file"
    sudo sed -i "s|^WorkingDirectory=.*|WorkingDirectory=/opt/waldur-agent|" "$service_file"
}

# Update all service files
update_service_file "/etc/systemd/system/waldur-agent-order-process.service" "order_process"
update_service_file "/etc/systemd/system/waldur-agent-report.service" "report"
update_service_file "/etc/systemd/system/waldur-agent-membership-sync.service" "membership_sync"
```

### 3. Enable and Start Services

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable and start services
sudo systemctl enable waldur-agent-order-process.service
sudo systemctl enable waldur-agent-report.service
sudo systemctl enable waldur-agent-membership-sync.service

sudo systemctl start waldur-agent-order-process.service
sudo systemctl start waldur-agent-report.service
sudo systemctl start waldur-agent-membership-sync.service
```

## Firewall Configuration

Configure firewall if needed:

```bash
# Check if firewall is running
sudo systemctl status firewalld

# Allow outbound HTTPS (if using HTTPS for Waldur API)
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload

# For custom ports or STOMP, add specific rules:
# sudo firewall-cmd --permanent --add-port=61613/tcp # STOMP
# sudo firewall-cmd --reload
```

## SELinux Configuration

Rocky 9 has SELinux enabled by default. Configure it for the agent:

```bash
# Check SELinux status
sestatus

# Set proper SELinux contexts
sudo setsebool -P httpd_can_network_connect 1
sudo semanage fcontext -a -t bin_t "/opt/waldur-agent/.local/bin/waldur_site_agent"
sudo restorecon -R /opt/waldur-agent/.local/bin/

# If using custom directories, add contexts:
sudo semanage fcontext -a -t admin_home_t "/opt/waldur-agent(/.*)?"
sudo restorecon -R /opt/waldur-agent/
```

## Verification

### 1. Test Installation

```bash
# Test agent command
sudo -u waldur-agent waldur_site_agent --help

# Test configuration
sudo -u waldur-agent waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
```

### 2. Check Service Status

```bash
# Check all services
sudo systemctl status waldur-agent-*

# Check logs
sudo journalctl -u waldur-agent-order-process.service -f
```

### 3. Test Connectivity

```bash
# Test Waldur API connectivity (replace with your actual URL and token)
curl -H "Authorization: Token YOUR_TOKEN" https://your-waldur.example.com/api/

# Test backend connectivity (for SLURM)
sudo -u waldur-agent sacct --help
```

## Monitoring and Maintenance

### 1. Log Monitoring

```bash
# Monitor all agent logs
sudo journalctl -u waldur-agent-* -f

# Check for errors
sudo journalctl -u waldur-agent-* --since "1 hour ago" | grep -i error
```

### 2. Health Check Script

Create a health check script:

```bash
sudo tee /usr/local/bin/check-waldur-agent.sh << 'EOF'
#!/bin/bash

SERVICES=("waldur-agent-order-process" "waldur-agent-report" "waldur-agent-membership-sync")
FAILED=0

for service in "${SERVICES[@]}"; do
    if ! systemctl is-active --quiet "$service"; then
        echo "CRITICAL: $service is not running"
        FAILED=1
    fi
done

if [ $FAILED -eq 0 ]; then
    echo "OK: All Waldur agent services are running"
    exit 0
else
    exit 1
fi
EOF

sudo chmod +x /usr/local/bin/check-waldur-agent.sh

# Test the script
/usr/local/bin/check-waldur-agent.sh
```

### 3. Automatic Updates

Set up automatic security updates:

```bash
# Install dnf-automatic
sudo dnf install -y dnf-automatic

# Configure for security updates only
sudo sed -i 's/apply_updates = no/apply_updates = yes/' /etc/dnf/automatic.conf
sudo sed -i 's/upgrade_type = default/upgrade_type = security/' /etc/dnf/automatic.conf

# Enable the service
sudo systemctl enable --now dnf-automatic.timer
```

## Troubleshooting

### Common Issues

#### Permission Denied Errors

```bash
# Check file ownership
ls -la /etc/waldur/
ls -la /opt/waldur-agent/.local/bin/

# Fix ownership if needed
sudo chown -R waldur-agent:waldur-agent /opt/waldur-agent/
```

#### SELinux Denials

```bash
# Check for denials
sudo sealert -a /var/log/audit/audit.log

# Generate policy if needed
sudo ausearch -c 'waldur_site_age' --raw | audit2allow -M my-waldur-agent
sudo semodule -i my-waldur-agent.pp
```

#### Network Connectivity

```bash
# Test DNS resolution
nslookup your-waldur.example.com

# Test firewall
sudo firewall-cmd --list-all

# Test with curl
curl -v https://your-waldur.example.com/api/
```

#### Service Startup Issues

```bash
# Check service status
sudo systemctl status waldur-agent-order-process.service -l

# Check journal logs
sudo journalctl -u waldur-agent-order-process.service --no-pager
   ```

## Security Hardening

### 1. Secure Configuration File

```bash
# Set restrictive permissions
sudo chmod 600 /etc/waldur/waldur-site-agent-config.yaml
sudo chown waldur-agent:waldur-agent /etc/waldur/waldur-site-agent-config.yaml
```

### 2. Limit User Privileges

```bash
# Ensure waldur-agent user has minimal privileges
sudo usermod -s /usr/sbin/nologin waldur-agent  # Disable shell login
```

### 3. Network Security

```bash
# Restrict outbound connections (adjust as needed)
# Allow outbound HTTPS to Waldur API
sudo firewall-cmd --permanent --direct --add-rule ipv4 filter OUTPUT 0 \
  -m owner --uid-owner $(id -u waldur-agent) \
  -d your-waldur.example.com -p tcp --dport 443 -j ACCEPT

# Block all other outbound traffic for waldur-agent user
sudo firewall-cmd --permanent --direct --add-rule ipv4 filter OUTPUT 1 \
  -m owner --uid-owner $(id -u waldur-agent) -j DROP
sudo firewall-cmd --reload
```

This completes the Rocky Linux 9 specific installation guide. The next step would be to test these
instructions on the actual system.
