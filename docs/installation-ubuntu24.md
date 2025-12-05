# Ubuntu 24.04 LTS Installation Guide

This guide provides step-by-step instructions for installing Waldur Site Agent on Ubuntu 24.04 LTS (Noble Numbat).

## Prerequisites

- Ubuntu 24.04 LTS (Noble Numbat) installation
- SSH access with sudo privileges
- Internet connectivity

## System Preparation

### 1. Update System Packages

```bash
sudo apt update && sudo apt upgrade -y
```

### 2. Install Required System Packages

```bash
# Install development tools and dependencies
sudo apt install -y \
    build-essential \
    git \
    curl \
    wget \
    python3-dev \
    python3-pip \
    python3-venv \
    libssl-dev \
    libffi-dev \
    libbz2-dev \
    libsqlite3-dev \
    libreadline-dev \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev
```

### 3. Verify Python Installation

Ubuntu 24.04 comes with Python 3.12.3 by default, which is excellent for Waldur Site Agent:

```bash
# Check Python version
python3 --version
# Should show: Python 3.12.3

# Verify pip is available
python3 -m pip --version
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

### 1. Create Service User

```bash
# Create dedicated user for the agent
sudo adduser --system --group --home /opt/waldur-agent --shell /bin/bash waldur-agent

# Create configuration directory
sudo mkdir -p /etc/waldur
sudo chown waldur-agent:waldur-agent /etc/waldur
sudo chmod 750 /etc/waldur
```

### 2. Install Agent Using UV

```bash
# Switch to service user
sudo -u waldur-agent bash

# Navigate to home directory
cd ~

# Install waldur-site-agent using UV
uv tool install waldur-site-agent

# Add UV tools to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Verify installation
waldur_site_agent --help
```

### 3. Alternative: Install Using Pip (Virtual Environment)

```bash
# Switch to service user
sudo -u waldur-agent bash
cd ~

# Create virtual environment
python3 -m venv waldur-site-agent-env

# Activate virtual environment
source waldur-site-agent-env/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install waldur-site-agent
pip install waldur-site-agent

# Create wrapper script
mkdir -p ~/.local/bin
cat > ~/.local/bin/waldur_site_agent << 'EOF'
#!/bin/bash
source /opt/waldur-agent/waldur-site-agent-env/bin/activate
exec waldur_site_agent "$@"
EOF

chmod +x ~/.local/bin/waldur_site_agent

# Add to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### 4. Development Installation (Optional)

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
mkdir -p ~/.local/bin
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

#### Method 1: With UV (Recommended)

```bash
# Install SLURM plugin
uv tool install waldur-site-agent-slurm

# Install MOAB plugin
uv tool install waldur-site-agent-moab

# Install MUP plugin
uv tool install waldur-site-agent-mup

# Install OpenShift/OKD plugin
uv tool install waldur-site-agent-okd

# Install Harbor plugin
uv tool install waldur-site-agent-harbor

# Install Croit S3 plugin
uv tool install waldur-site-agent-croit-s3

# Install CSCS DWDI plugin
uv tool install waldur-site-agent-cscs-dwdi

# Install username management plugin
uv tool install waldur-site-agent-basic-username-management

# Install for service user (example with SLURM)
sudo -u waldur-agent bash -c "source ~/.local/bin/env && uv tool install waldur-site-agent-slurm"
```

#### Method 2: With Virtual Environment

```bash
# Install SLURM plugin in virtual environment
sudo -u waldur-agent bash
source waldur-site-agent-env/bin/activate
pip install waldur-site-agent-slurm

# Verify installation
python -c "import waldur_site_agent_slurm; print('SLURM plugin installed')"
```

#### Method 3: With System Package Manager (Future)

```bash
# Future Ubuntu packages (when available)
# sudo apt install python3-waldur-site-agent-slurm
# sudo apt install python3-waldur-site-agent-moab
# sudo apt install python3-waldur-site-agent-mup
# sudo apt install python3-waldur-site-agent-okd
# sudo apt install python3-waldur-site-agent-harbor
# sudo apt install python3-waldur-site-agent-croit-s3
# sudo apt install python3-waldur-site-agent-cscs-dwdi
# sudo apt install python3-waldur-site-agent-basic-username-management
```

### Plugin Verification

```bash
# Verify plugin installation with UV
sudo -u waldur-agent bash -c "source ~/.local/bin/env && python3 -c 'import
waldur_site_agent_slurm; print(\"SLURM plugin installed\")'"

# Check available backends (as service user)
sudo -u waldur-agent /opt/waldur-agent/.local/bin/waldur_site_diagnostics --help
```

### Backend-Specific Plugin Requirements

#### SLURM Plugin (waldur-site-agent-slurm)

**Required for**: SLURM cluster management

**Additional system requirements**:

```bash
# Install SLURM client tools
sudo apt install -y slurm-client

# Verify SLURM tools
sacct --help
sacctmgr --help
```

**Configuration**: Set `order_processing_backend: "slurm"` in your config file.

#### MOAB Plugin (waldur-site-agent-moab)

**Required for**: MOAB cluster management

**Additional system requirements**:

```bash
# Install MOAB client tools (consult your MOAB documentation for Ubuntu packages)
# Example (adjust based on your MOAB distribution):
# sudo apt install moab-client

# Verify MOAB tools (requires root access)
# sudo mam-list-accounts --help
```

**Note**: MOAB installation depends on your specific MOAB distribution. Consult your MOAB documentation for Ubuntu packages.

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
sudo snap install oc  # Ubuntu snap package

# Or install oc client manually
curl -LO https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/\
openshift-client-linux.tar.gz
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

## Configuration Setup

### 1. Download Configuration Template

```bash
# Download configuration template
sudo curl -L \
https://raw.githubusercontent.com/waldur/waldur-site-agent/main/examples/waldur-site-agent-config.yaml.example \
  -o /etc/waldur/waldur-site-agent-config.yaml

# Set proper ownership and permissions
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

## Backend-Specific Setup

### SLURM Backend (if applicable)

```bash
# Install SLURM client tools
sudo apt install -y slurm-client

# Verify SLURM tools are available
sacct --help
sacctmgr --help
```

### MOAB Backend (if applicable)

MOAB installation depends on your specific MOAB distribution. Consult your MOAB documentation for Ubuntu packages.

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

### 2. Modify Service Files for Ubuntu

Update the service files to use the correct paths:

```bash
# Function to update service file
update_service_file() {
    local service_file="$1"
    local mode="$2"

    sudo sed -i "s|^User=.*|User=waldur-agent|" "$service_file"
    sudo sed -i "s|^Group=.*|Group=waldur-agent|" "$service_file"
    sudo sed -i "s|^ExecStart=.*|ExecStart=/opt/waldur-agent/.local/bin/waldur_site_agent -m $mode -c
/etc/waldur/waldur-site-agent-config.yaml|" "$service_file"
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

Ubuntu 24.04 uses UFW (Uncomplicated Firewall):

```bash
# Check firewall status
sudo ufw status

# If UFW is active, allow outbound HTTPS (usually allowed by default)
sudo ufw allow out 443/tcp

# For custom ports or MQTT/STOMP, add specific rules:
# sudo ufw allow out 8883/tcp  # MQTT over TLS
# sudo ufw allow out 61613/tcp # STOMP
```

## AppArmor Configuration (if enabled)

Ubuntu 24.04 may have AppArmor enabled:

```bash
# Check AppArmor status
sudo aa-status

# If needed, create AppArmor profile for the agent
# This is typically not required for standard installations
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

```bash
# Create health check script
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

```bash
# Install unattended-upgrades for security updates
sudo apt install -y unattended-upgrades

# Configure automatic security updates
echo 'Unattended-Upgrade::Automatic-Reboot "false";' | sudo tee -a /etc/apt/apt.conf.d/50unattended-upgrades
```

## Ubuntu 24.04 Specific Features

### 1. Snap Package Alternative

```bash
# Ubuntu 24.04 has excellent snap support
# Alternative installation via snap (if available in future):
# sudo snap install waldur-site-agent
```

### 2. Python 3.12 Benefits

- Improved performance over previous versions
- Better type annotations support
- Enhanced error messages
- Native support for all waldur-site-agent dependencies

### 3. System Integration

```bash
# Use systemd user services (alternative approach)
# This allows running without sudo but requires different setup

# Create user service directory
sudo -u waldur-agent mkdir -p ~/.config/systemd/user

# Copy and modify service files for user services
# (This is advanced configuration - use system services for standard deployments)
```

## Troubleshooting

### Common Issues

#### Permission Denied Errors

```bash
   # Check file ownership
   ls -la /etc/waldur/
   ls -la /opt/waldur-agent/

   # Fix ownership if needed
   sudo chown -R waldur-agent:waldur-agent /opt/waldur-agent/
   ```

#### Python/UV Path Issues

```bash
   # Verify PATH includes UV tools
   sudo -u waldur-agent echo $PATH

   # Manually source bashrc if needed
   sudo -u waldur-agent bash -c "source ~/.bashrc && which waldur_site_agent"
   ```

#### Network Connectivity

```bash
   # Test DNS resolution
   nslookup your-waldur.example.com

   # Test UFW firewall
   sudo ufw status verbose

   # Test with curl
   curl -v https://your-waldur.example.com/api/
   ```

#### Service Startup Issues

```bash
   # Check service status with details
   sudo systemctl status waldur-agent-order-process.service -l

   # Check journal logs
   sudo journalctl -u waldur-agent-order-process.service --no-pager

   # Test command manually
   sudo -u waldur-agent /opt/waldur-agent/.local/bin/waldur_site_agent --help
   ```

#### AppArmor Issues

```bash
   # Check for AppArmor denials
   sudo dmesg | grep -i apparmor | tail -10

   # Check AppArmor logs
   sudo journalctl | grep -i apparmor | tail -10
   ```

## Security Hardening

### 1. File Permissions

```bash
# Ensure restrictive permissions
sudo chmod 600 /etc/waldur/waldur-site-agent-config.yaml
sudo chmod 750 /etc/waldur
sudo chmod 755 /opt/waldur-agent
```

### 2. Service User Security

```bash
# Verify service user is properly configured
sudo passwd -l waldur-agent  # Lock password (account is system account)
sudo usermod -s /usr/sbin/nologin waldur-agent  # Disable shell login
```

### 3. Network Security

```bash
# Restrict outbound connections (advanced)
# Use iptables or UFW rules to limit network access to required endpoints only

# Example: Allow only HTTPS to Waldur API
sudo ufw allow out on any to YOUR_WALDUR_HOST port 443 proto tcp
```

## Performance Optimization

### 1. System Resources

```bash
# Monitor resource usage
sudo systemctl status waldur-agent-* | grep -A3 -B3 Memory
top -p $(pgrep -d, -f waldur_site_agent)
```

### 2. Log Rotation

```bash
# Configure log rotation for systemd journals
sudo mkdir -p /etc/systemd/journald.conf.d
echo '[Journal]
SystemMaxUse=100M
RuntimeMaxUse=50M
MaxRetentionSec=1month' | sudo tee /etc/systemd/journald.conf.d/waldur-agent.conf

sudo systemctl restart systemd-journald
```

This completes the comprehensive Ubuntu 24.04 LTS installation guide for Waldur Site Agent.
