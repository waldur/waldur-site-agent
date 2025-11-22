# Rocky 9 Installation Validation Results

## Test Environment

- **OS**: Rocky Linux 9.2 (Blue Onyx)
- **Test Date**: November 21, 2025
- **Server**: 193.40.154.165
- **Initial Access**: SSH as `rocky` user

## Validation Progress

### ✅ Completed Steps

#### System Information Verification

- Confirmed Rocky Linux 9.2 (Blue Onyx)
- ID: rocky, VERSION_ID: 9.2
- Support until 2032-05-31

#### System Update Process

- `sudo dnf update -y` initiated successfully
- Process began updating 280 packages including kernel 5.14.0-570.58.1.el9_6
- Large updates including linux-firmware (658 MB) and other system components

#### Development Tools Installation

- `dnf groupinstall "Development Tools"` started successfully
- Installation included essential packages:
  - gcc, gcc-c++, make, git, autoconf, automake
  - binutils, bison, flex, libtool, etc.

### ⚠️ Interrupted Steps

#### Connection Lost

- Server became unreachable during package installation
- SSH connection refused (port 22)
- Likely system reboot during kernel update process

## Identified Requirements for Rocky 9

Based on initial testing and system analysis:

### System Dependencies

1. **EPEL Repository** - Required for additional packages

   ```bash
   sudo dnf install -y epel-release
   ```

2. **Development Tools Group** - Essential for building Python packages

   ```bash
   sudo dnf groupinstall "Development Tools" -y
   ```

3. **System Libraries** - Required for waldur-site-agent dependencies

   ```bash
   sudo dnf install -y openssl-devel libffi-devel bzip2-devel sqlite-devel
   ```

### Python 3.11 Installation

Rocky 9 ships with Python 3.9 by default. For optimal compatibility:

```bash
# Install from EPEL repository
sudo dnf install -y python3.11 python3.11-pip python3.11-devel
```

### Security Considerations

1. **SELinux** - Enabled by default, requires proper contexts
2. **Firewalld** - Active, needs configuration for API endpoints
3. **Service User** - Dedicated user recommended for security

### Service Management

1. **Systemd** - Version supports required features
2. **Journal Logging** - Available for log management
3. **Service Dependencies** - Standard systemd unit files compatible

## Recommended Installation Refinements

### 1. Robust Installation Script

Create a script that handles common issues:

```bash
#!/bin/bash
# rocky9-install-waldur-agent.sh

set -e

echo "Installing Waldur Site Agent on Rocky Linux 9..."

# Update system
sudo dnf update -y

# Install EPEL
sudo dnf install -y epel-release

# Install development tools (in one command to reduce interruptions)
sudo dnf groupinstall "Development Tools" -y
sudo dnf install -y git curl wget openssl-devel libffi-devel bzip2-devel sqlite-devel python3.11 python3.11-pip python3.11-devel

# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

echo "Base system preparation complete."
```

### 2. Service User Setup

```bash
# Create service user with proper home directory
sudo useradd -r -s /bin/bash -d /opt/waldur-agent -m waldur-agent

# Set up directory structure
sudo mkdir -p /etc/waldur /var/log/waldur-agent
sudo chown waldur-agent:waldur-agent /etc/waldur /var/log/waldur-agent
sudo chmod 750 /etc/waldur /var/log/waldur-agent
```

### 3. SELinux Configuration

```bash
# Set proper contexts
sudo setsebool -P httpd_can_network_connect 1
sudo semanage fcontext -a -t admin_home_t "/opt/waldur-agent(/.*)?"
sudo restorecon -R /opt/waldur-agent/
```

## Next Steps for Complete Validation

1. **Reconnect to System** - When server is available
2. **Complete Installation** - Run through full process
3. **Test All Modes** - Verify each agent mode works
4. **Document Issues** - Any Rocky 9 specific problems
5. **Performance Testing** - Resource usage and stability

## Known Considerations

### Package Management

- DNF is the package manager (not YUM)
- EPEL repository needed for additional packages
- Rocky repositories mirror RHEL structure

### Python Environment

- Default Python 3.9 should work but 3.11 recommended
- UV package manager preferred over pip
- Virtual environments recommended for isolation

### Networking

- Firewalld active by default
- NetworkManager handles network configuration
- IPv6 enabled by default

### Security

- SELinux enforcing by default
- Automatic security updates available via dnf-automatic
- Audit logging enabled

## Lessons Learned

1. **Large Updates** - Rocky 9 systems may require significant updates on fresh install
2. **Reboot Required** - Kernel updates may cause system restart
3. **Connection Stability** - Plan for potential interruptions during system updates
4. **EPEL Dependency** - Many development packages require EPEL repository

## Recommendations for Documentation

1. **Add Reboot Warning** - Inform users about potential system restart during updates
2. **Connection Recovery** - Document how to handle SSH disconnections
3. **Verification Steps** - Add commands to verify installation at each step
4. **Troubleshooting** - Common issues and solutions section
