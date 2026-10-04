# Installation Guide

This guide installs Waldur Site Agent on a Linux host and gets it ready to run as
systemd services. For Kubernetes, use the [Helm chart](#kubernetes-helm) instead.
For the fastest path to a first working agent, follow the [Quickstart](quickstart.md),
which also covers creating the offering in Waldur.

Every command in this guide was run on Ubuntu 24.04 and Rocky Linux 9.

## Before you start

- **A Waldur offering** of type `Waldur site agent`, its UUID, and an API token for an
  account with the `OFFERING.MANAGER` role on it — see
  [Quickstart, step 1](quickstart.md#1-create-the-offering-in-waldur).
- **Root access** to the host the agent runs on. For SLURM this is a host that can run
  `sacctmgr` and `sacct` against your cluster's accounting database.
- **Outbound HTTPS** from that host to the Waldur API. Event mode additionally connects
  to Waldur's STOMP endpoint over WebSocket (see
  [Configuration](configuration.md#stomp_enabled)).

## What gets installed

The agent is a core package plus one plugin package per backend. **The core package
contains no backends**: installing only `waldur-site-agent` gives an agent that fails
with `Unable to create backend` for every offering. Install, in the same environment:

- `waldur-site-agent` — the agent and its commands
- the plugin for each backend your offerings use, e.g. `waldur-site-agent-slurm`
  (the full list is in the [README](../README.md))
- `waldur-site-agent-basic-username-management` — provides the default
  `username_management_backend: "base"`. Install it unless every offering sets another
  username backend (for example `ldap` from `waldur-site-agent-ldap`)

All packages share one version number. Install and upgrade them together.

## 1. Operating system packages

=== "Ubuntu 24.04"

    ```bash
    sudo apt update
    sudo apt install -y curl ca-certificates
    # SLURM backend only: sacct and sacctmgr
    sudo apt install -y slurm-client
    ```

=== "Rocky Linux 9"

    ```bash
    # curl is preinstalled (curl-minimal); installing the full curl package conflicts with it
    # SLURM backend only: sacct and sacctmgr (from EPEL)
    sudo dnf install -y epel-release
    sudo dnf install -y slurm
    ```

For SLURM, the host also needs your cluster's `slurm.conf` (and munge key, if your
cluster uses munge) so that `sacctmgr show cluster` works. Check that before going on.
Other backends talk to their service over HTTP and need nothing extra; see the plugin's
README for anything specific (for example the `oc` client for OKD).

## 2. Install the agent

The recommended install uses [uv](https://docs.astral.sh/uv/) (0.8.5 or newer). With
`--managed-python` it runs the agent on a Python it downloads into
`/opt/waldur-agent/python`, so the host's Python and its OS updates do not affect the agent.
Without the flag uv reuses a matching system Python (Ubuntu 24.04's `/usr/bin/python3.12`).
uv puts the agent's commands in `/usr/local/bin`, where systemd finds them.

Rocky Linux leaves `/usr/local/bin` out of `sudo`'s `secure_path` and out of root's
`PATH`, so this guide calls every command by its full path under `sudo`. That works
on both systems; on Ubuntu the short names work too.

```bash
# uv, system-wide
curl -LsSf https://astral.sh/uv/install.sh \
  | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh

# The agent with the SLURM plugin and the default username backend
sudo env UV_TOOL_DIR=/opt/waldur-agent/tools \
         UV_TOOL_BIN_DIR=/usr/local/bin \
         UV_PYTHON_INSTALL_DIR=/opt/waldur-agent/python \
  /usr/local/bin/uv tool install --python 3.12 --managed-python waldur-site-agent \
    --with-executables-from waldur-site-agent-slurm \
    --with waldur-site-agent-basic-username-management
```

For other backends, add one `--with waldur-site-agent-<plugin>` per plugin. Use
`--with-executables-from` instead of `--with` for a plugin that ships its own command,
so the command lands on the `PATH` too — the SLURM plugin ships
`waldur_site_diagnose_slurm_account`.

Do **not** run a separate `uv tool install waldur-site-agent-<plugin>`: every
`uv tool install` creates its own isolated environment, and the agent never sees a
plugin installed into a different one.

Pin a version (recommended for production) by adding `==<VERSION>` to every package,
for example `waldur-site-agent==1.0.8` and `waldur-site-agent-slurm==1.0.8`.

### Check the install

```bash
/usr/local/bin/waldur_site_agent --help

# The backends the agent can see — every backend your offerings use must be listed
/opt/waldur-agent/tools/waldur-site-agent/bin/python -c "
from waldur_site_agent.common.utils import BACKENDS, USERNAME_BACKENDS
print('backends:', sorted(BACKENDS), 'username backends:', sorted(USERNAME_BACKENDS))"
```

With the command above this prints `backends: ['slurm'] username backends: ['base']`.
For the pip install below, use `/opt/waldur-agent/venv/bin/python` instead.

### Alternative: a virtual environment with pip

If uv is not an option, use a virtual environment (Python 3.9.2 or newer) and link the
commands into `/usr/local/bin`:

```bash
sudo python3 -m venv /opt/waldur-agent/venv
sudo /opt/waldur-agent/venv/bin/pip install \
  waldur-site-agent waldur-site-agent-slurm waldur-site-agent-basic-username-management
sudo ln -sf /opt/waldur-agent/venv/bin/waldur_* /usr/local/bin/
```

Re-run the `ln` line after installing a plugin that adds a command.

## 3. Configuration

```bash
sudo mkdir -p /etc/waldur
sudo curl -L \
  https://raw.githubusercontent.com/waldur/waldur-site-agent/main/examples/waldur-site-agent-config.yaml.example \
  -o /etc/waldur/waldur-site-agent-config.yaml
sudo chmod 600 /etc/waldur/waldur-site-agent-config.yaml
```

The example holds several offerings; keep the ones you need and fill in, for each:

- `waldur_api_url`, `waldur_offering_uuid`
- `waldur_api_token`, or the three `oidc_*` keys for OIDC client credentials
- `backend_type` and the `order_processing_backend`, `membership_sync_backend` and
  `reporting_backend` you run — an offering without them is skipped or fails
- `backend_settings` and `backend_components` for your backend

The [Configuration Reference](configuration.md) describes every key. Then push the
offering's components into Waldur:

```bash
sudo /usr/local/bin/waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```

If your backend creates home directories (SLURM with
`enable_user_homedir_account_creation` left on), create them for existing users once:

```bash
sudo /usr/local/bin/waldur_site_create_homedirs -c /etc/waldur/waldur-site-agent-config.yaml
```

## 4. Verify

```bash
sudo /usr/local/bin/waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
echo "exit code: $?"
```

Diagnostics checks both sides for every offering: the Waldur API, token and offering,
and the backend itself (for SLURM it runs the SLURM tools). It exits non-zero when
something is wrong. For SLURM, `waldur_site_diagnose_slurm_account <allocation-account>`
compares one account's actual SLURM state with what Waldur expects, once resources exist; the
account name is the resource's backend ID in Waldur.

## 5. Run it

Install the systemd units as described in the
[Deployment Guide](deployment.md#systemd-service-setup).

## Firewall and SELinux

- The agent only makes outbound connections: HTTPS to the Waldur API and, in event
  mode, a WebSocket to the STOMP endpoint (`stomp_ws_host` / `stomp_ws_port`). No
  inbound ports are needed.
- Behind a proxy, set `global_proxy` in the configuration (HTTP or SOCKS5). The STOMP
  WebSocket does not use it yet; set `https_proxy` in the unit's environment for event
  mode.
- With SELinux enforcing, check `ausearch -m avc -ts recent` if a unit fails to start
  or the agent cannot reach Waldur or the backend.

## Kubernetes (Helm)

```bash
helm repo add waldur https://waldur.github.io/waldur-site-agent
helm repo update
helm install waldur-site-agent waldur/waldur-site-agent --values my-values.yaml
```

The image is built with every plugin in the repository at that release. See the
[chart README](../helm/waldur-site-agent/README.md) for the values and modes.

## Development installation

```bash
git clone https://github.com/waldur/waldur-site-agent.git
cd waldur-site-agent
uv sync --all-packages
uv run waldur_site_agent --help
```

`uv sync --all-packages` installs the core package and every plugin into the
repository's `.venv`.
