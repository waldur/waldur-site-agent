# Upgrading Waldur Site Agent

This guide covers what to consider before upgrading, the recommended upgrade sequence,
and how to validate that the new agent version works correctly with your Waldur Mastermind instance.

## What to Consider Before Upgrading

### The `waldur-api-client` pin is the compatibility contract

The agent talks to Waldur Mastermind through the `waldur-api-client` Python package,
which is a generated SDK pinned to an **exact version** in the agent's `pyproject.toml`.

A compatible pair means:

- No API endpoints used by the agent have been removed or had incompatible schema changes
  in Mastermind since that SDK version was generated.

When you upgrade the agent to a new release, its `waldur-api-client` pin almost always
bumps too. Check the [CHANGELOG](../CHANGELOG.md) for entries about `waldur-api-client`
upgrades — these indicate a new API surface is required and you must ensure Mastermind
is recent enough to support it.

### Review the CHANGELOG for breaking changes

Before upgrading, read the CHANGELOG entries between your current version and the target
version and look for:

| Signal | What it means |
|---|---|
| New required configuration keys | Add them to the config file before starting the agent, or it will fail to start. |
| Removed configuration keys | Remove them. Unknown keys are ignored without a warning, so they silently stop working. |
| Backend behaviour changes | Check the CHANGELOG description; verify flags and settings match the new expectations. |
| New plugin packages | Install the relevant `waldur-site-agent-<plugin>` package if you use that backend. |

### Verify Mastermind is compatible first

The agent requires Mastermind to expose API endpoints that match its `waldur-api-client` pin.
**Upgrade Mastermind before the agent** — a newer agent talking to an older Mastermind can
fail with `404 Not Found` or schema validation errors on endpoints the old Mastermind
does not yet expose.

### STOMP / event-process mode

If you run `agent-event-process`, also check whether any new event types were added.
The agent subscribes to topics at startup; a configuration mismatch between agent and
the RabbitMQ/STOMP broker does not prevent startup but can cause silent gaps in processing.

---

## Upgrade Order

**Always upgrade Waldur Mastermind first, then the site agent.**

```text
1. Upgrade Waldur Mastermind
2. Verify Mastermind is healthy (API responds, worker processes running)
3. Stop site agent services
4. Upgrade waldur-site-agent (and plugins)
5. Update configuration if the release requires new keys
6. Start site agent services
7. Validate (see below)
```

### Why Mastermind first?

The agent reads from and writes to Mastermind. During a Mastermind upgrade the agent
can safely continue running against the old version — it will use existing endpoints.
The reverse is not safe: a new agent may call endpoints that do not yet exist in an
older Mastermind, causing immediate errors.

### Stopping services

```bash
# Polling mode
systemctl stop waldur-agent-order-process waldur-agent-membership-sync waldur-agent-report

# Event-process (STOMP) mode
systemctl stop waldur-agent-event-process waldur-agent-report
```

### Upgrading the package

All plugin packages share the core package's version number. **Always upgrade every installed
plugin together with the core**, to the same version.

For the [uv install](installation.md#2-install-the-agent), re-run the install with `--force`
and the new version on every package. `uv tool upgrade` is not enough: it keeps the version pins
the tool was installed with, so a pinned install does not move.

```bash
V=<NEW_VERSION>
sudo env UV_TOOL_DIR=/opt/waldur-agent/tools \
         UV_TOOL_BIN_DIR=/usr/local/bin \
         UV_PYTHON_INSTALL_DIR=/opt/waldur-agent/python \
  /usr/local/bin/uv tool install --force --python 3.12 --managed-python "waldur-site-agent==$V" \
    --with-executables-from "waldur-site-agent-slurm==$V" \
    --with "waldur-site-agent-basic-username-management==$V"
```

List the same plugins as in your original install. For the pip install into a virtual
environment:

```bash
V=<NEW_VERSION>
sudo /opt/waldur-agent/venv/bin/pip install --upgrade \
  "waldur-site-agent==$V" \
  "waldur-site-agent-slurm==$V" \
  "waldur-site-agent-basic-username-management==$V"
```

### Helm chart

If you deploy via Helm, the chart version mirrors the agent release version.
Charts are published to <https://waldur.github.io/waldur-site-agent>. If you have
not added that repository yet:

```bash
helm repo add waldur https://waldur.github.io/waldur-site-agent
```

Refresh the index, check which versions are available, then upgrade. Update
`image.tag` (or use the chart's default) and run:

```bash
helm repo update
helm search repo waldur/waldur-site-agent --versions
helm upgrade waldur-site-agent waldur/waldur-site-agent --version <NEW_VERSION>
```

Add `--devel` to both `helm search` and `helm upgrade` when moving to a release
candidate — Helm hides pre-release versions otherwise.

See the [chart README](../helm/waldur-site-agent/README.md) for the full list of
configurable values.

---

## Validating the Upgrade

### 1. Run diagnostics

`waldur_site_diagnostics` checks connectivity, token permissions, offering availability,
and backend health for every offering in the configuration:

```bash
sudo /usr/local/bin/waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
```

A successful run prints `DIAGNOSTICS START … DIAGNOSTICS END` with no errors and exits 0.
Any `ERROR` line indicates a problem to fix before starting production services.

### 2. Smoke-test order processing

After starting services, verify the agent is processing work by checking logs for
normal activity within one reconciliation interval:

```bash
# Polling mode — look for successful order/membership cycles
journalctl -u waldur-agent-order-process.service -f

# Event-process mode — look for STOMP connection confirmation and heartbeats
journalctl -u waldur-agent-event-process.service -f
```

Signs of a healthy agent, as the agent logs them:

- every mode: `Running agent in <mode> mode`, then `Processing offering <name> (<uuid>)`
- `order_process`: `There are no pending or executing orders`, or one line per order it works on
- `membership_sync`: `Fetched N resources (N with backend_id set) under <name> offering`, then
  `Refreshing resource <name> (<backend id>) last sync` for each resource
- `report`: `Synching data to Waldur`
- `event_process`: `Started unified STOMP connection for queue consumer_<uuid>`
- no repeated `error` lines within the first few minutes

### 3. Check liveness

Each unit's heartbeat must stay fresh (see
[Deployment → Heartbeat file per service](deployment.md#heartbeat-file-per-service)):

```bash
sudo /usr/local/bin/waldur_site_healthz --liveness-only \
  --heartbeat-path /run/waldur-site-agent-order-process/heartbeat
```

### 4. Run a test order (optional but recommended for major upgrades)

Place a small test order through Waldur and confirm the agent picks it up, provisions
the backend resource, and transitions the order to `Done` within a reasonable time.

---

## SLURM Plugin

See [SLURM Plugin Upgrade Notes](../plugins/slurm/docs/upgrading.md) for SLURM-specific
`backend_settings` reference, QoS configuration, account hierarchy behaviour,
and post-upgrade validation steps.

---

## Rollback

The agent is stateless — its only persistent state is in Waldur Mastermind and the
backend (e.g. SLURM accounts). Rolling back is safe as long as the older agent version
is compatible with the current Mastermind version.

Re-run the install from [Upgrading the package](#upgrading-the-package) with the previous
version, then restart:

```bash
sudo systemctl restart 'waldur-agent-*'
```

If you also rolled back Mastermind, roll it back before rolling back the agent,
following the same Mastermind-first order.
