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
| Removed configuration keys | Remove them — the agent may reject an unrecognised key. |
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

```bash
# PyPI install
pip install --upgrade waldur-site-agent

# With specific plugins (upgrade all at once to keep versions in sync)
pip install --upgrade \
  waldur-site-agent \
  waldur-site-agent-slurm \
  waldur-site-agent-keycloak-client
```

All plugin packages share the same version number as the core package.
**Always upgrade all installed plugins together with the core.**

### Helm chart

If you deploy via Helm, the chart version mirrors the agent release version.
Update `image.tag` (or use the chart's default) and run:

```bash
helm upgrade waldur-site-agent waldur/waldur-site-agent --version <NEW_VERSION>
```

---

## Validating the Upgrade

### 1. Run diagnostics

`waldur_site_diagnostics` checks connectivity, token permissions, offering availability,
and backend health for every offering in the configuration:

```bash
waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml
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

Signs of a healthy agent:

- `Connected to STOMP broker` (event-process mode)
- `Processing orders…` / `Membership sync complete` (polling mode)
- No repeated `ERROR` lines within the first few minutes
- Heartbeat updates visible in Waldur UI under the offering's agent status

### 3. Check the Waldur UI

In the Waldur service provider interface, open the offering and verify:

- Agent heartbeat timestamp is recent (updated within the last reconciliation interval)
- No offering-level error banners

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

```bash
pip install waldur-site-agent==<PREVIOUS_VERSION>
systemctl restart waldur-agent-*
```

If you also rolled back Mastermind, roll it back before rolling back the agent,
following the same Mastermind-first order.
