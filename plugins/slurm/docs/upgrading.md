# Upgrading the SLURM Plugin

This page covers SLURM-specific considerations when upgrading `waldur-site-agent-slurm`.
Read the [general upgrade guide](../../../docs/upgrading.md) first.

## Required `backend_settings` keys

The SLURM backend reads the following keys from `backend_settings`.
Required keys must be present or the agent will fail to start.

| Key | Required | Notes |
|---|---|---|
| `default_account` | **Yes** | `DefaultAccount=` set on user associations; must exist in the cluster |
| `default_account_policy` | No | `common` (default), `individual`, or `none` — see below |
| `root_account` | No | Parent of the top-tier customer account. Defaults to `default_account`, then `root` |
| `customer_prefix` | **Yes** | Prefix for customer-level SLURM accounts |
| `project_prefix` | **Yes** | Prefix for project-level SLURM accounts |
| `allocation_prefix` | **Yes** | Prefix for allocation accounts |
| `cluster_name` | No | Must match the offering's `backend_id` in Waldur; required in multi-cluster setups |
| `slurm_bin_path` | No | Default `/usr/bin` |
| `parent_account` | No | Set for flat hierarchies (no customer tier); omit for nested hierarchy |
| `default_partition` | No | Fallback SLURM partition |
| `enforce_offering_partitions` | No | Default `false` |
| `enable_user_homedir_account_creation` | No | Default `true` |
| `default_homedir_umask` | No | Default `0077` |

Check the [CHANGELOG](../../../CHANGELOG.md) for any new required keys before upgrading.

## `default_account_policy`

Controls which account is passed as `DefaultAccount=` when the agent creates a
user→account association (`sacctmgr add user …`). The default account is where a
user's jobs charge when they submit without an explicit `-A`/`--account`.

- **`common`** (default) — `DefaultAccount=<default_account>` on every
  association. Stable: always references the configured, backend-verified
  account.
- **`individual`** — `DefaultAccount=<resource_id>` (the per-resource account).
  Keeps users off the org-wide root by default, **but** when that resource is
  terminated and its account deleted, the user's `DefaultAccount` dangles and
  SLURM rejects their job submissions until an operator repairs it.
- **`none`** — `DefaultAccount=` omitted entirely; sacctmgr auto-assigns for new
  users. Relies on the deployment's sacctmgr auto-assignment for brand-new users;
  for an existing user whose prior default account was deleted, the stale default
  is left unchanged (they may be unable to submit until repaired).

`common` is the safe default and is what most deployments should use. Only switch
to `individual` or `none` if you understand the dangling-`DefaultAccount` failure
modes above and have an operational process to handle them. An invalid value
(e.g. a typo) raises an error at agent startup rather than silently falling back
to `common`.

## QoS configuration

QoS state is driven by the `paused` and `downscaled` flags set by Waldur Mastermind
(via policy or manual action). The agent maps these flags to SLURM QoS names:

```yaml
backend_settings:
  qos_default: "normal"      # Applied when resource is active
  qos_downscaled: "low"      # Applied when Mastermind sets downscaled=true
  qos_paused: "pause"        # Applied when Mastermind sets paused=true
```

Optional per-account QoS creation during resource provisioning is available via
`qos_management`:

```yaml
backend_settings:
  qos_management:
    enabled: true
    # ...other QoS management keys
```

## Account hierarchy and `sync_resource_project`

When a project is moved to a different customer in Waldur, the SLURM account's parent
must be updated to reflect the new customer account. The agent handles this via
`sync_resource_project`, called at:

- **Polling mode**: every `order_process` or `membership_sync` cycle.
- **Event-process (STOMP) mode**: on incoming `RESOURCE` events, and periodically every
  reconciliation interval. Mastermind also pushes a `RESOURCE` event immediately when a
  project moves, so the hierarchy is corrected without waiting for the next cycle.

`sync_resource_project` is skipped when `parent_account` is set (flat hierarchy).

## Validating after upgrade

### Run diagnostics

`waldur_site_diagnostics` calls `SlurmBackend.diagnostics()`, which prints the configured
prefixes, `default_account`, and SLURM version, and returns an error if `sinfo` is
unreachable:

```bash
waldur_site_diagnostics -c /etc/waldur/waldur-site-agent-config.yaml 2>&1 | grep -E "SLURM|slurm|ERROR"
```

A healthy output looks like:

```text
SLURM customer prefix          = hpc_c_
SLURM project prefix           = hpc_p_
SLURM allocation prefix        = hpc_a_
SLURM default account          = root
Slurm version: slurm 23.11.4
```

### Verify account hierarchy for recently moved projects (STOMP mode)

If any project was moved between customers while the agent was not running or
was on an older version, the SLURM account parent may be stale. Trigger a sync
by restarting the agent or waiting for one reconciliation interval.

To check a specific account's parent directly:

```bash
sacctmgr show account <allocation_account> format=Account,ParentName -P
```

### Confirm QoS names exist in SLURM

If you use `qos_downscaled` or `qos_paused`, verify the referenced QoS objects
exist in the cluster:

```bash
sacctmgr show qos format=Name -P
```

A QoS name in `backend_settings` that does not exist in SLURM will cause the
downscale or pause action to fail with a `BackendError`.

## Filesystem quotas

If you use `homedir_quota` or `project_directory` (Lustre/CephFS/XFS quotas), no
migration steps are required — quota configuration is read fresh on each resource
creation or user add. After upgrading, run diagnostics and create a test resource to
confirm quota-setting commands succeed.

See [SLURM Storage Quotas](../../../docs/slurm-storage-quotas.md) for full configuration reference.
