# MOAB plugin for Waldur Site Agent

Manages accounts in **Moab Accounting Manager (MAM)** by running the `mam-*`
command-line tools on the agent host. A Waldur resource becomes a MAM account
with a fund; its limit is a deposit into that fund.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `moab` | `waldur_site_agent.backends` | order processing, membership sync, reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process`.

| Operation | Behaviour |
|---|---|
| Create resource | `mam-create-account` (name, description, organization) and `mam-create-fund` |
| Terminate resource | `mam-delete-account` and `mam-delete-fund` |
| Update limits | `mam-deposit` of the `deposit` limit into the account's fund |
| Add / remove members | `mam-modify-account --add-user` / `--del-user` |
| Pause / downscale / restore | **No-op** — returns `False`, nothing changes in MAM |
| Usage reporting | `mam-list-usagerecords`, summed per user and account |

The agent host must have the MAM client tools on `PATH` and credentials that may
create accounts and funds.

## Configuration

```yaml
offerings:
  - name: "MOAB cluster"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<token>"
    waldur_offering_uuid: "<offering uuid>"
    backend_type: "moab"
    order_processing_backend: "moab"
    membership_sync_backend: "moab"
    reporting_backend: "moab"
    backend_settings:
      customer_prefix: "c_"
      project_prefix: "p_"
      allocation_prefix: "a_"
    backend_components:
      deposit:
        limit: 1000
        measured_unit: "EUR"
        unit_factor: 1
        accounting_type: "limit"
        label: "Deposit"
```

### Backend settings

The plugin reads no settings of its own. The keys above are read by the agent
core for every backend (resource and project id prefixes, `default_account`,
`soft_delete`, backend-id uniqueness); see [Configuration](../../docs/configuration.md).
They are validated by `waldur_site_agent_moab.schemas.MoabBackendSettingsSchema`,
so a misspelt key is logged as a warning when the agent loads its configuration.

### Components

The backend uses exactly one component, **`deposit`**, and requires it: the
backend fails to start without it. Its `unit_factor` is forced to `1`.

## Tests

```bash
cd plugins/moab && uv run pytest tests/
```
