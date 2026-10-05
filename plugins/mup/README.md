# MUP plugin for Waldur Site Agent

Integrates with **MUP**, the Portuguese project allocation portal, over its REST
API. A Waldur project becomes a MUP project and each Waldur resource becomes one
or more MUP allocations in it — one per component with a limit.

| Waldur | MUP |
|---|---|
| Project (named `<anything> / <grant number>`) | Project, looked up and created by grant number |
| Resource | One allocation per component, with the component's `mup_allocation_type` |
| Project member | User (a user request when MUP does not know the user yet) and project member |

The grant number is the part of the Waldur project name after the first `/`; a
project whose name has none cannot be ordered against this offering.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `mup` | `waldur_site_agent.backends` | order processing, membership sync, reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process`.

| Operation | Behaviour |
|---|---|
| Create resource | Creates and activates the MUP project if needed, then one allocation per component |
| Terminate resource | Deactivates the MUP project (allocations are not deleted) |
| Update limits | Updates each component's allocation size |
| Add / remove members | Creates the MUP user or user request, adds or deactivates the project member |
| Pause / downscale / restore | **No-op** — returns `False`, nothing changes in MUP |
| Usage reporting | Allocation usage from MUP, converted with each component's `unit_factor` |

Users the agent creates start in MUP's "pending approval" state; a MUP
administrator may have to approve them before they can join projects.

## Configuration

```yaml
offerings:
  - name: "MUP HPC allocation"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<token>"
    waldur_offering_uuid: "<offering uuid>"
    backend_type: "mup"
    order_processing_backend: "mup"
    membership_sync_backend: "mup"
    reporting_backend: "mup"
    backend_settings:
      api_url: "https://mup.example.pt/"
      username: "<mup user>"
      password: "<mup password>"
      default_research_field: 1
      default_agency: "FCT"
      project_prefix: "waldur_"
      allocation_prefix: "alloc_"
    backend_components:
      cpu:
        measured_unit: "core-hours"
        unit_factor: 1
        accounting_type: "limit"
        label: "CPU core hours"
        mup_allocation_type: "Deucalion x86_64"
      storage:
        measured_unit: "GB"
        unit_factor: 1
        accounting_type: "limit"
        label: "Storage"
        mup_allocation_type: "storage"
```

### Backend settings

Validated by `waldur_site_agent_mup.schemas.MUPBackendSettingsSchema`; a
misspelt or missing required key is logged as a warning when the agent loads
its configuration, and the backend refuses to start without the three required
ones.

| Setting | Required | Default | Description |
|---|---|---|---|
| `api_url` | yes | — | MUP base URL |
| `username` | yes | — | MUP API user |
| `password` | yes | — | MUP API password |
| `default_research_field` | no | `1` | Research field id for new projects |
| `default_agency` | no | `FCT` | Funding agency for new projects |
| `default_storage_limit` | no | `1000` | Storage allocation size in GB when an order gives none |
| `project_prefix` | no | `waldur_` | Prefix of project identifiers |
| `allocation_prefix` | no | `alloc_` | Prefix of allocation identifiers |
| `default_user_salutation` | no | `Dr.` | Used when creating a user request |
| `default_user_gender` | no | `Other` | Used when the Waldur user has none |
| `default_user_birth_year` | no | `1990` | Used when the Waldur user has no birth date |
| `default_user_country` | no | `Portugal` | Full country name |
| `default_user_institution_type` | no | `Academic` | |
| `default_user_institution` | no | `Research Institution` | |
| `default_user_biography` | no | `Researcher using Waldur site agent for resource allocation` | |
| `user_funding_agency_prefix` | no | `WALDUR-SITE-AGENT-` | Prefix of generated grant ids for users |

Keys the agent core reads for every backend (`customer_prefix`, `soft_delete`,
backend-id uniqueness) are described in [Configuration](../../docs/configuration.md).

### Components

Every component must use `accounting_type: limit` — the backend refuses to
start otherwise. `mup_allocation_type` names the MUP allocation type the
component's allocation is created with (default `Deucalion x86_64`).

## Tests

```bash
cd plugins/mup && uv run pytest tests/
```
