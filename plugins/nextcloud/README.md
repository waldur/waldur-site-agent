# waldur-site-agent-nextcloud

Nextcloud plugin for [Waldur Site Agent](https://github.com/waldur/waldur-site-agent).

Users purchase storage plans in Waldur and automatically receive access to a shared
Nextcloud Group Folder. Login is via Keycloak OIDC.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `nextcloud` | `waldur_site_agent.backends` | order processing, membership sync, reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process` (recommended).

| Operation | Behaviour |
|---|---|
| Create resource | Nextcloud group and Group Folder with the ordered quota |
| Terminate resource | Deletes the Group Folder and the group |
| Update limits | Sets the Group Folder quota |
| Add / remove members | Adds / removes users in the Nextcloud group |
| Pause | Sets the folder quota to 0 |
| Downscale | Sets the folder quota to 1 GiB |
| Restore | **No-op** — returns `False`; the quota comes back with the next limit update |
| Usage reporting | Group Folder bytes used |

## How it works

- Each Waldur resource → a Nextcloud **Group** + **Group Folder** (with quota from the plan)
- Users added to the resource → added to the Nextcloud group → can access the folder
- Usage is reported as Group Folder bytes used

## Prerequisites

Install and enable in Nextcloud admin:
- **User OIDC** app (`user_oidc`) — configure to point at your Keycloak realm
- **Group Folders** app (`groupfolders`)

In the OIDC provider settings, set the user ID attribute to `preferred_username`
(matches the Keycloak username that the site-agent uses when calling the OCS API).

## Configuration example

```yaml
offerings:
  - name: "Nextcloud Storage"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<site agent api token>"
    waldur_offering_uuid: "<offering uuid>"
    backend_type: "nextcloud"
    order_processing_backend: "nextcloud"
    membership_sync_backend: "nextcloud"
    reporting_backend: "nextcloud"
    # STOMP is strongly recommended for Nextcloud.  On event-based deployments
    # the agent retries the team-member fetch to avoid a race where the
    # create-order event arrives before Waldur commits the initial membership.
    stomp_enabled: true
    # stomp_membership_sync_enabled: false  # uncomment to keep HTTP polling for
    #                                       # membership sync even with STOMP on
    backend_settings:
      nextcloud_url: "https://nextcloud.example.com"
      admin_username: "admin"
      admin_password: "<nextcloud admin or app password>"
      group_prefix: "waldur-"          # prefix for auto-created groups
      default_storage_quota_gb: 25     # fallback if plan has no storage limit (GiB)
      allow_resharing: false           # allow users to reshare folder contents
    backend_components:
      storage:
        # Quota values are in GiB (1 GiB = 1024³ bytes), consistent with how
        # Nextcloud labels storage in its UI.
        measured_unit: "GiB"
        accounting_type: "limit"
        label: "Storage"
        unit_factor: 1
```

### Backend settings reference

| Key | Default | Description |
|-----|---------|-------------|
| `nextcloud_url` | *(required)* | Base URL of the Nextcloud instance |
| `admin_username` | *(required)* | Admin account for OCS API calls |
| `admin_password` | *(required)* | Admin password or app password |
| `group_prefix` | `"waldur-"` | Prefix prepended to every auto-created group name |
| `default_storage_quota_gb` | `25` | Fallback quota (GiB) when the Waldur plan has no storage limit |
| `allow_resharing` | `false` | If `false`, group members cannot reshare folder contents |

The agent does not expand `${VAR}` references in its configuration file; write the values
in, or generate the file from a template at deploy time. The settings are validated by
`waldur_site_agent_nextcloud.schemas.NextcloudBackendSettingsSchema`; a misspelt or missing
required key is logged as a warning when the agent loads its configuration, and the backend
refuses to start without the required ones. The backend also requires a `storage` component
with `accounting_type: limit`.

### STOMP settings reference

| Key | Default | Description |
|-----|---------|-------------|
| `stomp_enabled` | `false` | Enable STOMP event-driven processing |
| `stomp_membership_sync_enabled` | `null` | Inherits `stomp_enabled`. Set `false` to keep HTTP polling for membership. |

#### `team_fetch_attempts` (backend-level, non-configurable)

The Nextcloud backend sets `team_fetch_attempts = 4` internally.
On order-processing paths (including STOMP create-order events) the event can
arrive before Waldur has committed the initial team membership row.  The agent
retries the team-member fetch up to 4 times (with a 3 s delay between
attempts) if the list comes back empty.  Other backends default to 1 (no
retry); this higher value is specific to Nextcloud's create-order path.

Note: the retry only helps when the team is entirely absent.  A non-empty
but stale team (new member not yet committed) breaks on the first attempt.

## Tests

```bash
cd plugins/nextcloud && uv run pytest tests/
```
