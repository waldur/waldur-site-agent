# DigitalOcean plugin for Waldur Site Agent

This plugin integrates Waldur Site Agent with DigitalOcean using the
`python-digitalocean` SDK. It provisions droplets based on marketplace orders
and exposes droplet metadata back to Waldur.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `digitalocean` | `waldur_site_agent.backends` | order processing, membership sync, reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process`.

| Operation | Behaviour |
|---|---|
| Create resource | Droplet with the region, image and size from the order or the defaults; its id is the backend id |
| Terminate resource | Destroys the droplet |
| Update limits | Resizes the droplet when the limits match an entry in `size_mapping` |
| Add / remove members | **No-op** — droplets have no per-member access in the agent |
| Pause / downscale | Shuts the droplet down |
| Restore | Powers the droplet on |
| Usage reporting | **No-op** — reports nothing |

A forced resource sync does **not** re-create a droplet that was deleted outside Waldur:
a new droplet would get a new id, leaving Waldur pointing at the old one. Terminate the
resource and order a new one instead.

## Configuration

Example configuration for an offering:

```yaml
offerings:
  - name: DigitalOcean VM
    waldur_api_url: https://waldur.example.com/api/
    waldur_api_token: <TOKEN>
    waldur_offering_uuid: <OFFERING_UUID>
    backend_type: digitalocean
    order_processing_backend: digitalocean
    reporting_backend: digitalocean
    membership_sync_backend: digitalocean
    backend_settings:
      token: <DIGITALOCEAN_API_TOKEN>
      default_region: ams3
      default_image: ubuntu-22-04-x64
      default_size: s-1vcpu-1gb
      default_user_data: |
        #cloud-config
        packages:
          - htop
      default_tags:
        - waldur
    backend_components:
      cpu:
        measured_unit: Cores
        unit_factor: 1
        accounting_type: limit
        label: CPU
      ram:
        measured_unit: MiB
        unit_factor: 1
        accounting_type: limit
        label: RAM
      disk:
        measured_unit: MiB
        unit_factor: 1
        accounting_type: limit
        label: Disk
```

### Backend settings

Validated by `waldur_site_agent_digitalocean.schemas.DigitalOceanBackendSettingsSchema`;
a missing `token` is logged as a warning when the agent loads its configuration, and the
backend refuses to start without it.

| Setting | Required | Default | Description |
|---|---|---|---|
| `token` | yes | — | DigitalOcean API token |
| `default_region` | no | — | Region when the order gives none |
| `default_image` | no | — | Image when the order gives none |
| `default_size` | no | — | Size slug when the order gives none |
| `default_user_data` | no | — | Cloud-init user data when the order gives none |
| `default_tags` | no | `[]` | Tags on every droplet |
| `default_ssh_key_id` | no | — | SSH key id when the order gives none |
| `default_ssh_key_fingerprint` | no | — | SSH key fingerprint when the order gives none |
| `default_ssh_key_name` | no | — | Name for a key created from `default_ssh_public_key` |
| `default_ssh_public_key` | no | — | Public key, created in DigitalOcean if missing |
| `size_mapping` | no | `{}` | Size slug → limits, for resizing on limit updates |

A droplet needs a region, an image and a size: an order that gives none of them and no
default fails.

## Resource attributes

You can override defaults per resource using attributes passed from Waldur:

- `region` or `backend_region_id`
- `image` or `backend_image_id`
- `size` or `backend_size_id`
- `user_data` or `cloud_init`
- `ssh_key_id`, `ssh_key_fingerprint`, or `ssh_public_key`
- `ssh_key_name` (optional when using `ssh_public_key`)
- `tags` (list of strings)

If `ssh_public_key` is provided, the plugin will create the key in DigitalOcean
if it does not already exist.

## Resize via limits

To resize droplets from UPDATE orders, you can provide a size mapping:

```yaml
backend_settings:
  size_mapping:
    s-1vcpu-1gb:
      cpu: 1
      ram: 1024
      disk: 25
```

When limits match an entry in `size_mapping`, the droplet will be resized to
the corresponding `size_slug`.

## Tests

```bash
cd plugins/digitalocean && uv run pytest tests/
```
