# DigitalOcean plugin for Waldur Site Agent

This plugin integrates Waldur Site Agent with DigitalOcean using the
`python-digitalocean` SDK. It provisions droplets based on marketplace orders
and exposes droplet metadata back to Waldur.

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

Every droplet the agent creates also gets the tag `waldur-resource:<resource uuid, 32 hex
digits without dashes>`. Ordered or default tags starting with `waldur-resource:` are
dropped, so one order cannot claim another resource's droplet, and a droplet carrying
more than one such tag is never adopted.
If an order is retried after the droplet was created but before Waldur recorded
its id, the agent finds the droplet by this tag and adopts it instead of creating
a second one. Two droplets with the same resource tag stop the order with an
error; delete the extra one and the next retry continues. Do not remove the tag
from a droplet whose order has not finished.

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
