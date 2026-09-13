# Azure plugin for Waldur Site Agent

Provisions and meters Microsoft Azure virtual machines through the Azure
Resource Manager SDKs, for a Site Agent offering (`Marketplace.Slurm`). Azure
credentials and the Azure client libraries live at the provider site, with the
agent.

A create order yields a running machine and a terminate order deletes it.
Pausing and downscaling deallocate it, restoring starts it again. There are no
start, stop or restart actions.

## Provisioning a machine

An order creates, in this order: a resource group, a virtual network, a subnet, a
public IP address, a network interface, and the machine itself. The agent keeps
no state, so the chain runs inline and everything needed afterwards is derived
from the resource name or read out of the ARM id Azure returns.

Unless `default_resource_group` names a shared group, each machine gets a group
of its own — which is also how deletion stays complete: dropping the group takes
the network objects with it. In a shared group each object is deleted separately,
in reverse order of creation, and the machine name carries the Waldur resource's
uuid so that two resources whose names sanitize alike cannot address one machine.

Which of the two routes a deletion takes is read from the group's own name, not
from the current setting: changing `default_resource_group` after resources exist
must not turn a later terminate order into the deletion of a shared group.

A chain that fails part-way is rolled back along the same path: Waldur only
records a machine's id once the machine exists, so anything a failed order left
behind would bill with nothing in Waldur pointing at it. The names are derived
from the Waldur resource's uuid, so an order retried after the agent was killed
mid-create addresses the same objects instead of building a second set. The
machine's OS disk is created with Azure's delete option set, because deleting a
machine otherwise leaves its managed disk.

The machine is reachable over SSH when two things hold: the order carries a key,
either as `ssh_public_key` or as `ssh_key` holding the UUID of a service provider
key, and the offering names the addresses that may connect in
`allowed_ssh_ranges`. A public address of the Standard SKU — which is what these
machines get — admits no inbound traffic on its own, so a security group is what
opens the port, and it is created only for the ranges configured. Nothing is
opened to the whole internet by default.

An administrator password is generated to satisfy Azure and is never reported
anywhere, so the key is the only way in. An order that carries none — or names a
key the service provider has not registered — is refused before anything is
created, rather than provisioned into a machine that bills and admits nobody.

Pausing and downscaling are staff actions in Waldur, offered once the
offering's `supports_pausing` and `supports_downscaling` plugin options are set.
Both deallocate the machine rather than powering it off: a powered-off machine
keeps its host reserved and keeps billing the full compute rate, so only
deallocation does what these actions exist for. The disks bill either way.
Restoring the resource starts the machine.

## What is not implemented

Resizing: `set_resource_limits` refuses, so a plan change that would move a
machine to another size is not carried out on Azure. Membership calls do nothing
and report as much: access is the SSH key the order carried, so there is no
membership on the backend to add anyone to.

## Metering

Reports allocation, not consumption: the cores, memory and disk of the size a
machine runs at. Azure charges for those for as long as the machine exists. The
Azure Consumption API is not read.

Figures are mapped onto the offering's components by name — `cpu`/`cores`,
`ram`/`memory`, `disk`/`storage` — or by an explicit `backend_name` on the
component, and divided by its `unit_factor`. A component this plugin cannot see
is left out rather than reported as zero, as is a whole resource whose shape
Azure would not return. Zero is a legitimate usage value and the core writes it
straight through.

## Clients

`clients/` holds one module per Azure service, each building its SDK client on
first use, so a client a run never touches costs nothing:

| Module | Covers |
| ------ | ------ |
| `resource.py` | Subscription reachability, regions, resource groups |
| `compute.py` | Machine sizes, availability zones, images, virtual machines, disks |
| `network.py` | Networks, subnets, interfaces, public IPs, SSH security group |

## Configuration

```yaml
offerings:
  - name: Azure VM
    waldur_api_url: https://waldur.example.com/api/
    waldur_api_token: <TOKEN>
    waldur_offering_uuid: <OFFERING_UUID>
    backend_type: azure
    order_processing_backend: azure
    reporting_backend: azure
    backend_settings:
      subscription_id: <SUBSCRIPTION_ID>
      tenant_id: <TENANT_ID>
      client_id: <CLIENT_ID>
      client_secret: <CLIENT_SECRET>
      default_location: westeurope
      default_size: Standard_B1s
      default_image: Canonical:0001-com-ubuntu-server-jammy:22_04-lts:latest
      # Without any range a machine accepts no SSH connections.
      allowed_ssh_ranges:
        - 203.0.113.0/24
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

| Setting | Required | Description |
| ------- | -------- | ----------- |
| `subscription_id` | yes | Azure subscription the resources are created in |
| `tenant_id` | yes | Azure AD tenant of the service principal |
| `client_id` | yes | Service principal client ID |
| `client_secret` | yes | Service principal client secret |
| `default_location` | no | Region used when an order does not name one |
| `default_resource_group` | no | Resource group to place resources in; created on demand when unset |
| `default_size` | no | Virtual machine size used when an order does not name one |
| `default_image` | no | Image as `publisher:offer:sku:version` |
| `network_cidr` | no | Address space of the virtual network (default `10.0.0.0/16`) |
| `subnet_cidr` | no | Address range of the subnet (default `10.0.0.0/24`) |
| `allowed_ssh_ranges` | no | CIDR prefixes allowed to reach machines over SSH; without any, none |

## Offerings taken over from mastermind

After the mastermind upgrade, former Azure offerings are Site Agent offerings
with their resources intact, and mastermind no longer calls Azure. Run the agent
with the offering's UUID and the service principal credentials. The four
credential settings carry the same names as the options of mastermind's former
Azure service settings; the upgrade deletes those settings, so copy the values
before upgrading.

The machines keep their names and ARM ids, and the naming scheme and default
network ranges are the ones this plugin uses, so terminating and pausing work
on them.

## Order attributes

The provider defines these as the offering's order options — `location`,
`size` and `image` typically as select options, `ssh_public_key` as text — so
that the order form asks for them. Each overrides the offering default for one
machine:

| Attribute | Meaning |
| --------- | ------- |
| `location` | Azure region |
| `size` | Machine size, e.g. `Standard_B1s` |
| `image` | `publisher:offer:sku:version`, or the four fields as an object |
| `ssh_public_key` | SSH key text |
| `ssh_key` | UUID of a service provider SSH key, resolved by the agent |
| `user_data` / `cloud_init` | cloud-init payload |
| `username` | Administrator name (default `waldur`) |

## Tests

```bash
cd plugins/azure && uv run pytest tests/
```
