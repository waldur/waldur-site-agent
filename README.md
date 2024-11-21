# Waldur Site Agent

Agent for Mastermind integration with a provider's site.
The main purpose of the agent is data syncronization between Waldur
and an application (for example SLURM or MOAB cluster).
The agent uses order-related information
from Waldur to manage accounts in the site and
accounting-related info from the site (backend) to update usage data in Waldur.
For now, the agent supports only SLURM and MOAB clusters as a site.

## Architecture

Agent is a stateless application, which is deployed
on a machine having access to a backend data.
It consists of three sub-applications:

- `agent-order-process`, which fetches ordering data from Waldur and updates
  a state of a backend correspondingly;
  (e.g. creates/updates/deletes SLURM accounts);
- `agent-report`, which reports computing usage and limits info from
  a backend to Waldur (e.g. update of resource usages);
- `agent-membership-sync`, which syncs membership info between Waldur
  and a backend (e.g. adds users to a SLURM allocation).

### Integration with Waldur

For this, the agent uses [Waldur client](https://github.com/waldur/python-waldur-client)
based on Python and REST communication with [Waldur backend](https://github.com/waldur/waldur-mastermind).
`Agent-order-process` application pulls data of orders created
in Waldur and creates/updates/removes backend resources based on this info.
`Agent-report` fetches usage data pushes it to Waldur.
`Agent-membership-sync` fetches associations from
a backend and syncronizes it with remote ones.

### Integration with the site

#### SLURM cluster

The agent relies on SLURM command line utilities (e.g. `sacct` and `sacctmgr`)
and should run on a headnode of the SLURM cluster.

#### MOAB cluster

The agent relies on MOAB command line utilities (e.g. `mam-list-accounts` and `mam-create-account`)
and should run on a headnode of the MOAB cluster as a root user.

## Agent configuration

The application supports the following CLI arguments:

- `-m`, `--mode` - mode of agent; supported values:
  `order_process`, `report` and `membership_sync`; default is `order_process`.
- `-c`, `--config-file` - path to the config file with provider settings.

Optional environment variables:

- `WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES` - trigger period for `order_process`
  mode in minutes (default is 5);
- `WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` - trigger period for `report`
  mode in minutes (default is 30);
- `WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES` - trigger period for `membership_sync`
  mode in minutes (default is 5).
- `REQUESTS_VERIFY_SSL` - flag for SSL verification
  for Waldur client, default is `true`.
- `SENTRY_ENVIRONMENT` - name of the Sentry environment.

The primary config for the agent is a `waldur-site-agent-config.yaml`.
Using it, the agent can serve several offerings
and setup backend-related data, for example computing component settings.
File [example](./examples/waldur-site-agent-config.yaml.example) and [reference](#provider-config-file-reference).

**NB:** for MOAB, the only acceptable component is `deposit`.
All other specified components are ignored by the agent.

## Deployment

A user should deploy 3 separate instances of the agent.
The first one (called agent-order-process) fetches data
from Waldur with further processing,
the second one (called agent-report)
sends usage data from the backend to Waldur
and the third one syncs membership information between Waldur and the backend.
All the instances must be configured with CLI variables and provider config file.

To deploy them, you need to setup and
start the systemd services.

### Prerequisite: offering configuration in Waldur

#### SLURM and MOAB

The agents require existing offering data in Waldur.
As a service provider owner, you should create an offering in the marketplace:

- Go to `Service Provider` section of the organization
  and open offering creation menu
- Input a name, choose a category, select `SLURM remote allocation`
  from the drop-down list on the bottom and click `Create` button

![offering-uuid](img/remote-slurm-offering.png)

- Open the offering page and create a plan in the `Accounting`
  section: click `Add plan` and input the necessary details
- Go to `Integration` section, click `Show integration steps`
  and ensure they are completed within your SLURM/MOAB cluster.

### Setup

Firstly, install the `waldur-site-agent` module:

```bash
pip install waldur-site-agent
```

Secondly, create the provider config file and adjust the content for your needs.

```sh
cp examples/waldur-site-agent-config.yaml.example /etc/waldur/waldur-site-agent-config.yaml
```

Please use the `waldur_site_load_components` command
to load computing components into Waldur.

```bash
waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```

Thirdly, put systemd unit
and provider config files to the corresponding locations.

- agent-order-process systemd unit: [waldur-agent-order-process.service](systemd-conf/agent-order-process/agent.service)

- agent-report systemd unit: [waldur-agent-report.service](systemd-conf/agent-report/agent.service)

- agent-membership-sync systemd unit: [waldur-agent-membership-sync.service](systemd-conf/agent-membership-sync/agent.service)

```bash
# For agent-order-process
cp systemd-conf/agent-order-process/agent.service /etc/systemd/system/waldur-agent-order-process.service

# For agent-report
cp systemd-conf/agent-report/agent.service /etc/systemd/system/waldur-agent-report.service

# For agent-membership-sync
cp systemd-conf/agent-membership-sync/agent.service /etc/systemd/system/waldur-agent-membership-sync.service
```

After these preparation steps, run the following script to apply the changes.

```bash
systemctl daemon-reload
systemctl start waldur-agent-order-process.service
systemctl enable waldur-agent-order-process.service # to start after reboot
systemctl start waldur-agent-report.service
systemctl enable waldur-agent-report.service
systemctl start waldur-agent-membership-sync.service
systemctl enable waldur-agent-membership-sync.service
```

### Older systemd versions

If you want to deploy the agents on a machine
with systemd revision older than 240,
you should use files with legacy configuration:

- systemd legacy unit file for agent-pull:
  [waldur-site-agent-pull-legacy.service](systemd-conf/agent-pull/waldur-site-agent-pull-legacy.service)
- systemd legacy unit file for agent-push:
  [waldur-site-agent-push-legacy.service](systemd-conf/agent-push/waldur-site-agent-push-legacy.service)

```bash
# For agent-order-process
cp systemd-conf/agent-order-process/agent-legacy.service /etc/systemd/system/waldur-agent-order-process-legacy.service
# For agent-report
cp systemd-conf/agent-report/agent-legacy.service /etc/systemd/system/waldur-agent-report-legacy.service
# For agent-membership-sync
cp systemd-conf/agent-membership-sync/agent-legacy.service /etc/systemd/system/waldur-agent-membership-sync-legacy.service
```

## Provider config file reference

```yaml
sentry_dsn: "" # Data Source Name for Sentry (more info https://docs.sentry.io/product/sentry-basics/dsn-explainer/).
offerings: # Settings for offerings
  - name: "Example SLURM Offering" # offering name
    waldur_api_url: "http://localhost:8081/api/" # URL of Waldur API (e.g. http://localhost:8081/api/).
    waldur_api_token: "" # Token to access the Waldur API.
    waldur_offering_uuid: "" # UUID of the offering in Waldur.
    backend_type: "slurm" # type of backend, for now only `slurm` and `moab` is supported
    backend_settings: # backend-specific settings
      default_account: "root" # Default parent account name in SLURM cluster
        # for new ones
      customer_prefix: "hpc_" # Prefix for customer's accounts.
      project_prefix: "hpc_" # Prefix for project's accounts.
      allocation_prefix: "hpc_" # Prefix used for allocation's accounts.
        # created by the agent.
      qos_downscaled: "limited" # The QoS set to an account after downscaling
      qos_paused: "paused" # The QoS set to an account after pausing
      qos_default: "normal" # The default QoS for account in the SLURM cluster
      enable_user_homedir_account_creation: true # Whether to create home directories
        # for users associated to accounts.
      homedir_umask: "0700" # A umask for created homedirs
    backend_components: # Computing components on backend with accounting data
      cpu: # Type of the component, for example `cpu`
        measured_unit: "k-Hours" # Waldur measured unit for accounting.
          # For example `k-Hours` for CPU
        unit_factor: 60000 # Factor for conversion from measured unit
          # to backend ones.
          # For example 60000 (60 * 1000) for CPU in SLURM,
          # which uses cpu-minutes for accounting
        accounting_type: "usage" # Can be either `usage` or `limit`
        label: "CPU" # A label for the component in Waldur
      mem:
        limit: 10 # Amount of measured units for Waldur (SLURM measured unit is MB-minutes)
        measured_unit: 'gb-Hours' # Waldur measured unit for accounting
        unit_factor: 61440 # Unit factor for conversion from measured unit
          # to SLURM units (60 * 1024)
        accounting_type: usage # Can be usage or limit
        label: RAM # A label for a component in Waldur
  - name: "Example MOAB Offering"
    waldur_api_url: "http://localhost:8081/api/"
    waldur_api_token: ""
    waldur_offering_uuid: ""
    backend_type: "moab"
    backend_settings:
      default_account: root
      customer_prefix: "hpc_mt_"
      project_prefix: "hpc_mt_"
      allocation_prefix: "hpc_mt_"
      enable_user_homedir_account_creation: true
    backend_components:
      deposit: # For MOAB backend, only "deposit" is supported
        measured_unit: 'EUR'
        accounting_type: limit
        label: Deposit (EUR)
```
