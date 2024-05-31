# Agent for Service Provider Integration

Agent for Mastermind integration with a provider's site.
The main purpose of the agent is data syncronization between Waldur instance
and an application (for example SLURM or MOAB cluster).
The application uses order-related information
from Waldur to manage accounts in the site and
accounting-related info from the site to update usage data in Waldur.
For now, the agent supports only SLURM cluster as a site.

## Architecture

This is a stateless application, which is deployed
on a machine having access to SLURM cluster data.
The agent consists of two sub-applications:

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
`Agent-report` fetches data of usage, limits
and associations from a backend and pushes it to Waldur.

### Integration with the site

#### SLURM cluster

For this, the agent uses SLURM command line utilities (e.g. `sacct` and `sacctmgr`).

## Setup

The application supports the following environmental variables
(required ones formatted with bold font):

- **`WALDUR_API_URL`** - URL of Waldur Mastermind API (e.g. `http://localhost:8081/api/`).
- **`WALDUR_API_TOKEN`** - token for access to Mastermind API.
- **`WALDUR_SITE_AGENT_MODE`** - accepts values: `order_process`, `report` and `membership_sync`.
  If `pull`, then application sends data
  from SLURM cluster to Waldur, vice versa if `push`.
- **`WALDUR_OFFERING_UUID`** - UUID of corresponding offering in Waldur.
- `WALDUR_BACKEND_TYPE` - a type of a backend site;
  for now, only `slurm` is supported. Default is `slurm`.
- `REQUESTS_VERIFY_SSL` - flag for SSL verification
  for Waldur client, default is `true`.
- `SLURM_TRES_CONFIG_PATH` - a path to the SLURM TRES
  configuration, default is `./config-components.yaml`.
- `SLURM_CUSTOMER_PREFIX` - prefix used for customer's accounts, default is `hpc_`.
- `SLURM_PROJECT_PREFIX` - prefix used for project's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_PREFIX` - prefix used for allocation's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_NAME_MAX_LEN` - maximum length of account name
  created by the application.
- `SLURM_DEFAULT_ACCOUNT` - default account name existing
  in SLURM cluster for creation of new accounts. Default is `waldur`.
- `ENABLE_USER_HOMEDIR_ACCOUNT_CREATION` - whether to create
  home directories for users related to accounts.
- `SENTRY_DSN` - Data Source Name for Sentry
  (more info [here](https://docs.sentry.io/product/sentry-basics/dsn-explainer/)).
- `SENTRY_ENVIRONMENT` - name of the Sentry environment.

Alternatively, the agent can serve several offerings. For this:

1. Create a config file for offerings with the following format:

    ```yaml
    offerings:
    - name: example-offering-01
      waldur_api_url: https://waldur1.exmaple.com/api/
      waldur_api_token: <token1>
      waldur_offering_uuid: <uuid1>
      backend_type: slurm
    - name: example-offering-02
      waldur_api_url: https://waldur2.exmaple.com/api/
      waldur_api_token: <token2>
      waldur_offering_uuid: <uuid2>
      backend_type: slurm
    ```

2. Add a variable `WALDUR_CONFIG_FILE_PATH` to the environment, for example:

   ```bash
   export WALDUR_CONFIG_FILE_PATH=/etc/waldur-site-agent/offerings.yaml
   ```

**NB**: Environment variables take precedence over the config file,
so if you define `WALDUR_API_URL`, `WALDUR_API_TOKEN`
and `WALDUR_OFFERING_UUID`, the file is ignored.

## Deployment

### Test environment

In order to test the agent, a user should deploy 3 separate instances of it.
The first one (called agent-order-process) fetches data
from Waldur with further processing,
the second one (called agent-report)
sends data from the backend to Waldur
and the third one syncs membership information between Waldur and the backend.
Both instances must be configured with environment variables (from e.g. .env-file),
file for computing components and an optional file for multiple offerings.

The example of `.env-file` for agents:

```env
WALDUR_SITE_AGENT_MODE=order_process # The setup for `agent-order-process`
# Use "report" for `agent-report` or "membership_sync" for `agent-membership-sync`
WALDUR_API_URL=http://waldur.example.com/api/ # Waldur API URL
WALDUR_API_TOKEN=changeme # Token of a service provider in Waldur
WALDUR_OFFERING_UUID=changeme # UUID of SLURM offering in Waldur
SLURM_DEFAULT_ACCOUNT=root # Default account for SLURM
```

### Systemd deployment

In case of native deployment, you need to setup and
run the a systemd service executing Python module.

#### SLURM agent

The agent requires `sacct` and `sacctmgr` to be accessible
on a machine, so it should run on a headnode of the SLURM cluster.
Firstly, install the `waldur-site-agent`:

```bash
pip install waldur-site-agent
```

Secondly, put systemd unit, environment
and TRES config files to the corresponding locations.
Don't forget to modify Waldur-related values the env files.

##### agent-order-process files for the agent

- `systemd unit`: [waldur-agent-order-process.service](systemd-conf/agent-order-process/agent.service)
- `example .env`: [waldur-agent-order-process.env](systemd-conf/agent-order-process/agent.env)

##### agent-report files for the agent

- `systemd unit`: [waldur-agent-report.service](systemd-conf/agent-report/agent.service)
- `example .env`: [waldur-agent-report.env](systemd-conf/agent-report/agent.env)

##### agent-membership-sync files for the agent

- `systemd unit`: [waldur-agent-membership-sync.service](systemd-conf/agent-membership-sync/agent.service)
- `example .env`: [waldur-agent-membership-sync.env](systemd-conf/agent-membership-sync/agent.env)

##### Common files

- [example of a file for config components](https://github.com/waldur/waldur-site-agent/blob/main/config-components.yaml.example)

```bash
mkdir /etc/waldur-site-agent/
# you can use a different path and set SLURM_TRES_CONFIG_PATH to it
cp ./config-components.yaml.example /etc/waldur-site-agent/tres.yaml

# For agent-order-process
cp systemd-conf/agent-order-process/agent.service /etc/systemd/system/waldur-agent-order-process.service
cp systemd-conf/agent-order-process/waldur-agent-order-process.env /etc/waldur-site-agent/agent-order-process.env

# For agent-report
cp systemd-conf/agent-report/agent.service /etc/systemd/system/waldur-agent-report.service
cp systemd-conf/agent-report/waldur-agent-report.env /etc/waldur-site-agent/agent-report.env

# For agent-membership-sync
cp systemd-conf/agent-membership-sync/agent.service /etc/systemd/system/waldur-agent-membership-sync.service
cp systemd-conf/agent-membership-sync/waldur-agent-membership-sync.env /etc/waldur-site-agent/agent-membership-sync.env
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

##### Older systemd versions

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

### Compute components configuration

#### SLURM TRES

To setup TRES-related info, the agent uses the corresponding
configuration file configured by `SLURM_TRES_CONFIG_PATH` environment variable
(`config-components.yaml` by default).
Each entry of the file incudes key-value-formatted data.
A key is a type of TRES (with optional name if type is `gres`)
and the value contains limit, measured unit, type of accounting and label.
The script `waldur_slurm_load_components` sends this data to Waldur:

```bash
waldur_slurm_load_components
```

If a user wants to change this information,
a path of a custom config file should be set
for `SLURM_TRES_CONFIG_PATH` variable.

## Service provider configuration

### SLURM

The agents require existing offering data in Waldur.
As a service provider owner, you should create an offering in the marketplace:

- Go to `Provider` section on the left tab -> `Add new offering` button
- Input a name, choose a category, select `SLURM remote allocation`
  from the drop-down list on the bottom and click `Create` button

![offering-uuid](img/remote-slurm-offering.png)

- Open the offering page and create a plan in the `Accounting`
  section: click `Add plan` and input the necessary details
- Go to `Integration` section, click `Show integration steps`
  and ensure they are completed within your SLURM cluster.
