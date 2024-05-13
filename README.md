# Agent for Service Provider Integration

Agent for Mastermind integration with a provider's site.
The main purpose of the agent is data syncronization between Waldur instance and an application (for example SLURM or MOAB cluster).
The application uses order-related information from Waldur to manage accounts in the site and
accounting-related info from the site to update usage data in Waldur.
For now, the agent supports only SLURM cluster as a site.

## Architecture

This is a stateless application, which is deployed on a machine having access to SLURM cluster data.
The agent consists of two sub-applications:

- agent-pull, which fetches data from Waldur and updates a state of a SLURM cluster correspondingly (e.g. creation of SLURM accounts ordered in Waldur);
- agent-push, which sends data from SLURM cluster to Waldur (e.g. update of resource usages).

### Integration with Waldur

For this, the agent uses [Waldur client](https://github.com/waldur/python-waldur-client) based on Python and REST communication with [Waldur backend](https://github.com/waldur/waldur-mastermind). `Agent-pull` application pulls data of orders created for a specific offering linked to the site and creates/updates/removes SLURM accounts based on this info. `Agent-push` fetches data of usage, limits and associations from the site and pushes it to Waldur.

### Integration with the site

#### SLURM cluster

For this, the agent uses SLURM command line utilities (e.g. `sacct` and `sacctmgr`).
The access to the binaries can be either direct or using docker client.
In the latter case, the agent is required to have access to `docker` binary and to docker socket (e.g. `/var/run/docker.sock`).

## Setup

The application supports the following environmental variables (required ones formatted with bold font):

- **`WALDUR_API_URL`** - URL of Waldur Mastermind API (e.g. `http://localhost:8081/api/`).
- **`WALDUR_API_TOKEN`** - token for access to Mastermind API.
- **`WALDUR_SYNC_DIRECTION`** - accepts two values: `push` and `pull`. If `pull`, then application sends data from SLURM cluster to Waldur, vice versa if `push`.
- **`WALDUR_OFFERING_UUID`** - UUID of corresponding offering in Waldur.
- `REQUESTS_VERIFY_SSL` - flag for SSL verification for Waldur client, default is `true`.
- `SLURM_TRES_CONFIG_PATH` - a path to the SLURM TRES configuration, default is `./config-components.yaml`.
- `SLURM_DEPLOYMENT_TYPE` - type of SLURM deployment. accepts two values: `docker` and `native`, default is `docker`.
- `SLURM_CUSTOMER_PREFIX` - prefix used for customer's accounts, default is `hpc_`.
- `SLURM_PROJECT_PREFIX` - prefix used for project's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_PREFIX` - prefix used for allocation's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_NAME_MAX_LEN` - maximum length of account name created by the application.
- `SLURM_DEFAULT_ACCOUNT` - default account name existing in SLURM cluster for creation of new accounts. Default is `waldur`.
- `SLURM_CONTAINER_NAME` - name of a headnode SLURM container; must be set if SLURM_DEPLOYMENT_TYPE is docker.
- `ENABLE_USER_HOMEDIR_ACCOUNT_CREATION` - whether to create home directories for users related to accounts.
- `SENTRY_DSN` - Data Source Name for Sentry (more info [here](https://docs.sentry.io/product/sentry-basics/dsn-explainer/)).
- `SENTRY_ENVIRONMENT` - name of the Sentry environment.

Alternatively, the agent can serve several offerings. For this:

1. Create a config file for offerings with the following format:

    ```yaml
    offerings:
    - name: example-offering-01
      waldur_api_url: https://waldur1.exmaple.com/api/
      waldur_api_token: <token1>
      waldur_offering_uuid: <uuid1>
    - name: example-offering-02
      waldur_api_url: https://waldur2.exmaple.com/api/
      waldur_api_token: <token2>
      waldur_offering_uuid: <uuid2>
    ```

2. Add a variable `WALDUR_CONFIG_FILE_PATH` to the environment, for example:

   ```bash
   export WALDUR_CONFIG_FILE_PATH=/etc/waldur-site-agent/offerings.yaml
   ```

**NB**: Environment variables take precedence over the config file, so if you define `WALDUR_API_URL`, `WALDUR_API_TOKEN` and `WALDUR_OFFERING_UUID`, the file is ignored.

## Deployment

### Test environment

In order to test the agent, a user should deploy 2 separate instances of it.
The first one (called agent-pull) is for fetching data from Waldur with further processing and the second one (called agent-push) is for sending data from SLURM cluster to Waldur.
Both instances must be configured with environment variables (from e.g. .env-file), file for computing components and an optional file for multiple offerings.

The example of `.env-file` for agent-pull:

```env
WALDUR_SYNC_DIRECTION=pull # The setup for agent-pull
WALDUR_API_URL=http://waldur.example.com/api/ # Waldur API URL
WALDUR_API_TOKEN=9e1132b9616ebfe943ddf632ca32bbb7e1109a32 # Token of a service provider in Waldur
WALDUR_OFFERING_UUID=e21a0f0030b447deb63bedf69db6742e # UUID of SLURM offering in Waldur
SLURM_DEFAULT_ACCOUNT=root # Default account for SLURM
SLURM_CONTAINER_NAME=slurmctld # Name of SLURM namenode container
```

The example of .env-file for agent-push:

```env
WALDUR_SYNC_DIRECTION=push # The setup for agent-push
WALDUR_API_URL=http://waldur.example.com/api/ # Waldur API URL
WALDUR_API_TOKEN=9e1132b9616ebfe943ddf632ca32bbb7e1109a32 # Token of a service provider in Waldur
WALDUR_OFFERING_UUID=e21a0f0030b447deb63bedf69db6742e # UUID of SLURM offering in Waldur
SLURM_CONTAINER_NAME=slurmctld # Name of SLURM namenode container
```

### Docker-based deployment

You can find the Docker Compose configuration for testing in [examples/docker-compose/](examples/docker-compose/) folder:

- [docker-compose.yml](examples/docker-compose/docker-compose.yml)
- [agent-pull](examples/docker-compose/waldur-agent-pull-env)
- [agent-push](examples/docker-compose/waldur-agent-push-env)

In order to test it, you need to execute following commands in your terminal app:

```bash
cd examples/docker-compose
docker-compose up -d
```

### Systemd deployment

In case of native deployment, you need to setup and run the a systemd service executing Python module.

#### SLURM agent

The agent requires `sacct` and `sacctmgr` to be accessible on a machine, so it should run on a headnode of the SLURM cluster.
Firstly, install the waldur-site-agent:

```bash
pip install waldur-site-agent
```

Secondly, put systemd unit, environment and and TRES config files to the corresponding locations.
Don't forget to modify Waldur-related values the env files.

##### agent-pull files for a SLURM agent

- `systemd unit`: [waldur-site-agent-pull.service](systemd-conf/agent-pull/waldur-site-agent-pull.service)
- `example .env`: [waldur-site-agent-pull.env](systemd-conf/agent-pull/waldur-site-agent-pull.env)

##### agent-push files for a SLURM agent

- `systemd unit`: [waldur-site-agent-push.service](systemd-conf/agent-push/waldur-site-agent-push.service)
- `example .env`: [waldur-site-agent-push.env](systemd-conf/agent-push/waldur-site-agent-push.env)

#### Common files

- [example of a file for config components](https://github.com/waldur/waldur-site-agent/blob/main/config-components.yaml.example)

```bash
# For agent-pull
cp systemd-conf/agent-pull/waldur-site-agent-pull.service /etc/systemd/system/
mkdir /etc/waldur-site-agent/
cp systemd-conf/agent-pull/waldur-site-agent-pull.env /etc/waldur-site-agent/pull.env
cp ./config-components.yaml.example /etc/waldur-site-agent/tres.yaml # you can use a different path and set SLURM_TRES_CONFIG_PATH to it


# For agent-push
cp systemd-conf/agent-push/waldur-site-agent-push.service /etc/systemd/system/
cp systemd-conf/agent-push/waldur-site-agent-push.env /etc/waldur-site-agent/push.env
```

After these preparation steps, run the following script to apply the changes.

```bash
systemctl daemon-reload
systemctl start waldur-site-agent-pull
systemctl enable waldur-site-agent-pull # to start after reboot
systemctl start waldur-site-agent-push
systemctl enable waldur-site-agent-push # to start after reboot
```

#### Older systemd versions

If you want to deploy the agents on a machine with systemd revision older than 240, you should use files with legacy configuration:

- systemd legacy unit file for agent-pull: [waldur-site-agent-pull-legacy.service](systemd-conf/agent-pull/waldur-site-agent-pull-legacy.service)
- systemd legacy unit file for agent-push: [waldur-site-agent-push-legacy.service](systemd-conf/agent-push/waldur-site-agent-push-legacy.service)

```bash
# For pulling agent
cp systemd-conf/agent-pull/waldur-site-agent-pull-legacy.service /etc/systemd/system/waldur-site-agent-pull.service
# For pushing agent
cp systemd-conf/agent-push/waldur-site-agent-push-legacy.service /etc/systemd/system/waldur-site-agent-push.service
```

### TRES configuration

To setup TRES-related info, the agent uses the corresponding configuration file configured by `SLURM_TRES_CONFIG_PATH` environment variable (`config-components.yaml` by default). Each entry of the file incudes key-value-formatted data.
A key is a type of TRES (with optional name if type is `gres`) and the value contains limit, measured unit, type of accounting and label.
The script `waldur_slurm_load_components` sends this data to Waldur:

```bash
waldur_slurm_load_components
```

If a user wants to change this information, a path of a custom config file should be set for `SLURM_TRES_CONFIG_PATH` variable.

## Service provider configuration

### SLURM

The agents require existing offering data in Waldur.
As a service provider owner, you should create an offering in the marketplace:

- Go to `Provider` section on the left tab -> `Add new offering` button
- Input a name, choose a category, select `SLURM remote allocation` from the drop-down list on the bottom and click `Create` button

![offering-uuid](img/remote-slurm-offering.png)

- Open the offering page and create a plan in the `Accounting` section: click `Add plan` and input the necessary details
- Go to `Integration` section, click `Show integration steps` and ensure they are completed within your SLURM cluster.
