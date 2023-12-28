# Waldur SLURM Integration Service

Service for Mastermind integration with SLURM cluster. The main purpose of the service is data syncronization between Waldur instance and SLURM cluster. The application uses order-related information from Waldur to manage accounts in SLURM and accounting-related info from SLURM to update usage data in Waldur.

## Architecture

This is a stateless application, which should be deployed on a machine having access to SLURM cluster data. The service consists of two sub-applications:

- service-pull, which fetches data from Waldur and updates a state of a SLURM cluster correspondingly (e.g. creation of SLURM accounts ordered in Waldur)
- service-push, which sends data from SLURM cluster to Waldur (e.g. update of resource usages)

### Integration with Waldur

For this, the service uses [Waldur client](https://github.com/waldur/python-waldur-client) based on Python and REST communication with [Waldur backend](https://github.com/waldur/waldur-mastermind). Service-pull application pulls orders data created for a specific offering linked to SLURM cluster and creates/updates/removes SLURM accounts based on the data. Service-push fetches data of usage, limits and associations from SLURM cluster and pushes it to Waldur.

### Integration with SLURM cluster

For this, service uses uses SLURM command line utilities (e.g. `sacct` and `sacctmgr`). The access to the binaries can be either direct or using docker client. In the latter case, the service is required to have access to `docker` binary and to docker socket (e.g. `/var/run/docker.sock`).

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
- `SENTRY_DSN` - Data Source Name for Sentry (more info [here](https://docs.sentry.io/product/sentry-basics/dsn-explainer/))
- `ENABLE_USER_HOMEDIR_ACCOUNT_CREATION` - whether to create home directories for users related to accounts

## Deployment

### Test environment

In order to test the service, a user should deploy 2 separate instances of the service.
The first one (called service-pull) is for fetching data from Waldur with further processing and the second one (called service-push) is for sending data from SLURM cluster to Waldur.
Both instances must be configured with environment variables from e.g. .env-file.

The example of .env-file for service-pull:

```env
WALDUR_SYNC_DIRECTION=pull # The setup for service-pull
WALDUR_API_URL=http://waldur.example.com/api/ # Waldur API URL
WALDUR_API_TOKEN=9e1132b9616ebfe943ddf632ca32bbb7e1109a32 # Token of a service provider in Waldur
WALDUR_OFFERING_UUID=e21a0f0030b447deb63bedf69db6742e # UUID of SLURM offering in Waldur
SLURM_DEFAULT_ACCOUNT=root # Default account for SLURM
SLURM_CONTAINER_NAME=slurmctld # Name of SLURM namenode container
```

The example of .env-file for service-push:

```env
WALDUR_SYNC_DIRECTION=push # The setup for service-push
WALDUR_API_URL=http://waldur.example.com/api/ # Waldur API URL
WALDUR_API_TOKEN=9e1132b9616ebfe943ddf632ca32bbb7e1109a32 # Token of a service provider in Waldur
WALDUR_OFFERING_UUID=e21a0f0030b447deb63bedf69db6742e # UUID of SLURM offering in Waldur
SLURM_CONTAINER_NAME=slurmctld # Name of SLURM namenode container
```

### Docker-based deployment

You can find the Docker Compose configuration for testing in `examples/docker-compose/` folder.

In order to test it, you need to execute following commands in your terminal app:

```bash
cd examples/docker-compose
docker-compose up -d
```

### Systemd deployment

If your SLURM cluster doesn't run in Docker, you need to deploy the a systemd service starting using Python module.
The agent requires `sacct` and `sacctmgr` to be accessible on a machine, so it should run on a headnode of the SLURM cluster.
Firstly, install the waldur-slurm-agent:

```bash
pip install waldur-slurm-agent
```

Secondly, put systemd unit, environment and and TRES config files to the corresponding locations.
Don't forget to modify Waldur-related values the env files.

```bash
# For pulling service
cp systemd-conf/service-pull/waldur-slurm-service-pull.service /etc/systemd/system/
mkdir /etc/waldur-slurm-service/
cp systemd-conf/service-pull/waldur-slurm-service-pull.env /etc/waldur-slurm-service/pull.env
cp ./config-components.yaml /etc/waldur-slurm-service/tres.yaml # you can use a different path and set SLURM_TRES_CONFIG_PATH to it


# For pushing service
cp systemd-conf/service-push/waldur-slurm-service-push.service /etc/systemd/system/
cp systemd-conf/service-push/waldur-slurm-service-push.env /etc/waldur-slurm-service/push.env
```

After the preparation, run the following to apply the changes.

```bash
systemctl daemon-reload
systemctl start waldur-slurm-service-pull
systemctl enable waldur-slurm-service-pull # to start after reboot
systemctl start waldur-slurm-service-push
systemctl enable waldur-slurm-service-push # to start after reboot
```

#### Older systemd versions

If you want to deploy the services on a machine with systemd revision older than 240, you should use files with legacy configuration:

```bash
# For pulling service
cp systemd-conf/service-pull/waldur-slurm-service-pull-legacy.service /etc/systemd/system/waldur-slurm-service-pull.service
# For pushing service
cp systemd-conf/service-push/waldur-slurm-service-push-legacy.service /etc/systemd/system/waldur-slurm-service-push.service
```

### TRES configuration

To setup TRES-related info, the service uses the corresponding configuration file configured by `SLURM_TRES_CONFIG_PATH` environment variable (`config-components.yaml` by default). Each entry of the file incudes key-value-formatted data.
A key is a type of TRES (with optional name if type is `gres`) and the value contains limit, measured unit, type of accounting and label.
The service sends this data to Waldur when you run it with `--load-components`:

```bash
python3 -m waldur_slurm.main --load-components
```

If a user wants to change this information, a custom config file should be mounted into a container and set `SLURM_TRES_CONFIG_PATH` value to a correct location.

## Service provider configuration

The services require existing offering in Waldur.
As a service provider owner, you should create an offering in the marketplace:

- Go to `Provider` section on the left tab -> `Add new offering` button
- Input a name, choose a category, select `SLURM remote allocation` from the drop-down list on the bottom and click `Create` button

![offering-uuid](img/remote-slurm-offering.png)

- Open the offering page and create a plan in the `Accounting` section: click `Add plan` and input the necessary details
- Go to `Integration` section, click `Show integration steps` and ensure they are completed within your SLURM cluster.
