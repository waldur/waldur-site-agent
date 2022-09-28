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
- `SLURM_DEPLOYMENT_TYPE` - type of SLURM deployment. accepts two values: `docker` and `native`, default is `docker`.
- `SLURM_CUSTOMER_PREFIX` - prefix used for customer's accounts, default is `hpc_`.
- `SLURM_PROJECT_PREFIX` - prefix used for project's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_PREFIX` - prefix used for allocation's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_NAME_MAX_LEN` - maximum length of account name created by the application.
- `SLURM_DEFAULT_ACCOUNT` - default account name existing in SLURM cluster for creation of new accounts. Default is `waldur`.
- `SLURM_CONTAINER_NAME` - name of a headnode SLURM container; must be set if SLURM_DEPLOYMENT_TYPE is docker.
- `WALDUR_SLURM_USERNAME_SOURCE` - source of SLURM username in Waldur. It can be either `freeipa` or `local`, default is `local`.

## Deployment

### Test environment

In order to test the service, a user should deploy 2 separate instances of the service. The first one (called service-pull) is for fetching data from Waldur with further processing and the second one (called service-push) is for sending data from SLURM cluster to Waldur. Both instances must be configured with environment variables from e.g. .env-file.g

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

The current revision of the project supports only Docker-based deployment. You can find the Docker Compose configuration for testing in `examples/docker-compose/` folder.

In order to test it, you need to execute following commands in your terminal app:

```bash
cd examples/docker-compose
docker-compose up -d
```

### TRES configuration

To setup TRES-related info, the service uses the corresponding configuration file `config-components.yaml` in the root directory. Each entry of the file incudes key-value-formatted data.
A key is a type of TRES (with optional name if type is gres) and the value contains limit, measured unit, type of accounting and label.
The service sends this data to Waldur each time when it is restarted.
If a user wants to change this information, a custom config file should be mounted into a container.
