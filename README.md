# Waldur SLURM Integration Service

Service for Mastermind integration with SLURM cluster

## Setup

The application supports the following environmental variables (required ones formatted with bold font):

- **`WALDUR_API_URL`** - URL of Waldur Mastermind API (e.g. `http://localhost:8081/api/`).
- **`WALDUR_API_TOKEN`** - token for access to Mastermind API.
- **`WALDUR_SYNC_DIRECTION`** - accepts two values: `push` and `pull`. If `pull`, then application sends data from SLURM cluster to Waldur, vice versa if `push`.
- `WALDUR_OFFERING_UUID` - UUID of corresponding offering in Waldur, mandatory only if `WALDUR_SYNC_DIRECTION` is `pull`.
- `SLURM_DEPLOYMENT_TYPE` - type of SLURM deployment. accepts two values: `docker` and `native`, default is `docker`.
- `SLURM_CUSTOMER_PREFIX` - prefix used for customer's accounts, default is `hpc_`.
- `SLURM_PROJECT_PREFIX` - prefix used for project's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_PREFIX` - prefix used for allocation's accounts, default is `hpc_`.
- `SLURM_ALLOCATION_NAME_MAX_LEN` - maximum length of account name created by the application.
- `SLURM_DEFAULT_CPU_LIMIT` - CPU resource limit for allocation if not specified by Waldur. Default is `16000`.
- `SLURM_DEFAULT_GPU_LIMIT` - GPU resource limit for allocation if not specified by Waldur. Default is `400`.
- `SLURM_DEFAULT_RAM_LIMIT` - RAM resource limit for allocation if not specified by Waldur. Default is `100000 * 2 ** 10`.
- `SLURM_DEFAULT_ACCOUNT` - default account name existing in SLURM cluster for creation of new accounts. Default is `waldur`.
