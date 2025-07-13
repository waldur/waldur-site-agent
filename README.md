# Waldur Site Agent

Project for Waldur integration with a service provider's site.
The main purpose of the agent is data syncronization between Waldur
and a service backend (for example SLURM or MOAB cluster).
The agent uses order information from Waldur
to manage accounts in the site (backend) and accounting info
from the site to update usage data in Waldur.
For now, the agent supports only SLURM and MOAB clusters as a service backend.

## Architecture

Agent is a stateless application, which is deployed
on a machine with access to backend data.
It supports 4 modes:

- `agent-order-process`, which fetches ordering data from Waldur and updates
  a state of backend object correspondingly;
  (e.g. creates/updates/deletes SLURM accounts);
- `agent-report`, which reports computing usage and limits info from
  the backend to Waldur (e.g. update of resource usages);
- `agent-membership-sync`, which syncronizes membership info between Waldur
  and the backend (e.g. adds users to a SLURM allocation);
- `agent-event-process`, which uses event-based approach to do the same as
  `agent-order-process` and `agent-membership-sync`; requires either MQTT- or STOMP-plugin
  as an event delivery system between Waldur and the agent.

Code-wise, the agent consists of the main python module called `waldur-site-agent`
and set of plugins with implementation for different backends
(for example, `waldur-site-agent-slurm`).
The main module contains the configuration for running the
application, shared utils and abstract classes for backends.
A plugin should contain implementation of the abstract classes exposing them
via entry-points in the respecting `pyproject.toml` file.
Each plugin depends on the main module,
while this module uses plugin classes via entry-points.

### Core architecture & plugin system

```mermaid
---
config:
  layout: elk
---
graph TB
    subgraph "Core Package"
        WA[waldur-site-agent<br/>Core Logic & Processing]
        BB[BaseBackend<br/>Abstract Interface]
        BC[BaseClient<br/>Abstract Interface]
        CU[Common Utils<br/>Entry Point Discovery]
    end

    subgraph "Plugin Ecosystem"
        PLUGINS[Backend Plugins<br/>SLURM, MOAB, MUP, etc.]
        UMANAGE[Username Management<br/>Plugins]
    end

    subgraph "Entry Point System"
        EP_BACKENDS[waldur_site_agent.backends]
        EP_USERNAME[waldur_site_agent.username_management_backends]
    end

    %% Core dependencies
    WA --> BB
    WA --> BC
    WA --> CU

    %% Plugin registration and discovery
    CU --> EP_BACKENDS
    CU --> EP_USERNAME
    EP_BACKENDS -.-> PLUGINS
    EP_USERNAME -.-> UMANAGE

    %% Plugin inheritance
    PLUGINS -.-> BB
    PLUGINS -.-> BC
    UMANAGE -.-> BB

    %% Styling
    classDef corePackage fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef plugin fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef entrypoint fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px

    class WA,BB,BC,CU corePackage
    class PLUGINS,UMANAGE plugin
    class EP_BACKENDS,EP_USERNAME entrypoint
```

### Agent modes & external systems

```mermaid
---
config:
  layout: elk
---
graph TB
    subgraph "Agent Modes"
        ORDER[agent-order-process<br/>Order Processing]
        REPORT[agent-report<br/>Usage Reporting]
        SYNC[agent-membership-sync<br/>Membership Sync]
        EVENT[agent-event-process<br/>Event Processing]
    end

    subgraph "Plugin Layer"
        PLUGINS[Backend Plugins<br/>SLURM, MOAB, MUP, etc.]
    end

    subgraph "External Systems"
        WALDUR[Waldur Mastermind<br/>REST API]
        BACKENDS[Cluster Backends<br/>CLI/API Systems]
        MQTT[MQTT/STOMP Broker<br/>Event Processing]
    end

    %% Agent mode usage of plugins
    ORDER --> PLUGINS
    REPORT --> PLUGINS
    SYNC --> PLUGINS
    EVENT --> PLUGINS

    %% External connections
    ORDER <--> WALDUR
    REPORT <--> WALDUR
    SYNC <--> WALDUR
    EVENT <--> WALDUR
    EVENT <--> MQTT
    PLUGINS <--> BACKENDS

    %% Styling
    classDef agent fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    classDef plugin fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef external fill:#fff3e0,stroke:#e65100,stroke-width:2px

    class ORDER,REPORT,SYNC,EVENT agent
    class PLUGINS plugin
    class WALDUR,BACKENDS,MQTT external
```

### Key plugin features

- **Automatic Discovery**: Plugins are automatically discovered via Python entry points
- **Modular Backends**: Each backend (SLURM, MOAB, MUP, etc) is a separate plugin package
- **Independent Versioning**: Plugins can be versioned and distributed separately
- **Extensible**: External developers can create custom backends by implementing `BaseBackend`
- **Multi-Backend Support**: Different backends for order processing, reporting, and membership sync

### Built-in plugin structure

```text
plugins/{backend_name}/
├── pyproject.toml              # Entry point registration
├── waldur_site_agent_{name}/   # Plugin implementation
│   ├── backend.py             # Backend class inheriting BaseBackend
│   ├── client.py              # Client for external system communication
│   └── parser.py              # Data parsing utilities (optional)
└── tests/                     # Plugin-specific tests
```

### Integration with Waldur

The agent uses a [Python-based Waldur client](https://github.com/waldur/python-waldur-client)
communicating with [Waldur backend](https://github.com/waldur/waldur-mastermind)
via REST interface.
`Agent-order-process` application pulls data of orders created
in Waldur and creates/updates/removes backend resources based on this info.
`Agent-report` fetches usage data pushes it to Waldur.
`Agent-membership-sync` fetches associations from
a backend and syncronizes it with remote ones.
`Agent-event-process` manages Waldur orders and membership in event-based way.

### Integration with the site

#### SLURM cluster

The agent relies on SLURM command-line utilities (e.g. `sacct` and `sacctmgr`)
and should run on a headnode of the SLURM cluster.

#### MOAB cluster

The agent relies on MOAB command line utilities (e.g. `mam-list-accounts` and `mam-create-account`)
and should run on a headnode of the MOAB cluster as a root user.

## Agent configuration

The application supports the following CLI arguments:

- `-m`, `--mode` - mode of agent; supported values:
  `order_process`, `report`, `membership_sync` and `event_process`; default is `order_process`.
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

The main config source for the agent is `waldur-site-agent-config.yaml` file.
Using it, the agent can serve multiple offerings
and setup backend-related data, for example settings of computing components.
File [example](./examples/waldur-site-agent-config.yaml.example) and [reference](#provider-config-file-reference).

**NB:** for MOAB, the only acceptable backend component is `deposit`.
All other specified components are ignored by the agent.

**NB:** The `timezone` setting is important when agent and Waldur are deployed in
different timezones, this setting can be used to prevent billing period mismatches
at month boundaries. Recommended: `timezone: "UTC"`.

## Deployment

A user can deploy 4 separate instances of the agent.
The first one (called agent-order-process) fetches data
from Waldur with further processing,
the second one (called agent-report)
sends usage data from the backend to Waldur
the third one syncs membership information between Waldur
and the backend (called agent-membership-sync) and
the optional fourth one processes order and membership info in event-based way (agent-event-process).
The last one covers the same functionality as the first two services,
but uses event bus for integration with Waldur.
All the instances must be configured with provider config file and CLI variables.

To deploy them, you need to setup and
start the systemd services.

**Note**: only one of these service combinations is possible:

1. agent-order-process, agent-membership-sync and agent-report
2. agent-event-process and agent-report

### Prerequisite: offering configuration in Waldur

#### SLURM and MOAB

Agents require a pre-created offering in Waldur.
As a service provider owner, you should create a new offering in the marketplace:

- Go to `Service Provider` section of the organization
  and open offering creation menu
- Input a name, choose a category, select `Waldur site agent`
  from the drop-down list on the bottom and click `Create` button

![offering-creation](img/offering-creation.png)

- Open the offering page, choose `Edit` tab, click `Accounting` section,
  choose `Accounting plans` from the drop-down list and create a plan:
  click `Add plan` and input the necessary details;

![offering-plan](img/offering-plan.png)

- In the same page, click `Integration` section choose `User management`
  from the drop-down list and set the
  `Service provider can create offering user` option to `Yes`;

![offering-user-management](img/offering-user-management.png)

- Activate the offering using the big green button `Activate`.

**Note**: You will need to set the offering UUID in the agent config file.
For this, you can copy the UUID from the `Integration -> Credentials`
section on the same page:

![offering-uuid](img/offering-uuid.png)

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
This step is necessary for correct setup of the offering in Waldur.

```bash
waldur_site_load_components -c /etc/waldur/waldur-site-agent-config.yaml
```

Thirdly, put systemd unit
and provider config files to the corresponding locations.

- agent-order-process systemd unit: [waldur-agent-order-process.service](systemd-conf/agent-order-process/agent.service)

- agent-report systemd unit: [waldur-agent-report.service](systemd-conf/agent-report/agent.service)

- agent-membership-sync systemd unit: [waldur-agent-membership-sync.service](systemd-conf/agent-membership-sync/agent.service)

- agent-event-process systemd unit: [waldur-agent-event-process.service](systemd-conf/agent-event-process/agent.service)

```bash
# For agent-order-process
cp systemd-conf/agent-order-process/agent.service /etc/systemd/system/waldur-agent-order-process.service

# For agent-report
cp systemd-conf/agent-report/agent.service /etc/systemd/system/waldur-agent-report.service

# For agent-membership-sync
cp systemd-conf/agent-membership-sync/agent.service /etc/systemd/system/waldur-agent-membership-sync.service

# For agent-event-process
cp systemd-conf/agent-event-process/agent.service /etc/systemd/system/waldur-agent-event-process.service
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

systemctl start waldur-agent-event-process.service
systemctl enable waldur-agent-event-process.service # to start after reboot
```

### Event-based processing

Each offering from the config file can have `mqtt_enabled` or `stomp_enabled`
set to `true` or `false` (`false` by default). **NB:** only of them can be `true`

If this setting set to `true`, the offering is ignored
by `agent-order-process` and `agent-membership-sync`,
instead, the `agent-event-process` takes care of it.

### Older systemd versions

If you want to deploy the agents on a machine with systemd revision older than 240,
you should use files with legacy configuration:

```bash
# For agent-order-process
cp systemd-conf/agent-order-process/agent-legacy.service /etc/systemd/system/waldur-agent-order-process-legacy.service
# For agent-report
cp systemd-conf/agent-report/agent-legacy.service /etc/systemd/system/waldur-agent-report-legacy.service
# For agent-membership-sync
cp systemd-conf/agent-membership-sync/agent-legacy.service /etc/systemd/system/waldur-agent-membership-sync-legacy.service
# For agent-event-process
cp systemd-conf/agent-event-process/agent-legacy.service /etc/systemd/system/waldur-agent-event-process-legacy.service
```

### Custom backends

A user should explicitly set backend type for each agent process in an offering config.
For this, a user should use the following settings in an offering item:

- `order_processing_backend` - name of the backend from entrypoints
  to use for order processing;
- `membership_sync_backend` - name of the backend from entrypoints
  to use for membership syncing;
- `reporting_backend` - name of the backend from entrypoints
  to use for reporting.

If a setting is omitted, the agent doesn't start the respecting process.

For example, given the following config:

```yaml
...
offerings:
  - name: "Example SLURM Offering"
    ...
    stomp_enabled: true
    order_processing_backend: "slurm"
    reporting_backend: "custom-slurm-api"
    # Note: membership_sync_backend is omitted
```

the agent starts

- STOMP-based service only for order processing via SLURM
- usage-reporting service using a custom SLURM API,
  which is provided via custom module's entry-point

### Custom backends for username retrieval and generation

By default, the agent doesn't generate usernames for users of resources.
For this, a custom username management backend can be included in the agent:

1. add a path to your class in the `project.entry-points."waldur_site_agent.username_management_backends"`
   section of `pyproject.toml` file, example:

   ```toml
   ...
   [project.entry-points."waldur_site_agent.username_management_backends"]
   custom_backend = "your_project.backend.usernames:CustomUsernameManagementBackend"
   ...
   ```

2. the class should implement the interface
   `waldur_site_agent.backends.username_backend.backend:AbstractUsernameManagementBackend`
3. rebuild the agent, e.g. `poetry install`, `uv sync`
4. add `username_management_backend` field to offerings in your agent config, example:

  ```yaml
  offerings: # Settings for offerings
  - name: "Example Offering" # offering name
    ...
     # Note: the value matches to the setting's key from the step 1
    username_management_backend: "custom_backend" # Name of the backend from entrypoints
    ...
  ```

## Provider config file reference

```yaml
sentry_dsn: "" # Data Source Name for Sentry (more info https://docs.sentry.io/product/sentry-basics/dsn-explainer/).
timezone: "UTC" # Timezone for billing period calculations (e.g. "UTC", "Europe/Tallinn").
  # Defaults to system timezone if not specified.
offerings: # Settings for offerings
  - name: "Example SLURM Offering" # offering name
    waldur_api_url: "http://localhost:8081/api/" # URL of Waldur API (e.g. http://localhost:8081/api/).
    waldur_api_token: "" # Token to access the Waldur API.
    waldur_offering_uuid: "" # UUID of the offering in Waldur.
    stomp_enabled: false # STOMP feature toggler
    mqtt_enabled: true # MQTT feature toggler
    websocket_use_tls: true # Whether to use TLS for websocket connection
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
      customer_prefix: "c_"
      project_prefix: "p_"
      allocation_prefix: "a_"
      enable_user_homedir_account_creation: true
    backend_components:
      deposit: # For MOAB backend, only "deposit" is supported
        measured_unit: 'EUR'
        accounting_type: limit
        label: Deposit (EUR)
```
