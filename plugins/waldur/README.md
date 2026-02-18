# Waldur Federation Plugin for Waldur Site Agent

Waldur-to-Waldur federation backend plugin for Waldur Site Agent. Enables federating
resources, usage, and memberships between two Waldur instances (Waldur A and Waldur B),
replacing the `marketplace_remote` Django app with a stateless, polling-based approach.

## Overview

The plugin acts as a bridge: Waldur A (the "local" instance) receives orders from users
and delegates resource lifecycle management to Waldur B (the "target" instance) via its
marketplace API. Usage is pulled back from Waldur B and reported to Waldur A, with
optional component type conversion.

```mermaid
graph LR
    subgraph "Waldur A (Local)"
        USER[User]
        ORDER_A[Marketplace Order]
        RESOURCE_A[Resource on A]
    end

    subgraph "Site Agent"
        BACKEND[WaldurBackend]
        MAPPER[ComponentMapper]
        CLIENT[WaldurClient]
    end

    subgraph "Waldur B (Target)"
        ORDER_B[Marketplace Order]
        RESOURCE_B[Resource on B]
        USAGE_B[Usage Data]
    end

    USER --> ORDER_A
    ORDER_A --> BACKEND
    BACKEND --> MAPPER
    MAPPER --> CLIENT
    CLIENT --> ORDER_B
    ORDER_B --> RESOURCE_B

    USAGE_B --> CLIENT
    CLIENT --> MAPPER
    MAPPER --> BACKEND
    BACKEND --> RESOURCE_A

    classDef waldurA fill:#e3f2fd
    classDef agent fill:#f3e5f5
    classDef waldurB fill:#fff3e0

    class USER,ORDER_A,RESOURCE_A waldurA
    class BACKEND,MAPPER,CLIENT agent
    class ORDER_B,RESOURCE_B,USAGE_B waldurB
```

## Features

- **Order Forwarding**: Create, update, and terminate resources on Waldur B via marketplace orders
- **Non-blocking Order Creation**: Returns immediately after submitting order on B;
  tracks completion via `check_pending_order()` on subsequent polling cycles
- **Target STOMP Subscriptions**: Optional instant order-completion notifications
  from Waldur B via STOMP, eliminating polling delay
- **Component Mapping**: Configurable conversion factors between Waldur A and Waldur B component types
- **Passthrough Mode**: 1:1 forwarding when no conversion is needed
- **Usage Pulling**: Fetches total and per-user usage from Waldur B, reverse-converts to Waldur A components
- **Membership Sync**: Synchronizes project memberships with configurable user matching (CUID, email, username)
- **Project Tracking**: Automatic project creation on Waldur B with `backend_id` mapping

## Architecture

### Component Overview

```mermaid
graph TB
    subgraph "WaldurBackend"
        INIT[Initialization<br/>Validate settings, create client]
        LIFECYCLE[Resource Lifecycle<br/>create / update / delete]
        USAGE[Usage Reporting<br/>pull + reverse-convert]
        MEMBERS[Membership Sync<br/>add / remove users]
    end

    subgraph "ComponentMapper"
        FWD[Forward Conversion<br/>source limits x factor = target limits]
        REV[Reverse Conversion<br/>target usage / factor = source usage]
    end

    subgraph "WaldurClient"
        ORDERS[Order Operations<br/>create / poll / retrieve]
        PROJECTS[Project Operations<br/>find / create / manage]
        USERS[User Operations<br/>resolve / add / remove]
        USAGES[Usage Operations<br/>component + per-user]
    end

    subgraph "waldur_api_client"
        HTTP[AuthenticatedClient<br/>httpx-based HTTP]
    end

    LIFECYCLE --> FWD
    LIFECYCLE --> ORDERS
    USAGE --> USAGES
    USAGE --> REV
    MEMBERS --> USERS
    MEMBERS --> PROJECTS
    ORDERS --> HTTP
    PROJECTS --> HTTP
    USERS --> HTTP
    USAGES --> HTTP

    classDef backend fill:#e3f2fd
    classDef mapper fill:#e8f5e9
    classDef client fill:#f3e5f5
    classDef http fill:#fff3e0

    class INIT,LIFECYCLE,USAGE,MEMBERS backend
    class FWD,REV mapper
    class ORDERS,PROJECTS,USERS,USAGES client
    class HTTP http
```

### Resource Creation Flow (Non-blocking)

Resource creation uses non-blocking (async) order submission. The agent submits
the order on Waldur B and returns immediately. The core processor tracks
completion on subsequent polling cycles via `check_pending_order()`.

```mermaid
sequenceDiagram
    participant A as Waldur A
    participant SA as Site Agent
    participant B as Waldur B

    A->>SA: New CREATE order
    SA->>SA: Convert limits via ComponentMapper
    SA->>B: Find project by backend_id
    alt Project not found
        SA->>B: Create project (backend_id = custUUID_projUUID)
    end
    SA->>B: Create marketplace order (limits, offering)
    B-->>SA: Order UUID + resource UUID (immediate)
    SA->>A: Set backend_id = target_resource_uuid
    SA->>A: Set order backend_id = target_order_uuid
    Note over SA: Order stays EXECUTING on A

    loop Subsequent processor cycles
        A->>SA: Process offering (next cycle)
        SA->>SA: Order has backend_id → call check_pending_order()
        SA->>B: Get target order state
        alt Target order DONE
            B-->>SA: DONE
            SA->>A: set_state_done
        else Target order still pending
            B-->>SA: EXECUTING / PENDING_PROVIDER
            Note over SA: Skip, check again next cycle
        else Target order ERRED
            B-->>SA: ERRED
            SA->>A: set_state_erred
        end
    end
```

**Key design rule:** The agent does NOT set `backend_id` on the target resource
(Waldur B). Only the source resource (Waldur A) gets `backend_id` = B's resource
UUID. Waldur B's `backend_id` is managed by B's own service provider.

### Target STOMP Event Subscriptions (Optional)

When `target_stomp_enabled` is `true`, the agent subscribes to ORDER events on
Waldur B via STOMP. This provides instant notification when target orders
complete, eliminating the polling delay from `check_pending_order()`.

```mermaid
sequenceDiagram
    participant A as Waldur A
    participant SA as Site Agent
    participant B as Waldur B
    participant STOMP as Waldur B STOMP

    Note over SA,STOMP: On startup (event_process mode)
    SA->>B: Register agent identity
    SA->>B: Create ORDER event subscription
    SA->>STOMP: Connect via WebSocket

    Note over SA,STOMP: On target order completion
    STOMP-->>SA: ORDER event (order_uuid, state=DONE)
    SA->>SA: Find source order by backend_id = target_order_uuid
    SA->>A: set_state_done on source order
```

### Order and Resource Sync Lifecycle

The following diagram shows how orders and resources on Waldur A map
to orders and resources on Waldur B, and how `backend_id` links them.

```mermaid
graph TB
    subgraph "Waldur A (Source)"
        OA_CREATE["CREATE Order<br/>uuid: abc-123"]
        OA_UPDATE["UPDATE Order<br/>uuid: def-456"]
        OA_TERMINATE["TERMINATE Order<br/>uuid: ghi-789"]
        RA["Resource on A<br/>uuid: res-A<br/>backend_id: res-B<br/>state: OK"]
    end

    subgraph "Site Agent"
        direction TB
        PROC["OfferingOrderProcessor"]
        BACKEND["WaldurBackend"]
        MAPPER["ComponentMapper"]
    end

    subgraph "Waldur B (Target)"
        OB_CREATE["CREATE Order on B<br/>uuid: ob-1<br/>state: DONE"]
        OB_UPDATE["UPDATE Order on B<br/>uuid: ob-2<br/>state: DONE"]
        OB_TERMINATE["TERMINATE Order on B<br/>uuid: ob-3<br/>state: DONE"]
        RB["Resource on B<br/>uuid: res-B<br/>state: OK"]
        PB["Project on B<br/>backend_id: custA_projA"]
    end

    OA_CREATE -->|"1. Fetch pending"| PROC
    PROC -->|"2. create_resource_with_id()"| BACKEND
    BACKEND -->|"3. Convert limits"| MAPPER
    MAPPER -->|"4. Create order"| OB_CREATE
    OB_CREATE -->|"creates"| RB
    RB -.->|"backend_id = res-B"| RA
    OB_CREATE -.->|"order backend_id = ob-1"| OA_CREATE

    OA_UPDATE -->|"set_resource_limits()"| BACKEND
    BACKEND -->|"Convert + order"| OB_UPDATE

    OA_TERMINATE -->|"delete_resource()"| BACKEND
    BACKEND -->|"Terminate order"| OB_TERMINATE

    RB -->|"belongs to"| PB

    classDef waldurA fill:#e3f2fd
    classDef agent fill:#f3e5f5
    classDef waldurB fill:#fff3e0

    class OA_CREATE,OA_UPDATE,OA_TERMINATE,RA waldurA
    class PROC,BACKEND,MAPPER agent
    class OB_CREATE,OB_UPDATE,OB_TERMINATE,RB,PB waldurB
```

**`backend_id` mapping:**

| Entity on A | `backend_id` value | Points to |
|---|---|---|
| Resource on A | `res-B` (UUID) | Resource UUID on Waldur B |
| CREATE Order on A | `ob-1` (UUID) | CREATE Order UUID on Waldur B |
| Project on B | `custA_projA` | `{customer_uuid_on_A}_{project_uuid_on_A}` |

### Full Order State Machine (Create)

```mermaid
stateDiagram-v2
    state "Waldur A" as A {
        [*] --> pending_consumer_A: User creates order
        pending_consumer_A --> pending_provider_A: Auto-transition
        pending_provider_A --> executing_A: Agent approves
        executing_A --> done_A: Agent sets done
        executing_A --> erred_A: Agent sets erred
    }

    state "Waldur B" as B {
        [*] --> pending_consumer_B: Agent creates order
        pending_consumer_B --> pending_provider_B: Auto-transition
        pending_provider_B --> executing_B: B's processor approves
        executing_B --> done_B: B's processor completes
        executing_B --> erred_B: B's processor fails
    }

    note right of A
        Cycle 1: Agent picks up order,
        submits to B, sets backend_id
        Cycle 2+: check_pending_order()
        polls B until terminal
    end note

    note right of B
        With target STOMP: ORDER
        event sent on state change,
        agent reacts instantly
    end note
```

### STOMP vs Polling: Order Completion

```mermaid
sequenceDiagram
    participant A as Waldur A
    participant SA as Site Agent
    participant B as Waldur B

    Note over A,B: Polling mode (target_stomp_enabled=false)
    SA->>B: Create order on B
    B-->>SA: Order UUID (immediate)
    SA->>A: Set backend_id on A's order
    loop Every processor cycle (e.g., 60s)
        SA->>B: GET order state
        B-->>SA: EXECUTING
    end
    SA->>B: GET order state
    B-->>SA: DONE
    SA->>A: set_state_done

    Note over A,B: STOMP mode (target_stomp_enabled=true)
    SA->>B: Create order on B
    B-->>SA: Order UUID (immediate)
    SA->>A: Set backend_id on A's order
    Note over B: Order completes on B
    B-->>SA: STOMP event: order DONE (instant)
    SA->>A: set_state_done (no polling needed)
```

### Usage Reporting Flow

```mermaid
sequenceDiagram
    participant A as Waldur A
    participant SA as Site Agent
    participant B as Waldur B

    A->>SA: Request usage report
    SA->>B: Get component usages (resource UUID)
    B-->>SA: Target component usages (gpu_hours, storage_gb_hours)
    SA->>B: Get per-user component usages
    B-->>SA: Per-user target usages
    SA->>SA: Reverse-convert via ComponentMapper
    Note over SA: node_hours = gpu_hours/5 + storage_gb_hours/10
    SA-->>A: Usage report in source components (node_hours)
```

### Component Mapping

The `ComponentMapper` handles bidirectional conversion between component types
on Waldur A (source) and Waldur B (target).

```mermaid
graph LR
    subgraph "Waldur A (Source)"
        NH[node_hours = 100]
    end

    subgraph "ComponentMapper"
        direction TB
        FWD["Forward (limits)<br/>value x factor"]
        REV["Reverse (usage)<br/>value / factor"]
    end

    subgraph "Waldur B (Target)"
        GPU[gpu_hours = 500<br/>factor: 5.0]
        STOR[storage_gb_hours = 1000<br/>factor: 10.0]
    end

    NH -- "100 x 5" --> GPU
    NH -- "100 x 10" --> STOR

    GPU -- "500 / 5 = 100" --> REV
    STOR -- "800 / 10 = 80" --> REV
    REV -- "100 + 80 = 180" --> NH

    classDef source fill:#e3f2fd
    classDef mapper fill:#e8f5e9
    classDef target fill:#fff3e0

    class NH source
    class FWD,REV mapper
    class GPU,STOR target
```

**Passthrough mode**: When no `target_components` are configured for a component,
it maps 1:1 with the same name and factor 1.0.

**Fan-out**: A single source component can map to multiple target components.

**Fan-in (reverse)**: Multiple target components contributing to the same source
component are summed: `source = SUM(target_value / factor)`.

## Configuration

### Full Example (Polling Mode)

```yaml
offerings:
  - name: "Federated HPC Access"
    waldur_api_url: "https://waldur-a.example.com/api/"
    waldur_api_token: "token-for-waldur-a"
    waldur_offering_uuid: "offering-uuid-on-waldur-a"
    backend_type: "waldur"
    order_processing_backend: "waldur"
    membership_sync_backend: "waldur"
    reporting_backend: "waldur"
    backend_settings:
      target_api_url: "https://waldur-b.example.com/api/"
      target_api_token: "service-account-token-for-waldur-b"
      target_offering_uuid: "offering-uuid-on-waldur-b"
      target_customer_uuid: "customer-uuid-on-waldur-b"
      user_match_field: "cuid"        # cuid | email | username
      order_poll_timeout: 300          # seconds
      order_poll_interval: 5           # seconds
      user_not_found_action: "warn"    # warn | fail
    backend_components:
      node_hours:
        measured_unit: "Hours"
        unit_factor: 1
        accounting_type: "usage"
        label: "Node Hours"
        target_components:
          gpu_hours:
            factor: 5.0
          storage_gb_hours:
            factor: 10.0
```

### Full Example (Event Processing with Target STOMP)

```yaml
offerings:
  - name: "Federated HPC Access"
    waldur_api_url: "https://waldur-a.example.com/api/"
    waldur_api_token: "token-for-waldur-a"
    waldur_offering_uuid: "offering-uuid-on-waldur-a"
    backend_type: "waldur"
    order_processing_backend: "waldur"
    membership_sync_backend: "waldur"
    reporting_backend: "waldur"

    # Source STOMP: receive events from Waldur A
    stomp_enabled: true
    websocket_use_tls: true
    # stomp_ws_host: "waldur-a.example.com"  # defaults to API host
    # stomp_ws_port: 443                      # defaults to 443 (TLS) or 80
    # stomp_ws_path: "/rmqws-stomp"           # defaults to /rmqws-stomp

    backend_settings:
      target_api_url: "https://waldur-b.example.com/"
      target_api_token: "service-account-token-for-waldur-b"
      target_offering_uuid: "offering-uuid-on-waldur-b"
      target_customer_uuid: "customer-uuid-on-waldur-b"
      user_match_field: "cuid"
      order_poll_timeout: 300
      order_poll_interval: 5
      user_not_found_action: "warn"
      # Target STOMP: subscribe to ORDER events on Waldur B
      target_stomp_enabled: true
      # Agent identity on B requires a Marketplace.Slurm offering UUID
      target_stomp_offering_uuid: "agent-based-offering-uuid-on-waldur-b"

    backend_components:
      node_hours:
        measured_unit: "Hours"
        unit_factor: 1
        accounting_type: "usage"
        label: "Node Hours"
        target_components:
          gpu_hours:
            factor: 5.0
          storage_gb_hours:
            factor: 10.0
```

### Passthrough Configuration

When Waldur A and Waldur B use the same component types, omit `target_components`:

```yaml
    backend_components:
      cpu:
        measured_unit: "Hours"
        unit_factor: 1
        accounting_type: "usage"
        label: "CPU Hours"
      mem:
        measured_unit: "GB"
        unit_factor: 1
        accounting_type: "usage"
        label: "Memory GB"
```

### Source STOMP Settings (Offering Level)

These settings are on the offering itself (not inside `backend_settings`):

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `stomp_enabled` | No | `false` | Enable STOMP event processing from Waldur A |
| `websocket_use_tls` | No | `true` | Use TLS for WebSocket connections |
| `stomp_ws_host` | No | API host | STOMP WebSocket host (defaults to Waldur A API host) |
| `stomp_ws_port` | No | `443`/`80` | STOMP WebSocket port (443 for TLS, 80 otherwise) |
| `stomp_ws_path` | No | `/rmqws-stomp` | STOMP WebSocket path |

### Backend Settings Reference

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `target_api_url` | Yes | -- | Base URL for Waldur B API |
| `target_api_token` | Yes | -- | Service account token for Waldur B |
| `target_offering_uuid` | Yes | -- | Offering UUID on Waldur B |
| `target_customer_uuid` | Yes | -- | Customer/organization UUID on Waldur B |
| `user_match_field` | No | `cuid` | User matching strategy: `cuid`, `email`, or `username` |
| `order_poll_timeout` | No | `300` | Max seconds to wait for synchronous order completion (update/terminate) |
| `order_poll_interval` | No | `5` | Seconds between synchronous order state polls |
| `user_not_found_action` | No | `warn` | When user not found: `warn` or `fail` |
| `target_stomp_enabled` | No | `false` | STOMP subscription on B for instant order completion |
| `target_stomp_offering_uuid` | No | `target_offering_uuid` | `Marketplace.Slurm` offering on B for agent identity |

### Component Target Configuration

Each source component can optionally define `target_components`:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `factor` | No | `1.0` | Conversion factor (must be > 0). Target = source x factor |

## Usage

### Agent Modes

```bash
# Process orders: create/update/terminate resources on Waldur B
uv run waldur_site_agent -m order_process -c config.yaml

# Report usage: pull from Waldur B, reverse-convert, report to Waldur A
uv run waldur_site_agent -m report -c config.yaml

# Sync memberships: resolve users and manage project teams on Waldur B
uv run waldur_site_agent -m membership_sync -c config.yaml

# Event processing: STOMP-based real-time order/membership handling
# Requires stomp_enabled: true in config
uv run waldur_site_agent -m event_process -c config.yaml
```

### Agent Mode Data Flow

```mermaid
graph TB
    subgraph "order_process mode"
        OP_FETCH[Fetch pending orders<br/>from Waldur A]
        OP_CREATE[Create resource<br/>on Waldur B]
        OP_UPDATE[Update limits<br/>on Waldur B]
        OP_DELETE[Terminate resource<br/>on Waldur B]
        OP_REPORT[Report result<br/>to Waldur A]

        OP_FETCH --> OP_CREATE
        OP_FETCH --> OP_UPDATE
        OP_FETCH --> OP_DELETE
        OP_CREATE --> OP_REPORT
        OP_UPDATE --> OP_REPORT
        OP_DELETE --> OP_REPORT
    end

    subgraph "report mode"
        R_LIST[List resources<br/>on Waldur B]
        R_PULL[Pull component usages<br/>+ per-user usages]
        R_CONVERT[Reverse-convert<br/>via ComponentMapper]
        R_SUBMIT[Submit usage<br/>to Waldur A]

        R_LIST --> R_PULL --> R_CONVERT --> R_SUBMIT
    end

    subgraph "membership_sync mode"
        M_DIFF[Compute membership diff<br/>Waldur A vs Waldur B]
        M_RESOLVE[Resolve users<br/>cuid / email / username]
        M_ADD[Add to project<br/>on Waldur B]
        M_REMOVE[Remove from project<br/>on Waldur B]

        M_DIFF --> M_RESOLVE
        M_RESOLVE --> M_ADD
        M_RESOLVE --> M_REMOVE
    end

    classDef orderMode fill:#e3f2fd
    classDef reportMode fill:#e8f5e9
    classDef memberMode fill:#f3e5f5

    class OP_FETCH,OP_CREATE,OP_UPDATE,OP_DELETE,OP_REPORT orderMode
    class R_LIST,R_PULL,R_CONVERT,R_SUBMIT reportMode
    class M_DIFF,M_RESOLVE,M_ADD,M_REMOVE memberMode
```

## Plugin Structure

```text
plugins/waldur/
├── pyproject.toml                         # Package metadata + entry points
├── README.md
├── waldur_site_agent_waldur/
│   ├── __init__.py
│   ├── backend.py                         # WaldurBackend(BaseBackend)
│   ├── client.py                          # WaldurClient(BaseClient)
│   ├── component_mapping.py               # ComponentMapper (forward + reverse)
│   ├── schemas.py                         # Pydantic validation schemas
│   ├── target_event_handler.py            # STOMP handler for Waldur B ORDER events
│   └── username_backend.py               # Identity bridge username management backend
└── tests/
    ├── __init__.py
    ├── conftest.py                        # Shared test fixtures
    ├── integration_helpers.py             # Test setup helpers (WaldurTestSetup)
    ├── test_backend.py                    # Backend unit tests (36 tests)
    ├── test_client.py                     # Client tests (20 tests)
    ├── test_component_mapping.py          # Mapper tests (22 tests)
    ├── test_integration.py                # Integration tests (56 tests)
    ├── test_target_event_handler.py       # Target event handler tests
    ├── test_username_backend.py           # Identity bridge username backend tests (22 tests)
    └── e2e/                               # End-to-end tests against live instances
        ├── conftest.py                    # E2E fixtures, AutoApproveWaldurBackend, MessageCapture
        ├── test_e2e_federation.py         # REST polling lifecycle tests (Tests 1-4)
        ├── test_e2e_stomp.py              # STOMP event tests (Tests 5-7)
        └── TEST_PLAN.md                   # Detailed E2E test plan
```

### Entry Points

The plugin registers four entry points for automatic discovery:

```toml
[project.entry-points."waldur_site_agent.backends"]
waldur = "waldur_site_agent_waldur.backend:WaldurBackend"

[project.entry-points."waldur_site_agent.component_schemas"]
waldur = "waldur_site_agent_waldur.schemas:WaldurComponentSchema"

[project.entry-points."waldur_site_agent.backend_settings_schemas"]
waldur = "waldur_site_agent_waldur.schemas:WaldurBackendSettingsSchema"

[project.entry-points."waldur_site_agent.username_management_backends"]
waldur-identity-bridge = "waldur_site_agent_waldur.username_backend:WaldurIdentityBridgeUsernameBackend"
```

## User Matching

The plugin supports three strategies for resolving Waldur A users on Waldur B:

| Strategy | Setting | Waldur B API | Use Case |
|----------|---------|-------------|----------|
| eduTeams CUID | `cuid` | `remote_eduteams` endpoint | Both instances use eduTeams federation |
| Email | `email` | `users_list(email=...)` | Shared email addresses between instances |
| Username | `username` | `users_list(username=...)` | Shared usernames between instances |

Resolved user UUIDs are cached for the lifetime of the backend instance to minimize API calls.

When a user cannot be resolved on Waldur B:

- **`warn`** (default): Log a warning and skip the user
- **`fail`**: Raise a `BackendError` (caught per-user, does not abort the batch)

## Identity Bridge Integration

The plugin includes a username management backend (`waldur-identity-bridge`) that pushes
user profiles from Waldur A to Waldur B via the Identity Bridge API before membership sync.
This ensures users exist on Waldur B before the agent tries to resolve and add them to projects.

### How It Works

1. During membership sync, `sync_user_profiles()` is called before user resolution
2. For each offering user on Waldur A, it sends `POST /api/identity-bridge/` to Waldur B
3. Identity Bridge creates the user if they don't exist, or updates attributes if they do
4. Users that disappear from the offering are deactivated via `POST /api/identity-bridge/remove/`

### Identity Bridge Configuration

```yaml
offerings:
  - name: "Federated HPC Access"
    waldur_api_url: "https://waldur-a.example.com/api/"
    waldur_api_token: "token-for-waldur-a"
    waldur_offering_uuid: "offering-uuid-on-waldur-a"
    username_management_backend: "waldur-identity-bridge"
    backend_type: "waldur"
    backend_settings:
      target_api_url: "https://waldur-b.example.com/api/"
      target_api_token: "service-account-token-for-waldur-b"
      target_offering_uuid: "offering-uuid-on-waldur-b"
      target_customer_uuid: "customer-uuid-on-waldur-b"
      identity_bridge_source: "isd:efp"  # Required for identity bridge
```

### Identity Bridge Settings

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `identity_bridge_source` | Yes | `""` | ISD source identifier (e.g. `isd:efp`) |

### User Attributes Synced

The backend pushes all exposed offering user attributes to identity bridge, including:
first name, last name, email, organization, affiliations, phone number, gender,
birth date, nationality, and other profile fields configured via `OfferingUserAttributeConfig`.

## Project Mapping

Projects on Waldur B are tracked using `backend_id`:

```text
backend_id = "{customer_uuid_on_A}_{project_uuid_on_A}"
```

On each resource creation, the plugin:

1. Searches for an existing project on Waldur B with the matching `backend_id`
2. Creates a new project under the configured `target_customer_uuid` if not found
3. Uses the project for all subsequent operations on that resource

## Testing

```bash
# Run unit tests
.venv/bin/python -m pytest plugins/waldur/tests/test_backend.py -v
.venv/bin/python -m pytest plugins/waldur/tests/test_client.py -v
.venv/bin/python -m pytest plugins/waldur/tests/test_component_mapping.py -v
.venv/bin/python -m pytest plugins/waldur/tests/test_target_event_handler.py -v

# Run integration tests (requires WALDUR_INTEGRATION_TESTS=true)
WALDUR_INTEGRATION_TESTS=true \
.venv/bin/python -m pytest plugins/waldur/tests/test_integration.py -v

# Run all E2E tests (REST + STOMP) against live instances
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=puhuri-federation-config.yaml \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/ -v -s

# Run REST polling E2E tests only (Tests 1-4)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=puhuri-federation-config.yaml \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_federation.py -v -s

# Run STOMP event E2E tests only (Tests 5-7)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=puhuri-federation-config.yaml \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_stomp.py -v -s

# Run with coverage
.venv/bin/python -m pytest plugins/waldur/tests/ --cov=waldur_site_agent_waldur
```

### Test Coverage

| Module | Tests | Focus |
|--------|-------|-------|
| `test_component_mapping.py` | 22 | Forward/reverse conversion, passthrough, round-trip |
| `test_client.py` | 20 | API operations with mocked `waldur_api_client` |
| `test_backend.py` | 36 | Resource lifecycle, async orders, usage reporting, membership sync |
| `test_username_backend.py` | 22 | Identity bridge username backend, attribute mapping, user sync |
| `test_target_event_handler.py` | -- | STOMP ORDER event handling, source order state updates |
| `test_integration.py` | 56 | Integration tests against real single Waldur instance |
| `e2e/test_e2e_federation.py` | 4 | REST polling lifecycle (create, update, terminate) |
| `e2e/test_e2e_stomp.py` | 4 | STOMP connections + event capture + order flow + cleanup |

## Comparison with marketplace_remote

This plugin replaces the `marketplace_remote` Django app from waldur-mastermind:

| Capability | marketplace_remote | This Plugin |
|---|---|---|
| Order forwarding | Celery tasks + Django signals | Polling + optional STOMP events, stateless |
| Order creation | Synchronous (Celery blocks) | Non-blocking (returns immediately, tracks async) |
| Project tracking | Django model (ProjectUpdateRequest) | `backend_id` on Waldur B projects |
| Order polling | Celery retries (OrderStatePullTask) | `check_pending_order()` on subsequent cycles |
| Target events | N/A | Optional STOMP subscription for instant completion |
| Usage pulling | Direct DB writes (ComponentUsage model) | API fetch + reverse conversion |
| User sync | eduTeams CUID only | Configurable: cuid / email / username |
| Component mapping | 1:1 (same component types) | Configurable conversion factors |
| State management | Django ORM | Stateless (no local DB) |
| Offering sync | Yes (pull offerings, plans, screenshots) | Not needed (configured in YAML) |
| Invoice pulling | Yes | Not applicable (Waldur A handles billing) |
| Robot accounts | Yes | Not applicable |
