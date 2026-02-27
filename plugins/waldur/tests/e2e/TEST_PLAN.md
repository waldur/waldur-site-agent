# Waldur Federation E2E Test Plan

## Overview

End-to-end tests for Waldur-to-Waldur federation via the site agent.
The agent sits between two Waldur instances and forwards orders, usage,
and memberships.

```text
Waldur A (source)                     Site Agent                     Waldur B (target)
Marketplace.Slurm offering      <-->  OfferingOrderProcessor   <-->  Marketplace.Slurm offering
                                      WaldurBackend
```

The tests exercise both operational modes:

- **REST polling mode** (`order_process`): Agent polls for orders on A,
  creates resources on B, checks order completion on B via
  `check_pending_order()` on subsequent processor cycles.
- **STOMP event mode** (`event_process`): Agent receives ORDER events
  from both Waldur A (source) and Waldur B (target) via STOMP over
  WebSocket. Target STOMP provides instant order completion notification.

## Environment

| Variable | Value | Description |
|---|---|---|
| `WALDUR_E2E_TESTS` | `true` | Gate: skip all E2E tests if not set |
| `WALDUR_E2E_CONFIG` | `<path-to-config.yaml>` | Agent config file |
| `WALDUR_E2E_PROJECT_A_UUID` | `<uuid>` | Project UUID on Waldur A |

### Instance Requirements

| Instance | Requirements |
|---|---|
| Waldur A (source) | Active `Marketplace.Slurm` offering with plan; see Step 2 for token permissions |
| Waldur B (target) | Active `Marketplace.Slurm` offering with matching components; see Step 1 for token permissions |

### Setup Instructions

Follow these steps to prepare two Waldur instances for E2E testing.
All operations can be done via Waldur Admin UI or REST API.

#### Step 1: Waldur B (Target) — Organization and Offering

Create the target side first, because you'll need its UUIDs for the
agent config.

1. **Create or choose an organization** on Waldur B.
   Note its UUID — this becomes `target_customer_uuid`.

2. **Create a `Marketplace.Slurm` offering** under that organization:
   - Offering type: `Marketplace.Slurm` (required for STOMP event signals
     and agent identity registration)
   - Add components that match your source offering. For each component:
     - **Type**: `limit` (billing type)
     - **Billing type**: `limit`
     - **Measured unit**: any (e.g., "Units", "Hours")
   - Note the offering UUID — this becomes `target_offering_uuid`.

3. **Add a plan** to the offering:
   - Any name (e.g., "Default")
   - Set prices for each component (can be 0 for testing)

4. **Activate the offering**:
   - Set offering state to `Active` (via Admin UI or API)

5. **Create an API token** on Waldur B:
   - The token user must be a **customer owner** (can be a non-SP
     customer separate from the offering's service provider) and an
     **ISD identity manager** (`is_identity_manager: true` with
     `managed_isds` set)
   - This becomes `target_api_token`

#### Step 2: Waldur A (Source) — Offering and Project

1. **Create a `Marketplace.Slurm` offering** on Waldur A:
   - Offering type: `Marketplace.Slurm`
   - Add components that map to B's components. For each component:
     - **Type**: `limit` (billing type)
     - **Billing type**: `limit`
     - **Measured unit**: any (e.g., "Node-hours", "TB-hours")
   - Note the offering UUID — this becomes `waldur_offering_uuid`.

2. **Add a plan** to the offering:
   - Any name (e.g., "Default")
   - Set prices for each component (can be 0 for testing)

3. **Activate the offering**:
   - Set offering state to `Active`

4. **Create a project** on Waldur A:
   - The project must belong to an organization that has access to the
     offering (via category or direct assignment)
   - Note the project UUID — this becomes `WALDUR_E2E_PROJECT_A_UUID`

5. **Create an API token** on Waldur A:
   - The token user must have **OFFERING.MANAGER** role on the offering
   - This becomes `waldur_api_token`

#### Step 3: Component Mapping

The agent config maps source components (A) to target components (B).
Two modes are available:

**Passthrough mode** (1:1, same component names):

```yaml
backend_components:
  cpu:
    measured_unit: "Hours"
    unit_factor: 1.0
    accounting_type: "limit"
    label: "CPU"
    # No target_components → forwarded as-is to B
```

Both offerings must have a component with the internal name `cpu`.

**Conversion mode** (N:M with factors):

```yaml
backend_components:
  node_hours:                    # Component name on A
    measured_unit: "Node-hours"
    unit_factor: 1.0
    accounting_type: "limit"
    label: "Node Hours"
    target_components:
      cpu_k_hours:               # Component name on B
        factor: 128.0            # target_value = source_value * 128
```

A's `node_hours` component maps to B's `cpu_k_hours` with a 128x
multiplier. One source component can map to multiple target components.

**Important:** Component internal names (the YAML keys) must match the
component types defined on each respective offering. Check component
types via API:

```text
GET /api/marketplace-provider-offerings/<uuid>/ → components[].type
```

#### Step 4: Optional — STOMP Event Processing

For STOMP tests (Tests 5-8), additional setup is required.

**On Waldur A:**

- Verify `/rmqws-stomp` WebSocket endpoint is available:

```bash
curl -sI https://<waldur-a-host>/rmqws-stomp
# Expected: HTTP 426 (Upgrade Required)
```

- Set `stomp_enabled: true` and `websocket_use_tls: true` in config

**On Waldur B (target STOMP):**

- Verify `/rmqws-stomp` WebSocket endpoint is available (same curl test)
- The target offering is already `Marketplace.Slurm` (Step 1), so agent
  identity registration works directly. Set `target_stomp_offering_uuid`
  to the same UUID as `target_offering_uuid`.
- Set `target_stomp_enabled: true` in backend settings

If Waldur B does not have `/rmqws-stomp` configured (returns HTTP 200
instead of 426), skip target STOMP tests. The agent will fall back to
polling via `check_pending_order()`.

#### Step 5: User Matching

The agent maps users between Waldur A and B using a configurable field.
Set `user_match_field` in backend settings:

| Value | Matches on | When to use |
|---|---|---|
| `cuid` | Community User ID | Both instances use same IdP (e.g., eduTEAMS) |
| `email` | Email address | Users have same email on both instances |
| `username` | Username | Users have same username on both |

For E2E tests that only exercise order processing (Tests 1-4), user
matching is not critical. For membership sync tests, users must exist
on both instances with matching field values.

### Configuration Template

```yaml
timezone: "UTC"
offerings:
  - name: "Federation E2E"
    waldur_api_url: "https://waldur-a.example.com/api/"
    waldur_api_token: "<token-for-A>"
    waldur_offering_uuid: "<offering-uuid-on-A>"
    backend_type: "waldur"
    order_processing_backend: "waldur"
    reporting_backend: "waldur"
    membership_sync_backend: "waldur"

    # For STOMP tests (optional)
    stomp_enabled: true
    websocket_use_tls: true

    backend_settings:
      target_api_url: "https://waldur-b.example.com/"
      target_api_token: "<token-for-B>"
      target_offering_uuid: "<offering-uuid-on-B>"
      target_customer_uuid: "<customer-uuid-on-B>"
      user_match_field: "cuid"
      order_poll_timeout: 300
      order_poll_interval: 5
      user_not_found_action: "warn"
      # For target STOMP tests (optional); same UUID as target_offering_uuid
      # since the target offering is Marketplace.Slurm
      target_stomp_enabled: true
      target_stomp_offering_uuid: "<offering-uuid-on-B>"

    backend_components:
      # Example: passthrough (1:1) or with conversion factors
      component_a:
        measured_unit: "Units"
        unit_factor: 1.0
        accounting_type: "limit"
        label: "Component A"
        target_components:
          target_component_a:
            factor: 128.0
```

## How to Run

```bash
# All E2E tests (REST + STOMP)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=<config.yaml> \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/ -v -s

# REST polling tests only (Tests 1-4)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=<config.yaml> \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_federation.py -v -s

# STOMP event tests only (Tests 5-7)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=<config.yaml> \
WALDUR_E2E_PROJECT_A_UUID=<uuid> \
.venv/bin/python -m pytest plugins/waldur/tests/e2e/test_e2e_stomp.py -v -s
```

## Test Scenarios: REST Polling Mode

### Test 1: Processor Initialization (`test_processor_init`)

**Purpose:** Verify `OfferingOrderProcessor` connects to Waldur A with
`WaldurBackend` pointing at Waldur B.

**Steps:**

1. Load config from YAML
2. Create `OfferingOrderProcessor(offering, waldur_client_a, backend)`
3. Verify `processor.resource_backend` is the backend instance

**Expected:** Processor initializes without errors.

### Test 2: Create Order (`test_create_order`)

**Purpose:** Full non-blocking create lifecycle:
order on A -> processor creates resource on B -> order completes.

**Steps:**

1. Fetch offering URL and plan URL from
   `marketplace-public-offerings/{uuid}/` on A
2. Fetch project URL from `projects/{uuid}/` on A
3. Build limits from configured components (small test values)
4. Create order on A via `marketplace_orders_create`
5. Run `_run_processor_until_order_terminal()` (max 15 cycles, 3s delay):
   - Cycle 1: Processor picks up order, calls
     `WaldurBackend.create_resource_with_id()` (non-blocking)
   - Backend submits order on B, returns `pending_order_id`
   - Processor sets source order `backend_id` = target order UUID
   - Cycle 2+: Processor calls `check_pending_order(backend_id)`
   - `AutoApproveWaldurBackend` auto-approves `PENDING_PROVIDER` on B
   - Eventually returns `True` -> processor marks order DONE
6. Verify resource on A has `backend_id` set (= B's resource UUID)
7. Verify resource exists on B using A's `backend_id` as UUID

**Expected:**

- Order reaches terminal state (DONE or ERRED)
- Resource on A has non-empty `backend_id`
- Resource exists on B at UUID = A's `backend_id`
- Component limits converted correctly (source * factor = target)

**Key design rule:** Agent does NOT set `backend_id` on target resource (B).
Only A's resource gets `backend_id` = B's resource UUID.

### Test 3: Update Limits (`test_update_limits`)

**Purpose:** Update limits on an existing resource.

**Depends on:** Test 2 (needs `resource_uuid_a`)

**Steps:**

1. Create update order on A via
   `POST /api/marketplace-resources/{uuid}/update_limits/`
2. Run `_run_processor_until_order_terminal()`
   - Processor calls `WaldurBackend.set_resource_limits(backend_id, limits)`
   - Backend converts limits and creates update order on B
   - `AutoApproveWaldurClient.poll_order_completion()` auto-approves on B
3. Verify order reaches terminal state

**Expected:**

- Resource limits updated on B (with conversion factor applied)

**Known issue:** Waldur may create a "shadow" resource for Update orders
with empty `backend_id`. See Known Issues section.

### Test 4: Terminate Resource (`test_terminate_resource`)

**Purpose:** Terminate resource through the processor.

**Depends on:** Test 2 (needs `resource_uuid_a`, `resource_uuid_b`)

**Steps:**

1. Create terminate order on A via
   `POST /api/marketplace-resources/{uuid}/terminate/`
2. Run `_run_processor_until_order_terminal()`
   - Processor calls `WaldurBackend.delete_resource(waldur_resource)`
   - Backend creates terminate order on B
   - Polls B for order completion
3. Verify resource on A state != `OK`
4. Verify resource on B state != `OK`

**Expected:**

- Both resources end up in non-OK state (typically `TERMINATED`)

## Test Scenarios: STOMP Event Mode

These tests require `stomp_enabled: true` in the config and
a Waldur instance with STOMP-over-WebSocket (`/rmqws-stomp`) configured.

### Test 5: Source STOMP Connection (Waldur A) — Automated

**Purpose:** Verify STOMP connections to Waldur A establish correctly.

**Pre-flight:** `check_stomp_available()` sends HTTP GET to
`/rmqws-stomp` — expects HTTP 426. Skips test if unavailable.

**Steps:**

1. `setup_stomp_offering_subscriptions()` registers agent identity,
   creates event subscriptions, and establishes STOMP connections
2. Test verifies each source consumer `conn.is_connected()`
3. Report captures connection details per subscription type

**Expected:**

- 5 source STOMP connections established (ORDER, USER_ROLE, RESOURCE,
  SERVICE_ACCOUNT, COURSE_ACCOUNT)
- All connections report `is_connected() == True`

**Prerequisites:** Waldur A must have `/rmqws-stomp` WebSocket endpoint
configured in nginx, proxying to RabbitMQ's `rabbitmq_web_stomp` plugin.

**Verification:**

```bash
# Should return HTTP 426 (Upgrade Required) — correct for WebSocket
curl -sI https://<waldur-a-host>/rmqws-stomp
```

### Test 6: Target STOMP Connection (Waldur B) — Automated

**Purpose:** Verify STOMP connection to Waldur B for instant order
completion notifications.

**Config required:**

```yaml
backend_settings:
  target_stomp_enabled: true
  target_stomp_offering_uuid: "<slurm-offering-uuid-on-B>"
```

**Steps:**

1. `setup_stomp_offering_subscriptions()` also sets up target STOMP
   when `target_stomp_enabled=true`. Target consumers have offering
   name prefixed with `"Target: "`.
2. Test verifies each target consumer `conn.is_connected()`
3. If no target consumers exist, test is skipped (graceful)

**Expected:**

- 1 target STOMP connection established (ORDER events on B)
- Connection reports `is_connected() == True`
- Skipped gracefully if `target_stomp_enabled=false`

**Prerequisites:** Waldur B must have `/rmqws-stomp` WebSocket endpoint
configured. Verify with:

```bash
# Should return HTTP 426 (Upgrade Required)
curl -sI https://<waldur-b-host>/rmqws-stomp
# If HTTP 200 with text/html — STOMP is NOT configured on this server
```

### Test 7: STOMP Order Event Flow (Automated)

**Purpose:** Verify STOMP events are received while orders are
processed via the standard REST-based processor. This is a hybrid
approach: STOMP connections are established and events are captured
in a thread-safe `MessageCapture`, while order processing uses the
same REST `_run_processor_until_order_terminal()` as Tests 1-4.

**Depends on:** Tests 5+6 (STOMP connections established)

**Steps:**

1. Create a CREATE order on Waldur A via REST API
2. Wait for source STOMP event (order notification from A, 30s timeout)
3. Process order via REST-based `_run_processor_until_order_terminal()`
   (same mechanism as Test 2)
4. Fetch resource info and verify `backend_id` on A
5. If target STOMP is active, wait for target STOMP event (30s timeout)
6. Snapshot resource state on A

**Expected:**

- Source STOMP event received with matching `order_uuid` and
  `order_state=pending-consumer`
- Order reaches terminal state (DONE) via REST processing
- Resource on A has `backend_id` set
- If target STOMP active: target event may be captured (timing-dependent)

**Cleanup (Test 7b):**

The test class includes a cleanup test that terminates the resource
created in Test 7, using the same REST processor mechanism.

### Test 8: Fallback When Target STOMP Unavailable

**Purpose:** Verify federation works in polling mode when Waldur B
does not have STOMP-over-WebSocket configured.

**Config:**

```yaml
backend_settings:
  target_stomp_enabled: false  # or omit entirely
```

**Steps:**

1. Start agent in `order_process` mode (polling)
2. Create order on A
3. Processor creates resource on B (non-blocking)
4. Processor polls `check_pending_order()` on subsequent cycles
5. Auto-approve on B (in tests) or wait for B's backend processor
6. `check_pending_order()` returns `True` when target order DONE

**Expected:**

- Same end result as STOMP mode, but with polling delay
  (max: `order_poll_timeout` seconds)
- This is the same flow as Tests 1-4 above

## Known Issues

### 1. `set_state_done` Returns HTTP 500

The `set_state_done` API endpoint on some Waldur staging instances
intermittently returns HTTP 500. The flow:

1. Backend operation succeeds (resource created/updated/terminated on B)
2. `_process_create_order()` returns `True`
3. Processor calls `marketplace_orders_set_state_done.sync_detailed()`
4. Server returns HTTP 500 -> `UnexpectedStatus` exception
5. Generic exception handler at `processors.py:573` catches it
6. Handler calls `set_state_erred` -> order marked ERRED despite success

**Impact:** Tests must tolerate ERRED state and verify actual
resource state.

**Mitigation:** `_run_processor_until_order_terminal()` returns the
final `OrderState` without failing. Tests verify resource state
regardless of order state.

### 2. Shadow Resource on Update/Terminate Orders

Waldur creates a "shadow" resource entry for Update and Terminate
orders. The order's `marketplace_resource_uuid` points to the shadow:

- Empty `backend_id`
- Empty `name`

The original resource retains its `backend_id` and is in `OK` state.

**Impact:**

- Update: `ValueError: badly formed hexadecimal UUID string`
  when calling `UUID("")`
- Terminate: `Empty backend_id for resource, skipping deletion`

**Planned fix:** `_resolve_resource_backend_id()` helper in
`processors.py` that falls back to listing resources in the same
offering+project when `backend_id` is empty.

### 3. Target STOMP WebSocket Not Configured

Some Waldur instances do not have the `/rmqws-stomp` WebSocket proxy
configured in nginx.

**Verification:**

```bash
# WebSocket endpoint available (correct):
curl -sI https://<host>/rmqws-stomp
# HTTP/2 426 (Upgrade Required)

# WebSocket not configured (serves frontend instead):
curl -sI https://<host>/rmqws-stomp
# HTTP/2 200 text/html
```

**Impact:** Target STOMP subscriptions cannot connect. Agent falls
back to polling via `check_pending_order()`.

**Fix:** Configure nginx to proxy `/rmqws-stomp` to RabbitMQ's
`rabbitmq_web_stomp` plugin (typically port 15674).

### 4. Source STOMP Reconnections

STOMP connections may disconnect and reconnect periodically. Likely
caused by heartbeat timeout mismatch between client (10s) and server.

**Impact:** Functional but generates log noise. May miss events during
reconnection window.

## Test Infrastructure

### MessageCapture (conftest.py)

Thread-safe STOMP message capture for automated tests. Wraps or
replaces STOMP `on_message_callback` handlers on `WaldurListener`.

- Source handlers: replaced with capture-only (no order processing)
- Target handlers: wrapped with capture + delegate to original handler

```python
class MessageCapture:
    def make_handler(self, delegate=None):
        # Returns STOMP handler: (frame, offering, user_agent) -> None
        # Captures message, signals waiters, optionally delegates
    def wait_for_order_event(self, order_uuid, timeout=60):
        # Blocks until ORDER event with matching UUID, or timeout
```

### AutoApproveWaldurBackend (conftest.py)

Extends `WaldurBackend`. Overrides `check_pending_order()` to
auto-approve `PENDING_PROVIDER` orders on B. Required because there
is no real backend processor (e.g., SLURM site agent) running on B
in tests.

```python
class AutoApproveWaldurBackend(WaldurBackend):
    def check_pending_order(self, order_backend_id: str) -> bool:
        # PENDING_PROVIDER -> approve via API, return False
        # DONE -> return True
        # ERRED/CANCELED/REJECTED -> raise BackendError
```

### AutoApproveWaldurClient (integration_helpers.py)

Extends `WaldurClient`. Overrides `poll_order_completion()` to
auto-approve `PENDING_PROVIDER` orders. Used by the backend for
synchronous operations (update limits, terminate).

### _run_processor_until_order_terminal (test_e2e_federation.py)

Runs `process_offering()` in a loop (max 15 cycles, 3s between).
Returns the final `OrderState` without failing on ERRED.

## File Inventory

| File | Purpose |
|---|---|
| `conftest.py` | Fixtures: config, offering, clients, AutoApproveWaldurBackend, MessageCapture |
| `test_e2e_federation.py` | REST polling E2E tests (create -> update -> terminate) |
| `test_e2e_stomp.py` | STOMP event E2E tests (connections + event capture + order flow) |
| `../integration_helpers.py` | WaldurTestSetup, AutoApproveWaldurClient |
| `../../waldur_site_agent_waldur/backend.py` | WaldurBackend with target STOMP |
| `../../waldur_site_agent_waldur/target_event_handler.py` | STOMP handler for B's ORDER events |
| `../../waldur_site_agent_waldur/schemas.py` | Pydantic validation for backend settings |
