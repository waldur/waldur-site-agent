# Integration Test Report: Username Sync and STOMP Event Routing

**Date:** 2026-02-26T21:55:00Z
**Branch:** `feature/sync-usernames-waldur`
**Waldur:** `http://localhost:8000/api/`
**RabbitMQ:** `localhost:15674/ws`
**Result:** 18 passed, 0 failed (117.18s)

## Test Suites

| Suite | Tests | Result |
|-------|-------|--------|
| TestUsernameSyncIntegration | 6 | All passed |
| TestIdentityManagerEventRouting | 5 | All passed |
| TestPeriodicReconciliationIntegration | 7 | All passed |

## Permission Model Under Test

| Role | User | Permissions |
|------|------|-------------|
| user_a | OFFERING.MANAGER on A | List/manage offering users, agent identity |
| user_b | CUSTOMER.OWNER on C (non-SP) + IDM | Offering user access via ISD overlap |
| subject_user | Regular user with `active_isds` | Offering user on both offerings |

## Suite 1: TestUsernameSyncIntegration

Polling-based username synchronization from Waldur B to Waldur A.

**Key change:** Offering users are now auto-created via Waldur's natural
`role_granted` -> `create_or_restore_offering_users_for_user` flow instead
of being manually POST'd to `/api/marketplace-offering-users/`.

### test_01 — Environment Setup and Token Verification

Creates all entities, assigns roles, creates resources, triggers offering
user auto-creation, verifies access.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API (staff)
    participant Django as Django ORM (shell)

    Note over Test,Waldur: Entity creation (staff token)
    Test->>Waldur: POST /api/marketplace-categories/
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST /api/customers/ (customer A, B)
    Waldur-->>Test: 201 Created (x2)
    Test->>Waldur: POST /api/marketplace-service-providers/ (SP A, B)
    Waldur-->>Test: 201 Created (x2)
    Test->>Waldur: POST /api/projects/ (project A)
    Waldur-->>Test: 201 Created

    Note over Test,Waldur: Offerings A + B (Marketplace.Slurm)
    Test->>Waldur: POST /api/marketplace-provider-offerings/ (A)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST .../create_offering_component/ (cpu, mem)
    Waldur-->>Test: 201 Created (x2)
    Test->>Waldur: POST /api/marketplace-plans/
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST .../activate/
    Waldur-->>Test: 200 OK
    Test->>Django: SET plugin_options.service_provider_can_create_offering_user=True
    Note over Test: (Repeat for offering B)

    Note over Test,Waldur: Users and roles
    Test->>Waldur: POST /api/users/ (user_a, user_b, subject_user)
    Waldur-->>Test: 201 Created (x3)
    Test->>Waldur: POST /api/customers/ (customer C, non-SP)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST .../add_user/ (user_a → OFFERING.MANAGER on A)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST /api/customers/.../add_user/ (user_b → CUSTOMER.OWNER on C)
    Waldur-->>Test: 201 Created
    Test->>Waldur: PATCH /api/users/.../ (user_b: is_identity_manager, managed_isds)
    Waldur-->>Test: 200 OK
    Test->>Waldur: POST /api/identity-bridge/ (push subject_user)
    Waldur-->>Test: 200 OK

    Note over Test,Django: Resource creation + offering user auto-creation
    Test->>Django: Resource.objects.create(offering=A, project=project_A, state=OK)
    Django-->>Test: resource_a_uuid
    Test->>Waldur: POST /api/projects/.../add_user/ (subject_user → PROJECT.MEMBER on A)
    Waldur-->>Test: 201 Created
    Test->>Django: create_or_restore_offering_users_for_user(subject_user, project_A)
    Note over Django: Auto-creates offering user on A (state=CREATION_REQUESTED)

    Test->>Waldur: POST /api/projects/ (project B under customer C)
    Waldur-->>Test: 201 Created
    Test->>Django: Resource.objects.create(offering=B, project=project_B, state=OK)
    Django-->>Test: resource_b_uuid
    Test->>Waldur: POST /api/projects/.../add_user/ (subject_user → PROJECT.MEMBER on B)
    Waldur-->>Test: 201 Created
    Test->>Django: create_or_restore_offering_users_for_user(subject_user, project_B)
    Note over Django: Auto-creates offering user on B (state=CREATION_REQUESTED)

    Note over Test,Waldur: Verify role-based access
    Test->>Waldur: GET /api/marketplace-offering-users/?offering_uuid=... (user_a token)
    Waldur-->>Test: 200 OK (user_a sees offering A users)
    Test->>Waldur: GET /api/marketplace-offering-users/?offering_uuid=... (user_b token)
    Waldur-->>Test: 200 OK (user_b sees offering B users via ISD)
```

### test_02 — Verify Offering Users Auto-Created

Polls for auto-created offering users on both offerings. Transitions A to OK state.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API (staff)

    Note over Test,Waldur: Poll for auto-created offering user on A
    Test->>Waldur: GET /api/marketplace-offering-users/?offering_uuid=... (poll)
    Waldur-->>Test: 200 OK [{uuid: ..., state: Requested, user_uuid: subject_user}]
    Note over Test: Found! Auto-created in CREATION_REQUESTED state ✓

    Test->>Waldur: PATCH /api/marketplace-offering-users/.../ (username=placeholder)
    Waldur-->>Test: 200 OK → state: OK

    Note over Test,Waldur: Poll for auto-created offering user on B
    Test->>Waldur: GET /api/marketplace-offering-users/?offering_uuid=... (poll)
    Waldur-->>Test: 200 OK [{uuid: ..., state: Requested, user_uuid: subject_user}]
    Note over Test: Found! Auto-created in CREATION_REQUESTED state ✓
```

### test_03 — Set Target Username on B

Sets the "real" username on offering B (transitions CREATION_REQUESTED -> OK).

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API (staff)

    Test->>Waldur: PATCH /api/marketplace-offering-users/.../ (username=inttest-sync-...)
    Waldur-->>Test: 200 OK (state: OK)
    Note over Test: Target username set on B, state transitioned to OK
```

### test_04 — Sync Usernames (B → A)

Calls `sync_offering_user_usernames()` which reads B, compares with A, patches mismatches.

```mermaid
sequenceDiagram
    participant Agent as sync_offering_user_usernames()
    participant B as Waldur B (user_b token)
    participant A as Waldur A (user_a token)

    Agent->>B: GET /api/marketplace-offering-users/?offering_uuid=...&state=OK&page_size=100
    B-->>Agent: 200 OK [{uuid: ..., username: inttest-sync-...}]
    Agent->>A: GET /api/marketplace-offering-users/?offering_uuid=...&state=OK&state=Creating&state=Requested&page_size=100
    A-->>Agent: 200 OK [{uuid: ..., username: placeholder}]
    Note over Agent: Mismatch detected: placeholder ≠ inttest-sync-...
    Agent->>A: PATCH /api/marketplace-offering-users/.../ (username=inttest-sync-...)
    A-->>Agent: 200 OK

    Note over Agent: Verify
    Agent->>A: GET /api/marketplace-offering-users/.../
    A-->>Agent: 200 OK (username=inttest-sync-...) ✓
```

### test_05 — Idempotent Second Sync

Runs sync again — no PATCH needed since usernames already match.

```mermaid
sequenceDiagram
    participant Agent as sync_offering_user_usernames()
    participant B as Waldur B
    participant A as Waldur A

    Agent->>B: GET /api/marketplace-offering-users/?...&state=OK&page_size=100
    B-->>Agent: 200 OK [{username: inttest-sync-...}]
    Agent->>A: GET /api/marketplace-offering-users/?...&state=OK&state=Creating&state=Requested&page_size=100
    A-->>Agent: 200 OK [{username: inttest-sync-...}]
    Note over Agent: Usernames match — no PATCH needed ✓
```

### test_06 — Cleanup

Deletes auto-created offering users and all entities.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API (staff)

    Test->>Waldur: DELETE /api/marketplace-offering-users/.../ (A)
    Waldur-->>Test: 204 No Content
    Test->>Waldur: DELETE /api/marketplace-offering-users/.../ (B)
    Waldur-->>Test: 204 No Content
    Test->>Waldur: DELETE users, customers, offerings, projects, SPs
    Waldur-->>Test: 204 No Content
```

## Suite 2: TestIdentityManagerEventRouting

STOMP event delivery to OFFERING.MANAGER (user_a) and ISD identity manager (user_b).

### test_01 — Verify Prerequisites

Same entity setup as Suite 1 plus STOMP availability check.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API (staff)
    participant RMQ as RabbitMQ

    Note over Test,RMQ: Setup (same as Suite 1)
    Test->>Waldur: Create category, customers, SPs, project, offerings, users, roles
    Waldur-->>Test: All 201/200

    Note over Test,RMQ: STOMP availability
    Test->>RMQ: GET http://localhost:15674/ws
    RMQ-->>Test: 426 Upgrade Required ✓ (WebSocket available)
```

### test_02 — Setup STOMP Subscriptions

Registers agent identities and STOMP subscriptions for both users.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API
    participant RMQ as RabbitMQ

    Note over Test,RMQ: user_a STOMP setup (OFFERING.MANAGER path)
    Test->>Waldur: POST .../add_user/ (user_a → OFFERING.MANAGER on offering B)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST /api/marketplace-site-agent-identities/ (name=inttest-ua)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST .../register_event_subscription/ (offering_user)
    Waldur-->>Test: 201 Created
    Test->>RMQ: PUT /api/permissions/.../test (grant vhost access)
    RMQ-->>Test: 204 No Content
    Test->>Waldur: POST /api/event-subscriptions/.../create_queue/
    Waldur-->>Test: 201 Created
    Test->>RMQ: STOMP CONNECT + SUBSCRIBE
    Note over Test: user_a connected=True ✓

    Note over Test,RMQ: user_b STOMP setup (ISD identity manager path)
    Test->>Waldur: POST /api/marketplace-site-agent-identities/ (name=inttest-ub)
    Waldur-->>Test: 201 Created
    Test->>Waldur: POST .../register_event_subscription/ (offering_user)
    Waldur-->>Test: 201 Created
    Test->>RMQ: STOMP CONNECT + SUBSCRIBE
    Note over Test: user_b connected=True ✓
```

### test_03 — Trigger and Verify Events

Creates an offering user on offering B, patches username, verifies both subscribers receive events.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API
    participant RMQ as RabbitMQ
    participant UA as user_a STOMP
    participant UB as user_b STOMP

    Note over Test,UB: Create offering user on B + set username
    Test->>Waldur: POST /api/marketplace-offering-users/ (subject_user on offering B)
    Waldur-->>Test: 201 Created
    Test->>Waldur: PATCH /api/marketplace-offering-users/.../ (username=stomp-test-...)
    Waldur-->>Test: 200 OK

    Note over Waldur,UB: STOMP events delivered
    RMQ->>UA: MESSAGE {action: update, username: stomp-test-...} ✓
    RMQ->>UB: MESSAGE {action: update, username: stomp-test-...} ✓
```

### test_04 — Verify Events After Clearing ISDs

Clears user_b's `managed_isds`, triggers another username change, verifies user_b stops receiving events.

```mermaid
sequenceDiagram
    participant Test
    participant Waldur as Waldur API
    participant UA as user_a STOMP
    participant UB as user_b STOMP

    Test->>Waldur: PATCH /api/users/.../ (managed_isds=[])
    Waldur-->>Test: 200 OK

    Test->>Waldur: PATCH /api/marketplace-offering-users/.../ (username=no-isd-...)
    Waldur-->>Test: 200 OK

    Note over UA: user_a received event ✓
    Note over UB: user_b did NOT receive event ✓ (no ISD access)

    Test->>Waldur: PATCH /api/users/.../ (managed_isds=[isd:integration-test])
    Waldur-->>Test: 200 OK (restore)
```

### test_05 — Cleanup

Disconnects STOMP, deletes all entities.

## Suite 3: TestPeriodicReconciliationIntegration

Tests `run_periodic_username_reconciliation()` end-to-end.

### test_01 — Verify Prerequisites (Suite 3)

Same entity setup as Suite 1 (separate env instance).

### test_02 — Create Offering Users (Suite 3)

Creates offering users on both offerings (uses manual POST since this suite
tests reconciliation logic, not auto-creation).

### test_03 — Set Target Username (Suite 3)

Sets username on offering B.

### test_04 — Run Periodic Reconciliation

Calls `run_periodic_username_reconciliation()` which internally calls `sync_offering_user_usernames()`.

```mermaid
sequenceDiagram
    participant Agent as run_periodic_username_reconciliation()
    participant B as Waldur B
    participant A as Waldur A

    Agent->>B: GET /api/marketplace-offering-users/?...&state=OK&page_size=100
    B-->>Agent: 200 OK [{username: reconcile-...}]
    Agent->>A: GET /api/marketplace-offering-users/?...&state=OK&state=Creating&state=Requested&page_size=100
    A-->>Agent: 200 OK [{username: placeholder}]
    Note over Agent: Mismatch detected
    Agent->>A: PATCH /api/marketplace-offering-users/.../ (username=reconcile-...)
    A-->>Agent: 200 OK ✓
```

### test_05 — Idempotent Second Reconciliation

Second call is a no-op (usernames already match).

### test_06 — Skips Non-qualifying Offering

Verifies reconciliation skips offerings without `stomp_enabled` or `membership_sync_backend`.

### test_07 — Cleanup

Deletes offering users and entities.

## Key Design Decisions

### Natural Offering User Auto-Creation (Suite 1)

The `env` fixture uses Waldur's natural flow for creating offering users:

1. **Resource creation via Django ORM** — `Marketplace.Slurm` offerings can't create
   orders via API without a real SLURM backend (no `scope`/service_settings). Resources
   are created directly with `state=Resource.States.OK`.

2. **Project role assignment via API** — `POST /api/projects/{uuid}/add_user/` fires
   the `role_granted` signal.

3. **Task invocation via Django shell** — Since there's no Celery worker with `runserver`,
   the `create_or_restore_offering_users_for_user` task is called directly via
   `uv run waldur shell -c "..."`.

This produces offering users in `CREATION_REQUESTED` state with empty username
(`username_generation_policy=service_provider`), matching production behavior.

### STOMP Message Summary (Suite 2)

| # | Action | Received by |
|---|--------|-------------|
| 1-5 | create, update, username_set | user_a + user_b |
| 6-7 | update, username_set (after clearing ISDs) | user_a only |

**Key finding:** After clearing `managed_isds` on user_b, event publishing correctly
filters based on ISD identity manager access.
