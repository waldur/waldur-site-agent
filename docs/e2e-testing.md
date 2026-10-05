# E2E Testing

End-to-end tests validate the site agent against a real Waldur instance
with a SLURM emulator backend. Orders complete synchronously — no remote
cluster or second Waldur instance is needed.

## Architecture

```text
┌───────────────────────────────────────────┐
│ Test runner (pytest)                      │
│                                           │
│  ┌────────────┐  ┌─────────────────────┐ │
│  │ Waldur API │  │ SLURM emulator      │ │
│  │ client     │  │ (.venv/bin/sacctmgr)│ │
│  └─────┬──────┘  └──────────┬──────────┘ │
│        │ REST API           │ CLI calls   │
│        ▼                    ▼             │
│  ┌────────────────────────────────────┐  │
│  │ OfferingOrderProcessor /           │  │
│  │ OfferingMembershipProcessor /      │  │
│  │ OfferingReportProcessor            │  │
│  └────────────────────────────────────┘  │
└───────────────────────────────────────────┘
         │
         ▼
┌───────────────────────────────────────────┐
│ Docker stack (ci/docker-compose.e2e.yml)  │
│                                           │
│  PostgreSQL 16  ─  RabbitMQ (ws:15674)   │
│  Waldur API     ─  Waldur Celery worker  │
└───────────────────────────────────────────┘
```

The Docker stack boots PostgreSQL, RabbitMQ (with `rabbitmq_web_stomp`),
and Waldur Mastermind (API + Celery worker). A demo preset
(`ci/site_agent_e2e.json`) loads the users, projects, offerings, plans, components,
and role assignments.

## Test suites

Each file reads one agent-config variable and runs in one CI job (see
[CI pipeline](#ci-pipeline)). `tests/test_docs_e2e_index.py` fails when a
`test_e2e_*.py` file has no row here, when its Job column disagrees with the
job whose `script` runs it, when a file runs in no job (unless its section says
the suite is manual) or in several, and when a CI job or a config variable is
missing from this page. It does not check the Config column.

### SLURM (`plugins/slurm/tests/e2e/`)

Config: the `WALDUR_E2E_*_CONFIG` variable the file reads (`CONFIG` =
`WALDUR_E2E_CONFIG`, `STOMP` = `WALDUR_E2E_STOMP_CONFIG`, and so on). Job:
`REST` = `E2E: REST & STOMP`, `policy` = `E2E: policy, QoS & LDAP`,
`matrix` = `E2E: QoS matrix`.

| File | Config | Job | What it validates |
|------|--------|-----|-------------------|
| `test_e2e_api_optimizations.py` | CONFIG | REST | Order lifecycle, membership sync, reporting |
| `test_e2e_benchmark.py` | CONFIG | REST | API call counts and response sizes; scales to N resources |
| `test_e2e_default_account_policy.py` | CONFIG | REST | `default_account_policy` sets the DefaultAccount |
| `test_e2e_membership_stale.py` | MEMBERSHIP | REST | Which backend users membership sync removes or keeps |
| `test_e2e_order_reconciliation.py` | CONFIG | REST | Periodic order reconciliation recovers stuck orders |
| `test_e2e_partition_associations.py` | CONFIG | REST | Offering partitions applied to user associations |
| `test_e2e_prepaid.py` | CONFIG | REST | Prepaid billing model |
| `test_e2e_qos_backcompat.py` | CONFIG | REST | Backwards compatibility of the periodic-limits handler |
| `test_e2e_resources_sync.py` | CONFIG | REST | Forced resources sync after SLURM data loss |
| `test_e2e_rest_api.py` | — ² | REST | `SlurmRestClient` against the emulator's REST API |
| `test_e2e_restore.py` | CONFIG | REST | Restoring a resource from TERMINATED |
| `test_e2e_stomp.py` | STOMP | REST | STOMP connections, event delivery, orders with STOMP |
| `test_e2e_ldap.py` | LDAP ³ | policy | LDAP-integrated SLURM backend |
| `test_e2e_policy.py` | POLICY | policy | Periodic usage policy evaluation |
| `test_e2e_qos_polling.py` | POLICY | policy | QoS application via the polling path |
| `test_e2e_qos_stomp.py` | STOMP | policy | QoS application via STOMP RESOURCE events |
| `test_e2e_qos_matrix.py` | POLICY | matrix | QoS sweep across all 11 policy configurations |

² Runs against the emulator's slurmrestd; no agent config.
³ `WALDUR_E2E_LDAP_CONFIG`, `WALDUR_E2E_LDAP_INVERTED_CONFIG` and
`WALDUR_E2E_LDAP_PROJECT_GROUPS_CONFIG`.

### ldap-roles (`plugins/ldap-roles/tests/e2e/`)

| File | Config | Job | What it validates |
|------|-----------------|--------|-------------------|
| `test_e2e_ldap_roles.py` | LDAP_ROLES | policy | ldap-roles membership-sync backend |

### Azure (`plugins/azure/tests/e2e/`)

| File | Config | Job | What it validates |
|------|-----------------|--------|-------------------|
| `test_e2e_orders.py` | CONFIG | `E2E: Azure` | Orders carried through the agent onto Azure (SDK faked) |

### Waldur federation (`plugins/waldur/tests/e2e/`)

These need two Waldur instances (A and B) and are **not wired into CI**; run
them by hand against a federation pair. See `plugins/waldur/tests/e2e/TEST_PLAN.md`.

| File | Config | What it validates |
|------|-----------------|-------------------|
| `test_e2e_federation.py` | CONFIG | Waldur A → Waldur B order processing |
| `test_e2e_membership_sync.py` | CONFIG | Membership sync across federation |
| `test_e2e_offering_user_pubsub.py` | CONFIG | OFFERING_USER attribute sync via STOMP |
| `test_e2e_order_rejection.py` | CONFIG | Order rejection handling |
| `test_e2e_stomp.py` | CONFIG | STOMP event routing for federation |
| `test_e2e_usage_sync.py` | CONFIG | Usage reporting B → A |
| `test_e2e_username_sync.py` | CONFIG | Username reconciliation B → A |

## Running locally

### Prerequisites

1. A running Waldur instance with demo data loaded
2. `uv sync --all-packages` (installs core + all plugins + slurm-emulator)
3. A config YAML pointing at your Waldur instance

### Boot the Docker stack (optional — for a fresh local instance)

```bash
docker compose -f ci/docker-compose.e2e.yml up waldur-db-migration
docker compose -f ci/docker-compose.e2e.yml up -d
# add `--profile ldap` (or COMPOSE_PROFILES=ldap) to also start OpenLDAP for test_e2e_ldap.py

# Wait for API to be ready
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/
# Should return 401

# Load demo preset
docker compose -f ci/docker-compose.e2e.yml exec waldur-api \
  waldur demo_presets load site_agent_e2e --no-cleanup
```

### Create a local config

Copy `ci/e2e-ci-config.yaml` and change the API host from `docker` to
`localhost`:

<!-- docs-check: skip -->

```yaml
# e2e-local-config.yaml
offerings:
  - name: "E2E SLURM Usage"
    waldur_api_url: "http://localhost:8080/api/"
    waldur_api_token: "e2e0000000000000000000000000token001"
    waldur_offering_uuid: "e2ef0000000000000000000000000001"
    stomp_enabled: false
    # ... rest same as ci/e2e-ci-config.yaml
```

For STOMP tests, create a second config with `stomp_enabled: true` and
STOMP connection settings:

<!-- docs-check: skip -->

```yaml
# e2e-local-config-stomp.yaml
offerings:
  - name: "E2E SLURM STOMP"
    waldur_api_url: "http://localhost:8080/api/"
    waldur_api_token: "e2e0000000000000000000000000token001"
    waldur_offering_uuid: "e2ef0000000000000000000000000001"
    stomp_enabled: true
    stomp_ws_host: "localhost"
    stomp_ws_port: 15674
    stomp_ws_path: "/ws"
    websocket_use_tls: false
    # ... rest same as ci/e2e-ci-config-stomp.yaml
```

### Run the tests

```bash
# REST E2E tests (API optimizations + benchmarks)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=e2e-local-config.yaml \
WALDUR_E2E_PROJECT_A_UUID=e2eb0000000000000000000000000001 \
.venv/bin/python -m pytest plugins/slurm/tests/e2e/ -v \
  --ignore=plugins/slurm/tests/e2e/test_e2e_stomp.py

# STOMP E2E tests
WALDUR_E2E_TESTS=true \
WALDUR_E2E_STOMP_CONFIG=e2e-local-config-stomp.yaml \
WALDUR_E2E_PROJECT_A_UUID=e2eb0000000000000000000000000001 \
.venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_stomp.py -v

# Multi-resource benchmark (default N=800, reduce for quick runs)
WALDUR_E2E_TESTS=true \
WALDUR_E2E_CONFIG=e2e-local-config.yaml \
WALDUR_E2E_PROJECT_A_UUID=e2eb0000000000000000000000000001 \
WALDUR_E2E_BENCH_RESOURCES=10 \
.venv/bin/python -m pytest plugins/slurm/tests/e2e/test_e2e_benchmark.py -v -k multi
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `WALDUR_E2E_TESTS` | Yes | Set to `true` to enable E2E tests (skipped otherwise) |
| `WALDUR_E2E_PROJECT_A_UUID` | Yes | Project UUID on Waldur to create orders in |
| `WALDUR_E2E_CONFIG` | Per file | Agent config for the REST suites (`stomp_enabled: false`) |
| `WALDUR_E2E_STOMP_CONFIG` | Per file | Agent config with `stomp_enabled: true` and WebSocket settings |
| `WALDUR_E2E_MEMBERSHIP_CONFIG` | Per file | Config for `test_e2e_membership_stale.py` |
| `WALDUR_E2E_POLICY_CONFIG` | Per file | Config with periodic-limits policies (policy and QoS suites) |
| `WALDUR_E2E_LDAP_CONFIG` | Per file | SLURM + LDAP config |
| `WALDUR_E2E_LDAP_INVERTED_CONFIG` | Per file | SLURM + LDAP config with inverted group mapping |
| `WALDUR_E2E_LDAP_PROJECT_GROUPS_CONFIG` | Per file | SLURM + LDAP config writing project groups |
| `WALDUR_E2E_LDAP_ROLES_CONFIG` | Per file | ldap-roles backend config |
| `WALDUR_E2E_BENCH_RESOURCES` | No | Number of resources for multi-resource benchmark (default: 800, CI uses 5) |

The CI values are the `ci/e2e-ci-config*.yaml` files; the [test suite tables](#test-suites)
show which file reads which variable.

## CI pipeline

The E2E suites run as four jobs in `.gitlab-ci.yml`, all extending a shared
`.E2E base` that boots its own Waldur stack:

- `E2E: REST & STOMP` — the SLURM REST suites, `test_e2e_stomp.py`,
  `test_e2e_rest_api.py`. Runs on MRs touching slurm/ldap/ldap-client/ldap-roles/azure/core/ci
  files, on tags, and when `RUN_E2E_TESTS` / `RUN_E2E` is set.
- `E2E: policy, QoS & LDAP` — `test_e2e_policy.py`, `test_e2e_qos_polling.py`,
  `test_e2e_qos_stomp.py`, `test_e2e_ldap.py` and ldap-roles'
  `test_e2e_ldap_roles.py`. Same triggers as above.
- `E2E: Azure` — `plugins/azure/tests/e2e/test_e2e_orders.py`, with the Azure
  SDK faked in-process. Same triggers as above.
- `E2E: QoS matrix` — `test_e2e_qos_matrix.py` (11 policy configs, ~5 min).
  Runs on tags; on `main` / MRs only with `RUN_E2E_TESTS`.

The waldur federation suite is not run in CI.

The REST job lists its test files explicitly. A new `test_e2e_*.py` must be
added to exactly one job's `script`, otherwise it never runs in CI — and to
the tables on this page; `tests/test_docs_e2e_index.py` checks both.

### CI flow (per job)

1. Install Docker CLI + Compose plugin (static binaries)
2. `uv sync --all-packages` — install site-agent + slurm-emulator
3. `docker compose -f ci/docker-compose.e2e.yml up` — boot Waldur stack
4. Wait for the Celery worker and the API (`curl http://docker:8080/api/`)
5. Copy and load `site_agent_e2e` demo preset
6. Force-set deterministic auth token
7. One `pytest` invocation over the job's test files, with coverage disabled
   (`-o addopts=`) and every `WALDUR_E2E_*_CONFIG` variable the files need
8. Collect JUnit XML reports, stack logs, and markdown reports as artifacts

Steps 1-6 live in `before_script` and cost ~3 min per job; splitting the
suites across jobs trades that for running them in parallel (~21 min serial
→ ~10 min wall).

### CI files

| File | Purpose |
|------|---------|
| `ci/docker-compose.e2e.yml` | Minimal Waldur stack: PostgreSQL, RabbitMQ (with web_stomp), API + worker |
| `ci/e2e-ci-config.yaml` | REST config: SLURM usage/limits/mixed and the Azure offering, no STOMP |
| `ci/e2e-ci-config-stomp.yaml` | STOMP test config: a SLURM offering and a policy offering, `stomp_enabled: true` |
| `ci/e2e-ci-config-membership.yaml` | `WALDUR_E2E_MEMBERSHIP_CONFIG` |
| `ci/e2e-ci-config-policy.yaml` | `WALDUR_E2E_POLICY_CONFIG` |
| `ci/e2e-ci-config-ldap.yaml`, `-ldap-inverted.yaml`, `-ldap-project-groups.yaml` | The three SLURM + LDAP configs |
| `ci/e2e-ci-config-ldap-roles.yaml` | `WALDUR_E2E_LDAP_ROLES_CONFIG` |
| `ci/ldap-seed.ldif` | OpenLDAP seed data (`--profile ldap`) |
| `ci/site_agent_e2e.json` | Demo preset: users, projects, offerings, plans, components, roles, offering users |
| `ci/override.conf.py` | Mastermind Django settings (Celery broker, RabbitMQ STOMP) |
| `ci/rabbitmq-enabled-plugins` | Enables `rabbitmq_management`, `rabbitmq_web_stomp`, `rabbitmq_stomp` |
| `ci/rabbitmq.conf` | RabbitMQ connection and permissions config |
| `ci/createdb-celery_results.sql` | Creates the `celery_results` database for Celery |

### Artifacts

- `e2e-report-*.xml` — JUnit test results (one file per job)
- `waldur-stack-logs.txt` — Docker stack logs for debugging failures
- `plugins/slurm/tests/e2e/*-report.md` — Detailed markdown reports with API call tables
- `plugins/slurm/tests/e2e/*-report.json` — Machine-readable API call counts

## Test reports

Each test run produces a markdown report and a JSON summary in
`plugins/slurm/tests/e2e/`. The markdown report includes:

- Per-test API call tables (method, URL, status, response size)
- Order/resource state snapshots at each processor cycle
- API call summary table (calls and bytes per test)

These reports are useful for tracking API efficiency across changes.

## Troubleshooting

### Tests are skipped

All E2E tests are gated by `WALDUR_E2E_TESTS=true`. If tests show as
"skipped", check that the environment variable is set.

### "WALDUR_E2E_CONFIG not set" / "WALDUR_E2E_STOMP_CONFIG not set"

REST tests need `WALDUR_E2E_CONFIG`, STOMP tests need
`WALDUR_E2E_STOMP_CONFIG`. They use separate config files because
STOMP tests require `stomp_enabled: true` with WebSocket connection
settings.

### STOMP tests skip with "endpoint not reachable"

The STOMP tests check that RabbitMQ's web_stomp endpoint is accessible
before attempting connections. Verify that:

- RabbitMQ is running with `rabbitmq_web_stomp` plugin enabled
- Port 15674 is exposed and reachable
- The `stomp_ws_host` and `stomp_ws_port` in config match your setup

### Order stuck in non-terminal state

The processor runs up to 10 cycles with 2s delays. With the SLURM
emulator, orders should complete in 1 cycle. If orders are stuck:

- Check Waldur API logs for errors
- Verify the demo preset loaded correctly
- Check that the emulator state file (`/tmp/slurm_emulator_db.json`)
  is writable

### CI job times out

The E2E job has a default 1-hour timeout. The Waldur DB migration takes
~14 minutes, REST tests ~2 minutes, STOMP tests ~30 seconds. If the job
times out, check the Docker stack logs artifact for migration issues.
