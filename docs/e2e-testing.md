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
(`ci/site_agent_e2e.json`) loads 6 users, 3 offerings, plans, components,
and role assignments.

## Test suites

### SLURM E2E tests (`plugins/slurm/tests/e2e/`)

| File | Tests | What it validates |
|------|-------|-------------------|
| `test_e2e_api_optimizations.py` | ~20 | Order lifecycle (create/update/terminate), membership sync, reporting |
| `test_e2e_benchmark.py` | ~10 | API call counts and response sizes; scales to N resources |
| `test_e2e_stomp.py` | 4 | STOMP WebSocket connections, event delivery, order processing with STOMP active |

### Waldur federation E2E tests (`plugins/waldur/tests/e2e/`)

| File | Tests | What it validates |
|------|-------|-------------------|
| `test_e2e_federation.py` | ~10 | Full Waldur A → Waldur B order processing pipeline |
| `test_e2e_username_sync.py` | ~8 | Username reconciliation between federated instances |
| `test_e2e_usage_sync.py` | ~5 | Usage reporting across federation |
| `test_e2e_stomp.py` | ~5 | STOMP event routing for federation |
| `test_e2e_offering_user_pubsub.py` | ~4 | Offering user attribute sync via STOMP |
| `test_e2e_order_rejection.py` | ~3 | Order rejection handling in federation |

## Running locally

### Prerequisites

1. A running Waldur instance with demo data loaded
2. `uv sync --all-packages` (installs core + all plugins + slurm-emulator)
3. A config YAML pointing at your Waldur instance

### Boot the Docker stack (optional — for a fresh local instance)

```bash
docker compose -f ci/docker-compose.e2e.yml up waldur-db-migration
docker compose -f ci/docker-compose.e2e.yml up -d

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
| `WALDUR_E2E_CONFIG` | For REST tests | Path to agent config YAML (`stomp_enabled: false`) |
| `WALDUR_E2E_STOMP_CONFIG` | For STOMP tests | Path to agent config YAML (`stomp_enabled: true`) |
| `WALDUR_E2E_PROJECT_A_UUID` | Yes | Project UUID on Waldur to create orders in |
| `WALDUR_E2E_BENCH_RESOURCES` | No | Number of resources for multi-resource benchmark (default: 800, CI uses 5) |

## CI pipeline

The E2E job runs in the `E2E integration tests` stage in `.gitlab-ci.yml`.
It triggers on pushes to `main` and release tags.

### CI flow

1. Install Docker CLI + Compose plugin (static binaries)
2. `uv sync --all-packages` — install site-agent + slurm-emulator
3. `docker compose -f ci/docker-compose.e2e.yml up` — boot Waldur stack
4. Wait for API health check (`curl http://docker:8080/api/`)
5. Copy and load `site_agent_e2e` demo preset
6. Force-set deterministic auth token
7. **REST E2E tests** — `pytest plugins/slurm/tests/e2e/ --ignore=test_e2e_stomp.py`
8. **STOMP E2E tests** — `pytest plugins/slurm/tests/e2e/test_e2e_stomp.py`
9. Collect JUnit XML reports, stack logs, and markdown reports as artifacts

The REST and STOMP tests run sequentially in the same job to reuse the
~14min Docker stack boot + migration time.

### CI files

| File | Purpose |
|------|---------|
| `ci/docker-compose.e2e.yml` | Minimal Waldur stack: PostgreSQL, RabbitMQ (with web_stomp), API + worker |
| `ci/e2e-ci-config.yaml` | REST test config: 3 offerings (usage/limits/mixed), `stomp_enabled: false` |
| `ci/e2e-ci-config-stomp.yaml` | STOMP test config: 1 offering, `stomp_enabled: true` |
| `ci/site_agent_e2e.json` | Demo preset: 6 users, 3 offerings, plans, components, roles |
| `ci/override.conf.py` | Mastermind Django settings (Celery broker, RabbitMQ STOMP) |
| `ci/rabbitmq-enabled-plugins` | Enables `rabbitmq_management`, `rabbitmq_web_stomp`, `rabbitmq_stomp` |
| `ci/rabbitmq.conf` | RabbitMQ connection and permissions config |
| `ci/createdb-celery_results.sql` | Creates the `celery_results` database for Celery |

### Artifacts

- `e2e-report-rest.xml` / `e2e-report-stomp.xml` — JUnit test results
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
