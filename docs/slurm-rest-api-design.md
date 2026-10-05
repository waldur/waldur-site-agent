# Design: Optional SLURM REST API (slurmrestd) Execution Mode

Status: implemented
Author: Waldur team

This is a design record for the optional `slurmrestd` execution mode, kept
to explain the non-obvious decisions behind it (hand-rolled client, hybrid
REST/CLI split, version mapping, no silent fallback). The feature is built;
the sections below describe what was implemented. User-facing configuration
lives in [the SLURM plugin README](../plugins/slurm/README.md).

## Summary

Add an optional execution mode to the SLURM plugin that talks to
`slurmrestd` over HTTP instead of shelling out to `sacctmgr` /
`scancel`. The mode is selected per offering via `backend_settings`,
defaults to the existing CLI behavior, and requires no changes to the
core agent or to the `SlurmBackend` business logic.

REST mode covers account, association, user, QOS and limit management,
job listing/cancellation, health checks and usage reporting (aggregated
client-side from `/slurmdb/vX/jobs` records). The only operations that
still shell out to `sacctmgr` are RawUsage resets of an account or a
QOS, which have no REST endpoint.

Usage reporting was originally kept on `sacct` (see section 4 for the
original reasoning); it has since moved to REST.

The implementation is a second client class, `SlurmRestClient`, behind a
shared client interface — a **hand-rolled httpx client pinned to a
configurable API version**, not a generated OpenAPI client.

## Motivation

- The CLI mode requires SLURM client binaries and a working
  munge/SACK auth domain on the agent host. REST mode moves management
  and usage reporting to HTTP + JWT; only RawUsage resets still need
  `sacctmgr` on the host.
- Structured JSON responses remove a whole class of output-parsing
  fragility (`--parsable2` pipe-splitting, locale/format drift between
  SLURM releases).
- Several deployments already run `slurmrestd` for other tooling
  (Slurm-web, AWS PCS, custom portals); integrating with it is becoming
  the standard pattern.

## Background: the CLI architecture

All SLURM interaction funnels through
`plugins/slurm/waldur_site_agent_slurm/client.py`:

- `SlurmClient._execute_command()` builds command lines
  (`sacctmgr --parsable2 --noheader --immediate ...`), injects cluster
  filtering, and runs them via `BaseClient.execute_command()`
  (subprocess).
- Output is parsed into typed structures: `ClientResource`,
  `Association`, `SlurmReportLine`.
- `SlurmBackend.__init__` instantiates `SlurmClient` directly; the
  backend only calls public client methods and never touches command
  syntax itself.
- Non-SLURM operations (homedir creation, project directories, Lustre
  quotas) live on the **backend**, not the client, and behave
  identically in both modes. The user-existence check is a client
  method: `id -u` in CLI mode, `GET /slurmdb/vX/user/{name}` in REST
  mode.

The client's public method surface (~35 methods) is therefore the
natural swap point. Its weakness today: only the 13 `BaseClient`
methods have an interface contract; the ~20 SLURM-specific extension
methods (QOS, fairshare, parents, partitions, job control) do not.

## slurmrestd background

- Two namespaces: `/slurm/vX` (slurmctld: jobs, partitions, ping) and
  `/slurmdb/vX` (slurmdbd: accounts, associations, users, QOS, TRES).
- Content schemas come from versioned `data_parser/v0.0.XX` plugins.
  Each SLURM major release ships ~4 versions; a version lives ~2 years
  (4 major releases), then is removed. As of mid-2026:
  - `v0.0.42` — widest deployed common denominator (SLURM 24.11–26.05).
  - `v0.0.43` — SLURM 25.05–27.05.
- Authentication: JWT via `X-SLURM-USER-TOKEN` + `X-SLURM-USER-NAME`
  headers (or `Authorization: Bearer`). Since SLURM 25.05 running
  slurmrestd as root/SlurmUser is unsupported; the production pattern
  is slurmrestd on a unix socket or localhost TCP, agent authenticating
  as an unprivileged user with `AdminLevel=Administrator` in slurmdbd —
  the same privilege model as the `sacctmgr` path today.
- API quirks that the client must handle:
  - HTTP 200 with a non-empty `errors[]` array in the body — the body,
    not the status code, is authoritative.
  - "Maybe-unset" numbers are tri-state structs
    `{"set": bool, "infinite": bool, "number": N}`; clearing a limit
    means sending `set: false`, not `null`.
  - No pagination — job queries must always be bounded with
    `start_time` / `end_time` (the usage queries are bounded by the
    reporting month).

### Why not a generated OpenAPI client

SchedMD publishes no official Python client and only smoke-tests the
spec against openapi-generator. Community experience with generated
clients is poor: spec validation failures (`--skip-validate-spec`
required), `RecursionError` on package import due to spec size,
generator-version churn, and awkward handling of the tri-state numeric
structs. Production integrators (Slurm-web, AWS PCS consumers)
hand-roll thin clients against the small endpoint set they need. We
need ~15 endpoints; a thin client with a per-version field-mapping
layer is smaller, debuggable, and testable.

## Design

### 1. A shared client interface

`SlurmClientInterface` (ABC, `waldur_site_agent_slurm/interface.py`)
captures the full public surface of `SlurmClient`: the `BaseClient`
methods plus all SLURM-specific extensions (`get/set_account_parent`,
`create_association_with_partition(s)`, QOS management, fairshare,
`cancel_active_user_jobs`, `list_active_user_jobs`,
`get_historical_usage_report`, ...).

Both `SlurmClient` (CLI) and `SlurmRestClient` implement it.
`SlurmBackend` keeps calling `self.client.*` unchanged; its only new
responsibility is choosing the client class from `execution_mode`.

### 2. `SlurmRestClient`

Lives in `waldur_site_agent_slurm/rest_client.py`: a thin transport
layer (`httpx`) with endpoint mappings and payload builders.

- **httpx**, not requests: unix-socket support via
  `httpx.HTTPTransport(uds=...)` and a single client type for both UDS
  and TCP. `httpx` is a regular dependency of the plugin (the
  `[rest]` extra still exists and only raises the minimum to
  `httpx>=0.27`).
- API version is a config string (`api_version`, default `v0.0.43`);
  URLs are templated (`/slurmdb/{ver}/associations`). Payload
  field-name differences between versions are isolated in a small
  mapping layer.
- Every response is checked for body-level `errors[]` / `warnings[]`;
  errors map to `BackendError` (same exception contract as CLI).
  Warnings are logged. Single-entity existence lookups pass
  `allow_errors=True` so a "not found" answer reads as absent rather
  than raising.
- Idempotency semantics mirror the CLI client (e.g. the CLI suppresses
  "Nothing modified" on `modify`; REST equivalents are similarly
  tolerant of no-op updates).
- The `executed_commands` log mirrors the CLI client's for debugging
  and tests; in REST mode it also surfaces the commands run by the
  delegated CLI client (RawUsage resets).
- Responses are converted into the same typed structures
  (`ClientResource`, `Association`, `SlurmReportLine`) so parsers and
  backend logic are reused, not duplicated.

### 3. Operation mapping

- `sacctmgr add/show/remove account` — `POST/GET/DELETE /slurmdb/vX/accounts`, `/account/{name}`
- `sacctmgr add user ... account=...` — `POST /slurmdb/vX/users_association` / `/associations`
- `sacctmgr modify account set GrpTRESMins=...` — `POST /slurmdb/vX/associations` with
  `max.tres.group.minutes`
- `sacctmgr modify ... set qos/fairshare/parent` — association payload fields `qos`, `shares_raw`,
  `parent_account`
- `sacctmgr show association ...` (limits, users, QOS) — `GET
  /slurmdb/vX/associations?account=...&cluster=...`
- QOS create/delete/modify — `GET/POST/DELETE /slurmdb/vX/qos`
- `scancel -A account [-u user]` — `GET /slurm/vX/jobs` filtered, then `DELETE /slurm/vX/job/{id}`
  per job
- `sinfo -V` (version, health check) — `GET /slurm/vX/ping`
- `sacct --starttime --endtime --accounts ...` (usage) — `GET
  /slurmdb/vX/jobs?account=...&start_time=...&end_time=...`, aggregated client-side (section 4)
- `sacctmgr modify account/qos set RawUsage=0` — **Stays on CLI** — no REST equivalent (see
  Limitations)

### 4. Usage reporting

**Original decision (kept for context):** there is no `sreport`
equivalent in the REST API, so usage would have to be re-derived from
raw `/slurmdb/vX/jobs` records. Because that feeds billing, usage
reporting was first left on `sacct` in both modes, with `SlurmRestClient`
delegating it to an internal `SlurmClient`.

**Current implementation:** `get_usage_report` and
`get_historical_usage_report` query `GET /slurmdb/vX/jobs/` for the
month window (with `cluster` when `cluster_name` is set; the account and
cluster are re-checked client-side because older slurmrestd versions
ignore unknown query parameters) and render each job as the same
`Account|TRES|Elapsed|User` line the CLI parser consumes, so the
aggregation downstream is shared with CLI mode:

- **Elapsed** is clipped to the reporting window, as `sacct --truncate`
  does: a job spanning a month boundary is billed to each month only for
  its part inside it; a running job counts until the window closes.
- **TRES** come from the job's `tres.allocated`, falling back to
  `tres.requested` for jobs that never started. The CLI path bills
  `ReqTRES`. The two modes therefore differ for jobs whose allocation
  differs from the request (for example whole-node allocation), which is
  worth checking against `sacct` for a period before switching a billed
  offering to REST mode.

### 5. Authentication

Config supports, in order of preference:

1. `token_file` — path to a JWT, re-read on HTTP 401, so an external
   rotator (e.g. a cron job running `scontrol token`) keeps the agent
   working without restarts.
2. `token_env` — environment variable name holding a static token
   (consistent with existing secret handling in the agent).

Future (not in initial scope): self-minted HS256 tokens from a shared
`jwt_key`, and JWKS-based RS256 for Keycloak-integrated sites.

The client sends both `X-SLURM-USER-TOKEN` and `X-SLURM-USER-NAME`.

### 6. Configuration

Backward compatible — absence of the new keys means CLI mode:

<!-- docs-check: skip -->

```yaml
offerings:
  - name: "My SLURM Cluster"
    backend_type: "slurm"
    backend_settings:
      # ... all existing settings unchanged ...
      execution_mode: "rest"      # "cli" (default) | "rest"
      rest_api:
        url: "unix:///run/slurmrestd/slurmrestd.sock"  # or http://localhost:6820
        api_version: "v0.0.43"   # default; pin to what your SLURM ships
        username: "waldur-agent"
        token_file: "/etc/waldur/slurmrestd.token"
        # token_env: SLURM_JWT     # alternative to token_file
        verify_ssl: true           # for TLS-terminating reverse proxies
```

`SlurmBackendSettingsSchema` has an `execution_mode` enum and a nested
`rest_api` model (required iff `execution_mode: rest`, together with
`cluster_name`). `diagnostics()` logs the execution mode and the SLURM
version from `GET /slurm/vX/ping`; `ping()` lists accounts. The full key
reference is in the [SLURM plugin README](../plugins/slurm/README.md#rest-api-execution-mode).

### 7. Limitations (documented, not worked around silently)

- RawUsage resets (`reset_raw_usage`, `reset_qos_raw_usage`) have no
  REST endpoint and stay on the CLI client. Without `sacctmgr` on the
  host they fail with an explicit `BackendError` instead of a
  "command not found".
- Usage figures are aggregated client-side from job records and bill
  allocated TRES, so they can differ from CLI-mode (`sacct ReqTRES`)
  figures (section 4).
- Beyond the delegated resets, there is no
  automatic REST→CLI fallback on errors: silent divergence between
  modes is worse than a loud failure. The mode is an explicit
  per-offering choice.
- The slurmdb write path is younger than the CLI (crash in
  `accounts_association` and a QOS POST memory leak were fixed only in
  SLURM 25.11). REST mode therefore stays **opt-in** with CLI as the
  default for the foreseeable future, and the docs state the minimum
  recommended SLURM version (25.11) for REST mode even where older
  API versions would technically work.

## Testing

- **Unit tests** (`tests/test_rest_client.py`) with `httpx.MockTransport`,
  asserting request paths, query params, and payloads — the REST analogue
  of `tests/test_command_construction.py`.
- **Config tests** (`tests/test_rest_mode_config.py`) cover schema
  validation and client selection.
- **E2E against slurm-emulator** (`tests/e2e/test_e2e_rest_api.py`):
  the emulator ships a slurmrestd-compatible API (v0.0.46); the suite
  starts it as a subprocess and drives the real `SlurmRestClient` over
  HTTP. Gated by `WALDUR_E2E_TESTS` and skipped unless the emulator's
  slurmrestd app is importable.
- **E2E against real slurmrestd**: still future work — a containerized
  slurmrestd + slurmdbd, gated like the existing e2e suites.

## Future work

Tracked separately, not part of this design:

- A REST path (or a documented alternative) for RawUsage resets, so a
  REST-mode host needs no SLURM binaries at all.
- Self-minted HS256 / JWKS-based authentication.

## References

- SLURM REST API client guide: <https://slurm.schedmd.com/rest_clients.html>
- slurmrestd: <https://slurm.schedmd.com/slurmrestd.html>
- JWT auth: <https://slurm.schedmd.com/jwt.html>
- API reference (latest): <https://slurm.schedmd.com/rest_api.html>
- OpenAPI release notes: <https://slurm.schedmd.com/openapi_release_notes.html>
- Slurm-web architecture (prior art for the colocated-agent pattern):
  <https://docs.rackslab.io/slurm-web/overview/architecture.html>
