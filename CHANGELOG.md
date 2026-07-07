# Changelog

## 1.0.6-rc.8 - 2026-07-07

- **SLURM**: Add optional `slurmrestd` REST API execution mode as an alternative to CLI-based command execution.
- **SLURM**: Sanitize newlines in account descriptions to prevent malformed `sacctmgr` commands (issue #17).
- **SLURM**: Match account names case-insensitively in `get_account_parent`.
- **SLURM**: Skip unchanged periodic settings to avoid redundant `sacctmgr` modifications.
- **SLURM**: Fix allocation account being orphaned at root after a project reparent.
- **Federation**: Add configurable resource-limit sync direction via `limit_sync_direction`.
- **Federation**: Skip no-op limit-update orders to avoid redundant processing.
- **Federation**: Sync project OECD code, industry flag, and science sub-domain to the Waldur backend ([WAL-10044]).
- **Federation**: Refactor resource `end_date` sync to match project `end_date` sync ([WAL-10000]).
- **Core**: Surface real past-period usage 400 errors instead of masking them ([WAL-10071]).
- **Core**: Ensure usage data is reported with 2 decimal places while preserving idempotency.
- **Core**: Flag backend users for removal when they leave all projects ([gh-13]).
- **Core**: Add early exit in `create_user_homedirs` for existing home directories ([gh-15]).
- **cscs-dwdi**: Add inference reporting support.
- **Docs**: Document cluster-side verification of raw-usage resets and the account name policy vs. resource slug template conflict ([WAL-9925]).

> 26 commits, 58 files changed (+5215/-339)

---

## 1.0.5 - 2026-06-29

### Highlights

This release strengthens SLURM account management and broadens authentication options for the site agent. Operators gain finer control over how default accounts are assigned to users and can now decouple the SLURM root account from per-user default accounts. The agent can also authenticate against Waldur Mastermind using Bearer tokens, simplifying deployment in environments that don't use OIDC.

### What's New

- **SLURM**: Add a `default_account_policy` setting to control how default accounts are assigned to users.
- **Core**: Add support for Bearer token authentication against Waldur Mastermind, alongside the existing OIDC flow.

### Improvements

- **SLURM**: Decouple the SLURM root account from the user default account, allowing independent configuration of each ([ONS-1240]).

### Statistics

> 3 commits, 29 files changed (+1087/-175 lines)

---

## 1.0.5-rc.13 - 2026-06-18

- **OpenNebula**: Add support for vLLM inference VMs, including a sample offering, model-by-name selection, and component `unit_factor` applied when sizing VMs.
- **Waldur**: Add project `end_date` synchronization to the Waldur backend. ([WAL-9999])
- **Waldur**: Add project role reconciliation in polling mode to sync role changes.
- **Core**: Handle forced offering resource synchronization requests over the event channel. ([WAL-10023])
- **Core**: Touch the heartbeat file at the start of offering processing so it stays fresh during long runs.
- **Core**: Migrate agent HTTP clients (croit-s3, harbor, mup, okd, rancher, slurm) from `requests` to `httpx`. ([WAL-8954])
- **Core**: Skip `set_usage` when reported usage already matches what Waldur has, avoiding redundant updates.
- **Waldur**: Round usage to 2 decimals before reporting, and add missing fields to `WaldurBackendSettingsSchema`.
- **SLURM**: Stop `service_provider_can_create_offering_user` from blocking partition sync. ([WAL-9925])
- **SLURM**: Create missing customer/project accounts in `sync_resource_project`, and read account parent from the association rather than `show account`.
- **SLURM**: Accept bare-name output from `sacctmgr list cluster`. ([gh-12])
- **Federation**: Adopt an in-flight B-side terminate order when the resource is already terminating, and fix terminate/update orders timing out while waiting for B approval. ([WAL-9967])
- **Core**: Fix agent identity management request to follow the new SDK pattern, and bump the API client.
- **Core**: Output details of the readiness probe exception for easier diagnosis.
- Add an upgrade guide with SLURM-specific steps. ([WAL-8090])
- Fix OSV dependency scan failures.

> 44 commits, 101 files changed (+5133/-871)

---

## 1.0.4 - 2026-05-22

### Highlights

This release simplifies SLURM resource gating by removing the agent-side QoS-threshold check and relying on the `paused`/`downscaled` flags from Waldur Mastermind as the single source of truth, making pause and downscale behavior more predictable. Event-processing (STOMP) mode now keeps backend account hierarchies in sync periodically, so project structure changes propagate reliably without restarting the agent. A security alert was also cleared by bumping a transitive dependency.

### What's New

- **Federation/SLURM**: Periodically sync the backend account hierarchy while running in event-process mode, keeping project structure aligned without manual intervention ([HPCMP-487]).

### Improvements

- **SLURM**: Remove the agent-side QoS-threshold gate and rely on the `paused`/`downscaled` flags for pause and downscale decisions, simplifying configuration and making behavior consistent across polling and STOMP modes.
- **Security**: Bump `idna` to 3.15 to clear an osv-scanner alert.

### Bug Fixes

- **Core**: Preserve the offering component limit period when loading components.

### Statistics

> 5 commits, 29 files changed (+2818/-782 lines)

---

## 1.0.4-rc.14 - 2026-05-18

### Changes

- **rancher-kc-crd**: Add new CRD-driven Rancher + Keycloak sync plugin, including emitting `spec.cluster` bindings from Resource-level roles.
- **SLURM**: Support resource recovery from terminated state.
- **SLURM**: Apply offering partitions to user associations (WAL-9925).
- **SLURM**: Add optional filesystem quota support for user home directories.
- **Federation**: Handle 503 errors during Waldur federation sync (WAL-9932).
- **Federation**: Fix empty user creation in federation sync (WAL-9918).
- **MUP**: Extend CUID sync to account for already-existing backend accounts, and allow `backend_id` to store the SLURM account identifier (WAL-9909).
- **Core**: Add optional pre-flight check for order processing.
- **Core**: Implement collection of diagnostics from the site agent, including log buffering/shipping (WAL-9263).
- **Core**: Add configurable option to preserve or limit error message/traceback reporting (WAL-9910).
- **Core**: Add periodic offering user reconciliation for `event_process` mode.
- **Core**: Filter component usages by `billing_period` in the historical usage loader.
- **SLURM**: Fix account parent not updated when a project moves organization (HPCMP-487).
- **SLURM**: Fix unknown TRES keys passed to `sacctmgr` causing `BackendError`; filter TRES keys via `list_components()`.
- **SLURM**: Fix CUID-only users synced to backend before ToS acceptance, and agent skipping users in identity bridge sync.
- **Core**: Fix resource end date sync not working.
- **Core**: Fix offering users stuck in CREATING/PENDING states during membership sync.
- **Core**: Fix `username_set` event sending local name instead of CUID to the identity bridge.
- **Core**: Fix `default_homedir_umask` variable name.
- **Helm**: Fix hardcoded liveness probe timeouts across all deployments and stale-heartbeat liveness failures during long reconciliation cycles.
- **Rancher**: Fix example configs to match core config format.
- Upgrade `waldur-api-client` to 8.0.8.dev188; migrate from pre-commit to prek; add mermaid diagram validation hook; refresh plugin development and SLURM storage-quota docs.

### Statistics

> 46 commits, 120 files changed (+11734/-524 lines)

---

## 1.0.2 - 2026-04-13

### Highlights

This release introduces prepaid billing support for SLURM offerings, allowing operators to configure offerings with upfront billing instead of pay-as-you-go. Event processing mode now includes periodic order reconciliation, ensuring orders are not lost if STOMP messages are missed. The Rancher plugin gains fine-grained per-GPU-type limit control, and CI pipelines now include dependency vulnerability scanning.

### What's New

- **SLURM**: Add prepaid billing support for offerings, enabling upfront allocation-based billing as an alternative to usage-based billing.
- **Core**: Add periodic order reconciliation to event processing mode, automatically catching orders that may have been missed during STOMP event delivery.
- **Rancher**: Add per-GPU-type limit support, allowing operators to set resource limits for individual GPU types within a project.
- **CI**: Add osv-scanner dependency vulnerability scanning to the CI pipeline.

### Improvements

- **Core**: Add logging for home directory creation to improve observability during user provisioning.

### Bug Fixes

- **SLURM**: Fix sacctmgr QOS flags being passed as a single subprocess argument instead of separate arguments (WAL-9816).

### Statistics

> 6 commits, 32 files changed (+3089/−254 lines)

---

## 1.0.1-rc.11 - 2026-04-02

- **MUP**: Add initial backend mappings, usage reporting via new endpoint, and myaccessid username support (WAL-9686, WAL-9800).
- **Federation**: Sync project description and effective_id between Waldur instances.
- **Federation**: Add configurable `end_date_sync_direction` setting.
- **Federation**: Filter identity bridge payloads by allowed fields.
- **SLURM**: Create associations on `username_set` offering user message to avoid race conditions.
- **SLURM**: Add default account setting to customer account.
- **K8s-ut-namespace**: Align with latest managed-namespace-operator and add `cr_user_identity_lowercase` setting.
- **K8s-ut-namespace**: Remove usage reporting due to missing cross-namespace access.
- **Core**: Add Elastic APM integration support (WAL-8988).
- **Core**: Improve order processing logging for easier debugging.
- **Core**: Fix readiness probe to use authenticated endpoint.
- **Core**: Fix historical usage loader.
- **SLURM**: Fix mapped component usage reporting (WAL-9815).
- **CSCS-DWDI**: Fix inodes component name and Keycloak secure token refresh.
- **CSCS-DWDI**: Report zero usage when API returns no data for an account.
- **Federation**: Fix resource limits being overridden during event processing.
- **Federation**: Fix identity bridge writing CUID as offering user username.
- **Release**: Add RC release support with `-rc.N` format.

> 34 commits, 67 files changed (+5571/-911 lines)

---

## 1.0.0 - 2026-03-20

### Highlights

This release adds Keycloak SAML integration for the OpenNebula VDC plugin, enabling SSO-based user provisioning through Keycloak identity providers. SLURM operators can now scope commands to specific clusters in multi-cluster environments. The core agent is also more resilient, with fixes for liveness probe timeouts and improved federation identity handling.

### What's New

- **OpenNebula**: Add Keycloak SAML integration for VDC provisioning, including automatic group mapping and end-to-end setup documentation ([WAL-9218]).
- **SLURM**: Add cluster filtering to `sacctmgr` and `sacct` commands for multi-cluster deployments.
- **Core**: Make backend ID retry count configurable.

### Improvements

- **Core**: Downgrade remote Waldur version fetch failure to debug level to reduce log noise when the target instance restricts `/api/version/`.

### Bug Fixes

- **Core**: Fix liveness probe failures by replacing blocking sleeps with tick-based main loops.
- **Federation**: Fix URL construction and identity bridge payload in the Waldur federation client.
- **Federation**: Fix user identity resolution in the Waldur federation backend.
- **SLURM**: Fix parent account quoting in `sacctmgr` commands.

> 12 commits, 72 files changed (+4275/−684 lines)

---

## 0.9.9 - 2026-03-11

### Highlights

This release introduces the new **LDAP username management plugin**, enabling automated LDAP account provisioning with welcome email notifications for SLURM-based sites. Plugins can now control order approval via the new `evaluate_pending_order` hook, and all deployments benefit from lighter health checks and reduced API bandwidth through field-level filtering.

### What's New

- **LDAP**: Add LDAP username management plugin with account creation, password generation, and configurable welcome email notifications (HTML and plain text templates).
- **SLURM**: Extend backend to delegate username operations to the LDAP plugin, including `waldur_username_attribute` support for flexible user identity mapping.
- **Core**: Add `evaluate_pending_order` hook allowing plugins to programmatically approve or reject pending orders before processing.
- **SLURM**: Add E2E tests for periodic usage policies.

### Improvements

- **Core**: Replace heavy diagnostics probe with a lightweight `/healthz` endpoint across all deployment modes (event processing, order processing, membership sync, reporting).
- **Core**: Limit API GET requests to only the fields used in code, reducing payload sizes and improving performance.
- **Core**: Pass cluster filter to historical usage queries for more accurate multi-period reporting.
- **Federation**: Remove deprecated `target_stomp_offering_uuid` configuration and fix STOMP subscription handling.
- **Federation**: Fix Waldur-to-Waldur membership sync and stabilise integration tests.

### Bug Fixes

- **CSCS-DWDI**: Fix `storage_inodes` condition that never matched due to incorrectly chained `in` operators.
- **Docs**: Fix outdated and inaccurate plugin documentation across all README files and plugin metadata.

### Statistics

> 11 commits, 94 files changed (+8297/−614 lines)

---

## 0.9.8 - 2026-03-06

This release delivers a targeted fix for a data integrity issue in the Waldur Python SDK dependency. Operators who encountered errors related to nullable foreign key fields will benefit from this update.

### Bug Fixes

- **Core**: Bump Waldur Python SDK to resolve a nullable foreign key handling issue.

### Statistics

> 1 commit, 2 files changed (+5/-5 lines)

---

## 0.9.7 - 2026-03-06

This release adds new configuration options for the federation and Kubernetes namespace plugins, making user resolution and user synchronization behavior more flexible for operators. Resource limit handling is also improved with automatic unit factor conversion in the core backend. Additionally, offering component updates are now more accurate, and the codebase is cleaner with SLURM-specific logic removed from shared code.

### What's New

- **Federation**: Add configurable `user_resolve_method` setting to the Waldur federation plugin, allowing operators to control how users are resolved during synchronization.
- **K8s-ut-namespace**: Add `sync_users_to_cr` setting to optionally control whether users are synchronized to cluster roles.
- **Core**: Add `unit_factor` conversion support to `BaseBackend.set_resource_limits`, enabling backends to correctly scale resource limit values.

### Improvements

- **Core**: Update limit-type offering components instead of skipping them, improving accuracy of component state synchronization.
- **Core**: Remove SLURM-specific references from generic agent code, making the shared processing layer properly backend-agnostic.
- **Core**: Update Waldur Python client to the latest version.

### Bug Fixes

- **Core**: Fix incorrect log level in the username management backend.

> 7 commits, 18 files changed (+633/-106 lines)

---

Now I have the full picture. Here's the changelog entry:

## 0.9.6 - 2026-03-03

### Highlights

This patch release ensures that sparse field selection for Waldur API resource queries includes all fields required by processors. Previously, missing fields (such as slug identifiers and offering backend ID) could cause failures or incomplete data during membership and report processing.

### Bug Fixes

- **Core**: Add missing fields (`slug`, `project_slug`, `customer_slug`, `offering_backend_id`) to sparse resource field selection in membership and report processors, preventing potential data gaps during synchronization.

### What's New

- **Core**: Add comprehensive test coverage for processor field selection to validate that all required resource fields are included in API queries.

> 1 commit, 2 files changed (+242/-0 lines)

---

## 0.9.5 - 2026-03-03

### Highlights

This patch release improves deployment reliability by fixing Helm chart issues and adding a comprehensive helm-unittest test suite to catch template regressions early. It also resolves a crash that occurred when the username management backend plugin was not installed.

### Bug Fixes

- **Core**: Fix KeyError when the username management backend plugin is not installed.
- **Helm**: Fix Helm chart template issues across all deployment manifests (order processing, event processing, membership sync, and reporting).

### Improvements

- **Helm**: Add helm-unittest test suite covering all deployment templates, secrets, helpers, and value combinations.

### Statistics

> 2 commits, 18 files changed (+949/−24 lines)

---

## 0.9.4 - 2026-03-02

### Highlights

This release dramatically reduces Waldur API load — polling cycles now make ~80% fewer API calls thanks to aggressive caching and bulk usage endpoints. Federation operators gain real-time username synchronization from a remote Waldur instance via STOMP, eliminating the need for manual user mapping. Several reliability fixes address STOMP connection stability, order processing correctness, and month-boundary usage reporting.

### What's New

- **Federation**: Add offering user username sync from a remote Waldur instance with real-time STOMP event subscription.
- **CI**: Add end-to-end integration test pipeline using a Docker-in-Docker Waldur stack, enabling automated STOMP and API tests against a live environment.
- **Release tooling**: Add `release.sh` orchestrator, `bump_versions.py` auto-discovery, and changelog generation scripts, replacing hardcoded CI version bumps.

### Improvements

- **Core**: Reduce API calls per polling cycle by ~80% through response caching across processor methods.
- **Core**: Use bulk `set_user_usages` endpoint to batch per-user usage reporting into a single API call.
- **Rancher**: Switch usage reporting to `ResourceQuota status.used` for accurate actual-usage figures.

### Bug Fixes

- **Core**: Fix order processing skipping resource creation when `order.backend_id` is set by external systems (e.g. SharePoint) by gating async order tracking behind a `supports_async_orders` backend capability flag.
- **Core**: Fix STOMP heartbeat misconfiguration that caused spurious reconnects every ~30 seconds.
- **Core**: Fix month-boundary race condition in usage reporting that could attribute usage to the wrong period.
- **Rancher**: Fix backend overwriting resource quotas with hardcoded defaults instead of preserving existing values.
- **Release**: Fix release script to regenerate `uv.lock` after version bumps so Docker builds use correct workspace package versions.

> 16 commits, 74 files changed (+10,217/−541 lines)

---

## 0.9.2 - 2026-02-24

### Highlights

Patch release fixing a CI publishing gap for recently added plugins.

### Bug Fixes

- **CI**: Add missing version bump for digitalocean and opennebula plugins in CI publish job.

> 1 commit, 1 file changed (+4 lines)

---

## 0.9.1 - 2026-02-24

### Highlights

A reliability-focused release that hardens STOMP reconnection, fixes SLURM edge cases, and adds the k8s-ut-namespace plugin for Kubernetes ManagedNamespace provisioning. The offering user state machine is now more robust with proper handling of error and cross-transition states.

### What's New

- **Kubernetes**: Add k8s-ut-namespace backend plugin for ManagedNamespace provisioning.
- **Core**: Add multi-period usage reporting support.
- **Core**: Move historical usage loader from SLURM plugin to core for reuse by other plugins.
- **Core**: Extend `load_components_to_waldur()` to pass all OfferingComponentRequest fields.
- **Helm**: Add writable `/tmp` to Helm deployments and report zero usage for idle resources.

### Improvements

- **Core**: Close offering user state machine gaps: handle ERROR_CREATING, cross-transitions, and `set_validation_complete` (EFP-56079).
- **CI**: Add release version bumping for keycloak-client and k8s-ut-namespace plugins.

### Bug Fixes

- **STOMP**: Fix reconnection storm with exponential backoff and cascade prevention (HPCMP-407).
- **SLURM**: Handle sacctmgr "Nothing modified" response on unchanged limits (HPCMP-438).
- **SLURM**: Protect agent against SLURM emulator binaries shadowing real commands.
- **SLURM**: Fix tests to match absolute bin path resolution.

> 13 commits, 61 files changed (+5530/-732 lines)

---

## 0.9.0 - 2026-02-18

### Highlights

Major feature release adding the OpenNebula plugin for VDC and VM management, an identity bridge for Waldur federation username management, and passthrough attributes for federation orders. MQTT support has been fully removed in favor of STOMP.

### What's New

- **OpenNebula**: Add OpenNebula plugin with VDC and VM management support.
- **Federation**: Add identity bridge username management backend for Waldur federation.
- **Federation**: Support passthrough_attributes for Waldur federation orders.

### Improvements

- **Core**: Remove MQTT support in favor of STOMP.

> 4 commits, 75 files changed (+11328/-1024 lines)

---

## 0.8.9 - 2026-02-11

### Highlights

Adds Waldur-to-Waldur federation with non-blocking order processing via STOMP, and hardens SLURM command execution against invalid flags.

### What's New

- **Federation**: Add Waldur federation with non-blocking orders and target STOMP.
- **SLURM**: Sync service and course accounts on resource creation.

### Bug Fixes

- **SLURM**: Guard against invalid SLURM command flags in `_execute_command`.
- **SLURM**: Fix sacct command call.
- **CI**: Add waldur plugin to CI publish job version bumping.

> 5 commits, 26 files changed (+3694/-217 lines)

---

## 0.8.7 - 2026-02-09

### Highlights

Patch release fixing Rancher role setting and improving exception reporting.

### Bug Fixes

- **Rancher**: Fix role setting.
- **Core**: Fix exception details reporting.

> 2 commits, 10 files changed (+385/-61 lines)

---

## 0.8.6 - 2026-02-06

### Highlights

The largest release in recent history. Migrates the agent to structured JSON logging, adds the Waldur-to-Waldur federation plugin and the DigitalOcean/libcloud plugin, introduces unit factor reporting, and makes usage accounting timezone-aware. Includes substantial improvements to SLURM diagnostics and CI test coverage.

### What's New

- **Waldur**: Add Waldur-to-Waldur federation backend plugin (WAL-9658).
- **DigitalOcean**: Move DigitalOcean/libcloud backend to agent (WAL-7840).
- **Core**: Migrate agent to structured JSON logging (WAL-9095).
- **Core**: Introduce `unit_factor_reporting` for component usage.
- **Core**: Log version information at agent startup.
- **SLURM**: Add SLURM diagnostics CLI tool.
- **SLURM**: Report executed SLURM commands to Mastermind.

### Improvements

- **Core**: Use ObservableObjectTypeEnum for STOMP handler routing (HPCMP-421).
- **Core**: Propagate configured timezone to backends for usage accounting.
- **Core**: Make `get_usage_report` timezone-aware.
- **Core**: Validate `service_provider_can_create_offering_user` in membership sync.
- **CI**: Improve test coverage info collection and CI test jobs.
- **Docs**: Update documentation for plugin creation.

### Bug Fixes

- **SLURM**: Fix limit values.
- **SLURM**: Fix resource creation flow.
- **CSCS-DWDI**: Fix compute plugin.
- **Tests**: Fix FakeDatetime JSON serialization in membership sync tests.

> 26 commits, 68 files changed (+10392/-314 lines)

---

## 0.8.5 - 2026-01-27

### Highlights

Dependency bump for waldur-api-client.

### Improvements

- **Core**: Bump waldur-api-client.

> 1 commit, 2 files changed (+8/-7 lines)

---

## 0.8.4 - 2026-01-27

### Highlights

Integrates the agent with event subscription queues for faster order processing, and includes multiple fixes to the CSCS DWDI storage plugin and SLURM limit handling.

### What's New

- **Core**: Integrate agent with event subscription queues.

### Improvements

- **Core**: Bump waldur-api-client to 7.9.8.
- **Core**: Add guard for empty backend IDs.

### Bug Fixes

- **SLURM**: Fix limit value types.
- **SLURM**: Fix limit update logging.
- **CSCS-DWDI**: Fix storage format issues and backend reporting.

> 11 commits, 15 files changed (+1247/-464 lines)

---

## 0.8.3 - 2026-01-16

### Highlights

Fixes Pydantic compatibility issues in the CSCS DWDI plugin and adds configurable log level support.

### Improvements

- **Core**: Allow setting log level via config file.

### Bug Fixes

- **CSCS-DWDI**: Fix Pydantic model access — convert to dictionary for element access in newer Pydantic versions.
- **CSCS-DWDI**: Apply same Pydantic fixes to storage backend.

> 6 commits, 9 files changed (+117/-31 lines)

---
