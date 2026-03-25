# Changelog

## 1.0.1-rc.6 - 2026-03-25

- **MUP**: Add initial resource mappings and order processing ([WAL-9686]).
- **Federation**: Sync project description from source to target Waldur instance.
- **Federation**: Sync effective_id from target back to source Waldur instance.
- **Federation**: Add configurable `end_date_sync_direction` setting for resource end dates.
- **Federation**: Fix resource limits being overridden during event processing.
- **Federation**: Fix identity bridge backend writing CUID as offering user username.
- **K8s-ut-namespace**: Align plugin with latest managed-namespace-operator.
- **K8s-ut-namespace**: Add `cr_user_identity_lowercase` setting for case control.
- **SLURM**: Add default account setting to customer account creation.
- **Core**: Fix readiness probe to use authenticated endpoint.
- **Core**: Add RC release support with `-rc.N` version format.

> 17 commits, 52 files changed (+3779/−763 lines)

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
