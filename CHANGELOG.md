# Changelog

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
