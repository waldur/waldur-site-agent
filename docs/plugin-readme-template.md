# Plugin README template

Every plugin under `plugins/` keeps its README in this shape, so an operator
can find the same facts in the same place for every backend. Copy the skeleton
below into a new plugin's `README.md` and fill it in from the code, not from
memory: the entry points come from the plugin's `pyproject.toml`, the settings
from its `schemas.py`, the operations from its backend class.

Rules that apply to every section:

- **No hard-coded test counts or file trees.** They are out of date the next
  time someone adds a test. Name the test directory and the command instead.
- **Every complete configuration example must load.** A YAML block that
  contains `offerings:` must load with the real configuration loader, use only
  keys the agent reads and registered backend names (checked in CI by
  `tests/test_docs_config_examples.py`); a deliberately partial snippet is marked with
  `<!-- docs-check: skip -->` on its own line before the opening fence (a blank line in
  between is fine).
- **No-ops are named.** If a backend method does nothing (for example
  `pause_resource` returns `True` without touching the backend), the operations
  table says so rather than leaving the reader to assume it works.

## Skeleton

````markdown
# <Plugin name> plugin for Waldur Site Agent

One paragraph: what the plugin manages on the backend, and what a Waldur
resource becomes there (a SLURM account, a Harbor project, a Kubernetes
namespace, ...).

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `<name>` | `waldur_site_agent.backends` | order processing, membership sync, reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process`
(list the ones that do something for this backend).

| Operation | Behaviour |
|---|---|
| Create resource | ... |
| Terminate resource | ... |
| Update limits | ... |
| Add / remove members | ... |
| Pause / downscale / restore | ... or **No-op** |
| Usage reporting | ... or **No-op** (reports nothing) |

## Configuration

A complete offering that loads as written. The `<name>` placeholders below are not real
backends, so this template's copy is marked to be skipped; drop the marker in the plugin
README, where CI loads the block through the real configuration loader.

<!-- docs-check: skip -->

```yaml
offerings:
  - name: "..."
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<token>"
    waldur_offering_uuid: "<uuid>"
    backend_type: "<name>"
    order_processing_backend: "<name>"
    membership_sync_backend: "<name>"
    reporting_backend: "<name>"
    backend_settings: {...}
    backend_components: {...}
```

### Backend settings

The settings are validated by `<package>.schemas.<SchemaClass>`; a misspelt or
missing required key is logged as a warning when the agent loads its
configuration. Keys the agent core reads for every backend (prefixes,
`soft_delete`, backend-id uniqueness) are described in
[Configuration](../../docs/configuration.md).

| Setting | Required | Default | Description |
|---|---|---|---|

### Components

What each component means on the backend and which `accounting_type` it needs.

## Tests

```bash
cd plugins/<plugin> && uv run pytest tests/
```
````
