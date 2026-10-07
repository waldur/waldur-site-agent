# Configuration Validation

The agent validates its YAML configuration with Pydantic when it starts. This page says what is
checked, which problems stop the agent and which only log a warning, how to read the errors, and
what loading the file does **not** catch. The last section is for plugin authors who want their
plugin's settings validated.

## What happens when the file is loaded

First the top-level keys are checked (**fatal**). Then, for every offering, in this order:

1. Plugin-specific component fields are checked against the plugin's component schema, if the
   plugin registered one under the offering's `backend_type` (**warning only**).
2. Each entry under `backend_components` is checked against the core component model (**fatal**).
3. `backend_settings` is checked against the plugin's settings schema, if the plugin registered one
   under the offering's `backend_type` (**warning only**).
4. The offering itself is checked (**fatal**).

Loading stops at the first model that fails, so a file with problems in several places reports
them one place at a time.

### Fatal: the agent does not start

<!-- pyml disable-num-lines 12 line-length -->
| Check | Rule |
| ----- | ---- |
| Required offering keys | `name`, `waldur_api_url`, `waldur_offering_uuid`, `backend_type` |
| Credentials | `waldur_api_token`, or all three of `oidc_token_url`, `oidc_client_id`, `oidc_client_secret`; a partial OIDC set is rejected even when a token is also set |
| STOMP and OIDC | `stomp_enabled: true` requires `waldur_api_token` |
| URLs | `waldur_api_url` and `oidc_token_url` must start with `http://` or `https://`; a missing trailing `/` on `waldur_api_url` is added |
| Components | `measured_unit`, `accounting_type` and `label` are required; `accounting_type` is one of `usage`, `limit`, `one` |
| `timezone` | must be a known IANA zone name |
| `log_level` | one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (any case) |
| `reporting_periods` | integer from 1 to 12 |
| `sentry_dsn`, `elastic_apm_server_url` | a valid URL when set (an empty string means unset) |
| `log_shipping` | `ship_interval_seconds` ≥ 10, `buffer_size_mb` ≥ 1 |

`backend_type` is lowercased on load.

### Warning only: the agent starts anyway

A plugin settings or component schema failure is logged and the agent continues with the values
exactly as written:

<!-- pyml disable-num-lines 3 line-length -->
```text
{"event": "Plugin schema validation failed for slurm settings: 1 validation error for SlurmBackendSettingsSchema\ndefault_account\n  Field required [type=missing, ...]", "level": "warning", "logger": "waldur_site_agent.backend", ...}
```

Treat these warnings as errors: the backend usually fails later, when it first reads the setting.

### Not checked at all

- **Unknown keys are ignored** at the top level and inside offerings, so a misspelt optional key
  silently keeps its default. Unknown keys inside a component are kept and passed to the backend
  (plugins use them for their own component fields) — a misspelt one is simply never read. Plugin
  settings schemas may allow extra keys too; the SLURM one does.
- **`*_backend` names are not checked on load.** An unregistered name such as `cscs-dwdi` (the
  registered ones are `cscs-dwdi-compute`, `-storage` and `-inference`) logs
  `Unsupported backend type for reporting_backend: …` when that process starts. An offering with no
  `*_backend` at all is silently skipped by `order_process`; `membership_sync` logs
  `Unable to create backend for <offering>` for it, and `report` stops the whole process with
  that error.
- **Nothing is contacted.** Credentials, the offering UUID and the backend are not tried until the
  agent runs. `waldur_site_diagnostics -c <config>` tries them: it queries Waldur with each
  offering's credentials and runs the order processing backend's diagnostics. It exits 1 when those
  backend diagnostics fail or `cluster_name` does not match the offering's `backend_id`. Waldur
  errors are logged; if the offering itself cannot be fetched (rejected token, unknown UUID) the
  command then stops with a traceback, so read the output rather than relying on the exit status.

## Reading the errors

A fatal error is raised as `ValueError: Configuration validation failed:` followed by Pydantic's
report, which names the model, the field and the reason:

<!-- pyml disable-num-lines 5 line-length -->
```text
ValueError: Configuration validation failed: 1 validation error for Offering
waldur_api_url
  Value error, waldur_api_url must start with http:// or https:// [type=value_error, input_value='waldur.example.com/api/', input_type=str]
```

The model name tells you where to look:

| Model in the message | Where the problem is |
| -------------------- | -------------------- |
| `RootConfiguration` | a top-level key (`timezone`, `log_level`, …) |
| `Offering` | an offering key; a rule over several keys has no field line, only the message |
| `BackendComponent` | an entry under `backend_components` |

Messages for the cross-field rules:

<!-- pyml disable-num-lines 5 line-length -->
```text
Value error, Either waldur_api_token or all of oidc_token_url, oidc_client_id, oidc_client_secret must be set
Value error, oidc_token_url, oidc_client_id and oidc_client_secret must be set together; only some of them are set
Value error, stomp_enabled requires waldur_api_token: the STOMP session is authenticated with the static API token, which an OIDC-only offering does not have. Use polling mode or configure waldur_api_token.
```

A missing required key reads `Field required [type=missing, …]` under the key's name; an invalid
`accounting_type` reads `Input should be 'usage', 'limit' or 'one'`.

## For plugin authors: validation schemas

A plugin can validate its own `backend_settings` and its plugin-specific component fields by
registering Pydantic models. Schemas are looked up by the offering's **`backend_type`** only —
not by `order_processing_backend`, `membership_sync_backend` or `reporting_backend` — so register
the schema under the name operators put in `backend_type`.

### Base classes

Both base classes live in `waldur_site_agent.common.plugin_schemas` and forbid unknown fields by
default (`extra="forbid"`), so a typo in a plugin key is reported:

- `PluginBackendSettingsSchema` validates the whole `backend_settings` mapping.
  `HomedirSettingsSchema` extends it with the home-directory settings core reads
  (`enable_user_homedir_account_creation`, `default_homedir_umask`, `homedir_base_path`,
  `homedir_quota`); inherit from it if your backend creates home directories.
- `PluginComponentSchema` validates only the component fields the core model does not know. Core
  strips `measured_unit`, `unit_factor`, `accounting_type`, `label` and the other core fields
  before calling it, so the schema declares just the plugin's own fields.

The Waldur federation plugin is a working example:

```python
from pydantic import Field

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
    TargetComponentConfig,
)


class WaldurComponentSchema(PluginComponentSchema):
    target_components: dict[str, TargetComponentConfig] = Field(
        default_factory=dict,
        description="Mapping of target component names to conversion config.",
    )


class WaldurBackendSettingsSchema(PluginBackendSettingsSchema):
    target_api_url: str = Field(..., description="Base URL for the target Waldur B API endpoint")
    target_api_token: str = Field(..., description="Authentication token for Waldur B API")
    # ... further settings
```

### Registering the schemas

In the plugin's `pyproject.toml`, under the same name as the backend entry point:

```toml
[project.entry-points."waldur_site_agent.component_schemas"]
waldur = "waldur_site_agent_waldur.schemas:WaldurComponentSchema"

[project.entry-points."waldur_site_agent.backend_settings_schemas"]
waldur = "waldur_site_agent_waldur.schemas:WaldurBackendSettingsSchema"
```

### What validation does with your schema

- On success, `backend_settings` is replaced by `model_dump(exclude_unset=True)` of your model, so
  defaults declared in the schema are **not** filled in — read them with a default in the backend
  too.
- On failure, the error is logged as a warning and the raw values are passed through unchanged
  (see [Warning only](#warning-only-the-agent-starts-anyway)). Do not rely on the schema to stop a
  misconfigured agent; raise a `BackendError` from the backend's constructor for settings it
  cannot work without.

### Python 3.9 compatibility

Plugins must run on Python 3.9. Set `model_config = ConfigDict(...)` rather than a
`ClassVar` dict, and use `Optional[X]` / `Union[X, Y]` instead of `X | Y` in field annotations.
