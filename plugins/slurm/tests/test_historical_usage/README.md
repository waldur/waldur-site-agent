# SLURM historical usage tests

SLURM-specific tests for historical usage: reading a past month's usage from
SLURM accounting, as `waldur_site_load_historical_usage` does through
`SlurmBackend.get_usage_report_for_period`. They drive the real `SlurmClient`
against slurm-emulator's `sacct` / `sacctmgr` command classes, imported as a
Python package — no emulator process is needed.

The backend-agnostic parts — the loader command and the backend utilities —
are tested in the core suite: `tests/test_historical_usage_loader.py` and
`tests/test_backend_utils_historical.py` at the repository root.

## Modules

| Module | What it covers |
|---|---|
| `test_slurm_client_historical.py` | `SlurmClient.get_historical_usage_report()` against the emulated `sacct` |
| `test_slurm_backend_historical.py` | `SlurmBackend.get_usage_report_for_period()`: aggregation and unit conversion |
| `test_integration.py` | Multi-month flows through client and backend |
| `conftest.py` | Emulator database, time engine, emulated commands, and usage records for January–March 2024 |

## Running

slurm-emulator is in the plugin's `dev` dependency group, so
`uv sync --all-packages` installs it; without it the tests are skipped
(`slurm-emulator not installed`).

```bash
cd plugins/slurm
uv run pytest tests/test_historical_usage/
uv run pytest tests/test_historical_usage/test_slurm_backend_historical.py

# the core loader tests, from the repository root
uv run pytest tests/test_historical_usage_loader.py tests/test_backend_utils_historical.py
```

`test_integration.py` carries `@pytest.mark.integration` and
`@pytest.mark.emulator`, but the markers are not registered or used to select
tests; run files or directories instead.
