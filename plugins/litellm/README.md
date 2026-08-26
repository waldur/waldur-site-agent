# LiteLLM Plugin for Waldur Site Agent

This plugin integrates a [LiteLLM proxy](https://docs.litellm.ai/docs/proxy/virtual_keys) with
Waldur so that LLM inference access can be sold, provisioned, metered, and billed through the
Waldur marketplace. It provisions per-customer virtual keys from marketplace orders and reports
token usage and spend back to Waldur for metering and billing.

It is the same shape as the sibling `envoy-ai-gateway` plugin, but talks to LiteLLM's management
REST API instead of Kubernetes Secrets — so there is no cluster access and no separate usage
warehouse to deploy.

## Features

- **Virtual key lifecycle**: provision, rotate, pause, restore, and terminate LiteLLM keys
  directly from Waldur marketplace orders
- **Native block/unblock pause**: suspend access with `POST /key/block`; a blocked key returns
  401 at the proxy and unblocking restores it without new key material
- **Usage reporting**: report per-resource input/output tokens and USD spend from LiteLLM's own
  daily spend table — no sidecar collector
- **Enforcement backstop**: mirror Waldur's limits onto each key (`max_budget`, `tpm_limit`,
  `rpm_limit`) so a burst between reporting cycles cannot outrun metering
- **Model allowlist**: restrict every key of an offering to a set of models

## Overview

The plugin exposes two backends that are normally paired on a single composed offering:

- **Management backend** (`litellm`): the `order_processing_backend` and `membership_sync_backend`.
  Owns the key lifecycle.
- **Usage reporting backend** (`litellm-usage`): the `reporting_backend`. Reads LiteLLM's spend
  data and submits it to Waldur.

### How keys are identified

LiteLLM stores keys **sha256-hashed** and returns the plaintext `sk-…` exactly once, in the
`/key/generate` response. The agent applies the key and immediately reports it to Waldur, which
holds the only copy (encrypted) — the plugin keeps nothing.

Everything after provisioning is therefore addressed by one of two handles:

| Handle | What it is | Used for |
|---|---|---|
| `key_alias` | `<resource_backend_id>-<n>`, agent-chosen, stable across rotation | Waldur's `client_id`; key lookup |
| `token` | LiteLLM's sha256 of the key | `/key/block`, `/key/unblock`, `/key/update`, `/key/delete` |

The resource's `backend_id` is its UUID hex, and each key is a numbered slot beneath it. Keys are
minted **non-expiring** (no `duration`): Waldur owns the resource lifecycle, and an expiry it does
not know about would revoke access behind its back.

### Limit enforcement

Primary path — Waldur is the authority:

1. `litellm-usage` reports usage to Waldur
2. a `LIMIT` component reaches its limit and the offering sets
   `plugin_options.action_on_usage_limit: pause`
3. mastermind pauses the resource
4. the agent calls `pause_resource()`, which blocks every key of the resource → 401 at the proxy

Backstop — `set_resource_limits()` mirrors the resource's limits onto each key (`max_budget`,
`tpm_limit`, `rpm_limit`). Each key carries the resource's **full** budget, not a share of it: the
keys are alternatives for one consumer, and splitting the budget would throttle a consumer using a
single key to half its entitlement.

The backstop is a **full target state**, not a patch: every one of the three fields is written on
every reconciliation, `null` where there is to be no cap. Dropping `tpm` from a resource's limits
therefore clears `tpm_limit` on its keys (or returns it to the offering-wide `tpm_limit` default);
writing only the fields the resource currently carries would leave a removed cap on the key
forever, with no later cycle able to clear it. `null` really does clear on LiteLLM's side — see
[Key management](#key-management) for the verified behaviour.

#### Which limits reach the key

`set_resource_limits()` reads the resource's Waldur limits by name, so a field is only driven
per-resource when the offering **declares a component of that name**:

| Waldur limit | LiteLLM field | Falls back to |
|---|---|---|
| `token_cost` | `max_budget` | no cap (there is no offering-wide budget setting) |
| `tpm` | `tpm_limit` | the offering's `tpm_limit` backend setting |
| `rpm` | `rpm_limit` | the offering's `rpm_limit` backend setting |

`waldur_resource.limits` only ever carries components the offering declares. An offering that
declares just `input_tokens`, `output_tokens` and `token_cost` — the common case, and the example
below — therefore drives **`max_budget` only**; `tpm_limit` and `rpm_limit` stay at whatever the
`tpm_limit` / `rpm_limit` backend settings say, the same value for every resource on the offering.

To make rate limits per-resource instead, declare `tpm` and/or `rpm` as `LIMIT` components — see
the commented lines in the example below. They are rate caps, not meters: nothing reports usage
against them, so they exist purely to carry a number onto the key. `budget_duration` is not
per-resource under any configuration; it is an offering-wide setting and only applies alongside a
`max_budget`.

> ### Set `budget_duration` whenever `token_cost` is a limit
>
> A LiteLLM `max_budget` with **no** `budget_duration` is a *lifetime* budget: it never resets, and
> the key's `spend` keeps accumulating against it. So on an offering that limits `token_cost` but
> leaves `budget_duration` unset, the first billing period that exhausts the budget blocks the key
> **permanently** — Waldur's period rolls over and un-pauses the resource, but the proxy keeps
> answering 401, and nothing in this plugin resets `max_budget` or clears `spend`. A backstop that
> fails closed forever is worse than no backstop, so set `budget_duration` to match the offering's
> billing period.
>
> Even then the two clocks are not aligned: LiteLLM resets a key's budget on its **own** schedule,
> counted from when the key was created, not from the start of Waldur's billing month. A key minted
> on the 20th under `budget_duration: "30d"` resets on the 20th. This only affects the backstop —
> Waldur's own metering and its report → pause path stay month-aligned — but it means a rotated or
> late-minted key can carry a reset date its siblings do not share.

## Backend Types

### Management Backend (`litellm`)

| Agent method | LiteLLM call |
|---|---|
| `ping()` | `GET /health/readiness` (requires `db: connected`) |
| `create_resource_with_id()` | register only — no key minted here; surfaces endpoint `{api_url}/v1` |
| `generate_resource_keys()` | `POST /key/generate` per missing slot, yielding one at a time |
| `list_resource_client_ids()` | `GET /key/list`, narrowed by substring then filtered to `^<backend_id>-\d+$` |
| `rotate_resource_key()` | `POST /key/{token}/regenerate`, falling back to delete + generate |
| `prune_unknown_resource_keys()` | `POST /key/delete` for slots Waldur does not hold |
| `pause_resource()` | `POST /key/block` for every key of the resource |
| `restore_resource()` | `POST /key/unblock` for every key |
| `downscale_resource()` | same as pause (a key has no partial-capacity state) |
| `delete_resource()` | `POST /key/delete` for every key |
| `set_resource_limits()` | `POST /key/update` — full target state of the four backstop fields |
| `get_resource_metadata()` | `active` = any key not blocked |
| `recreate_missing_resource()` | returns `False` + warning — the agent cannot restore a key it never kept |

> **`soft_delete` does not apply.** `delete_resource()` is overridden, so the core's soft-delete
> path (zeroing limits instead of removing the account) is never reached and the setting is
> ignored: a terminate always deletes the keys. There is no soft form of it — a key left in place
> with a zero budget is still a live credential, and the agent cannot hand back the plaintext of
> one it kept.

### Usage Reporting Backend (`litellm-usage`)

Read-only. Reports whichever of `input_tokens`, `output_tokens` and `token_cost` the offering
declares, so an offering that prices tokens Waldur-side and one that bills LiteLLM's upstream cost
both work without a mode flag.

**Source:** `GET /user/daily/activity`, walking `results[].breakdown.api_keys` — see
[Why not `/global/spend/report`](#why-not-globalspendreport) below.

**One fetch per pass:** `/user/daily/activity` cannot be filtered by key, so a call returns the
whole proxy's month and this backend picks out the rows it wants. The processor pulls one resource
at a time (and once more per historical period), so the fetched rows are reused across the
resources of a pass — otherwise each resource would walk the entire table again. `usage_cache_ttl`
bounds that reuse so it cannot outlive the pass. It defaults to **half**
`WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` (so 15 minutes on the default 30-minute period): a whole
period would keep an entry alive into the next pass, which is exactly when fresh numbers are due.

**Rollup:** usage is attributed per **resource**, not per key. The `-<n>` slot suffix is stripped
from the alias and the rows are summed, so rotating a key does not split a tenant's bill and a
resource used across several days and several keys is billed for all of it.

## Configuration

Both backends are normally combined on one composed offering and share a single `backend_settings`
block.

### Management backend settings (`litellm`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | LiteLLM proxy base URL |
| `api_token` | yes | — | Master or admin key for the management API |
| `models` | no | all | Model allowlist pushed onto every key |
| `budget_duration` | no | — | Reset period for `max_budget`, e.g. `30d`. **Set it whenever `token_cost` is a limit** |
| `tpm_limit` | no | — | Default tokens-per-minute cap on each key |
| `rpm_limit` | no | — | Default requests-per-minute cap on each key |
| `verify_ssl` | no | `true` | Verify the proxy's TLS certificate |
| `timeout` | no | `30` | Per-request timeout in seconds |

### Usage reporting backend settings (`litellm-usage`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | LiteLLM proxy base URL |
| `api_token` | yes | — | Master or admin key for the spend API |
| `verify_ssl` | no | `true` | Verify the proxy's TLS certificate |
| `timeout` | no | `30` | Per-request timeout in seconds |
| `usage_cache_ttl` | no | half report period | Seconds a fetched month of rows is reused in one pass; `0` disables |

### Composed offering (both backends)

```yaml
offerings:
  - name: "LLM Inference"
    order_processing_backend: "litellm"
    membership_sync_backend: "litellm"     # required — pause/restore blocks the keys
    reporting_backend: "litellm-usage"

    backend_settings:
      api_url: "https://litellm.example.com"
      api_token: "sk-master-..."
      models: ["gpt-4o", "llama-3.3-70b"]   # optional allowlist
      budget_duration: "30d"                # required in practice whenever token_cost is a limit
      tpm_limit: null
      rpm_limit: null
      verify_ssl: true

    backend_components:
      input_tokens:  { measured_unit: "tokens", accounting_type: "usage", label: "Input tokens" }
      output_tokens: { measured_unit: "tokens", accounting_type: "usage", label: "Output tokens" }
      token_cost:    { measured_unit: "USD",    accounting_type: "usage", label: "Inference cost" }

      # Optional: declare these only to drive tpm_limit / rpm_limit per resource rather
      # than from the offering-wide backend_settings above. They carry a cap onto the
      # key; no usage is ever reported against them.
      # tpm:         { measured_unit: "tokens/min",   accounting_type: "limit", label: "Tokens per minute" }
      # rpm:         { measured_unit: "requests/min", accounting_type: "limit", label: "Requests per minute" }
```

Enforcement is configured on the Waldur **offering**, not here: the component must be
`billing_type: LIMIT` and the offering must set `plugin_options.action_on_usage_limit: pause`.

## LiteLLM API behaviour

Verified against `ghcr.io/berriai/litellm:main-stable` (open-source, **no** enterprise licence) on
2026-08-25. These are the answers the implementation is built on; re-check them when targeting a
much older or newer proxy.

### Enterprise-gated endpoints

Three endpoints refuse to run without a `LITELLM_LICENSE`, and they do not agree on a status code —
they only agree on the prose, which is why the client discriminates on the message text:

| Endpoint | Status | Consequence |
|---|---|---|
| `POST /key/{key}/regenerate` | 500 | rotation falls back to delete + generate |
| `GET /global/spend/report` | 400 | **not usable** as the usage source |
| `GET /key/spend/report` | 403 | not usable |

#### Why not `/global/spend/report`

The issue specified `GET /global/spend/report?group_by=api_key` as the usage source. It is
enterprise-only, so on an open-source proxy it returns 400 and reports nothing. `GET
/user/daily/activity` was chosen instead:

- not gated
- **pre-aggregated per day**, so a month's report never walks per-request rows
- `results[].breakdown.api_keys.<hash>` carries `metadata.key_alias` alongside
  `metrics.prompt_tokens`, `metrics.completion_tokens` and `metrics.spend` — so no separate
  hash → alias map is needed
- paginated via `metadata.page` / `total_pages` / `has_more`

Only `breakdown.api_keys` is read. The same numbers reappear under `breakdown.models` and
`breakdown.model_groups` split by model and by model group; walking more than one breakdown would
count the same spend twice.

`GET /spend/logs/v2` also works without a licence and is the per-request fallback if the daily
aggregate ever proves insufficient — but its rows carry only the key **hash**, not the alias, so
using it would reintroduce the hash → alias map.

### Key management

- **`/key/generate` accepts `blocked: true`** and it persists — a key minted blocked returns 401
  at the proxy immediately. The response carries `key` (plaintext, once) and `token` (the sha256).
- **`/key/block`, `/key/unblock`, `/key/delete` and `/key/update` all accept the hash** as `key`;
  `/key/delete` takes a list, as `{"keys": [...]}`. An unknown hash returns **404
  `Key not found.`**, which the client maps to "no such key" rather than to a failure.
- **`/key/list` supports `key_alias` (exact) and `substring_matching=true`.** Substring matching is
  a server-side narrowing only: `res1-` also returns `res1-extra-1`, so the exact
  `^<backend_id>-\d+$` pattern still has to be applied client-side. `return_full_object=true` is
  required to get `token` and `blocked`. Paginated via `page` / `size` / `total_pages`.
- **`/key/update` treats an explicit `null` as *clear*, not *ignore*.** Posting
  `{"key": <hash>, "max_budget": null, "budget_duration": null, "tpm_limit": null, "rpm_limit": null}`
  against a key that carried all four returns 200 and leaves every one of them `null` on the
  following `/key/info` and `/key/list`. This is what makes the full-target-state backstop above
  work; if the proxy ignored nulls instead, a removed cap would stay on the key forever.
- **`/key/list` returns `total_pages` and `current_page` at the top level** of the payload
  (alongside `keys` and `total_count`), not nested under a `metadata` object — unlike
  `/user/daily/activity`, which nests them. Paging past the last page returns an empty `keys` list
  rather than an error. The two clients read the field from different places for this reason.
- **`blocked` comes back as `null`, not `false`,** for a key that was never blocked, so it must be
  read as truthy/falsy rather than compared against `False`.
- **Aliases are globally unique**: a duplicate `key_alias` is rejected with 400. A delete frees the
  alias, which is what makes the delete-then-generate rotation fallback work.
- **`/health/readiness` needs no auth** and returns `{"status": "healthy", "db": "connected"}`. The
  key API is useless without the database, so `db != "connected"` is treated as down.
- Spend rows are flushed in batches, so usage lags a live request by a few seconds.

## Tests

```bash
cd plugins/litellm && uv run pytest tests/
```

The HTTP layer is mocked throughout — no live proxy is required.

### Running against a real proxy

```bash
docker network create litellm-net
docker run -d --name litellm-db --network litellm-net \
  -e POSTGRES_PASSWORD=litellm -e POSTGRES_USER=litellm -e POSTGRES_DB=litellm postgres:17
docker run -d --name litellm --network litellm-net -p 4000:4000 \
  -v $PWD/config.yaml:/app/config.yaml \
  -e DATABASE_URL=postgresql://litellm:litellm@litellm-db:5432/litellm \
  -e LITELLM_MASTER_KEY=sk-master-local \
  ghcr.io/berriai/litellm:main-stable --config /app/config.yaml --port 4000
```

A `config.yaml` with a `mock_response` model exercises the whole path without an upstream provider
or an API key:

```yaml
model_list:
  - model_name: fake-gpt
    litellm_params:
      model: openai/gpt-4o
      api_key: sk-fake
      mock_response: "Hello from the mock model."
general_settings:
  master_key: sk-master-local
```
