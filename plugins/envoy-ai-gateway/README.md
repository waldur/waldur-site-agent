# Envoy AI Gateway Plugin for Waldur Site Agent

This plugin integrates [Envoy AI Gateway](https://aigateway.envoyproxy.io/) with Waldur so that
LLM inference access can be sold, provisioned, metered, and billed through the Waldur marketplace.
It provisions per-customer API keys from marketplace orders and reports usage back to Waldur for
metering and billing. Token/cost budgets are enforced by Waldur (report usage -> mastermind pauses
the resource, or a single key, when a limit is reached -> the agent blocks the keys), not by
the gateway.

The plugin is Kubernetes-native: API keys live in Kubernetes Secrets that the Envoy Gateway
`SecurityPolicy` reads.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `envoy` | `waldur_site_agent.backends` | order processing, membership sync |
| `envoy-usage` | `waldur_site_agent.backends` | reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process` (for API key commands).

| Operation | Behaviour |
|---|---|
| Create resource | Registers the resource and its gateway endpoint; the agent then issues its API keys |
| Terminate resource | Removes every key of the resource from both Secrets |
| Update limits | **No-op** — Waldur enforces limits by pausing the resource from reported usage |
| Pause / restore | Moves the resource's keys to the blocked Secret and back |
| Downscale | Same as pause — keys are blocked; a key has no partial-capacity state |
| Usage reporting | `envoy`: **No-op**. `envoy-usage`: usage from the warehouse API |

## Features

- **API key lifecycle**: provision, pause, restore, and terminate gateway API keys directly from
  Waldur marketplace orders
- **Per-key governance**: request, rotate, pause, resume and delete one key at a time from the
  portal. The resource's other keys keep serving
- **Two-secret pause model**: suspend access at the authentication layer by moving a key between
  an *active* and a *blocked* Secret — no key regeneration on restore
- **Usage reporting**: report token usage from a pluggable usage warehouse back to Waldur for
  metering and billing, per resource and, where the usage shipper records it, per key
- **In-cluster or external**: run inside the cluster (in-cluster config) or against a kubeconfig
  context for local development

## Overview

The plugin exposes two backends that are normally paired on a single composed offering:

- **Management backend** (`envoy`): the `order_processing_backend`. Owns the API key lifecycle:
  provision, pause, restore and terminate for the whole resource, and Waldur's per-key commands.
- **Usage reporting backend** (`envoy-usage`): the `reporting_backend`. Reads token usage from a
  usage warehouse and submits it to Waldur, per resource and, where the rows name a key, per key.

Token pricing itself lives in Waldur: the reporting backend submits raw token counts, and the
offering plan prices them (e.g. €/token). Everything is keyed off the resource's `backend_id`:
its keys are the Secret entries `<backend_id>-<n>`, and its usage is recorded under the
`backend_id` itself.

For deployments that price usage upstream instead (per-model rates, discounts), the sibling
`cscs-dwdi-inference` reporting backend is the cost-model alternative: it reports a pre-priced
`token_cost` component that Waldur records as-is rather than raw token counts. Pair it in place of
`envoy-usage` when the warehouse — not Waldur — owns pricing.

## Backend Types

### Management Backend (`envoy`)

Envoy Gateway's `SecurityPolicy` authenticates requests against API keys stored as `clientID: key`
entries in a Kubernetes Secret. This backend manages those entries.

**Key lifecycle** — to pause and restore a key without regenerating it, two Secrets are kept and
entries move between them:

- **create (order)**: register the resource under the backend id the order processor generates
  (`{allocation_prefix}{resource slug}`) and surface the gateway endpoint on it. The agent then
  generates the resource's keys, adds each as a `<backend_id>-<n>: key` entry to the **active**
  Secret, and reports it to Waldur.
- **pause**: move every key of the resource **active → blocked** — authentication now fails
  with 401.
- **restore**: move the resource's keys **blocked → active**, except keys paused on their own.
- **terminate**: remove every key of the resource from both Secrets.

**Per-key lifecycle.** Each key is one Secret entry, `<backend_id>-<n>`, so the backend declares
`supports_resource_api_key_lifecycle` and carries out Waldur's commands for a single key. The agent
applies each command to the Secrets first and then acknowledges it to Waldur:

| Command | Acknowledgement | What happens in the Secrets |
|---------|-----------------|-----------------------------|
| create | `set_key` | A new entry under the next free `<backend_id>-<n>`; blocked on a paused resource |
| rotate | `set_key` | The value is overwritten in place; a key left paused by an erred pause serves again |
| pause | `set_paused` | The entry moves to the blocked Secret as `<client_id>.paused` |
| resume | `set_ok` | The entry moves back to the active Secret, or to a plain blocked entry on a paused resource |
| update | `set_ok` | Nothing, as Waldur enforces limits; a key left paused by an erred pause serves again |
| delete | `set_deleted` | The entry is removed from both Secrets |

A new key never reuses a client_id Waldur has held for the resource, including a deleted key's,
because Waldur attributes usage by client_id. A key resumed on a paused resource comes back with
the other keys when the resource is restored.

Waldur accepts these commands only on an offering with the `enable_api_key_provisioning` plugin
option set; without it, keys can still be rotated. A key command is not an order, but the agent
picks it up alongside the orders:

- In `order_process` mode, on the next cycle, after the orders (every
  `WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES`, 5 by default), however recently Waldur issued it.
- In `event_process` mode with `stomp_enabled: true`, as soon as Waldur sends it. If the message is
  lost, the reconciliation sweep applies the command once it has been pending for 30 minutes, on
  the sweep's next run (every `WALDUR_SITE_AGENT_RECONCILIATION_PERIOD_MINUTES`, 60 by default).

As with orders, `order_process` skips an offering with `stomp_enabled: true`.

**Resource pause and key pause.** Pausing a whole resource and pausing one key are independent,
and the name of an entry in the blocked Secret records which one holds it back. The gateway only
reads the active Secret; the blocked Secret is where a switched-off key's value waits, so it can
serve again without being regenerated. Take a resource `abc` with the keys `abc-1`, `abc-2` and
`abc-3`:

```text
                               active Secret         blocked Secret
consumer pauses abc-2          abc-1, abc-3          abc-2.paused
Waldur pauses the resource     (empty)               abc-1, abc-3, abc-2.paused, abc.resource-paused
Waldur unpauses the resource   abc-1, abc-3          abc-2.paused
```

- **`<client_id>.paused`** — the key is paused on its own. Only a resume, rotate or update brings
  it back.
- **`<client_id>`** — the key is held back because its resource is paused. Restoring the resource
  moves these entries, and only these, back to the active Secret. This matters because the
  membership sync restores every resource that is not paused on every cycle: without the suffix,
  a key paused on its own would serve again within one cycle.
- **`<backend_id>.resource-paused`** — records the resource pause itself; it is a flag, not a
  key. A key added to the resource checks it to decide whether it lands active or blocked. The
  keys alone cannot answer that when the resource has no keys left, or when every key left is
  paused on its own.

No additional Secret is needed.

Rotate and update settle a key as OK in Waldur, so after either one the key serves, unless its
resource is paused. Waldur allows both on an Erred key, and an Erred key can still be held by its own
pause when the pause was applied but its acknowledgement failed.

**Limits and models.** A key's limits work like the resource's limits. The plugin stores and enforces
nothing: the reporting backend reports each key's usage, and Waldur pauses the key once its usage
reaches a limit. The gateway has no per-key model rule, so a command that would restrict a key to
some models fails with an error rather than leaving an unrestricted key that Waldur shows as
restricted. Leave a key's model list empty on this backend.

**Kubernetes objects touched:** Secrets (`get`/`patch`) in the configured namespace.

**Limit enforcement** — the gateway does not cap usage. The reporting backend submits usage to
Waldur; when a `LIMIT` component's reported usage reaches its limit and the offering sets
`plugin_options.action_on_usage_limit: pause`, mastermind pauses the resource and the agent blocks
its keys (active -> blocked Secret). When usage drops back below the limit, the resource is
unpaused and the keys are restored. A key's own limit works the same way for that key alone (see
[Limits and models](#management-backend-envoy) above).

### Usage Reporting Backend (`envoy-usage`)

A read-only backend that reports token usage from a usage warehouse — any HTTP service that
exposes the endpoints below (for example a small collector that aggregates the gateway's
per-request usage metrics). Usage is reported per resource and, when the rows name the key they
came through, per key as well (see [Per-key usage](#per-key-usage)).

**API endpoints used:**

- `GET /usage-month?from=YYYY-MM&to=YYYY-MM&client_id=...` — per-`client_id` usage rows for a
  month range; response shape `{"usage": [{"client_id": "...", "input_tokens": N, "output_tokens": N}]}`,
  where a row may also carry `key_client_id`
- `GET /health` — liveness check used by `ping()`

The backend maps the warehouse's token fields onto the offering's components, reporting only the
components the offering defines. It implements `get_usage_report_for_period()`, so historical
usage can be bulk-loaded with the core `waldur_site_load_historical_usage` command.

## Configuration

The two backends are typically combined on one composed offering and share a single
`backend_settings` block. The keys for each backend are listed separately below, followed by a
combined example.

### Management backend settings (`envoy`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `namespace` | yes | — | Namespace holding the api-key Secrets |
| `gateway_url` | yes | — | Public base URL of the gateway, used for the resource endpoint |
| `apikey_secret` | no | `envoy-ai-gateway-apikeys` | Secret the `SecurityPolicy` reads keys from |
| `blocked_secret` | no | `<apikey_secret>-blocked` | Secret holding paused keys (resource or key paused) |
| `kubeconfig_path` | no | in-cluster | Path to a kubeconfig; omit to use in-cluster config |
| `kube_context` | no | — | kubeconfig context (local/dev); without it and `kubeconfig_path`: in-cluster |

### Usage reporting backend settings (`envoy-usage`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | Usage warehouse base URL |
| `api_token` | no | — | Bearer token for the warehouse API |

### Composed offering (both backends)

```yaml
offerings:
  - name: "LLM Inference"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<waldur api token>"
    waldur_offering_uuid: "<offering uuid>"
    backend_type: "envoy"
    order_processing_backend: "envoy"
    membership_sync_backend: "envoy"     # required for pause/restore (key blocking)
    reporting_backend: "envoy-usage"
    stomp_enabled: true                  # apply API key commands (rotate, per-key) immediately

    backend_settings:
      # --- management backend (envoy) ---
      namespace: "ai-gateway"
      gateway_url: "https://ai-gateway.example.com"
      apikey_secret: "envoy-ai-gateway-apikeys"   # optional, this is the default
      blocked_secret: null                         # optional, defaults to <apikey_secret>-blocked
      kubeconfig_path: null                        # omit to use in-cluster config
      # --- usage reporting backend (envoy-usage) ---
      api_url: "http://usage-warehouse:9000"
      api_token: null

    backend_components:
      input_tokens:
        measured_unit: "tokens"
        accounting_type: "usage"
        label: "Input tokens"
      output_tokens:
        measured_unit: "tokens"
        accounting_type: "usage"
        label: "Output tokens"
```

The `input_tokens` / `output_tokens` components are the usage meters the reporting backend fills
in; `accounting_type: usage` here is the agent-side metering type. Enforcement is configured on the
Waldur **offering**, not in this file: for a resource to auto-pause, the matching offering
component must be `billing_type: LIMIT` and the offering must set
`plugin_options.action_on_usage_limit: pause` — only `LIMIT` components are checked against their
limit. When reported usage reaches the limit, Waldur pauses the resource and the agent blocks its
keys. Blocking runs in the membership-sync loop, so `membership_sync_backend: envoy` must be set
(as above) — without it the agent never blocks the keys and the limit is not enforced.

## Prerequisites

- **Both Secrets must pre-exist.** Create empty `apikey_secret` and `<apikey_secret>-blocked`
  Secrets in `namespace` as part of the deployment; the plugin patches entries into them but does
  not create the Secrets themselves.
- **RBAC.** The agent's ServiceAccount needs the permissions below.

### Kubernetes RBAC

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: waldur-site-agent
  namespace: ai-gateway
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "patch"]
```

## Usage Reporting

The `envoy-usage` backend is read-only. It implements usage reporting and resource pull, but does
not create, pause, restore, or otherwise manage resources (those belong to the `envoy` backend).
Usage is reported for the current month via `_get_usage_report()` and for arbitrary past months
via `get_usage_report_for_period()`, which the historical loader uses.

### Per-key requests, per-resource billing

A resource owns several API keys, but usage is attributed **per resource** — otherwise
rotating a key would split a tenant's bill in two.

The gateway forwards `x-client-id` per key (`<resource_backend_id>-<n>`), and the rollup
happens at the **usage-shipper** (a Vector pipeline): its `remap` transform strips the
`-<n>` suffix, so usage lands under the resource's `backend_id`:

```coffee
# usage-shipper (Vector) remap transform
cid = replace(cid, r'-\d+$', "")
```

`envoy-usage` then queries the warehouse by `resource_backend_id` unchanged — no key
enumeration. Pausing, restoring and terminating the resource are the opposite: they act on the
gateway Secret rather than on usage, so they fan out to **every** client-id the resource owns.
Waldur's per-key commands act on the one client-id they name.

### Per-key usage

Waldur enforces a key's limits against the usage reported for that key. The shipper supplies it by
keeping the unstripped client-id on each row as `key_client_id`, beside the stripped
`client_id`:

```coffee
# usage-shipper (Vector) remap transform
.key_client_id = cid
cid = replace(cid, r'-\d+$', "")
```

```json
{"usage": [{"client_id": "<backend_id>", "key_client_id": "<backend_id>-1",
            "input_tokens": 120, "output_tokens": 30}]}
```

Billing does not change. The resource total is still the sum of every row for the resource's
`client_id`, whether or not the row names a key. Rows that name one of the resource's keys are also
summed per key, so the per-key figures add up to that total. The agent reports every key's
current-month usage with `report_usage`, including deleted keys, in the same `report` cycle
that submits the resource total. A key with no rows this month is
reported as zero, so last month's figure does not keep it over its limit. Rows naming a key Waldur
does not hold are logged and not reported, so the per-key figures then add up to less than the
total.

A resource that has non-zero usage on a row naming none of its keys is not reported per key for
that month. Splitting only part of the total across keys would under-count against their limits.
This happens with rows shipped before the shipper recorded keys. It is also skipped for an
offering that does not have Waldur's `enable_api_key_provisioning` plugin option set.

## Installation

The plugin is discovered automatically once `waldur-site-agent-envoy-ai-gateway` is installed
alongside `waldur-site-agent`.

### UV Workspace Installation

```bash
# Install all workspace packages including this plugin
uv sync --all-packages
```

### Manual Installation

```bash
# From source
pip install -e plugins/envoy-ai-gateway/
```

## Testing

Run the tests from inside the plugin directory; the plugin's entry points only resolve there.

```bash
cd plugins/envoy-ai-gateway

# Run all plugin tests
uv run pytest tests/

# Run with coverage
uv run pytest tests/ --cov=waldur_site_agent_envoy_ai_gateway
```

The suite covers the resource-level key lifecycle (provision/pause/restore/terminate), each
per-key command and how it interacts with a resource pause, the usage warehouse client, and usage
report mapping, per resource and per key. The Kubernetes and HTTP clients are mocked or replaced
by an in-memory Secret store, so no live cluster or warehouse is required.

## Troubleshooting

### Keys are provisioned but requests are rejected

- Confirm the `SecurityPolicy` reads the Secret named by `apikey_secret`.
- Check the key landed in the **active** Secret, not the blocked one (`kubectl get secret ...`).
- Verify the request sends the API key the order surfaced.

### One key is rejected while the resource's other keys work

- The key is paused on its own: the blocked Secret holds it as `<client_id>.paused`. Resume it
  from Waldur; restoring the resource does not bring it back.
- The key is deleted, or its entry was never written. Check the key's state in Waldur: a command
  that failed leaves the key Erred with the error attached.

### A new key is blocked

- The resource is paused in Waldur, or the blocked Secret still holds its
  `<backend_id>.resource-paused` marker. A resource that is not paused has the marker cleared on
  the next membership-sync cycle.

### A per-key command stays pending in Waldur

- Confirm the agent runs in `order_process` or `event_process` mode; `report` and
  `membership_sync` do not carry out key commands.
- In `order_process` mode, the command is applied on the next cycle (5 minutes by default). That
  mode skips an offering with `stomp_enabled: true`, so such an offering needs `event_process`.
- In `event_process` mode, a command whose STOMP message was lost waits for the reconciliation
  sweep: 30 minutes pending, then the sweep's next run (hourly by default).
- The agent log names each command it applies: `Applying API key <action> to key <uuid>`.

### Usage reports are empty or zero

- Check the warehouse is reachable: the backend's `ping()` hits `GET /health`.
- Confirm the warehouse returns rows keyed by the same `client_id` the gateway authenticates with
  (the resource `backend_id`).
- Confirm the offering's `backend_components` are named to match the warehouse fields
  (`input_tokens`, `output_tokens`).

### Per-key usage is not reported

- The Waldur offering must set the `enable_api_key_provisioning` plugin option.
- The warehouse rows must carry `key_client_id` (see [Per-key usage](#per-key-usage)). A resource
  with non-zero usage on a row that names none of its keys is reported only as a total; the agent
  logs `Usage of <backend_id> is not attributed to its keys`.

### RBAC errors on Secrets

- Grant `get`/`patch` on Secrets in `namespace` (see
  [Kubernetes RBAC](#kubernetes-rbac)).

## Development

### Project Structure

```text
plugins/envoy-ai-gateway/
├── pyproject.toml
├── README.md
├── waldur_site_agent_envoy_ai_gateway/
│   ├── __init__.py
│   ├── backend.py          # EnvoyAIGatewayBackend — resource and per-key lifecycle (management)
│   ├── client.py           # EnvoyAIGatewayClient — K8s Secrets (active, blocked, paused entries)
│   ├── reporting.py        # EnvoyUsageReportingBackend — token usage, per resource and per key
│   ├── usage_client.py     # EnvoyUsageClient — usage warehouse HTTP client
│   └── schemas.py          # Pydantic configuration schemas
└── tests/
    ├── test_envoy_backend.py
    ├── test_envoy_client.py
    ├── test_envoy_key_lifecycle.py
    ├── test_envoy_usage_backend.py
    └── test_envoy_usage_client.py
```

### Key Classes

- **`EnvoyAIGatewayBackend`** (`envoy`): management backend — provisions and blocks/unblocks a
  resource's keys, and mints, pauses, resumes, updates and deletes a single key
- **`EnvoyAIGatewayClient`**: Kubernetes client for api-key Secrets
- **`EnvoyUsageReportingBackend`** (`envoy-usage`): reports token usage to Waldur, per resource
  and per key
- **`EnvoyUsageClient`**: HTTP client for the usage warehouse

### Registered Entry Points

Each backend registers under the same name across three groups (`waldur_site_agent.backends`,
`waldur_site_agent.component_schemas`, `waldur_site_agent.backend_settings_schemas`):

- `envoy` — management backend + its schemas
- `envoy-usage` — reporting backend + its schemas

### Extension Points

- **Different usage warehouse**: implement the two HTTP endpoints, or subclass `EnvoyUsageClient`
  to match another warehouse's API.
- **Cost-based reporting**: report a priced `token_cost` component instead of raw token counts if
  pricing should live outside Waldur — the sibling `cscs-dwdi-inference` backend implements exactly
  this model and can be used as the `reporting_backend` in place of `envoy-usage`.
