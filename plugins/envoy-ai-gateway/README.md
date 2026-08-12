# Envoy AI Gateway Plugin for Waldur Site Agent

This plugin integrates [Envoy AI Gateway](https://aigateway.envoyproxy.io/) with Waldur so that
LLM inference access can be sold, provisioned, metered, and billed through the Waldur marketplace.
It provisions per-customer API keys from marketplace orders and reports usage back to Waldur for
metering and billing. Token/cost budgets are enforced by Waldur (report usage -> mastermind pauses
the resource when a limit is reached -> the agent blocks the key), not by the gateway.

The plugin is Kubernetes-native: API keys live in Kubernetes Secrets that the Envoy Gateway
`SecurityPolicy` reads.

## Features

- **API key lifecycle**: provision, pause, restore, and terminate gateway API keys directly from
  Waldur marketplace orders
- **Two-secret pause model**: suspend access at the authentication layer by moving a key between
  an *active* and a *blocked* Secret — no key regeneration on restore
- **Usage reporting**: report per-key token usage from a pluggable usage warehouse back to Waldur
  for metering and billing
- **In-cluster or external**: run inside the cluster (in-cluster config) or against a kubeconfig
  context for local development

## Overview

The plugin exposes two backends that are normally paired on a single composed offering:

- **Management backend** (`envoy`): the `order_processing_backend`. Owns the API key lifecycle
  (provision, pause, restore, terminate).
- **Usage reporting backend** (`envoy-usage`): the `reporting_backend`. Reads per-key token usage
  from a usage warehouse and submits it to Waldur.

Token pricing itself lives in Waldur: the reporting backend submits raw token counts, and the
offering plan prices them (e.g. €/token). Keys and usage are keyed off a single
`client_id` (the resource's `backend_id`) — provision it once and everything else follows.

For deployments that price usage upstream instead (per-model rates, discounts), the sibling
`cscs-dwdi-inference` reporting backend is the cost-model alternative: it reports a pre-priced
`token_cost` component that Waldur records as-is rather than raw token counts. Pair it in place of
`envoy-usage` when the warehouse — not Waldur — owns pricing.

## Backend Types

### Management Backend (`envoy`)

Envoy Gateway's `SecurityPolicy` authenticates requests against API keys stored as `clientID: key`
entries in a Kubernetes Secret. This backend manages those entries.

**Key lifecycle** — to support pause/restore using only the `client_id`, two Secrets are kept and
entries move between them:

- **create (order)**: generate a `client_id` + random key, add `client_id: key` to the **active**
  Secret, and return `client_id` as the `backend_id` (surfacing the key + endpoint on the
  resource).
- **pause**: move the entry **active → blocked** — authentication now fails with 401.
- **restore**: move the entry **blocked → active**.
- **terminate**: remove the entry from both Secrets.

**Kubernetes objects touched:** Secrets (`get`/`patch`) in the configured namespace.

**Limit enforcement** — the gateway does not cap usage. The reporting backend submits usage to
Waldur; when a `LIMIT` component's reported usage reaches its limit and the offering sets
`plugin_options.action_on_usage_limit: pause`, mastermind pauses the resource and the agent blocks
the key (active -> blocked Secret). When usage drops back below the limit, the resource is unpaused
and the key is restored.

### Usage Reporting Backend (`envoy-usage`)

A read-only backend that reports per-key token usage from a usage warehouse — any HTTP service
that exposes the endpoints below (for example a small collector that aggregates the gateway's
per-request usage metrics).

**API endpoints used:**

- `GET /usage-month?from=YYYY-MM&to=YYYY-MM&client_id=...` — per-`client_id` usage rows for a
  month range; response shape `{"usage": [{"client_id": "...", "input_tokens": N, "output_tokens": N}]}`
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
| `blocked_secret` | no | `<apikey_secret>-blocked` | Secret holding paused keys |
| `kubeconfig_path` | no | in-cluster | Path to a kubeconfig; omit to use in-cluster config |
| `kube_context` | no | current | kubeconfig context to target (local/dev) |

### Usage reporting backend settings (`envoy-usage`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | Usage warehouse base URL |
| `api_token` | no | — | Bearer token for the warehouse API |

### Composed offering (both backends)

```yaml
offerings:
  - name: "LLM Inference"
    order_processing_backend: "envoy"
    membership_sync_backend: "envoy"     # required for pause/restore (key blocking)
    reporting_backend: "envoy-usage"

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
limit. When reported usage reaches the limit, Waldur pauses the resource and the agent blocks the
key. Blocking runs in the membership-sync loop, so `membership_sync_backend: envoy` must be set
(as above) — without it the agent never blocks the key and the limit is not enforced.

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
enumeration. Pause, restore and terminate are the opposite: they act on the gateway Secret
rather than on usage, so they fan out to **every** client-id the resource owns.

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

```bash
# Run all plugin tests
uv run pytest plugins/envoy-ai-gateway/tests/

# Run with coverage
uv run pytest plugins/envoy-ai-gateway/tests/ --cov=waldur_site_agent_envoy_ai_gateway
```

The suite covers key lifecycle (provision/pause/restore/terminate), the usage warehouse client, and
usage report mapping — all with the Kubernetes and HTTP clients mocked, so no live cluster or
warehouse is required.

## Troubleshooting

### Keys are provisioned but requests are rejected

- Confirm the `SecurityPolicy` reads the Secret named by `apikey_secret`.
- Check the key landed in the **active** Secret, not the blocked one (`kubectl get secret ...`).
- Verify the request sends the API key the order surfaced.

### Usage reports are empty or zero

- Check the warehouse is reachable: the backend's `ping()` hits `GET /health`.
- Confirm the warehouse returns rows keyed by the same `client_id` the gateway authenticates with
  (the resource `backend_id`).
- Confirm the offering's `backend_components` are named to match the warehouse fields
  (`input_tokens`, `output_tokens`).

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
│   ├── backend.py          # EnvoyAIGatewayBackend — key lifecycle (management)
│   ├── client.py           # EnvoyAIGatewayClient — K8s Secrets
│   ├── reporting.py        # EnvoyUsageReportingBackend — token usage reporting
│   ├── usage_client.py     # EnvoyUsageClient — usage warehouse HTTP client
│   └── schemas.py          # Pydantic configuration schemas
└── tests/
    ├── test_envoy_backend.py
    ├── test_envoy_client.py
    ├── test_envoy_usage_backend.py
    └── test_envoy_usage_client.py
```

### Key Classes

- **`EnvoyAIGatewayBackend`** (`envoy`): management backend — provisions and blocks/unblocks keys
- **`EnvoyAIGatewayClient`**: Kubernetes client for api-key Secrets
- **`EnvoyUsageReportingBackend`** (`envoy-usage`): reports token usage to Waldur
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
