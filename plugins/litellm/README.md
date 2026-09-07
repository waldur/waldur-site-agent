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
- **Chat surface (optional)**: provision people into [Open WebUI](https://openwebui.com)
  alongside their LiteLLM user, bill their chat usage back to the resource, and revoke
  both together — see [The chat surface](#the-chat-surface-open-webui)

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

## The chat surface (Open WebUI)

Optional, and off unless the offering's `backend_settings` carry an `openwebui:` block.
It turns the offering from "API keys" into "API keys **and** a chat product", with usage
from both surfaces billed to the same Waldur resource — the thing the customer actually
asks for when they say they want to see one number.

### How the two systems are married

By **email**, because nothing else is available. Open WebUI mints its own opaque user id
and offers no way to set one; LiteLLM declined to match the forwarded address against
its `user_email` column ([BerriAI/litellm#21927], closed as not planned). So the LiteLLM
user is created with `user_id` **set to the person's address**, which is what the
proxy resolves the forwarded header onto.

Open WebUI authenticates every upstream call with **one shared virtual key** and, with
`ENABLE_FORWARD_USER_INFO_HEADERS=True`, forwards the signed-in person on each request:

```yaml
# on the proxy
general_settings:
  user_header_mappings:
    - header_name: X-OpenWebUI-User-Email
      litellm_user_role: internal_user
```

Only the **first** entry whose role is `internal_user` is read; the proxy takes that
header name and ignores every other mapping in the list.

### What that header does — and does not — do

Verified against `ghcr.io/berriai/litellm:main-stable` on 2026-08-31, and the reason the
plugin is shaped the way it is. `add_internal_user_from_user_mapping` writes the header
value onto `user_api_key_dict.user_id` and does nothing else. It runs in the **pre-call**
path, *after* `user_api_key_auth`:

| Probe | Result |
|---|---|
| chat request under an address the proxy has never seen | **200**, served |
| `GET /user/info` for that address afterwards | **404**, no user was created |
| over-budget user (spend 0.00045, `max_budget` 1e-7) via **their own key** | **429** |
| the same over-budget user via the **shared key + header** | **200** |

So a LiteLLM user is an **attribution record, not a gate**. Its `max_budget`,
`tpm_limit`, `rpm_limit` and `models` are never consulted for chat traffic — that
request authenticated as the shared key and was checked against the shared key's owner.
This plugin therefore writes only identity and ownership onto the user: a budget there
would read as an enforced cap that enforces nothing.

Two consequences are built into the backend:

- **Revocation happens in Open WebUI.** Deleting the LiteLLM user does not end chat
  access; the address simply becomes one the proxy has never seen, which it serves.
  `LiteLLM_DailyUserSpend.user_id` has no foreign key to the user table, and the
  user-table rollup is an `UPDATE`, never an upsert — so the spend accrues on daily rows
  while the (absent) user row moves by nothing.
- **Pausing a resource closes both surfaces.** Blocking the keys only closes the API one.
  Chat runs on the shared key, which this plugin neither owns nor may block — blocking it
  would cut off every other tenant on the proxy — so a paused resource is paused person
  by person, by demoting each member's chat account. A pause always *demotes*, whatever
  `delete_accounts_on_removal` says: it is a temporary state, and deleting the account
  there would destroy the person's conversations over an unpaid invoice. Restore
  re-enables a demoted account and creates nothing that is not already there.

### One entitlement per person

A LiteLLM `user_id` is global and here it is an address, so usage arriving under an
address can only be billed to **one** resource. The plugin stamps the owning resource
into the user's `metadata` and **refuses** to move an address another resource already
owns, rather than taking it over: a silent takeover would move the first resource's chat
billing onto the second one while the person kept chatting throughout.

That makes the offering responsible for enforcing one chat-enabled resource per person
at order time. The plugin can refuse the second claim; it cannot choose which should win.
A record with **no** owner stamped on it is adopted instead of refused — it is either a
user an admin added by hand or one left by an interrupted add, and refusing would leave
a resource unable to provision its own member.

### What the Waldur offering must have

The chat surface is reached through the person's **offering user**, and the agent keys on
its `username`. An offering user with a blank username is skipped: that person holds a
resource in state OK, sees it in the portal, and has no chat account, with nothing on
either side saying why.

That is the default. `generate_username` falls back to the `service_provider` policy,
which returns an empty string on purpose and waits for the provider to assign a username
by hand — correct for a backend with its own account namespace (SLURM, FreeIPA), wrong
here, where the identity is just the person's address.

So set `username_generation_policy` in the offering's `plugin_options` to one that
generates automatically — `waldur_username` or `identity_claim`. Leave it unset and the
integration appears to work for whoever was set up by hand and silently does nothing for
everyone who joins later.

The agent logs a warning naming every team member skipped this way, so the condition is
visible in the sync log rather than only in the absence of an account.

### Account provisioning

- **`sso` (default)** — the agent creates nothing; the identity provider creates the
  account on first login with the address it asserts. The agent still **re-enables** a
  previously demoted account, because an SSO login into one does not restore it, and
  still revokes.
- **`managed_password`** — the agent creates the account through
  `POST /api/v1/auths/add` with the operator-supplied `initial_password`, mirroring what
  an admin does by hand today.

`sso` is the target state: it removes password minting, transport and storage from
Waldur entirely, and makes the address match structural rather than coincidental. It has
been verified end to end against real Keycloak — see [Running behind an identity
provider](#running-behind-an-identity-provider-sso), which also carries the deployment
requirements that come with it.

`managed_password` exists because that is where most deployments start — Waldur has no
encrypted channel to hand a password to an end user, so one initial secret is shared by
every account it creates and the person is expected to change it on first login.

Removal **demotes** the account to Open WebUI's `pending` role by default rather than
deleting it: access ends either way, and demotion keeps the person's conversations so a
re-add gives back what they had. `delete_accounts_on_removal: true` deletes instead — on
the paths where the entitlement is actually gone (a member leaving the project, the
resource being terminated) and never on pause.

### What the Open WebUI deployment must have

Verified against `ghcr.io/open-webui/open-webui:main` on 2026-09-03. Three settings on
the Open WebUI side are load-bearing; with any of them wrong the integration fails in a
way that looks like a Waldur bug.

- **`ENABLE_FORWARD_USER_INFO_HEADERS=True`** — the whole design rests on it. Without
  it every request bills to the shared key's owner and per-person attribution returns
  nothing — *silently*, because usage still records correctly, just all under one
  identity.
- **`BYPASS_MODEL_ACCESS_CONTROL=True`** — without it a newly created account sees an
  **empty model list** and every chat returns `Model not found`. Open WebUI shows
  non-admins only models that have a stored model record, and models discovered from a
  connection have none.
- **`ENABLE_API_KEYS`** (admin config, **not** the env var) — the credential this plugin
  authenticates with.

`BYPASS_MODEL_ACCESS_CONTROL` is also the right call on its own merits: LiteLLM already
decides which models a key may use, so gating them a second time in Open WebUI only adds
a place for the two to disagree. The alternative is an admin persisting and publishing
each model by hand, repeated whenever the proxy's model list changes.

**`ENABLE_API_KEYS` is a persistent config**, stored in the database on first boot, after
which the environment variable is ignored — setting it on an existing container does
nothing. Change it through `POST /api/v1/auths/admin/config`, which requires the entire
document, so read it and write it back. Many Open WebUI settings behave this way; treat
env vars as first-boot defaults rather than as configuration.

Use an **API key**, not a session JWT, for `openwebui.api_token`: `JWT_EXPIRES_IN`
defaults to `4w`, so an agent configured with a JWT works for a month and then fails
every sync.

#### Verified admin API behaviour

- Demoting a member to `pending` while they hold a **live** session token: the next
  model call is **401**. Revocation is immediate, with no wait for token expiry.
- Background calls (title / tag / follow-up generation) carry the user header, so they
  bill to the person rather than leaking to the shared key.
- `GET /api/v1/users/` returns `{"users": [...], "total": N}`, and `query` is a
  *substring* match — which is why `find_user` filters exactly afterwards.
- `POST /api/v1/users/{id}/update` is a **replace, not a merge**. On every released
  version its `UserUpdateForm` requires `role`, `name`, `email` and
  `profile_image_url`, and the handler writes all four unconditionally; they became
  optional only after v0.6.34, on `main`. A body carrying just the role is a 422 there,
  so `set_role` echoes the whole account back from the record it looked up.
- `POST /api/v1/auths/add` for an existing address is a **400**, so callers look the
  account up first.
- `DELETE /api/v1/users/{id}` for an unknown id is **`200 true`**, not 404.

Sign-in still succeeds for a demoted account: the person reaches the "awaiting approval"
screen and can read their old conversations, but every model call is refused. That is the
intended shape of a paused subscription, and worth stating to the customer explicitly,
because "they can still log in" otherwise reads as a failure to revoke.

#### Running behind an identity provider (SSO)

Verified against Keycloak 26.0 on 2026-09-03: an OIDC login for an address Open WebUI
has never seen creates the account, stores **exactly** the address the `email` claim
asserts, and lands it in `pending`; chatting is refused with 401 until the agent
promotes it. So the address match that the whole design rests on is structural under
SSO rather than coincidental — provided Waldur and Open WebUI authenticate against the
same provider.

Three things about account creation that are easy to get wrong:

- **`ENABLE_OAUTH_SIGNUP` must be on.** It governs OIDC account creation, and the agent
  cannot create SSO accounts itself. `ENABLE_SIGNUP` is a *different* flag covering only
  the password form; leaving it off is correct and does not restrict SSO.
- **`DEFAULT_USER_ROLE=pending` is the real gate.** It is the only one that applies to
  every creation path — OIDC, the admin API, the password form. With it set to `user`,
  anyone the identity provider will authenticate has chat access the moment they log in,
  whether or not they hold a Waldur resource.
- **Create the admin account before anyone else can reach the URL.** See below.

**The first account created is a permanent administrator that the agent can never
manage.** Open WebUI guards the first user *row* by identity rather than by role
(`routers/users.py`): no other admin may update it, and it cannot be deleted, and that
survives any later role change. Two behaviours combine badly here — the first account
becomes `admin` regardless of `DEFAULT_USER_ROLE`, and `ENABLE_SIGNUP=False` does not
apply to it either (signup on an empty database succeeds and returns `role: admin`).

So if the first person to reach a fresh instance is a customer arriving over SSO, they
become an administrator whose access Waldur can never revoke; the agent's `set_role`
fails for them with:

```json
403 {"detail":"The requested action has been restricted as a security measure."}
```

The same 403 occurs through an admin session cookie, so it is not a limitation of
API-key authentication and there is no configuration that lifts it. Seed the admin
account deliberately as the first thing after the database is created.

Group and role claims are **not** consumed: SSO answers "who is this", and entitlement
still comes from Waldur membership.

### How chat usage is billed

`GET /user/daily/activity` returns two breakdowns of the **same** records:

```text
breakdown.api_keys[<hash>]   -> metrics, metadata.key_alias
breakdown.entities[<user_id>] -> metrics, api_key_breakdown[<hash>].metadata.key_alias
```

Summing both double-counts, so the rollup takes each record once:

1. every `api_keys` row whose alias is one of the resource's `-<n>` slots → that resource
   (the API surface, unchanged from the API-only offering);
2. every `entities` row whose id is a managed address → the resource that owns it,
   **skipping** the keys already counted in step 1.

Requests on a key with no user attached — every key this plugin mints — land under
LiteLLM's `"Unassigned"` bucket, which no managed address ever matches, so the common
case separates on its own. Step 2's exclusion covers the other one: a person calling the
API with their own key appears in both breakdowns and must be billed once.

The address-to-resource map is read from the proxy (`GET /user/list`, filtered on the
metadata stamp), not from Waldur — the reporting backend never sees a team list, and the
stamp is in any case what decides where an address bills. An API-only offering never
makes that call.

### Chat surface settings (`openwebui:`, nested under the management settings)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | Open WebUI base URL for the admin API |
| `api_token` | yes | — | Admin API token |
| `url` | no | — | User-facing chat URL surfaced as a resource endpoint in the portal |
| `account_provisioning` | no | `sso` | `sso` or `managed_password` |
| `initial_password` | when `managed_password` | — | Temporary password for created accounts |
| `delete_accounts_on_removal` | no | `false` | Delete instead of demoting; removal and terminate only, never pause |
| `verify_ssl` | no | `true` | Verify Open WebUI's TLS certificate |
| `timeout` | no | `30` | Per-request timeout in seconds |

Membership sync must be enabled (`membership_sync_backend: litellm`) — it is the pass
that provisions and revokes people.

[BerriAI/litellm#21927]: https://github.com/BerriAI/litellm/issues/21927

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
| `pause_resource()` | `POST /key/block` for every key, plus Open WebUI role → `pending` per member |
| `restore_resource()` | `POST /key/unblock` for every key, plus Open WebUI role → `user` per demoted member |
| `downscale_resource()` | same as pause (a key has no partial-capacity state) |
| `delete_resource()` | `POST /key/delete` for every key |
| `set_resource_limits()` | `POST /key/update` — full target state of the four backstop fields |
| `add_users_to_resource()` | `POST /user/new` (or `/user/update` to adopt), plus Open WebUI `auths/add` |
| `remove_users_from_resource()` | Open WebUI role → `pending` (or `DELETE`), then `POST /user/delete` |
| `list_resource_users()` | `GET /user/list`, filtered on the `waldur_resource` metadata stamp |
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

`component_metrics` decouples the component name from the metric it reads, which is what an
offering selling **several models at different prices under one offering** needs — see
[Billing many models from one offering](#billing-many-models-from-one-offering).

**Source:** `GET /user/daily/activity`, walking `results[].breakdown.api_keys` — see
[Why not `/global/spend/report`](#why-not-globalspendreport) below.

**One fetch per pass:** `/user/daily/activity` cannot be filtered by key, so a call returns the
whole proxy's month and this backend picks out the rows it wants. The processor pulls one resource
at a time (and once more per historical period), so the fetched rows are reused across the
resources of a pass — otherwise each resource would walk the entire table again. `usage_cache_ttl`
bounds that reuse so it cannot outlive the pass. It defaults to **half**
`WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` (so 15 minutes on the default 30-minute period): a whole
period would keep an entry alive into the next pass, which is exactly when fresh numbers are due.

**Rollup:** usage is attributed per **resource**, not per key. On an offering with a
chat surface it is also attributed per **person** — see
[How chat usage is billed](#how-chat-usage-is-billed). The `-<n>` slot suffix is stripped
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
| `openwebui` | no | — | Chat surface; see [The chat surface](#the-chat-surface-open-webui) |

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

      # Optional chat surface. Omit the whole block for an API-only offering.
      openwebui:
        api_url: "https://chat.example.com"
        api_token: "owui-admin-token"
        url: "https://chat.example.com"
        account_provisioning: "sso"          # or "managed_password" + initial_password
        delete_accounts_on_removal: false

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

## Billing many models from one offering

A provider selling `gpt-4o` alongside a small local model cannot price them with `input_tokens`
and `output_tokens`: those carry one rate for every model on the offering, so a million frontier
tokens and a million cheap ones invoice identically. Waldur has no per-model dimension to fix this
with — `ComponentUsage` is `(resource, component, period, quantity)` and nothing more.

LiteLLM has already solved it. The proxy applies its own per-model cost map and reports the result
as `spend`, which this plugin surfaces as the `token_cost` metric. Billing that figure at a plan
price of **1.0** charges exactly what the proxy costed, model by model, with no Waldur-side rate to
keep in sync and no markup.

The obstacle is that one component cannot both cap and charge. Waldur invoices `USAGE` components
and ignores `LIMIT` ones, and only a `LIMIT` component can carry a cap through `resource.limits` —
so an offering that wants both a spending cap and cost-passthrough billing needs the same number
under two names:

```yaml
    backend_settings:
      # inference_cost is not named after a metric, so say what it reads. The other
      # three are metered by their names alone.
      component_metrics:
        inference_cost: token_cost

    backend_components:
      token_cost:     { measured_unit: "USD", accounting_type: "limit", label: "Budget" }
      inference_cost: { measured_unit: "USD", accounting_type: "usage", label: "Inference cost" }
      input_tokens:   { measured_unit: "tokens", accounting_type: "usage", label: "Input tokens" }
      output_tokens:  { measured_unit: "tokens", accounting_type: "usage", label: "Output tokens" }
```

Reading `component_metrics`: the **left** side is the component name, which is the offering's own
and means nothing to the plugin. The **right** side is the metric, and only three values are
accepted — `input_tokens`, `output_tokens`, and `token_cost`, which is LiteLLM's per-model USD
spend (its `spend` field). Anything else raises at startup.

The mapping *adds* to the name-based default rather than replacing it: a component called
`input_tokens`, `output_tokens` or `token_cost` is metered whether or not it appears here, so the
mapping carries only what the names do not already say — one line, for `inference_cost`.

`inference_cost` is a convention, not a keyword: nothing in the plugin knows the name, so it can be
`cost`, `spend`, or anything else, as long as the offering, `backend_components` and
`component_metrics` all agree on it. What makes it the charge is `accounting_type: usage` plus its
plan price — not what it is called.

`token_cost` stays the cap, under its usual name — it reads as a token count rather than the USD
ceiling it is, but the component type is data in Waldur and existing offerings key off that string.

Prices on the Waldur plan:

| Component | Price | Why |
|---|---|---|
| `inference_cost` | **1.0** | Quantity is already USD, so the rate is dollars-per-dollar; else a flat markup |
| `input_tokens`, `output_tokens` | **0** | Volume meters. They appear on the invoice as counts and charge nothing |
| `token_cost` (the cap) | **0** | The customer chooses their own ceiling at order time and pays only for what they use |

> ### The cap component must be priced at 0
>
> A `LIMIT` component is invoiced on **the limit itself** — `billing_limit.py` bills
> `quantity = the limit value` at the plan price, independently of any usage reported against it.
> So a non-zero price here charges the customer for the size of their cap: set a $50 budget and
> the invoice carries $50 for the cap *plus* whatever was actually spent. Price it at 0 and the
> limit becomes a pure ceiling the customer picks for themselves.
>
> This is separate from usage reporting. The `spend` figure this plugin reports against
> `token_cost` is what drives `action_on_usage_limit: pause`, and `billing_usage.py` ignores it
> for invoicing — only `USAGE` components bill from usage. Both statements are true at once, which
> is why the cap and the charge have to be two components.

Otherwise `token_cost` keeps its usual job: it sets the key's `max_budget` backstop, and with
`action_on_usage_limit: pause` it is what stops the resource. Set `budget_duration` as always.

What this gives up is per-model **invoice lines**: the customer sees one `Inference cost` figure,
not a breakdown. Only per-model components can produce that, and they require the prices to be
declared in Waldur rather than in the proxy.

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
