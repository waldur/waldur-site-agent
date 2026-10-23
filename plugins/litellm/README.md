# LiteLLM Plugin for Waldur Site Agent

This plugin integrates a [LiteLLM proxy](https://docs.litellm.ai/docs/proxy/virtual_keys) with
Waldur so that LLM inference access can be sold, provisioned, metered, and billed through the
Waldur marketplace. It provisions per-customer virtual keys from marketplace orders and reports
token usage and spend back to Waldur for metering and billing.

It is the same shape as the sibling `envoy-ai-gateway` plugin, but talks to LiteLLM's management
REST API instead of Kubernetes Secrets — so there is no cluster access and no separate usage
warehouse to deploy.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `litellm` | `waldur_site_agent.backends` | order processing, membership sync |
| `litellm-usage` | `waldur_site_agent.backends` | reporting |

**Modes:** `order_process`, `membership_sync`, `report`, `event_process` (for API key commands).

| Operation | Behaviour |
|---|---|
| Create resource | Registers the resource and its `{api_url}/v1` endpoint; the agent then issues its keys |
| Terminate resource | Deletes every key of the resource (`soft_delete` does not apply) |
| Update limits | `POST /key/update` with the backstop fields on every key |
| Add / remove members | LiteLLM users (and Open WebUI accounts when configured) |
| Pause / downscale / restore | Blocks / unblocks every key; a key paused on its own stays blocked |
| Per-key commands | Create, pause, resume, update and delete one key; see [Per-key API keys](#per-key-api-keys) |
| Usage reporting | `litellm`: **No-op**. `litellm-usage`: tokens and spend, per resource, key and day |

## Features

- **Virtual key lifecycle**: provision, rotate, pause, restore, and terminate LiteLLM keys
  directly from Waldur marketplace orders
- **Native block/unblock pause**: suspend access with `POST /key/block`; a blocked key returns
  401 at the proxy and unblocking restores it without new key material
- **Usage reporting**: report per-resource input/output tokens and USD spend from LiteLLM's own
  daily spend table — no sidecar collector — per day to a Waldur with
  [spending limits](#spending-limits-per-period)
- **Per-model pricing**: bill LiteLLM's own per-model spend, under a spending cap the customer
  chooses — see [Pricing](#pricing)
- **Enforcement backstop**: mirror Waldur's limits onto each key (`max_budget`, `tpm_limit`,
  `rpm_limit`, and day and week [spending limits](#spending-limits-per-period) as budget
  windows) so a burst between reporting cycles cannot outrun metering
- **Model allowlist**: restrict every key of an offering to a set of models, and one key to a
  subset of them
- **Per-key API keys**: with `enable_api_key_provisioning` on the Waldur offering, each key is
  created, limited, paused, resumed and deleted on its own, and its usage is reported on its own
- **Chat surface (optional)**: provision people into [Open WebUI](https://openwebui.com)
  alongside their LiteLLM user, bill their chat usage back to the resource, and revoke
  both together — see [The chat surface](#the-chat-surface-open-webui). With per-key API keys,
  a person's chat follows the key assigned to them — see
  [Chat that follows the key](#chat-that-follows-the-key)

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

Each key is a numbered slot beneath the resource's `backend_id`. Keys are minted
**non-expiring** (no `duration`): Waldur owns the resource lifecycle, and an expiry it does not
know about would revoke access behind its back.

#### Where the `backend_id` comes from

`resource_backend_id_source` picks what a new resource's `backend_id` is derived from:

- **`slug`** (default): `allocation_prefix` + the resource slug, as the order processor derives
  it for every plugin. The slug comes from a name the ordering user picks.
- **`uuid`**: the resource UUID hex, e.g. `3f2c…9a1e-1`, `3f2c…9a1e-2`.

**Use `uuid` on any proxy that also holds keys Waldur did not create.** Key aliases live in the
proxy's single global namespace, and the plugin claims every key matching
`^<backend_id>-\d+$` as a slot of the resource. Under `slug`, a resource whose slug happens to
be `rkd43-opencode` adopts a hand-made key `rkd43-opencode-2`: the key is metered against the
Waldur project, has its budget rewritten, is blocked when the resource pauses and is deleted
with it. Any user with order rights can pick the name, so this can be done on purpose. The slug's
own dedupe suffix (`test-alloc-2`) is also indistinguishable from a slot number. Under `uuid`,
no alias outside the agent's own has that shape.

**Migrating.** Switching to `uuid` affects only resources provisioned afterwards. Existing
resources keep the slug-based `backend_id` they were created with, including when they are
restored, so their keys stay reachable and nothing needs rewriting on the proxy. To move an
existing resource to a UUID alias, terminate it and order it again. To find the resources that
are still slug-based, list the aliases that aren't `<32 hex>-<n>`.

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

`waldur_resource.limits` only ever carries `LIMIT` components the offering declares. An offering
priced as in [Pricing](#pricing) declares none, and its `token_cost` drives `max_budget` and the
budget windows through its [spending limits](#spending-limits-per-period) instead; one on the
[two-component shape](#before-spending-limits-two-components) drives `max_budget` from its
`token_cost` limit. Either way `tpm_limit` and `rpm_limit` stay at whatever the `tpm_limit` /
`rpm_limit` backend settings say, the same value for every resource on the offering.

To make rate limits per-resource instead, declare `tpm` and/or `rpm` as `LIMIT` components — see
the commented lines in the example below. They are rate caps, not meters: nothing reports usage
against them, so they exist purely to carry a number onto the key.

> ### The budget resets on the 1st, with Waldur's month
>
> Every `max_budget` the plugin writes carries `budget_duration: "1mo"`: LiteLLM resets the key's
> `spend` at 00:00 on the 1st of each month, in the proxy's timezone. Waldur counts limits per
> calendar month in UTC, so the two windows are the same month, and the proxy can only refuse a
> key whose spend Waldur also counts as at its limit (the proxy sees less: not chat, and each key
> alone). Between two usage reports the proxy stops a burst first; the next report pauses the key
> in Waldur too.
>
> Any other window disagrees with Waldur. With a window running from the 15th, a person who spends
> their $20 on Sep 25 is resumed by Waldur on Oct 1 but refused by the proxy until Oct 15, while
> the portal shows the key active with nothing used. No window at all is worse: `max_budget` is then
> a lifetime budget and blocks the key for good. So the `budget_duration` backend setting is
> ignored, with a warning at startup when it says anything but `1mo`, and the limits
> reconciliation moves keys holding another duration onto `1mo`.
>
> **Keep the proxy on UTC midnight.** `litellm_settings.timezone` and
> `litellm_settings.budget_reset_time` move LiteLLM's reset; leave them unset (UTC, 00:00) or the
> reset lands hours away from Waldur's month boundary. The agent cannot read them.

#### Spending limits per period

A Waldur with spending limits (waldur/waldur-mastermind#789, #817) lets a customer cap a
usage-based component per calendar period in UTC, on a resource and on each of its keys: day,
week (from Monday), month, quarter and year. The limits on the proxy's spend — `token_cost`,
or a component [`component_metrics`](#pricing) feeds from it — reach every key of the resource:

| Period | On the key | Resets |
|---|---|---|
| day | a budget window, `{max_budget, budget_duration: "1d"}` in `budget_limits` | 00:00 UTC |
| week | a budget window, `budget_duration: "7d"` | Monday, 00:00 UTC |
| month | `max_budget` with `1mo`, the lower of it and a limit-based `token_cost` | the 1st, 00:00 UTC |
| quarter, year | nothing: **Waldur's pause alone** | — |

The proxy checks every window on each request, beside `max_budget`, and refuses the key at the
first one reached. Quarter and year have no duration that resets when Waldur's period does:
`3mo` is refused, `90d` and `365d` count from the day they were written, and `1y` silently
resets every night. Writing one would let the proxy refuse a key that Waldur shows under its
limit, so they are left out. A spending limit on any other component, `input_tokens` say, has no
proxy field either; Waldur enforces it from the usage reports.

**Turn on `api_key_daily_usage` to offer day and week limits on keys.** Waldur offers a key
its day and week limits only where the offering's `plugin_options.api_key_daily_usage` is on,
because it checks them against the key's usage per day; without it a key takes month limits
alone (and none of its own where `api_key_limits` is off; it is on unless set). `litellm-usage`
reports each key's usage per day (see [Per-key usage](#per-key-usage)), so turn the option on
for a LiteLLM offering. While it is off the agent logs a warning for the offering on every
report cycle; nothing about what it reports changes. A resource's own day and week limits do
not depend on it.

Each window is the tighter of the key's limit and the resource's for that period, as
`max_budget` is (see [Limits](#limits)). Windows need a proxy with `budget_limits` (checked on
LiteLLM 1.98 and 1.106); an offering that offers no day or week limit never sends the field.

**After midnight the proxy lags.** It resets an expired window from its budget reset job, which
runs about every ten minutes (`proxy_budget_rescheduler_min_time` and `_max_time` under
`general_settings`, 597 and 605 seconds by default). Until it has run, a key refused by
yesterday's window stays refused, though Waldur resumes it within five minutes. Lower both
settings (say, to 60 and 120) to shorten that. LiteLLM 1.98 adds up to a minute more while it
serves the key from its cache (a key refused by its window served again 71 seconds after the
reset with the job at 10 seconds; on 1.106, 5 seconds). The plugin writes `budget_limits` only when a
window changes, and leaves it alone while a window is past its reset time: every write moves
each window's reset to the next one from now and keeps the spend it counted, so a write at
00:03 would keep yesterday's spend against today's budget until the following midnight, or a
whole week for a weekly window. A key's update or resume in that gap is applied without its
new windows, which the next limits pass writes: the membership sync when polling, the hourly
resource status pass under STOMP.

A refusal is `budget_exceeded`, `ExceededBudget: Key over 1d budget`: status 429 on LiteLLM
1.98, 422 from 1.106.

The membership sync reads a resource's spending limits on every pass when polling. An order
carries the limit-based components only, so the spending limits on the keys stay as last read
until then. Under STOMP membership sync they are read with the next message for the resource.
A key's create and rotation read them from Waldur too, so a new key starts with them (see
[Per-key API keys](#per-key-api-keys)).

## Per-key API keys

Turn on `plugin_options.enable_api_key_provisioning` on the Waldur offering and Waldur governs
a resource's keys one by one: a manager requests a key (for a person, if they like), gives it
its own limits and model list, pauses, resumes and deletes it, and Waldur reads each key's
usage. The plugin carries out each command on the proxy, then acknowledges it to Waldur. An
offering without the option behaves as before: two keys per resource, reveal and rotate.

| Command | What happens on the proxy |
|---|---|
| create | `POST /key/generate` as the next free `<backend_id>-<n>`; blocked on a paused resource |
| pause | `POST /key/update` with `blocked: true` and the key's own-pause record, in one write |
| resume / update | `POST /key/update` with the key's backstop, `models`, `blocked` and records, in one write |
| delete | `POST /key/delete`; a key already gone counts as deleted |
| rotate | `/key/regenerate` (or delete + mint), applying Waldur's limits and model list for the key when given |

A new key skips every slot Waldur still holds, deleted keys included: Waldur attributes usage
by alias and refuses a reused one. Every command can run twice: the agent's sweep replays one
whose acknowledgement was lost.

Rotation lifts a key's own pause instead of keeping it: Waldur rotates only a key it shows as
serving and settles it OK, so a key still paused on its own at that point is the residue of a
pause whose acknowledgement was lost.

A rotation applies the key's limits and model list as Waldur holds them -- carried on the
command, or read from Waldur -- whichever way the key is rotated, exactly as an update would,
own-limit stamp and tighter-of backstop included. Only when Waldur cannot be read does the key
keep the settings it has on the proxy.

**A key changes hands by rotation.** Waldur rotates a key whose assignee is set, changed or
cleared (waldur/waldur-mastermind#772), carrying any limits and models the same edit set. The
previous holder -- or any project member, while the key was unassigned -- may have revealed the
old secret; since API usage on the key counts against its limit and its chat follows the new
holder, the old secret has to stop working. After the rotation the agent hands the resource's
assignees to the proxy at once, as after a create, so chat moves to the new holder without
waiting for the next membership pass. A Waldur older than that keeps the secret on
reassignment: there, rotate a key after reassigning it.

On an open-source proxy a rotation is a delete and a mint under the same alias, so the new key
starts with no proxy spend: its `max_budget` backstop counts from zero again. Waldur's own
metering is not affected, since usage is attributed by alias. A key the proxy lost is minted
back with its own model list and limits from Waldur; if those cannot be read, it is minted
paused with no models until an update or a resume from Waldur applies them, rather than with
the offering's.

Under key management the agent mints no keys when a resource is created: every key comes from
a request in Waldur (waldur/waldur-site-agent#71, waldur/waldur-site-agent!619). Every key
created or rotated takes its resource's limits from Waldur, read with the command: the
limit-based components and the day, week and month spending limits (an empty set clears the
windows). That matters most for a resource's first key, which has no sibling to read them from,
and for a key the proxy lost when a rotation mints it back, which may have been the resource's
only one. Where Waldur sends no spending limits (a Waldur or `waldur-api-client` that predates
them), a key takes them as the last membership sync stamped them on its siblings; a first key
then has none until the next sync.

### What is recorded on the key

The proxy has one `blocked` flag per key and one set of limit fields, so the reasons behind
them are kept in the key's `metadata`:

| Metadata | Meaning |
|---|---|
| `waldur_key_paused` | Paused on its own, not with its resource: a resource restore leaves it blocked |
| `waldur_key_limits` | The key's own limits, by Waldur component name; spending limits per period as `spend/<period>` |
| `waldur_resource_limits` | The resource's limits as last reconciled, for a key created before the next |
| `waldur_key_models` | The key's model list as Waldur sent it: `["*"]`, names, or `[]` |
| `waldur_holder_chat_spend` | The holder's chat this month, taken off the key's `max_budget` (see [Limits](#limits)) |
| `waldur_holder_chat_spend_windows` | The same for the day and week windows, `{period: spend}` |

A key's `models` on the proxy is Waldur's list resolved against the offering's `models`
setting when the key was written, and Waldur sends nothing when that setting changes. So the
membership sync resolves every key's list again from `waldur_key_models`: a key with every
model gets the offering's current list, and a named list keeps only the models the offering
still serves (`no-default-models` when none is left). A model dropped from the offering
therefore stops answering on the API at the next pass, as it closes in chat. A key written
before the record existed only loses the models the offering no longer serves; nothing is
added to it.

A key paused on its own is no evidence that its resource is paused, so a resource's pause is
read from its other keys: every key not paused on its own is blocked. That
cannot record the pause of a resource with **no** keys, so the backend also remembers the
pauses it applied. Before it creates, rotates, resumes or updates a key, the agent applies Waldur's
own flag first, so a key created on a paused keyless resource lands blocked, and a key
resumed on a paused resource whose keys are all paused on their own stays blocked.

### Limits

A key's own limit on a component the proxy has a field for (`token_cost`, `tpm`, `rpm`, see
[Which limits reach the key](#which-limits-reach-the-key)) is written on that key only. Where
the resource limits the same component, the tighter of the two applies, field by field; a 0
in the key's limits means it has none of its own. The membership-sync reconciliation keeps it
that way, and removing the key's limit returns the key to the resource's.

From a Waldur with per-period key limits, a key's limit on the proxy's spend comes per period
(`spending_limits` on the command) and lands as for a resource: the day and the week as
windows, the month on `max_budget` (see
[Spending limits per period](#spending-limits-per-period)). Each is the tighter of the key's
and the resource's for the period.

Where chat follows the key, the key's `max_budget` is that limit **minus its holder's chat this
month**, and each window its own limit minus their chat that day or that week. Waldur counts a
key's usage as its API spend plus its holder's chat, but the proxy checks the key's budgets
against its own spend only, since chat arrives on Open WebUI's shared key. With the chat taken
off, the proxy refuses the key once the two together reach the limit; chat alone at or past it
leaves a budget of 0. The chat is the same figure the per-key report puts on the key: the
month's chat of whoever the key is stamped on, on the days their address belonged to this
resource, without the requests they made with a minted key. Chat from before someone moved
here is billed to the resource they left, so it stays off this key's budgets too. The
membership sync recomputes it every pass, and so do a key's update, resume and rotation, and
the mint of a key the proxy lost; it is read from `/user/daily/activity`, from the 1st or from
Monday, whichever is earlier, only for a resource whose keys have holders. When it cannot be
read, the amounts last taken off (`waldur_holder_chat_spend` and
`waldur_holder_chat_spend_windows`) stay off, so a failed read never loosens the cap.

The chat taken off `max_budget` is the calendar month's, the month the budget resets on (see
[The budget resets on the 1st](#which-limits-reach-the-key)); a window's is its UTC day or week,
from Monday. The chat read is the UTC month's,
so it matches the proxy's default reset. A proxy set to another `timezone` or `budget_reset_time`
resets up to a day off it: between the two resets, a key's budget is netted against the wrong
month's chat, too tight east of UTC and too loose west of it.

It lags both ways by up to one pass: chat since the last pass is not taken off yet, and on the
1st the proxy resets the key's spend while last month's chat is still taken off (at midnight,
a window's, while yesterday's chat is). When polling,
the pass is the membership sync (5 minutes by default). Under STOMP membership sync there is no
periodic limits pass, so the resource status reconciliation
(`WALDUR_SITE_AGENT_RESOURCE_STATUS_RECONCILIATION_MINUTES`, 60 by default) recomputes it on
every resource that is neither paused nor downscaled; with that set to 0, it waits for the next
message for the resource. A paused resource's keys are blocked anyway, and its resume recomputes
them. These writes leave the key's `budget_duration` out unless it changes: naming it moves the
proxy's next reset, which on the 1st would skip the month's.

It covers the key's own budget only. Each key still carries the whole resource budget, without
its siblings' spend or other members' chat; the offering's *Action on usage limit* is what
holds the resource total.

Limits on `input_tokens` and `output_tokens` have no proxy field, nor have quarter and year
limits. Waldur enforces them, and every key limit, from the per-key usage report: a key whose
usage reaches its limit is paused, and resumed when it is under the limit again, at the
latest once the limit's period resets.

### Models

A key's model list narrows within the offering's `models`:

| Waldur sends | The key gets |
|---|---|
| `["*"]`, or no list (a Waldur older than `["*"]`) | the offering's `models` (every model when that is empty) |
| a list of names | those names; a name the offering does not serve fails the command, naming it |
| `[]` (no models) | `["no-default-models"]`: every call to a real model answers 403 |

The proxy reads an empty `models` on a key as every model, so `[]` cannot be written as it is.
Waldur sends `[]` when every model a key allows is switched off on its resource or retired
from the offering; the key is written with the proxy's reserved `no-default-models` name as
its only model instead, which no deployment carries. Its holder's chat stays open with no
model in it.

### Per-key usage

`litellm-usage` reports each key's figure for the month, and to a Waldur with per-day key usage
each day of it instead, every day of the month so far, zeros included. API usage lands on the key it was
served on, whatever address the request carried. On an offering with a chat surface, a
person's chat usage lands on the key assigned to them (see below). Chat usage from someone with
no key assigned now -- their key was deleted or reassigned this month, say -- lands on
`<backend_id>-unassigned`, which is no key: Waldur holds none by that name, so it counts
against no key's limit and the agent leaves it out of the per-key report.
Either way the keys of a resource sum to its total.

A person's chat for the whole month counts against the key stamped on them **now**, so
reassigning or deleting a key mid-month moves that month's chat with it: the new holder's
chat lands on the key, the previous holder's leaves it. A key paused at its limit by chat
can come back under it that way, and Waldur then resumes it. Reported per day, the days that
chat left are set back to zero, so the key's day and week usage drop at once, but Waldur never
lowers a key's month figure within the month: a key paused at its month limit stays paused
until the month turns or its limit is raised.

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
| over-budget user (spend 0.00045, `max_budget` 1e-7) via **their own key** | **429** (422 from LiteLLM 1.106) |
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
address can only be billed to **one** resource at a time. The plugin records the owning
resource in the user's `metadata` and **refuses** to move an address another resource
currently owns, rather than taking it over: a silent takeover would move the first
resource's chat billing onto the second one while the person kept chatting throughout.

"Currently owns" means an open ownership period (see
[When chat usage is billed to a resource](#when-chat-usage-is-billed-to-a-resource)).
Once the person is removed from the first resource its period closes, and another
resource can claim the address — the move is then a new period, not a takeover.

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
  connection have none. **Except with per-key API keys**, which need it **off**: there the
  plugin gives each model a record and a group, see
  [Chat that follows the key](#chat-that-follows-the-key).
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

Groups and model access, verified against 0.11.3 on 2026-10-15 with
`BYPASS_MODEL_ACCESS_CONTROL=False`:

- `POST /api/v1/models/model/access/update` with
  `{"id", "name", "access_grants": [{"principal_type": "group", "principal_id": <id>, "permission": "read"}]}`
  **replaces** the model's grants, and creates the model's record when it has none.
- A person then lists and can chat with only the models of the groups they are in; any other
  is refused at chat time too. A change of membership applies on the next request.
- `POST /api/v1/groups/id/{id}/users/add` with `{"user_ids": [...]}` is safe **one user per
  call only**: a batch that includes an existing member silently drops the new members
  added before it, and still answers 200. Removing a non-member changes nothing.
- `GET /api/v1/users/{id}/groups` lists one account's groups.

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
2. every `entities` row whose id is a managed address → the resource that owned that
   address **on the row's day**, **skipping** the keys already counted in step 1.

Requests on a key with no user attached — every key this plugin mints — land under
LiteLLM's `"Unassigned"` bucket, which no managed address ever matches, so the common
case separates on its own. Step 2's exclusion covers the other one: a person calling the
API with their own key appears in both breakdowns and must be billed once.

A proxy that gives an entity no `api_key_breakdown` leaves step 2 unable to tell which key
each record was served on, and the address on it is whatever the request's header said: a
key holder can name another member. Such a total is counted only on a day when none of the
plugin's keys was used (only the shared key, or keys with no slot alias); on any other day
it is skipped with a warning, losing that day's chat usage rather than billing a person for
someone else's API calls. Proxies validated with this plugin always give the split.

The address-to-resource history is read from the proxy (`GET /user/list`, from the
metadata on each user), not from Waldur — the reporting backend never sees a team list,
and the record is in any case what decides where an address bills. An API-only offering
never makes that call.

### When chat usage is billed to a resource

A person's chat usage is billed to the resource that owned their address **on the day
the usage was recorded** — the proxy's UTC day, which is how it buckets usage rows. Every
reporting pass recomputes the whole month, so this is what keeps a figure stable once
reported:

- **Removed mid-month:** chat access ends as before, and the usage they had this month
  stays on the resource in every later report of the month.
- **Moved from A to B:** usage before the move stays on A, usage after it goes to B. The
  day of the move itself is billed to A, the resource they left — usage is per day, so
  that day cannot be split.
- **Historical reload** of an earlier month attributes each day of it the same way.

Ownership lives in the LiteLLM user's `metadata` as a list of periods, oldest first, both
ends inclusive and `to: null` for the current owner:

```json
{
  "waldur_username": "ada",
  "waldur_resource": "4f1c…",
  "waldur_ownership": [
    {"resource": "9b2e…", "from": null,         "to": "2026-10-14"},
    {"resource": "4f1c…", "from": "2026-10-14", "to": null}
  ]
}
```

- Adding a member opens a period from today; removing one closes it today. **The record
  is kept** on removal — deleting it is not what ends chat access (demoting the Open WebUI
  account is), and deleting it took the month's usage off the bill.
- `waldur_resource` mirrors the current owner and is `null` once released, so an agent
  from before this change, still running during an upgrade, stops billing a released
  address too.
- A record written before this change has only `waldur_resource`. It is read as one open
  period with no start, so a current member's usage is attributed exactly as before.
- A record with no open period is **deleted** once its last period ended before the
  retention window — `ownership_retention_months` calendar months, counting the current
  one (default 12). Keep that at least as large as the agent's `reporting_periods`, or a
  month the agent still reports loses a removed member's chat usage. Periods older than
  the window are also trimmed from records that stay. Pruning walks every user on the
  proxy, so give every agent sharing one proxy the same setting.
- A historical reload of a month older than the window bills nothing for people removed
  before it — their periods are gone — and, since a reload overwrites the month's figure,
  lowers it. Raise `ownership_retention_months` before reloading that far back.
- Upgrade every agent sharing the proxy together. An older agent reads the
  `waldur_resource` mirror correctly, but one that writes a record replaces its periods,
  and the record then reads as owned with no start.
- Records an admin made by hand carry no periods and are never pruned.

### Chat that follows the key

On an offering with both a chat surface and
[per-key API keys](#per-key-api-keys), chat is granted by the key assigned to a person,
not by project membership:

- **Chat needs an assigned key.** A member's chat account is enabled while a key assigned to
  them serves, and disabled when it is paused (at its limit or by hand), deleted or assigned
  to someone else, when they leave the project, and while the resource is paused. A member
  with no key has no chat. That includes people who chatted by membership before key
  management was turned on: their chat closes until a key is assigned to them.
- **Chat offers only the key's models.** Open WebUI lists and serves a person only the models
  whose group they are in. The plugin keeps one group per model of the offering
  (`waldur-model-<model>`), grants each model to its group, and puts the person in the
  groups of their key's models: all of them for a key with every model. A model dropped
  from the offering's `models` closes too: every `waldur-model-*` group counts as the
  plugin's, so people leave the groups of retired models on the next membership sync. An
  Open WebUI with such groups therefore serves one key-managed offering only.
- **Chat counts against the key.** The person's chat usage is reported on their key, so
  Waldur pauses the key, and with it the chat, at the key's limit. Their chat this month is
  also taken off the key's `max_budget` (see [Limits](#limits)), so the proxy stops the key's
  API calls once API spend and chat together reach the limit.
- **One key carries a person's chat.** Waldur lets a person hold several keys on a resource;
  their chat follows the lowest-numbered one, whatever its state, and is closed while that key
  is paused. Moving chat to another key while one is paused at its limit would let chat
  alternate between the two and never stop.

Who holds which key comes from Waldur: the membership sync reads the resource's keys and
their assignees on every pass, and the key's alias is stamped on the person's LiteLLM user
(`waldur_api_key` in its `metadata`). So a reassignment, a missed command, or an account that
only appeared at the person's first sign-in all converge on the next membership sync. A newly
created key's assignee is applied straight away.

How the two request paths are governed:

| | API calls, with the person's key | Chat, through Open WebUI |
|---|---|---|
| Authenticates as | the person's key | Open WebUI's shared key |
| Usage reported on | the key it was served on | the key assigned to the person |
| Model list | the proxy, on the key | Open WebUI, by group membership |
| Day, week and month limits | Waldur (report → pause), plus the key's budgets net of chat | Waldur (report → pause) |
| Quarter and year limits | Waldur (report → pause) | Waldur (report → pause) |
| Per-minute limits | the proxy, on the key | **not enforced** |
| Pause | the key is blocked | the chat account is disabled |

Two limits of this design are accepted:

- **Chat can overshoot a limit** until the next usage report (30 minutes by default): only
  Waldur enforces it there.
- **Per-minute limits do not apply in chat.** Chat authenticates as the shared key, which
  the proxy checks instead.

What the Open WebUI deployment needs on top of the list above:

- **`BYPASS_MODEL_ACCESS_CONTROL=False`.** With it on, everyone sees every model.
- **The offering's `models` setting.** It is the list the plugin makes groups for; without
  it, chat model access is left to the operator.
- **`DEFAULT_GROUP_ID` empty**, or naming a group that grants no models. It is applied to
  every account at creation, the agent's own included.
- **Group sync from the identity provider off**: `ENABLE_OAUTH_GROUP_MANAGEMENT`,
  `WEBUI_AUTH_TRUSTED_GROUPS_HEADER` and SCIM each rewrite a person's groups at sign-in and
  would undo the plugin's. If OAuth group management has to stay on, set
  `OAUTH_BLOCKED_GROUPS` to cover `waldur-model-*`.
- **The task model and the context-compaction model left unset** ("current model"). Open
  WebUI checks them as the user, so naming one would need every user to have access to it.
- **`DEFAULT_USER_ROLE=pending`**, as for SSO above, so an account a person creates at sign-in
  stays closed until the plugin opens it.

The plugin re-grants each model to its group on every pass, so a model an admin set to
Public in the UI is closed again. Admins bypass model access control altogether.

`BYPASS_MODEL_ACCESS_CONTROL` is instance-wide: an Open WebUI shared with an offering that
grants chat by membership hides every model from that offering's people too. Give such an
offering its own Open WebUI.

Turning key management off again leaves each person's `waldur_api_key` stamp on their LiteLLM
user, and restore keeps reading it. Remove the stamps (or the users, which the next
membership sync re-adds) when switching an offering back to chat by membership.

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
| `delete_resource()` | releases every member as below, then `POST /key/delete` for every key |
| `set_resource_limits()` | `POST /key/update` — full target state of the four backstop fields |
| `sync_resource_limits()` | pushes Waldur's limits onto the keys (the base pulls them the other way) |
| `add_users_to_resource()` | `POST /user/new` (or `/user/update`), opening a period; plus Open WebUI `auths/add` |
| `remove_users_from_resource()` | Open WebUI role → `pending` (or `DELETE`), then `/user/update` closing the period |
| `list_resource_users()` | `GET /user/list`, filtered on the open period; prunes [expired][ownership] records |
| `get_resource_metadata()` | `active` = any key not blocked |
| `mint_resource_key()` | `POST /key/generate` as the next free slot, with the key's own limits, models and records |
| `pause_resource_key()` | `POST /key/update`: `blocked: true` and `waldur_key_paused`, plus the holder's chat closed |
| `resume_` / `update_resource_key()` | `POST /key/update`: backstop, `models`, `blocked`, records; chat follows |
| `delete_resource_key()` | `POST /key/delete`, plus the holder's chat closed and unlinked |
| `sync_resource_key_assignees()` | stamps each member's key alias, opens or closes their chat, sets their model groups |
| `recreate_missing_resource()` | returns `False` + warning — the agent cannot restore a key it never kept |

> **`soft_delete` does not apply.** `delete_resource()` is overridden, so the core's soft-delete
> path (zeroing limits instead of removing the account) is never reached and the setting is
> ignored: a terminate always deletes the keys. There is no soft form of it — a key left in place
> with a zero budget is still a live credential, and the agent cannot hand back the plaintext of
> one it kept.

### Usage Reporting Backend (`litellm-usage`)

Read-only. Reports whichever of `input_tokens`, `output_tokens` and `token_cost` the offering
declares, plus every component `component_metrics` maps onto one of them.

`component_metrics` decouples the component name from the metric it reads, which is what lets one
spend figure be both the cap and the charge — see [Pricing](#pricing).

**Source:** `GET /user/daily/activity`, walking `results[].breakdown.api_keys` — see
[Why not `/global/spend/report`](#why-not-globalspendreport) below.

**One fetch per pass:** `/user/daily/activity` cannot be filtered by key, so a call returns the
whole proxy's month and this backend picks out the rows it wants. The processor pulls one resource
at a time (and once more per historical period), so the fetched rows are reused across the
resources of a pass — otherwise each resource would walk the entire table again. `usage_cache_ttl`
bounds that reuse so it cannot outlive the pass. It defaults to **half**
`WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES` (so 15 minutes on the default 30-minute period): a whole
period would keep an entry alive into the next pass, which is exactly when fresh numbers are due.

**Per key:** `get_resource_key_usage_report()` reports the same rows one level finer, for an
offering with [per-key API keys](#per-key-api-keys).

**Per day:** every row is one UTC day's, so the same rows also give the usage per day
(`get_daily_usage_report_for_period()`, `get_resource_key_daily_usage_report()`). A Waldur with
[spending limits](#spending-limits-per-period) takes it (`daily_usages`), and needs it to check day
and week limits; the agent then sends every day of the month so far, a day with no usage as zero,
and Waldur derives the month from the days. Only usage-based components go per day; a
limit-based one keeps its month total. A Waldur without per-day usage gets month totals, as
before. The first report after the upgrade sends the whole month so far, which is what Waldur
needs from an integration that switches to days mid-month.

**Rollup:** the resource total is attributed per **resource**, not per key. On an offering with a
chat surface it is also attributed per **person** — see
[How chat usage is billed](#how-chat-usage-is-billed). The `-<n>` slot suffix is stripped
from the alias and the rows are summed, so rotating a key does not split a tenant's bill and a
resource used across several days and several keys is billed for all of it.

## Pricing

Sell inference the way the proxy prices it: per model. This is the shape the
[configuration example](#composed-offering-both-backends) uses, and the one to start from even
when the offering serves a single model today — adding a second one later then changes nothing on
the Waldur plan.

### Why tokens alone cannot price it

A provider selling `gpt-4o` alongside a small local model cannot price them with `input_tokens`
and `output_tokens`: those carry one rate for every model on the offering, so a million frontier
tokens and a million cheap ones invoice identically. Waldur has no per-model dimension to fix this
with — `ComponentUsage` is `(resource, component, period, quantity)` and nothing more.

LiteLLM has already solved it. The proxy applies its own per-model cost map and reports the result
as `spend`, which this plugin surfaces as the `token_cost` metric. Billing that figure at a plan
price of **1.0** charges exactly what the proxy costed, model by model, with no Waldur-side rate to
keep in sync and no markup.

### One spend figure, capped by spending limits

With [spending limits](#spending-limits-per-period), a usage-based component carries its own
cap, so one component is both the charge and the cap:

```yaml
    backend_components:
      input_tokens:  { measured_unit: "tokens", accounting_type: "usage", label: "Input tokens" }
      output_tokens: { measured_unit: "tokens", accounting_type: "usage", label: "Output tokens" }
      token_cost:    { measured_unit: "USD",    accounting_type: "usage", label: "Inference cost" }
```

On the Waldur offering, let customers set spending limits on `token_cost` ("Customers can set
spending limits" in the component dialog, with the periods to offer and a default and a maximum
for each), and set `plugin_options.action_on_usage_limit: pause`. No `component_metrics` is
needed: all three components are named after their metric.

| Component | Price | Role |
|---|---|---|
| `token_cost` | **1.0** | the **charge**, LiteLLM's per-model spend; its spending limits are the keys' budgets |
| `input_tokens`, `output_tokens` | **0** | volume meters. They appear on the invoice as counts and charge nothing |

`token_cost`'s quantity is already USD, so its rate is dollars-per-dollar: 1.15 is a 15% markup.
A spending limit is never billed; the customer pays for what they use.

Offer day, week and month to have the proxy refuse a key at its limit between two usage reports;
quarter and year are Waldur's pause alone (see
[Spending limits per period](#spending-limits-per-period)). Day and week need usage per day,
which this plugin reports.

`token_cost` keeps its name although it reads as a token count rather than USD: the component
type is data in Waldur, and existing offerings key off that string.

The per-model prices live in the proxy's cost map, not in Waldur: changing a model's
`input_cost_per_token` in the LiteLLM config changes the next bill with no plan revision. And the
customer sees one `Inference cost` line, not a breakdown by model. Only per-model components can
produce that, and they require the prices to be declared in Waldur rather than in the proxy.

### Before spending limits: two components

A Waldur without spending limits can only cap a `LIMIT` component, and invoices a `LIMIT`
component on the limit the customer set, never on usage. So an offering that wanted a cap and
per-model billing reported the same spend under two names, and offerings set up that way keep
working unchanged:

```yaml
    backend_settings:
      # inference_cost is not named after a metric, so say what it reads.
      component_metrics:
        inference_cost: token_cost

    backend_components:
      input_tokens:   { measured_unit: "tokens", accounting_type: "usage", label: "Input tokens" }
      output_tokens:  { measured_unit: "tokens", accounting_type: "usage", label: "Output tokens" }
      token_cost:     { measured_unit: "USD",    accounting_type: "limit", label: "Budget" }
      inference_cost: { measured_unit: "USD",    accounting_type: "usage", label: "Inference cost" }
```

| Component | Price | Role |
|---|---|---|
| `token_cost` | **0** | the **cap**: the order's budget, each key's `max_budget`, what the pause fires on |
| `inference_cost` | **1.0** | the **charge**: LiteLLM's per-model spend, invoiced |
| `input_tokens`, `output_tokens` | **0** | volume, for the record |

> **The cap component must be priced at 0.** `billing_limit.py` bills a `LIMIT` component at
> `quantity = the limit value`, independently of any usage reported against it, so a non-zero
> price charges the customer for the size of their cap: set a $50 budget and the invoice carries
> $50 for the cap *plus* whatever was actually spent. The `spend` reported against `token_cost`
> drives `action_on_usage_limit: pause`, and `billing_usage.py` ignores it for invoicing.

Reading `component_metrics`: the **left** side is the component name, which is the offering's own
and means nothing to the plugin. The **right** side is the metric, and only three values are
accepted — `input_tokens`, `output_tokens`, and `token_cost`, which is LiteLLM's per-model USD
spend (its `spend` field). Anything else raises at startup. The mapping *adds* to the name-based
default: a component called `input_tokens`, `output_tokens` or `token_cost` is metered whether or
not it appears here. Without the line for `inference_cost`, it reports zero forever.

**Moving an offering off it.** `inference_cost` is fed from the proxy's spend, so its spending
limits reach the keys as `token_cost`'s would, and the offering can move without renaming
anything:

1. Let customers set spending limits on `inference_cost`, with at least a month period.
2. Give each resource a month spending limit on `inference_cost` equal to its `token_cost`
   limit (*Set spending limits*), or ask its customer to. While both are set, the keys carry the
   lower of the two.
3. Clear the `token_cost` limits, then remove `token_cost` from the offering and from
   `backend_components`. Keep `component_metrics`: `inference_cost` still needs it.

New offerings start from the single-component shape above.

### Other shapes

- **No cap.** `token_cost` as above with no spending limit periods offered: billing is per
  model, the keys carry no `max_budget`, and `action_on_usage_limit: pause` has nothing to act on.
- **One rate per token.** Price `input_tokens` and `output_tokens` on the plan. Right only when
  every model on the offering costs the same per token.

## Configuration

Both backends are normally combined on one composed offering and share a single `backend_settings`
block.

### Management backend settings (`litellm`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | LiteLLM proxy base URL |
| `api_token` | yes | — | Master or admin key for the management API |
| `models` | no | all | Model allowlist pushed onto every key |
| `budget_duration` | no | — | Ignored: `max_budget` always resets on the 1st (`1mo`); other values are logged |
| `tpm_limit` | no | — | Default tokens-per-minute cap on each key |
| `rpm_limit` | no | — | Default requests-per-minute cap on each key |
| `verify_ssl` | no | `true` | Verify the proxy's TLS certificate |
| `timeout` | no | `30` | Per-request timeout in seconds |
| `resource_backend_id_source` | no | `slug` | `slug` or `uuid`; see [below](#where-the-backend_id-comes-from) |
| `ownership_retention_months` | no | `12` | Months, incl. the current one, a removed member is kept; see [ownership] |
| `openwebui` | no | — | Chat surface; see [The chat surface](#the-chat-surface-open-webui) |

### Usage reporting backend settings (`litellm-usage`)

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `api_url` | yes | — | LiteLLM proxy base URL |
| `api_token` | yes | — | Master or admin key for the spend API |
| `verify_ssl` | no | `true` | Verify the proxy's TLS certificate |
| `timeout` | no | `30` | Per-request timeout in seconds |
| `component_metrics` | no | — | Extra component → metric pairs; see [Pricing](#pricing) |
| `usage_cache_ttl` | no | half report period | Seconds a fetched month of rows is reused in one pass; `0` disables |

### Composed offering (both backends)

```yaml
offerings:
  - name: "LLM Inference"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "<waldur api token>"
    waldur_offering_uuid: "<offering uuid>"
    backend_type: "litellm"
    order_processing_backend: "litellm"
    membership_sync_backend: "litellm"     # required — pause/restore blocks the keys
    reporting_backend: "litellm-usage"

    backend_settings:
      api_url: "https://litellm.example.com"
      api_token: "sk-master-..."
      models: ["gpt-4o", "llama-3.3-70b"]   # optional allowlist
      tpm_limit: null
      rpm_limit: null
      verify_ssl: true
      resource_backend_id_source: "uuid"    # key aliases <resource-uuid>-<n>; default "slug"

      # Optional chat surface. Omit the whole block for an API-only offering.
      openwebui:
        api_url: "https://chat.example.com"
        api_token: "owui-admin-token"
        url: "https://chat.example.com"
        account_provisioning: "sso"          # or "managed_password" + initial_password
        delete_accounts_on_removal: false

    # Per-model pricing, capped by the customer's spending limits on token_cost; see
    # Pricing for what to price each at on the plan.
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

Enforcement is configured on the Waldur **offering**, not here: customers must be able to set
spending limits on its `token_cost` component, and the offering must set
`plugin_options.action_on_usage_limit: pause`. Price the plan as in
[Pricing](#one-spend-figure-capped-by-spending-limits).

## LiteLLM API behaviour

Verified against `ghcr.io/berriai/litellm:main-stable` (open-source, **no** enterprise licence) on
2026-08-25. These are the answers the implementation is built on; re-check them when targeting a
much older or newer proxy.

The per-key API keys were validated on LiteLLM 1.105.1. LiteLLM 1.107.0 answers 500 instead of
401/403 when it refuses a key for authentication or budget (a blocked key, a key over its
`max_budget`, a model the key does not allow): the refusal still holds, but clients see a server
error.

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
- **`budget_duration: "1mo"` resets on the 1st.** A key given it on Oct 15 reads
  `budget_reset_at: 2026-11-01T00:00:00+00:00`, and so does every later `/key/update` that
  carries it: the reset is recomputed from the duration, not counted from the update. Moving a
  key from another duration keeps its `spend`. A key at its `max_budget` answers 429 (422 from
  1.106). Verified on LiteLLM 1.98, 2026-10-15. So the plugin's `/key/update` names
  `budget_duration` only when the key does not carry it already: an update that names it on the
  1st, before the proxy's reset job has run, moves the reset to the following 1st and keeps last
  month's `spend` (`prepare_key_update_data` in LiteLLM's key management endpoints).
- **`budget_limits` holds a key's budget windows**, each `{"max_budget", "budget_duration"}`;
  the proxy adds `reset_at`. The field names in LiteLLM's docstring, `budget_limit` and
  `time_period`, are refused with 422. `1d` resets at the next 00:00, `7d`, `1w` and `weekly`
  on the next Monday, `1mo` and `30d` on the 1st; any other `Nd` counts from today, `3mo` and
  `12mo` answer 500, and an unknown unit such as `1y` falls back to daily. Each window's spend
  is counted by its duration, so a `/key/update` with new amounts keeps it, and it survives a
  restart without Redis. A `/key/update` that carries `budget_limits` recomputes every window's
  `reset_at` from now; `[]` clears them. Verified on LiteLLM 1.98 and 1.106.1, 2026-10-21.
- **`/key/list` returns `total_pages` and `current_page` at the top level** of the payload
  (alongside `keys` and `total_count`), not nested under a `metadata` object — unlike
  `/user/daily/activity`, which nests them. Paging past the last page returns an empty `keys` list
  rather than an error. The two clients read the field from different places for this reason.
- **`/key/generate` and `/key/update` take `metadata`, and `/key/update` *replaces* it.**
  Posting `{"metadata": {"only": 1}}` to a key carrying three entries leaves exactly
  `{"only": 1}`. So the plugin always writes a key's whole metadata, merged with what it
  read. `/user/update` replaces a user's `metadata` the same way. Verified 2026-10-15.
- **One `/key/update` can set `blocked` and `metadata` together**, and a key blocked that way
  answers 401 at once. `/key/update` with `models` narrows the key: a model outside the list
  answers 403. `models: []` is stored as `[]`, which the proxy reads as every model.
  Verified 2026-10-15.
- **A key with `models: ["no-default-models"]` can call no model.** A real model answers 403,
  the reserved name itself 400 (no deployment), and the key's `/v1/models` lists nothing.
  LiteLLM documents the name for users only; on a key it works because a key calls only the
  names it lists. A made-up name blocks calls too, but `/v1/models` then lists it.
  Verified 2026-10-15.
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

[ownership]: #when-chat-usage-is-billed-to-a-resource
