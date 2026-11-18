# Ceph S3 Storage Plugin for Waldur Site Agent

Integrates Waldur Mastermind with Ceph object storage. Each marketplace resource
creates one RGW user, with its access keys managed as Waldur resource API keys
and its ordered ceiling applied as an RGW quota.

Two **flavours** reach the same RGW concepts by different routes:

| | `croit` (default) | `radosgw` |
|---|---|---|
| Talks to | croit's management API | RadosGW Admin Ops on the gateway |
| Authenticates with | bearer token or basic auth | SigV4, as an RGW user with caps |
| Users, keys, quotas | yes | yes |
| GB-day metering | yes | **no** — see [Usage Reporting](#usage-reporting) |

The flavours differ only inside the client. Everything above it — key lifecycle,
quota mapping, metadata, Waldur plumbing — is shared.

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `ceph_s3` | `waldur_site_agent.backends` | order processing, membership sync |
| `croit_usage` | `waldur_site_agent.backends` | reporting (croit flavour only) |

**Modes:** `order_process`, `membership_sync`, `event_process`, and `report` with
`reporting_backend: croit_usage`.

| Operation | Behaviour |
|---|---|
| Create resource | S3 user under `{allocation_prefix}{resource slug}`, user and bucket quotas |
| Terminate resource | Deletes the S3 user |
| Update limits | Updates the quotas |
| Add / remove members | **No-op** — returns `True`; one S3 user per resource, no per-member access |
| Pause / downscale / restore | **No-op** — returns `True` without changing quotas |
| Usage reporting | `ceph_s3`: **No-op** (reports nothing). `croit_usage`: GB-days from croit statistics |

## Features

- **Automatic S3 User Creation**: One S3 user per marketplace resource with slug-based naming
- **Usage-Based Billing**: storage billed in GB-days, integrated from croit's
  metrics over the billing period. Object counts are not billed
- **Safety Quota Enforcement**: an ordered ceiling is applied as both a RadosGW
  user quota (the tenant total) and a bucket quota (per bucket)
- **Credential Management**: two S3 access keys per resource, encrypted in Waldur and
  managed from the portal (reveal / rotate). The count is fixed at provisioning —
  rotation replaces a key's value in place, so there is no add or revoke
- **Bearer Token Authentication**: Secure API access with configurable SSL verification

## Installation

Add the plugin to your UV workspace:

```bash
cd /path/to/waldur-site-agent
uv add ./plugins/ceph-s3
```

## Configuration

### Basic Configuration

```yaml
offerings:
  - name: "Ceph S3 Object Storage"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your_waldur_api_token"
    waldur_offering_uuid: "713c299671a14f5db9723a793291bc78"

    # Event processing settings
    stomp_enabled: true
    websocket_use_tls: false

    # Backend roles. Metering is a separate backend because only croit can do
    # it; see Usage Reporting.
    backend_type: "ceph_s3"
    order_processing_backend: "ceph_s3"
    membership_sync_backend: "ceph_s3"
    reporting_backend: "croit_usage"

    # Backend settings for the croit flavour
    backend_settings:
      flavour: "croit"                         # or "radosgw"; croit is the default
      api_url: "https://192.168.240.34"        # management API
      s3_endpoint: "https://s3.example.com"    # what tenants connect to
      s3_region: "default"                     # RadosGW zonegroup api_name
      token: "your-bearer-token"
      verify_ssl: false
      tenant_template: "{customer_slug}_{customer_uuid:.8}"   # optional

    # Component mapping. label and measured_unit are required by the config
    # validator for every component — the agent refuses to start without them.
    # This plugin additionally requires unit_factor on the storage component;
    # see below for why a missing one cannot be defaulted.
    backend_components:
      s3_storage:
        accounting_type: "usage"
        backend_name: "storage"
        label: "Storage"
        measured_unit: "GB-day"  # the billed quantity is GB x days, not GB
        unit_factor: 1000000000  # decimal GB (1000 GB = 1 TB), not GiB
```

### The radosgw flavour

Talks Admin Ops directly to the gateway, for a Ceph cluster croit does not manage.

```yaml
    backend_type: "ceph_s3"
    order_processing_backend: "ceph_s3"
    membership_sync_backend: "ceph_s3"
    # No reporting_backend: this flavour cannot meter. See Usage Reporting.

    backend_settings:
      flavour: "radosgw"
      s3_endpoint: "https://s3.example.com"    # Admin Ops hangs off this host
      admin_access_key: "ADMINACCESSKEY000000"
      admin_secret_key: "..."
      s3_region: "us-east-1"                   # must match what clients sign with
      admin_path: "admin"                      # only if rgw_admin_entry was renamed
      verify_ssl: true
```

`api_url`, `token`, `username` and `password` belong to the croit flavour and are
**rejected** here rather than ignored — an offering carrying both sets is a
half-finished migration, and silently picking one would authenticate somewhere the
operator did not intend.

#### Two behaviours that differ from croit

- **No GB-day metering of its own.** Admin Ops exposes no stored-bytes series:
  `/admin/usage` is bandwidth and operation counts, and bucket stats are a
  point-in-time size. On a croit cluster, `croit_usage` can still meter it from
  croit's statistics (see
  [Reporting for a radosgw-managed offering](#reporting-for-a-radosgw-managed-offering-on-a-croit-cluster)).
  On plain Ceph, leave `reporting_backend` unset and bill on a fixed component.
- **Quota enforcement overshoots.** RGW syncs bucket statistics into the
  user-quota check periodically rather than on every write. Measured on squid: a
  6 MiB ceiling against 5 MiB already stored accepted a further 7 MiB before
  answering `QuotaExceeded`. The ceiling bounds the invoice approximately, not
  exactly.

#### Account mode: an RGW account per resource

With `rgw_accounts: true` each resource becomes an
[RGW account](https://docs.ceph.com/en/latest/radosgw/account/) with its own root
user, instead of a plain S3 user. The root user's keys are the resource's API keys,
as in user mode. With them the consumer can use the IAM API, which RGW serves on the
same endpoint as S3, to create their own users, access keys, groups and policies
inside the account. A key that can only read one bucket is then the consumer's own
business, not a support request.

```yaml
    backend_settings:
      flavour: "radosgw"
      rgw_accounts: true
      s3_endpoint: "https://s3.example.com"
      admin_access_key: "ADMINACCESSKEY000000"
      admin_secret_key: "..."
```

Requires **Ceph 20.2.2 or later**.
- Earlier gateways check `GET`/`DELETE /admin/account` against a cap that cannot
  be granted, so both are refused for everyone.
- They also have no account quota endpoint. 19.2.x answers the quota `PUT` with
  `200` and changes nothing.
- The agent reads every quota back, so an older gateway fails the order with an
  error naming the version, rather than leaving an account uncapped.

What changes compared with user mode:

- **Ceilings apply to the account.** Account users' buckets belong to the
  account, and RGW stores a per-user quota on an account user without enforcing
  it. So the ordered ceiling is the account quota, with the same figure as the
  account's per-bucket quota, and it caps the consumer's IAM users too.
- **The backend id is still the root user's uid.** The account is named after it
  and carries the same ownership stamp, so an interrupted create is adopted on
  retry, and an account of that name that is not this resource's is refused. The
  account id is published as `s3_account_id`, which is what the consumer's IAM
  policy ARNs name.
- **Display names are uids.** The root user's display name is its IAM
  `UserName`, held to `[\w+=,.@-]{1,64}`, so the uid is used in place of the
  resource name.
- **Termination refuses while the account owns any bucket.** In user mode RGW
  enforces this itself. For an account root user it does not: the user deletes
  with `200` while the account still owns buckets. The agent therefore checks
  before deleting anyone. It then removes every user in the account, including
  ones the consumer created through IAM, and finally the account. Finding them
  means listing every user on the gateway, because Admin Ops cannot filter by
  account.
- **IAM roles, groups and OIDC providers are removed at termination too.** RGW
  will not delete an account that still holds them, and Admin Ops cannot see
  them. Before deleting any user, the agent gives the root user a temporary key,
  removes them through the account's IAM API, and drops the key again. If that
  fails, nothing is deleted and the order error says why. (Without this step, a
  role was enough to leave an account without users, which only
  `radosgw-admin role delete` on a cluster node could clean up.)
- **What the consumer creates with the root key is theirs.** The root key can
  create IAM users, access keys, roles and groups inside the account. Waldur's
  key rotation replaces only the keys Waldur issued: IAM users and their keys
  keep working after a rotation. Removing someone's access therefore means
  removing their IAM user, or terminating the resource.
- **Pause suspends every user of the account.** Pausing a resource in Waldur
  suspends the root user and every IAM user (RGW suspends per user, so the root
  alone would not stop the others); restoring reinstates them all. A plain user,
  on either flavour, is suspended the same way. Downscaling has no S3 meaning and
  changes nothing.
- **Existing offerings.** Resources provisioned before the switch stay plain
  users. Account membership cannot be added without moving a user's buckets.

Cluster preparation (from a shell with `radosgw-admin`):

1. Check `ceph versions` shows 20.2.2 or later on every RGW.
2. Check the IAM API is enabled: `ceph config get client.rgw rgw_enable_apis`
   must list `iam`, which is the default.
3. Create the agent's user. It needs no `metadata` cap and must not be a
   `--system` user:

   ```bash
   radosgw-admin user create --uid=waldur-agent --display-name="Waldur site agent"
   radosgw-admin caps add --uid=waldur-agent --caps="users=*;buckets=*;accounts=*"
   ```

4. In a multisite setup, point `s3_endpoint` at the metadata master zone.
5. Optional: to let consumers use IAM roles with `AssumeRole`, enable STS with
   `rgw_s3_auth_use_sts = true` and an `rgw_sts_key` (16 hex characters, the same
   on every RGW in the zone). The agent itself does not use STS.

On croit-managed clusters, accounts and IAM are not shown in the croit UI. They are
standard RGW objects in the cluster and live alongside what croit manages. Mention
them when raising a croit support ticket about S3.

### Configuration Options

#### Backend Settings

Both entry points validate these with
`waldur_site_agent_ceph_s3.schemas.CephS3BackendSettingsSchema`; a misspelt key is logged
as a warning when the agent loads its configuration. Which keys are required depends on
the flavour and is checked when the backend starts.

- **`flavour`** (optional, default: `"croit"`): `croit` or `radosgw`
- **`api_url`** (croit only, required): croit **management** API base URL (/api is appended)
- **`s3_endpoint`** (required): the S3 data endpoint tenants connect to. This is a
  *different host* from `api_url` — RadosGW runs on the storage nodes while the
  management API runs on the croit VM, and croit exposes no VIP or DNS name to derive
  it from. Point it at a load balancer or DNS name in front of the RGW nodes
- **`s3_region`** (optional, default: `"default"`): the region S3 clients sign with.
  Maps to the RadosGW zonegroup's `api_name`; croit exposes no endpoint to read it back,
  and `"default"` is Ceph's out-of-the-box zonegroup name. Set it if yours was named
- **`token`** (croit only): bearer token for API authentication
- **`username`** (croit only): API username (alternative to token). croit refuses
  HTTP Basic on its API, so the pair is exchanged at `/auth/login-form` for a session
  token, which the client renews before it expires
- **`password`** (croit only): API password (alternative to token)
- **`admin_access_key`** (radosgw only, required): access key of the RGW user
  holding `users=*;buckets=read`
- **`admin_secret_key`** (radosgw only, required): its secret
- **`admin_path`** (radosgw only, default: `"admin"`): the value of
  `rgw_admin_entry`, if the operator renamed it
- **`verify_ssl`** (optional, default: `true`): Enable/disable SSL certificate verification
- **`timeout`** (optional, default: `30`): Request timeout in seconds
- **`allocation_prefix`** (optional, default: `"waldur-"`): namespace every uid this
  agent creates is built under — the only setting that changes a provisioned uid. A
  core setting, but the core defaults it to `""`, which this plugin overrides: an
  empty namespace puts a consumer-chosen resource name straight into the cluster's
  global one. See Username Generation
- **`tenant_template`** (optional, either flavour): RadosGW tenant for each new
  user, rendered from the resource at creation. In account mode the account is
  created in that tenant too. See [Tenants](#tenants)
- **`default_tenant`** (optional, either flavour): one fixed tenant, the older form
  of `tenant_template`, taken as written (letters, digits and `_`). Ignored when
  `tenant_template` is set
- **`default_placement`** (optional): Default placement rule. Honoured by both
  flavours
- **`default_storage_class`** (croit only, optional): Default storage class.
  Rejected on the radosgw flavour: Admin Ops user-create takes a placement but no
  storage class

The billing period boundary for GB-day reporting follows the **agent's global**
`timezone`, which the reporting processor injects into the backend — the same value
it uses to file the usage, so the two cannot drift. The configuration validator
rejects a timezone that does not parse: the backend used to fall back to UTC while
the processor fell back to naive system local time, so near a month boundary a typo
filed a whole period's usage against the wrong month.

**Set it to whatever Mastermind computes billing periods in.** The agent derives both
the integration bounds and the `billing_period` it files against from its own clock,
so an agent on `Europe/Tallinn` reporting to a UTC Mastermind integrates the first
hours of each month into the period Mastermind has already closed. A fixed component
would not notice; GB-days move storage-hours onto the wrong invoice.

#### Component Types

##### Usage-Based Storage (`s3_storage`)

The only billed component. Reports GB-days — storage integrated over the billing
period — with optional safety quota enforcement:

```yaml
s3_storage:
  accounting_type: "usage"
  backend_name: "storage"
  label: "Storage"
  measured_unit: "GB-day"  # GB x days; see Usage Reporting
  unit_factor: 1000000000  # decimal GB (1000 GB = 1 TB), not GiB
```

**`unit_factor` is mandatory here and must be above 1.** The core config model
defaults it to `1.0` and normalises every component through that default, so an
omitted value arrives at the plugin as a real one — nothing downstream can tell
"not configured" from "configured as 1". At 1 both directions break silently: the
quota path sends an ordered 5 GB ceiling as **5 bytes**, capping the tenant at
nothing, and the metering path divides by 1, reporting byte-days against a GB-day
price. The backend therefore refuses to construct rather than start and misbill,
so a missing `unit_factor` fails at agent startup with the component named.

Setting `limit_amount`/`limit_period` on this component is harmless but noisy.
`validate_amount()` only *raises* for non-usage components; for a usage component
it logs a warning and the report lands anyway. Two things make that warning fire
early: a GB-denominated threshold is compared against a GB-day amount, and the
running total includes the row about to be overwritten, so cumulative reporting
roughly double-counts. Leave it unset unless you want the log noise.

## Username Generation

**Provisioned resources are named when the order is processed.** The plugin's
`_get_resource_backend_id()` builds `f"{allocation_prefix}{resource_slug}".lower()`,
with `allocation_prefix` defaulting to `waldur-` (not to the core's empty string), and
Waldur caps every slug at 10 characters (`SLUG_NAME_LIMIT`), appending `-2`, `-3`, … on
collision. So a resource named `cust-0-proj-0-ceph-s3-of` becomes the uid
`waldur-cust-0-pro`.

**Format**: `{allocation_prefix}{resource_slug}` — **Example**: `waldur-cust-0-pro`

With a [tenant](#tenants) configured, the backend id is `tenant$uid`, e.g.
`acme_corp_aaaabbbb$waldur-cust-0-pro`.

This is the only naming scheme the plugin has. A second one — `user_prefix`,
`slug_separator`, `max_username_length` and a `create_resource()` that read them —
existed alongside it and was never reached, since the order processor calls
`create_resource_with_id()`. It has been removed; an offering still carrying those
three keys is unaffected, they are simply ignored.

## Tenants

A RadosGW tenant gives its users their own bucket namespace: two organisations can
each own a bucket called `data`, where without tenants the first to create it takes
the name cluster-wide. `tenant_template` (either flavour) puts each new S3 user into
a tenant rendered from the resource being provisioned:

```yaml
      tenant_template: "{customer_slug}_{customer_uuid:.8}"
```

| Field | Rendered as |
|---|---|
| `resource_slug`, `customer_slug`, `project_slug`, `offering_slug` | lower-cased, `-` → `_` |
| `resource_uuid`, `customer_uuid`, `project_uuid`, `offering_uuid` | 32 hex characters, no hyphens |

A format spec truncates: `{customer_uuid:.8}` is the first eight hex characters.
Literal text may use lower-case letters, digits and `_`, the only characters RGW
allows in a tenant name; the rendered tenant is at most 64 characters.

- **Pick the scope deliberately.** `customer_*` gives one namespace per organisation,
  `project_*` one per project, `resource_*` one per S3 user (full isolation, but a
  user then shares its namespace with nobody, so cross-user bucket policies need
  `tenant:bucket` spelling).
- **Prefer a UUID, or a slug plus a short UUID.** Slugs can be edited, and `a-b`
  and `a_b` render to the same tenant; the UUID suffix keeps two organisations
  from landing in one namespace.
- **The tenant is rendered once.** RGW addresses a tenanted user only as
  `tenant$uid`, so that is what the backend id holds, and every later call (keys,
  quotas, deletion, reporting) uses it as stored. Renaming a slug or editing the
  template affects new resources only; existing ones stay where they were created.
- **A field the resource lacks fails the order** rather than falling back to the
  global namespace. A malformed template fails at agent start.
- **Reporting meters tenanted users through their buckets.** croit's per-user
  statistics cannot name a tenanted user (`tenant$uid` is refused with 400, and
  no other spelling returns a series), but its per-bucket statistics name a
  tenanted bucket as `tenant_bucket`. A tenanted resource is therefore metered as
  the sum of its buckets, the same way as an account (see
  [Reporting for a radosgw-managed offering](#reporting-for-a-radosgw-managed-offering-on-a-croit-cluster)).
- **Account mode** (radosgw, `rgw_accounts: true`) creates the resource's RGW
  account in the same tenant, with its root user as `tenant$uid`. RGW keeps an
  account's users in the account's tenant, so the consumer's IAM users and every
  bucket they create stay in that namespace too. `tenant_template:
  "{resource_uuid}"` gives each resource a namespace of its own (verified on Ceph
  20.2.5: two tenants each own a bucket called `data`, and neither sees the
  other's).
- **Clients** reaching a bucket in another tenant address it as `tenant:bucket`;
  their own buckets need no change. The tenant is published as the
  `s3_tenant` metadata key (empty when untenanted), next to `s3_user`.

## Usage Reporting

**On a croit cluster only, and it needs its own backend.** Set

```yaml
    reporting_backend: "croit_usage"
```

Metering reads croit's statistics subsystem, which plain Ceph does not have, so it
lives in a separate backend class. It works for both flavours: a croit-flavoured
offering reads croit with its own API settings, and a radosgw-flavoured offering
needs croit's address and credentials in `croit_reporting` (see
[Reporting for a radosgw-managed offering](#reporting-for-a-radosgw-managed-offering-on-a-croit-cluster)).
On plain Ceph, leave `reporting_backend` unset.

Leaving `reporting_backend` at `ceph_s3` reports **nothing** — deliberately, not
zeros. A zero is a legitimate usage value and would overwrite the period's
accrued total; silence leaves the last good figure in place.

Storage is billed in **GB-days** — the area under the storage curve over the
billing period, not a reading taken at report time. Object counts are not billed.

Usage is metered **per resource**, not per key: the series is the S3 user's, so
the resource's two access keys never enter the calculation and rotating one
cannot change a bill.

### Reporting for a radosgw-managed offering on a croit cluster

Management can talk to RGW directly (`flavour: radosgw`, including
[account mode](#account-mode-an-rgw-account-per-resource)) while usage still comes
from croit's statistics. `croit_usage` then needs croit's own address and
credentials in `croit_reporting`. The radosgw flavour keeps refusing croit's
top-level settings, so the two credential sets stay apart:

```yaml
    reporting_backend: "croit_usage"
    backend_settings:
      flavour: "radosgw"
      rgw_accounts: true
      s3_endpoint: "https://s3.example.com"
      admin_access_key: "ADMINACCESSKEY000000"
      admin_secret_key: "..."
      croit_reporting:
        api_url: "https://croit.example.com"
        token: "..."                      # or username + password
        verify_ssl: true
        bucket_state_file: "/var/lib/waldur-site-agent/ceph-s3-buckets.json"
```

**Accounts and tenanted users are metered per bucket, as an interim.**
- croit's per-user graph has nothing for an RGW account: the account owns the
  buckets, and its users hold no storage of their own.
- croit does keep a per-bucket graph (`s3-bucket-data`), which covers account
  buckets. So each pass lists the account's buckets through its root user, which
  includes buckets the consumer's IAM users created, then integrates each
  bucket's `Data` series and reports the sum.
- A tenanted bucket is named `tenant_bucket` in croit's statistics.
- This stays until croit can report an account as a whole.

**Buckets deleted during the month stay billed.**
- croit keeps a deleted bucket's series, but nothing lists the bucket any more.
- The reporter therefore remembers every bucket it has seen, per billing month,
  for the last 13 months. It holds the list in memory, and in `bucket_state_file`
  too when that is set (written once per pass, when it changed), so the list
  survives an agent restart and a historical recompute still knows a month's
  deleted buckets.
- A bucket series is cut after its last real reading. croit answers null for
  every interval after a bucket is deleted, so carrying the last reading across
  those nulls would bill the bucket until the month ends.
- Historical loads (`waldur_site_load_historical_usage`) fail, naming the
  resource, when a month cannot be measured, instead of letting it read as zero.
- Without the file, a restart forgets buckets deleted before it, and their days up
  to the deletion go unbilled.
- An account with no bucket known this period is not reported at all, rather than
  reported as zero, because a zero would overwrite the period's accrued total.

**New buckets.** A bucket croit has not sampled yet adds nothing until it has,
which takes a few minutes after its first write.

### Where the number comes from

`GET /api/stats?graph=s3-user-data&template-s3-user-name=<uid>` returns croit's
per-user storage series as `{"t": unix seconds, "v": bytes}` datapoints at 180 s
native resolution.

croit answers at most 10,000 points per request and averages the series beyond
that, on steps placed relative to the requested window. Read whole, a month would
come back averaged differently on every pass, and the same past days would
integrate to a slightly different total each time — enough to make a
month-to-date figure drop. The period is therefore read in windows of 9,000 native
samples (18.75 days) and joined, so every pass integrates the same 180 s samples.

Each pass asks for the period so far and integrates:

```text
GB-days = Σ  (bytes_i / unit_factor) × (t_i+1 − t_i) / 86400
```

Each datapoint is one rectangle — the reading, held until the next sample. A null
`v` is missing telemetry, so the previous reading is carried across it; dropping
the interval would bill it at zero and turn a metrics outage into a silent
discount.

Recomputed from the period start every time rather than accumulated, so the value
is absolute: reporting is idempotent, and an agent that was down for two days
returns the correct figure on its next pass with no catch-up logic. It also only
rises within a period, so Waldur's anomaly detection stays useful and
`supports_decreasing_usage` stays at the default `False`.

### Why GB-days rather than GB

Waldur bills a usage component as a flat `unit_price × quantity` — the `PER_DAY`
proration in invoicing applies only to *fixed* components. So for a price list
denominated per GB per day, the days have to be inside the quantity. Reporting
plain GB against a per-day price under-bills by roughly the number of days in the
month.

### Precision

The integral is computed in `Decimal` and rounded half-up to two places, which is
what the reporter's `"%.2f"` wire format preserves. The report leaves the backend
as plain floats, like every other plugin.

### Report Format

The reporter reads `TOTAL_ACCOUNT_USAGE` and skips any resource whose report
lacks it. There is exactly one S3 user per resource, so the account total *is*
that user's usage; per-user entries would key on Waldur offering users, which
this backend does not manage.

A resource whose series cannot be read is **absent** from the report rather than
present with a zero — zero is a valid usage value and would overwrite the
period's accrued total.

```json
{
  "cust-0-pro": {
    "TOTAL_ACCOUNT_USAGE": {
      "s3_storage": 3100.0
    }
  }
}
```

Values are plain numbers already in Waldur units, with `unit_factor` applied —
not `{"usage": n}` wrappers.

## Resource Metadata

Each S3 user resource exposes comprehensive metadata:

### S3 Connection

Credentials are **not** in resource metadata. Each resource owns two S3 access keys, stored
encrypted in Waldur and revealed or rotated per key from the portal (see
`waldur-mastermind/docs/resource-api-keys.md`). Metadata carries only the non-secret
connection info, and the S3 endpoint is also published as a resource access endpoint:

```json
{
  "s3_endpoint": "https://s3.example.com",
  "s3_region": "default",
  "s3_user": "cust-0-pro"
}
```

These are deliberately **flat** keys. An offering's *Getting started* text is a template
interpolating `{backend_metadata_<key>}` one level deep, so a nested block would render as
`[object Object]`. Flat keys let a provider write usable instructions once, for example:

```text
Configure your S3 client:

```aws --endpoint-url {backend_metadata_s3_endpoint} s3 ls```

Your access key is on the **API keys** tab — click Reveal for the secret.
```

The agent mints both halves of each pair, applies them to the S3 user, and only then
reports them to Waldur — so a stored key is always one RadosGW already accepts. Rotation
replaces the access key as well as the secret, applying the new pair before dropping the
old one, so the resource's other key keeps working throughout.

**Every credential that works is one Waldur can rotate.** Two things would otherwise
break that:

- *RadosGW mints its own key when a user is created*, and Waldur never sees its secret.
  It is deleted right after the user is created, while it is that user's only key and
  before anyone holds credentials. This happens only on the branch that actually created
  the user. An existing uid is adopted with its keys left in place at provisioning time,
  but they are not in Waldur's known set, so the resource's first rotation prunes them.
- *A rotation whose reply to Waldur is lost* has already replaced the access key at the
  backend, so re-issuing it rotates from a client_id croit no longer has and would leave
  the intermediate key live and invisible. Each rotation therefore receives the resource's
  known client_ids and drops anything outside that set. If Waldur cannot be listed the set
  is `None` — unknown, so nothing is pruned; treating it as empty would delete every
  credential the resource has.

**Known limitation:** croit's create-key endpoint takes the secret as a query parameter
(`PUT /s3/users/{uid}/keys/{accessKey}?secretKey=…`), so it appears in croit's access log.
The bulk alternative (`PUT /s3/users/{uid}/keys`) keeps the secret in the body but replaces
the user's entire key set asynchronously, which can drop a sibling key when two rotations
overlap.

### Storage Summary

```json
{
  "storage_summary": {
    "bucket_count": 3,
    "total_size_bytes": 5368709120,
    "total_objects": 1250,
    "buckets": [
      {
        "name": "my-bucket",
        "size_bytes": 1073741824,
        "objects": 500
      }
    ]
  }
}
```

### Quota Information

```json
{
  "quotas": {
    "bucket_quota": {
      "enabled": true,
      "maxSize": 107374182400,
      "maxObjects": 10000
    },
    "user_quota": {
      "enabled": true,
      "maxSize": 107374182400,
      "maxObjects": 10000
    }
  }
}
```

## Safety Quota Enforcement

Ordering a ceiling is the only signal needed — there is no enable switch. The
plugin applies the ordered limits as RadosGW quotas:

1. **Create Resource**: apply the ordered ceilings (`max_storage_limit`,
   `max_object_limit`) — **on the create path only**; quotas are not re-asserted
   afterwards, so an existing resource keeps whatever it was given
2. **Prevent Overages**: the quota is what bounds the tenant, and therefore the
   invoice — usage billing itself has no ceiling

### Quota Types

Both are set from the same ordered ceiling:

- **User quota** (`PUT /s3/users/{uid}/quota`): the aggregate cap across everything
  the user owns. This is the one that bounds a tenant — croit exposes no way to
  cap the bucket count, so a per-bucket quota alone bounds nothing
- **Bucket quota** (`PUT /s3/users/{uid}/bucket-quota`): a per-bucket guard
- `maxSize` in bytes (`max_storage_limit` × `unit_factor`), `maxObjects` as a count

### How Safety Limits Work

1. **User Configuration**: Users set `max_storage_limit` and `max_object_limit` via Waldur marketplace form
2. **Resource Options**: Waldur passes these as resource attributes to the site agent
3. **Quota Application**: applied as user and bucket quotas during S3 user creation.
   A resource that reaches provisioning with no ceiling is logged as a warning
   naming the attributes that were present — an unbounded resource is one with no
   bound on its invoice
4. **Usage Billing**: actual consumption is measured and billed separately from the
   quota, which is a cap rather than a billing basis

## Waldur Marketplace Integration

### Creating the Matching Offering

To create a matching offering in Waldur Mastermind, run the setup script:

```bash
# In your Waldur Mastermind directory
cd /path/to/waldur-mastermind

# Run the offering creation script
DJANGO_SETTINGS_MODULE=waldur_core.server.settings uv run python -c "
import os
import sys
import django

# Setup Django
sys.path.insert(0, 'src')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'waldur_core.server.settings')
django.setup()

from django.db import transaction
from decimal import Decimal
from waldur_core.structure.tests.factories import CustomerFactory
from waldur_mastermind.marketplace.enums import SITE_AGENT_OFFERING, BillingTypes, OfferingStates
from waldur_mastermind.marketplace.models import Category, ServiceProvider, Offering, OfferingComponent, Plan, PlanComponent

def create_ceph_s3_offering():
    with transaction.atomic():
        # Create category
        category, _ = Category.objects.get_or_create(
            title='Storage',
            defaults={'description': 'Cloud storage services', 'icon': 'fa fa-hdd-o'}
        )

        # Create service provider
        customer, _ = CustomerFactory._meta.model.objects.get_or_create(
            name='Croit Storage Provider',
            defaults={'abbreviation': 'CROIT', 'native_name': 'Croit Storage Provider'}
        )
        service_provider, _ = ServiceProvider.objects.get_or_create(
            customer=customer,
            defaults={'description': 'Ceph S3 object storage services'}
        )

        # Create offering
        offering, created = Offering.objects.get_or_create(
            name='Ceph S3 Object Storage',
            defaults={
                'type': SITE_AGENT_OFFERING,
                'category': category,
                'customer': service_provider.customer,
                'description': 'S3-compatible object storage with usage-based billing. '
                               'Each resource provides one S3 user account with configurable safety limits.',
                'state': OfferingStates.ACTIVE,
                'billable': True,
                'plugin_options': {
                    'backend_type': 'ceph_s3',
                    'service_provider_can_create_offering_user': False,
                    'auto_create_admin_user': False,
                    # A ceiling change becomes an UPDATE order the agent applies,
                    # rather than a value Waldur records and croit never sees.
                    'create_orders_on_resource_option_change': True,
                },
                'options': {
                    'order': ['max_storage_limit', 'max_object_limit'],
                    'options': {
                        'max_storage_limit': {
                            'type': 'integer',
                            'label': 'Storage Limit (GB)',
                            'help_text': 'Maximum storage capacity in gigabytes (safety limit)',
                            'required': True,
                            'default': 100,
                            'min': 1,
                            'max': 10000,
                        },
                        'max_object_limit': {
                            'type': 'integer',
                            'label': 'Object Count Limit',
                            'help_text': 'Maximum number of objects; 0 means no limit',
                            'required': False,
                            'min': 0,
                        }
                    }
                },
                # The same two ceilings, editable on the resource after creation.
                # Only together with create_orders_on_resource_option_change:
                # without it Waldur stores an edit directly and the quota stays.
                'resource_options': {
                    'order': ['max_storage_limit', 'max_object_limit'],
                    'options': {
                        'max_storage_limit': {
                            'type': 'integer', 'label': 'Storage Limit (GB)',
                            'required': True, 'min': 1, 'max': 10000,
                        },
                        'max_object_limit': {
                            'type': 'integer', 'label': 'Object Count Limit',
                            'help_text': '0 means no limit',
                            'required': False, 'min': 0,
                        },
                    },
                },
            }
        )

        # Create components
        storage_component, _ = OfferingComponent.objects.get_or_create(
            offering=offering,
            type='s3_storage',
            defaults={
                'name': 'S3 Storage',
                'description': 'Object storage, billed per GB per day',
                'billing_type': BillingTypes.USAGE,
                'measured_unit': 'GB-day',
                'article_code': 'CROIT_S3_STORAGE',
            }
        )

        # Create plan
        plan, _ = Plan.objects.get_or_create(
            offering=offering,
            name='Standard Plan',
            defaults={
                'description': 'Pay-per-use S3 storage with configurable safety limits',
                'unit': 'month',
                'unit_price': Decimal('0.00'),
            }
        )

        # Create plan components with pricing
        PlanComponent.objects.get_or_create(
            plan=plan,
            component=storage_component,
            defaults={'price': Decimal('0.0010'), 'amount': 1}  # €0.0010 per GB-day
        )

        print(f'✓ Ceph S3 offering created: {offering.uuid}')
        print(f'  Add this UUID to your site agent config')
        return offering.uuid

create_ceph_s3_offering()
"

```

**Alternative**: Save the above code as `setup_ceph_s3_offering.py` and run:

```bash
DJANGO_SETTINGS_MODULE=waldur_core.server.settings uv run python setup_ceph_s3_offering.py
```

### Offering Configuration

The created Waldur offering will have:

- **Type**: `SITE_AGENT_OFFERING` ("Marketplace.SiteAgent")
- **Components**: `s3_storage` only (usage-based, billed in GB-days)
- **Options**: `max_storage_limit` and `max_object_limit` for user input (safety limits)
- **Pricing**: €0.0010 per GB-day for storage; objects are not billed

Leave `plugin_options.enable_api_key_provisioning` **off** on a Ceph S3
offering. It turns on per-key governance — request, assign, per-key limits,
pause, resume, delete — which this plugin does not support: it does not set
`supports_resource_api_key_lifecycle`, so the agent answers every such command
with `set_erred` and the key ends up Erred in the portal. Waldur cannot check
the option against the backend, so nothing stops it being turned on. Reveal and
rotate work without it.

### Order Payload Example

```json
{
  "offering": "http://localhost:8000/api/marketplace-public-offerings/{offering_uuid}/",
  "project": "http://localhost:8000/api/projects/{project_uuid}/",
  "plan": "http://localhost:8000/api/marketplace-public-offerings/{offering_uuid}/plans/{plan_uuid}/",
  "attributes": {
    "max_storage_limit": 100,
    "max_object_limit": 10000
  },
  "name": "my-s3-storage",
  "description": "S3 storage for my application",
  "accepting_terms_of_service": true
}
```

## Testing

Run the test suite:

```bash
cd plugins/ceph-s3
uv run pytest tests/ -v
```

## Development

### Adding New Components

1. Define component in site agent configuration:

```yaml
my_custom_component:
  accounting_type: "usage"
  backend_name: "custom_metric"
  label: "My custom metric"
  measured_unit: "units"
  unit_factor: 1
```

- Add usage collection logic in `_get_usage_report()`
- Add safety limit handling in `_apply_bucket_quotas()` if needed
- Add corresponding field in Waldur offering options for user input

### Error Handling

The plugin includes comprehensive error handling:

- **`CephS3AuthenticationError`**: API authentication failures
- **`CephS3UserNotFoundError`**: User doesn't exist
- **`CephS3UserExistsError`**: User already exists
- **`CroitS3GraphNotFoundError`**: the croit statistics graph behind usage reporting is missing
- **`CephS3APIError`**: General API errors
- **`CephS3Error`**: Base exception class

## Troubleshooting

### SSL Certificate Issues

```yaml
backend_settings:
  verify_ssl: false  # Disable for self-signed certificates
```

### Connection Timeouts

```yaml
backend_settings:
  timeout: 60  # Increase timeout for slow networks
```

### Username Length Issues

The uid is `{allocation_prefix}{resource_slug}`, and Waldur already caps the slug at
10 characters — so only `allocation_prefix` is yours to shorten. See Username
Generation.

### Debug Logging

Use standard Python logging configuration or waldur-site-agent logging settings to enable debug output for the plugin modules:

- `waldur_site_agent_ceph_s3.clients.croit` - croit API interactions
- `waldur_site_agent_ceph_s3.clients.radosgw` - RadosGW admin API interactions
- `waldur_site_agent_ceph_s3.backend` - Backend operations

Set the top-level `log_level: DEBUG` in the agent configuration.

## Resource Lifecycle

1. **Order Creation**: User submits order with `max_storage_limit` and `max_object_limit`
2. **User Creation**: Plugin creates the S3 user under the backend id the order processor generated
3. **Quota Application**: ceilings applied as user and bucket quotas
4. **Key Provisioning**: Two access keys minted, applied, and reported to Waldur encrypted;
   RadosGW's auto-generated key is removed
5. **Usage Tracking**: storage integrated into GB-days per billing period
6. **Ceiling Updates**: editing `max_storage_limit` or `max_object_limit` on the
   resource creates an UPDATE order carrying `new_options`, when the offering has both
   `resource_options` and `plugin_options.create_orders_on_resource_option_change`.
   The agent re-applies both quotas from the order attributes, overlaid by the current
   options, overlaid by the new ones, before Waldur records the change. A ceiling set
   to `0` is lifted: it is sent as `-1`, never as `0` (croit stores 0 as unlimited,
   RadosGW as a cap of nothing) and never left out (RadosGW keeps a left-out limit).
   Waldur refuses `null` for an integer option, so `0` is how a consumer clears one.
   Without `create_orders_on_resource_option_change`, Waldur stores an edit directly
   and the quota is not touched
7. **Resource Deletion**: the S3 user is removed — but only once it owns no
   buckets. croit refuses to delete a user that still holds data
   (`500 Unable to remove user with buckets.`), so termination of a tenant with
   data fails until the buckets are emptied and removed. The failure reason
   reaches the portal's order history and resource activity log.
