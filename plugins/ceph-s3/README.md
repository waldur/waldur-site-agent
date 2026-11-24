# Ceph S3 Storage Plugin for Waldur Site Agent

Integrates Waldur Mastermind with Ceph object storage. Each marketplace resource
gets an RGW tenant of its own with one user in it, managed through RGW's Admin Ops
API: its access keys are Waldur resource API keys, and its ordered ceiling is an
RGW quota.

Usage comes from one of two reporting backends:

| | `croit_usage` | `rgw_usage` |
|---|---|---|
| Reads | croit's statistics (croit clusters) | RGW itself (any Ceph) |
| Storage | GB-days integrated from croit's stored-bytes series | GB-days accrued from bucket statistics, pass by pass |
| Traffic and requests | no | yes, from the RGW usage log |

croit's own user management is no longer used: on a croit cluster the agent talks
to RGW directly, and reads croit only for statistics. An offering still configured
for it is refused at start; see
[Migrating from the croit flavour](#migrating-from-the-croit-flavour).

## At a glance

| Entry point | Group | Role |
|---|---|---|
| `ceph_s3` | `waldur_site_agent.backends` | order processing, membership sync |
| `croit_usage` | `waldur_site_agent.backends` | reporting from croit's statistics (croit clusters) |
| `rgw_usage` | `waldur_site_agent.backends` | reporting from RGW's Admin Ops (any Ceph) |

**Modes:** `order_process`, `membership_sync`, `event_process`, and `report` with
`reporting_backend: croit_usage` or `rgw_usage`.

| Operation | Behaviour |
|---|---|
| Create resource | A tenant of its own with one S3 user, `root`; user and bucket quotas. The tenant is the backend id |
| Terminate resource | Deletes the user (in account mode, the account and its users); refused while it owns buckets |
| Update limits | Updates the quotas |
| Add / remove members | **No-op** — returns `True`; one S3 user per resource, no per-member access |
| Pause / restore | Suspends / reinstates every user of the resource |
| Downscale | **No-op** — returns `True` without changing quotas |
| Usage reporting | `ceph_s3`: none. `croit_usage`: croit's statistics. `rgw_usage`: RGW's Admin Ops |

## Features

- **A tenant per resource**: every resource is an RGW tenant with its own bucket
  namespace, and one user Waldur creates in it (or, in account mode, an RGW account
  whose IAM the consumer runs)
- **Usage-Based Billing**: storage billed in GB-days, integrated from croit's
  metrics or accrued from RGW's bucket statistics over the billing period, and,
  with `rgw_usage`, traffic and requests from the RGW usage log. Object counts are
  not billed
- **Safety Quota Enforcement**: an ordered ceiling is applied as both a RadosGW
  user quota (the tenant total) and a bucket quota (per bucket)
- **Credential Management**: two S3 access keys per resource, encrypted in Waldur and
  managed from the portal (reveal / rotate). The count is fixed at provisioning —
  rotation replaces a key's value in place, so there is no add or revoke

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

    # Backend roles. Metering is a separate backend: croit_usage reads croit's
    # statistics, rgw_usage reads RGW itself; see Usage Reporting.
    backend_type: "ceph_s3"
    order_processing_backend: "ceph_s3"
    membership_sync_backend: "ceph_s3"
    reporting_backend: "rgw_usage"             # or croit_usage on a croit cluster

    backend_settings:
      s3_endpoint: "https://s3.example.com"    # what tenants connect to; Admin Ops too
      admin_access_key: "ADMINACCESSKEY000000" # an RGW user with admin caps, below
      admin_secret_key: "..."
      s3_region: "default"                     # must match what clients sign with
      admin_path: "admin"                      # only if rgw_admin_entry was renamed
      verify_ssl: true
      # tenant_template: "{customer_abbreviation:.12}_{project_name:.12}_{resource_name:.20}_{resource_uuid:.8}"
      # On a croit cluster, with reporting_backend: croit_usage:
      # croit_reporting:
      #   api_url: "https://croit.example.com"
      #   token: "..."                       # or username + password
      #   bucket_state_file: "/var/lib/waldur-site-agent/ceph-s3-buckets.json"

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

`flavour: radosgw` is still accepted, and may be left out.

### Preparing the cluster

Run these where `radosgw-admin` works, i.e. on a host with the cluster's admin
keyring (on croit, a shell on any of its servers).

1. **Create the agent's user.** An ordinary RGW user with admin caps: it needs no
   `metadata` cap and must not be a `--system` user. `usage=read` is only needed
   for `rgw_usage`, `accounts=*` only for account mode; granting both does no harm.

   ```bash
   radosgw-admin user create --uid=waldur-agent --display-name="Waldur site agent"
   radosgw-admin caps add --uid=waldur-agent --caps="users=*;buckets=*;usage=read;accounts=*"
   ```

2. **Read its keys** into `admin_access_key` and `admin_secret_key`:

   ```bash
   radosgw-admin user info --uid=waldur-agent   # keys[0].access_key, keys[0].secret_key
   ```

3. **Open Admin Ops to the agent.** The gateway serves it itself, under `/admin`
   (`rgw_admin_entry`), on the same address as S3; some load balancers in front of
   public gateways block that path. From the agent's host, an unsigned request
   must reach RGW, which refuses it in its own words:

   ```bash
   curl -sk https://s3.example.com/admin/user
   # {"Code":"AccessDenied", ...} from RGW -- reachable.
   # An HTML page, a 404 or a timeout comes from something in between.
   ```

4. **For `rgw_usage` traffic and request billing**, turn the usage log on and
   restart the gateways (Ceph ships with it off; storage billing does not need it):

   ```bash
   ceph config set client.rgw rgw_enable_usage_log true
   ```

5. **For account mode**, see the checks under
   [Account mode](#account-mode-an-rgw-account-per-resource) (Ceph 20.2.2 or later).

6. **Only for `default_storage_class`**: the class must exist in the placement
   named by `default_placement`. To add one, for example `COLD` on the default
   placement (single-zone cluster; with a realm, commit with
   `radosgw-admin period update --commit`):

   ```bash
   radosgw-admin zonegroup placement add --rgw-zonegroup=default \
     --placement-id=default-placement --storage-class=COLD
   radosgw-admin zone placement add --rgw-zone=default \
     --placement-id=default-placement --storage-class=COLD \
     --data-pool=default.rgw.cold.data
   ```

   Then restart the gateways. RGW refuses a class the placement does not have
   (`InvalidArgument`, which fails the order), and stores `STANDARD` as no class
   at all, i.e. the placement's default.

**Without a shell on the cluster**, an existing RGW admin with the `users=*` cap
can create the same user through Admin Ops:
`PUT /admin/user?uid=waldur-agent&display-name=Waldur+site+agent&user-caps=users=*;buckets=*;usage=read;accounts=*`
signed with that admin's keys (SigV4, service `s3`); the response carries the new
keys.

### Migrating from the croit flavour

Users used to be managed through croit's API (`flavour: croit`, then the default).
That is gone: an offering still configured for it -- `flavour: croit`, or croit's
`api_url`, `token`, `username` or `password` at the top of `backend_settings` --
fails at agent start with these steps:

1. **Create an RGW admin user** and make Admin Ops reachable from the agent, as in
   [Preparing the cluster](#preparing-the-cluster) (steps 1-3; step 4 too if the
   offering moves to `rgw_usage`).
2. **Set `admin_access_key` and `admin_secret_key`** (and `admin_path` if
   `rgw_admin_entry` was renamed). Remove `flavour: croit`.
3. **If the offering reports with `croit_usage`**, move `api_url` and `token` (or
   `username` and `password`) under `croit_reporting`, and set
   `croit_reporting.bucket_state_file` -- the croit flavour could not keep one, so
   until now a bucket deleted mid-month could drop out of the bill after a restart.

Nothing about existing resources changes. croit managed the same RGW users the
agent now reaches directly, so their backend ids, tenants and ownership stamps
stay as they are.

```yaml
# before                                   # after
backend_settings:                          backend_settings:
  flavour: "croit"                           s3_endpoint: "https://s3.example.com"
  api_url: "https://croit.example.com"       admin_access_key: "..."
  token: "..."                               admin_secret_key: "..."
  s3_endpoint: "https://s3.example.com"      croit_reporting:
                                               api_url: "https://croit.example.com"
                                               token: "..."
                                               bucket_state_file: "/var/lib/..."
```

`default_storage_class` now needs `default_placement` as well: RGW applies a
storage class only as part of a placement rule.

### Things to know

- **Storage is accrued, not integrated, with `rgw_usage`.** Admin Ops exposes no
  stored-bytes series, only a bucket's size now, so `rgw_usage` accrues GB-days
  pass by pass (see [Reporting from RGW itself](#reporting-from-rgw-itself-rgw_usage)).
  On a croit cluster, `croit_usage` integrates croit's statistics instead (see
  [Reporting with croit's statistics](#reporting-with-croits-statistics)).
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
- **The backend id is the tenant**, as in user mode. The account lives in it, is
  named `root` after its root user and carries the same ownership stamp, so an
  interrupted create is adopted on retry, and an account of that name that is not
  this resource's is refused. The account id is published as `s3_account_id`,
  which is what the consumer's IAM policy ARNs name.
- **Display names are uids.** The root user's display name is its IAM
  `UserName`, held to `[\w+=,.@-]{1,64}`, so the bare uid (`root`) is used in
  place of the resource name.
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
  alone would not stop the others); restoring reinstates them all. A plain user
  is suspended the same way. Downscaling has no S3 meaning and changes nothing.
- **Existing offerings.** Resources provisioned before the switch stay plain
  users. Account membership cannot be added without moving a user's buckets.

Cluster preparation (from a shell with `radosgw-admin`):

1. Check `ceph versions` shows 20.2.2 or later on every RGW.
2. Check the IAM API is enabled: `ceph config get client.rgw rgw_enable_apis`
   must list `iam`, which is the default.
3. Create the agent's user as in [Preparing the cluster](#preparing-the-cluster);
   it must hold `accounts=*`.
4. In a multisite setup, point `s3_endpoint` at the metadata master zone.
5. Optional: to let consumers use IAM roles with `AssumeRole`, enable STS with
   `rgw_s3_auth_use_sts = true` and an `rgw_sts_key` (16 hex characters, the same
   on every RGW in the zone). The agent itself does not use STS.

On croit clusters, accounts and IAM are not shown in the croit UI. They are
standard RGW objects in the cluster and live alongside what croit manages. Mention
them when raising a croit support ticket about S3.

### Configuration Options

#### Backend Settings

Every entry point validates these with
`waldur_site_agent_ceph_s3.schemas.CephS3BackendSettingsSchema`; a misspelt key is logged
as a warning when the agent loads its configuration. Required keys are checked when the
backend starts.

- **`s3_endpoint`** (required): the S3 endpoint tenants connect to, and the one the
  agent reaches Admin Ops on. Point it at a load balancer or DNS name in front of the
  RGW nodes
- **`s3_region`** (optional, default: `"default"`): the region S3 clients sign with.
  Maps to the RadosGW zonegroup's `api_name`; `"default"` is Ceph's out-of-the-box
  zonegroup name. Set it if yours was named
- **`admin_access_key`** (required): access key of the RGW user holding
  `users=*;buckets=*`, plus `usage=read` for `rgw_usage` and `accounts=*` for
  account mode
- **`admin_secret_key`** (required): its secret
- **`admin_path`** (optional, default: `"admin"`): the value of `rgw_admin_entry`,
  if the operator renamed it
- **`flavour`** (optional): `radosgw`, the only one. `croit` is refused with the
  migration steps, as are croit's former `api_url`, `token`, `username` and
  `password`; see [Migrating from the croit flavour](#migrating-from-the-croit-flavour)
- **`croit_reporting`** (`croit_usage` only): croit's `api_url`, `token` or
  `username` and `password`, `verify_ssl`, `timeout` and `bucket_state_file`, for
  reading its statistics
- **`verify_ssl`** (optional, default: `true`): Enable/disable SSL certificate verification
- **`timeout`** (optional, default: `30`): Request timeout in seconds
- **`tenant_template`** (optional): the tenant each new resource
  gets for itself, which becomes its backend id. Must contain `{resource_uuid}` or
  `{resource_slug}`. See [Tenants and backend ids](#tenants-and-backend-ids)
- **`default_tenant`**: no longer supported. It put every resource into one shared
  tenant, and the agent refuses to start while it is set
- **`allocation_prefix`**: not used by new resources. Resources from before one
  tenant per resource have uids built from it (`waldur-<slug>`), and keep them;
  see [Resources from before one tenant per resource](#resources-from-before-one-tenant-per-resource)
- **`default_placement`** (optional): Default placement rule for new users
- **`default_storage_class`** (optional): Default storage class for new users,
  within `default_placement`, which it requires: RGW applies a storage class only
  as part of a placement rule
- **`storage_size`** (`rgw_usage` only, default: `"logical"`): which bucket size is
  billed, `logical` (the bytes stored, RGW's `size`; what croit's statistics bill)
  or `allocated` (RGW's `size_actual`, including allocation overhead)

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

Reports GB-days — storage integrated or accrued over the billing period — with
optional safety quota enforcement. With `rgw_usage`, traffic and request components
can be billed beside it (see
[Reporting from RGW itself](#reporting-from-rgw-itself-rgw_usage)):

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

## Tenants and backend ids

**Every resource gets a RadosGW tenant of its own, and the tenant is the resource's
backend id.** A tenant is a bucket namespace. Resources that share one can squat
each other's bucket names, learn them from a "bucket already exists" answer, and
grant each other access in bucket policies without a `tenant:` prefix; a tenant
per resource leaves none of that. Data, quotas and keys were already per resource.

Inside its tenant the resource has one user Waldur creates, always named `root`
(in account mode, the RGW account is named `root` as well). So a resource whose
tenant is `acme_research_tea_backups_1a2b3c4d` is the S3 user
`acme_research_tea_backups_1a2b3c4d$root`, and its backend id is the tenant.

The tenant is rendered once, when the order is processed, from `tenant_template`:

```yaml
      tenant_template: "{customer_abbreviation:.12}_{project_name:.12}_{resource_name:.20}_{resource_uuid:.8}"
```

That is also the default when the setting is absent.

| Field | Rendered as |
|---|---|
| `resource_uuid`, `customer_uuid`, `project_uuid`, `offering_uuid` | 32 hex characters, no hyphens |
| `resource_slug`, `customer_slug`, `project_slug`, `offering_slug` | lower-cased, `-` → `_` |
| `resource_name`, `project_name`, `customer_name` | accents dropped, lower-cased, other runs → `_`; else the slug |
| `customer_abbreviation` | the organisation's abbreviation, folded like a name; else the customer slug |

- **The template must name the resource**: `{resource_uuid}` or `{resource_slug}`.
  A template rendered from the customer, project or offering alone is refused at
  agent start, because every resource of that scope would share the tenant. So is
  `default_tenant`, the older setting for one fixed tenant. Remove it; existing
  resources keep the tenant they were created in.
- **`{field:.N}` cuts a field to N characters**, dropping an underscore the cut
  leaves at the end. `{resource_uuid}` may not be cut below 8 characters and
  `{resource_slug}` not at all: a cut slug no longer tells resources apart. No
  other format spec is accepted.
- **Literal text** may use lower-case letters, digits and `_`, the only characters
  RGW allows in a tenant name. The rendered tenant is at most 64 characters; RGW
  itself sets no limit, this plugin does. A longer one fails the order, so cut
  fields of unbounded length.
- **The tenant is rendered once.** Renaming the organisation, project or resource,
  moving the resource to another project, or editing the template affects new
  resources only. Existing ones stay where they were created. The name is a label
  for people reading RGW listings, not something the agent relies on.
- **A field the resource lacks fails the order**: nothing is created.
- **`customer_abbreviation` needs a Waldur that serves it** on marketplace
  resources. Against an older one the field is absent and the slug is used.
- **Consumers see the tenant.** It is published as the `s3_tenant` metadata key,
  next to `s3_user`, and clients reaching a bucket in another tenant address it as
  `tenant:bucket`; their own buckets need no prefix.
- **`croit_usage` meters a tenanted user through its buckets.** croit's per-user
  statistics cannot name a tenanted user (`tenant$uid` is refused with 400, and
  no other spelling returns a series), but its per-bucket statistics name a
  tenanted bucket as `tenant_bucket`. A tenanted resource is therefore metered as
  the sum of its buckets, the same way as an account (see
  [Reporting with croit's statistics](#reporting-with-croits-statistics)).
- **Account mode** (`rgw_accounts: true`) creates the account in the
  resource's tenant, so the consumer's IAM users and every bucket they create stay
  in that namespace (verified on Ceph 20.2.5: two tenants each own a bucket called
  `data`, and neither sees the other's).

### Resources from before one tenant per resource

Their backend ids name the S3 user, not a tenant, and they keep working as they
are, for every operation including reporting:

- **`tenant$uid`**: a user created in a tenant by an earlier `tenant_template` or
  `default_tenant`, possibly shared with other resources.
- **a bare uid**, `{allocation_prefix}{resource_slug}` (e.g. `waldur-cust-0-pro`):
  a user in RGW's global namespace.

A bare uid with the `waldur-` prefix contains a hyphen, which no tenant name can,
so it is addressed as stored. Older bare uids may not have one -- `backup` from
before the `waldur-` default, or croit-s3's `waldur_<org>_<project>_<uuid>` -- and
look exactly like a tenant. For an id of that shape the agent asks RGW: the
tenant's `root` user if it exists, otherwise the bare uid if that does. A found
answer is remembered until the resource is created or deleted again.

Moving an old resource into a tenant of its own means copying its buckets to a new
user, which is an operator task, not something the agent attempts. A resource
re-created after termination keeps its stored backend id, in whichever shape. An
old bare uid that looks like a tenant comes back as that tenant's `root` user,
since nothing tells the two apart once the user is gone.

Two resources that render the same tenant cannot both have it: the second order
fails, naming the tenant, without touching the first. The processor's
`check_backend_id_uniqueness`, `backend_id_max_retries` and
`account_name_generation_policy` settings do not apply, since the backend id is
the tenant, not the id the processor generates.

Every user and account Waldur creates carries an ownership stamp in its email
field, `waldur-<resource uuid>@invalid` (`.invalid` is reserved, so it is never a
deliverable address). It is how the agent tells its own interrupted create, which
it finishes, from someone else's user, which it refuses to touch. RGW allows an
email address only once, so a resource cannot get a second user either: a retry
that renders a different tenant after a rename fails, naming the stamp already in
use. Users created before carry the older `waldur-<uuid>-<uid>@invalid`, which is
still recognised.

## Usage Reporting

Metering needs its own backend, one of two:

- **`croit_usage`** reads croit's statistics subsystem, so it needs a croit
  cluster, and croit's address and credentials in `croit_reporting` (see
  [Reporting with croit's statistics](#reporting-with-croits-statistics)).
- **`rgw_usage`** reads RGW's own Admin Ops, so it works on any Ceph, croit or
  not, and adds traffic and request metering (see
  [Reporting from RGW itself](#reporting-from-rgw-itself-rgw_usage)).

Leaving `reporting_backend` at `ceph_s3` reports **nothing** — deliberately, not
zeros. A zero is a legitimate usage value and would overwrite the period's
accrued total; silence leaves the last good figure in place.

Storage is billed in **GB-days** — the area under the storage curve over the
billing period, not a reading taken at report time. Object counts are not billed.

Usage is metered **per resource**, not per key: the series is the S3 user's, so
the resource's two access keys never enter the calculation and rotating one
cannot change a bill.

### Reporting from RGW itself (`rgw_usage`)

Plain users and [account mode](#account-mode-an-rgw-account-per-resource) alike.
No croit, no agent-side state and no volume are needed.

```yaml
    reporting_backend: "rgw_usage"
    backend_settings:
      rgw_accounts: true
      s3_endpoint: "https://s3.example.com"
      admin_access_key: "ADMINACCESSKEY000000"   # caps: users=*;buckets=*;accounts=*;usage=read
      admin_secret_key: "..."
      storage_size: "logical"                    # or "allocated"
    backend_components:
      s3_storage:
        accounting_type: "usage"
        backend_name: "storage"
        label: "Storage"
        measured_unit: "GB-day"
        unit_factor: 1000000000
        accrual_unit: "day"                      # "hour" for a GB-hour price
      s3_egress:
        accounting_type: "usage"
        backend_name: "bytes_sent"               # served to clients
        label: "Data out"
        measured_unit: "GB"
        unit_factor: 1000000000
      s3_ingress:
        accounting_type: "usage"
        backend_name: "bytes_received"           # written by clients
        label: "Data in"
        measured_unit: "GB"
        unit_factor: 1000000000
      s3_requests:
        accounting_type: "usage"
        backend_name: "ops"
        label: "Requests"
        measured_unit: "1000 requests"
        unit_factor: 1000
```

Any of the four may be left out. A component with another `backend_name` is not
reported.

**Storage is accrued, the way Waldur bills OpenStack usage.**
- Each pass reads what the resource stores **now**: the sum of its bucket sizes
  (`GET /admin/bucket?uid=…&stats=true`). In account mode the call goes through the
  root user, which lists every bucket of the account, including those the
  consumer's IAM users created.
- The pass adds a level times the time since the previous pass to the month's
  GB-days, as Waldur's OpenStack accumulator does. The interval is billed at the
  level the previous pass measured, and the level measured now is recorded for the
  next one, so storage is billed from the pass that first sees it. An outage longer
  than 24 hours is billed only for 24 of them, since the level may have changed
  anywhere in it. The 24 hours are counted after the up to 12 hours a quiet
  checkpoint may lag (see below), so an outage shorter than a day is billed whole.
- The checkpoint (the exact total and the moment it is accrued to) is kept in
  Waldur, in the usage row's description, for example
  `Accrued 123.456789 to 2026-11-21T23:40:12Z, at a level of 9.01 at that moment`.
  A restart, a retry or a second agent instance reads the same checkpoint, so
  nothing is counted twice, and rounding to Waldur's two decimals never drifts the
  sum. If someone changes the figure in Waldur, accrual continues from theirs.
- The first pass of a month closes the previous month to its last instant at the
  level it measures. That needs `reporting_periods` of 2 or more (the default) and
  a Waldur that still accepts that month's usage; otherwise the month's last
  interval goes unbilled. The new month starts at its first instant, at the level
  the previous month last recorded, or at the resource's creation.
- A pass that changes nothing (the same level, the same rounded total) does not
  write; the next pass bills the longer interval at the same level. An idle
  resource is still written at least every 12 hours, so its checkpoint never gets
  near the 24-hour cap.
- A level is held across the whole interval since the previous pass, so storage
  that comes and goes between two passes (30 minutes by default) is not seen, and
  a change is billed from the pass that first sees it. Over a month the error is
  at most one pass interval's worth of each change.
- Past months cannot be recomputed: nothing remembers the levels. The historical
  loader refuses a storage-only offering rather than report a month as zero.
- A pass that cannot read the buckets reports nothing for storage and keeps the
  checkpoint, so the next pass bills the gap (up to the cap).
- The root user's own statistics (`GET /admin/user?stats=true`) are not used: RGW
  refreshes them once a day by default (`rgw_user_quota_sync_interval`), so they
  read 0 or stale.

**Traffic and requests come from the RGW usage log.**
- `GET /admin/usage` totals for the billing period: bytes sent to clients, bytes
  received from them, and operations. The gateway logs them per bucket owner and
  hour: the account in account mode, whichever of its users made the request, and
  the user otherwise. Deleted buckets keep their traffic.
- The log is kept by the cluster, so every pass reports the month's absolute
  total, and a past month is read again for the historical loader.
- Prerequisites on the cluster: `rgw_enable_usage_log = true` on every gateway,
  and the `usage=read` cap on the agent's admin user
  (`radosgw-admin caps add --uid <admin> --caps "usage=read"`); without the cap the
  agent logs an error naming it and reports the other components.
- Ceph ships with the usage log off, and an unwritten log reads like tenants that
  sent nothing. Zeros are still reported, since an empty log is not a failed read.
  But when every resource reads zero, the agent checks the last day of the log
  across every owner (at most hourly, and not while the log has been seen written
  that day) and logs a warning naming `rgw_enable_usage_log` when it is empty. A
  past month that every resource read as zero is checked the same way, once; the
  historical loader refuses such a month rather than write never-measured zeros.
- In the reporting passes, a resource's traffic is read from its creation hour, so
  a uid freed by a deleted resource and reused by a new one does not carry the old
  one's traffic over, and the new resource's RGW account is looked up afresh. The
  historical loader does not pass the resource, so it reads whole months.
- The log is kept per UTC hour. With a reporting timezone whose offset is not a
  whole hour, the hour that straddles a month boundary is counted wholly in one
  month.
- Each pass reads only what is new. The agent keeps, in memory, the totals of the
  hours that ended over an hour ago (a gateway flushes its entries every 30 s), and
  reads the hours settled since the previous pass plus the last hour or two. A
  finished month is read once it has settled, then kept. A restart costs one full
  read of the month, never a wrong figure.
- The log grows until it is trimmed (`radosgw-admin usage trim`), and the agent
  never trims it. It holds one entry per owner, bucket and hour, spread over
  `rgw_usage_max_shards` (32) objects, so a month adds about
  `720 × buckets / 32` entries to each object: 112,500 for 1,000 resources of five
  buckets. Ceph reports `LARGE_OMAP_OBJECTS` above 200,000 entries per object
  (`osd_deep_scrub_large_omap_object_key_threshold`), so at that size trim every
  month once its invoices are final, keeping the previous month for the historical
  loader (`radosgw-admin usage trim --end-date <first day of last month>`).
- IAM API calls in account mode are logged too, with an empty bucket name, and
  count as operations.

### Reporting with croit's statistics

Users are managed through RGW (including
[account mode](#account-mode-an-rgw-account-per-resource)) while usage comes from
croit's statistics. `croit_usage` reads croit with its own address and credentials
in `croit_reporting`, kept apart from the RGW admin credentials:

```yaml
    reporting_backend: "croit_usage"
    backend_settings:
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

**Every tenanted resource is metered per bucket** -- every resource created with
a tenant of its own, and every account. Only older untenanted users still read
croit's per-user graph.
- croit's per-user graph has nothing for an RGW account (the account owns the
  buckets, and its users hold no storage of their own), and refuses a tenanted
  user's name.
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
  to the deletion go unbilled. Since every new resource is metered per bucket,
  set it.
- An account with no bucket known this period is not reported at all, rather than
  reported as zero, because a zero would overwrite the period's accrued total.

**New buckets.** A bucket croit has not sampled yet adds nothing until it has,
which takes a few minutes after its first write.

### Where the number comes from

`GET /api/stats?graph=s3-user-data&template-s3-user-name=<uid>` returns croit's
per-user storage series as `{"t": unix seconds, "v": bytes}` datapoints at 180 s
native resolution; `graph=s3-bucket-data&template-s3-bucket-name=<tenant>_<bucket>`
the same per bucket, summed over a resource's buckets.

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
  "acme_research_backups_1a2b3c4d": {
    "TOTAL_ACCOUNT_USAGE": {
      "s3_storage": 3100.0
    }
  }
}
```

Values are plain numbers already in Waldur units, with `unit_factor` applied —
not `{"usage": n}` wrappers.

## Resource Metadata

Each resource publishes the connection keys below, plus `user_info` (`uid`,
display `name`, `suspended`), a storage summary and its quotas. The ownership
stamp, placement and storage class are left out: they describe the provider, not
the resource.

### S3 Connection

Credentials are **not** in resource metadata. Each resource owns two S3 access keys, stored
encrypted in Waldur and revealed or rotated per key from the portal (see
`waldur-mastermind/docs/resource-api-keys.md`). Metadata carries only the non-secret
connection info, and the S3 endpoint is also published as a resource access endpoint:

```json
{
  "s3_endpoint": "https://s3.example.com",
  "s3_region": "default",
  "s3_user": "acme_research_backups_1a2b3c4d$root",
  "s3_tenant": "acme_research_backups_1a2b3c4d",
  "s3_account_id": "RGW23034912253664941"
}
```

`s3_account_id` is present in account mode only. A resource from before one tenant
per resource has its stored uid as `s3_user`, and an empty `s3_tenant` when it was
untenanted.

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

- *A key Waldur never issued.* Users are created with `generate-key=False`, so RGW
  mints none of its own. An existing uid is adopted with its keys left in place at
  provisioning time, but they are not in Waldur's known set, so the resource's first
  rotation prunes them.
- *A rotation whose reply to Waldur is lost* has already replaced the access key at the
  backend, so re-issuing it rotates from a client_id RGW no longer has and would leave
  the intermediate key live and invisible. Each rotation therefore receives the resource's
  known client_ids and drops anything outside that set. If Waldur cannot be listed the set
  is `None` — unknown, so nothing is pruned; treating it as empty would delete every
  credential the resource has.

**Known limitation:** Admin Ops takes a key's secret as a query parameter
(`PUT /admin/user?key&secret-key=…`), so it can appear in the gateway's access log
and in that of any proxy in front of it. The agent scrubs it from its own logs and
error messages.

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
      "max_size_bytes": 100000000000,
      "max_objects": 10000
    },
    "user_quota": {
      "enabled": true,
      "max_size_bytes": 100000000000,
      "max_objects": 10000
    }
  }
}
```

In account mode `user_quota` is replaced by `account_quota`, the account's.

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

- **User quota** (`PUT /admin/user?quota&quota-type=user`): the aggregate cap across
  everything the user owns. This is the one that bounds a tenant -- the bucket count
  is not capped, so a per-bucket quota alone bounds nothing
- **Bucket quota** (`quota-type=bucket`): a per-bucket guard
- `max-size` in bytes (`max_storage_limit` × `unit_factor`), `max-objects` as a count
- In account mode both are set on the account instead

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
            name='Example Storage Provider',
            defaults={'abbreviation': 'EXAMPLE', 'native_name': 'Example Storage Provider'}
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
                               'Each resource is an S3 tenant of its own with configurable safety limits.',
                'state': OfferingStates.ACTIVE,
                'billable': True,
                'plugin_options': {
                    'backend_type': 'ceph_s3',
                    'service_provider_can_create_offering_user': False,
                    'auto_create_admin_user': False,
                    # A ceiling change becomes an UPDATE order the agent applies,
                    # rather than a value Waldur records and RGW never sees.
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
                'article_code': 'S3_STORAGE',
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
- **`CephS3AccountNotFoundError`**, **`CephS3AccountExistsError`**: the same for
  RGW accounts
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

### Tenant name too long

The order fails with "not a valid RGW tenant name". The rendered tenant must fit in
64 characters: cut the unbounded fields in `tenant_template`, e.g.
`{resource_name:.20}`. See [Tenants and backend ids](#tenants-and-backend-ids).

### Debug Logging

Use standard Python logging configuration or waldur-site-agent logging settings to enable debug output for the plugin modules:

- `waldur_site_agent_ceph_s3.clients.radosgw` - RadosGW admin API interactions
- `waldur_site_agent_ceph_s3.clients.croit` - croit statistics, for `croit_usage`
- `waldur_site_agent_ceph_s3.clients.iam` - account-mode IAM clean-up at termination
- `waldur_site_agent_ceph_s3.backend` - Backend operations
- `waldur_site_agent_ceph_s3.reporting` - `croit_usage`
- `waldur_site_agent_ceph_s3.rgw_reporting` - `rgw_usage`

Set the top-level `log_level: DEBUG` in the agent configuration.

## Resource Lifecycle

1. **Order Creation**: User submits order with `max_storage_limit` and `max_object_limit`
2. **User Creation**: Plugin renders the resource's tenant and creates its `root` user there;
   the tenant becomes the backend id
3. **Quota Application**: ceilings applied as user and bucket quotas
4. **Key Provisioning**: Two access keys minted, applied, and reported to Waldur encrypted;
   RGW mints none of its own
5. **Usage Tracking**: storage billed in GB-days per billing period, integrated from
   croit's statistics (`croit_usage`) or accrued from bucket sizes (`rgw_usage`)
6. **Ceiling Updates**: editing `max_storage_limit` or `max_object_limit` on the
   resource creates an UPDATE order carrying `new_options`, when the offering has both
   `resource_options` and `plugin_options.create_orders_on_resource_option_change`.
   The agent re-applies both quotas from the order attributes, overlaid by the current
   options, overlaid by the new ones, before Waldur records the change. A ceiling set
   to `0` is lifted: it is sent as `-1`, never as `0` (RadosGW reads 0 as a cap of
   nothing) and never left out (RadosGW keeps a left-out limit).
   Waldur refuses `null` for an integer option, so `0` is how a consumer clears one.
   Without `create_orders_on_resource_option_change`, Waldur stores an edit directly
   and the quota is not touched
7. **Resource Deletion**: the `root` user (in account mode, the account and all its
   users) is removed — but only once it owns no
   buckets. RGW refuses to delete a user that still holds data (the agent never
   purges), and in account mode the agent checks before deleting anyone, so
   termination of a tenant with data fails until the buckets are emptied and removed. The failure reason
   reaches the portal's order history and resource activity log.
