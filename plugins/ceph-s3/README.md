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
| GB-day metering | yes, from croit's series | yes, from a series you collect |

The flavours differ only inside the client. Everything above it — key lifecycle,
quota mapping, metadata, Waldur plumbing — is shared.

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
      default_tenant: ""

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
    reporting_backend: "prometheus_usage"      # not croit_usage; see Usage Reporting

    backend_settings:
      flavour: "radosgw"
      s3_endpoint: "https://s3.example.com"    # Admin Ops hangs off this host
      admin_access_key: "ADMINACCESSKEY000000"
      admin_secret_key: "..."
      s3_region: "us-east-1"                   # must match what clients sign with
      admin_path: "admin"                      # only if rgw_admin_entry was renamed
      verify_ssl: true
      # Where the storage series is read back from. Anything speaking the
      # Prometheus HTTP API; something else has to be putting the readings in.
      prometheus_url: "https://victoriametrics.example.com"
```

`api_url`, `token`, `username` and `password` belong to the croit flavour and are
**rejected** here rather than ignored — an offering carrying both sets is a
half-finished migration, and silently picking one would authenticate somewhere the
operator did not intend.

#### Two behaviours that differ from croit

- **Metering needs a collector you run.** Admin Ops exposes no per-user
  stored-bytes series to integrate: `/admin/usage` is bandwidth and operation
  counts, and bucket stats are a point-in-time size. `prometheus_usage` therefore
  reads the curve out of a time series database rather than from the cluster, and
  something outside the agent has to be filling it — a cron job pushing
  `radosgw-admin bucket stats` to `/api/v1/import/prometheus`, or a RadosGW
  exporter behind vmagent. **That collector is now part of the billing path**: a
  gap in the series is a gap in the invoice, and nothing in the agent will notice
  it stopped.
- **Quota enforcement overshoots.** RGW syncs bucket statistics into the
  user-quota check periodically rather than on every write. Measured on squid: a
  6 MiB ceiling against 5 MiB already stored accepted a further 7 MiB before
  answering `QuotaExceeded`. The ceiling bounds the invoice approximately, not
  exactly.

### Configuration Options

#### Backend Settings

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
- **`username`** (croit only): API username (alternative to token)
- **`password`** (croit only): API password (alternative to token)
- **`admin_access_key`** (radosgw only, required): access key of the RGW user
  holding `users=*;buckets=read`
- **`admin_secret_key`** (radosgw only, required): its secret
- **`admin_path`** (radosgw only, default: `"admin"`): the value of
  `rgw_admin_entry`, if the operator renamed it
- **`prometheus_url`** (required by `prometheus_usage`): base URL of a
  Prometheus-compatible database — VictoriaMetrics, Mimir, Thanos, Prometheus
  itself. Only `/api/v1/query_range` is used
- **`prometheus_metric`** (optional, default: `"ceph_rgw_user_stored_bytes"`):
  the gauge carrying bytes held. Per-user is the safer shape — it can carry an
  explicit zero for a user that owns no buckets, which a per-bucket metric cannot.
  A per-bucket gauge works too, since the query sums by owner either way; set
  `radosgw_usage_bucket_bytes` for the community RadosGW exporter
- **`prometheus_owner_label`** (optional, default: `"owner"`): the label carrying
  the S3 uid. It has to match the resource's `backend_id`, which is what the
  report is keyed by
- **`prometheus_step`** (optional, default: `"30m"`): range query resolution.
  Match it to the collector's interval; a month at 30 m is 1488 points, and a
  range query caps out around 11k
- **`prometheus_lookback`** (optional, default: the step): how far back each
  evaluation point looks for a reading. A bare selector carries only the
  database's 5-minute staleness window, so a collector writing less often than
  that is seen only when its samples happen to land just before a grid point.
  Raise it above the step if the collector is slower than the resolution you want
- **`prometheus_timeout`** (optional, default: `30`), **`prometheus_verify_ssl`**
  (optional, default: `true`), and **`prometheus_username`** /
  **`prometheus_password`** or **`prometheus_token`** for authentication
- **`verify_ssl`** (optional, default: `true`): Enable/disable SSL certificate verification
- **`timeout`** (optional, default: `30`): Request timeout in seconds
- **`allocation_prefix`** (optional, default: `"waldur-"`): namespace every uid this
  agent creates is built under — the only setting that changes a provisioned uid. A
  core setting, but the core defaults it to `""`, which this plugin overrides: an
  empty namespace puts a consumer-chosen resource name straight into the cluster's
  global one. See Username Generation
- **`default_tenant`** (croit only, optional): Default RadosGW tenant. Rejected on
  the radosgw flavour: a tenanted uid is `tenant$uid`, and the client's path
  validator refuses `$`
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

**Provisioned resources are named by the order processor, not by this plugin.**
`_get_resource_backend_id()` in the core produces
`f"{allocation_prefix}{resource_slug}".lower()`, and Waldur caps every slug at 10
characters (`SLUG_NAME_LIMIT`), appending `-2`, `-3`, … on collision. So a resource
named `cust-0-proj-0-ceph-s3-of` becomes the uid `cust-0-pro`.

**Format**: `{allocation_prefix}{resource_slug}` — **Example**: `cust-0-pro`

This is the only naming scheme the plugin has. A second one — `user_prefix`,
`slug_separator`, `max_username_length` and a `create_resource()` that read them —
existed alongside it and was never reached, since the order processor calls
`create_resource_with_id()`. It has been removed; an offering still carrying those
three keys is unaffected, they are simply ignored.

## Usage Reporting

**Metering needs its own backend, and which one depends on the flavour.** Set

```yaml
    reporting_backend: "croit_usage"       # croit flavour
    reporting_backend: "prometheus_usage"  # radosgw flavour
```

Both integrate the same curve the same way and differ only in where the readings
come from: croit holds a per-user series already, while RadosGW holds none, so
`prometheus_usage` reads one out of a time series database that somebody else
fills (see
[Metering needs a collector you run](#two-behaviours-that-differ-from-croit)).

`croit_usage` refuses to start on a radosgw offering, having nothing to read.
`prometheus_usage` imposes no such restriction — where the bytes came from is a
property of whatever fills the database — but on croit prefer `croit_usage`: its
series is native, sampled at 180 s, and covers periods predating any collector
you deploy.

Because that split is a fact about which class is wired in, it lives in the
configuration rather than as a method that works on one flavour and fails on the
other.

Leaving `reporting_backend` at `ceph_s3` reports **nothing** — deliberately, not
zeros. A zero is a legitimate usage value and would overwrite the period's
accrued total; silence leaves the last good figure in place.

Storage is billed in **GB-days** — the area under the storage curve over the
billing period, not a reading taken at report time. Object counts are not billed.

Usage is metered **per resource**, not per key: the series is the S3 user's, so
the resource's two access keys never enter the calculation and rotating one
cannot change a bill.

### Where the number comes from, on croit

`GET /api/stats?graph=s3-user-data&template-s3-user-name=<uid>` returns croit's
per-user storage series as `{"t": unix seconds, "v": bytes}` datapoints at 180 s
native resolution. Each pass asks for the period so far and integrates:

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

### Where the number comes from, on radosgw

A collector outside the agent records bytes held per bucket, labelled by the S3
uid. The agent then asks the database for the whole period in one range query:

```promql
sum by (owner) (last_over_time(ceph_rgw_user_stored_bytes[30m]))
```

Summing in the query is what makes a single request answer for every resource,
and it means buckets appearing and disappearing mid-period need no handling — the
sum follows them. `last_over_time` over one collection interval is what makes the
reading visible at all: a range query evaluates on a grid of `step`, and a bare
selector would only carry the database's 5-minute staleness window, so whether a
month billed correctly or not at all would come down to the phase between the
collector's cron and the grid. The reply's `[timestamp, value]` pairs are the same
rectangles croit's datapoints are, integrated by the same code.

Prometheus renders a stale or absent reading as `NaN`, which is handed to the
integrator as a null and carried the way a null in croit's series is: billing the
gap at zero would turn a collector outage into a silent discount.

Two things follow from the series being somebody else's:

- **The collector is part of the billing path.** Nothing in the agent can tell a
  tenant that deleted everything from a collector that stopped — both are simply
  absent from the reply. Absence reports nothing and leaves the accrued total
  alone, which is the safe direction, but it means **a tenant's drop to zero only
  reaches the invoice if the collector publishes an explicit zero for it**. Emit
  one for every S3 user, not only for users that currently own buckets.
- **Retention has to cover re-reporting.** `get_usage_report_for_period` reads a
  past month straight out of the database, which is what makes the tail of a month
  billable after rollover. That needs roughly 90 days retained. Prometheus itself
  defaults to 15 and documents that it is unsuitable where full accuracy is
  required; VictoriaMetrics is the better target for invoice data.

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
                            'help_text': 'Maximum number of objects (optional)',
                            'required': False,
                            'min': 0,
                        }
                    }
                },
                # resource_options is deliberately not set: it would surface an
                # "update options" action the agent cannot honour, since it does
                # not act on new_options and applies quotas only at creation.
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

- **Type**: `SITE_AGENT_OFFERING` ("Marketplace.Slurm")
- **Components**: `s3_storage` only (usage-based, billed in GB-days)
- **Options**: `max_storage_limit` and `max_object_limit` for user input (safety limits)
- **Pricing**: €0.0010 per GB-day for storage; objects are not billed

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

- **`CroitS3AuthenticationError`**: API authentication failures
- **`CroitS3UserNotFoundError`**: User doesn't exist
- **`CroitS3UserExistsError`**: User already exists
- **`CroitS3GraphNotFoundError`**: the statistics graph behind usage reporting is missing
- **`CroitS3APIError`**: General API errors
- **`CroitS3Error`**: Base exception class

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

- `waldur_site_agent_ceph_s3.client` - HTTP API interactions
- `waldur_site_agent_ceph_s3.backend` - Backend operations

## Resource Lifecycle

1. **Order Creation**: User submits order with `max_storage_limit` and `max_object_limit`
2. **User Creation**: Plugin creates the S3 user under the backend id the order processor generated
3. **Quota Application**: ceilings applied as user and bucket quotas
4. **Key Provisioning**: Two access keys minted, applied, and reported to Waldur encrypted;
   RadosGW's auto-generated key is removed
5. **Usage Tracking**: storage integrated into GB-days per billing period
6. **Limit Updates**: **not supported.** The ceiling is an order attribute, fixed at
   creation. Waldur can carry an `update_options` order, but the agent does not act
   on `new_options`, and quotas are applied only on the create path — so changing a
   ceiling today means terminating and re-ordering
7. **Resource Deletion**: the S3 user is removed — but only once it owns no
   buckets. croit refuses to delete a user that still holds data
   (`500 Unable to remove user with buckets.`), so termination of a tenant with
   data fails until the buckets are emptied and removed. The failure reason
   reaches the portal's order history and resource activity log.
