# Croit S3 Storage Plugin for Waldur Site Agent

This plugin provides integration between Waldur Mastermind and Croit S3 storage systems via RadosGW API.
Each marketplace resource automatically creates one S3 user with configurable safety limits.

## Features

- **Automatic S3 User Creation**: One S3 user per marketplace resource with slug-based naming
- **Usage-Based Billing**: Track actual storage and object consumption
- **Safety Quota Enforcement**: Optional bucket quotas based on user-specified limits
- **Usage Reporting**: Real-time storage and object count metrics
- **Credential Management**: S3 access keys exposed via resource metadata
- **Bearer Token Authentication**: Secure API access with configurable SSL verification

## Installation

Add the plugin to your UV workspace:

```bash
cd /path/to/waldur-site-agent
uv add ./plugins/croit-s3
```

## Configuration

### Basic Configuration

```yaml
offerings:
  - name: "Croit S3 Object Storage"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "your_waldur_api_token"
    waldur_offering_uuid: "713c299671a14f5db9723a793291bc78"

    # Event processing settings
    stomp_enabled: true
    websocket_use_tls: false

    # Backend type
    backend_type: "croit_s3"

    # Croit S3-specific backend settings
    backend_settings:
      api_url: "https://192.168.240.34"
      token: "your-bearer-token"
      verify_ssl: false
      user_prefix: "waldur_"
      slug_separator: "_"
      max_username_length: 64
      default_tenant: ""

    # Component mapping
    backend_components:
      s3_storage:
        accounting_type: "usage"
        backend_name: "storage"
        unit_factor: 1073741824  # Convert bytes to GB
        enforce_limits: true
      s3_objects:
        accounting_type: "usage"
        backend_name: "objects"
        enforce_limits: true
```

### Configuration Options

#### Backend Settings

- **`api_url`** (required): Croit API base URL (will be appended with /api)
- **`token`** (optional): Bearer token for API authentication
- **`username`** (optional): API username (alternative to token)
- **`password`** (optional): API password (alternative to token)
- **`verify_ssl`** (optional, default: `true`): Enable/disable SSL certificate verification
- **`timeout`** (optional, default: `30`): Request timeout in seconds
- **`user_prefix`** (optional, default: `"waldur_"`): Prefix for generated usernames
- **`slug_separator`** (optional, default: `"_"`): Separator for slug components
- **`max_username_length`** (optional, default: `64`): Maximum username length
- **`default_tenant`** (optional): Default RadosGW tenant
- **`default_placement`** (optional): Default placement rule
- **`default_storage_class`** (optional): Default storage class

#### Component Types

##### Usage-Based Storage (`s3_storage`)

Tracks actual storage consumption with optional safety quota enforcement:

```yaml
s3_storage:
  accounting_type: "usage"
  backend_name: "storage"
  unit_factor: 1073741824  # Bytes to GB conversion
  enforce_limits: true     # Apply safety limits from resource options as bucket quotas
```

##### Usage-Based Objects (`s3_objects`)

Tracks object count with optional safety quota enforcement:

```yaml
s3_objects:
  accounting_type: "usage"
  backend_name: "objects"
  enforce_limits: true     # Apply safety limits from resource options as object quotas
```

**Note**: The plugin automatically creates one S3 user per marketplace resource. No separate user component is needed.

## Username Generation

Usernames are automatically generated from Waldur resource metadata:

**Format**: `{prefix}{org_slug}_{project_slug}_{resource_uuid_short}`

**Example**: `waldur_myorg_myproject_12345678`

### Slug Cleaning Rules

- Convert to lowercase
- Replace non-alphanumeric characters with underscores
- Remove consecutive underscores
- Truncate if exceeds maximum length
- Preserve prefix and resource UUID

## Usage Reporting

The plugin collects usage metrics for all user buckets:

### Storage Usage

- Sums `usageSum.size` across all user buckets
- Converts bytes to configured units (e.g., GB)
- Reports actual storage consumption

### Object Usage

- Sums `usageSum.numObjects` across all user buckets
- Reports total object count

### Report Format

```json
{
  "waldur_org_proj_12345678": {
    "s3_storage": {"usage": 150},
    "s3_objects": {"usage": 5000}
  }
}
```

## Resource Metadata

Each S3 user resource exposes comprehensive metadata:

### S3 Credentials

```json
{
  "s3_credentials": {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "endpoint": "https://192.168.240.34",
    "region": "default"
  }
}
```

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
    }
  }
}
```

## Safety Quota Enforcement

When `enforce_limits: true` is set for usage-based components, the plugin automatically applies safety limits from
resource options as RadosGW bucket quotas:

1. **Create Resource**: Apply initial quotas based on user-specified safety limits (storage_limit, object_limit)
2. **Prevent Overages**: Quotas act as safety nets to prevent unexpected usage charges
3. **Monitor Usage**: Include quota utilization in usage reports

### Quota Types

- **Storage Quota**: `maxSize` in bytes (converted from storage_limit in GB)
- **Object Quota**: `maxObjects` as integer count (from object_limit)

### How Safety Limits Work

1. **User Configuration**: Users set `storage_limit` and `object_limit` via Waldur marketplace form
2. **Resource Options**: Waldur passes these as resource attributes to the site agent
3. **Quota Application**: Plugin applies these as bucket quotas during S3 user creation
4. **Usage Billing**: Actual consumption is tracked and billed separately from quotas

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

def create_croit_s3_offering():
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
            defaults={'description': 'Croit S3 object storage services'}
        )

        # Create offering
        offering, created = Offering.objects.get_or_create(
            name='Croit S3 Object Storage',
            defaults={
                'type': SITE_AGENT_OFFERING,
                'category': category,
                'customer': service_provider.customer,
                'description': 'S3-compatible object storage with usage-based billing. '
                               'Each resource provides one S3 user account with configurable safety limits.',
                'state': OfferingStates.ACTIVE,
                'billable': True,
                'plugin_options': {
                    'backend_type': 'croit_s3',
                    'create_orders_on_resource_option_change': True,
                    'service_provider_can_create_offering_user': False,
                    'auto_create_admin_user': False,
                },
                'options': {
                    'order': ['storage_limit', 'object_limit'],
                    'options': {
                        'storage_limit': {
                            'type': 'integer',
                            'label': 'Storage Limit (GB)',
                            'help_text': 'Maximum storage capacity in gigabytes (safety limit)',
                            'required': True,
                            'default': 100,
                            'min': 1,
                            'max': 10000,
                        },
                        'object_limit': {
                            'type': 'integer',
                            'label': 'Object Count Limit',
                            'help_text': 'Maximum number of objects that can be stored (safety limit)',
                            'required': True,
                            'default': 10000,
                            'min': 100,
                            'max': 10000000,
                        }
                    }
                },
                'resource_options': {
                    'order': ['storage_limit', 'object_limit'],
                    'options': {
                        'storage_limit': {
                            'type': 'integer',
                            'label': 'Storage Limit (GB)',
                            'help_text': 'Storage limit to enforce as bucket quota',
                            'required': True,
                        },
                        'object_limit': {
                            'type': 'integer',
                            'label': 'Object Count Limit',
                            'help_text': 'Object limit to enforce as bucket quota',
                            'required': True,
                        }
                    }
                }
            }
        )

        # Create components
        storage_component, _ = OfferingComponent.objects.get_or_create(
            offering=offering,
            type='s3_storage',
            defaults={
                'name': 'S3 Storage',
                'description': 'Object storage capacity in GB',
                'billing_type': BillingTypes.USAGE,
                'measured_unit': 'GB',
                'article_code': 'CROIT_S3_STORAGE',
                'default_limit': 100,
            }
        )

        objects_component, _ = OfferingComponent.objects.get_or_create(
            offering=offering,
            type='s3_objects',
            defaults={
                'name': 'S3 Objects',
                'description': 'Number of stored objects',
                'billing_type': BillingTypes.USAGE,
                'measured_unit': 'objects',
                'article_code': 'CROIT_S3_OBJECTS',
                'default_limit': 10000,
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
            defaults={'price': Decimal('0.02'), 'amount': 1}  # €0.02/GB/month
        )

        PlanComponent.objects.get_or_create(
            plan=plan,
            component=objects_component,
            defaults={'price': Decimal('0.0001'), 'amount': 1}  # €0.0001/object/month
        )

        print(f'✓ Croit S3 offering created: {offering.uuid}')
        print(f'  Add this UUID to your site agent config')
        return offering.uuid

create_croit_s3_offering()
"

```

**Alternative**: Save the above code as `setup_croit_s3_offering.py` and run:

```bash
DJANGO_SETTINGS_MODULE=waldur_core.server.settings uv run python setup_croit_s3_offering.py
```

### Offering Configuration

The created Waldur offering will have:

- **Type**: `SITE_AGENT_OFFERING` ("Marketplace.Slurm")
- **Components**: `s3_storage` and `s3_objects` (both usage-based billing)
- **Options**: `storage_limit` and `object_limit` for user input (safety limits)
- **Plugin Options**: `create_orders_on_resource_option_change: true`
- **Pricing**: €0.02/GB/month for storage, €0.0001/object/month for objects

### Order Payload Example

```json
{
  "offering": "http://localhost:8000/api/marketplace-public-offerings/{offering_uuid}/",
  "project": "http://localhost:8000/api/projects/{project_uuid}/",
  "plan": "http://localhost:8000/api/marketplace-public-offerings/{offering_uuid}/plans/{plan_uuid}/",
  "attributes": {
    "storage_limit": 100,
    "object_limit": 10000
  },
  "name": "my-s3-storage",
  "description": "S3 storage for my application",
  "accepting_terms_of_service": true
}
```

## Testing

Run the test suite:

```bash
cd plugins/croit-s3
uv run pytest tests/ -v
```

## Development

### Adding New Components

1. Define component in site agent configuration:

```yaml
my_custom_component:
  accounting_type: "usage"
  backend_name: "custom_metric"
  unit_factor: 1
  enforce_limits: false
```

- Add usage collection logic in `_get_usage_report()`
- Add safety limit handling in `_apply_bucket_quotas()` if needed
- Add corresponding field in Waldur offering options for user input

### Error Handling

The plugin includes comprehensive error handling:

- **`CroitS3AuthenticationError`**: API authentication failures
- **`CroitS3UserNotFoundError`**: User doesn't exist
- **`CroitS3UserExistsError`**: User already exists
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

```yaml
backend_settings:
  max_username_length: 32  # Adjust for backend constraints
  user_prefix: "w_"        # Shorten prefix
```

### Debug Logging

Use standard Python logging configuration or waldur-site-agent logging settings to enable debug output for the plugin modules:

- `waldur_site_agent_croit_s3.client` - HTTP API interactions
- `waldur_site_agent_croit_s3.backend` - Backend operations

## Resource Lifecycle

1. **Order Creation**: User submits order with `storage_limit` and `object_limit`
2. **User Creation**: Plugin creates S3 user with slug-based username
3. **Quota Application**: Safety limits applied as bucket quotas
4. **Credential Exposure**: Access keys returned via resource metadata
5. **Usage Tracking**: Real-time storage and object consumption reporting
6. **Limit Updates**: Users can modify safety limits (creates new orders)
7. **Resource Deletion**: S3 user and all buckets are removed
