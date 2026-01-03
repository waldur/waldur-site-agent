# Order Templates

This directory contains Jinja2 templates for generating test orders for different scenarios.

## Template Structure

Templates are organized by order type:

- `create/` - Templates for CREATE orders
- `update/` - Templates for UPDATE orders
- `terminate/` - Templates for TERMINATE orders

## Available Templates

### CREATE Templates

- **`create/basic.json`** - Minimal CREATE order with basic fields
- **`create/with-limits.json`** - CREATE order with resource limits (cpu, mem, gpu)
- **`create/slurm-full.json`** - Full SLURM allocation with all attributes

### UPDATE Templates

- **`update/limits-only.json`** - Update only resource limits
- **`update/attributes-and-limits.json`** - Update both limits and attributes

### TERMINATE Templates

- **`terminate/basic.json`** - Basic termination order

## Template Variables

Templates use Jinja2 syntax with custom filters:

- `{{ variable_name }}` - Required variable
- `{{ variable_name | default('value') }}` - Optional variable with default
- `{{ uuid4() }}` - Generate random UUID
- `{{ timestamp() }}` - Current timestamp
- `{{ from_json }}` - Parse JSON string
- `{{ to_json }}` - Convert to JSON string

## Common Variables

All templates support these common variables:

- `order_uuid` - Order UUID (auto-generated if not provided)
- `offering_uuid` - Target offering UUID (required)
- `resource_name` - Name of the resource
- `project_slug` - Project slug
- `customer_slug` - Customer slug
- `state` - Order state (defaults to 'EXECUTING')

## Usage Examples

### Basic CREATE Order

```bash
waldur_site_test_order -c config.yaml --template create/basic.json \
  --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \
  --var resource_name=my-test-allocation
```

### CREATE with Custom Limits

```bash
waldur_site_test_order -c config.yaml --template create/with-limits.json \
  --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \
  --var resource_name=gpu-allocation \
  --var cpu_limit=5000 \
  --var mem_limit=8192 \
  --var gpu_limit=4
```

### UPDATE Limits

```bash
waldur_site_test_order -c config.yaml --template update/limits-only.json \
  --var marketplace_resource_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \
  --var resource_name=my-allocation \
  --var new_cpu_limit=10000 \
  --var new_mem_limit=16384
```

### TERMINATE Resource

```bash
waldur_site_test_order -c config.yaml --template terminate/basic.json \
  --var marketplace_resource_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \
  --var resource_name=my-allocation
```

## Creating Custom Templates

To create a custom template:

1. Create a new `.json` file in the appropriate subdirectory
2. Use Jinja2 syntax for variable substitution
3. Include required fields for the order type
4. Test with `--validate-only` flag first

Example custom template:

```json
{
  "uuid": "{{ '' | uuid4() }}",
  "type": "Create",
  "resource_name": "{{ resource_name }}",
  "offering_uuid": "{{ offering_uuid }}",
  "marketplace_resource_uuid": "{{ '' | uuid4() }}",
  "state": "executing",
  "attributes": {
    "custom_field": "{{ custom_value | default('default') }}"
  }
}
```

## Template Validation

Validate templates before use:

```bash
# Validate template syntax (use proper UUID format)
waldur_site_test_order --template create/basic.json --validate-only \
  --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c

# Generate order without execution
waldur_site_test_order --template create/basic.json --generate-only \
  --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c -o generated_order.json
```

## UUID Format Requirements

All UUID fields require proper UUID format (e.g., `d629d5e4-5567-425d-a9cd-bdc1af67b32c`). You can generate UUIDs using:
- Online tools like <https://www.uuidgenerator.net/>
- Command line: `uuidgen` (macOS/Linux) or `python -c "import uuid; print(uuid.uuid4())"`
