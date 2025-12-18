# Configuration Validation with Pydantic

The Waldur Site Agent uses Pydantic for robust YAML configuration validation, providing type safety,
clear error messages, and extensible plugin-specific validation.

## Overview

The validation system consists of two layers:

1. **Core Validation**: Universal fields validated by core Pydantic models
2. **Plugin Validation**: Plugin-specific fields validated by plugin-provided schemas

## Core Configuration Validation

### Basic Structure

All configurations are validated using Pydantic models with enum-based validation:

```yaml
sentry_dsn: "https://key@o123.ingest.sentry.io/456"  # URL validation
timezone: "UTC"
offerings:
  - name: "My SLURM Cluster"
    waldur_api_url: "https://waldur.example.com/api/"  # URL validation + auto-normalization
    waldur_api_token: "your_token_here"
    waldur_offering_uuid: "uuid-here"
    backend_type: "slurm"  # Auto-lowercased

    backend_components:
      cpu:
        measured_unit: "k-Hours"        # Required string
        accounting_type: "usage"        # Enum: "usage" or "limit"
        label: "CPU"                    # Required string
        unit_factor: 60000              # Optional float
        limit: 1000                     # Optional float
```

### Core Validation Features

**Automatic Validation:**

- **Required Fields**: `name`, `waldur_api_url`, `waldur_api_token`, `waldur_offering_uuid`, `backend_type`
- **URL Validation**: `waldur_api_url` must be valid HTTP/HTTPS URL (auto-adds trailing slash)
- **Enum Validation**: `accounting_type` must be "usage" or "limit"
- **Type Conversion**: `backend_type` automatically lowercased

**Optional URL Validation:**

- **Sentry DSN**: Must be valid URL when provided (empty string → `None`)

### AccountingType Enum

The `accounting_type` field uses a validated enum:

```python
from waldur_site_agent.common.structures import AccountingType

# Valid values
AccountingType.USAGE   # "usage"
AccountingType.LIMIT   # "limit"
```

**Benefits:**

- IDE autocomplete
- Compile-time type checking
- Clear validation errors
- No custom validator code needed

## Plugin-Specific Validation

### How Plugin Schemas Work

Plugins can provide their own Pydantic schemas to validate plugin-specific configuration fields:

1. **Plugin defines schema**: Creates Pydantic models for their specific fields
2. **Entry point registration**: Registers schema via `pyproject.toml`
3. **Automatic discovery**: Core discovers and applies plugin validation
4. **Graceful fallback**: Invalid plugin fields warn but don't break config

### Creating Plugin Schemas

#### Step 1: Create Schema File

Create `schemas.py` in your plugin:

```python
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import ConfigDict, Field, field_validator

from waldur_site_agent.common.plugin_schemas import (
    PluginBackendSettingsSchema,
    PluginComponentSchema,
)


class MyPeriodType(Enum):
    """Period types for my plugin."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class MyComponentSchema(PluginComponentSchema):
    """My plugin-specific component validation."""

    model_config = ConfigDict(extra="allow")  # Allow core fields

    # Plugin-specific fields
    my_period_type: Optional[MyPeriodType] = Field(
        default=None,
        description="Period type for my plugin features"
    )
    my_custom_ratio: Optional[float] = Field(
        default=None,
        description="Custom ratio (0.0-1.0)"
    )

    @field_validator("my_custom_ratio")
    @classmethod
    def validate_ratio(cls, v: Optional[float]) -> Optional[float]:
        """Validate custom ratio is between 0.0 and 1.0."""
        if v is not None and (v < 0.0 or v > 1.0):
            msg = "my_custom_ratio must be between 0.0 and 1.0"
            raise ValueError(msg)
        return v
```

#### Step 2: Register Entry Points

Add to your plugin's `pyproject.toml`:

```toml
[project.entry-points."waldur_site_agent.component_schemas"]
my-plugin = "waldur_site_agent_my_plugin.schemas:MyComponentSchema"

[project.entry-points."waldur_site_agent.backend_settings_schemas"]
my-plugin = "waldur_site_agent_my_plugin.schemas:MyBackendSettingsSchema"
```

#### Step 3: Use in Configuration

Your plugin-specific fields are now validated:

```yaml
offerings:
  - name: "My Plugin Offering"
    waldur_api_url: "https://waldur.example.com/api/"
    waldur_api_token: "token"
    waldur_offering_uuid: "uuid"
    backend_type: "my-plugin"

    backend_components:
      cpu:
        # Core fields (validated by BackendComponent)
        measured_unit: "Hours"
        accounting_type: "usage"  # AccountingType enum
        label: "CPU"

        # Plugin fields (validated by MyComponentSchema)
        my_period_type: "quarterly"  # MyPeriodType enum
        my_custom_ratio: 0.25        # 0.0-1.0 validation
```

## Best Practices

### Use ConfigDict for Python 3.9+ Compatibility

**✅ Correct approach:**

```python
from pydantic import ConfigDict

class MySchema(PluginComponentSchema):
    model_config = ConfigDict(extra="allow")  # Works on all Python versions
```

**❌ Avoid:**

```python
from typing import ClassVar

class MySchema(PluginComponentSchema):
    model_config: ClassVar = {"extra": "allow"}  # Fails on Python 3.9
```

### Prefer Enums Over String Validation

**✅ Better approach:**

```python
class BackendType(Enum):
    SLURM = "slurm"
    MUP = "mup"

backend_type: Optional[BackendType] = Field(default=None)
```

**❌ Avoid:**

```python
@field_validator("backend_type")
@classmethod
def validate_backend_type(cls, v):
    if v not in {"slurm", "mup"}:
        raise ValueError("Invalid backend type")
    return v
```

## SLURM Plugin Example

The SLURM plugin demonstrates real-world plugin validation:

```python
class PeriodType(Enum):
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"

class SlurmComponentSchema(PluginComponentSchema):
    model_config = ConfigDict(extra="allow")

    period_type: Optional[PeriodType] = Field(default=None)
    carryover_enabled: Optional[bool] = Field(default=None)
    grace_ratio: Optional[float] = Field(default=None)
```

## Error Handling

### Core Validation Errors (Fatal)

Stop configuration loading with clear error messages:

```text
ValidationError: 2 validation errors for Offering
waldur_api_url
  Value error, waldur_api_url must start with http:// or https://
accounting_type
  Input should be 'usage' or 'limit'
```

### Plugin Validation Errors (Warnings)

Log warnings but continue with configuration loading:

```text
Warning: Plugin schema validation failed for slurm.cpu: 1 validation error
period_type: Input should be 'monthly', 'quarterly' or 'annual'
```

## Benefits

### Type Safety

- **IDE Support**: Full autocomplete for configuration fields
- **Compile-time Checking**: Catch errors before runtime
- **Clear Documentation**: Field descriptions provide inline help

### Runtime Validation

- **Immediate Feedback**: Configuration errors caught at startup
- **Rich Error Messages**: Pydantic provides detailed validation feedback
- **Graceful Degradation**: Plugin validation warns but doesn't break

### Maintainability

- **Enum-Based**: No custom string validation code needed
- **Extensible**: Plugins add validation without core changes
- **Evidence-Based**: Schemas based on actual plugin requirements
- **Future-Proof**: Easy to add new validation rules

This validation system provides robust configuration management while maintaining clean separation
between core and plugin concerns.
