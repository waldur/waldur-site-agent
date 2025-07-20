# CSCS HPC Storage Backend

A Waldur Site Agent backend plugin for managing CSCS HPC Storage systems. This backend generates JSON
files for storage resource provisioning instead of directly interfacing with storage systems.

## Overview

The CSCS HPC Storage backend is designed as a drop-in replacement for existing CSCS storage integrations.
It generates two types of JSON files that can be consumed by external web servers or storage management systems:

1. **All storage resources**: `YYYY-MM-DD-HH-MM-all.json` - Contains all storage resources for the offering
2. **Specific orders**: `YYYY-MM-DD-HH-MM-{order_type}_{order_uuid}.json` - Contains only the resources from specific orders

## Features

- **File-based output**: Generates JSON files instead of direct system integration
- **Hierarchical storage structure**: Maps Waldur organization → customer → project to storage
  tenant → customer → project
- **Configurable quotas**: Automatic inode quota calculation based on storage size
- **Mock data support**: Development/testing mode with generated target item data
- **Flexible configuration**: Customizable file system types, output directories, and quota coefficients

## Configuration

### Backend Settings

```yaml
backend_settings:
  output_directory: "cscs-storage-orders/"     # Output directory for JSON files
  storage_file_system: "lustre"               # Storage file system type
  inode_soft_coefficient: 1.33                # Multiplier for soft inode limits
  inode_hard_coefficient: 2.0                 # Multiplier for hard inode limits
  use_mock_target_items: false                # Enable mock data for development
```

### Backend Components

```yaml
backend_components:
  storage:
    measured_unit: "TB"                       # Storage unit (terabytes)
    accounting_type: "limit"                  # Accounting type for quotas
    label: "Storage"                          # Display label in Waldur
    unit_factor: 1                           # Conversion factor (TB to TB)
```

## Data Mapping

### Waldur to Storage Hierarchy

- **Waldur Organization** → **Storage Tenant** (customer_slug)
- **Waldur Project** → **Storage Customer** (project_slug)
- **Waldur Resource** → **Storage Project** (resource_slug)

### Mount Point Generation

Mount points follow the pattern: `/{storage_system}/store/{tenant}/{customer}/{project}`

Example: `/lustre/store/university/physics-dept/climate-sim`

### Resource Attributes

The backend extracts the following from Waldur resource attributes:

- `permissions`: Octal permissions (e.g., "2770")
- `storage_data_type`: Data type classification (e.g., "store", "scratch", "archive")
