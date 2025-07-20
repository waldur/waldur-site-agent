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

## Architecture

The CSCS HPC Storage backend follows a dual-path architecture that separates individual order processing from bulk
resource synchronization:

```mermaid
graph TD
    subgraph "Waldur Site Agent Core"
        OA[Order Processing<br/>order_process mode]
        WM[Waldur Mastermind<br/>API Client]
        CSB[CSCS Backend<br/>create_resource()]
    end

    subgraph "CSCS HPC Storage Plugin"
        SS[Sync Script<br/>waldur_cscs_storage_sync]
        GOR[generate_order_json()]
        GAR[generate_all_resources_json()]
    end

    subgraph "File System Output"
        OF[Order Files<br/>YYYY-MM-DD-HH-MM-{type}_{uuid}.json]
        AF[All Resources File<br/>YYYY-MM-DD-HH-MM-all.json]
    end

    subgraph "External Systems"
        WS[Web Server<br/>File Consumption]
        SMS[Storage Management<br/>System]
    end

    %% Order Processing Flow
    OA --> WM
    WM --> CSB
    CSB --> GOR
    GOR --> OF

    %% Bulk Sync Flow
    SS --> WM
    WM --> GAR
    GAR --> AF

    %% External Consumption
    OF --> WS
    AF --> WS
    WS --> SMS

    %% Styling
    classDef core fill:#e3f2fd
    classDef plugin fill:#f3e5f5
    classDef output fill:#fff8e1
    classDef external fill:#e8f5e8

    class OA,WM,CSB core
    class SS,GOR,GAR plugin
    class OF,AF output
    class WS,SMS external
```

### Processing Flows

**Individual Order Processing:**
1. Waldur Site Agent processes orders in `order_process` mode
2. Backend `create_resource()` method generates specific order JSON files
3. Files contain single resource data for immediate processing

**Bulk Resource Synchronization:**
1. Separate sync script (`waldur_cscs_storage_sync`) runs independently
2. Fetches all resources from Waldur API for configured offerings
3. Generates comprehensive `all.json` files with complete resource lists
4. Supports dry-run mode and selective offering synchronization

### Usage

**Run sync script for all offerings:**

```bash
uv run waldur_cscs_storage_sync --config /path/to/config.yaml
```

**Run sync script for specific offering:**

```bash
uv run waldur_cscs_storage_sync --config /path/to/config.yaml --offering-uuid <uuid>
```

**Dry-run mode:**

```bash
uv run waldur_cscs_storage_sync --config /path/to/config.yaml --dry-run --verbose
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
