"""Shared fixtures for OpenNebula plugin tests."""

import pytest


@pytest.fixture()
def backend_settings():
    return {
        "api_url": "http://localhost:2633/RPC2",
        "credentials": "oneadmin:testpass",
        "zone_id": 0,
    }


@pytest.fixture()
def backend_components():
    return {
        "cpu": {
            "limit": 100,
            "measured_unit": "cores",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "CPU Cores",
        },
        "ram": {
            "limit": 1024,
            "measured_unit": "MB",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "RAM",
        },
        "storage": {
            "limit": 10240,
            "measured_unit": "MB",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "Storage",
        },
    }


@pytest.fixture()
def vm_backend_settings():
    return {
        "api_url": "http://localhost:2633/RPC2",
        "credentials": "oneadmin:testpass",
        "zone_id": 0,
        "resource_type": "vm",
    }


@pytest.fixture()
def vm_backend_components():
    return {
        "vcpu": {
            "limit": 16,
            "measured_unit": "cores",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "vCPU",
        },
        "vm_ram": {
            "limit": 32768,
            "measured_unit": "MB",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "RAM",
        },
        "vm_disk": {
            "limit": 102400,
            "measured_unit": "MB",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "Disk",
        },
    }
