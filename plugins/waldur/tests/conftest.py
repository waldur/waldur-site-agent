"""Shared test fixtures for Waldur federation plugin tests."""

import pytest


@pytest.fixture()
def backend_components_with_conversion():
    """Backend components with target component conversion configured."""
    return {
        "node_hours": {
            "measured_unit": "Hours",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "Node Hours",
            "target_components": {
                "gpu_hours": {"factor": 5.0},
                "storage_gb_hours": {"factor": 10.0},
            },
        },
    }


@pytest.fixture()
def backend_components_passthrough():
    """Backend components in passthrough mode (no target_components)."""
    return {
        "cpu": {
            "measured_unit": "Hours",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "CPU Hours",
        },
        "mem": {
            "measured_unit": "GB",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "Memory GB",
        },
    }


@pytest.fixture()
def backend_components_mixed():
    """Mixed components: some with conversion, some passthrough."""
    return {
        "node_hours": {
            "measured_unit": "Hours",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "Node Hours",
            "target_components": {
                "gpu_hours": {"factor": 5.0},
            },
        },
        "storage": {
            "measured_unit": "GB",
            "unit_factor": 1,
            "accounting_type": "usage",
            "label": "Storage GB",
        },
    }


@pytest.fixture()
def backend_settings():
    """Default backend settings for Waldur federation."""
    return {
        "target_api_url": "https://waldur-b.example.com/api/",
        "target_api_token": "test-token-waldur-b",
        "target_offering_uuid": "offering-uuid-on-waldur-b",
        "target_customer_uuid": "customer-uuid-on-waldur-b",
        "user_match_field": "cuid",
        "order_poll_timeout": 10,
        "order_poll_interval": 1,
        "user_not_found_action": "warn",
    }
