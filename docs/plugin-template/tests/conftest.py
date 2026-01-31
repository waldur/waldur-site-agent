"""Common test fixtures for the custom backend plugin."""

from unittest.mock import MagicMock

import pytest

from waldur_site_agent_mycustom.backend import MyCustomBackend


@pytest.fixture
def backend_settings():
    """Standard backend settings for testing."""
    return {
        "default_account": "root",
        "customer_prefix": "c_",
        "project_prefix": "p_",
        "allocation_prefix": "a_",
    }


@pytest.fixture
def backend_components():
    """Standard backend components for testing."""
    return {
        "cpu": {
            "limit": 10,
            "measured_unit": "k-Hours",
            "unit_factor": 60000,
            "accounting_type": "usage",
            "label": "CPU",
        },
        "mem": {
            "limit": 10,
            "measured_unit": "gb-Hours",
            "unit_factor": 61440,
            "accounting_type": "usage",
            "label": "RAM",
        },
    }


@pytest.fixture
def backend(backend_settings, backend_components):
    """Backend instance with a mocked client."""
    b = MyCustomBackend(backend_settings, backend_components)
    b.client = MagicMock()
    return b
