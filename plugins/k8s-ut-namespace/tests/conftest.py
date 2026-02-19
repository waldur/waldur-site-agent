"""Shared fixtures for K8s UT namespace plugin tests."""

import pytest
from unittest.mock import MagicMock
from uuid import uuid4

from waldur_api_client.models.resource import Resource as WaldurResource


class MockResourceLimits:
    """Mock ResourceLimits for testing."""

    def __init__(self, cpu=4, ram=8, storage=100, gpu=1):
        self.cpu = cpu
        self.ram = ram
        self.storage = storage
        self.gpu = gpu

    def to_dict(self):
        return {
            "cpu": self.cpu,
            "ram": self.ram,
            "storage": self.storage,
            "gpu": self.gpu,
        }


@pytest.fixture
def backend_settings():
    """Basic backend settings for testing."""
    return {
        "kubeconfig_path": "/tmp/fake-kubeconfig",
        "cr_namespace": "waldur-system",
        "namespace_prefix": "waldur-",
        "default_role": "readwrite",
        "keycloak_enabled": True,
        "keycloak_use_user_id": True,
        "keycloak": {
            "keycloak_url": "https://keycloak.example.com/auth/",
            "keycloak_realm": "test",
            "client_id": "admin-cli",
            "keycloak_username": "admin",
            "keycloak_password": "test-password",
            "keycloak_ssl_verify": False,
        },
    }


@pytest.fixture
def backend_settings_no_keycloak():
    """Backend settings without Keycloak."""
    return {
        "kubeconfig_path": "/tmp/fake-kubeconfig",
        "cr_namespace": "waldur-system",
        "namespace_prefix": "waldur-",
        "default_role": "readwrite",
        "keycloak_enabled": False,
    }


@pytest.fixture
def backend_components():
    """Component definitions for testing."""
    return {
        "cpu": {"type": "cpu", "unit_factor": 1, "accounting_type": "limit"},
        "ram": {"type": "ram", "unit_factor": 1, "accounting_type": "limit"},
        "storage": {"type": "storage", "unit_factor": 1, "accounting_type": "limit"},
        "gpu": {"type": "gpu", "unit_factor": 1, "accounting_type": "limit"},
    }


@pytest.fixture
def waldur_resource():
    """Sample Waldur resource for testing."""
    return WaldurResource(
        uuid=uuid4(),
        name="Test Namespace",
        slug="test-ns",
        customer_slug="test-customer",
        customer_uuid="cust-uuid-1234",
        project_slug="test-project",
        project_name="Test Project",
        project_uuid="proj-uuid-5678",
        backend_id="",
        limits=MockResourceLimits(),
    )


@pytest.fixture
def waldur_resource_with_backend_id(waldur_resource):
    """Waldur resource with backend ID set."""
    waldur_resource.backend_id = "waldur-test-ns"
    return waldur_resource
