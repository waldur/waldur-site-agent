"""Tests for DigitalOcean backend."""

import uuid
from unittest.mock import Mock, patch

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_digitalocean.backend import DigitalOceanBackend


@pytest.fixture
def backend_settings():
    """Base DigitalOcean backend settings for tests."""
    return {
        "token": "test-token",
        "default_region": "ams3",
        "default_image": "ubuntu-22-04-x64",
        "default_size": "s-1vcpu-1gb",
        "default_user_data": "#cloud-config\npackages:\n  - htop\n",
        "default_tags": ["waldur"],
    }


@pytest.fixture
def backend_components():
    """Component configuration for tests."""
    return {
        "cpu": {
            "measured_unit": "Cores",
            "unit_factor": 1,
            "accounting_type": "limit",
            "label": "CPU",
        }
    }


@pytest.fixture
def waldur_resource():
    """Mock Waldur resource."""
    resource = Mock(spec=WaldurResource)
    resource.uuid = uuid.uuid4()
    resource.name = "Test Droplet"
    resource.slug = "test-droplet"
    resource.limits = None
    resource.attributes = {}
    resource.options = {}
    return resource


def _build_backend(settings: dict, components: dict) -> DigitalOceanBackend:
    """Create backend instance with mocked client."""
    with patch("waldur_site_agent_digitalocean.backend.DigitalOceanClient"):
        backend = DigitalOceanBackend(settings, components)
    backend.client = Mock()
    return backend


def test_backend_init_requires_token(backend_components):
    """Ensure backend requires token setting."""
    with pytest.raises(BackendError):
        _build_backend({}, backend_components)


def test_create_resource_uses_defaults(
    backend_settings, backend_components, waldur_resource
):
    """Ensure create_resource uses default settings."""
    backend = _build_backend(backend_settings, backend_components)
    droplet = Mock()
    droplet.id = 12345
    backend.client.create_droplet.return_value = droplet
    backend.client.resolve_ssh_key.return_value = None

    result = backend.create_resource(waldur_resource)

    assert result.backend_id == "12345"
    backend.client.create_droplet.assert_called_once_with(
        name="test-droplet",
        region="ams3",
        image="ubuntu-22-04-x64",
        size_slug="s-1vcpu-1gb",
        user_data=backend_settings["default_user_data"],
        ssh_key_ids=[],
        tags=["waldur"],
    )


def test_create_resource_missing_region_raises(
    backend_settings, backend_components, waldur_resource
):
    """Ensure create_resource validates required settings."""
    backend_settings = {**backend_settings, "default_region": None}
    backend = _build_backend(backend_settings, backend_components)

    with pytest.raises(BackendError):
        backend.create_resource(waldur_resource)


def test_set_resource_limits_resizes(backend_settings, backend_components):
    """Ensure resize is triggered when limits match size mapping."""
    backend_settings = {
        **backend_settings,
        "size_mapping": {"s-1vcpu-1gb": {"cpu": 1}},
    }
    backend = _build_backend(backend_settings, backend_components)

    backend.set_resource_limits("droplet-1", {"cpu": 1})

    backend.client.resize_droplet.assert_called_once_with(
        "droplet-1", size_slug="s-1vcpu-1gb", disk=False
    )
