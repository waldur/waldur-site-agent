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
    backend.client.list_droplets_by_tag.return_value = []
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
        tags=["waldur", f"waldur-resource:{waldur_resource.uuid.hex}"],
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


def test_create_resource_with_id_provisions_droplet_from_attributes(
    backend_settings, backend_components, waldur_resource
):
    """The order processor's entry point creates the droplet with the ordered shape.

    The processor calls ``create_resource_with_id`` with a slug-derived id; the
    droplet must still get its region, image and size, and the backend id must
    be the droplet id DigitalOcean assigned.
    """
    backend = _build_backend(backend_settings, backend_components)
    waldur_resource.attributes = {"region": "fra1", "image": "debian-12-x64", "size": "s-2vcpu-4gb"}
    droplet = Mock()
    droplet.id = 4242
    backend.client.create_droplet.return_value = droplet
    backend.client.resolve_ssh_key.return_value = None

    result = backend.create_resource_with_id(waldur_resource, "waldur-test-droplet")

    assert result.backend_id == "4242"
    backend.client.create_resource.assert_not_called()
    kwargs = backend.client.create_droplet.call_args.kwargs
    assert kwargs["region"] == "fra1"
    assert kwargs["image"] == "debian-12-x64"
    assert kwargs["size_slug"] == "s-2vcpu-4gb"


def test_create_resource_with_id_reads_sdk_resource_attributes(backend_settings, backend_components):
    """On the order path the resource is an SDK model, and its attributes are not a dict."""
    backend = _build_backend(backend_settings, backend_components)
    waldur_resource = WaldurResource.from_dict(
        {
            "uuid": uuid.uuid4().hex,
            "name": "val-e3",
            "slug": "val-e3",
            "attributes": {"region": "fra1", "image": "debian-12-x64", "size": "s-2vcpu-4gb"},
            "options": {"tags": ["from-options"]},
        }
    )
    droplet = Mock()
    droplet.id = 4343
    backend.client.create_droplet.return_value = droplet
    backend.client.resolve_ssh_key.return_value = None

    result = backend.create_resource_with_id(waldur_resource, "waldur-val-e3")

    assert result.backend_id == "4343"
    kwargs = backend.client.create_droplet.call_args.kwargs
    assert (kwargs["region"], kwargs["image"], kwargs["size_slug"]) == (
        "fra1",
        "debian-12-x64",
        "s-2vcpu-4gb",
    )
    assert kwargs["tags"] == ["from-options", f"waldur-resource:{waldur_resource.uuid.hex}"]


def test_recreate_missing_resource_does_not_create_a_droplet(
    backend_settings, backend_components, waldur_resource
):
    """A forced sync must not create a new, unreferenced droplet for a lost one.

    A new droplet gets a new id, so Waldur would keep the dead id while the new
    droplet runs up a bill on every sync.
    """
    backend = _build_backend(backend_settings, backend_components)
    waldur_resource.backend_id = "1000"
    backend.client.get_resource.return_value = None

    assert backend.recreate_missing_resource(waldur_resource) is False
    backend.client.create_droplet.assert_not_called()
    backend.client.create_resource.assert_not_called()


def test_create_resource_with_id_refuses_to_recreate_under_existing_id(
    backend_settings, backend_components, waldur_resource
):
    """The order path re-creates under the resource's own backend_id when the droplet is gone.

    DigitalOcean cannot reuse a droplet id; creating one would leave Waldur
    linked to the dead id, so the order fails with an explanation instead.
    """
    backend = _build_backend(backend_settings, backend_components)
    waldur_resource.backend_id = "1000"

    with pytest.raises(BackendError, match="1000"):
        backend.create_resource_with_id(waldur_resource, "1000")
    backend.client.create_droplet.assert_not_called()
