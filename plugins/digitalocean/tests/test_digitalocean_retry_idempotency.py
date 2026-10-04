"""A retried order must not provision a second droplet.

If ``create_droplet`` succeeds but recording the backend id in Waldur fails, the
order stays executing with no ``backend_id`` and the processor calls the create
path again. The droplet the first attempt created is found by its resource tag
and adopted instead of being duplicated.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent_digitalocean.backend import DigitalOceanBackend


class FakeDigitalOceanClient:
    """Records droplets the way the DigitalOcean API would, tags included."""

    def __init__(self) -> None:
        self.droplets: list[SimpleNamespace] = []
        self._next_id = 1000

    def add_droplet(self, tags: list[str]) -> SimpleNamespace:
        droplet = SimpleNamespace(id=self._next_id, tags=list(tags))
        self._next_id += 1
        self.droplets.append(droplet)
        return droplet

    def create_droplet(self, **kwargs: object) -> SimpleNamespace:
        return self.add_droplet(list(kwargs.get("tags") or []))

    def list_droplets_by_tag(self, tag: str) -> list[SimpleNamespace]:
        return [droplet for droplet in self.droplets if tag in droplet.tags]

    def resolve_ssh_key(self, **_kwargs: object) -> None:
        return None


def _backend() -> tuple[DigitalOceanBackend, FakeDigitalOceanClient]:
    with patch("waldur_site_agent_digitalocean.backend.DigitalOceanClient"):
        backend = DigitalOceanBackend(
            {
                "token": "test-token",
                "default_region": "ams3",
                "default_image": "ubuntu-24-04-x64",
                "default_size": "s-1vcpu-1gb",
            },
            {"cpu": {"measured_unit": "Cores", "unit_factor": 1, "accounting_type": "limit", "label": "CPU"}},
        )
    client = FakeDigitalOceanClient()
    backend.client = client  # type: ignore[assignment]
    return backend, client


def _resource() -> WaldurResource:
    """The order path hands the backend an SDK model with no backend_id yet."""
    return WaldurResource.from_dict(
        {"uuid": uuid.uuid4().hex, "name": "val-retry", "slug": "val-retry", "attributes": {}}
    )


def test_retry_after_partial_order_adopts_the_droplet() -> None:
    backend, client = _backend()
    resource = _resource()

    first = backend.create_resource_with_id(resource, "waldur-val-retry")
    # set_backend_id failed, so the resource still has no backend_id and the
    # processor tries again, possibly with its next candidate id.
    second = backend.create_resource_with_id(resource, "waldur-val-retry-0")

    assert len(client.droplets) == 1
    assert second.backend_id == first.backend_id


def test_new_droplet_carries_the_resource_tag() -> None:
    backend, client = _backend()
    resource = _resource()

    backend.create_resource_with_id(resource, "waldur-val-retry")

    assert f"waldur-resource:{resource.uuid.hex}" in client.droplets[0].tags


def test_droplet_tagged_for_another_resource_is_not_adopted() -> None:
    backend, client = _backend()
    other = client.add_droplet([f"waldur-resource:{uuid.uuid4().hex}"])
    resource = _resource()

    result = backend.create_resource_with_id(resource, "waldur-val-retry")

    assert result.backend_id != str(other.id)
    assert len(client.droplets) == 2
    assert f"waldur-resource:{resource.uuid.hex}" in client.droplets[-1].tags


def test_two_droplets_with_the_resource_tag_are_refused() -> None:
    backend, client = _backend()
    resource = _resource()
    tag = f"waldur-resource:{resource.uuid.hex}"
    client.add_droplet([tag])
    client.add_droplet([tag])

    with pytest.raises(BackendError, match="2 droplets"):
        backend.create_resource_with_id(resource, "waldur-val-retry")
    assert len(client.droplets) == 2


def test_ordered_tags_cannot_plant_another_resources_adoption_tag() -> None:
    """A user who orders a droplet tagged for someone else's resource must not get it adopted."""
    backend, client = _backend()
    victim = _resource()
    attacker = WaldurResource.from_dict(
        {
            "uuid": uuid.uuid4().hex,
            "name": "attacker",
            "slug": "attacker",
            "attributes": {"tags": [f"waldur-resource:{victim.uuid.hex}", "mine"]},
        }
    )

    attacker_result = backend.create_resource_with_id(attacker, "waldur-attacker")
    attacker_tags = client.droplets[0].tags
    victim_result = backend.create_resource_with_id(victim, "waldur-victim")

    assert f"waldur-resource:{victim.uuid.hex}" not in attacker_tags
    assert "mine" in attacker_tags
    assert victim_result.backend_id != attacker_result.backend_id
    assert len(client.droplets) == 2


def test_droplet_with_two_adoption_tags_is_not_adopted() -> None:
    """A droplet carrying our tag and another resource's tag was planted or tampered with."""
    backend, client = _backend()
    resource = _resource()
    planted = client.add_droplet(
        [f"waldur-resource:{resource.uuid.hex}", f"waldur-resource:{uuid.uuid4().hex}"]
    )

    result = backend.create_resource_with_id(resource, "waldur-val-retry")

    assert result.backend_id != str(planted.id)
    assert len(client.droplets) == 2


def test_resource_tag_accepts_a_string_uuid() -> None:
    backend, client = _backend()
    resource = _resource()
    resource.uuid = str(uuid.UUID(resource.uuid.hex))  # type: ignore[assignment]

    backend.create_resource_with_id(resource, "waldur-val-retry")

    assert f"waldur-resource:{resource.uuid.replace('-', '')}" in client.droplets[0].tags  # type: ignore[attr-defined]
