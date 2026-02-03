"""Tests for DigitalOcean client wrapper."""

from unittest.mock import Mock, patch

import pytest
import digitalocean

from waldur_site_agent_digitalocean.client import (
    DigitalOceanClient,
    NotFoundError,
    UnauthorizedError,
)


def _build_client():
    """Create client with mocked manager."""
    with patch("waldur_site_agent_digitalocean.client.digitalocean.Manager") as manager_cls:
        manager = manager_cls.return_value
        return DigitalOceanClient(token="token"), manager


def test_list_resources_maps_auth_error():
    """Ensure DataReadError is mapped to UnauthorizedError."""
    client, manager = _build_client()
    manager.get_all_droplets.side_effect = digitalocean.DataReadError(
        "Unable to authenticate you."
    )

    with pytest.raises(UnauthorizedError):
        client.list_resources()


def test_get_resource_not_found_returns_none():
    """Ensure not found droplet results in None."""
    client, manager = _build_client()
    manager.get_droplet.side_effect = digitalocean.DataReadError(
        "The resource you were accessing could not be found."
    )

    result = client.get_resource("missing")

    assert result is None


def test_resolve_ssh_key_creates_key_when_missing():
    """Ensure public key path creates SSH key when not found."""
    client, _manager = _build_client()
    client.load_ssh_key = Mock(side_effect=NotFoundError("not found"))
    created_key = Mock()
    client.create_ssh_key = Mock(return_value=created_key)

    result = client.resolve_ssh_key(
        ssh_key_public_key="ssh-rsa AAAA",
        ssh_key_name="waldur-test",
    )

    assert result is created_key
    client.create_ssh_key.assert_called_once_with("waldur-test", "ssh-rsa AAAA")


def test_resolve_ssh_key_by_id_uses_load():
    """Ensure SSH key is loaded when id or fingerprint is provided."""
    client, _manager = _build_client()
    loaded_key = Mock()
    client.load_ssh_key = Mock(return_value=loaded_key)

    result = client.resolve_ssh_key(ssh_key_id=123, ssh_key_fingerprint="fp")

    assert result is loaded_key
    client.load_ssh_key.assert_called_once_with(
        ssh_key_id=123, ssh_key_fingerprint="fp", ssh_key_name=None
    )
