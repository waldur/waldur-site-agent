"""Every Azure SDK call this plugin makes, checked against the installed SDK.

The unit tests mock the management clients and the E2E fixtures replace the whole
``AzureClient`` facade, so neither notices when a method name, an enum member or a
parameter list differs from what ``azure-mgmt-*`` actually offers: a method that
no longer exists, an enum member from the msrest-era SDK, or operations whose
trailing arguments became a single model.

Nothing here talks to Azure. Signatures are bound and models are constructed, both
of which are local.
"""

from __future__ import annotations

import inspect

import pytest
from azure.mgmt.compute.models import (
    DiskCreateOption,
    LinuxConfiguration,
    OSProfile,
    SshConfiguration,
    SshPublicKey,
)
from azure.mgmt.compute.operations import (
    DisksOperations,
    ResourceSkusOperations,
    VirtualMachineImagesOperations,
    VirtualMachineSizesOperations,
    VirtualMachinesOperations,
)
from azure.mgmt.network.models import (
    NetworkInterface,
    NetworkInterfaceIPConfiguration,
    NetworkSecurityGroup,
    SecurityRule,
)
from azure.mgmt.network.operations import (
    NetworkInterfacesOperations,
    NetworkSecurityGroupsOperations,
    PublicIPAddressesOperations,
    SubnetsOperations,
    VirtualNetworksOperations,
)
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient

class _DummyCredential:
    """Enough of a credential to construct a client; never used for a request."""

    def get_token(self, *scopes: str, **kwargs: object) -> None:
        raise NotImplementedError


RECEIVER = object()
MODEL = object()  # Stands in for whatever model or dict body the call passes.

# (label, operations class, method, positional arguments, keyword arguments)
COMPUTE_CALLS = [
    ("sizes list", VirtualMachineSizesOperations, "list", ("westeurope",), {}),
    ("skus list", ResourceSkusOperations, "list", (), {"filter": "location eq 'eu'"}),
    ("images publishers", VirtualMachineImagesOperations, "list_publishers", ("eu",), {}),
    ("images offers", VirtualMachineImagesOperations, "list_offers", ("eu", "pub"), {}),
    ("images skus", VirtualMachineImagesOperations, "list_skus", ("eu", "pub", "offer"), {}),
    ("images list", VirtualMachineImagesOperations, "list", ("eu", "pub", "offer", "sku"), {}),
    ("images get", VirtualMachineImagesOperations, "get", ("eu", "pub", "offer", "sku", "1"), {}),
    ("machines list all", VirtualMachinesOperations, "list_all", (), {}),
    ("machines list", VirtualMachinesOperations, "list", ("rg",), {}),
    ("machine get", VirtualMachinesOperations, "get", ("rg", "vm"), {"expand": "instanceView"}),
    ("machine create", VirtualMachinesOperations, "begin_create_or_update", ("rg", "vm", MODEL), {}),
    ("machine delete", VirtualMachinesOperations, "begin_delete", ("rg", "vm"), {}),
    ("machine start", VirtualMachinesOperations, "begin_start", ("rg", "vm"), {}),
    ("machine restart", VirtualMachinesOperations, "begin_restart", ("rg", "vm"), {}),
    ("machine deallocate", VirtualMachinesOperations, "begin_deallocate", ("rg", "vm"), {}),
    ("disk create", DisksOperations, "begin_create_or_update", ("rg", "disk", MODEL), {}),
]

NETWORK_CALLS = [
    ("network create", VirtualNetworksOperations, "begin_create_or_update", ("rg", "net", MODEL), {}),
    ("network get", VirtualNetworksOperations, "get", ("rg", "net"), {}),
    ("network delete", VirtualNetworksOperations, "begin_delete", ("rg", "net"), {}),
    ("subnet create", SubnetsOperations, "begin_create_or_update", ("rg", "net", "sub", MODEL), {}),
    ("subnet get", SubnetsOperations, "get", ("rg", "net", "sub"), {}),
    ("subnet delete", SubnetsOperations, "begin_delete", ("rg", "net", "sub"), {}),
    ("nic create", NetworkInterfacesOperations, "begin_create_or_update", ("rg", "nic", MODEL), {}),
    ("nic get", NetworkInterfacesOperations, "get", ("rg", "nic"), {}),
    ("nic delete", NetworkInterfacesOperations, "begin_delete", ("rg", "nic"), {}),
    ("public ip create", PublicIPAddressesOperations, "begin_create_or_update", ("rg", "ip", MODEL), {}),
    ("public ip get", PublicIPAddressesOperations, "get", ("rg", "ip"), {}),
    ("public ip list all", PublicIPAddressesOperations, "list_all", (), {}),
    ("public ip delete", PublicIPAddressesOperations, "begin_delete", ("rg", "ip"), {}),
    ("security group create", NetworkSecurityGroupsOperations, "begin_create_or_update", ("rg", "nsg", MODEL), {}),
    ("security group delete", NetworkSecurityGroupsOperations, "begin_delete", ("rg", "nsg"), {}),
]

ALL_CALLS = COMPUTE_CALLS + NETWORK_CALLS


@pytest.mark.parametrize(
    ("label", "operations", "method", "args", "kwargs"),
    ALL_CALLS,
    ids=[call[0] for call in ALL_CALLS],
)
def test_the_call_matches_the_installed_sdk(label, operations, method, args, kwargs):
    """The method exists and accepts the arguments the plugin passes."""
    del label
    operation = getattr(operations, method, None)
    assert operation is not None, f"{operations.__name__} has no {method}"
    inspect.signature(operation).bind(RECEIVER, *args, **kwargs)


# Resource Manager exposes its operations through a version-pinned module whose
# path changes with the API version, so these are read off a constructed client
# instead. Constructing one performs no request: the credential is only used when
# a call is made.
RESOURCE_CALLS = [
    ("resource group create", "resource_groups", "create_or_update", ("rg", {"location": "eu"}), {}),
    ("resource group delete", "resource_groups", "begin_delete", ("rg",), {}),
    ("resource groups list", "resource_groups", "list", (), {}),
    ("providers get", "providers", "get", ("Microsoft.Resources",), {}),
]


@pytest.mark.parametrize(
    ("label", "group", "method", "args", "kwargs"),
    RESOURCE_CALLS,
    ids=[call[0] for call in RESOURCE_CALLS],
)
def test_the_resource_call_matches_the_installed_sdk(label, group, method, args, kwargs):
    """Same check for Resource Manager, through a client rather than a module."""
    del label
    client = ResourceManagementClient(_DummyCredential(), "subscription")
    operation = getattr(getattr(client, group), method)
    inspect.signature(operation).bind(*args, **kwargs)


def test_the_subscription_calls_match_the_installed_sdk():
    """``ping`` and the region listing are the only two, and both are read here."""
    client = SubscriptionClient(_DummyCredential())
    inspect.signature(client.subscriptions.get).bind("subscription")
    inspect.signature(client.subscriptions.list_locations).bind("subscription")


@pytest.mark.parametrize(
    ("enum", "member"),
    [
        (DiskCreateOption, "empty"),
    ],
)
def test_the_enum_member_exists(enum, member):
    """``DiskCreateOption.Empty`` is what a stale spelling looks like."""
    assert hasattr(enum, member)


@pytest.mark.parametrize(
    ("model", "kwargs"),
    [
        (NetworkInterfaceIPConfiguration, {"name": "ipconf", "subnet": {"id": "subnet"}}),
        (NetworkInterface, {"location": "eu", "ip_configurations": []}),
        (NetworkSecurityGroup, {"location": "eu", "security_rules": []}),
        (
            SecurityRule,
            {
                "name": "default-allow-ssh",
                "protocol": "Tcp",
                "source_port_range": "*",
                "destination_port_range": "22",
                "direction": "Inbound",
                "source_address_prefixes": ["203.0.113.0/24"],
                "destination_address_prefix": "*",
                "access": "Allow",
                "priority": 1000,
            },
        ),
        (OSProfile, {"computer_name": "vm", "admin_username": "waldur", "admin_password": "s"}),
        (SshPublicKey, {"key_data": "ssh-ed25519 AAAA"}),
        (SshConfiguration, {"public_keys": []}),
        (LinuxConfiguration, {"ssh": None}),
    ],
    ids=lambda value: value.__name__ if inspect.isclass(value) else "",
)
def test_the_model_accepts_what_the_plugin_passes(model, kwargs):
    """A renamed field would otherwise surface as a silently ignored setting."""
    assert model(**kwargs) is not None
