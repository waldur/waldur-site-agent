"""Names for the objects a virtual machine needs, and how to read them back.

The agent keeps no database, so everything it must find again after a restart is
either derived from the resource name or parsed out of the ARM id Azure returns.
Offerings taken over from mastermind have machines already named by this
scheme, which is what lets the agent find and delete them.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Optional

# Azure allows up to 64 characters for a Linux virtual machine name. The derived
# names add a prefix of at most 7 characters ("subnet"), and the resource group
# adds "group-xxxx-", so the machine name itself is cut short enough for every
# derived name to stay within its own limit.
_MAX_VM_NAME_LENGTH = 50
_GROUP_INFIX_LENGTH = 4
_FALLBACK_VM_NAME = "waldur-vm"

_INVALID_CHARACTERS = re.compile(r"[^a-z0-9-]+")
_REPEATED_HYPHENS = re.compile(r"-{2,}")

def _arm_id(provider: str, collection: str) -> "re.Pattern[str]":
    return re.compile(
        r"^/subscriptions/(?P<subscription_id>[^/]+)"
        r"/resourceGroups/(?P<resource_group>[^/]+)"
        rf"/providers/{provider}/{collection}/(?P<name>[^/]+)$",
        re.IGNORECASE,
    )


# The group a machine of ours gets when the offering names no shared
# one. Matching it is how deletion tells "a group holding only this resource,
# safe to drop whole" from "the operator's group, shared with resources this
# agent never made" — a question the current configuration cannot answer, because
# the setting may have changed since the resource was created.
_DEDICATED_GROUP_PREFIX = re.compile(r"^group-[0-9a-f]{4}-")

_VIRTUAL_MACHINE_ID = _arm_id(r"Microsoft\.Compute", "virtualMachines")


def is_dedicated_resource_group(resource_group: str, resource_name: str) -> bool:
    """Whether this group was created for this resource alone.

    Deleting a group takes everything in it, so the test is deliberately exact:
    the plugin's own prefix, its four-character infix, and then the resource's
    own name. An operator's group called ``group-abcd-web`` would have to be a
    deliberate imitation to pass.
    """
    match = _DEDICATED_GROUP_PREFIX.match(resource_group or "")
    if not match:
        return False
    return resource_group[match.end() :] == resource_name


def _group_infix(unique_suffix: Optional[str]) -> str:
    """Return the four characters that keep two same-named groups apart.

    Taken from the resource's uuid so that a retried order derives the same
    group name and finds what the killed attempt left behind. A resource with no
    uuid to offer falls back to chance, which is still unique but no longer
    findable.
    """
    hex_digits = "".join(
        character for character in (unique_suffix or "").lower() if character in "0123456789abcdef"
    )
    return hex_digits[:_GROUP_INFIX_LENGTH].ljust(_GROUP_INFIX_LENGTH, "0") if hex_digits else (
        uuid.uuid4().hex[:_GROUP_INFIX_LENGTH]
    )


def sanitize_name(name: str, suffix: Optional[str] = None) -> str:
    """Reduce a Waldur resource name to something Azure accepts.

    Azure rejects a name outright rather than trimming it, and the failure
    arrives as a provisioning error on the user's order, so the sanitising
    happens here instead. ``suffix`` is appended after trimming, so it survives a
    long name — being the part that makes the name unique, it is the last thing
    that may be cut.
    """
    sanitized = _INVALID_CHARACTERS.sub("-", (name or "").lower())
    sanitized = _REPEATED_HYPHENS.sub("-", sanitized).strip("-")
    sanitized = sanitized[:_MAX_VM_NAME_LENGTH].strip("-")
    sanitized = sanitized or _FALLBACK_VM_NAME
    if suffix:
        return f"{sanitized}-{sanitize_name(suffix)}"
    return sanitized


@dataclass(frozen=True)
class VirtualMachineNames:
    """Every Azure object created for one virtual machine."""

    virtual_machine: str
    resource_group: str
    network: str
    subnet: str
    network_interface: str
    ip_configuration: str
    public_ip: str
    security_group: str

    @classmethod
    def build(
        cls,
        name: str,
        resource_group: Optional[str] = None,
        unique_suffix: Optional[str] = None,
    ) -> VirtualMachineNames:
        """Derive the names from a resource name.

        ``resource_group`` names a shared group configured by the operator. When
        it is absent the machine gets a group of its own, named after the
        resource rather than at random: resource group names are unique per
        subscription, so two projects asking for the same machine name must not
        collide, but a group whose name cannot be derived again is a group a
        retried order cannot find. An order killed mid-create would otherwise
        leave a machine billing under a name nothing points at, and build a
        second one beside it.

        ``unique_suffix`` is the resource's own uuid, and it does both jobs: the
        infix of a dedicated group, and — inside a shared group, where the name
        is all that separates two machines — part of the machine name itself.
        Two resources whose names sanitize alike, as every pair of non-Latin
        names does, would otherwise address one machine.
        """
        in_shared_group = bool(resource_group)
        vm_name = sanitize_name(name, suffix=unique_suffix if in_shared_group else None)
        if in_shared_group:
            group = str(resource_group)
        else:
            group = f"group-{_group_infix(unique_suffix)}-{vm_name}"
        return cls.of(vm_name, group)

    @classmethod
    def of(cls, vm_name: str, resource_group: str) -> VirtualMachineNames:
        """Derive the companion names from a machine name Azure already has.

        The name arrives from the ARM id, where Azure recorded what was actually
        created, so it must be used as it stands. Putting it through ``build``
        again would sanitize a sanitized name — trimming a long one a second time
        — and every name derived here would address objects that do not exist,
        silently: a delete that finds nothing reports success and leaves the
        machine running.
        """
        return cls(
            virtual_machine=vm_name,
            resource_group=resource_group,
            network=f"net{vm_name}",
            subnet=f"subnet{vm_name}",
            network_interface=f"nic{vm_name}",
            ip_configuration=f"ipconf{vm_name}",
            public_ip=f"pubip{vm_name}",
            security_group=f"nsg{vm_name}",
        )


@dataclass(frozen=True)
class VirtualMachineId:
    """The parts of an ARM virtual machine id the agent needs."""

    subscription_id: str
    resource_group: str
    name: str

    @property
    def names(self) -> VirtualMachineNames:
        """Return the names of the objects created alongside this machine."""
        return VirtualMachineNames.of(self.name, self.resource_group)


def parse_virtual_machine_id(backend_id: str) -> VirtualMachineId:
    """Read the resource group and machine name out of an ARM id.

    The ARM id is what Waldur stores as the resource's backend id. Reading the
    group back from it, rather than deriving it again, keeps deletion working for
    every group name, including one whose infix was drawn by chance for a
    resource that had no uuid.

    Raises:
        ValueError: If the id is not an ARM virtual machine id.
    """
    match = _VIRTUAL_MACHINE_ID.match(backend_id or "")
    if not match:
        msg = f"Not an Azure virtual machine id: {backend_id!r}"
        raise ValueError(msg)
    return VirtualMachineId(
        subscription_id=match.group("subscription_id"),
        resource_group=match.group("resource_group"),
        name=match.group("name"),
    )
