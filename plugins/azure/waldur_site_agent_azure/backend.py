"""Azure backend for Waldur Site Agent.

Provisions virtual machines (the ``Azure.VirtualMachine`` offering type) by
orchestrating the per-service clients: a machine needs a resource group, a
network, a subnet, a public address and an interface before it can be created.
The agent keeps no state, so the chain runs inline here and everything needed
afterwards is either derived from the resource name or read back out of the ARM
id Azure returns.
"""

from __future__ import annotations

import logging
import secrets
import string
from typing import Any, Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import BackendType, backends, structures
from waldur_site_agent.backend.exceptions import BackendError

from .base_client import AzureCoreClient
from .clients import AzureClient, AzureClientError
from .metering import AllocationMeter
from .naming import (
    VirtualMachineId,
    VirtualMachineNames,
    is_dedicated_resource_group,
    parse_virtual_machine_id,
)

logger = logging.getLogger(__name__)

_REQUIRED_SETTINGS = ("subscription_id", "tenant_id", "client_id", "client_secret")

# The private ranges of each machine's own network. Machines of offerings taken
# over from mastermind already use these ranges.
_DEFAULT_NETWORK_CIDR = "10.0.0.0/16"
_DEFAULT_SUBNET_CIDR = "10.0.0.0/24"

# Azure wants 12-72 characters from three of four character classes. The
# password is never handed out: an SSH key is what the user logs in with, and
# Azure will not create a Linux machine without one credential or the other.
_PASSWORD_LENGTH = 24
_IMAGE_REFERENCE_PARTS = 4


def _generate_password() -> str:
    """Return a random administrator password that is never shown to anyone."""
    alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
    while True:
        password = "".join(secrets.choice(alphabet) for _ in range(_PASSWORD_LENGTH))
        if (
            any(character.islower() for character in password)
            and any(character.isupper() for character in password)
            and any(character.isdigit() for character in password)
        ):
            return password


class AzureBackend(backends.BaseBackend):
    """Azure backend implementation for Waldur Site Agent."""

    def __init__(
        self, backend_settings: dict[str, object], backend_components: dict[str, dict]
    ) -> None:
        """Initialize the Azure backend with settings and components."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = BackendType.AZURE.value

        missing = [name for name in _REQUIRED_SETTINGS if not backend_settings.get(name)]
        if missing:
            msg = f"Azure backend requires {', '.join(missing)} in backend_settings"
            raise BackendError(msg)

        self.azure_client = AzureClient(
            subscription_id=str(backend_settings["subscription_id"]),
            tenant_id=str(backend_settings["tenant_id"]),
            client_id=str(backend_settings["client_id"]),
            client_secret=str(backend_settings["client_secret"]),
        )
        self.client = AzureCoreClient(self.azure_client)

        self.meter = AllocationMeter(self.azure_client, backend_components)
        self._measured: set[str] = set()

        self.default_location = backend_settings.get("default_location")
        self.default_resource_group = backend_settings.get("default_resource_group")
        self.default_size = backend_settings.get("default_size")
        self.default_image = backend_settings.get("default_image")
        self.network_cidr = backend_settings.get("network_cidr") or _DEFAULT_NETWORK_CIDR
        self.subnet_cidr = backend_settings.get("subnet_cidr") or _DEFAULT_SUBNET_CIDR
        # A lone address is accepted where a list is expected: iterating a bare
        # string would otherwise open SSH to one range per character.
        configured_ranges = backend_settings.get("allowed_ssh_ranges") or []
        self.allowed_ssh_ranges: list[str] = (
            [str(entry) for entry in configured_ranges]
            if isinstance(configured_ranges, (list, tuple, set))
            else [str(configured_ranges)]
        )

    def ping(self, raise_exception: bool = False) -> bool:
        """Check whether the configured Azure subscription is reachable."""
        if self.azure_client.ping():
            return True
        if raise_exception:
            msg = "Azure backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log diagnostic information about backend settings."""
        logger.info("=== Azure backend diagnostics ===")
        logger.info("Subscription: %s", self.azure_client.subscription_id)
        logger.info("Components: %s", list(self.backend_components.keys()))
        logger.info("Default location: %s", self.default_location)
        logger.info("Default size: %s", self.default_size)
        logger.info("Resource group: %s", self.default_resource_group or "one per machine")
        return self.ping(raise_exception=False)

    def list_components(self) -> list[str]:
        """Return the component names configured for this offering."""
        return list(self.backend_components.keys())

    def list_resources(self) -> list[structures.BackendResourceInfo]:
        """List the machines in the subscription by the id Waldur stores.

        The inherited implementation reports each resource under its name, but
        what Waldur holds in ``backend_id`` is the ARM id. Comparing the two sets
        never matches anything, so every machine the agent already manages would
        be offered for import again.
        """
        return [
            structures.BackendResourceInfo(backend_id=resource.backend_id)
            for resource in self.client.list_resources()
        ]

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Report what each resource holds on Azure, in the offering's components.

        Allocation rather than consumption; see ``metering.py`` for why there is
        no consumption figure to report.
        """
        report = self.meter.report(resource_backend_ids)
        # Remembered so the pull below can tell "measured nothing" from "not
        # measured", without asking Azure for the same shapes twice.
        self._measured = set(report)
        return report

    def _pull_backend_resource(
        self, resource_backend_id: str
    ) -> Optional[structures.BackendResourceInfo]:
        """Pull the resource without substituting zero for an unread shape.

        The base fills every component with 0 when the usage report omits a
        resource. Zero is a legitimate usage value, so an Azure outage or a size
        the region no longer offers would be written into Waldur as "this machine
        holds nothing" — and for a usage-priced offering, as a period with no
        charge.
        """
        info = super()._pull_backend_resource(resource_backend_id)
        if info is None:
            return None
        if resource_backend_id not in self._measured:
            info.usage = {}
        return info

    def _pre_create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> None:
        """Set up no prerequisites: the whole chain runs in ``create_resource``."""
        del waldur_resource, user_context

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Return the Waldur-side limits; the Azure size carries the real shape."""
        if not waldur_resource.limits:
            return {}, {}
        return {}, waldur_resource.limits.to_dict()

    def create_resource(
        self,
        waldur_resource: WaldurResource,
        user_context: Optional[dict] = None,
    ) -> structures.BackendResourceInfo:
        """Create a virtual machine and everything it needs to be reachable."""
        attributes = self._attributes(waldur_resource)
        location = self._require(
            attributes.get("location") or self.default_location, "location"
        )
        name = waldur_resource.name or getattr(waldur_resource, "slug", "")

        size = self._require(attributes.get("size") or self.default_size, "size")
        image = self._image_reference(
            self._require(attributes.get("image") or self.default_image, "image")
        )
        # The administrator password below is generated to satisfy Azure and is
        # reported to no one, so the key is the only way into the machine. This
        # is checked before anything is created: the alternative is a machine
        # that bills and cannot be logged into.
        ssh_key = self._require_ssh_key(attributes, user_context, name)
        shared_group = str(self.default_resource_group) if self.default_resource_group else None
        names = VirtualMachineNames.build(
            name,
            resource_group=shared_group,
            # The uuid is always offered; naming decides what it is needed for -
            # the machine name in a shared group, the group's own infix outside
            # one - so that a retried order derives the same names as the attempt
            # it is repeating.
            unique_suffix=self._unique_suffix(waldur_resource),
        )

        logger.info(
            "Creating Azure virtual machine %s in %s (size=%s, group=%s)",
            names.virtual_machine,
            location,
            size,
            names.resource_group,
        )

        network = self.azure_client.network
        self.azure_client.resource.create_resource_group(location, names.resource_group)
        try:
            network.create_network(
                location, names.resource_group, names.network, self.network_cidr
            ).result()
            subnet = network.create_subnet(
                names.resource_group, names.network, names.subnet, self.subnet_cidr
            ).result()
            public_ip = network.create_public_ip(
                location, names.resource_group, names.public_ip
            ).result()
            security_group = self._open_ssh(location, names)
            nic = network.create_network_interface(
                location,
                names.resource_group,
                names.network_interface,
                names.ip_configuration,
                subnet.id,
                public_ip_id=public_ip.id,
                security_group_id=security_group.id if security_group else None,
            ).result()

            virtual_machine = self.azure_client.compute.create_virtual_machine(
                location=location,
                resource_group_name=names.resource_group,
                vm_name=names.virtual_machine,
                size_name=size,
                nic_id=nic.id,
                image_reference=image,
                username=str(attributes.get("username") or "waldur"),
                password=_generate_password(),
                custom_data=attributes.get("user_data") or attributes.get("cloud_init"),
                ssh_key=ssh_key,
            ).result()
        except Exception:
            # Waldur records a machine's id only once the machine exists, so
            # anything a failed chain leaves behind would bill with nothing in
            # Waldur pointing at it. A failure on the machine itself is routine:
            # a size the region does not offer, a quota, a bad image reference.
            logger.exception("Rolling back the partly created machine %s", names.virtual_machine)
            self._delete_machine_objects(names, dedicated=not shared_group)
            raise

        logger.info("Azure virtual machine created: %s", virtual_machine.id)
        return structures.BackendResourceInfo(
            backend_id=virtual_machine.id,
            limits=waldur_resource.limits.to_dict() if waldur_resource.limits else {},
            backend_metadata=self._describe(virtual_machine, public_ip, nic),
        )

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> structures.BackendResourceInfo:
        """Create the resource, ignoring the backend id the core suggested.

        This is the method the order processor actually calls; ``create_resource``
        is only the entry point when something calls the backend directly. The
        base implementation builds an id from the resource slug and hands it to
        ``client.create_resource``, which suits a backend where the caller names
        the account. Azure names its own: the id is the ARM path, and it does not
        exist until the machine or server does. The processor writes whatever
        ``backend_id`` comes back into Waldur, so the suggestion is dropped here
        rather than worked around later.
        """
        logger.debug(
            "Ignoring suggested backend id %s: Azure assigns the resource id",
            resource_backend_id,
        )
        return self.create_resource(waldur_resource, user_context)

    def recreate_missing_resource(self, waldur_resource: WaldurResource) -> bool:
        """Refuse to recreate a resource under its old id.

        Reconciliation exists for backends that can be handed an id and rebuild
        the account behind it. A new Azure machine gets a new ARM path, so
        "recreating" one would leave Waldur pointing at the id of a resource that
        no longer exists while a second, unreferenced resource runs up a bill.
        """
        logger.warning(
            "Not recreating Azure resource %s: a new resource would get a new id, "
            "leaving Waldur pointing at the old one",
            waldur_resource.backend_id,
        )
        return False

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: str) -> None:
        """Delete a virtual machine and the objects created alongside it."""
        del kwargs
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning(
                "No backend ID for resource %s, nothing to delete", waldur_resource.uuid
            )
            return

        vm_id = self._parse(backend_id)

        # Which route to take is read from the group's own name, not from the
        # current setting: an operator who adds, clears or renames
        # default_resource_group after provisioning would otherwise have their
        # shared group deleted whole, along with every resource in it that this
        # agent never made.
        if is_dedicated_resource_group(vm_id.resource_group, vm_id.name):
            self._delete_machine_objects(vm_id.names, dedicated=True)
            return

        self._delete_machine_objects(vm_id.names, dedicated=False)

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return what Waldur can show about the machine."""
        vm_id = self._parse(resource_backend_id)
        names = vm_id.names
        virtual_machine = self.azure_client.compute.get_virtual_machine(
            vm_id.resource_group, vm_id.name, expand="instanceView"
        )
        public_ip = self._optional(
            self.azure_client.network.get_public_ip, vm_id.resource_group, names.public_ip
        )
        nic = self._optional(
            self.azure_client.network.get_network_interface,
            vm_id.resource_group,
            names.network_interface,
        )
        return self._describe(virtual_machine, public_ip, nic)

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """Deallocate the machine: compute stops billing once the host is released."""
        return self._stop(resource_backend_id, "downscale")

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Deallocate the machine."""
        return self._stop(resource_backend_id, "pause")

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Power the machine back on."""
        vm_id = self._parse(resource_backend_id)
        try:
            self.azure_client.compute.start_virtual_machine(
                vm_id.resource_group, vm_id.name
            ).result()
        except AzureClientError as exc:
            logger.warning("Unable to restore Azure machine %s: %s", vm_id.name, exc)
            return False
        return True

    def _delete_machine_objects(
        self, names: VirtualMachineNames, dedicated: bool
    ) -> None:
        """Remove a machine and everything created alongside it.

        Every step tolerates absence, which is what lets one method serve both a
        terminate order and the rollback of a chain that failed part-way: in the
        second case most of these were never created, and in the first an earlier
        interrupted attempt may already have taken some.
        """
        group = names.resource_group
        if dedicated:
            logger.info("Deleting Azure resource group %s", group)
            self._delete_if_present(self.azure_client.resource.delete_resource_group, group)
            return

        network = self.azure_client.network
        logger.info("Deleting Azure virtual machine %s", names.virtual_machine)
        # Reverse order of creation: Azure refuses to delete a subnet an
        # interface still references, or a security group still attached to one.
        self._delete_if_present(
            self.azure_client.compute.delete_virtual_machine, group, names.virtual_machine
        )
        self._delete_if_present(network.delete_network_interface, group, names.network_interface)
        self._delete_if_present(network.delete_network_security_group, group, names.security_group)
        self._delete_if_present(network.delete_public_ip, group, names.public_ip)
        self._delete_if_present(network.delete_subnet, group, names.network, names.subnet)
        self._delete_if_present(network.delete_network, group, names.network)

    def _open_ssh(self, location: str, names: VirtualMachineNames) -> Optional[Any]:
        """Create the security group that lets SSH in, when any range may use it.

        A public address of the Standard SKU — which is what these machines get —
        admits no inbound traffic on its own, so without this the machine is
        unreachable by the one route it offers. The ranges are not defaulted to
        the whole internet: an operator says who may connect.
        """
        if not self.allowed_ssh_ranges:
            logger.warning(
                "No allowed_ssh_ranges configured: machine %s will accept no SSH "
                "connections until a range is added to the offering",
                names.virtual_machine,
            )
            return None
        logger.info(
            "Opening SSH on %s to %s",
            names.virtual_machine,
            ", ".join(self.allowed_ssh_ranges),
        )
        return self.azure_client.network.create_ssh_security_group(
            location, names.resource_group, names.security_group, self.allowed_ssh_ranges
        ).result()

    @staticmethod
    def _delete_if_present(delete: Any, *args: str) -> None:
        """Delete an object that an older resource may never have had."""
        try:
            delete(*args).result()
        except AzureClientError as exc:
            if not exc.not_found:
                raise
            logger.info("Nothing to delete: %s", exc)

    @staticmethod
    def _unique_suffix(waldur_resource: WaldurResource) -> str:
        """Return the part of the resource uuid that keeps names apart."""
        resource_uuid = getattr(waldur_resource, "uuid", None)
        text = str(getattr(resource_uuid, "hex", None) or resource_uuid or "")
        return text[:8]

    def _stop(self, resource_backend_id: str, action: str) -> bool:
        vm_id = self._parse(resource_backend_id)
        try:
            self.azure_client.compute.deallocate_virtual_machine(
                vm_id.resource_group, vm_id.name
            ).result()
        except AzureClientError as exc:
            logger.warning("Unable to %s Azure machine %s: %s", action, vm_id.name, exc)
            return False
        return True

    def _parse(self, backend_id: str) -> VirtualMachineId:
        try:
            return parse_virtual_machine_id(backend_id)
        except ValueError as exc:
            raise BackendError(str(exc)) from exc

    @staticmethod
    def _optional(getter: Any, *args: str) -> Any:
        """Fetch an object that may legitimately be gone, but re-raise anything else."""
        try:
            return getter(*args)
        except AzureClientError as exc:
            if exc.not_found:
                return None
            raise

    @staticmethod
    def _attributes(waldur_resource: WaldurResource) -> dict[str, Any]:
        attributes = getattr(waldur_resource, "attributes", None) or {}
        if hasattr(attributes, "to_dict"):
            attributes = attributes.to_dict()
        return dict(attributes)

    @staticmethod
    def _require(value: Optional[object], what: str) -> str:
        if value in (None, ""):
            msg = f"Azure {what} is set neither on the order nor in backend_settings"
            raise BackendError(msg)
        return str(value)

    @staticmethod
    def _image_reference(image: object) -> dict[str, str]:
        """Turn ``publisher:offer:sku:version`` into the four fields Azure wants."""
        fields = ("publisher", "offer", "sku", "version")
        if isinstance(image, dict):
            missing = set(fields) - set(image)
            if missing:
                msg = f"Azure image reference is missing {', '.join(sorted(missing))}"
                raise BackendError(msg)
            return {key: str(image[key]) for key in fields}

        parts = str(image).split(":")
        if len(parts) != _IMAGE_REFERENCE_PARTS:
            msg = f"Azure image {image!r} is not in publisher:offer:sku:version form"
            raise BackendError(msg)
        return dict(zip(fields, parts))

    def _require_ssh_key(
        self, attributes: dict[str, Any], user_context: Optional[dict], name: str
    ) -> str:
        """Return the SSH key the machine will be reachable by, or refuse.

        A machine has no second credential: the generated password is not
        reported anywhere, and Waldur has no channel to carry it. Provisioning
        without a key would report the order done and hand over a machine nobody
        can log into, which is worse than an order that fails saying why.
        """
        key = self._ssh_public_key(attributes, user_context)
        if key:
            return key
        msg = (
            f"Order for {name} carries no SSH key, and a machine has no other "
            "credential: it would be created unreachable. Set ssh_public_key on "
            "the order, or ssh_key holding the uuid of a service provider key."
        )
        raise BackendError(msg)

    @staticmethod
    def _ssh_public_key(
        attributes: dict[str, Any], user_context: Optional[dict]
    ) -> Optional[str]:
        """Resolve the SSH key, given either literally or as a Waldur key UUID."""
        literal = attributes.get("ssh_public_key")
        if literal:
            return str(literal)

        key_uuid = attributes.get("ssh_key") or attributes.get("ssh_key_uuid")
        if not key_uuid:
            return None
        keys = (user_context or {}).get("ssh_keys") or {}
        public_key = keys.get(str(key_uuid))
        if not public_key:
            logger.warning("SSH key %s is not among the service provider keys", key_uuid)
            return None
        return str(public_key)

    def _describe(self, virtual_machine: Any, public_ip: Any, nic: Any) -> dict[str, Any]:
        """Flatten what Waldur can show about a machine into backend metadata."""
        hardware_profile = getattr(virtual_machine, "hardware_profile", None)
        private_ip = None
        if nic is not None and getattr(nic, "ip_configurations", None):
            private_ip = getattr(nic.ip_configurations[0], "private_ip_address", None)
        return {
            "backend_type": self.backend_type,
            "virtual_machine": {
                "id": getattr(virtual_machine, "id", None),
                "name": getattr(virtual_machine, "name", None),
                "location": getattr(virtual_machine, "location", None),
                "size": getattr(hardware_profile, "vm_size", None),
                "power_state": self._power_state(virtual_machine),
                "public_ip": getattr(public_ip, "ip_address", None),
                "private_ip": private_ip,
            },
        }

    @staticmethod
    def _power_state(virtual_machine: Any) -> Optional[str]:
        """Read the power state out of the instance view, when it was expanded.

        Azure reports it as a status code such as ``PowerState/running``. The
        provisioning state next to it says whether the last operation finished,
        not whether the machine is up.
        """
        instance_view = getattr(virtual_machine, "instance_view", None)
        for status in getattr(instance_view, "statuses", None) or []:
            code = getattr(status, "code", "") or ""
            if code.startswith("PowerState/"):
                return code.split("/", 1)[1]
        return None
