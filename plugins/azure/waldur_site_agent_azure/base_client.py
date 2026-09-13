"""The object the agent core holds as ``AzureBackend.client``.

``BaseClient`` is a resource-lifecycle contract shaped around SLURM-like
backends: create by name, associate users, report usage. Azure provisioning does
not fit through it — a machine needs a resource group, a network, a subnet, a
public address and an interface before it exists — so the backend orchestrates
the per-service clients directly and this adapter covers only what the core
itself calls: "does this resource still exist" and "what is on the backend".

The rest raise ``NotImplementedError``. The inherited ``UnknownClient`` answers
them with empty values instead, and that is worse than a failure here: the core
treats an empty ``get_resource`` as "the resource is missing" and re-provisions
it, which for Azure means paying for a second machine.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from waldur_site_agent.backend.clients import BaseClient
from waldur_site_agent.backend.structures import Association, ClientResource

from .clients import AzureClient, AzureClientError
from .naming import parse_virtual_machine_id

logger = logging.getLogger(__name__)


class AzureCoreClient(BaseClient):
    """Answers the existence questions the agent core asks about a resource."""

    def __init__(self, azure_client: AzureClient) -> None:
        """Bind the adapter to the Azure clients the backend uses."""
        self.azure_client = azure_client

    def list_resources(self) -> list[ClientResource]:
        """List every virtual machine in the subscription."""
        return [
            ClientResource(name=vm.name, backend_id=vm.id)
            for vm in self.azure_client.compute.list_all_virtual_machines()
        ]

    def get_resource(self, resource_id: str) -> Optional[ClientResource]:
        """Return the resource with this ARM id, or None when it is gone.

        Only a 404 counts as absence. Any other failure is re-raised: reporting
        "missing" because Azure was unreachable would invite the core to
        provision a replacement for a machine that is still running.
        """
        try:
            fetch = self._fetch(resource_id)
        except ValueError:
            logger.warning("Not an Azure resource id this plugin manages: %s", resource_id)
            return None

        try:
            resource = fetch()
        except AzureClientError as exc:
            if exc.not_found:
                return None
            raise
        return ClientResource(name=resource.name, backend_id=resource.id)

    def _fetch(self, resource_id: str) -> Callable[[], Any]:
        """Return the getter for the machine this id names."""
        vm_id = parse_virtual_machine_id(resource_id)
        return lambda: self.azure_client.compute.get_virtual_machine(
            vm_id.resource_group, vm_id.name
        )

    def _unsupported(self, action: str) -> NotImplementedError:
        return NotImplementedError(
            f"The Azure plugin does not {action} through the client interface; "
            "provisioning is orchestrated by AzureBackend"
        )

    def create_resource(
        self,
        name: str,
        description: str,
        organization: str,
        parent_name: Optional[str] = None,
    ) -> str:
        """Not supported: creation needs the network objects the backend builds."""
        raise self._unsupported("create resources")

    def delete_resource(self, name: str) -> str:
        """Not supported: deletion needs the resource group from the ARM id."""
        raise self._unsupported("delete resources")

    def set_resource_limits(
        self, resource_id: str, limits_dict: dict[str, int]
    ) -> Optional[str]:
        """Not supported: resizing a machine is not implemented yet."""
        raise self._unsupported("set limits on resources")

    def get_resource_limits(self, resource_id: str) -> dict[str, int]:
        """Not supported: limits live in the offering, not on the machine."""
        raise self._unsupported("read limits from resources")

    def get_resource_user_limits(self, resource_id: str) -> dict[str, dict[str, int]]:
        """Not supported: Azure has no per-user limits on a machine."""
        raise self._unsupported("read per-user limits from resources")

    def set_resource_user_limits(
        self, resource_id: str, username: str, limits_dict: dict[str, int]
    ) -> str:
        """Not supported: Azure has no per-user limits on a machine."""
        raise self._unsupported("set per-user limits on resources")

    # Access to a machine is the SSH key the order carried, granted while the
    # machine was built; there is no membership to add anyone to afterwards. The
    # core calls these on every order that has offering users, and on every
    # membership sync pass, so raising here would log a failure per resource per
    # pass while changing nothing. Reporting "nothing to do" is the truth.
    def get_association(self, user: str, resource_id: str) -> Optional[Association]:
        """Report no association: machine access is the SSH key, not membership."""
        del user, resource_id
        return None

    def create_association(
        self, username: str, resource_id: str, default_account: Optional[str] = None
    ) -> str:
        """Do nothing: machine access is the SSH key, not membership."""
        del resource_id, default_account
        return username

    def delete_association(self, username: str, resource_id: str) -> str:
        """Do nothing: machine access is the SSH key, not membership."""
        del resource_id
        return username

    def get_usage_report(
        self, resource_ids: list[str], timezone: Optional[str] = None
    ) -> list:
        """Not supported: metering is not implemented yet."""
        raise self._unsupported("report usage for resources")

    def list_resource_users(self, resource_id: str) -> list[str]:
        """Return no users: a machine carries no membership list on the backend."""
        del resource_id
        return []
