"""What a resource holds on Azure, expressed in the offering's components.

This reports allocation, not consumption: the cores, memory and disk of the size
a machine runs at. Azure charges for those for as long as the machine exists, so
they are what an offering priced per component is priced on.

True consumption records live in the Azure Consumption API, which this plugin
does not read.

A resource whose shape cannot be read is left out of the report entirely rather
than reported as zero. Zero is a legitimate usage value, and the core writes it
straight into Waldur.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from .clients import AzureClient, AzureClientError
from .naming import parse_virtual_machine_id

logger = logging.getLogger(__name__)

TOTAL_USAGE = "TOTAL_ACCOUNT_USAGE"

CORES = "cores"
RAM_MB = "ram_mb"
DISK_MB = "disk_mb"

# What an operator is likely to have called a component when they were not
# thinking about this plugin at all. An explicit ``backend_name`` on the
# component wins over every guess here.
_COMPONENT_GUESSES = {
    "cpu": CORES,
    "cores": CORES,
    "vcpu": CORES,
    "vcores": CORES,
    "ram": RAM_MB,
    "mem": RAM_MB,
    "memory": RAM_MB,
    "disk": DISK_MB,
    "storage": DISK_MB,
}


class AllocationMeter:
    """Turns the shape of an Azure resource into component figures."""

    def __init__(
        self, azure_client: AzureClient, backend_components: dict[str, dict]
    ) -> None:
        """Bind the meter to the Azure clients and the offering's components."""
        self.azure_client = azure_client
        self.backend_components = backend_components
        self._sizes_by_location: dict[str, dict[str, Any]] = {}

    def report(self, resource_backend_ids: list[str]) -> dict:
        """Return per-resource component figures, omitting what cannot be read."""
        report = {}
        for backend_id in resource_backend_ids:
            metrics = self._metrics(backend_id)
            if not metrics:
                continue
            components = self._as_components(metrics)
            if components:
                report[backend_id] = {TOTAL_USAGE: components}
        return report

    def _metrics(self, backend_id: str) -> dict[str, float]:
        """Read the raw Azure figures for one resource."""
        try:
            return self._machine_metrics(backend_id)
        except ValueError:
            logger.warning("Not an Azure resource id this plugin manages: %s", backend_id)
        except AzureClientError as exc:
            # Including a 404: a resource that has just been deleted has no shape
            # to report, and inventing one would bill for it.
            logger.warning("Unable to read the shape of %s: %s", backend_id, exc)
        return {}

    def _machine_metrics(self, backend_id: str) -> dict[str, float]:
        vm_id = parse_virtual_machine_id(backend_id)
        virtual_machine = self.azure_client.compute.get_virtual_machine(
            vm_id.resource_group, vm_id.name
        )
        size_name = getattr(
            getattr(virtual_machine, "hardware_profile", None), "vm_size", None
        )
        location = getattr(virtual_machine, "location", None)
        if not size_name or not location:
            logger.warning("Machine %s reports no size, skipping it", backend_id)
            return {}

        size = self._sizes(location).get(size_name)
        if size is None:
            logger.warning(
                "Size %s is not offered in %s, so machine %s cannot be measured",
                size_name,
                location,
                backend_id,
            )
            return {}

        # The OS disk and the temporary resource disk are both occupied by the
        # machine, so both count towards its disk figure.
        disk_mb = (size.os_disk_size_in_mb or 0) + (size.resource_disk_size_in_mb or 0)
        return {
            CORES: size.number_of_cores,
            RAM_MB: size.memory_in_mb,
            DISK_MB: disk_mb,
        }

    def _sizes(self, location: str) -> dict[str, Any]:
        """Return the machine sizes offered in a region, read once per run."""
        if location not in self._sizes_by_location:
            self._sizes_by_location[location] = {
                size.name: size
                for size in self.azure_client.compute.list_virtual_machine_sizes(location)
            }
        return self._sizes_by_location[location]

    def _as_components(self, metrics: dict[str, float]) -> dict[str, float]:
        """Map Azure figures onto the component names the offering uses."""
        components = {}
        for name, config in self.backend_components.items():
            metric = self._metric_for(name, config)
            if metric is None or metric not in metrics:
                continue
            unit_factor = config.get("unit_factor") or 1
            components[name] = metrics[metric] / unit_factor
        return components

    @staticmethod
    def _metric_for(name: str, config: dict) -> Optional[str]:
        backend_name = config.get("backend_name")
        if backend_name:
            return str(backend_name)
        return _COMPONENT_GUESSES.get(name.lower())
