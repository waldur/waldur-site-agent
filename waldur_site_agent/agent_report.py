"""Agent responsible for usage and limits reporting."""

import datetime
import traceback
from time import sleep
from typing import Dict, List

from waldur_client import (
    ComponentUsage,
)

from waldur_site_agent.backends import logger, utils
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.processors import OfferingBaseProcessor

from . import (
    MARKETPLACE_SLURM_OFFERING_TYPE,
    WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES,
    Offering,
    WaldurAgentConfiguration,
    common_utils,
)


class OfferingReportProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related resource and reports computing data to Waldur.
    """

    def process_offering(self) -> None:
        """Processes offering and reports resources usage to Waldur."""
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )
        self._print_current_user()

        waldur_offering = self.waldur_rest_client._get_offering(self.offering.uuid)
        common_utils.extend_backend_components(self.offering, waldur_offering["components"])

        waldur_resources = self.waldur_rest_client.filter_marketplace_provider_resources(
            {
                "offering_uuid": self.offering.uuid,
                "state": ["OK", common_utils.RESOURCE_ERRED_STATE],
                "field": ["backend_id", "uuid", "name", "offering_type", "state"],
            }
        )

        if len(waldur_resources) == 0:
            logger.info("No resources to process")
            return

        offering_type = waldur_resources[0].get("offering_type", "")

        waldur_resources_info = [
            Resource(
                name=resource_data["name"],
                backend_id=resource_data["backend_id"],
                marketplace_uuid=resource_data["uuid"],
                backend_type=self.offering.backend_type,
                state=resource_data["state"],
            )
            for resource_data in waldur_resources
        ]

        resource_report = self.resource_backend.pull_resources(waldur_resources_info)

        # TODO: make generic
        if offering_type == MARKETPLACE_SLURM_OFFERING_TYPE:
            # Allocations existing in Waldur but missing in SLURM cluster
            missing_resources = [
                Resource(
                    marketplace_uuid=resource_info["uuid"],
                    backend_id=resource_info["backend_id"],
                )
                for resource_info in waldur_resources
                if resource_info["backend_id"] not in set(resource_report.keys())
                and resource_info["state"] != common_utils.RESOURCE_ERRED_STATE
            ]
            logger.info("Number of missing resources %s", len(missing_resources))
            if len(missing_resources) > 0:
                common_utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    missing_resources,
                    {"error_message": "The resource is missing on the backend"},
                )

        self._process_resources(resource_report)

    def _submit_total_usage_for_resource(
        self,
        backend_resource: Resource,
        total_usage: Dict[str, float],
        waldur_components: List[Dict],
    ) -> None:
        """Reports total usage for a backend resource to Waldur."""
        logger.info("Setting usages: %s", total_usage)
        resource_uuid = backend_resource.marketplace_uuid
        plan_periods = self.waldur_rest_client.marketplace_provider_resource_get_plan_periods(
            resource_uuid
        )

        if len(plan_periods) == 0:
            logger.warning(
                "A corresponding ResourcePlanPeriod for resource %s was not found",
                backend_resource.name,
            )
            return

        plan_period = plan_periods[0]
        component_types = [component["type"] for component in waldur_components]
        missing_components = set(total_usage) - set(component_types)

        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )

        usage_objects = [
            ComponentUsage(type=component, amount=amount)
            for component, amount in total_usage.items()
            if component in component_types
        ]
        self.waldur_rest_client.create_component_usages(plan_period["uuid"], usage_objects)

    def _submit_user_usage_for_resource(
        self,
        username: str,
        user_usage: Dict[str, float],
        waldur_component_usages: List[Dict],
    ) -> None:
        """Reports per-user usage for a backend resource to Waldur."""
        logger.info("Setting usages for %s", username)
        component_usage_types = [
            component_usage["type"] for component_usage in waldur_component_usages
        ]
        missing_components = set(user_usage) - set(component_usage_types)

        if missing_components:
            logger.warning(
                "The following components are not found in Waldur: %s",
                ", ".join(missing_components),
            )

        offering_users = self.waldur_rest_client.list_remote_offering_users(
            {"username": username, "query": self.offering.uuid}
        )
        offering_user_uuid = None

        if len(offering_users) > 0:
            offering_user_uuid = offering_users[0]["uuid"]

        for component_usage in waldur_component_usages:
            component_type = component_usage["type"]
            usage = user_usage[component_type]
            logger.info(
                "Submitting usage for username %s: %s -> %s",
                username,
                component_type,
                usage,
            )
            self.waldur_rest_client.create_component_user_usage(
                component_usage["uuid"], usage, username, offering_user_uuid
            )

    def _process_resources(
        self,
        resource_report: Dict[str, Resource],
    ) -> None:
        """Processes usage report for the resource."""
        waldur_offering = self.waldur_rest_client._get_offering(self.offering.uuid)
        month_start = utils.month_start(datetime.datetime.now()).date()

        # TODO: this part is not generic yet, rather SLURM-specific
        for resource_backend_id, backend_resource in resource_report.items():
            try:
                logger.info("Processing %s", resource_backend_id)
                usages: Dict[str, Dict[str, float]] = backend_resource.usage

                # Set resource state OK if it is erred
                if backend_resource.state == common_utils.RESOURCE_ERRED_STATE:
                    self.waldur_rest_client.marketplace_provider_resource_set_as_ok(
                        backend_resource.marketplace_uuid
                    )

                # Submit usage
                total_usage = usages.pop("TOTAL_ACCOUNT_USAGE")
                self._submit_total_usage_for_resource(
                    backend_resource,
                    total_usage,
                    waldur_offering["components"],
                )

                # Skip the following actions if the dict is empty
                if not usages:
                    continue

                waldur_component_usages = self.waldur_rest_client.list_component_usages(
                    backend_resource.marketplace_uuid, date_after=month_start
                )

                logger.info("Setting per-user usages")
                for username, user_usage in usages.items():
                    self._submit_user_usage_for_resource(
                        username, user_usage, waldur_component_usages
                    )
            except Exception as e:
                logger.exception(
                    "Waldur REST client error while processing allocation %s: %s",
                    resource_backend_id,
                    e,
                )
                error_traceback = traceback.format_exc()
                common_utils.mark_waldur_resources_as_erred(
                    self.waldur_rest_client,
                    [backend_resource],
                    error_details={
                        "error_message": str(e),
                        "error_traceback": error_traceback,
                    },
                )


def process_offerings(waldur_offerings: List[Offering], user_agent: str = "") -> None:
    """Processes list of offerings."""
    logger.info("Number of offerings to process: %s", len(waldur_offerings))
    for offering in waldur_offerings:
        try:
            processor = OfferingReportProcessor(offering, user_agent)
            processor.process_offering()
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)


def start(configuration: WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    logger.info("Synching data to Waldur")
    while True:
        try:
            process_offerings(configuration.waldur_offerings, configuration.waldur_user_agent)
        except Exception as e:
            logger.exception("The application crashed due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_REPORT_PERIOD_MINUTES * 60)
