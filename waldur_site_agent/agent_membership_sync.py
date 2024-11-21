"""Agent responsible for membership control."""

import traceback
from time import sleep
from typing import Dict, List, Set

from waldur_site_agent import common_utils
from waldur_site_agent.backends import logger
from waldur_site_agent.backends.structures import Resource
from waldur_site_agent.processors import OfferingBaseProcessor

from . import (
    MARKETPLACE_SLURM_OFFERING_TYPE,
    WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES,
    Offering,
    WaldurAgentConfiguration,
)


class OfferingMembershipProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related resources and reports membership data to Waldur.
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
                "state": "OK",
                "field": [
                    "backend_id",
                    "uuid",
                    "name",
                    "resource_uuid",
                    "offering_type",
                    "restrict_member_access",
                    "downscaled",
                    "paused",
                ],
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
                marketplace_scope_uuid=resource_data["resource_uuid"],
                restrict_member_access=resource_data.get("restrict_member_access", False),
                downscaled=resource_data.get("downscaled", False),
                paused=resource_data.get("paused", False),
            )
            for resource_data in waldur_resources
        ]

        resource_report = self.resource_backend.pull_resources(waldur_resources_info)

        self._process_resources(resource_report, offering_type)

    def _sync_slurm_resource_users(
        self,
        backend_resource: Resource,
    ) -> None:
        """Syncs users for the resource between SLURM cluster and Waldur."""
        # This method is currently implemented for SLURM backend only
        logger.info("Syncing user list for resource %s", backend_resource.name)
        usernames = backend_resource.users
        local_usernames = set(usernames)
        logger.info("The usernames from the backend: %s", ", ".join(local_usernames))

        # Offering users sync
        # The service fetches offering users from Waldur and pushes them to the cluster
        # If an offering user is not in the team anymore, it will be removed from the backend
        logger.info("Synching offering users")
        team = self.waldur_rest_client.marketplace_provider_resource_get_team(
            backend_resource.marketplace_uuid
        )
        team_user_uuids = {user["uuid"] for user in team}

        offering_users = self.waldur_rest_client.list_remote_offering_users(
            {
                "offering_uuid": self.offering.uuid,
                "is_restricted": False,
            }
        )

        if backend_resource.restrict_member_access:
            # The idea is to remove the existing associations in both sides
            # and avoid creation of new associations
            logger.info("Resource restricted for members, removing all the existing associations")
            existing_offering_user_usernames: Set[str] = {
                offering_user["username"]
                for offering_user in offering_users
                if offering_user["username"] in local_usernames
                and offering_user["user_uuid"] in team_user_uuids
            }

            common_utils.delete_associations_from_waldur_allocation(
                self.waldur_rest_client, backend_resource, existing_offering_user_usernames
            )
            self.resource_backend.remove_users_from_account(
                backend_resource.backend_id, existing_offering_user_usernames
            )
            return

        new_offering_user_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["username"] not in local_usernames
            and offering_user["user_uuid"] in team_user_uuids
        }

        stale_offering_user_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users
            if offering_user["username"] in local_usernames
            and offering_user["user_uuid"] not in team_user_uuids
        }

        common_utils.create_associations_for_waldur_allocation(
            self.waldur_rest_client, backend_resource, new_offering_user_usernames
        )

        self.resource_backend.add_users_to_resource(
            backend_resource.backend_id,
            new_offering_user_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

        common_utils.delete_associations_from_waldur_allocation(
            self.waldur_rest_client, backend_resource, stale_offering_user_usernames
        )

        self.resource_backend.remove_users_from_account(
            backend_resource.backend_id,
            stale_offering_user_usernames,
        )

    def _process_resources(
        self,
        resource_report: Dict[str, Resource],
        offering_type: str,
    ) -> None:
        """Sync membership data for the resource."""
        # Push data to Mastermind using REST client

        for resource_backend_id, backend_resource in resource_report.items():
            logger.info("-" * 30)
            try:
                logger.info("Processing %s", resource_backend_id)
                # Sync users
                if offering_type == MARKETPLACE_SLURM_OFFERING_TYPE:
                    self._sync_slurm_resource_users(backend_resource)

                if backend_resource.paused:
                    logger.info("The resource pausing is requested, processing it")
                    pausing_done = self.resource_backend.pause_resource(backend_resource.backend_id)
                    if pausing_done:
                        logger.info("The pausing is successfully completed")
                    else:
                        logger.warning("The pausing is not done")
                elif backend_resource.downscaled:
                    logger.info("The resource downscaling is requested, processing it")
                    downscaling_done = self.resource_backend.downscale_resource(
                        backend_resource.backend_id
                    )
                    if downscaling_done:
                        logger.info("The downscaling is successfully completed")
                    else:
                        logger.warning("The downscaling is not done")
                else:
                    logger.info(
                        "The resource is not downscaled or paused, "
                        "resetting the QoS to the default one"
                    )
                    restoring_done = self.resource_backend.restore_resource(
                        backend_resource.backend_id
                    )
                    if restoring_done:
                        logger.info("The restoring is successfully completed")
                    else:
                        logger.info("The restoring is skipped")

                resource_metadata = self.resource_backend.get_resource_metadata(
                    backend_resource.backend_id
                )
                self.waldur_rest_client.marketplace_provider_resource_set_backend_metadata(
                    backend_resource.marketplace_uuid, resource_metadata
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
            processor = OfferingMembershipProcessor(offering, user_agent)
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
        sleep(WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60)
