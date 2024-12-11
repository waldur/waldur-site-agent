"""Module for order processing."""

from __future__ import annotations

import traceback
from time import sleep
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Set, Tuple

from waldur_client import (
    SlurmAllocationState,
    is_uuid,
)

from waldur_site_agent.backends import logger
from waldur_site_agent.processors import OfferingBaseProcessor

if TYPE_CHECKING:
    import types

    from waldur_site_agent.backends.structures import Resource

import json
import signal
import sys
from contextlib import contextmanager
from typing import Union

import paho.mqtt.client as mqtt

from waldur_site_agent.backends.exceptions import BackendError
from waldur_site_agent.event_subscription_processor import EventSubscriptionManager

from . import (
    MARKETPLACE_SLURM_OFFERING_TYPE,
    WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES,
    Offering,
    WaldurAgentConfiguration,
    common_utils,
)


class OfferingOrderProcessor(OfferingBaseProcessor):
    """Class for an offering processing.

    Processes related orders and creates necessary associations.
    """

    def process_offering(self) -> None:
        """Pulls data form Mastermind using REST client and creates objects on the backend."""
        logger.info(
            "Processing offering %s (%s)",
            self.offering.name,
            self.offering.uuid,
        )

        orders = self.waldur_rest_client.list_orders(
            {
                "offering_uuid": self.offering.uuid,
                "state": ["pending-provider", "executing"],
            }
        )

        if len(orders) == 0:
            logger.info("There are no pending or executing orders")
            return

        for order in orders:
            self.process_order(order)

    def get_order_info(self, order_uuid: str) -> Optional[dict]:
        """Get order info from Waldur."""
        try:
            return self.waldur_rest_client.get_order(order_uuid)
        except Exception as e:
            logger.error("Failed to get order %s info: %s", order_uuid, e)
            return None

    def process_order(self, order: dict) -> None:
        """Process a single order."""
        try:
            logger.info(
                "Processing order %s (%s) with state %s",
                order["attributes"].get("name", "N/A"),
                order["uuid"],
                order["state"],
            )

            if order["state"] == "executing":
                logger.info("Order is executing already, no need for approval")
            else:
                logger.info("Approving the order")
                self.waldur_rest_client.marketplace_order_approve_by_provider(order["uuid"])
                logger.info("Refreshing the order")
                order = self.waldur_rest_client.get_order(order["uuid"])

            order_is_done = False

            if order["type"] == "Create":
                order_is_done = self._process_create_order(order)

            if order["type"] == "Update":
                order_is_done = self._process_update_order(order)

            if order["type"] == "Terminate":
                order_is_done = self._process_terminate_order(order)

            # TODO: no need for update of orders for marketplace SLURM offerings
            if order_is_done:
                logger.info("Marking order as done")
                self.waldur_rest_client.marketplace_order_set_state_done(order["uuid"])

                logger.info("The order has been successfully processed")
            else:
                logger.warning("The order processing was not finished, skipping to the next one")

        except Exception as e:
            logger.exception(
                "Error while processing order %s: %s",
                order["uuid"],
                e,
            )
            self.waldur_rest_client.marketplace_order_set_state_erred(
                order["uuid"],
                error_message=str(e),
                error_traceback=traceback.format_exc(),
            )

    def _create_resource(
        self,
        waldur_resource: Dict,
    ) -> Resource | None:
        resource_uuid = waldur_resource["uuid"]
        resource_name = waldur_resource["name"]

        logger.info("Creating resource %s", resource_name)

        if not is_uuid(resource_uuid):
            logger.error("Unexpected resource UUID format, skipping the order")
            return None

        # TODO: figure out how to generalize it
        if (
            waldur_resource["state"] != "Creating"
            and waldur_resource["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE
        ):
            logger.info(
                "Setting SLURM allocation state (%s) to CREATING (current state is %s)",
                waldur_resource["uuid"],
                waldur_resource["state"],
            )
            self.waldur_rest_client.set_slurm_allocation_state(
                resource_uuid, SlurmAllocationState.CREATING
            )

        backend_resource = self.resource_backend.create_resource(waldur_resource)
        if backend_resource.backend_id == "":
            msg = f"Unable to create a backend resource for offering {self.offering}"
            raise BackendError(msg)

        logger.info("Updating resource metadata in Waldur")
        self.waldur_rest_client.marketplace_provider_resource_set_backend_id(
            resource_uuid, backend_resource.backend_id
        )

        if waldur_resource["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE:
            logger.info("Setting SLURM allocation backend ID")
            self.waldur_rest_client.set_slurm_allocation_backend_id(
                waldur_resource["uuid"], backend_resource.backend_id
            )

            logger.info("Updating allocation limits in Waldur")
            self.waldur_rest_client.set_slurm_allocation_limits(
                waldur_resource["uuid"], backend_resource.limits
            )

        return backend_resource

    def _add_users_to_resource(
        self,
        backend_resource: Resource,
    ) -> None:
        logger.info("Adding users to resource")
        logger.info("Fetching Waldur resource team")
        team = self.waldur_rest_client.marketplace_provider_resource_get_team(
            backend_resource.marketplace_uuid
        )
        user_uuids = {user["uuid"] for user in team}

        logger.info("Fetching Waldur offering users")
        offering_users_all = self.waldur_rest_client.list_remote_offering_users(
            {"offering_uuid": self.offering.uuid, "is_restricted": False}
        )
        offering_usernames: Set[str] = {
            offering_user["username"]
            for offering_user in offering_users_all
            if offering_user["user_uuid"] in user_uuids and offering_user["username"] != ""
        }

        logger.info("Adding usernames to resource in backend")
        added_users = self.resource_backend.add_users_to_resource(
            backend_resource.backend_id,
            offering_usernames,
            homedir_umask=self.offering.backend_settings.get("homedir_umask", "0700"),
        )

        common_utils.create_associations_for_waldur_allocation(
            self.waldur_rest_client, backend_resource, added_users
        )

    def _process_create_order(self, order: Dict) -> bool:
        # Wait until resource is created
        attempts = 0
        max_attempts = 4
        while "marketplace_resource_uuid" not in order:
            if attempts > max_attempts:
                logger.error("Order processing timed out")
                return False

            if order["state"] != "executing":
                logger.error("order has unexpected state %s", order["state"])
                return False

            logger.info("Waiting for resource creation...")
            sleep(5)

            order = self.waldur_rest_client.get_order(order["uuid"])
            attempts += 1

        if order["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE:
            # TODO: drop this cycle
            # after removal of waldur_slurm.Allocation model from Mastermind
            attempts = 0
            while order["resource_uuid"] is None:
                if attempts > max_attempts:
                    logger.error("Order processing timed out")
                    return False

                if order["state"] != "executing":
                    logger.error("order has unexpected state %s", order["state"])
                    return False

                logger.info("Waiting for Waldur allocation creation...")
                sleep(5)

                order = self.waldur_rest_client.get_order(order["uuid"])
                attempts += 1

        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(
            order["marketplace_resource_uuid"]
        )

        waldur_resource["project_slug"] = order["project_slug"]
        waldur_resource["customer_slug"] = order["customer_slug"]

        backend_resource = self._create_resource(waldur_resource)
        if backend_resource is None:
            msg = "Unable to create a resource"
            raise BackendError(msg)

        if order["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE:
            logger.info("Updating Waldur resource scope state")
            self.waldur_rest_client.set_slurm_allocation_state(
                waldur_resource["uuid"], SlurmAllocationState.OK
            )

            self._add_users_to_resource(
                backend_resource,
            )

        return True

    def _process_update_order(self, order: dict) -> bool:
        logger.info("Updating limits for %s", order["resource_name"])
        resource_uuid = order["marketplace_resource_uuid"]
        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(resource_uuid)

        if order["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE:
            self.waldur_rest_client.set_slurm_allocation_state(
                resource_uuid, SlurmAllocationState.UPDATING
            )

        resource_backend = common_utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        waldur_resource_backend_id = waldur_resource["backend_id"]

        new_limits = order["limits"]
        if not new_limits:
            logger.error(
                "Order %s (resource %s) with type" + "Update does not include new limits",
                order["uuid"],
                waldur_resource["name"],
            )

        if new_limits:
            resource_backend.set_resource_limits(waldur_resource_backend_id, new_limits)

        if order["offering_type"] == MARKETPLACE_SLURM_OFFERING_TYPE:
            logger.info("Updating Waldur resource scope state")
            self.waldur_rest_client.set_slurm_allocation_state(
                resource_uuid, SlurmAllocationState.OK
            )

        logger.info(
            "The limits for %s were updated successfully from %s to %s",
            waldur_resource["name"],
            order["attributes"]["old_limits"],
            new_limits,
        )
        return True

    def _process_terminate_order(self, order: dict) -> bool:
        logger.info("Terminating resource %s", order["resource_name"])
        resource_uuid = order["marketplace_resource_uuid"]

        waldur_resource = self.waldur_rest_client.get_marketplace_provider_resource(resource_uuid)
        project_slug = order["project_slug"]

        resource_backend = common_utils.get_backend_for_offering(self.offering)
        if resource_backend is None:
            return False

        resource_backend.delete_resource(waldur_resource["backend_id"], project_slug=project_slug)

        logger.info("Allocation has been terminated successfully")
        return True


def on_connect(
    client: mqtt.Client,
    userdata: Dict,
    flags: mqtt.ConnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: Union[mqtt.Properties, None],
) -> None:
    """Order-processing handler for MQTT connection event."""
    del flags, properties
    logger.info("Consumer connected with result code %s", reason_code)
    offering_uuid = userdata["offering"].uuid
    if not reason_code.is_failure:
        event_subscription_uuid = userdata["event_subscription"]["uuid"]
        topic_name = f"subscription/{event_subscription_uuid}/offering/{offering_uuid}/orders"
        logger.info("Subscribing to the topic %s", topic_name)
        client.subscribe(topic_name)
    else:
        logger.error("Consumer connection error (%s): %s", offering_uuid, reason_code.getName())


def on_message(client: mqtt.Client, userdata: Dict, msg: mqtt.MQTTMessage) -> None:
    """Order-processing handler for MQTT message event."""
    del client
    message_text = msg.payload.decode("utf-8")
    message = json.loads(message_text)
    logger.info("Received message: %s on topic %s", message, msg.topic)
    offering = userdata["offering"]
    user_agent = userdata["user_agent"]

    order_uuid = message["order_uuid"]
    processor = OfferingOrderProcessor(offering, user_agent)
    order = processor.get_order_info(order_uuid)
    if order is None:
        logger.error("Failed to process order %s", order_uuid)
        return
    processor.process_order(order)


def start_mqtt_consumers(
    waldur_offerings: List[Offering],
    waldur_user_agent: str,
    waldur_site_agent_mode: str,
) -> Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]]:
    """Start multiple MQTT consumers."""
    mqtt_consumers_map = {}
    for waldur_offering in waldur_offerings:
        if not waldur_offering.mqtt_enabled:
            logger.info("MQTT feature is disabled for the offering")
            continue

        event_subscription_manager = EventSubscriptionManager(
            waldur_offering,
            on_connect,
            on_message,
            waldur_user_agent,
            waldur_site_agent_mode,
        )
        event_subscription = event_subscription_manager.create_event_subscription()
        if event_subscription is None:
            logger.error(
                "Failed to create event subscription for the offering %s (%s)",
                waldur_offering.name,
                waldur_offering.uuid,
            )
            continue

        consumer = event_subscription_manager.start_mqtt_consumer(event_subscription)
        if consumer is None:
            logger.error(
                "Failed to start mqtt consumer for the offering %s (%s)",
                waldur_offering.name,
                waldur_offering.uuid,
            )
            event_subscription_manager.delete_event_subscription(event_subscription)
            continue
        mqtt_consumers_map[(waldur_offering.name, waldur_offering.uuid)] = (
            consumer,
            event_subscription,
            waldur_offering,
        )

    return mqtt_consumers_map


def stop_mqtt_consumers(
    mqtt_consumers_map: Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]],
) -> None:
    """Stop mqtt consumers and delete event subscriptions."""
    for (offering_name, offering_uuid), (
        mqttc,
        event_subscription,
        offering,
    ) in mqtt_consumers_map.items():
        try:
            event_subscription_manager = EventSubscriptionManager(
                offering,
            )
            logger.info("Stopping MQTT consumer for %s (%s)", offering_name, offering_uuid)
            event_subscription_manager.stop_mqtt_consumer(mqttc)
            logger.info("Deleting event subscription for %s (%s)", offering_name, offering_uuid)
            event_subscription_manager.delete_event_subscription(event_subscription)
        except Exception as exc:
            logger.exception("Unable to stop the consumer, reason: %s", exc)


def process_offering(offering: Offering, user_agent: str = "") -> None:
    """Processes the specified offering."""
    processor = OfferingOrderProcessor(offering, user_agent)
    processor.process_offering()


def run_initial_offering_processing(waldur_offerings: List[Offering], user_agent: str = "") -> None:
    """Runs processing of offerings with MQTT feature enabled."""
    logger.info("Processing offerings with MQTT feature enabled")
    for offering in waldur_offerings:
        try:
            if not offering.mqtt_enabled:
                continue

            process_offering(offering, user_agent)
        except Exception as e:
            logger.exception("Error occurred during initial offering process: %s", e)


def start_periodic_offering_processing(
    waldur_offerings: List[Offering], user_agent: str = ""
) -> None:
    """Processes offerings one-by-one periodically."""
    while True:
        logger.info("Number of offerings to process: %s", len(waldur_offerings))
        for offering in waldur_offerings:
            try:
                if offering.mqtt_enabled:
                    logger.info(
                        "Skipping HTTP polling for the offering %s, because it uses mqtt feature",
                        offering.name,
                    )
                    continue

                process_offering(offering, user_agent)
            except Exception as e:
                logger.exception("Unable to process the offering due to the error: %s", e)
        sleep(WALDUR_SITE_AGENT_ORDER_PROCESS_PERIOD_MINUTES * 60)


@contextmanager
def signal_handling(
    mqtt_consumers_map: Dict[Tuple[str, str], Tuple[mqtt.Client, dict, Offering]],
) -> Generator[None, None, None]:
    """Context manager for handling signals gracefully."""

    def signal_handler(signum: int, _: types.FrameType | None) -> None:
        signal_name = signal.Signals(signum).name
        logger.info("Received %s signal. Shutting down gracefully...", signal_name)
        stop_mqtt_consumers(mqtt_consumers_map)
        sys.exit(0)

    # Register signal handlers
    signals = (
        signal.SIGTERM,
        signal.SIGINT,
        signal.SIGTSTP,
        signal.SIGQUIT,
        signal.SIGKILL,
        signal.SIGSTOP,
    )
    original_handlers = {}

    try:
        # Save original handlers and set new ones
        for sig in signals:
            original_handlers[sig] = signal.signal(sig, signal_handler)
        yield
    finally:
        # Restore original handlers
        for sig, handler in original_handlers.items():
            signal.signal(sig, handler)


def start(configuration: WaldurAgentConfiguration) -> None:
    """Starts the main loop for offering processing."""
    try:
        run_initial_offering_processing(
            configuration.waldur_offerings, configuration.waldur_user_agent
        )

        mqtt_consumers_map = start_mqtt_consumers(
            configuration.waldur_offerings,
            configuration.waldur_user_agent,
            configuration.waldur_site_agent_mode,
        )

        with signal_handling(mqtt_consumers_map):
            start_periodic_offering_processing(
                configuration.waldur_offerings, configuration.waldur_user_agent
            )
    except Exception as e:
        logger.error("Error in main process: %s", e)
        if "mqtt_consumers_map" in locals():
            stop_mqtt_consumers(mqtt_consumers_map)
        sys.exit(1)
