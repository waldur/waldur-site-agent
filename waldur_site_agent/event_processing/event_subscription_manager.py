"""Classes and functions for event subscription management."""

import ssl
from pathlib import Path
from typing import Callable, Optional

import paho.mqtt.client as mqtt
import stomp
import urllib3.util
import yaml
from waldur_api_client.api.event_subscriptions import (
    event_subscriptions_create,
    event_subscriptions_destroy,
    event_subscriptions_retrieve,
)
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.event_subscription import (
    EventSubscription as ClientEventSubscriptionObject,
)
from waldur_api_client.models.event_subscription_request import EventSubscriptionRequest

from waldur_site_agent.backends import logger
from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.listener import WaldurListener, connect_to_stomp_server
from waldur_site_agent.event_processing.structures import EventSubscription, UserData

WALDUR_LISTENER_NAME = "waldur-listener"
OBJECT_TYPE_TO_HANDLER_STOMP = {
    "order": handlers.on_order_message_stomp,
    "user_role": handlers.on_user_role_message_stomp,
    "resource": handlers.on_resource_message_stomp,
}
PID_FILE_PATH = "/var/run/waldur_site_agent.pid"


class EventSubscriptionManager:
    """Class responsible for management over event subscriptions and MQTT clients."""

    def __init__(
        self,
        offering: Offering,
        on_connect_callback: Optional[Callable] = None,
        on_message_callback: Optional[Callable] = None,
        user_agent: str = "",
        observable_object_type: str = "",
    ) -> None:
        """Constructor."""
        self.waldur_rest_client = utils.get_client(offering.api_url, offering.api_token, user_agent)
        self.offering = offering
        self.user_agent = user_agent
        self.on_connect_callback = on_connect_callback
        self.on_message_callback = on_message_callback
        self.observable_object_type = observable_object_type

    def _read_pid_file(self) -> dict:
        content = {}
        logger.info("Reading event subscriptions info from %s", PID_FILE_PATH)
        try:
            with Path(PID_FILE_PATH).open("r", encoding="utf-8") as pid_file:
                content = yaml.safe_load(pid_file)
        except FileNotFoundError:
            return content
        return content or {}

    def _write_event_subscription_info_to_pidfile(
        self, event_subscription: EventSubscription
    ) -> None:
        """Write event subscription to file."""
        pid_file_content = self._read_pid_file()
        logger.info(
            "Writing %s event subscription info to %s", self.observable_object_type, PID_FILE_PATH
        )

        with Path(PID_FILE_PATH).open("w+", encoding="utf-8") as pid_file:
            payload = {self.observable_object_type: event_subscription["uuid"]}
            pid_file_content.update(payload)
            yaml.dump(pid_file_content, pid_file)

    def create_event_subscription(self) -> Optional[EventSubscription]:
        """Create event subscription."""
        try:
            logger.info(
                "Creating event subscription for offering %s (%s), object type: %s",
                self.offering.name,
                self.offering.uuid,
                self.observable_object_type,
            )
            request_body = EventSubscriptionRequest(
                description=(
                    f"Event subscription for waldur site agent {self.user_agent}, "
                    f"observable object type: {self.observable_object_type}"
                ),
                observable_objects=[
                    {
                        "object_type": self.observable_object_type,
                    }
                ],
            )
            event_subscription = event_subscriptions_create.sync(
                client=self.waldur_rest_client, body=request_body
            )

        except UnexpectedStatus as e:
            logger.error("Failed to create event subscription: %s", e)
            return None
        else:
            logger.info("Event subscription created: %s", event_subscription.uuid)
            return EventSubscription(
                uuid=event_subscription.uuid.hex,
                user_uuid=event_subscription.user_uuid.hex,
                observable_objects=list(event_subscription.observable_objects),
            )

    def get_or_create_event_subscription(self) -> Optional[EventSubscription]:
        """Ger or create event subscription."""
        try:
            pid_file_content = self._read_pid_file()
            event_subscription_uuid = pid_file_content.get(self.observable_object_type)
            if event_subscription_uuid is not None:
                logger.info(
                    "Fetching the existing event subscription %s info", event_subscription_uuid
                )
                # Get the event subscription
                event_subscription: ClientEventSubscriptionObject = (
                    event_subscriptions_retrieve.sync(
                        client=self.waldur_rest_client, uuid=event_subscription_uuid
                    )
                )
                return EventSubscription(
                    uuid=event_subscription.uuid.hex,
                    user_uuid=event_subscription.user_uuid.hex,
                    observable_objects=list(event_subscription.observable_objects),
                )
        except Exception as e:
            logger.warning("Unable to get an event subscription %s: %s", event_subscription_uuid, e)

        event_subscription = self.create_event_subscription()
        if event_subscription:
            self._write_event_subscription_info_to_pidfile(event_subscription)
        return event_subscription

    def _setup_mqtt_consumer(self, event_subscription: EventSubscription) -> mqtt.Client:
        logger.info(
            "Setting up MQTT consumer for event subscription %s",
            event_subscription["uuid"],
        )
        mqtt_client = mqtt.Client(
            client_id=f"paho-mqtt-client-{event_subscription['uuid']}-{self.observable_object_type}",
            transport="websockets",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        mqtt_client.ws_set_options(path="/rmqws", headers=None)
        vhost_name = event_subscription["user_uuid"]
        username = event_subscription["uuid"]
        username_full = f"{vhost_name}:{username}"
        password = self.offering.api_token
        mqtt_client.username_pw_set(username_full, password)
        if self.offering.websocket_use_tls:
            mqtt_client.tls_set()
        else:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            mqtt_client.tls_set_context(ssl_context)
            mqtt_client.tls_insecure_set(True)

        mqtt_client.on_connect = self.on_connect_callback
        mqtt_client.on_message = self.on_message_callback
        topic_postfix = self.observable_object_type
        userdata: UserData = {
            "event_subscription": event_subscription,
            "offering": self.offering,
            "user_agent": self.user_agent,
            "topic_postfix": topic_postfix,
        }
        mqtt_client.user_data_set(userdata)
        return mqtt_client

    def _setup_stomp_connection(
        self, event_subscription: EventSubscription
    ) -> stomp.WSStompConnection:
        logger.info(
            "Setting up STOMP connection for event subscription %s",
            event_subscription["uuid"],
        )
        # Mapped to a vhost in RabbitMQ bound to a Waldur User object
        vhost_name = event_subscription["user_uuid"]
        event_subscription_uuid = event_subscription["uuid"]
        # Mapped to a username in RabbitMQ bound to an the Waldur EventSubscription object
        username = event_subscription_uuid
        queue_name = (
            f"subscription_{event_subscription_uuid}_"
            f"offering_{self.offering.uuid}_{self.observable_object_type}"
        )
        stomp_host = urllib3.util.parse_url(self.offering.api_url).host
        stomp_port = 443
        password = self.offering.api_token
        connection = stomp.WSStompConnection(
            host_and_ports=[(stomp_host, stomp_port)], ws_path="/rmqws-stomp", vhost=vhost_name
        )
        if self.offering.websocket_use_tls:
            connection.set_ssl(for_hosts=[(stomp_host, stomp_port)])

        callback_function = OBJECT_TYPE_TO_HANDLER_STOMP[self.observable_object_type]
        connection.set_listener(
            WALDUR_LISTENER_NAME,
            WaldurListener(
                connection,
                queue_name,
                username,
                password,
                callback_function,
                self.offering,
                self.user_agent,
            ),
        )
        return connection

    def start_stomp_connection(
        self, event_subscription: EventSubscription
    ) -> Optional[stomp.Connection]:
        """Start STOMP connection."""
        connection = self._setup_stomp_connection(event_subscription)
        try:
            logger.info(
                "Starting STOMP connection for event subscription %s",
                event_subscription["uuid"],
            )
            connect_to_stomp_server(connection, event_subscription["uuid"], self.offering.api_token)
        except Exception as e:
            logger.error("Failed to start STOMP connection: %s", e)
            return None
        else:
            return connection

    def stop_stomp_connection(self, connection: stomp.WSStompConnection) -> None:
        """Stop the STOMP connection."""
        connection.remove_listener(WALDUR_LISTENER_NAME)
        connection.disconnect()

    def start_mqtt_consumer(self, event_subscription: EventSubscription) -> Optional[mqtt.Client]:
        """Start MQTT consumer."""
        try:
            mqtt_host = urllib3.util.parse_url(self.offering.api_url).host
            mqtt_port = 443
            logger.info(
                "Starting consumer for %s (%s), MQTT WS address: ws://%s:%s",
                self.offering.name,
                self.offering.uuid,
                mqtt_host,
                mqtt_port,
            )

            mqtt_client = self._setup_mqtt_consumer(event_subscription)

            try:
                topic_name = (
                    f"subscription/{event_subscription['uuid']}/offering/"
                    f"{self.offering.uuid}/{self.observable_object_type}"
                )
                logger.info("Connecting the consumer to the mqtt server, topic: %s", topic_name)
                mqtt_client.connect(mqtt_host, mqtt_port)
            except (ConnectionRefusedError, TimeoutError) as e:
                logger.error("Failed to connect to MQTT broker: %s", e)
                return None
            except Exception as e:
                logger.error("Unexpected error while connecting to MQTT broker: %s", e)
                return None

            try:
                logger.info("Starting the consumer")
                mqtt_client.loop_start()
                logger.info("MQTT consumer started")
            except Exception as e:
                logger.error("Failed to start MQTT consumer loop: %s", e)
                mqtt_client.disconnect()
                return None
            else:
                return mqtt_client

        except Exception as e:
            logger.error("Failed to start MQTT consumer: %s", e)
            return None

    def stop_mqtt_consumer(self, mqttc: mqtt.Client) -> None:
        """Stop the MQTT consumer."""
        mqttc.loop_stop()
        mqttc.disconnect()

    def delete_event_subscription(self, event_subscription: EventSubscription) -> None:
        """Delete the event subscription."""
        try:
            logger.info("Deleting event subscription %s", event_subscription["uuid"])
            event_subscriptions_destroy.sync_detailed(
                uuid=event_subscription["uuid"], client=self.waldur_rest_client
            )
            logger.info("Event subscription deleted: %s", event_subscription["uuid"])
        except UnexpectedStatus as e:
            logger.error(
                "Failed to delete event subscription %s: %s", event_subscription["uuid"], e
            )
