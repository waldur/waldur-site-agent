"""Classes and functions for event subscription management."""

import ssl
from typing import Callable, Optional

import paho.mqtt.client as mqtt
import urllib3.util
from waldur_client import WaldurClient

from waldur_site_agent.backends import logger
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing.structures import EventSubscription, UserData


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
        self.waldur_rest_client: WaldurClient = WaldurClient(
            offering.api_url, offering.api_token, user_agent
        )
        self.offering = offering
        self.user_agent = user_agent
        self.on_connect_callback = on_connect_callback
        self.on_message_callback = on_message_callback
        self.observable_object_type = observable_object_type

    def create_event_subscription(self) -> Optional[EventSubscription]:
        """Create event subscription."""
        try:
            logger.info(
                "Creating event subscription for offering %s (%s), object type: %s",
                self.offering.name,
                self.offering.uuid,
                self.observable_object_type,
            )
            event_subscription = self.waldur_rest_client.create_event_subscription(
                observable_objects=[
                    {
                        "object_type": self.observable_object_type,
                    }
                ],
                description=f"Event subscription for waldur site agent {self.user_agent},"
                f"observable object type: {self.observable_object_type}",
            )
        except Exception as e:
            logger.error("Failed to create event subscription: %s", e)
            return None
        else:
            logger.info("Event subscription created: %s", event_subscription["uuid"])
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
            self.waldur_rest_client.delete_event_subscription(event_subscription["uuid"])
            logger.info("Event subscription deleted: %s", event_subscription["uuid"])
        except Exception as e:
            logger.error(
                "Failed to delete event subscription %s: %s", event_subscription["uuid"], e
            )
