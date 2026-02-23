"""Classes and functions for event subscription management."""

import threading
from pathlib import Path
from typing import Callable, Optional

import stomp
import urllib3.util
import yaml
from waldur_api_client.api.event_subscriptions import (
    event_subscriptions_destroy,
)
from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models.event_subscription import EventSubscription
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.backend import logger
from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.listener import WaldurListener, connect_to_stomp_server

WALDUR_LISTENER_NAME = "waldur-listener"
OBJECT_TYPE_TO_HANDLER_STOMP = {
    ObservableObjectTypeEnum.ORDER: handlers.on_order_message_stomp,
    ObservableObjectTypeEnum.USER_ROLE: handlers.on_user_role_message_stomp,
    ObservableObjectTypeEnum.RESOURCE: handlers.on_resource_message_stomp,
    ObservableObjectTypeEnum.IMPORTABLE_RESOURCES: handlers.on_importable_resources_message_stomp,
    ObservableObjectTypeEnum.SERVICE_ACCOUNT: handlers.on_account_message_stomp,
    ObservableObjectTypeEnum.COURSE_ACCOUNT: handlers.on_account_message_stomp,
    ObservableObjectTypeEnum.RESOURCE_PERIODIC_LIMITS: (
        handlers.on_resource_periodic_limits_update_stomp
    ),
    ObservableObjectTypeEnum.OFFERING_USER: handlers.on_offering_user_message_stomp,
}
PID_FILE_PATH = "/var/run/waldur_site_agent.pid"


class EventSubscriptionManager:
    """Class responsible for management over event subscriptions and STOMP connections."""

    def __init__(
        self,
        offering: Offering,
        on_connect_callback: Optional[Callable] = None,
        on_message_callback: Optional[Callable] = None,
        user_agent: str = "",
        observable_object_type: str = "",
        global_proxy: str = "",
    ) -> None:
        """Constructor."""
        self.waldur_rest_client = utils.get_client(
            offering.api_url, offering.api_token, user_agent, offering.verify_ssl, global_proxy
        )
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

    def _delete_event_subscription_from_pidfile(self) -> None:
        """Delete event subscription from pidfile."""
        pid_file_content = self._read_pid_file()
        logger.info(
            "Deleting %s event subscription info from %s if exists",
            self.observable_object_type,
            PID_FILE_PATH,
        )
        pid_file_content.pop(self.observable_object_type, None)
        with Path(PID_FILE_PATH).open("w+", encoding="utf-8") as pid_file:
            yaml.dump(pid_file_content, pid_file)

    def setup_stomp_connection(
        self,
        event_subscription: EventSubscription,
        custom_stomp_ws_host: Optional[str] = None,
        custom_stomp_ws_port: Optional[int] = None,
        custom_stomp_ws_path: Optional[str] = None,
    ) -> stomp.WSStompConnection:
        """Create a STOMP connection with the given parameters.

        Args:
            event_subscription (EventSubscription): Event subscription.
            custom_stomp_ws_host (Optional[str], optional): Custom host of the STOMP server.
                Defaults to None.
            custom_stomp_ws_port (Optional[int], optional): Custom port of the STOMP server.
                Defaults to None.
            custom_stomp_ws_path (Optional[str], optional): Custom path of the STOMP server.
                Defaults to None.

        Returns:
            stomp.WSStompConnection: The constructed connection
        """
        logger.info(
            "Setting up STOMP connection for event subscription %s",
            event_subscription.uuid.hex,
        )
        # Mapped to a vhost in RabbitMQ bound to a Waldur User object
        vhost_name = event_subscription.user_uuid.hex
        event_subscription_uuid = event_subscription.uuid.hex
        # Mapped to a username in RabbitMQ bound to the Waldur EventSubscription object
        username = event_subscription_uuid
        queue_name = (
            f"subscription_{event_subscription_uuid}_"
            f"offering_{self.offering.uuid}_{self.observable_object_type}"
        )

        stomp_host = custom_stomp_ws_host or urllib3.util.parse_url(self.offering.api_url).host
        stomp_port = custom_stomp_ws_port or (
            443 if self.waldur_rest_client._verify_ssl else 80
        )  # TODO: Temporary workaround, improve later
        ws_path = custom_stomp_ws_path or "/rmqws-stomp"

        password = self.offering.api_token
        logger.info("Using %s:%s/%s%s broker", stomp_host, stomp_port, vhost_name, ws_path)
        # Transport-level reconnection is intentionally limited to a single attempt.
        # Application-level retries with exponential backoff are handled by
        # connect_to_stomp_server() in listener.py.  See the module docstring
        # there for the full reconnection design and stomp.py corner cases.
        # IMPORTANT: reconnect_attempts_max=0 disables ALL connection attempts
        # (the transport loop condition is `count < max`, so 0 < 0 is False).
        connection = stomp.WSStompConnection(
            host_and_ports=[(stomp_host, stomp_port)],
            ws_path=ws_path,
            vhost=vhost_name,
            reconnect_attempts_max=1,
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

        def create_stomp_thread(callback: Callable) -> threading.Thread:
            thread = threading.Thread(
                target=callback,
                group=None,
                name=f"waldur-{self.observable_object_type}-listener",
            )
            thread.daemon = True  # Don't let thread prevent termination
            thread.start()
            return thread

        connection.transport.override_threading(create_stomp_thread)
        return connection

    def start_stomp_connection(
        self,
        event_subscription: EventSubscription,
        connection: stomp.WSStompConnection,
    ) -> bool:
        """Start STOMP connection."""
        try:
            logger.info(
                "Starting STOMP connection for event subscription %s",
                event_subscription.uuid.hex,
            )
            connect_to_stomp_server(
                connection, event_subscription.uuid.hex, self.offering.api_token
            )
            logger.info(
                "Started STOMP connection for event subscription %s",
                event_subscription.uuid.hex,
            )
        except Exception as e:
            logger.error("Failed to start STOMP connection: %s", e)
            return False
        else:
            return True

    def stop_stomp_connection(self, connection: stomp.WSStompConnection) -> None:
        """Stop the STOMP connection."""
        connection.remove_listener(WALDUR_LISTENER_NAME)
        connection.disconnect()

    def delete_event_subscription(self, event_subscription: EventSubscription) -> None:
        """Delete the event subscription."""
        try:
            logger.info("Deleting event subscription %s", event_subscription["uuid"])
            event_subscriptions_destroy.sync_detailed(
                uuid=event_subscription["uuid"], client=self.waldur_rest_client
            )
            logger.info("Event subscription deleted: %s", event_subscription["uuid"])
            self._delete_event_subscription_from_pidfile()
        except UnexpectedStatus as e:
            logger.error(
                "Failed to delete event subscription %s: %s", event_subscription["uuid"], e
            )
