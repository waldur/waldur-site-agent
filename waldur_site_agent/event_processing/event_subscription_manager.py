"""Classes and functions for unified event-queue management.

Unified pub/sub (WAL-10011): each agent identity drains ONE RabbitMQ queue
(``consumer_{uuid}``) that receives every observable object type. The object
type of each message is carried in the payload, so a single STOMP connection is
opened per offering and messages are routed to per-type handlers at receive
time (``route_message``) rather than by binding one connection per type.
"""

import json
import threading
from typing import Callable, Optional

import stomp
import urllib3.util
from waldur_api_client.models.observable_object_type_enum import ObservableObjectTypeEnum

from waldur_site_agent.backend import logger
from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering, UnifiedQueue
from waldur_site_agent.event_processing import handlers
from waldur_site_agent.event_processing.listener import WaldurListener, connect_to_stomp_server

WALDUR_LISTENER_NAME = "waldur-listener"
OBJECT_TYPE_TO_HANDLER_STOMP: dict[ObservableObjectTypeEnum, Callable] = {
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
    ObservableObjectTypeEnum.OFFERING_RESOURCES_SYNC: (
        handlers.on_offering_resources_sync_message_stomp
    ),
    ObservableObjectTypeEnum.RESOURCE_API_KEY_ROTATION: (
        handlers.on_resource_api_key_rotation_stomp
    ),
}


def route_message(
    frame: stomp.utils.Frame,
    offering: Offering,
    user_agent: str,
    expose_backend_error_details: bool = True,
) -> None:
    """Dispatch a unified-queue message to the handler for its payload object type.

    Every message on ``consumer_{uuid}`` carries ``object_type`` in its body
    (stamped by Mastermind's build_messages/prepare_messages). This is the
    single-connection replacement for the legacy per-queue handler binding.
    """
    try:
        payload = json.loads(frame.body)
    except (ValueError, TypeError):
        logger.exception("Dropping non-JSON STOMP message: %s", frame.body)
        return
    raw_type = payload.get("object_type")
    try:
        object_type = ObservableObjectTypeEnum(raw_type)
    except ValueError:
        logger.warning("Unknown object_type %r in message, dropping", raw_type)
        return
    handler = OBJECT_TYPE_TO_HANDLER_STOMP.get(object_type)
    if handler is None:
        logger.warning("No handler registered for object_type %s, dropping", object_type)
        return
    handler(frame, offering, user_agent, expose_backend_error_details)


class EventSubscriptionManager:
    """Manages the single unified STOMP connection for one offering."""

    def __init__(
        self,
        offering: Offering,
        on_connect_callback: Optional[Callable] = None,
        on_message_callback: Optional[Callable] = None,
        user_agent: str = "",
        global_proxy: str = "",
        expose_backend_error_details: bool = True,
    ) -> None:
        """Constructor."""
        self.waldur_rest_client = utils.get_client_for_offering(offering, user_agent, global_proxy)
        self.offering = offering
        self.user_agent = user_agent
        self.on_connect_callback = on_connect_callback
        # A custom router may be injected (tests); default is payload routing.
        self.on_message_callback = on_message_callback or route_message
        self.expose_backend_error_details = expose_backend_error_details

    def setup_stomp_connection(
        self,
        unified_queue: UnifiedQueue,
        custom_stomp_ws_host: Optional[str] = None,
        custom_stomp_ws_port: Optional[int] = None,
        custom_stomp_ws_path: Optional[str] = None,
    ) -> stomp.WSStompConnection:
        """Create the STOMP connection to the unified consumer queue.

        Args:
            unified_queue: descriptor returned by register_queue (queue name,
                RMQ username, vhost).
            custom_stomp_ws_host: broker host override (e2e tests dial RabbitMQ
                web_stomp directly instead of the nginx proxy).
            custom_stomp_ws_port: broker port override.
            custom_stomp_ws_path: broker WebSocket path override.

        Returns:
            The constructed (not yet connected) STOMP connection.
        """
        logger.info(
            "Setting up unified STOMP connection for queue %s", unified_queue.queue_name
        )
        # vhost is the owning Waldur user; RMQ username/password authenticate the
        # consumer. For a session-token registration the RMQ password equals the
        # agent's api_token (see resolve_consumer_rmq_password on the server).
        vhost_name = unified_queue.vhost
        username = unified_queue.rmq_username
        password = self.offering.api_token
        queue_name = unified_queue.queue_name

        stomp_host = custom_stomp_ws_host or urllib3.util.parse_url(self.offering.api_url).host
        stomp_port = custom_stomp_ws_port or (
            443 if self.waldur_rest_client._verify_ssl else 80
        )
        ws_path = custom_stomp_ws_path or "/rmqws-stomp"

        logger.info("Using %s:%s/%s%s broker", stomp_host, stomp_port, vhost_name, ws_path)
        # reconnect_attempts_max=1: transport does a single attempt; app-level
        # retries with backoff live in connect_to_stomp_server() (listener.py).
        connection = stomp.WSStompConnection(
            host_and_ports=[(stomp_host, stomp_port)],
            ws_path=ws_path,
            vhost=vhost_name,
            reconnect_attempts_max=1,
            heartbeats=(10000, 10000),
        )
        if self.offering.websocket_use_tls:
            connection.set_ssl(for_hosts=[(stomp_host, stomp_port)])

        connection.set_listener(
            WALDUR_LISTENER_NAME,
            WaldurListener(
                connection,
                queue_name,
                username,
                password,
                self.on_message_callback,
                self.offering,
                self.user_agent,
                expose_backend_error_details=self.expose_backend_error_details,
            ),
        )

        def create_stomp_thread(callback: Callable) -> threading.Thread:
            thread = threading.Thread(
                target=callback,
                group=None,
                name=f"waldur-{self.offering.uuid}-listener",
            )
            thread.daemon = True
            thread.start()
            return thread

        connection.transport.override_threading(create_stomp_thread)
        return connection

    def start_stomp_connection(
        self,
        unified_queue: UnifiedQueue,
        connection: stomp.WSStompConnection,
    ) -> bool:
        """Start (connect) the unified STOMP connection."""
        try:
            logger.info("Starting unified STOMP connection for queue %s", unified_queue.queue_name)
            connect_to_stomp_server(
                connection, unified_queue.rmq_username, self.offering.api_token
            )
            logger.info("Started unified STOMP connection for queue %s", unified_queue.queue_name)
        except Exception as e:
            logger.error("Failed to start STOMP connection: %s", e)
            return False
        else:
            return True

    def stop_stomp_connection(self, connection: stomp.WSStompConnection) -> None:
        """Stop the STOMP connection.

        The unified queue itself is left in place so it is reused on the next
        start (register_queue is idempotent).
        """
        connection.remove_listener(WALDUR_LISTENER_NAME)
        connection.disconnect()
