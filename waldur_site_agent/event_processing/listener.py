"""Message listener module for Waldur STOMP plugin."""

import json
import time
from typing import Callable

import stomp.utils
from stomp.exception import StompException

from waldur_site_agent.backend import logger
from waldur_site_agent.common import structures


def connect_to_stomp_server(
    connection: stomp.StompConnection12, username: str, password: str
) -> None:
    """Connects the existing connection to the STOMP server."""
    while not connection.is_connected():
        try:
            logger.debug("Connecting to STOMP server as user %s", username)
            connection.connect(
                username,
                password,
                wait=True,
                headers={
                    "accept-version": "1.2",
                    "heart-beat": "10000,10000",  # Heartbeat configuration (client, server)
                },
            )
        except StompException as e:
            logger.error(
                "Failed to connect to the STOMP server, retrying in 10 seconds, reason: %s",
                e.__class__.__name__,
            )
            time.sleep(10)


class WaldurListener(stomp.ConnectionListener):
    """Message listener class for the STOMP plugin."""

    def __init__(
        self,
        conn: stomp.WSStompConnection,
        queue: str,
        username: str,
        password: str,
        on_message_callback: Callable,
        offering: structures.Offering,
        user_agent: str,
    ) -> None:
        """Constructor method."""
        self.queue = queue
        self.username = username
        self.password = password
        self.conn = conn
        self.on_message_callback = on_message_callback
        self.queue = queue
        self.offering = offering
        self.user_agent = user_agent

    def on_error(self, frame: stomp.utils.Frame) -> None:
        """Error handler method."""
        logger.error("Received an error %s", frame.body)

    def on_message(self, frame: stomp.utils.Frame) -> None:
        """Message handler method."""
        logger.info("Received a message %s on queue %s", json.loads(frame.body), self.queue)
        try:
            self.on_message_callback(frame, self.offering, self.user_agent)
        except Exception as e:
            logger.exception(
                "Error processing message %s on queue %s: %s", frame.body, self.queue, e
            )

    def on_connected(self, _: stomp.utils.Frame) -> None:
        """Connection handler method."""
        logger.debug("Subscribing to %s", self.queue)
        self.conn.subscribe(
            destination=self.queue, id="waldur-subscription-", ack="auto"
        )  # TODO: try ack='client'

    def on_disconnected(self) -> None:
        """Disconnection handler method."""
        logger.debug("Disconnected from queue %s", self.queue)
        # Reconnecting after missing heartbeat
        connect_to_stomp_server(self.conn, self.username, self.password)
