"""Message listener module for Waldur STOMP plugin.

Reconnection design
--------------------
Reconnection is handled at the **application level** by connect_to_stomp_server(),
NOT by stomp.py's built-in transport-level reconnection.  The transport's
``reconnect_attempts_max`` is set to 1 in event_subscription_manager.py so that
each call to ``connection.connect()`` makes exactly one WebSocket attempt.
Application-level retries with exponential backoff + jitter are layered on top.

Why a single strategy matters:
    stomp.py's WSTransport has its own retry loop inside ``attempt_connection()``
    with linear backoff.  If both that loop AND our application-level loop are
    active, they interleave unpredictably.  Disabling the transport retries
    (``reconnect_attempts_max=1``) gives us full control over timing, logging,
    and failure policy.

Key stomp.py corner cases (validated against RabbitMQ + rabbitmq_web_stomp):

1. ``reconnect_attempts_max=0`` disables ALL connection attempts — the
   ``attempt_connection()`` while-loop condition is
   ``connect_count < reconnect_attempts_max``, so ``0 < 0`` is False and the
   loop body never executes.  Use 1, not 0.

2. ``on_disconnected`` runs in the **receiver thread**.  Calling
   ``connection.connect()`` from there works because ``transport.start()``
   creates a *new* receiver thread, sends the CONNECT frame, and blocks on
   ``wait_for_connection()`` until the new thread processes the CONNECTED
   response.  The old receiver thread resumes once ``on_disconnected`` returns.

3. After a forced disconnect, the receiver loop sets ``self.running = False``
   and calls ``self.cleanup()`` (which sets ``self.socket = None``) before
   firing ``on_disconnected``.  ``transport.start()`` resets
   ``self.running = True`` and creates a fresh WebSocket, so state is clean.

4. WebSocket disconnect detection depends on heartbeats (configured at 10s).
   Server-side AMQP connection closures (e.g. ``rabbitmqctl close_all_connections``)
   do NOT immediately tear down the WebSocket layer — detection can take up to
   two heartbeat intervals (~20s).  Actual network drops or WebSocket close
   frames are detected immediately by the receiver thread.
"""

import json
import random
import threading
import time
from typing import Callable

import stomp.utils
from stomp.exception import ConnectFailedException, StompException

from waldur_site_agent.backend import logger
from waldur_site_agent.common import structures

BACKOFF_INITIAL = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_MAX = 120.0
BACKOFF_JITTER = 0.25
WARN_THRESHOLD = 3
RECONNECT_MAX_RETRIES = 10


def _calculate_backoff(attempt: int) -> float:
    """Calculate exponential backoff delay with jitter.

    Args:
        attempt: Zero-based attempt number.

    Returns:
        Sleep duration in seconds.
    """
    delay = min(BACKOFF_INITIAL * (BACKOFF_FACTOR**attempt), BACKOFF_MAX)
    jitter = delay * BACKOFF_JITTER * random.random()  # noqa: S311
    return delay + jitter


def connect_to_stomp_server(
    connection: stomp.StompConnection12,
    username: str,
    password: str,
    max_retries: int = 0,
) -> None:
    """Connects the existing connection to the STOMP server with retry logic.

    Each attempt calls ``connection.connect()`` which internally calls
    ``transport.start()`` → ``attempt_connection()`` (one WebSocket attempt)
    → ``Protocol12.connect()`` (STOMP CONNECT frame + wait for CONNECTED).

    Only ``StompException`` and ``OSError`` are retried.  Other exceptions
    (e.g. ``TypeError``, ``AttributeError``) propagate immediately so that
    programming errors are not silently swallowed in the retry loop.

    Args:
        connection: STOMP connection object.
        username: STOMP username.
        password: STOMP password.
        max_retries: Maximum number of retry attempts. 0 means infinite retries.

    Raises:
        ConnectFailedException: When max_retries is exceeded without connecting.
    """
    attempt = 0
    while not connection.is_connected():
        if max_retries > 0 and attempt >= max_retries:
            raise ConnectFailedException(
                f"Failed to connect after {max_retries} attempts"
            )

        try:
            logger.debug(
                "Connecting to STOMP server as user %s (attempt %d)",
                username,
                attempt + 1,
            )
            connection.connect(
                username,
                password,
                wait=True,
                headers={
                    "accept-version": "1.2",
                    "heart-beat": "10000,10000",
                },
            )
        except (StompException, OSError) as e:
            backoff = _calculate_backoff(attempt)
            log_fn = logger.warning if attempt < WARN_THRESHOLD else logger.error
            log_fn(
                "Failed to connect to STOMP server (attempt %d), "
                "retrying in %.1fs, reason: %s: %s",
                attempt + 1,
                backoff,
                e.__class__.__name__,
                e,
            )

            attempt += 1
            time.sleep(backoff)


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
        self.offering = offering
        self.user_agent = user_agent
        self._reconnect_lock = threading.Lock()

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
        # Use /amq/queue/ prefix to subscribe to pre-existing queue without attempting
        # declaration. The /queue/ prefix would redeclare and cause PRECONDITION_FAILED
        # errors when queue parameters (x-message-ttl, x-overflow, etc.) don't match.
        destination = f"/amq/queue/{self.queue}"
        logger.debug("Subscribing to %s", destination)
        self.conn.subscribe(
            destination=destination,
            id=self.queue,
            ack="auto",
        )

        logger.debug(
            "Successfully subscribed to queue: %s "
            "(subscription_id: %s, ack_mode: auto)",
            destination,
            self.queue,
        )
        logger.debug(
            "Connection info - host: %s, vhost: %s, ws_path: %s, connected: %s",
            self.conn.transport.current_host_and_port,
            self.conn.transport.vhost,
            self.conn.transport.ws_path,
            self.conn.is_connected(),
        )

    def on_disconnected(self) -> None:
        """Disconnection handler method.

        Called by stomp.py from the **receiver thread** after it detects a
        closed connection and has already cleaned up transport state
        (``running=False``, ``socket=None``).

        Uses a non-blocking lock to prevent concurrent reconnection cascades
        when multiple disconnect callbacks fire simultaneously.  The lock is
        held for the duration of the retry loop (bounded by RECONNECT_MAX_RETRIES
        with exponential backoff), after which it is released regardless of outcome.
        """
        if not self._reconnect_lock.acquire(blocking=False):
            logger.debug(
                "Reconnection already in progress for queue %s, skipping", self.queue
            )
            return

        try:
            logger.warning("Disconnected from queue %s, attempting reconnection", self.queue)
            connect_to_stomp_server(
                self.conn, self.username, self.password, max_retries=RECONNECT_MAX_RETRIES
            )
        except Exception as e:
            logger.error(
                "Reconnection failed for queue %s: %s: %s",
                self.queue,
                e.__class__.__name__,
                e,
            )
        finally:
            self._reconnect_lock.release()
