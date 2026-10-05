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

4. WebSocket disconnect detection depends on heartbeats (configured at 10s via
   ``heartbeats=(10000, 10000)`` on the ``WSStompConnection`` constructor).
   IMPORTANT: the heartbeats must be set on the *constructor*, not just in
   the CONNECT frame headers — otherwise stomp.py's HeartbeatListener never
   starts its send/receive loop, and RabbitMQ disconnects every ~30s.
   Server-side AMQP connection closures (e.g. ``rabbitmqctl close_all_connections``)
   do NOT immediately tear down the WebSocket layer — detection can take up to
   two heartbeat intervals (~20s).  Actual network drops or WebSocket close
   frames are detected immediately by the receiver thread.

Message handling design
-----------------------
Handlers do not run on the receiver thread. While a handler runs there, the
receiver reads neither frames nor heartbeats, so a handler that outlives the
heartbeat window gets the connection dropped. ``on_message`` only queues the
frame; one worker thread per listener (that is, per queue) runs the handlers in
arrival order, so per-queue ordering is kept.

The queue is subscribed with ``ack="client-individual"`` and a bounded
``prefetch-count``. The worker acks a message after its handler returns, and also
after the handler raised (the error is logged; a failing message is not retried).
A message the broker delivered but the agent has not acked is requeued by
RabbitMQ when the connection drops, so it is redelivered after the reconnect.

Every frame is tagged with the connection it arrived on. When that connection is
gone, the worker skips frames it has not started — the broker redelivers them on
the new connection — and never acks with an id from the old connection.
"""

import json
import random
import threading
import time
from contextlib import suppress
from queue import Empty, Queue
from typing import Callable, Optional

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
# Upper bound for one attempt: the WebSocket handshake (socket timeout on the
# connection) and the wait for the broker's CONNECTED frame.
CONNECT_TIMEOUT = 30.0
# Connect attempts made at startup and per watchdog setup retry before giving
# the connection to the watchdog; never unbounded, so the main loop always starts.
STARTUP_CONNECT_ATTEMPTS = 3
_CONNECTED_POLL_INTERVAL = 0.1
# Unacknowledged messages the broker may hand one consumer at a time. Messages
# beyond it wait in the durable queue instead of in this process.
STOMP_PREFETCH_COUNT = 1
STOMP_ACK_MODE = "client-individual"
_WORKER_POLL_INTERVAL = 1.0


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
    connect_timeout: Optional[float] = None,
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
        connect_timeout: Seconds to wait for the CONNECTED frame per attempt.
            None waits indefinitely (stomp.py's own ``wait=True``).

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
                wait=connect_timeout is None,
                headers={
                    "accept-version": "1.2",
                },
            )
            if connect_timeout is not None:
                _wait_until_connected(connection, connect_timeout)
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


def _wait_until_connected(connection: stomp.StompConnection12, timeout: float) -> None:
    """Wait for CONNECTED, giving up after ``timeout`` seconds.

    stomp.py's ``wait_for_connection(timeout)`` only changes how often it re-checks
    and never gives up, so a broker that accepts the socket but never answers
    would block the caller forever.
    """
    deadline = time.monotonic() + timeout
    while not connection.is_connected():
        if getattr(connection.transport, "connection_error", False):
            msg = "Broker rejected the STOMP CONNECT"
            raise ConnectFailedException(msg)
        if time.monotonic() >= deadline:
            with suppress(Exception):
                connection.transport.disconnect_socket()
            msg = f"No CONNECTED frame within {timeout:.0f}s"
            raise ConnectFailedException(msg)
        time.sleep(_CONNECTED_POLL_INTERVAL)


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
        expose_backend_error_details: bool = True,
        on_queue_missing: Optional[Callable[[], None]] = None,
    ) -> None:
        """Constructor method.

        ``on_queue_missing`` re-registers the consumer queue; it is called (off the
        receiver thread) when the broker reports the queue is gone, so the next
        reconnect can subscribe again instead of being closed after CONNECTED.
        """
        self.queue = queue
        self.username = username
        self.password = password
        self.conn = conn
        self.on_message_callback = on_message_callback
        self.offering = offering
        self.user_agent = user_agent
        self.expose_backend_error_details = expose_backend_error_details
        self._reconnect_lock = threading.Lock()
        self.on_queue_missing = on_queue_missing
        self._reregister_lock = threading.Lock()
        # Frames waiting for the worker, each tagged with the connection it came on.
        self._messages: Queue[Optional[tuple[int, stomp.utils.Frame]]] = Queue()
        self._connection_generation = 0
        self._generation_lock = threading.Lock()
        self._worker: Optional[threading.Thread] = None
        self._worker_start_lock = threading.Lock()
        self._closed = threading.Event()
        # time.time() when the running handler started; None while idle.
        self._handler_started_at: Optional[float] = None

    def on_error(self, frame: stomp.utils.Frame) -> None:
        """Error handler method."""
        message = (frame.headers or {}).get("message", "")
        logger.error("Received an error %s %s on queue %s", message, frame.body, self.queue)
        if self.on_queue_missing is not None and "NOT_FOUND" in message:
            threading.Thread(
                target=self._reregister_queue,
                name=f"waldur-{self.queue}-reregister",
                daemon=True,
            ).start()

    def _reregister_queue(self) -> None:
        if not self._reregister_lock.acquire(blocking=False):
            return
        try:
            logger.warning("Queue %s is missing on the broker, registering it again", self.queue)
            self.on_queue_missing()  # type: ignore[misc]
        except Exception:
            logger.exception("Unable to register queue %s again", self.queue)
        finally:
            self._reregister_lock.release()

    def reconnect_in_progress(self) -> bool:
        """True while ``on_disconnected`` or the watchdog is reconnecting."""
        return self._reconnect_lock.locked()

    def on_message(self, frame: stomp.utils.Frame) -> None:
        """Queue the message for the worker thread.

        Runs in stomp.py's receiver thread, which must keep reading frames and
        heartbeats, so no handler runs here and nothing may escape.
        """
        try:
            with self._generation_lock:
                generation = self._connection_generation
            self._messages.put((generation, frame))
            self.ensure_worker()
        except Exception:
            logger.exception("Unable to queue a message on queue %s", self.queue)

    def ensure_worker(self) -> None:
        """Start the worker thread unless it is running (or the listener is closed)."""
        with self._worker_start_lock:
            if self._closed.is_set() or (self._worker is not None and self._worker.is_alive()):
                return
            self._worker = threading.Thread(
                target=self._work, name=f"waldur-{self.queue}-worker", daemon=True
            )
            self._worker.start()

    def _work(self) -> None:
        while not self._closed.is_set():
            try:
                item = self._messages.get(timeout=_WORKER_POLL_INTERVAL)
            except Empty:
                continue
            if item is None:
                return
            generation, frame = item
            try:
                if not self._is_current(generation):
                    logger.info(
                        "Skipping a message from a dropped connection on queue %s; "
                        "the broker redelivers it",
                        self.queue,
                    )
                    continue
                self._handle(frame)
                self._ack(generation, frame)
            except BaseException as e:
                if self._closed.is_set() and isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
                logger.exception("Unexpected error in the worker of queue %s", self.queue)

    def handler_running_since(self) -> Optional[float]:
        """When the handler now running started (``time.time()``), or None while idle."""
        return self._handler_started_at

    def _is_current(self, generation: int) -> bool:
        with self._generation_lock:
            return generation == self._connection_generation

    def _handle(self, frame: stomp.utils.Frame) -> None:
        try:
            logger.info("Received a message %s on queue %s", json.loads(frame.body), self.queue)
        except ValueError:
            logger.warning("Received a non-JSON message on queue %s: %r", self.queue, frame.body)
        self._handler_started_at = time.time()
        try:
            self.on_message_callback(
                frame, self.offering, self.user_agent, self.expose_backend_error_details
            )
        except BaseException as e:
            # Anything a handler raises is logged and the message still acked; only
            # an interpreter exit during shutdown is let through.
            if self._closed.is_set() and isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            logger.exception(
                "Error processing message %s on queue %s: %s", frame.body, self.queue, e
            )
        finally:
            self._handler_started_at = None

    def _ack(self, generation: int, frame: stomp.utils.Frame) -> None:
        """Ack on the connection the message came on; a replaced one redelivers it.

        The generation lock is held across the check and the ack, so a reconnect
        cannot slip in between and get an id from the old connection.
        """
        ack_id = (frame.headers or {}).get("ack")
        if not ack_id:
            # STOMP 1.2 acks by the ``ack`` header; ``message-id`` is not valid here.
            logger.warning("Message on queue %s has no ack header, cannot ack it", self.queue)
            return
        with self._generation_lock:
            if generation != self._connection_generation:
                logger.info(
                    "Connection of queue %s was replaced while handling a message; "
                    "the broker redelivers it",
                    self.queue,
                )
                return
            try:
                self.conn.ack(ack_id)
            except Exception as e:
                logger.warning("Unable to ack a message on queue %s: %s", self.queue, e)

    def close(self) -> None:
        """Stop the worker; frames it has not handled are redelivered by the broker."""
        self._closed.set()
        self._messages.put(None)

    def _drop_connection_generation(self) -> None:
        with self._generation_lock:
            self._connection_generation += 1

    def on_connected(self, _: stomp.utils.Frame) -> None:
        """Connection handler method."""
        # Use /amq/queue/ prefix to subscribe to pre-existing queue without attempting
        # declaration. The /queue/ prefix would redeclare and cause PRECONDITION_FAILED
        # errors when queue parameters (x-message-ttl, x-overflow, etc.) don't match.
        destination = f"/amq/queue/{self.queue}"
        # A new connection: frames still queued from the previous one are stale.
        self._drop_connection_generation()
        logger.debug("Subscribing to %s", destination)
        self.conn.subscribe(
            destination=destination,
            id=self.queue,
            ack=STOMP_ACK_MODE,
            headers={"prefetch-count": str(STOMP_PREFETCH_COUNT)},
        )

        logger.debug(
            "Successfully subscribed to queue: %s "
            "(subscription_id: %s, ack_mode: %s, prefetch: %s)",
            destination,
            self.queue,
            STOMP_ACK_MODE,
            STOMP_PREFETCH_COUNT,
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
        logger.warning("Disconnected from queue %s, attempting reconnection", self.queue)
        # Unacked messages of the dropped connection are requeued by the broker.
        self._drop_connection_generation()
        self.reconnect(max_retries=RECONNECT_MAX_RETRIES)

    def reconnect(self, max_retries: int, connect_timeout: float = CONNECT_TIMEOUT) -> bool:
        """Reconnect unless a reconnection is already in progress.

        Shared by ``on_disconnected`` and the event-mode watchdog, which retries a
        connection this listener gave up on. Returns True when the connection is up.
        """
        if not self._reconnect_lock.acquire(blocking=False):
            logger.debug(
                "Reconnection already in progress for queue %s, skipping", self.queue
            )
            return False

        try:
            connect_to_stomp_server(
                self.conn,
                self.username,
                self.password,
                max_retries=max_retries,
                connect_timeout=connect_timeout,
            )
        except Exception as e:
            logger.error(
                "Reconnection failed for queue %s: %s: %s",
                self.queue,
                e.__class__.__name__,
                e,
            )
            return False
        else:
            return True
        finally:
            self._reconnect_lock.release()
