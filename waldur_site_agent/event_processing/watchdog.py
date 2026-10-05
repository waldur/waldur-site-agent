"""Keeps event-mode STOMP consumers alive and tells the main loop when they are not.

A ``WaldurListener`` reconnects on its own after a disconnect, but gives up after
``RECONNECT_MAX_RETRIES`` attempts (roughly ten minutes of backoff). An offering
whose queue registration fails at startup is never set up at all. Without this
watchdog both stay dead for the life of the process while the main loop keeps
touching the liveness heartbeat, so the agent looks healthy and receives nothing.

Handlers run on a worker thread per queue and the queue has a prefetch of one, so a
handler that hangs keeps the connection healthy while every later message waits. A
handler running longer than ``handler_stuck_after`` counts as the queue being down.

On every tick the watchdog retries what is down, with every connect bounded so a
tick never blocks for long, and reports unhealthy once something a restart could
fix has stayed down longer than ``unhealthy_after`` seconds. The main loop then
stops touching the heartbeat and the liveness probe restarts the agent.

Not counted towards liveness, only logged at ERROR, because a restart cannot fix
them and would crash-loop every other offering of the agent:

- consumers on a federation target (another Waldur's broker);
- setups refused with a 4xx (other than 408/429), e.g. a queue held by another
  user (409) or missing permissions (403). These are retried at the maximum
  backoff in case the server side is fixed.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from waldur_api_client.errors import UnexpectedStatus

from waldur_site_agent.backend import logger
from waldur_site_agent.common import WALDUR_SITE_AGENT_STOMP_HANDLER_STUCK_AFTER_MINUTES
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.event_processing import utils
from waldur_site_agent.event_processing.event_subscription_manager import (
    WALDUR_LISTENER_NAME,
)
from waldur_site_agent.event_processing.structures import (
    StompConsumer,
    StompConsumerKey,
    StompConsumersMap,
)

# One attempt per tick: the tick itself is the retry interval.
WATCHDOG_CONNECT_ATTEMPTS = 1
# A reconnected consumer counts as recovered only after this many consecutive
# ticks connected, so a queue the broker closes right after CONNECTED (e.g.
# SUBSCRIBE NOT_FOUND) is not reported as healthy on every tick.
STABLE_TICKS = 2
# Setup (identity + queue registration) is retried with backoff so a persistent
# refusal does not log an exception every minute.
SETUP_RETRY_INITIAL = 60.0
SETUP_RETRY_MAX = 15 * 60.0

_RETRIABLE_CLIENT_ERRORS = (408, 429)


def _is_persistent_refusal(error: Exception) -> bool:
    """A 4xx a restart cannot fix (the server refuses the request as made)."""
    return (
        isinstance(error, UnexpectedStatus)
        and HTTPStatus.BAD_REQUEST <= error.status_code < HTTPStatus.INTERNAL_SERVER_ERROR
        and error.status_code not in _RETRIABLE_CLIENT_ERRORS
    )


class StompWatchdog:
    """Reconnects dropped consumers and retries each missing consumer of an offering."""

    def __init__(
        self,
        consumers_map: StompConsumersMap,
        offerings: list[common_structures.Offering],
        user_agent: str,
        unhealthy_after: float,
        global_proxy: str = "",
        expose_backend_error_details: bool = True,
        handler_stuck_after: float = WALDUR_SITE_AGENT_STOMP_HANDLER_STUCK_AFTER_MINUTES * 60,
    ) -> None:
        """Watch ``consumers_map`` in place; recovered consumers are added to it."""
        self.handler_stuck_after = handler_stuck_after
        self.consumers_map = consumers_map
        self.offerings = offerings
        self.user_agent = user_agent
        self.unhealthy_after = unhealthy_after
        self.global_proxy = global_proxy
        self.expose_backend_error_details = expose_backend_error_details
        # When each liveness-relevant item was first seen down.
        self._down_since: dict[object, float] = {}
        # Same for items only reported, never withholding liveness.
        self._reported_down_since: dict[object, float] = {}
        self._reported: set[object] = set()
        self._connected_ticks: dict[object, int] = {}
        self._next_setup_retry: dict[object, float] = {}
        self._setup_retry_delay: dict[object, float] = {}
        self._object_types: dict[StompConsumerKey, list] = {}
        self._expects_targets: dict[StompConsumerKey, bool] = {}

    def check(self, now: float) -> bool:
        """Retry whatever is down; return False once something stayed down too long."""
        source_uuids = set()
        for offering in self.offerings:
            if not offering.stomp_enabled:
                continue
            source_uuids.add(offering.uuid)
            self._check_offering_setup(offering, now)

        for consumers in self.consumers_map.values():
            for consumer in consumers:
                self._check_consumer(consumer, now, critical=consumer[2].uuid in source_uuids)

        self._report_persistent_outages(now)
        return self._healthy(now)

    # -- setup of missing consumers ------------------------------------------

    def _check_offering_setup(self, offering: common_structures.Offering, now: float) -> None:
        map_key = (offering.name, offering.uuid)
        consumers = self.consumers_map.get(map_key, [])

        if self._offering_object_types(offering, map_key) and not any(
            consumer[2].uuid == offering.uuid for consumer in consumers
        ):
            self._retry_setup(offering, map_key, "main", now)

        if self._offering_expects_targets(offering, map_key) and not any(
            consumer[2].uuid != offering.uuid for consumer in consumers
        ):
            self._retry_setup(offering, map_key, "target", now)

    def _offering_object_types(
        self, offering: common_structures.Offering, map_key: StompConsumerKey
    ) -> list:
        if map_key not in self._object_types:
            self._object_types[map_key] = utils._determine_observable_object_types(offering)
        return self._object_types[map_key]

    def _offering_expects_targets(
        self, offering: common_structures.Offering, map_key: StompConsumerKey
    ) -> bool:
        if map_key not in self._expects_targets:
            try:
                self._expects_targets[map_key] = utils.offering_expects_target_consumers(offering)
            except Exception:
                logger.exception("Cannot tell whether offering %s has targets", offering.name)
                return False
        return self._expects_targets[map_key]

    def _retry_setup(
        self,
        offering: common_structures.Offering,
        map_key: StompConsumerKey,
        role: str,
        now: float,
    ) -> None:
        key = ("setup", map_key, role)
        if role == "main" and key not in self._reported:
            self._down_since.setdefault(key, now)
        else:
            self._reported_down_since.setdefault(key, now)
        if now < self._next_setup_retry.get(key, now):
            return

        logger.warning(
            "STOMP %s consumer is not set up for offering %s (%s), retrying",
            role,
            offering.name,
            offering.uuid,
        )
        try:
            if role == "main":
                consumer = utils.open_offering_consumer(
                    offering,
                    self.user_agent,
                    self.global_proxy,
                    expose_backend_error_details=self.expose_backend_error_details,
                    object_types=self._object_types[map_key],
                    connect_max_retries=WATCHDOG_CONNECT_ATTEMPTS,
                )
                new_consumers = [consumer] if consumer is not None else []
            else:
                new_consumers = utils.setup_offering_target_consumers(
                    offering, self.user_agent, self.global_proxy
                )
        except Exception as error:
            if _is_persistent_refusal(error):
                if key not in self._reported:
                    logger.error(
                        "STOMP setup for offering %s was refused (%s); not restarting the "
                        "agent over it, retrying every %.0f min",
                        offering.name,
                        error,
                        SETUP_RETRY_MAX / 60,
                    )
                self._reported.add(key)
                self._reported_down_since.setdefault(key, self._down_since.pop(key, now))
                self._schedule_setup_retry(key, now, SETUP_RETRY_MAX)
            else:
                logger.exception("STOMP setup retry failed for offering %s", offering.name)
                self._schedule_setup_retry(key, now)
            return

        if not new_consumers:
            self._schedule_setup_retry(key, now)
            return

        self.consumers_map.setdefault(map_key, []).extend(new_consumers)
        for item in (self._next_setup_retry, self._setup_retry_delay):
            item.pop(key, None)
        self._reported.discard(key)
        self._reported_down_since.pop(key, None)
        if self._down_since.pop(key, None) is not None:
            logger.info("STOMP %s consumer set up for offering %s", role, offering.name)

    def _schedule_setup_retry(
        self, key: object, now: float, delay: Optional[float] = None
    ) -> None:
        if delay is None:
            delay = min(
                self._setup_retry_delay.get(key, SETUP_RETRY_INITIAL / 2) * 2,
                SETUP_RETRY_MAX,
            )
        self._setup_retry_delay[key] = delay
        self._next_setup_retry[key] = now + delay

    # -- connections -----------------------------------------------------------

    def _check_consumer(self, consumer: StompConsumer, now: float, critical: bool) -> None:
        connection, unified_queue, offering = consumer
        key = ("consumer", id(connection))
        down_since = self._down_since if critical else self._reported_down_since

        listener = connection.get_listener(WALDUR_LISTENER_NAME)
        if listener is not None:
            # Restart the worker if anything ended it, so the queue keeps draining.
            listener.ensure_worker()
            self._check_handler(listener, unified_queue.queue_name, key, down_since, now)

        if connection.is_connected():
            ticks = self._connected_ticks.get(key, 0) + 1
            self._connected_ticks[key] = ticks
            if ticks >= STABLE_TICKS and down_since.pop(key, None) is not None:
                self._reported.discard(key)
                logger.info("STOMP recovered for queue %s", unified_queue.queue_name)
            return

        self._connected_ticks[key] = 0
        down_since.setdefault(key, now)
        if listener is None:
            return
        if listener.reconnect_in_progress():
            logger.debug(
                "Queue %s is already reconnecting, leaving it to the listener",
                unified_queue.queue_name,
            )
            return
        logger.warning(
            "STOMP consumer for offering %s (queue %s) is disconnected, reconnecting",
            offering.name,
            unified_queue.queue_name,
        )
        if listener.reconnect(max_retries=WATCHDOG_CONNECT_ATTEMPTS):
            self._connected_ticks[key] = 1

    def _check_handler(
        self,
        listener: object,
        queue_name: str,
        key: object,
        down_since: dict[object, float],
        now: float,
    ) -> None:
        """Count a handler running past ``handler_stuck_after`` as its queue being down."""
        handler_key = ("handler", key)
        started = listener.handler_running_since()  # type: ignore[attr-defined]
        if started is None or now - started < self.handler_stuck_after:
            if down_since.pop(handler_key, None) is not None:
                logger.info("Handler of queue %s is no longer stuck", queue_name)
            return
        if handler_key not in down_since:
            logger.error(
                "A message handler on queue %s has been running for %.0f s; "
                "later messages on the queue wait behind it",
                queue_name,
                now - started,
            )
        down_since.setdefault(handler_key, started + self.handler_stuck_after)

    # -- health ----------------------------------------------------------------

    def _report_persistent_outages(self, now: float) -> None:
        for key, since in self._reported_down_since.items():
            if key in self._reported or now - since < self.unhealthy_after:
                continue
            self._reported.add(key)
            logger.error(
                "STOMP %s has been down for %.0f s; not withholding liveness for it "
                "(a restart would not fix it)",
                key,
                now - since,
            )

    def _healthy(self, now: float) -> bool:
        oldest: Optional[float] = min(self._down_since.values(), default=None)
        if oldest is None or now - oldest < self.unhealthy_after:
            return True
        logger.error(
            "STOMP has been down for %.0f s (threshold %.0f s); "
            "withholding the liveness heartbeat",
            now - oldest,
            self.unhealthy_after,
        )
        return False
