"""Log shipping service for sending buffered logs to Waldur Mastermind.

This module provides a background service that periodically collects log entries
from the buffer and ships them to Waldur Mastermind via the API. The service
runs in a separate thread and handles batching, retries, and graceful shutdown.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional, Union

import httpx

from .log_buffer import CircularLogBuffer, LogEntry

logger = logging.getLogger(__name__)

# Waldur Mastermind API endpoint for agent logs.
# Expected to be available at: {api_url}marketplace-site-agent-logs/
_LOG_ENDPOINT = "marketplace-site-agent-logs/"
_HTTP_NOT_FOUND = 404


class LogShipper:
    """Background service for shipping log entries to Waldur Mastermind.

    The service runs in a separate daemon thread and periodically collects
    log entries from the buffer, batches them, and sends them to the API.
    If the endpoint is not yet available (HTTP 404), shipping is silently
    skipped until the endpoint becomes available.
    """

    def __init__(
        self,
        buffer: CircularLogBuffer,
        api_url: str,
        api_token: str,
        agent_identity_uuid: str,
        ship_interval: int = 60,
        batch_size: int = 100,
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> None:
        """Initialize the log shipper.

        Args:
            buffer: The CircularLogBuffer to read from
            api_url: Waldur API base URL (e.g. https://waldur.example.com/api/)
            api_token: Waldur API authentication token
            agent_identity_uuid: UUID of the AgentIdentity registered in Waldur
            ship_interval: Interval between shipments in seconds
            batch_size: Maximum number of log entries per batch
            max_retries: Maximum number of retry attempts for failed shipments
            retry_delay: Base delay between retry attempts in seconds (uses exponential backoff)
        """
        self.buffer = buffer
        self.api_url = api_url.rstrip("/") + "/"
        self.api_token = api_token
        self.agent_identity_uuid = agent_identity_uuid
        self.ship_interval = ship_interval
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._client: Optional[httpx.Client] = None
        self._stats: dict[str, Union[int, float, None]] = {
            "logs_shipped": 0,
            "batches_sent": 0,
            "failed_shipments": 0,
            "last_shipment": None,
        }

    def start(self) -> None:
        """Start the log shipping service in a background thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("Log shipper already running for agent %s", self.agent_identity_uuid)
            return

        self._client = httpx.Client(
            timeout=30,
            headers={
                "Authorization": f"Token {self.api_token}",
                "Content-Type": "application/json",
            },
        )
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._ship_loop,
            daemon=True,
            name=f"log-shipper-{self.agent_identity_uuid[:8]}",
        )
        self._thread.start()
        logger.info("Log shipper started for agent %s", self.agent_identity_uuid)

    def stop(self, timeout: int = 10) -> None:
        """Stop the log shipping service and ship any remaining logs.

        Args:
            timeout: Maximum time to wait for the thread to stop
        """
        if not self._thread:
            return

        logger.info("Stopping log shipper for agent %s", self.agent_identity_uuid)
        self._stop_event.set()

        if self._thread.is_alive():
            self._thread.join(timeout=timeout)

        self._ship_logs()

        if self._client:
            self._client.close()
            self._client = None

        logger.info("Log shipper stopped for agent %s", self.agent_identity_uuid)

    def _ship_loop(self) -> None:
        """Main loop that runs in the background thread."""
        while not self._stop_event.wait(self.ship_interval):
            try:
                self._ship_logs()
            except Exception:
                logger.exception(
                    "Error in log shipping loop for agent %s",
                    self.agent_identity_uuid,
                )

    def _ship_logs(self) -> None:
        """Collect and ship log entries to Waldur Mastermind."""
        entries = self.buffer.get_and_clear()
        if not entries:
            return

        logger.debug("Shipping %d log entries for agent %s", len(entries), self.agent_identity_uuid)

        for i in range(0, len(entries), self.batch_size):
            batch = entries[i : i + self.batch_size]
            self._ship_batch(batch)

    def _ship_batch(self, entries: list[LogEntry]) -> None:
        """Ship a batch of log entries with retry logic.

        If the endpoint returns HTTP 404 the batch is silently discarded
        (endpoint not yet deployed). For other errors, exponential backoff
        retry is applied up to max_retries times.

        Args:
            entries: List of log entries to ship
        """
        url = f"{self.api_url}{_LOG_ENDPOINT}"
        payload = [
            {
                "agent_identity_uuid": self.agent_identity_uuid,
                "timestamp": entry.timestamp,
                "level": entry.level,
                "message": entry.message,
                "module": entry.module,
            }
            for entry in entries
        ]

        for attempt in range(self.max_retries + 1):
            try:
                if self._client:
                    response = self._client.post(url, json=payload)
                else:
                    headers = {
                        "Authorization": f"Token {self.api_token}",
                        "Content-Type": "application/json",
                    }
                    with httpx.Client(timeout=30) as client:
                        response = client.post(url, json=payload, headers=headers)
                response.raise_for_status()

                self._stats["logs_shipped"] = (self._stats["logs_shipped"] or 0) + len(entries)
                self._stats["batches_sent"] = (self._stats["batches_sent"] or 0) + 1
                self._stats["last_shipment"] = time.time()
                logger.debug("Shipped batch of %d log entries to %s", len(entries), url)
                return

            except httpx.HTTPStatusError as e:
                if e.response.status_code == _HTTP_NOT_FOUND:
                    # Endpoint not yet implemented — skip silently
                    logger.debug(
                        "Log shipping endpoint not available yet (%s), skipping batch", url
                    )
                    return
                logger.warning(
                    "HTTP %d shipping logs to %s (attempt %d/%d)",
                    e.response.status_code,
                    url,
                    attempt + 1,
                    self.max_retries + 1,
                )

            except Exception as e:
                logger.warning(
                    "Failed to ship log batch to %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    self.max_retries + 1,
                    e,
                )

            if attempt < self.max_retries:
                time.sleep(self.retry_delay * (2**attempt))

        self._stats["failed_shipments"] = (self._stats["failed_shipments"] or 0) + 1
        logger.error(
            "Giving up shipping log batch of %d entries after %d attempts",
            len(entries),
            self.max_retries + 1,
        )


class LogShippingManager:
    """Manager for multiple log shippers across different agent identities."""

    def __init__(self) -> None:
        """Initialize the log shipping manager."""
        self.shippers: dict[str, LogShipper] = {}

    def add_shipper(self, agent_identity_uuid: str, shipper: LogShipper) -> None:
        """Add a log shipper for an agent identity, stopping any previous one."""
        if agent_identity_uuid in self.shippers:
            logger.warning("Replacing existing shipper for agent %s", agent_identity_uuid)
            self.shippers[agent_identity_uuid].stop()
        self.shippers[agent_identity_uuid] = shipper

    def start_all(self) -> None:
        """Start all registered log shippers."""
        for agent_identity_uuid, shipper in self.shippers.items():
            try:
                shipper.start()
            except Exception:
                logger.exception("Failed to start shipper for agent %s", agent_identity_uuid)

    def stop_all(self, timeout: int = 10) -> None:
        """Stop all log shippers."""
        for agent_identity_uuid, shipper in self.shippers.items():
            try:
                shipper.stop(timeout)
            except Exception:
                logger.exception("Failed to stop shipper for agent %s", agent_identity_uuid)
        self.shippers.clear()
