"""Integration test for the full log shipping pipeline.

Exercises the end-to-end flow without Docker:
  config YAML → load_configuration → setup_log_shippers (buffer only) →
  ensure_log_shipper(agent identity) → agent logs → CircularLogBuffer →
  LogShipper → HTTP POST (mocked with respx)

This verifies that all components wire together correctly:
  - LogShippingConfig parsed from YAML
  - setup_log_buffering installs BufferedLogHandler on the root logger
  - Agent log records flow into the buffer
  - LogShipper drains the buffer and POSTs to the correct endpoint
  - Payload is a JSON list with agent_identity_uuid and entry fields
  - teardown_log_shippers stops the shipper and flushes remaining entries
"""

import json
import logging
import tempfile
import unittest
from pathlib import Path

import httpx
import respx
import yaml

from waldur_site_agent.backend import (
    get_log_buffer_manager,
    get_log_shipping_manager,
    teardown_log_buffering,
)
from waldur_site_agent.common.log_handler import BufferedLogHandler
from waldur_site_agent.common.utils import (
    ensure_log_shipper,
    load_configuration,
    setup_log_shippers,
    teardown_log_shippers,
)

_API_URL = "http://localhost:9999/api/"
_TOKEN = "integration-test-token"
_OFFERING_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_AGENT_IDENTITY_UUID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
_ENDPOINT = f"{_API_URL}marketplace-site-agent-logs/"


def _write_config(log_shipping_enabled: bool = True) -> str:
    """Write a temporary YAML config and return the path."""
    config = {
        "log_shipping": {
            "enabled": log_shipping_enabled,
            "ship_interval_seconds": 3600,
            "buffer_size_mb": 1,
        },
        "offerings": [
            {
                "name": "Integration Test Offering",
                "waldur_api_url": _API_URL,
                "waldur_api_token": _TOKEN,
                "waldur_offering_uuid": _OFFERING_UUID,
                "backend_type": "slurm",
            }
        ],
    }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.dump(config, f)
    f.close()
    return f.name


class TestLogShippingIntegration(unittest.TestCase):
    """Full pipeline integration test using respx to mock the HTTP endpoint."""

    def setUp(self):
        # Clean up any leftover buffering state from other tests
        teardown_log_buffering()
        teardown_log_shippers()

    def tearDown(self):
        # Remove any BufferedLogHandlers from root logger
        root = logging.getLogger()
        for h in list(root.handlers):
            if isinstance(h, BufferedLogHandler):
                root.removeHandler(h)
        teardown_log_buffering()
        teardown_log_shippers()

    @respx.mock
    def test_full_pipeline_config_to_http(self):
        """Config → setup → log → ship → verify HTTP payload."""
        route = respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        # 1. Load config from YAML
        config_path = _write_config(log_shipping_enabled=True)
        try:
            configuration = load_configuration(config_path)
        finally:
            Path(config_path).unlink()

        # Verify log_shipping was parsed
        assert configuration.log_shipping.enabled is True
        offering = configuration.waldur_offerings[0]

        # 2. Buffer + handler; shipper starts after agent identity is known
        setup_log_shippers(configuration)
        ensure_log_shipper(offering, _AGENT_IDENTITY_UUID, configuration.log_shipping)
        shipper = get_log_shipping_manager().shippers[_AGENT_IDENTITY_UUID]

        # 3. Verify the buffer handler is installed on root logger
        root = logging.getLogger()
        buffer_handlers = [h for h in root.handlers if isinstance(h, BufferedLogHandler)]
        assert len(buffer_handlers) >= 1

        # 4. Emit some log records — they should flow into the buffer
        test_logger = logging.getLogger("integration.test")
        test_logger.warning("integration-test-message-1")
        test_logger.error("integration-test-message-2")

        # 5. Verify entries are in the buffer
        buffer = get_log_buffer_manager().get_buffer()
        assert buffer is not None
        stats = buffer.get_stats()
        assert stats["entry_count"] >= 2

        # 6. Trigger manual ship (don't wait for the timer)
        shipper._ship_logs()

        # 7. Verify HTTP call was made with correct payload
        assert route.call_count == 1
        body = json.loads(route.calls.last.request.content)
        assert isinstance(body, list)
        assert len(body) >= 2

        messages = [e["message"] for e in body]
        assert any("integration-test-message-1" in m for m in messages)
        assert any("integration-test-message-2" in m for m in messages)

        for entry in body:
            assert entry["agent_identity_uuid"] == _AGENT_IDENTITY_UUID
            assert "timestamp" in entry
            assert "level" in entry
            assert "message" in entry
            assert "module" in entry

        # 8. Verify shipper stats
        stats_shipper = shipper._stats
        assert stats_shipper["logs_shipped"] >= 2
        assert stats_shipper["batches_sent"] == 1

        # 9. Teardown
        teardown_log_shippers()

    @respx.mock
    def test_disabled_config_creates_no_shippers(self):
        """When log_shipping.enabled is False, no shippers are created."""
        config_path = _write_config(log_shipping_enabled=False)
        try:
            configuration = load_configuration(config_path)
        finally:
            Path(config_path).unlink()

        setup_log_shippers(configuration)
        offering = configuration.waldur_offerings[0]
        ensure_log_shipper(offering, _AGENT_IDENTITY_UUID, configuration.log_shipping)
        assert len(get_log_shipping_manager().shippers) == 0

    @respx.mock
    def test_404_endpoint_does_not_fail(self):
        """When the API returns 404, shipping silently skips without errors."""
        respx.post(_ENDPOINT).mock(return_value=httpx.Response(404))

        config_path = _write_config(log_shipping_enabled=True)
        try:
            configuration = load_configuration(config_path)
        finally:
            Path(config_path).unlink()

        offering = configuration.waldur_offerings[0]
        setup_log_shippers(configuration)
        ensure_log_shipper(offering, _AGENT_IDENTITY_UUID, configuration.log_shipping)
        shipper = get_log_shipping_manager().shippers[_AGENT_IDENTITY_UUID]

        # Emit a log and ship
        logging.getLogger("integration.404").warning("will-be-skipped")
        shipper._ship_logs()

        # Should not record as failure (404 is gracefully handled)
        stats = shipper._stats
        assert stats["failed_shipments"] == 0

        teardown_log_shippers()

    @respx.mock
    def test_teardown_flushes_remaining_entries(self):
        """teardown_log_shippers must flush buffered entries before stopping."""
        route = respx.post(_ENDPOINT).mock(return_value=httpx.Response(200))

        config_path = _write_config(log_shipping_enabled=True)
        try:
            configuration = load_configuration(config_path)
        finally:
            Path(config_path).unlink()

        offering = configuration.waldur_offerings[0]
        setup_log_shippers(configuration)
        ensure_log_shipper(offering, _AGENT_IDENTITY_UUID, configuration.log_shipping)

        # Emit logs but don't manually ship
        logging.getLogger("integration.flush").error("flush-me")

        # Teardown should flush
        teardown_log_shippers()

        # The stop() call inside teardown should have shipped
        assert route.call_count >= 1
        body = json.loads(route.calls.last.request.content)
        assert isinstance(body, list)
        messages = [e["message"] for e in body]
        assert any("flush-me" in m for m in messages)
