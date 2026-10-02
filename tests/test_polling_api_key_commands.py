"""Polling order processing carries out the API key commands Waldur has pending.

Key commands are not orders. Without STOMP, the order-processing loop is what
delivers them, every cycle and however recently Waldur issued them.
"""

import datetime
import unittest
from unittest import mock

from waldur_api_client.types import UNSET

from tests.fixtures import api_key_row, key_listing
from waldur_site_agent.common import resource_api_keys
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.polling_processing import agent_order_process

POLLING = "waldur_site_agent.polling_processing.agent_order_process"
KEYS = "waldur_site_agent.common.resource_api_keys"


def _make_offering(**overrides) -> common_structures.Offering:
    defaults = dict(
        name="gateway",
        waldur_offering_uuid="offering-uuid",
        waldur_api_url="https://example.com/api/",
        waldur_api_token="token",
        backend_type="envoy",
        order_processing_backend="envoy",
    )
    defaults.update(overrides)
    return common_structures.Offering(**defaults)


def _configuration(*offerings, expose_backend_error_details=True):
    configuration = mock.Mock()
    configuration.waldur_offerings = list(offerings)
    configuration.waldur_user_agent = "agent"
    configuration.expose_backend_error_details = expose_backend_error_details
    return configuration


def _key_backend(lifecycle=True):
    backend = mock.Mock()
    backend.supports_resource_api_keys = True
    backend.supports_resource_api_key_lifecycle = lifecycle
    return backend


def _pending_row(state="Updating", pending_action="pause", client_id="res-1-1"):
    """A key as the listing endpoint serves it, issued a moment ago."""
    return api_key_row(
        client_id=client_id,
        state=state,
        pending_action=pending_action,
        modified=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
    )


def _states(listing):
    return sorted(state.value for state in listing.sync_all.call_args.kwargs["state"])


@mock.patch(f"{POLLING}.agent_identity_management.ensure_agent_telemetry")
@mock.patch(f"{POLLING}.processors.OfferingOrderProcessor")
@mock.patch(f"{POLLING}.utils.get_backend_for_offering")
@mock.patch(f"{POLLING}.utils.get_client")
class TestPollingCarriesOutKeyCommands(unittest.TestCase):
    def _run(self, mock_get_backend, backend, rows, *offerings, **config):
        mock_get_backend.return_value = (backend, "1.0")
        listing = key_listing(rows)
        with mock.patch(f"{KEYS}.marketplace_resource_api_keys_list", listing), mock.patch(
            f"{KEYS}._acknowledge"
        ) as mock_ack, mock.patch(f"{KEYS}.utils.rotate_resource_api_key") as mock_rotate:
            agent_order_process._process_offerings(
                _configuration(*(offerings or (_make_offering(),)), **config)
            )
        return listing, mock_ack, mock_rotate

    def test_a_command_issued_a_moment_ago_is_carried_out(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """No pending threshold: there is no STOMP handler for the loop to race.

        A key paused at its limit stops serving on the next cycle, not an hour later.
        """
        backend = _key_backend()

        listing, mock_ack, _ = self._run(mock_get_backend, backend, [_pending_row()])

        filters = listing.sync_all.call_args.kwargs
        self.assertEqual(filters["offering_uuid"], "offering-uuid")
        # Every pending key, however recent.
        self.assertIs(filters["modified_before"], UNSET)
        backend.pause_resource_key.assert_called_once_with("res-1-1", "res-1")
        self.assertIs(
            mock_ack.call_args.args[0], resource_api_keys.marketplace_resource_api_keys_set_paused
        )

    def test_every_pending_state_is_taken(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """A request (Creating) and a delete (Deleting) are pending commands too."""
        listing, *_ = self._run(mock_get_backend, _key_backend(), [])
        self.assertEqual(_states(listing), ["Creating", "Deleting", "Updating"])

    def test_keys_are_taken_after_the_orders(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """A create order generates the resource's keys; its keys' commands come after."""
        calls = []
        mock_processor.return_value.process_offering.side_effect = lambda: calls.append("orders")
        listing = mock.Mock()
        listing.sync_all.side_effect = lambda **f: calls.append("keys") or []
        mock_get_backend.return_value = (_key_backend(), "1.0")
        with mock.patch(f"{KEYS}.marketplace_resource_api_keys_list", listing):
            agent_order_process._process_offerings(_configuration(_make_offering()))
        self.assertEqual(calls[0], "orders")
        self.assertIn("keys", calls)

    def test_a_failing_order_pass_does_not_hold_back_key_commands(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """Nothing else delivers them without STOMP: a key paused at its limit would serve on."""
        mock_processor.return_value.process_offering.side_effect = RuntimeError("orders down")
        backend = _key_backend()

        self._run(mock_get_backend, backend, [_pending_row()])

        backend.pause_resource_key.assert_called_once_with("res-1-1", "res-1")

    def test_a_rotation_is_carried_out(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """Rotation needs only supports_resource_api_keys, as on LiteLLM."""
        backend = _key_backend(lifecycle=False)
        row = _pending_row(pending_action="rotate")

        listing, _, mock_rotate = self._run(mock_get_backend, backend, [row])

        self.assertEqual(_states(listing), ["Updating"])
        mock_rotate.assert_called_once_with(
            mock_get_client.return_value,
            str(row.uuid),
            "res-1-1",
            backend,
            "res-1",
            str(row.resource_uuid),
            expose_backend_error_details=True,
        )

    def test_the_error_exposure_flag_is_forwarded(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        backend = _key_backend(lifecycle=False)
        _, _, mock_rotate = self._run(
            mock_get_backend,
            backend,
            [_pending_row(pending_action="rotate")],
            expose_backend_error_details=False,
        )
        self.assertIs(mock_rotate.call_args.kwargs["expose_backend_error_details"], False)

    def test_a_stomp_offering_is_left_to_stomp(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        """STOMP delivers its commands; polling them as well would run each twice."""
        listing, *_ = self._run(
            mock_get_backend, _key_backend(), [], _make_offering(stomp_enabled=True)
        )
        listing.sync_all.assert_not_called()

    def test_a_backend_without_keys_is_not_asked(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        listing, *_ = self._run(mock_get_backend, mock.Mock(spec=[]), [])
        listing.sync_all.assert_not_called()

    def test_a_failing_listing_does_not_stop_the_next_offering(
        self, mock_get_client, mock_get_backend, mock_processor, mock_telemetry
    ):
        backend = _key_backend()
        mock_get_backend.return_value = (backend, "1.0")
        rows = [_pending_row()]

        def _list(client, **filters):
            if client == "broken":
                raise Exception("api down")
            return rows

        listing = mock.Mock()
        listing.sync_all.side_effect = _list
        mock_get_client.side_effect = ["broken", "healthy"]
        with mock.patch(f"{KEYS}.marketplace_resource_api_keys_list", listing), mock.patch(
            f"{KEYS}._acknowledge"
        ):
            agent_order_process._process_offerings(
                _configuration(_make_offering(name="first"), _make_offering(name="second"))
            )

        backend.pause_resource_key.assert_called_once_with("res-1-1", "res-1")


class TestProcessPendingApiKeyCommands(unittest.TestCase):
    def test_a_cutoff_limits_the_listing_to_older_keys(self):
        """The event_process sweep passes one, so it leaves in-flight commands alone."""
        cutoff = datetime.datetime(2026, 9, 27, 12, 0, tzinfo=datetime.timezone.utc)
        listing = key_listing([])
        with mock.patch(f"{KEYS}.marketplace_resource_api_keys_list", listing):
            resource_api_keys.process_pending_api_key_commands(
                mock.Mock(), _key_backend(), _make_offering(), cutoff
            )
        self.assertEqual(listing.sync_all.call_args.kwargs["modified_before"], cutoff)
