"""Tests for the per-key API key commands and per-key usage reporting."""

from __future__ import annotations

import datetime
import json
import unittest
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import httpx
import stomp.utils
from waldur_api_client import AuthenticatedClient, errors

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common import resource_api_keys
from waldur_site_agent.common import structures as common_structures
from waldur_site_agent.common.resource_api_keys import ApiKeyCommand
from waldur_site_agent.event_processing.handlers import on_resource_api_key_command_stomp
from tests.fixtures import api_key_row

KEYS = "waldur_site_agent.common.resource_api_keys"
HANDLERS = "waldur_site_agent.event_processing.handlers"
ENDPOINT_PREFIX = "marketplace_resource_api_keys_"


def _backend(lifecycle: bool = True) -> mock.Mock:
    backend = mock.Mock()
    backend.supports_resource_api_keys = True
    backend.supports_resource_api_key_lifecycle = lifecycle
    return backend


def _command(action: str, **overrides: object) -> ApiKeyCommand:
    fields: dict[str, object] = {
        "action": action,
        "api_key_uuid": "key-uuid",
        "resource_uuid": "res-uuid",
        "resource_backend_id": "res-1",
        "client_id": "res-1-1",
    }
    fields.update(overrides)
    return ApiKeyCommand(**fields)  # type: ignore[arg-type]


def _acknowledged(mock_ack: mock.Mock, client: object = mock.sentinel.client) -> list[str]:
    """The endpoint of each acknowledgement, checking it went to the key in hand."""
    endpoints = []
    for call in mock_ack.call_args_list:
        endpoint, ack_client, command = call.args
        assert ack_client is client
        assert command.api_key_uuid == "key-uuid"
        endpoints.append(endpoint.__name__.rsplit(".", 1)[-1].replace(ENDPOINT_PREFIX, ""))
    return endpoints


@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}._acknowledge")
class TestExecuteApiKeyCommand(unittest.TestCase):
    """Each command runs on the backend first, then its own acknowledgement."""

    def _execute(self, command: ApiKeyCommand, backend: Optional[mock.Mock] = None) -> mock.Mock:
        backend = backend or _backend()
        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, command)
        return backend

    def test_pause_blocks_the_key_then_reports_it_paused(self, mock_post, mock_erred):
        backend = self._execute(_command("pause"))
        backend.pause_resource_key.assert_called_once_with("res-1-1", "res-1")
        self.assertEqual(_acknowledged(mock_post), ["set_paused"])
        mock_erred.assert_not_called()

    def test_resume_applies_the_settings_it_carries(self, mock_post, mock_erred):
        backend = self._execute(
            _command("resume", limits={"input_tokens": 10}, allowed_models=None)
        )
        backend.resume_resource_key.assert_called_once_with(
            "res-1-1", "res-1", limits={"input_tokens": 10}, allowed_models=None
        )
        self.assertEqual(_acknowledged(mock_post), ["set_ok"])

    def test_update_applies_settings_then_reports_ok(self, mock_post, mock_erred):
        backend = self._execute(_command("update", limits={"output_tokens": 5}))
        backend.update_resource_key.assert_called_once_with(
            "res-1-1", "res-1", limits={"output_tokens": 5}, allowed_models=None
        )
        self.assertEqual(_acknowledged(mock_post), ["set_ok"])

    def test_delete_revokes_then_reports_deleted(self, mock_post, mock_erred):
        backend = self._execute(_command("delete"))
        backend.delete_resource_key.assert_called_once_with("res-1-1", "res-1")
        self.assertEqual(_acknowledged(mock_post), ["set_deleted"])

    def test_deleting_a_request_that_never_reached_the_backend(self, mock_post, mock_erred):
        backend = self._execute(_command("delete", client_id=""))
        backend.delete_resource_key.assert_not_called()
        self.assertEqual(_acknowledged(mock_post), ["set_deleted"])

    def test_an_unknown_action_is_refused_without_acting(self, mock_post, mock_erred):
        backend = self._execute(_command("revoke"))
        self.assertEqual(
            [c[0] for c in backend.method_calls],
            [],
            "an unknown command must not reach the backend",
        )
        mock_post.assert_not_called()
        mock_erred.assert_not_called()

    def test_a_backend_without_the_lifecycle_reports_the_command_failed(
        self, mock_post, mock_erred
    ):
        """It keeps generate-and-rotate; anything else errs rather than spinning."""
        backend = self._execute(_command("pause"), _backend(lifecycle=False))
        backend.pause_resource_key.assert_not_called()
        mock_post.assert_not_called()
        mock_erred.assert_called_once()
        self.assertIn("pause", mock_erred.call_args.args[2])

    def test_rotation_needs_no_lifecycle(self, mock_post, mock_erred):
        backend = _backend(lifecycle=False)
        with mock.patch(f"{KEYS}.utils.rotate_resource_api_key") as mock_rotate:
            self._execute(_command("rotate"), backend)
        mock_rotate.assert_called_once_with(
            mock.sentinel.client,
            "key-uuid",
            "res-1-1",
            backend,
            "res-1",
            "res-uuid",
            expose_backend_error_details=True,
        )
        mock_erred.assert_not_called()

    def test_a_backend_failure_is_reported_as_erred(self, mock_post, mock_erred):
        backend = _backend()
        backend.pause_resource_key.side_effect = BackendError("gateway down")
        self._execute(_command("pause"), backend)
        mock_post.assert_not_called()
        mock_erred.assert_called_once_with(mock.sentinel.client, "key-uuid", "gateway down")

    def test_a_failed_acknowledgement_is_reported_as_erred(self, mock_post, mock_erred):
        """Otherwise the key stays in flight and the sweep replays it every tick."""
        mock_post.side_effect = httpx.ConnectError("waldur down")
        self._execute(_command("pause"))
        mock_erred.assert_called_once()

    def test_raw_errors_are_withheld_when_the_offering_opts_out(self, mock_post, mock_erred):
        backend = _backend()
        backend.pause_resource_key.side_effect = RuntimeError("internal path /etc/x")
        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("pause"), expose_backend_error_details=False
        )
        self.assertNotIn("/etc/x", mock_erred.call_args.args[2])

    def test_a_command_needing_a_client_id_without_one_errs(self, mock_post, mock_erred):
        backend = self._execute(_command("pause", client_id=""))
        backend.pause_resource_key.assert_not_called()
        mock_erred.assert_called_once()


@mock.patch(f"{KEYS}.marketplace_provider_resources_retrieve")
@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}.marketplace_resource_api_keys_set_key")
@mock.patch(f"{KEYS}.list_all_api_keys")
class TestCreate(unittest.TestCase):
    def _backend(self) -> mock.Mock:
        backend = _backend()
        backend.mint_resource_key.return_value = {"client_id": "res-1-3", "api_key": "sk-3"}
        return backend

    def test_mints_one_key_clear_of_every_client_id_waldur_holds(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        mock_list.return_value = [
            api_key_row(client_id="res-1-2"),
            api_key_row(client_id="res-1-1", state="Deleted"),
            api_key_row(client_id="", state="Creating", pending_action="create"),
        ]
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client,
            backend,
            _command("create", client_id="", limits={"input_tokens": 1}, allowed_models=None),
        )

        mock_list.assert_called_once_with(mock.sentinel.client, "res-uuid")
        backend.mint_resource_key.assert_called_once_with(
            "res-1", ["res-1-1", "res-1-2"], limits={"input_tokens": 1}, allowed_models=None
        )
        body = mock_set_key.sync.call_args.kwargs["body"]
        self.assertEqual((body.client_id, body.api_key), ("res-1-3", "sk-3"))
        self.assertEqual(mock_set_key.sync.call_args.kwargs["uuid"], "key-uuid")
        mock_erred.assert_not_called()

    def test_waits_when_the_known_ids_cannot_be_read(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """A partial set could hand out a deleted key's id, refused only once live.

        Nothing is minted, and the request is not erred either: a key that erred
        before it had a client_id can only be deleted, so a brief Waldur outage would
        end the request for good. The sweep replays it.
        """
        mock_list.side_effect = httpx.ConnectError("down")
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.mint_resource_key.assert_not_called()
        mock_erred.assert_not_called()

    def test_waits_when_the_resource_cannot_be_read(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        mock_list.return_value = []
        mock_retrieve.sync.side_effect = errors.UnexpectedStatus(503, b"", "url")
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.mint_resource_key.assert_not_called()
        mock_erred.assert_not_called()

    def test_a_refused_read_errs(self, mock_list, mock_set_key, mock_erred, mock_retrieve):
        mock_list.side_effect = errors.UnexpectedStatus(403, b"", "url")
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.mint_resource_key.assert_not_called()
        mock_erred.assert_called_once()

    def test_applies_the_resource_pause_before_minting(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """A resource whose keys are all gone drops out of the membership sync, so
        nothing else would tell the backend it is paused before the new key lands."""
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(paused=True, downscaled=False)
        backend = self._backend()
        order = mock.Mock()
        order.attach_mock(backend.pause_resource, "pause_resource")
        order.attach_mock(backend.mint_resource_key, "mint_resource_key")

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        self.assertEqual(
            [name for name, _, _ in order.mock_calls], ["pause_resource", "mint_resource_key"]
        )
        backend.pause_resource.assert_called_once_with("res-1")

    def test_applies_a_downscale_before_minting(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(paused=False, downscaled=True)
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.downscale_resource.assert_called_once_with("res-1")
        backend.pause_resource.assert_not_called()

    def test_a_live_resource_is_left_as_it_is(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(paused=False, downscaled=False)
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.pause_resource.assert_not_called()
        backend.downscale_resource.assert_not_called()
        # Lifts a pause recorded while the resource had no keys.
        backend.restore_resource.assert_called_once_with("res-1")
        backend.mint_resource_key.assert_called_once()

    def test_an_unexpected_error_errs_rather_than_waiting(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """Only an unanswered Waldur is worth waiting for; a bug should show."""
        mock_list.side_effect = KeyError("client_id")
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.mint_resource_key.assert_not_called()
        mock_erred.assert_called_once()

    def test_withdraws_the_key_when_waldur_refuses_it(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """A refused key is live with no row and a value nobody holds."""
        mock_list.return_value = []
        mock_set_key.sync.side_effect = errors.UnexpectedStatus(400, b"taken", "url")
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        backend.delete_resource_key.assert_called_once_with("res-1-3", "res-1")
        # Waldur answered and the key there is settled: an erred report would be
        # refused in turn.
        mock_erred.assert_not_called()

    def _create_without_answer(self, mock_list, mock_set_key, held: object) -> mock.Mock:
        """Create a key whose set_key gets no answer; Waldur then holds ``held``."""
        mock_list.return_value = []
        mock_set_key.sync.side_effect = httpx.ReadTimeout("slow")
        backend = self._backend()
        with mock.patch(f"{KEYS}.marketplace_resource_api_keys_retrieve") as mock_key:
            if isinstance(held, Exception):
                mock_key.sync.side_effect = held
            else:
                mock_key.sync.return_value = api_key_row(client_id=held)
            resource_api_keys.execute_api_key_command(
                mock.sentinel.client, backend, _command("create", client_id="")
            )
        return backend

    def test_keeps_the_key_when_the_unanswered_report_landed(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """Withdrawing a key Waldur holds would break one it shows as working."""
        backend = self._create_without_answer(mock_list, mock_set_key, "res-1-3")

        backend.delete_resource_key.assert_not_called()
        mock_erred.assert_called_once()

    def test_withdraws_the_key_when_the_unanswered_report_did_not_land(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """Otherwise it stays live with a value nobody holds, and the retry mints another."""
        backend = self._create_without_answer(mock_list, mock_set_key, "")

        backend.delete_resource_key.assert_called_once_with("res-1-3", "res-1")
        mock_erred.assert_called_once()

    def test_keeps_the_key_when_waldur_cannot_say_whether_it_landed(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        backend = self._create_without_answer(
            mock_list, mock_set_key, httpx.ConnectError("down")
        )

        backend.delete_resource_key.assert_not_called()
        mock_erred.assert_called_once()


class TestApiKeyCommandHandler(unittest.TestCase):
    """The STOMP handler dispatches on the payload's action."""

    @staticmethod
    def _frame(**body: object) -> mock.Mock:
        payload: dict[str, object] = {
            "action": "pause",
            "resource_uuid": "res-uuid",
            "resource_backend_id": "res-1",
            "api_key_uuid": "key-uuid",
            "client_id": "res-1-1",
        }
        payload.update(body)
        frame = mock.Mock(spec=stomp.utils.Frame)
        frame.body = json.dumps(payload)
        return frame

    @staticmethod
    def _offering() -> common_structures.Offering:
        return common_structures.Offering(
            name="llm",
            waldur_offering_uuid="off-uuid",
            waldur_api_url="https://example.com/api/",
            waldur_api_token="token",
            backend_type="envoy",
            order_processing_backend="envoy",
        )

    @mock.patch(f"{KEYS}._acknowledge")
    @mock.patch(f"{HANDLERS}.common_utils.get_client_for_offering")
    @mock.patch(f"{HANDLERS}.common_utils.get_backend_for_offering")
    def test_dispatches_a_create_with_its_settings(
        self, mock_get_backend, mock_get_client, mock_post
    ):
        backend = _backend()
        mock_get_backend.return_value = (backend, "1.0")
        with mock.patch(f"{KEYS}.execute_api_key_command") as mock_execute:
            on_resource_api_key_command_stomp(
                self._frame(
                    action="create", client_id="", limits={"input_tokens": 3}, allowed_models=[]
                ),
                self._offering(),
                "ua",
            )
        command = mock_execute.call_args.args[2]
        self.assertEqual(
            command,
            ApiKeyCommand(
                action="create",
                api_key_uuid="key-uuid",
                resource_uuid="res-uuid",
                resource_backend_id="res-1",
                client_id="",
                limits={"input_tokens": 3},
                allowed_models=[],
            ),
        )

    @mock.patch(f"{KEYS}._acknowledge")
    @mock.patch(f"{HANDLERS}.common_utils.get_client_for_offering")
    @mock.patch(f"{HANDLERS}.common_utils.get_backend_for_offering")
    def test_pause_acknowledges_through_set_paused(
        self, mock_get_backend, mock_get_client, mock_post
    ):
        backend = _backend()
        mock_get_backend.return_value = (backend, "1.0")

        on_resource_api_key_command_stomp(self._frame(), self._offering(), "ua")

        backend.pause_resource_key.assert_called_once_with("res-1-1", "res-1")
        self.assertEqual(
            _acknowledged(mock_post, client=mock_get_client.return_value), ["set_paused"]
        )

    @mock.patch(f"{HANDLERS}.common_utils.get_client_for_offering")
    @mock.patch(f"{HANDLERS}.common_utils.get_backend_for_offering")
    def test_a_backend_without_keys_is_left_alone(self, mock_get_backend, mock_get_client):
        mock_get_backend.return_value = (mock.Mock(spec=[]), "1.0")
        with mock.patch(f"{KEYS}.execute_api_key_command") as mock_execute:
            on_resource_api_key_command_stomp(self._frame(), self._offering(), "ua")
        mock_execute.assert_not_called()
        mock_get_client.assert_not_called()


def _client(handler: object) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url="https://w.example.com",
        token="t",  # noqa: S106
        httpx_args={"transport": httpx.MockTransport(handler)},  # type: ignore[arg-type]
    )


class TestWaldurRequests(unittest.TestCase):
    """What goes over the wire, not a mock of it."""

    def test_listing_a_resources_keys_asks_for_deleted_ones_too(self):
        """Without a state filter Waldur leaves deleted keys out."""
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            row = api_key_row(client_id="res-1-1", state="Deleted").to_dict()
            return httpx.Response(200, json=[row])

        rows = resource_api_keys.list_all_api_keys(_client(handler), "res-uuid")

        self.assertEqual([row.client_id for row in rows], ["res-1-1"])
        self.assertEqual(requests[0].url.path, "/api/marketplace-resource-api-keys/")
        self.assertEqual(requests[0].url.params["resource_uuid"], "res-uuid")
        self.assertIn("Deleted", requests[0].url.params.get_list("state"))

    def test_the_sweep_lists_the_states_a_command_leaves_a_key_in(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=[])

        resource_api_keys.find_pending_api_key_commands(
            _client(handler), "off-uuid", cutoff=None, lifecycle=True
        )

        self.assertEqual(len(requests), 1)
        self.assertEqual(
            sorted(requests[0].url.params.get_list("state")), ["Creating", "Deleting", "Updating"]
        )
        self.assertNotIn("modified_before", requests[0].url.params)

    def test_an_acknowledgement_posts_to_its_endpoint(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=api_key_row(state="Paused").to_dict())

        resource_api_keys._acknowledge(
            resource_api_keys.marketplace_resource_api_keys_set_paused,
            _client(handler),
            _command("pause"),
        )

        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(
            requests[0].url.path, "/api/marketplace-resource-api-keys/key-uuid/set_paused/"
        )


K1 = "00000000-0000-0000-0000-000000000001"
K2 = "00000000-0000-0000-0000-000000000002"


@mock.patch(f"{KEYS}.marketplace_resource_api_keys_report_usage")
@mock.patch(f"{KEYS}.list_all_api_keys")
class TestReportApiKeyUsages(unittest.TestCase):
    COMPONENTS = ["input_tokens", "output_tokens"]

    PERIOD = datetime.date(2026, 10, 1)

    def setUp(self) -> None:
        patcher = mock.patch(f"{KEYS}._current_period", return_value=self.PERIOD)
        self.mock_period = patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, mock_list: mock.Mock, report: object, keys: list[dict]) -> None:
        backend = mock.Mock()
        backend.get_resource_key_usage_report.return_value = {"res-1": report}
        mock_list.return_value = [api_key_row(**key) for key in keys]
        resource_api_keys.report_api_key_usages(
            mock.sentinel.client, backend, "res-uuid", "res-1", self.COMPONENTS
        )

    @staticmethod
    def _reported(mock_report: mock.Mock) -> list[tuple[str, dict]]:
        return sorted(
            (str(call.kwargs["uuid"]), call.kwargs["body"].usages.additional_properties)
            for call in mock_report.sync.call_args_list
        )

    def test_reports_each_key_deleted_ones_included(self, mock_list, mock_report):
        self._run(
            mock_list,
            {"res-1-1": {"input_tokens": 10, "output_tokens": 1}, "res-1-2": {"input_tokens": 4}},
            [
                {"uuid": K1, "client_id": "res-1-1", "current_usages": None},
                {"uuid": K2, "client_id": "res-1-2", "current_usages": {}, "state": "Deleted"},
            ],
        )
        mock_list.assert_called_once_with(mock.sentinel.client, "res-uuid")
        self.assertEqual(
            self._reported(mock_report),
            [
                (K1, {"input_tokens": 10.0, "output_tokens": 1.0}),
                (K2, {"input_tokens": 4.0, "output_tokens": 0.0}),
            ],
        )

    def test_a_key_with_no_usage_this_month_is_reset_to_zero(self, mock_list, mock_report):
        """Otherwise last month's figure sticks, and a key paused at its limit stays over it."""
        self._run(
            mock_list,
            {},
            [
                {
                    "uuid": K1,
                    "client_id": "res-1-1",
                    "current_usages": {"input_tokens": 900, "output_tokens": 3},
                }
            ],
        )
        self.assertEqual(
            self._reported(mock_report), [(K1, {"input_tokens": 0.0, "output_tokens": 0.0})]
        )

    def test_unchanged_usage_is_not_sent_again(self, mock_list, mock_report):
        self._run(
            mock_list,
            {"res-1-1": {"input_tokens": 10, "output_tokens": 1}},
            [
                {
                    "uuid": K1,
                    "client_id": "res-1-1",
                    "current_usages": {"input_tokens": 10, "output_tokens": 1},
                    "usage_period": "2026-10-01",
                }
            ],
        )
        mock_report.sync.assert_not_called()

    def test_the_first_report_of_a_month_is_sent_even_when_it_repeats_the_last(
        self, mock_list, mock_report
    ):
        """Waldur would otherwise keep counting the key's usage against last month."""
        self._run(
            mock_list,
            {},
            [
                {
                    "uuid": K1,
                    "client_id": "res-1-1",
                    "current_usages": {"input_tokens": 0, "output_tokens": 0},
                    "usage_period": "2026-09-01",
                }
            ],
        )
        self.assertEqual(
            self._reported(mock_report), [(K1, {"input_tokens": 0.0, "output_tokens": 0.0})]
        )

    def test_each_report_names_its_month(self, mock_list, mock_report):
        """Left out, Waldur files a late report under its own current month."""
        self._run(mock_list, {"res-1-1": {"input_tokens": 1}}, [{"uuid": K1, "client_id": "res-1-1"}])
        self.assertEqual(mock_report.sync.call_args.kwargs["body"].billing_period, self.PERIOD)

    def test_a_collection_straddling_the_turn_of_a_month_is_dropped(
        self, mock_list, mock_report
    ):
        """The backend reports the current month, so its figures may be either month's."""
        self.mock_period.side_effect = [datetime.date(2026, 9, 1), self.PERIOD]
        self._run(mock_list, {"res-1-1": {"input_tokens": 1}}, [{"uuid": K1, "client_id": "res-1-1"}])
        mock_list.assert_not_called()
        mock_report.sync.assert_not_called()

    def test_unattributable_usage_is_not_reported_per_key(self, mock_list, mock_report):
        self._run(mock_list, None, [{"uuid": K1, "client_id": "res-1-1"}])
        mock_list.assert_not_called()
        mock_report.sync.assert_not_called()

    def test_a_requested_key_without_a_client_id_is_skipped(self, mock_list, mock_report):
        self._run(mock_list, {"res-1-1": {"input_tokens": 1}}, [{"uuid": K1, "client_id": ""}])
        mock_report.sync.assert_not_called()

    def test_one_failing_report_does_not_stop_the_others(self, mock_list, mock_report):
        mock_report.sync.side_effect = [httpx.ConnectError("down"), None]
        self._run(
            mock_list,
            {"res-1-1": {"input_tokens": 1}, "res-1-2": {"input_tokens": 2}},
            [{"uuid": K1, "client_id": "res-1-1"}, {"uuid": K2, "client_id": "res-1-2"}],
        )
        self.assertEqual(mock_report.sync.call_count, 2)


class TestManagesApiKeys(unittest.TestCase):
    def test_reads_the_plugin_option_from_additional_properties(self):
        resource = SimpleNamespace(
            offering_plugin_options=SimpleNamespace(
                additional_properties={"enable_api_key_provisioning": True}
            )
        )
        self.assertTrue(resource_api_keys.manages_api_keys(resource))

    def test_reads_the_plugin_option_once_the_sdk_types_it(self):
        resource = SimpleNamespace(
            offering_plugin_options=SimpleNamespace(
                enable_api_key_provisioning=True, additional_properties={}
            )
        )
        self.assertTrue(resource_api_keys.manages_api_keys(resource))

    def test_off_by_default(self):
        self.assertFalse(resource_api_keys.manages_api_keys(SimpleNamespace()))


class TestReportProcessorHook(unittest.TestCase):
    """The report processor reports per-key usage only where both sides support it."""

    @staticmethod
    def _processor(supports: bool) -> object:
        from waldur_site_agent.common.processors import OfferingReportProcessor  # noqa: PLC0415

        processor = OfferingReportProcessor.__new__(OfferingReportProcessor)
        processor.waldur_rest_client = mock.sentinel.client
        processor.resource_backend = mock.Mock(
            supports_resource_api_key_usage=supports,
            backend_components={"input_tokens": {}, "output_tokens": {}},
        )
        return processor

    @staticmethod
    def _resource(managed: bool) -> SimpleNamespace:
        return SimpleNamespace(
            uuid=SimpleNamespace(hex="res-uuid"),
            backend_id="res-1",
            offering_plugin_options=SimpleNamespace(
                additional_properties={"enable_api_key_provisioning": managed}
            ),
        )

    @staticmethod
    def _offering() -> SimpleNamespace:
        return SimpleNamespace(
            components=[
                SimpleNamespace(type_="input_tokens"),
                SimpleNamespace(type_="output_tokens"),
                SimpleNamespace(type_="storage"),  # not metered by this backend
            ]
        )

    @mock.patch(f"{KEYS}.report_api_key_usages")
    def test_reports_the_components_both_sides_share(self, mock_report):
        processor = self._processor(supports=True)
        processor._report_api_key_usages(self._resource(managed=True), self._offering())
        mock_report.assert_called_once_with(
            mock.sentinel.client,
            processor.resource_backend,
            "res-uuid",
            "res-1",
            ["input_tokens", "output_tokens"],
        )

    @mock.patch(f"{KEYS}.report_api_key_usages")
    def test_skipped_when_waldur_does_not_govern_the_keys(self, mock_report):
        self._processor(supports=True)._report_api_key_usages(
            self._resource(managed=False), self._offering()
        )
        mock_report.assert_not_called()

    @mock.patch(f"{KEYS}.report_api_key_usages")
    def test_skipped_when_the_backend_cannot_attribute_usage(self, mock_report):
        self._processor(supports=False)._report_api_key_usages(
            self._resource(managed=True), self._offering()
        )
        mock_report.assert_not_called()

    @mock.patch(f"{KEYS}.report_api_key_usages", side_effect=RuntimeError("down"))
    def test_a_failure_does_not_escape(self, mock_report):
        """The resource total is already reported; a raise would retry all of it."""
        self._processor(supports=True)._report_api_key_usages(
            self._resource(managed=True), self._offering()
        )
