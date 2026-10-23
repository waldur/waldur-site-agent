"""Tests for the per-key API key commands and per-key usage reporting."""

from __future__ import annotations

import datetime
import json
import unittest
import uuid
from types import SimpleNamespace
from typing import Any, Optional
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
        with mock.patch(f"{KEYS}._read_resource", return_value=SimpleNamespace(paused=False)):
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
            key_settings=None,
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
            "res-1",
            ["res-1-1", "res-1-2"],
            limits={"input_tokens": 1},
            allowed_models=None,
            resource_limits=None,
        )
        body = mock_set_key.sync.call_args.kwargs["body"]
        self.assertEqual((body.client_id, body.api_key), ("res-1-3", "sk-3"))
        self.assertEqual(mock_set_key.sync.call_args.kwargs["uuid"], "key-uuid")
        mock_erred.assert_not_called()

    def test_hands_the_backend_the_resources_limits(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """A resource's first key has no sibling to read its resource's limits from."""
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(
            paused=False,
            downscaled=False,
            limits=SimpleNamespace(to_dict=lambda: {"token_cost": 50}),
        )
        backend = self._backend()

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        self.assertEqual(
            backend.mint_resource_key.call_args.kwargs["resource_limits"], {"token_cost": 50}
        )

    def test_hands_a_backend_with_periods_the_resources_windows(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """A first key is held to its resource's day and week limits from its mint."""
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(
            paused=False,
            downscaled=False,
            limits=SimpleNamespace(to_dict=lambda: {"token_cost": 50}),
            spending_limits={"token_cost": {"day": 5, "week": 20}},
        )
        backend = self._backend()
        backend.supports_spending_limit_periods = True

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        self.assertEqual(
            backend.mint_resource_key.call_args.kwargs["resource_spending_limits"],
            {"token_cost": {"day": 5.0, "week": 20.0}},
        )

    def test_a_backend_without_periods_gets_no_windows(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve
    ):
        """It keeps the signature it was written against."""
        mock_list.return_value = []
        mock_retrieve.sync.return_value = SimpleNamespace(
            paused=False, downscaled=False, spending_limits={"token_cost": {"day": 5}}
        )
        backend = self._backend()
        backend.supports_spending_limit_periods = False

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

        self.assertNotIn(
            "resource_spending_limits", backend.mint_resource_key.call_args.kwargs
        )

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

    def test_usage_of_a_key_waldur_does_not_hold_is_warned_of(self, mock_list, mock_report):
        with mock.patch(f"{KEYS}.logger") as mock_logger:
            self._run(mock_list, {"res-1-9": {"input_tokens": 1}}, [])
        mock_report.sync.assert_not_called()
        mock_logger.warning.assert_called_once()
        self.assertIn("res-1-9", mock_logger.warning.call_args.args)

    def test_the_unassigned_bucket_is_neither_reported_nor_warned_of(
        self, mock_list, mock_report
    ):
        """It is no key: warning of it would repeat every report cycle."""
        with mock.patch(f"{KEYS}.logger") as mock_logger:
            self._run(mock_list, {"res-1-unassigned": {"input_tokens": 3}}, [])
        mock_report.sync.assert_not_called()
        mock_logger.warning.assert_not_called()


@mock.patch(f"{KEYS}.marketplace_resource_api_keys_report_usage")
@mock.patch(f"{KEYS}.list_all_api_keys")
class TestFinalPreviousMonthReport(unittest.TestCase):
    """In the first days of a month each key's figures for the month before go out once more.

    The current month's report stops at the agent's last round before midnight; without
    a final report the usage after it never counts for a total limit, which never resets.
    """

    COMPONENTS = ["token_cost"]

    def setUp(self) -> None:
        resource_api_keys._final_reports.clear()
        self.addCleanup(resource_api_keys._final_reports.clear)

    def _at(self, today: datetime.date) -> None:
        """Fake the UTC clock: both the month reported and today."""
        for target, value in (
            ("_current_period", today.replace(day=1)),
            ("spending_limits.utc_today", today),
        ):
            patcher = mock.patch(f"{KEYS}.{target}", return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _backend(current: object, previous: object, daily: bool = False) -> mock.Mock:
        backend = mock.Mock(supports_daily_usage=daily)
        backend.get_resource_key_usage_report.return_value = {"res-1": current}
        backend.get_resource_key_usage_report_for_period.return_value = {"res-1": previous}
        return backend

    def _run(self, mock_list, backend, keys, **kwargs) -> None:  # noqa: ANN001
        mock_list.return_value = [api_key_row(**key) for key in keys]
        resource_api_keys.report_api_key_usages(
            mock.sentinel.client, backend, "res-uuid", "res-1", self.COMPONENTS, **kwargs
        )

    @staticmethod
    def _sent(mock_report: mock.Mock) -> list[tuple[str, datetime.date, dict]]:
        return [
            (
                str(call.kwargs["uuid"]),
                call.kwargs["body"].billing_period,
                call.kwargs["body"].usages.additional_properties,
            )
            for call in mock_report.sync.call_args_list
        ]

    def test_on_the_first_the_previous_month_goes_out_before_the_new_one(
        self, mock_list, mock_report
    ):
        """First, so a key Waldur still holds in the old month takes it as that month."""
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend({"res-1-1": {"token_cost": 0.5}}, {"res-1-1": {"token_cost": 9.25}})
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])

        backend.get_resource_key_usage_report_for_period.assert_called_once_with(
            ["res-1"], 2026, 10
        )
        self.assertEqual(
            self._sent(mock_report),
            [
                (K1, datetime.date(2026, 10, 1), {"token_cost": 9.25}),
                (K1, datetime.date(2026, 11, 1), {"token_cost": 0.5}),
            ],
        )
        mock_list.assert_called_once_with(mock.sentinel.client, "res-uuid")

    def test_january_reports_december_of_the_year_before(self, mock_list, mock_report):
        self._at(datetime.date(2027, 1, 2))
        backend = self._backend({}, {"res-1-1": {"token_cost": 1}})
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])

        backend.get_resource_key_usage_report_for_period.assert_called_once_with(
            ["res-1"], 2026, 12
        )
        self.assertEqual(self._sent(mock_report)[0][1], datetime.date(2026, 12, 1))

    def test_due_through_the_last_configured_day_only(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 3))
        backend = self._backend({}, {"res-1-1": {"token_cost": 1}})
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])
        self.assertIn(datetime.date(2026, 10, 1), [sent[1] for sent in self._sent(mock_report)])

        mock_report.reset_mock()
        backend.reset_mock()
        resource_api_keys._final_reports.clear()
        self._at(datetime.date(2026, 11, 4))
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])
        backend.get_resource_key_usage_report_for_period.assert_not_called()
        self.assertEqual(
            [sent[1] for sent in self._sent(mock_report)], [datetime.date(2026, 11, 1)]
        )

    def test_zero_days_turns_it_off(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend({}, {"res-1-1": {"token_cost": 1}})
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}], final_report_days=0)
        backend.get_resource_key_usage_report_for_period.assert_not_called()

    def test_figures_already_sent_are_not_sent_again_and_grown_ones_are(
        self, mock_list, mock_report
    ):
        """Waldur keeps the higher figure, so repeats are harmless but pointless."""
        self._at(datetime.date(2026, 11, 1))
        keys = [{"uuid": K1, "client_id": "res-1-1", "usage_period": "2026-11-01"}]
        backend = self._backend({}, {"res-1-1": {"token_cost": 9.25}})
        self._run(mock_list, backend, keys)
        self._run(mock_list, backend, keys)
        october = [sent for sent in self._sent(mock_report) if sent[1].month == 10]
        self.assertEqual(len(october), 1)

        # The proxy's records of the month's last minutes arrived late.
        backend.get_resource_key_usage_report_for_period.return_value = {
            "res-1": {"res-1-1": {"token_cost": 9.75}}
        }
        self._run(mock_list, backend, keys)
        october = [sent for sent in self._sent(mock_report) if sent[1].month == 10]
        self.assertEqual(october[-1][2], {"token_cost": 9.75})

    def test_a_key_waldur_already_holds_the_month_for_is_skipped(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend(None, {"res-1-1": {"token_cost": 9.25}})
        self._run(
            mock_list,
            backend,
            [
                {
                    "uuid": K1,
                    "client_id": "res-1-1",
                    "usage_period": "2026-10-01",
                    "current_usages": {"token_cost": 9.25},
                }
            ],
        )
        mock_report.sync.assert_not_called()

    def test_a_key_with_no_usage_in_the_month_is_skipped(self, mock_list, mock_report):
        """A zero raises nothing, and the listing holds every key ever deleted."""
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend(None, {"res-1-1": {"token_cost": 0}})
        self._run(
            mock_list,
            backend,
            [
                {"uuid": K1, "client_id": "res-1-1"},
                {"uuid": K2, "client_id": "res-1-2", "state": "Deleted"},
            ],
        )
        mock_report.sync.assert_not_called()

    def test_a_refusal_from_an_older_waldur_is_logged_once_and_the_month_goes_on(
        self, mock_list, mock_report
    ):
        """A Waldur keeping the latest month only answers 409; retrying repeats it."""
        self._at(datetime.date(2026, 11, 2))
        refused = errors.UnexpectedStatus(
            409, b"already holds its usage for 2026-11", httpx.URL("http://waldur/report_usage/")
        )
        mock_report.sync.side_effect = [refused, None, None]
        keys = [{"uuid": K1, "client_id": "res-1-1", "usage_period": "2026-11-01"}]
        backend = self._backend({"res-1-1": {"token_cost": 0.5}}, {"res-1-1": {"token_cost": 9}})

        with mock.patch(f"{KEYS}.logger") as mock_logger:
            self._run(mock_list, backend, keys)
            self._run(mock_list, backend, keys)

        months = [sent[1] for sent in self._sent(mock_report)]
        self.assertEqual(months.count(datetime.date(2026, 10, 1)), 1)
        self.assertEqual(months.count(datetime.date(2026, 11, 1)), 2)
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()

    def test_a_failure_to_reach_waldur_is_retried_next_round(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 1))
        mock_report.sync.side_effect = [httpx.ConnectError("down"), None, None]
        keys = [{"uuid": K1, "client_id": "res-1-1", "usage_period": "2026-11-01"}]
        backend = self._backend(None, {"res-1-1": {"token_cost": 9}})
        self._run(mock_list, backend, keys)
        self._run(mock_list, backend, keys)
        self.assertEqual(
            [sent[1] for sent in self._sent(mock_report)],
            [datetime.date(2026, 10, 1), datetime.date(2026, 10, 1)],
        )

    def test_a_backend_that_cannot_read_a_closed_month_reports_the_current_one(
        self, mock_list, mock_report
    ):
        """The base implementation returns no resource: nothing is sent for it."""
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend({"res-1-1": {"token_cost": 0.5}}, None)
        backend.get_resource_key_usage_report_for_period.return_value = {}
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])
        self.assertEqual(
            [sent[1] for sent in self._sent(mock_report)], [datetime.date(2026, 11, 1)]
        )

    def test_a_failing_collection_does_not_stop_the_current_month(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 1))
        backend = self._backend({"res-1-1": {"token_cost": 0.5}}, None)
        backend.get_resource_key_usage_report_for_period.side_effect = RuntimeError("down")
        self._run(mock_list, backend, [{"uuid": K1, "client_id": "res-1-1"}])
        self.assertEqual(
            [sent[1] for sent in self._sent(mock_report)], [datetime.date(2026, 11, 1)]
        )

    def test_a_key_taking_days_gets_every_day_of_the_closed_month(self, mock_list, mock_report):
        self._at(datetime.date(2026, 11, 1))
        backend = mock.Mock(supports_daily_usage=True)
        backend.get_resource_key_daily_usage_report.return_value = {"res-1": {}}
        backend.get_resource_key_daily_usage_report_for_period.return_value = {
            "res-1": {"res-1-1": {"2026-10-31": {"token_cost": 2.5}}}
        }
        self._run(
            mock_list,
            backend,
            [{"uuid": K1, "client_id": "res-1-1", "spending_limits": None}],
        )

        backend.get_resource_key_daily_usage_report_for_period.assert_called_once_with(
            ["res-1"], 2026, 10
        )
        body = mock_report.sync.call_args_list[0].kwargs["body"].to_dict()
        self.assertEqual(body["billing_period"], "2026-10-01")
        self.assertEqual(body["usages"], {})
        days = body["daily_usages"]
        self.assertEqual(len(days), 31)
        self.assertEqual(days[-1], {"type": "token_cost", "date": "2026-10-31", "amount": "2.50"})


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
            final_report_days=resource_api_keys.FINAL_REPORT_DAYS,
        )

    @mock.patch(f"{KEYS}.report_api_key_usages")
    def test_passes_the_configured_final_report_days(self, mock_report):
        processor = self._processor(supports=True)
        processor.api_key_final_report_days = 0
        processor._report_api_key_usages(self._resource(managed=True), self._offering())
        self.assertEqual(mock_report.call_args.kwargs["final_report_days"], 0)

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


def _offering_user(user_uuid: str, username: object, state: object = None) -> SimpleNamespace:
    from waldur_api_client.models.offering_user_state import OfferingUserState  # noqa: PLC0415

    return SimpleNamespace(
        user_uuid=uuid.UUID(user_uuid),
        username=username,
        user_email=f"{username}@example.com",
        state=OfferingUserState.OK if state is None else state,
    )


ADA_UUID = "a" * 32
BOB_UUID = "b" * 32


def _assignee_backend() -> mock.Mock:
    backend = _backend()
    backend.requires_resource_key_assignees = True
    return backend


def _managed_resource(managed: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        uuid=uuid.UUID("c" * 32),
        backend_id="res-1",
        offering_uuid=uuid.UUID("d" * 32),
        offering_plugin_options=SimpleNamespace(
            additional_properties={"enable_api_key_provisioning": managed}
        ),
    )


class TestKeyAssignments(unittest.TestCase):
    """Waldur's key listing joined to the offering users: client_id -> offering username.

    The username, not the address: it is what the backend added the person under, and
    an address changed in Waldur since is not the one the backend holds.
    """

    def test_each_key_maps_to_its_assignees_username(self):
        keys = [
            api_key_row(client_id="res-1-1", user_uuid=ADA_UUID),
            api_key_row(client_id="res-1-2", user_uuid=None),
        ]
        users = [_offering_user(ADA_UUID, "ada")]
        self.assertEqual(
            resource_api_keys.key_assignments(keys, users),
            {"res-1-1": "ada", "res-1-2": None},
        )

    def test_an_assignee_who_left_the_project_maps_to_nobody(self):
        """Their chat must close, not stay open on a key they still nominally hold."""
        from waldur_api_client.models.offering_user_state import (  # noqa: PLC0415
            OfferingUserState,
        )

        keys = [api_key_row(client_id="res-1-1", user_uuid=ADA_UUID)]
        users = [_offering_user(ADA_UUID, "ada", OfferingUserState.REQUESTED_DELETION)]
        self.assertEqual(resource_api_keys.key_assignments(keys, users), {"res-1-1": None})

    def test_an_assignee_with_no_username_yet_maps_to_nobody(self):
        keys = [api_key_row(client_id="res-1-1", user_uuid=ADA_UUID)]
        users = [_offering_user(ADA_UUID, "")]
        self.assertEqual(resource_api_keys.key_assignments(keys, users), {"res-1-1": None})

    def test_deleted_keys_and_keys_not_minted_yet_are_left_out(self):
        keys = [
            api_key_row(client_id="res-1-1", state="Deleted", user_uuid=ADA_UUID),
            api_key_row(client_id="", state="Creating", user_uuid=ADA_UUID),
        ]
        users = [_offering_user(ADA_UUID, "ada")]
        self.assertEqual(resource_api_keys.key_assignments(keys, users), {})


@mock.patch(f"{KEYS}.list_all_api_keys")
class TestSyncKeyAssignees(unittest.TestCase):
    def test_hands_the_backend_every_key_and_its_assignee(self, mock_list):
        mock_list.return_value = [api_key_row(client_id="res-1-1", user_uuid=BOB_UUID)]
        backend = _assignee_backend()
        resource = _managed_resource()

        resource_api_keys.sync_key_assignees(
            mock.sentinel.client, backend, resource, [_offering_user(BOB_UUID, "bob")]
        )

        mock_list.assert_called_once_with(mock.sentinel.client, "c" * 32)
        backend.sync_resource_key_assignees.assert_called_once_with(
            resource, {"res-1-1": "bob"}
        )

    def test_skipped_when_the_offering_does_not_manage_keys(self, mock_list):
        backend = _assignee_backend()
        resource_api_keys.sync_key_assignees(
            mock.sentinel.client, backend, _managed_resource(managed=False), []
        )
        mock_list.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()

    def test_skipped_for_a_backend_that_does_not_ask(self, mock_list):
        backend = _backend()  # a Mock: the flag is not literally True
        resource_api_keys.sync_key_assignees(
            mock.sentinel.client, backend, _managed_resource(), []
        )
        mock_list.assert_not_called()


@mock.patch(f"{KEYS}.marketplace_offering_users_list")
@mock.patch(f"{KEYS}.marketplace_provider_resources_retrieve")
@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}.marketplace_resource_api_keys_set_key")
@mock.patch(f"{KEYS}.list_all_api_keys")
class TestAssigneesAfterCreate(unittest.TestCase):
    """A new key's assignee reaches the backend at once, not on the next membership pass."""

    def _create(self, backend: mock.Mock) -> None:
        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("create", client_id="")
        )

    def _backend(self) -> mock.Mock:
        backend = _assignee_backend()
        backend.mint_resource_key.return_value = {"client_id": "res-1-2", "api_key": "sk-2"}
        return backend

    def test_the_backend_learns_the_new_keys_assignee(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve, mock_users
    ):
        mock_retrieve.sync.return_value = _managed_resource()
        mock_list.side_effect = [
            [api_key_row(client_id="res-1-1")],
            [
                api_key_row(client_id="res-1-1"),
                api_key_row(client_id="res-1-2", user_uuid=ADA_UUID),
            ],
        ]
        mock_users.sync_all.return_value = [_offering_user(ADA_UUID, "ada")]
        backend = self._backend()

        self._create(backend)

        backend.sync_resource_key_assignees.assert_called_once_with(
            mock_retrieve.sync.return_value, {"res-1-1": None, "res-1-2": "ada"}
        )
        self.assertEqual(
            mock_users.sync_all.call_args.kwargs["offering_uuid"], [uuid.UUID("d" * 32)]
        )
        mock_erred.assert_not_called()

    def test_a_failure_after_the_key_is_live_does_not_err_the_key(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve, mock_users
    ):
        """The key is minted and acknowledged; the next membership pass catches up."""
        mock_retrieve.sync.return_value = _managed_resource()
        mock_list.side_effect = [[], httpx.ConnectError("waldur down")]
        backend = self._backend()

        self._create(backend)

        mock_set_key.sync.assert_called_once()
        mock_erred.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()

    def test_skipped_when_the_offering_does_not_manage_keys(
        self, mock_list, mock_set_key, mock_erred, mock_retrieve, mock_users
    ):
        mock_retrieve.sync.return_value = _managed_resource(managed=False)
        mock_list.return_value = []
        backend = self._backend()

        self._create(backend)

        mock_list.assert_called_once()
        backend.sync_resource_key_assignees.assert_not_called()


class TestMembershipPassHandsOverAssignees(unittest.TestCase):
    @staticmethod
    def _processor() -> object:
        from waldur_site_agent.common.processors import (  # noqa: PLC0415
            OfferingMembershipProcessor,
        )

        processor: Any = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
        processor.waldur_rest_client = mock.sentinel.client
        processor.resource_backend = mock.Mock()
        processor.resource_backend.add_users_to_resource.return_value = set()
        processor.offering = SimpleNamespace(backend_settings={})
        processor._group_resource_usernames = mock.Mock(
            return_value=(set(), set(), set(), {}, {}, {}, {}, {})
        )
        processor._log_user_sync_failures = mock.Mock()
        processor._release_departed_users = mock.Mock()
        processor._confirmed_removals = mock.Mock(return_value=set())
        processor._report_membership_sync_statuses = mock.Mock()
        return processor

    @mock.patch(f"{KEYS}.sync_key_assignees")
    def test_the_user_sync_hands_over_the_assignees(self, mock_sync):
        processor = self._processor()
        resource = SimpleNamespace(name="r", backend_id="res-1", restrict_member_access=False)

        processor._sync_resource_users(resource, mock.Mock(), mock.sentinel.offering_users)

        mock_sync.assert_called_once_with(
            mock.sentinel.client,
            processor.resource_backend,
            resource,
            mock.sentinel.offering_users,
        )

    @mock.patch(f"{KEYS}.sync_key_assignees", side_effect=httpx.ConnectError("waldur down"))
    def test_a_failure_does_not_fail_the_user_sync(self, mock_sync):
        processor = self._processor()
        resource = SimpleNamespace(name="r", backend_id="res-1", restrict_member_access=False)

        processor._sync_resource_users(resource, mock.Mock(), [])

        processor._report_membership_sync_statuses.assert_called_once()


@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}._acknowledge")
@mock.patch(f"{KEYS}._read_resource")
class TestResourcePauseBeforeResume(unittest.TestCase):
    """A resumed or updated key of a paused resource must stay held.

    The backend cannot always tell from its keys that the resource is paused -- not
    when every key is paused on its own -- and each command gets a fresh backend, so
    Waldur's own flag is applied first, as before a create.
    """

    def test_a_resume_applies_the_resources_pause_first(self, mock_read, mock_post, mock_erred):
        mock_read.return_value = SimpleNamespace(paused=True, downscaled=False)
        backend = _backend()
        manager = mock.Mock()
        manager.attach_mock(backend.pause_resource, "pause_resource")
        manager.attach_mock(backend.resume_resource_key, "resume_resource_key")

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("resume"))

        self.assertEqual(
            [name for name, _args, _kwargs in manager.mock_calls],
            ["pause_resource", "resume_resource_key"],
        )
        backend.pause_resource.assert_called_once_with("res-1")

    def test_an_update_of_a_serving_resource_restores_it_first(
        self, mock_read, mock_post, mock_erred
    ):
        mock_read.return_value = SimpleNamespace(paused=False, downscaled=False)
        backend = _backend()

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("update"))

        backend.restore_resource.assert_called_once_with("res-1")
        backend.update_resource_key.assert_called_once()

    def test_a_resume_waits_when_waldur_cannot_be_read(self, mock_read, mock_post, mock_erred):
        mock_read.side_effect = resource_api_keys._NotYet("waldur down")
        backend = _backend()

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("resume"))

        backend.resume_resource_key.assert_not_called()
        mock_post.assert_not_called()
        mock_erred.assert_not_called()

    def test_a_pause_or_a_delete_needs_no_read(self, mock_read, mock_post, mock_erred):
        for action in ("pause", "delete"):
            resource_api_keys.execute_api_key_command(
                mock.sentinel.client, _backend(), _command(action)
            )
        mock_read.assert_not_called()


@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}.utils.rotate_resource_api_key")
@mock.patch(f"{KEYS}._read_resource")
class TestResourcePauseBeforeRotate(unittest.TestCase):
    """A rotation can mint or unblock a key, so it learns the resource's pause first."""

    def test_a_lifecycle_backend_learns_the_pause_before_rotating(
        self, mock_read, mock_rotate, mock_erred
    ):
        mock_read.return_value = SimpleNamespace(paused=True, downscaled=False)
        mock_rotate.return_value = True
        backend = _backend()
        manager = mock.Mock()
        manager.attach_mock(backend.pause_resource, "pause_resource")
        manager.attach_mock(mock_rotate, "rotate")

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("rotate"))

        self.assertEqual([name for name, *_ in manager.mock_calls], ["pause_resource", "rotate"])

    def test_a_rotation_waits_when_waldur_cannot_be_read(self, mock_read, mock_rotate, mock_erred):
        mock_read.side_effect = resource_api_keys._NotYet("waldur down")

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), _command("rotate")
        )

        mock_rotate.assert_not_called()
        mock_erred.assert_not_called()

    def test_a_refused_read_errs_the_rotation(self, mock_read, mock_rotate, mock_erred):
        mock_read.side_effect = errors.UnexpectedStatus(404, b"not found", "https://waldur.example.com/")

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), _command("rotate")
        )

        mock_rotate.assert_not_called()
        mock_erred.assert_called_once()

    def test_a_backend_without_the_lifecycle_rotates_as_before(
        self, mock_read, mock_rotate, mock_erred
    ):
        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(lifecycle=False), _command("rotate")
        )

        mock_read.assert_not_called()
        mock_rotate.assert_called_once()
        # Its rotate_resource_key keeps the signature it always had (ceph-s3's, say).
        self.assertIsNone(mock_rotate.call_args.kwargs["key_settings"])


@mock.patch(f"{KEYS}.marketplace_resource_api_keys_retrieve")
@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}.utils.rotate_resource_api_key")
@mock.patch(f"{KEYS}._read_resource", return_value=SimpleNamespace(paused=False))
class TestRotationCarriesTheKeysSettings(unittest.TestCase):
    """A backend that lost the key mints it back as restricted as Waldur shows it."""

    def test_a_stomp_rotation_reads_the_keys_settings(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        mock_retrieve.sync.return_value = api_key_row(
            client_id="res-1-1",
            limits={"input_tokens": 10},
            allowed_models=["gpt-4o", "o3"],
            effective_models=["gpt-4o"],
        )

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), _command("rotate")
        )

        mock_retrieve.sync.assert_called_once_with(uuid="key-uuid", client=mock.sentinel.client)
        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"],
            # The effective list: the key's own, narrowed by its resource's selection.
            {"limits": {"input_tokens": 10}, "allowed_models": ["gpt-4o"]},
        )

    def test_a_swept_rotation_uses_the_listing_row(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        row = api_key_row(
            client_id="res-1-1", state="Updating", pending_action="rotate", allowed_models=["*"]
        )

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), ApiKeyCommand.from_listing(row, "rotate")
        )

        mock_retrieve.sync.assert_not_called()
        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"], {"limits": {}, "allowed_models": ["*"]}
        )

    def test_a_stomp_rotation_that_carries_settings_uses_them(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        """Waldur rotates a key that changes hands, with the settings the edit set."""
        command = ApiKeyCommand.from_message(
            {
                "action": "rotate",
                "api_key_uuid": "key-uuid",
                "resource_uuid": "res-uuid",
                "resource_backend_id": "res-1",
                "client_id": "res-1-1",
                "limits": None,
                "allowed_models": ["gpt-4o"],
            }
        )

        resource_api_keys.execute_api_key_command(mock.sentinel.client, _backend(), command)

        mock_retrieve.sync.assert_not_called()
        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"],
            {"limits": {}, "allowed_models": ["gpt-4o"]},
        )

    def test_the_resources_limits_from_waldur_travel_with_the_rotation(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        """A lost key that was its resource's only one has no sibling to read them from."""
        mock_read.return_value = SimpleNamespace(
            paused=False, downscaled=False, limits={"token_cost": 2, "tpm": 100000}
        )

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client,
            _backend(),
            _command("rotate", limits={"token_cost": 5}, allowed_models=["*"]),
        )

        # The read that brought the pause up to date is the one the limits come from.
        mock_read.assert_called_once()
        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"],
            {
                "limits": {"token_cost": 5},
                "allowed_models": ["*"],
                "resource_limits": {"token_cost": 2, "tpm": 100000},
            },
        )

    def test_the_resources_limits_travel_when_the_keys_cannot_be_read(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        """The backend still fails closed on the key's own settings, not its resource's."""
        mock_read.return_value = SimpleNamespace(paused=False, limits={"token_cost": 2})
        mock_retrieve.sync.side_effect = httpx.ConnectError("waldur down")

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), _command("rotate")
        )

        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"], {"resource_limits": {"token_cost": 2}}
        )

    def test_the_resources_windows_travel_with_the_rotation_to_a_backend_with_periods(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        """A lost key minted back keeps its resource's day and week limits from Waldur."""
        backend = _backend()
        backend.supports_spending_limit_periods = True
        mock_read.return_value = SimpleNamespace(
            paused=False, limits={"token_cost": 2}, spending_limits={"token_cost": {"day": 1}}
        )

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("rotate", limits={}, allowed_models=["*"])
        )

        settings = mock_rotate.call_args.kwargs["key_settings"]
        self.assertEqual(settings["resource_limits"], {"token_cost": 2})
        self.assertEqual(settings["resource_spending_limits"], {"token_cost": {"day": 1.0}})

    def test_a_resource_without_windows_clears_them_on_rotation(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        backend = _backend()
        backend.supports_spending_limit_periods = True
        mock_read.return_value = SimpleNamespace(paused=False, spending_limits=None)

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, backend, _command("rotate", limits={}, allowed_models=["*"])
        )

        self.assertEqual(
            mock_rotate.call_args.kwargs["key_settings"]["resource_spending_limits"], {}
        )

    def test_unreadable_settings_still_rotate_without_them(
        self, mock_read, mock_rotate, mock_erred, mock_retrieve
    ):
        """A key the backend still holds keeps its settings; a lost one fails closed."""
        mock_retrieve.sync.side_effect = httpx.ConnectError("waldur down")

        resource_api_keys.execute_api_key_command(
            mock.sentinel.client, _backend(), _command("rotate")
        )

        mock_rotate.assert_called_once()
        self.assertIsNone(mock_rotate.call_args.kwargs["key_settings"])
        mock_erred.assert_not_called()


@mock.patch(f"{KEYS}.marketplace_offering_users_list")
@mock.patch(f"{KEYS}.list_all_api_keys")
@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}.utils.rotate_resource_api_key", return_value=True)
@mock.patch(f"{KEYS}._read_resource")
class TestAssigneesAfterRotate(unittest.TestCase):
    """Waldur rotates a key that changes hands, so its new assignee's chat moves at once."""

    def _rotate(self, backend: mock.Mock) -> None:
        resource_api_keys.execute_api_key_command(
            mock.sentinel.client,
            backend,
            _command("rotate", limits={}, allowed_models=["*"]),
        )

    def test_the_backend_learns_the_keys_new_assignee(
        self, mock_read, mock_rotate, mock_erred, mock_list, mock_users
    ):
        resource = _managed_resource()
        mock_read.return_value = resource
        mock_list.return_value = [api_key_row(client_id="res-1-1", user_uuid=BOB_UUID)]
        mock_users.sync_all.return_value = [_offering_user(BOB_UUID, "bob")]
        backend = _assignee_backend()

        self._rotate(backend)

        mock_rotate.assert_called_once()
        backend.sync_resource_key_assignees.assert_called_once_with(
            resource, {"res-1-1": "bob"}
        )
        mock_erred.assert_not_called()

    def test_a_failed_rotation_moves_nothing(
        self, mock_read, mock_rotate, mock_erred, mock_list, mock_users
    ):
        mock_read.return_value = _managed_resource()
        mock_rotate.return_value = False
        backend = _assignee_backend()

        self._rotate(backend)

        mock_list.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()

    def test_a_failed_sync_leaves_the_rotation_alone(
        self, mock_read, mock_rotate, mock_erred, mock_list, mock_users
    ):
        """The key is rotated and acknowledged; the next membership pass catches up."""
        mock_read.return_value = _managed_resource()
        mock_list.side_effect = httpx.ConnectError("waldur down")
        backend = _assignee_backend()

        self._rotate(backend)

        mock_erred.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()

    def test_skipped_when_the_offering_does_not_manage_keys(
        self, mock_read, mock_rotate, mock_erred, mock_list, mock_users
    ):
        mock_read.return_value = _managed_resource(managed=False)
        backend = _assignee_backend()

        self._rotate(backend)

        mock_list.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()

    def test_a_backend_without_the_lifecycle_syncs_nothing(
        self, mock_read, mock_rotate, mock_erred, mock_list, mock_users
    ):
        backend = _backend(lifecycle=False)
        backend.requires_resource_key_assignees = True

        self._rotate(backend)

        mock_read.assert_not_called()
        mock_list.assert_not_called()
        backend.sync_resource_key_assignees.assert_not_called()


@mock.patch(f"{KEYS}._set_erred")
@mock.patch(f"{KEYS}._acknowledge")
@mock.patch(f"{KEYS}.marketplace_provider_resources_retrieve")
class TestReadingTheResource(unittest.TestCase):
    def test_the_read_asks_for_the_resources_limits(self, mock_retrieve, mock_post, mock_erred):
        resource_api_keys._read_resource(mock.sentinel.client, _command("create"))

        self.assertIn("limits", [f.value for f in mock_retrieve.sync.call_args.kwargs["field"]])

    def test_the_read_asks_for_the_resources_spending_limits(
        self, mock_retrieve, mock_post, mock_erred
    ):
        resource_api_keys._read_resource(mock.sentinel.client, _command("create"))

        self.assertIn(
            "spending_limits", [f.value for f in mock_retrieve.sync.call_args.kwargs["field"]]
        )

    def test_a_refused_read_errs_a_resume_rather_than_leaving_it_pending(
        self, mock_retrieve, mock_post, mock_erred
    ):
        """Waldur answered and refused: replaying would only be refused again."""
        mock_retrieve.sync.side_effect = errors.UnexpectedStatus(404, b"not found", "https://waldur.example.com/")
        backend = _backend()

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("resume"))

        backend.resume_resource_key.assert_not_called()
        mock_erred.assert_called_once()

    def test_an_unanswered_read_leaves_an_update_pending(
        self, mock_retrieve, mock_post, mock_erred
    ):
        mock_retrieve.sync.side_effect = httpx.ConnectError("waldur down")
        backend = _backend()

        resource_api_keys.execute_api_key_command(mock.sentinel.client, backend, _command("update"))

        backend.update_resource_key.assert_not_called()
        mock_erred.assert_not_called()
