"""Tests for the resource API key event chain (agent-minted, multi-key)."""

import json
import unittest
import uuid as uuid_lib
from unittest import mock

import stomp.utils
from waldur_api_client.models import ObservableObjectTypeEnum
from waldur_api_client.types import UNSET

from waldur_site_agent.common import structures
from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.processors import OfferingOrderProcessor
from waldur_site_agent.event_processing import utils
from waldur_site_agent.event_processing.handlers import (
    on_resource_api_key_rotation_stomp,
)

HANDLERS = "waldur_site_agent.event_processing.handlers"
COMMON_UTILS = "waldur_site_agent.common.utils"
PROCESSORS = "waldur_site_agent.common.processors"


def _make_offering(**overrides):
    defaults = {
        "name": "test-offering",
        "waldur_offering_uuid": "test-uuid",
        "waldur_api_url": "https://example.com/api/",
        "waldur_api_token": "token",
        "backend_type": "envoy",
        "membership_sync_backend": "envoy",
        "order_processing_backend": "envoy",
    }
    defaults.update(overrides)
    return structures.Offering(**defaults)


def _make_frame(**body):
    payload = {
        "action": "rotate",
        "resource_uuid": "res-uuid-1",
        "resource_backend_id": "cid",
        "api_key_uuid": "key-uuid-1",
        "client_id": "cid-1",
    }
    payload.update(body)
    frame = mock.Mock(spec=stomp.utils.Frame)
    frame.body = json.dumps(payload)
    return frame


def _supporting_backend():
    backend = mock.Mock()
    backend.supports_resource_api_keys = True
    return backend


@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_set_erred")
@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_set_key")
@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_report_created")
class TestPushHelpers(unittest.TestCase):
    def test_provision_generates_and_reports_each_key(
        self, mock_created, mock_set_key, mock_erred
    ):
        backend = _supporting_backend()
        backend.generate_resource_keys.return_value = [
            {"client_id": "cid-1", "api_key": "sk-1"},
            {"client_id": "cid-2", "api_key": "sk-2"},
        ]

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "cid", backend)

        backend.generate_resource_keys.assert_called_once_with("cid", count=2)
        self.assertEqual(mock_created.sync.call_count, 2)

    def test_rotate_reports_new_value(self, mock_created, mock_set_key, mock_erred):
        backend = _supporting_backend()
        backend.rotate_resource_key.return_value = "sk-new"

        common_utils.rotate_resource_api_key(mock.Mock(), "key-uuid-1", "cid-1", backend, "cid")

        backend.rotate_resource_key.assert_called_once_with(
            "cid-1", "cid", known_client_ids=None
        )
        self.assertEqual(mock_set_key.sync.call_args.kwargs["uuid"], "key-uuid-1")
        body = mock_set_key.sync.call_args.kwargs["body"]
        self.assertEqual(body.api_key, "sk-new")
        # A stable-client_id backend must not send one at all.
        self.assertNotIn("client_id", body.to_dict())
        mock_erred.sync.assert_not_called()

    def test_rotate_reports_a_new_pair(self, mock_created, mock_set_key, mock_erred):
        # An S3 rotation mints a new access key, so both halves move.
        backend = _supporting_backend()
        backend.rotate_resource_key.return_value = {
            "client_id": "AKIANEW",
            "api_key": "s3secret",
        }

        common_utils.rotate_resource_api_key(
            mock.Mock(), "key-uuid-1", "AKIAOLD", backend, "uid-1"
        )

        backend.rotate_resource_key.assert_called_once_with(
            "AKIAOLD", "uid-1", known_client_ids=None
        )
        body = mock_set_key.sync.call_args.kwargs["body"]
        self.assertEqual(body.api_key, "s3secret")
        self.assertEqual(body.client_id, "AKIANEW")

    @mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_list")
    def test_rotate_forwards_the_resources_known_client_ids(
        self, mock_list, mock_created, mock_set_key, mock_erred
    ):
        """The backend needs Waldur's key set to spot one it stranded earlier."""
        backend = _supporting_backend()
        backend.rotate_resource_key.return_value = "sk-new"
        mock_list.sync_all.return_value = [
            mock.Mock(client_id="cid-1"),
            mock.Mock(client_id="cid-2"),
        ]

        common_utils.rotate_resource_api_key(
            mock.Mock(), "key-uuid-1", "cid-1", backend, "cid", "res-uuid-1"
        )

        backend.rotate_resource_key.assert_called_once_with(
            "cid-1", "cid", known_client_ids=["cid-1", "cid-2"]
        )

    @mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_list")
    def test_an_unlistable_resource_prunes_nothing(
        self, mock_list, mock_created, mock_set_key, mock_erred
    ):
        """A failed listing must be 'unknown', never 'Waldur holds no keys'.

        Passing an empty set here would tell the backend every live credential is an
        orphan, and the rotation would delete all of them.
        """
        backend = _supporting_backend()
        backend.rotate_resource_key.return_value = "sk-new"
        mock_list.sync_all.side_effect = Exception("api down")

        common_utils.rotate_resource_api_key(
            mock.Mock(), "key-uuid-1", "cid-1", backend, "cid", "res-uuid-1"
        )

        backend.rotate_resource_key.assert_called_once_with(
            "cid-1", "cid", known_client_ids=None
        )
        # The rotation itself still went through and was reported.
        self.assertEqual(mock_set_key.sync.call_args.kwargs["body"].api_key, "sk-new")

    def test_rotate_failure_reports_erred(
        self, mock_created, mock_set_key, mock_erred
    ):
        backend = _supporting_backend()
        backend.rotate_resource_key.side_effect = Exception("boom")

        common_utils.rotate_resource_api_key(mock.Mock(), "key-uuid-1", "cid-1", backend, "cid")

        mock_set_key.sync.assert_not_called()
        mock_erred.sync.assert_called_once()
        self.assertEqual(mock_erred.sync.call_args.kwargs["body"].error_message, "boom")


@mock.patch(f"{HANDLERS}.common_utils.rotate_resource_api_key")
@mock.patch(f"{HANDLERS}.common_utils.provision_resource_api_keys")
@mock.patch(f"{HANDLERS}.common_utils.get_client_for_offering")
@mock.patch(f"{HANDLERS}.common_utils.get_backend_for_offering")
class TestApiKeyHandlerDispatch(unittest.TestCase):
    def test_rotate_dispatch(self, mock_backend, mock_client, mock_prov, mock_rot):
        mock_backend.return_value = (_supporting_backend(), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(action="rotate"), _make_offering(), "ua")
        mock_rot.assert_called_once_with(
            mock_client.return_value,
            "key-uuid-1",
            "cid-1",
            mock_backend.return_value[0],
            "cid",
            "res-uuid-1",
            # The rotation path used to be the only handler ignoring the flag, so it
            # forwarded raw backend exception text to a consumer-visible field.
            expose_backend_error_details=True,
        )

    def test_revoke_is_no_longer_dispatched(
        self, mock_backend, mock_client, mock_prov, mock_rot
    ):
        # The key count is fixed at provisioning; a stale revoke command from an
        # older Waldur must be ignored, not guessed at.
        mock_backend.return_value = (_supporting_backend(), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(action="revoke"), _make_offering(), "ua")
        mock_rot.assert_not_called()

    def test_skips_when_backend_lacks_support(
        self, mock_backend, mock_client, mock_prov, mock_rot
    ):
        mock_backend.return_value = (mock.Mock(spec=[]), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(), _make_offering(), "ua")
        mock_rot.assert_not_called()

    def test_invalid_message_ignored(
        self, mock_backend, mock_client, mock_prov, mock_rot
    ):
        on_resource_api_key_rotation_stomp(
            _make_frame(resource_backend_id=""), _make_offering(), "ua"
        )
        mock_backend.assert_not_called()

    def test_helper_failure_does_not_crash(
        self, mock_backend, mock_client, mock_prov, mock_rot
    ):
        mock_backend.return_value = (_supporting_backend(), "1.0")
        mock_rot.side_effect = Exception("api down")
        on_resource_api_key_rotation_stomp(_make_frame(), _make_offering(), "ua")


@mock.patch(f"{PROCESSORS}.utils.provision_resource_api_keys")
class TestCreateResourceProvisionsKeys(unittest.TestCase):
    """_provision_resource_api_keys is the single unit that pushes keys.

    Both the fresh-create path and the reuse_backend_id (restore) path call it, so
    a restored resource is re-provisioned instead of coming back with no keys (F12).
    """

    def _run(self, supports):
        from waldur_site_agent.backend.structures import BackendResourceInfo

        processor = mock.Mock()
        processor.resource_backend = mock.Mock()
        processor.resource_backend.supports_resource_api_keys = supports
        resource = mock.Mock()
        resource.uuid = uuid_lib.uuid4()
        resource.name = "test"
        info = BackendResourceInfo(backend_id="cid")
        OfferingOrderProcessor._provision_resource_api_keys(processor, resource, info)
        return processor, resource

    def test_provisions_for_supporting_backend(self, mock_prov):
        processor, resource = self._run(True)
        mock_prov.assert_called_once_with(
            processor.waldur_rest_client, resource.uuid.hex, "cid", processor.resource_backend
        )

    def test_skipped_for_other_backends(self, mock_prov):
        self._run(False)
        mock_prov.assert_not_called()


class TestApiKeySubscription(unittest.TestCase):
    def test_subscribed_when_order_processing_enabled(self):
        result = utils._determine_observable_object_types(_make_offering())
        self.assertIn(ObservableObjectTypeEnum.RESOURCE_API_KEY_ROTATION, result)


class TestTheReportLegCannotStallARotation(unittest.TestCase):
    """Everything after the backend call used to sit outside the try.

    A failing set_key skipped set_erred, so the row stayed Updating with modified
    unadvanced -- and the reconciliation sweep selects exactly that, so every tick
    re-issued the rotation. Each pass mints a real S3 key and discards the last.
    """

    def test_a_failing_set_key_erres_the_row(self):
        backend = mock.Mock()
        backend.rotate_resource_key.return_value = {
            "client_id": "AKIANEW",
            "api_key": "sk-new",
        }

        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_key,
            "sync",
            side_effect=Exception("waldur 500"),
        ), mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_erred, "sync"
        ) as set_erred:
            common_utils.rotate_resource_api_key(
                mock.Mock(), "key-uuid", "AKIAOLD", backend, "waldur_u"
            )

        set_erred.assert_called_once()
        self.assertIn("waldur 500", set_erred.call_args.kwargs["body"].error_message)

    def test_a_dict_without_a_client_id_erres_rather_than_raising(self):
        """A backend returning the wrong shape must not escape the error path."""
        backend = mock.Mock()
        backend.rotate_resource_key.return_value = {"api_key": "sk-new"}

        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_key, "sync"
        ) as set_key, mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_erred, "sync"
        ) as set_erred:
            common_utils.rotate_resource_api_key(
                mock.Mock(), "key-uuid", "AKIAOLD", backend, "waldur_u"
            )

        set_key.assert_not_called()
        set_erred.assert_called_once()

    def test_a_none_result_erres_rather_than_reporting_an_empty_key(self):
        backend = mock.Mock()
        backend.rotate_resource_key.return_value = None

        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_key, "sync"
        ) as set_key, mock.patch.object(
            common_utils.marketplace_resource_api_keys_set_erred, "sync"
        ) as set_erred:
            common_utils.rotate_resource_api_key(
                mock.Mock(), "key-uuid", "AKIAOLD", backend, "waldur_u"
            )

        set_key.assert_not_called()
        set_erred.assert_called_once()


class TestTheKnownSetIsOnlyTrustedWhenItIsComplete(unittest.TestCase):
    """`None` means unknown and prunes nothing; a list is taken as the whole truth.

    So an empty or partial listing reads as "Waldur holds no keys" and the backends
    prune everything outside it -- taking out the tenant's live credentials.
    """

    def test_an_empty_listing_is_unknown(self):
        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_list, "sync_all", return_value=[]
        ):
            self.assertIsNone(
                common_utils._known_client_ids(mock.Mock(), "res-uuid")
            )

    def test_a_row_without_a_client_id_makes_the_whole_set_unknown(self):
        """An in-flight key is a key Waldur holds; it must not read as absent."""
        rows = [mock.Mock(client_id="AKIA1"), mock.Mock(client_id=UNSET)]
        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_list, "sync_all", return_value=rows
        ):
            self.assertIsNone(
                common_utils._known_client_ids(mock.Mock(), "res-uuid")
            )

    def test_a_complete_listing_is_returned(self):
        rows = [mock.Mock(client_id="AKIA1"), mock.Mock(client_id="AKIA2")]
        with mock.patch.object(
            common_utils.marketplace_resource_api_keys_list, "sync_all", return_value=rows
        ):
            self.assertEqual(
                common_utils._known_client_ids(mock.Mock(), "res-uuid"),
                ["AKIA1", "AKIA2"],
            )
