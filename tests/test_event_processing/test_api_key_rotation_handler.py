"""Tests for the resource API key event chain (agent-minted, multi-key)."""

import json
import unittest
import uuid as uuid_lib
from unittest import mock

import stomp.utils
from waldur_api_client.models import ObservableObjectTypeEnum

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
@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_destroy")
@mock.patch(f"{COMMON_UTILS}.marketplace_resource_api_keys_report_created")
class TestPushHelpers(unittest.TestCase):
    def test_provision_generates_and_reports_each_key(
        self, mock_created, mock_destroy, mock_set_key, mock_erred
    ):
        backend = _supporting_backend()
        backend.generate_resource_keys.return_value = [
            {"client_id": "cid-1", "api_key": "sk-1"},
            {"client_id": "cid-2", "api_key": "sk-2"},
        ]

        common_utils.provision_resource_api_keys(mock.Mock(), "res-uuid-1", "cid", backend)

        backend.generate_resource_keys.assert_called_once_with("cid")
        self.assertEqual(mock_created.sync.call_count, 2)

    def test_rotate_reports_new_value(self, mock_created, mock_destroy, mock_set_key, mock_erred):
        backend = _supporting_backend()
        backend.rotate_resource_key.return_value = "sk-new"

        common_utils.rotate_resource_api_key(mock.Mock(), "key-uuid-1", "cid-1", backend)

        backend.rotate_resource_key.assert_called_once_with("cid-1")
        self.assertEqual(mock_set_key.sync.call_args.kwargs["uuid"], "key-uuid-1")
        self.assertEqual(mock_set_key.sync.call_args.kwargs["body"].api_key, "sk-new")
        mock_erred.sync.assert_not_called()

    def test_rotate_failure_reports_erred(
        self, mock_created, mock_destroy, mock_set_key, mock_erred
    ):
        backend = _supporting_backend()
        backend.rotate_resource_key.side_effect = Exception("boom")

        common_utils.rotate_resource_api_key(mock.Mock(), "key-uuid-1", "cid-1", backend)

        mock_set_key.sync.assert_not_called()
        mock_erred.sync.assert_called_once()
        self.assertEqual(mock_erred.sync.call_args.kwargs["body"].error_message, "boom")

    def test_revoke_removes_and_confirms(
        self, mock_created, mock_destroy, mock_set_key, mock_erred
    ):
        backend = _supporting_backend()

        common_utils.revoke_resource_api_key(mock.Mock(), "key-uuid-1", "cid-1", backend)

        backend.revoke_resource_key.assert_called_once_with("cid-1")
        self.assertEqual(mock_destroy.sync.call_args.kwargs["uuid"], "key-uuid-1")


@mock.patch(f"{HANDLERS}.common_utils.revoke_resource_api_key")
@mock.patch(f"{HANDLERS}.common_utils.rotate_resource_api_key")
@mock.patch(f"{HANDLERS}.common_utils.provision_resource_api_keys")
@mock.patch(f"{HANDLERS}.common_utils.get_client_for_offering")
@mock.patch(f"{HANDLERS}.common_utils.get_backend_for_offering")
class TestApiKeyHandlerDispatch(unittest.TestCase):
    def test_rotate_dispatch(self, mock_backend, mock_client, mock_prov, mock_rot, mock_rev):
        mock_backend.return_value = (_supporting_backend(), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(action="rotate"), _make_offering(), "ua")
        mock_rot.assert_called_once_with(
            mock_client.return_value, "key-uuid-1", "cid-1", mock_backend.return_value[0]
        )

    def test_revoke_dispatch(self, mock_backend, mock_client, mock_prov, mock_rot, mock_rev):
        mock_backend.return_value = (_supporting_backend(), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(action="revoke"), _make_offering(), "ua")
        mock_rev.assert_called_once()

    def test_skips_when_backend_lacks_support(
        self, mock_backend, mock_client, mock_prov, mock_rot, mock_rev
    ):
        mock_backend.return_value = (mock.Mock(spec=[]), "1.0")
        on_resource_api_key_rotation_stomp(_make_frame(), _make_offering(), "ua")
        mock_rot.assert_not_called()

    def test_invalid_message_ignored(
        self, mock_backend, mock_client, mock_prov, mock_rot, mock_rev
    ):
        on_resource_api_key_rotation_stomp(
            _make_frame(resource_backend_id=""), _make_offering(), "ua"
        )
        mock_backend.assert_not_called()

    def test_helper_failure_does_not_crash(
        self, mock_backend, mock_client, mock_prov, mock_rot, mock_rev
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
