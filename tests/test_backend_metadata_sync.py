"""Tests for the backend-metadata write in membership sync.

Waldur saves the whole resource on every set_backend_metadata call, so the
membership processor skips the call when the backend reports what Waldur
already holds, and writes only when the value changed or is unknown.
"""

from unittest import mock
from uuid import UUID

import pytest
from waldur_api_client.models.backend_metadata import BackendMetadata
from waldur_api_client.types import UNSET

from waldur_site_agent.common.processors import OfferingMembershipProcessor

SET_BACKEND_METADATA = (
    "waldur_site_agent.common.processors.marketplace_provider_resources_set_backend_metadata"
)


@pytest.fixture()
def processor():
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.resource_backend = mock.Mock()
    return processor


def _resource(backend_metadata):
    resource = mock.Mock()
    resource.uuid = UUID("11111111-1111-1111-1111-111111111111")
    resource.backend_id = "alloc-01"
    resource.name = "alloc"
    resource.paused = False
    resource.downscaled = False
    resource.backend_metadata = backend_metadata
    return resource


@mock.patch(SET_BACKEND_METADATA)
def test_unchanged_metadata_is_not_written(mock_set, processor):
    metadata = {"state": "active", "qos": "normal"}
    processor.resource_backend.get_resource_metadata.return_value = dict(metadata)

    processor._sync_resource_status(_resource(BackendMetadata.from_dict(metadata)))

    mock_set.sync.assert_not_called()


@mock.patch(SET_BACKEND_METADATA)
def test_empty_metadata_on_both_sides_is_not_written(mock_set, processor):
    processor.resource_backend.get_resource_metadata.return_value = {}

    processor._sync_resource_status(_resource(BackendMetadata.from_dict({})))

    mock_set.sync.assert_not_called()


@mock.patch(SET_BACKEND_METADATA)
def test_changed_metadata_is_written(mock_set, processor):
    processor.resource_backend.get_resource_metadata.return_value = {
        "state": "active",
        "qos": "slowdown",
    }

    processor._sync_resource_status(
        _resource(BackendMetadata.from_dict({"state": "active", "qos": "normal"}))
    )

    mock_set.sync.assert_called_once()
    body = mock_set.sync.call_args.kwargs["body"]
    assert body.backend_metadata.to_dict() == {"state": "active", "qos": "slowdown"}


@mock.patch(SET_BACKEND_METADATA)
def test_metadata_is_written_when_the_current_value_was_not_fetched(mock_set, processor):
    processor.resource_backend.get_resource_metadata.return_value = {}

    processor._sync_resource_status(_resource(UNSET))

    mock_set.sync.assert_called_once()
