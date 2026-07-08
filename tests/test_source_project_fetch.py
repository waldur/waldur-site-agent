"""Tests for OfferingMembershipProcessor._fetch_source_project.

Core pre-fetches the source project (keeping Waldur communication in core) only
for backends that declare ``requires_source_project``; the result is passed to
sync_resource_project / sync_project_end_date as data.

The fetch uses the service-provider-scoped projects endpoint, which is
readable by a regular (non-staff) agent account.
"""

from __future__ import annotations

import uuid
from unittest import mock

from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import ProjectFieldEnum

from waldur_site_agent.common.processors import OfferingMembershipProcessor

_SP_UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_PATCH_TARGET = (
    "waldur_site_agent.common.processors.marketplace_service_providers_projects_list"
)


def _make_processor(requires_source_project: bool):
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.resource_backend = mock.Mock()
    processor.resource_backend.requires_source_project = requires_source_project
    processor.service_provider = mock.Mock(uuid=_SP_UUID)
    processor._source_project_cache = {}
    return processor


def _make_resource(project_uuid):
    resource = mock.Mock()
    resource.project_uuid = project_uuid
    return resource


def test_skips_fetch_when_backend_does_not_require_it():
    processor = _make_processor(requires_source_project=False)
    with mock.patch(_PATCH_TARGET) as mock_list:
        result = processor._fetch_source_project(_make_resource(uuid.uuid4()))

    assert result is None
    mock_list.sync.assert_not_called()


def test_fetches_and_returns_project_when_required():
    processor = _make_processor(requires_source_project=True)
    project_uuid = uuid.uuid4()
    a_project = mock.Mock()
    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.return_value = [a_project]
        result = processor._fetch_source_project(_make_resource(project_uuid))

    assert result is a_project
    mock_list.sync.assert_called_once()
    call = mock_list.sync.call_args
    assert call.args == (_SP_UUID,)
    assert call.kwargs["query"] == project_uuid.hex
    assert call.kwargs["client"] is processor.waldur_rest_client
    # Only the fields the backend reads are requested, not the full project.
    assert ProjectFieldEnum.IS_INDUSTRY in call.kwargs["field"]


def test_caches_source_project_per_project_for_the_cycle():
    """Resources sharing a project fetch it from Waldur A only once."""
    processor = _make_processor(requires_source_project=True)
    project_uuid = uuid.uuid4()
    a_project = mock.Mock()
    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.return_value = [a_project]
        first = processor._fetch_source_project(_make_resource(project_uuid))
        second = processor._fetch_source_project(_make_resource(project_uuid))

    assert first is a_project
    assert second is a_project
    mock_list.sync.assert_called_once()


def test_returns_none_when_project_not_among_service_provider_projects():
    """An empty result yields None; the miss is cached for the cycle."""
    processor = _make_processor(requires_source_project=True)
    project_uuid = uuid.uuid4()
    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.return_value = []
        result = processor._fetch_source_project(_make_resource(project_uuid))
        again = processor._fetch_source_project(_make_resource(project_uuid))

    assert result is None
    assert again is None
    mock_list.sync.assert_called_once()


def test_skips_when_query_matches_multiple_projects():
    """The query param can match other fields, so an ambiguous result is skipped."""
    processor = _make_processor(requires_source_project=True)
    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.return_value = [mock.Mock(), mock.Mock()]
        result = processor._fetch_source_project(_make_resource(uuid.uuid4()))

    assert result is None


def test_returns_none_when_fetch_raises_unexpected_status():
    processor = _make_processor(requires_source_project=True)
    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.side_effect = UnexpectedStatus(
            403,
            b"error",
            "https://waldur-a.example.com/api/marketplace-service-providers/",
        )
        result = processor._fetch_source_project(_make_resource(uuid.uuid4()))

    assert result is None


def test_sync_all_resource_projects_passes_fetched_project_to_backend():
    """The loop pre-fetches the project and hands it to sync_resource_project."""
    processor = _make_processor(requires_source_project=True)
    processor.offering = mock.Mock()
    resource = _make_resource(uuid.uuid4())
    processor._get_waldur_resources = mock.Mock(return_value=[resource])
    processor.resource_backend.pull_resources.return_value = {
        "key": (resource, mock.Mock())
    }
    a_project = mock.Mock()

    with mock.patch(_PATCH_TARGET) as mock_list:
        mock_list.sync.return_value = [a_project]
        processor.sync_all_resource_projects()

    processor.resource_backend.sync_resource_project.assert_called_once_with(
        resource, a_project
    )
