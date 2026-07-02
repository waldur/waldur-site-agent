"""Tests for OfferingMembershipProcessor._fetch_source_project.

Core pre-fetches the source project (keeping Waldur communication in core) only
for backends that declare ``requires_source_project``; the result is passed to
sync_resource_project / sync_project_end_date as data.
"""

from __future__ import annotations

import uuid
from unittest import mock

from waldur_api_client.errors import UnexpectedStatus
from waldur_api_client.models import ProjectFieldEnum

from waldur_site_agent.common.processors import OfferingMembershipProcessor


def _make_processor(requires_source_project: bool):
    processor = OfferingMembershipProcessor.__new__(OfferingMembershipProcessor)
    processor.waldur_rest_client = mock.Mock()
    processor.resource_backend = mock.Mock()
    processor.resource_backend.requires_source_project = requires_source_project
    processor._source_project_cache = {}
    return processor


def _make_resource(project_uuid):
    resource = mock.Mock()
    resource.project_uuid = project_uuid
    return resource


def test_skips_fetch_when_backend_does_not_require_it():
    processor = _make_processor(requires_source_project=False)
    with mock.patch(
        "waldur_site_agent.common.processors.projects_retrieve"
    ) as mock_retrieve:
        result = processor._fetch_source_project(_make_resource(uuid.uuid4()))

    assert result is None
    mock_retrieve.sync.assert_not_called()


def test_fetches_and_returns_project_when_required():
    processor = _make_processor(requires_source_project=True)
    project_uuid = uuid.uuid4()
    a_project = mock.Mock()
    with mock.patch(
        "waldur_site_agent.common.processors.projects_retrieve"
    ) as mock_retrieve:
        mock_retrieve.sync.return_value = a_project
        result = processor._fetch_source_project(_make_resource(project_uuid))

    assert result is a_project
    mock_retrieve.sync.assert_called_once()
    call = mock_retrieve.sync.call_args
    assert call.kwargs["uuid"] == project_uuid
    assert call.kwargs["client"] is processor.waldur_rest_client
    # Only the fields the backend reads are requested, not the full project.
    assert ProjectFieldEnum.IS_INDUSTRY in call.kwargs["field"]


def test_caches_source_project_per_project_for_the_cycle():
    """Resources sharing a project fetch it from Waldur A only once."""
    processor = _make_processor(requires_source_project=True)
    project_uuid = uuid.uuid4()
    a_project = mock.Mock()
    with mock.patch(
        "waldur_site_agent.common.processors.projects_retrieve"
    ) as mock_retrieve:
        mock_retrieve.sync.return_value = a_project
        first = processor._fetch_source_project(_make_resource(project_uuid))
        second = processor._fetch_source_project(_make_resource(project_uuid))

    assert first is a_project
    assert second is a_project
    mock_retrieve.sync.assert_called_once()


def test_returns_none_when_fetch_raises_unexpected_status():
    processor = _make_processor(requires_source_project=True)
    with mock.patch(
        "waldur_site_agent.common.processors.projects_retrieve"
    ) as mock_retrieve:
        mock_retrieve.sync.side_effect = UnexpectedStatus(
            500, b"error", "https://waldur-a.example.com/api/projects/"
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

    with mock.patch(
        "waldur_site_agent.common.processors.projects_retrieve"
    ) as mock_retrieve:
        mock_retrieve.sync.return_value = a_project
        processor.sync_all_resource_projects()

    processor.resource_backend.sync_resource_project.assert_called_once_with(
        resource, a_project
    )
