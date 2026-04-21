"""Tests that sparse field selection includes all fields required by backends.

Verifies that _get_waldur_resources and process_offering request the fields
that plugin backends (cscs-dwdi, k8s-ut-namespace, rancher, etc.) access
on WaldurResource objects.
"""

from __future__ import annotations

import uuid
from unittest import mock

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_field_enum import ResourceFieldEnum
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.models.service_provider import ServiceProvider

from waldur_site_agent.common.processors import (
    OfferingMembershipProcessor,
    OfferingReportProcessor,
)

# Fields that plugin backends access on WaldurResource objects passed
# via pull_resource / pull_resources / add_user / remove_user etc.
_BACKEND_REQUIRED_FIELDS = {
    # Core fields used by all backends
    ResourceFieldEnum.UUID,
    ResourceFieldEnum.BACKEND_ID,
    ResourceFieldEnum.NAME,
    ResourceFieldEnum.STATE,
    ResourceFieldEnum.PROJECT_UUID,
    ResourceFieldEnum.LIMITS,
    ResourceFieldEnum.BACKEND_METADATA,
    ResourceFieldEnum.OFFERING_PLUGIN_OPTIONS,
    # cscs-dwdi: offering_backend_id used for cluster filtering
    ResourceFieldEnum.OFFERING_BACKEND_ID,
    # k8s-ut-namespace: slug used for Keycloak group names
    ResourceFieldEnum.SLUG,
    # rancher: project_slug and customer_slug used for
    # Keycloak groups and Rancher project creation
    ResourceFieldEnum.PROJECT_SLUG,
    ResourceFieldEnum.CUSTOMER_SLUG,
}

# Additional fields used only in the membership sync path
_MEMBERSHIP_EXTRA_FIELDS = {
    ResourceFieldEnum.RESTRICT_MEMBER_ACCESS,
    ResourceFieldEnum.PAUSED,
    ResourceFieldEnum.DOWNSCALED,
    # Required by sync_resource_end_date: omitting end_date causes the scheduler
    # to read it as UNSET (→ None) and overwrite the real value on Waldur B.
    ResourceFieldEnum.END_DATE,
}


def _make_processor(cls):
    """Create a processor instance bypassing __init__."""
    processor = cls.__new__(cls)
    processor._offering_users_cache = None
    processor.waldur_rest_client = mock.Mock()
    processor.offering = mock.Mock()
    processor.offering.uuid = uuid.uuid4().hex
    processor.offering.name = "test-offering"
    processor.resource_backend = mock.Mock()
    processor.service_provider = ServiceProvider(uuid=uuid.uuid4())
    processor.timezone = ""
    return processor


def _make_membership_processor():
    processor = _make_processor(OfferingMembershipProcessor)
    processor._team_cache = {}
    processor._service_accounts_cache = {}
    processor._course_accounts_cache = {}
    return processor


def _make_report_processor():
    return _make_processor(OfferingReportProcessor)


def _make_waldur_resource(**kwargs):
    defaults = dict(
        uuid=uuid.uuid4(),
        name="test-resource",
        backend_id="test-backend-id",
        state=ResourceState.OK,
        project_uuid=uuid.uuid4(),
    )
    defaults.update(kwargs)
    return WaldurResource(**defaults)


class TestMembershipProcessorFieldSelection:
    """Verify _get_waldur_resources requests all required fields."""

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_all_backend_required_fields(self, mock_api):
        """Fields needed by plugin backends are included in the API request."""
        mock_api.sync_all.return_value = [_make_waldur_resource()]

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        missing = _BACKEND_REQUIRED_FIELDS - requested_fields
        assert not missing, f"Missing fields in membership _get_waldur_resources: {missing}"

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_membership_specific_fields(self, mock_api):
        """Membership-specific fields (paused, downscaled, etc.) are included."""
        mock_api.sync_all.return_value = [_make_waldur_resource()]

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        missing = _MEMBERSHIP_EXTRA_FIELDS - requested_fields
        assert not missing, f"Missing membership fields: {missing}"

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_offering_backend_id(self, mock_api):
        """offering_backend_id is requested (needed by cscs-dwdi for cluster filtering)."""
        mock_api.sync_all.return_value = []

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.OFFERING_BACKEND_ID in requested_fields

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_slug(self, mock_api):
        """slug is requested (needed by k8s-ut-namespace for Keycloak group names)."""
        mock_api.sync_all.return_value = []

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.SLUG in requested_fields

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_project_and_customer_slug(self, mock_api):
        """project_slug and customer_slug are requested (needed by rancher)."""
        mock_api.sync_all.return_value = []

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.PROJECT_SLUG in requested_fields
        assert ResourceFieldEnum.CUSTOMER_SLUG in requested_fields

    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_end_date_fields_for_sync(self, mock_api):
        """end_date and end_date_updated_at are fetched in the scheduled membership sync.

        Without these fields the resource arrives with end_date=UNSET, which
        sync_resource_end_date converts to None and pushes to Waldur B —
        silently clearing a real end_date on every sync cycle.
        """
        mock_api.sync_all.return_value = []

        processor = _make_membership_processor()
        processor._get_waldur_resources()

        call_kwargs = mock_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.END_DATE in requested_fields, (
            "end_date missing: scheduler will read UNSET and overwrite real end_date on B with None"
        )


class TestReportProcessorFieldSelection:
    """Verify process_offering requests all required fields."""

    @mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_offerings_retrieve"
    )
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_all_backend_required_fields(self, mock_resources_api, mock_offering_api):
        """Fields needed by plugin backends are included in the API request."""
        mock_resources_api.sync_all.return_value = []
        mock_offering_api.sync.return_value = mock.Mock()

        processor = _make_report_processor()
        processor.process_offering()

        call_kwargs = mock_resources_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        missing = _BACKEND_REQUIRED_FIELDS - requested_fields
        assert not missing, f"Missing fields in report process_offering: {missing}"

    @mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_offerings_retrieve"
    )
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_offering_backend_id(self, mock_resources_api, mock_offering_api):
        """offering_backend_id is requested (needed by cscs-dwdi for cluster filtering)."""
        mock_resources_api.sync_all.return_value = []
        mock_offering_api.sync.return_value = mock.Mock()

        processor = _make_report_processor()
        processor.process_offering()

        call_kwargs = mock_resources_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.OFFERING_BACKEND_ID in requested_fields

    @mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_offerings_retrieve"
    )
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_slug(self, mock_resources_api, mock_offering_api):
        """slug is requested (needed by k8s-ut-namespace for Keycloak group names)."""
        mock_resources_api.sync_all.return_value = []
        mock_offering_api.sync.return_value = mock.Mock()

        processor = _make_report_processor()
        processor.process_offering()

        call_kwargs = mock_resources_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.SLUG in requested_fields

    @mock.patch(
        "waldur_site_agent.common.processors.marketplace_provider_offerings_retrieve"
    )
    @mock.patch("waldur_site_agent.common.processors.marketplace_provider_resources_list")
    def test_includes_project_and_customer_slug(self, mock_resources_api, mock_offering_api):
        """project_slug and customer_slug are requested (needed by rancher)."""
        mock_resources_api.sync_all.return_value = []
        mock_offering_api.sync.return_value = mock.Mock()

        processor = _make_report_processor()
        processor.process_offering()

        call_kwargs = mock_resources_api.sync_all.call_args
        requested_fields = set(call_kwargs.kwargs.get("field", []))

        assert ResourceFieldEnum.PROJECT_SLUG in requested_fields
        assert ResourceFieldEnum.CUSTOMER_SLUG in requested_fields
