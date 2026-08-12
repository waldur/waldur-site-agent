"""Tests for CephS3Backend."""

import datetime
import json
from decimal import Decimal
import pytest
from unittest import mock
from unittest.mock import Mock, patch

from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_attributes import ResourceAttributes
from waldur_api_client.models.resource_limits import ResourceLimits

from waldur_site_agent.backend.exceptions import DuplicateResourceError

from waldur_site_agent_ceph_s3.backend import _DEFAULT_KEY_COUNT, CephS3Backend
from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend
from waldur_site_agent_ceph_s3.exceptions import (
    CephS3APIError,
    CephS3Error,
    CephS3UserExistsError,
)


class TestCephS3Backend:
    """Test CephS3Backend functionality."""

    @pytest.fixture
    def backend_settings(self):
        """Backend settings for testing."""
        return {
            "api_url": "https://test.croit.io",
            "s3_endpoint": "https://s3.test.croit.io",
            "username": "admin",
            "password": "secret",
            "verify_ssl": False,
        }

    @pytest.fixture
    def backend_components(self):
        """Backend components for testing."""
        return {
            "s3_storage": {
                "accounting_type": "usage",
                "backend_name": "storage",
                # Decimal GB, matching what the README tells operators to set.
                # The same constant converts bytes to GB for billing and GB to
                # bytes for the ceiling, so the two conventions disagree by 7%
                # on the invoice and the cap at once.
                "unit_factor": 1000000000,
            },
            "s3_user": {
                "accounting_type": "limit",
                "backend_name": "user_quota",
            },
        }

    @pytest.fixture
    def mock_client(self):
        """Mock CroitClient."""
        client = Mock()
        # A real client appends /api to the configured URL; the S3 data endpoint is
        # that URL without the suffix.
        client.api_url = "https://test.croit.io/api"
        client.ping.return_value = True
        client.list_users.return_value = []
        client.create_user.return_value = {}
        client.delete_user.return_value = True
        # Stamped, because the default subject of these tests is a user this agent
        # provisioned. Tests about an account the cluster already had override it.
        client.get_user_info.return_value = {
            "uid": "test_user",
            "name": "Test User",
            "email": "waldur-12345678-1234-5678-9abc-123456789abc-test_user@invalid",
        }
        client.list_user_keys.return_value = []
        client.get_user_buckets.return_value = []
        client.set_user_bucket_quota.return_value = None
        client.set_user_quota.return_value = None
        client.get_user_quota.return_value = {"bucket_quota": {}, "user_quota": {}}
        # Part of the client contract: diagnostics asks each flavour to describe
        # what it talks to, rather than reading croit-only attributes off it.
        client.connection_summary.return_value = {"API URL": "https://test.croit.io/api"}
        return client

    @pytest.fixture
    def waldur_resource(self):
        """Mock Waldur resource."""
        resource = Mock(spec=WaldurResource)
        resource.uuid = "12345678-1234-5678-9abc-123456789abc"
        resource.name = "Test S3 Storage"
        resource.organization = {"slug": "test-org", "name": "Test Organization"}
        resource.project = {"slug": "test-project", "name": "Test Project"}

        # Mock limits
        limits = Mock(spec=ResourceLimits)
        limits.s3_storage = 100  # 100 GB
        resource.limits = limits

        # The real generated type, not a dict: it supports [] but not .get(), and is
        # truthy even when empty, so a dict here hides both of those from every test
        # that touches the quota path.
        resource.attributes = ResourceAttributes.from_dict(
            {
                "storage_limit": 100,  # 100 GB
            }
        )

        return resource

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_backend_initialization(
        self, mock_client_class, backend_settings, backend_components
    ):
        """Test backend initialization."""
        mock_client_class.return_value = Mock()

        backend = CephS3Backend(backend_settings, backend_components)

        assert backend.backend_type == "ceph_s3"
        mock_client_class.assert_called_once()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_missing_required_setting(self, mock_client_class, backend_components):
        """Test initialization with missing required setting."""
        incomplete_settings = {"api_url": "https://test.croit.io"}

        with pytest.raises(
            ValueError,
            match="Either 'token' or both 'username' and 'password' must be provided",
        ):
            CephS3Backend(incomplete_settings, backend_components)

    def test_uid_is_namespaced_when_no_prefix_is_configured(
        self, backend_settings, backend_components
    ):
        """An unset allocation_prefix must not put Waldur uids in the global namespace.

        The uid is the first 10 characters of a consumer-chosen resource name, so
        with an empty prefix an order can name any account the cluster already has.
        """
        with patch("waldur_site_agent_ceph_s3.backend.CroitClient"):
            backend = CephS3Backend(backend_settings, backend_components)

            assert backend._get_resource_backend_id("backup") == "waldur-backup"

    def test_operator_allocation_prefix_wins(
        self, backend_settings, backend_components
    ):
        """A configured namespace is the operator's to choose."""
        backend_settings["allocation_prefix"] = "tenant-"
        with patch("waldur_site_agent_ceph_s3.backend.CroitClient"):
            backend = CephS3Backend(backend_settings, backend_components)

            assert backend._get_resource_backend_id("backup") == "tenant-backup"

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_delete_resource(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test resource deletion."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        # Set the backend_id that would be used for deletion
        waldur_resource.backend_id = "waldur_test_org_test_project_12345678"

        result = backend.delete_resource(waldur_resource)

        assert result is None
        mock_client.delete_user.assert_called_once_with(
            "waldur_test_org_test_project_12345678"
        )

    # --- GB-day usage reporting ---

    @staticmethod
    def _series(*pairs):
        """Build a croit datapoint list from (offset_seconds, gigabytes) pairs."""
        return [
            {"t": 1785542400 + offset, "v": None if gb is None else gb * 1000000000}
            for offset, gb in pairs
        ]

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_storage_is_reported_as_gb_days(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The billed quantity is the area under the storage curve, not a reading.

        The price list bills per GB per day, and Waldur multiplies price by
        quantity flat -- so the days have to be inside the quantity.
        """
        mock_client_class.return_value = mock_client
        # 100 GB for an hour, then 200 GB for an hour.
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 100), (3600, 200), (7200, 200)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        usage = backend._get_usage_report(["waldur_u"])["waldur_u"][
            "TOTAL_ACCOUNT_USAGE"
        ]

        # 100 * 1/24 + 200 * 1/24 + 200 * 1/24 = 20.83 GB-days. The final reading
        # closes one bucket of its own; dropping it was the plugin's whole in-period
        # error budget, and at coarse consolidation it reached 2.13%.
        assert usage["s3_storage"] == 20.83

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_gap_in_the_series_holds_the_last_reading(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """A null is missing telemetry, not an empty bucket.

        Dropping the interval would bill it at zero, so a metrics outage would
        silently discount the tenant.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 240), (3600, None), (7200, 240)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        usage = backend._get_usage_report(["waldur_u"])["waldur_u"][
            "TOTAL_ACCOUNT_USAGE"
        ]

        # Three hours at 240 GB -- the null carries the previous reading, and the
        # final reading closes a bucket of its own: 240 * 3/24 = 30 GB-days.
        assert usage["s3_storage"] == 30.0

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_failed_series_omits_the_resource_rather_than_reporting_zero(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Never report 0 for a resource we could not measure.

        Zero is a valid usage value, so it would overwrite a real accrued total
        for the period and produce an invoice nobody notices is wrong.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.side_effect = CephS3APIError("boom")
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        assert backend._get_usage_report(["waldur_u"]) == {}

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_an_empty_series_omits_the_resource(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """No datapoints means no measurement, which is not the same as no usage."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = []
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        assert backend._get_usage_report(["waldur_u"]) == {}

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_objects_are_not_reported(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Only storage is billed, so the object count no longer rides along.

        It stays visible through get_resource_metadata's storage_summary.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 100), (3600, 100)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        usage = backend._get_usage_report(["waldur_u"])["waldur_u"][
            "TOTAL_ACCOUNT_USAGE"
        ]

        assert "s3_objects" not in usage
        mock_client.get_user_buckets.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_the_series_is_requested_from_the_start_of_the_billing_period(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Usage accrues per billing period, so the integral starts at month start."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 100), (3600, 100)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        backend._get_usage_report(["waldur_u"])

        uid, start, end = mock_client.get_user_storage_series.call_args.args
        assert uid == "waldur_u"
        started = datetime.datetime.fromtimestamp(start, datetime.timezone.utc)
        assert (started.day, started.hour, started.minute) == (1, 0, 0)
        # The window is now closed at both ends so the final bucket can be billed.
        assert end > start

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_an_unreadable_series_is_not_pulled_as_zero(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The base class zero-fills a resource its report omitted.

        That is right where "no usage record" means "used nothing", and wrong for
        a period-to-date accrual: a croit outage would submit 0 and overwrite the
        month's GB-days. Skipping leaves the last good value standing, and the next
        successful pass recomputes the whole period anyway.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = []
        mock_client.get_resource.return_value = {"uid": "waldur_u"}
        mock_client.list_resource_users.return_value = []
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        assert backend._pull_backend_resource("waldur_u") is None

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_measured_zero_is_still_pulled(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """A tenant genuinely holding nothing must still report, at 0."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_storage_series.return_value = self._series((0, 0), (3600, 0))
        mock_client.get_resource.return_value = {"uid": "waldur_u"}
        mock_client.list_resource_users.return_value = []
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend._pull_backend_resource("waldur_u")

        assert info is not None
        assert info.usage["TOTAL_ACCOUNT_USAGE"]["s3_storage"] == 0.0

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_partial_units_survive_rounding(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Short holdings must not round away to nothing.

        Integer division used to bill every partial unit as zero. The same hazard
        applies to GB-days, where an hour of a small volume is a small fraction.
        """
        mock_client_class.return_value = mock_client
        # 12 GB held across two six-minute buckets (the second closed by the
        # window end): 12 * 720/86400 = 0.10 GB-days.
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 12), (360, 12)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        usage = backend._get_usage_report(["waldur_u"])["waldur_u"][
            "TOTAL_ACCOUNT_USAGE"
        ]

        assert usage["s3_storage"] == 0.1
        # The reporter formats with "%.2f", so this is what reaches Waldur.
        assert f"{usage['s3_storage']:.2f}" == "0.10"
        # The reporter subtracts these from amounts the API returns as floats, and
        # Decimal - float raises. Equality alone would not catch it: Decimal("0.05")
        # == 0.05 is True, so assert the type the reporter actually needs.
        assert not isinstance(usage["s3_storage"], Decimal)

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_usage_rounds_rather_than_truncates(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        mock_client_class.return_value = mock_client
        # 3 GB across two half-hour buckets, the second closed by the window end:
        # 3 * 3600/86400 = 0.125 GB-days, which truncation would bill as 0.12 every
        # period. The figure has to land on a half-cent for this to test anything.
        mock_client.get_user_storage_series.return_value = self._series(
            (0, 3), (1800, 3)
        )
        backend = CroitUsageBackend(backend_settings, backend_components)
        backend.client = mock_client

        usage = backend._get_usage_report(["waldur_u"])["waldur_u"][
            "TOTAL_ACCOUNT_USAGE"
        ]

        assert usage["s3_storage"] == 0.13

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_get_resource_metadata(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test resource metadata retrieval."""
        mock_client_class.return_value = mock_client
        mock_client.get_user_info.return_value = {
            "uid": "waldur_test_org_test_project_12345678",
            "name": "Test User",
            "email": "test@example.com",
            "suspended": False,
        }
        mock_client.get_user_buckets.return_value = [
            {"name": "test-bucket", "size_bytes": 1024000, "num_objects": 50}
        ]
        mock_client.get_user_quota.return_value = {
            "bucket_quota": {"enabled": True, "maxSize": 100 * 1024 * 1024 * 1024},
            "user_quota": {"enabled": False},
        }
        # Set the API URL to match expected endpoint
        mock_client.api_url = "https://test.croit.io/api"

        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        metadata = backend.get_resource_metadata(
            "waldur_test_org_test_project_12345678"
        )

        assert "s3_endpoint" in metadata
        assert "user_info" in metadata
        assert "storage_summary" in metadata
        assert "quotas" in metadata
        assert "backend_info" in metadata

        # Connection info only — credentials are resource API keys, not metadata.
        # Flat keys, so {backend_metadata_s3_endpoint} works in a Getting started
        # template; a nested dict would interpolate as [object Object].
        assert metadata["s3_endpoint"] == "https://s3.test.croit.io"
        assert metadata["s3_region"] == "default"
        # The same keys provisioning publishes, so a resource reads identically
        # whether it was just created or last touched by the reporting pass.
        assert metadata["s3_user"] == "waldur_test_org_test_project_12345678"

        # Check storage summary
        storage = metadata["storage_summary"]
        assert storage["bucket_count"] == 1
        assert storage["total_size_bytes"] == 1024000
        assert storage["total_objects"] == 50

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_apply_bucket_quotas(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test bucket quota application."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("test_user", waldur_resource.attributes.to_dict())

        # Check that quota was set with correct values
        mock_client.set_user_bucket_quota.assert_called_once()
        call_args = mock_client.set_user_bucket_quota.call_args[0]
        quota = call_args[1]

        assert quota["enabled"] is True
        assert quota["max_size_bytes"] == 100 * 1000000000  # 100 GB in bytes

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_quota_ceilings_use_the_max_prefixed_keys(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """The ceiling is named apart from the billed quantity.

        Storage is invoiced in GB-days; an attribute called `storage_limit` next
        to that reads like the billing basis rather than a cap on what may be
        held at once.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas(
            "waldur_u", {"max_storage_limit": 7, "max_object_limit": 42}
        )

        quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert quota["max_size_bytes"] == 7 * 1000000000
        assert quota["max_objects"] == 42

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_quotas_still_honour_the_pre_rename_keys(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
    ):
        """Resources ordered before the rename must keep their ceiling.

        Their attributes are fixed at order time, so a restore or re-provision
        would otherwise silently come back with no quota at all.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("waldur_u", {"storage_limit": 5, "object_limit": 100})

        quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert quota["max_size_bytes"] == 5 * 1000000000
        assert quota["max_objects"] == 100

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_the_ceiling_is_applied_as_an_aggregate_user_quota(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """A bucket quota caps each bucket, so on its own it bounds nothing.

        croit exposes no way to cap the bucket count -- RadosGW's max_buckets is
        absent from its API -- so a tenant under a 10 GB *bucket* quota can still
        hold 10 buckets. The user quota is the only ceiling on the total, and
        therefore the only ceiling on the invoice.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("waldur_u", {"max_storage_limit": 10})

        user_quota = mock_client.set_user_quota.call_args.args[1]
        assert user_quota["max_size_bytes"] == 10 * 1000000000
        assert user_quota["enabled"] is True
        # Kept as a per-bucket guard, but it is not what bounds the tenant.
        bucket_quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert bucket_quota["max_size_bytes"] == 10 * 1000000000

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_an_ordered_ceiling_needs_no_enforce_flag(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Ordering a ceiling is the only signal needed to enforce it.

        `enforce_limits` used to gate this, read in one place and defaulting to
        False -- so an operator could order a limit, see it recorded on the
        resource, and get no quota, with nothing to distinguish that from an
        offering that never had one.
        """
        mock_client_class.return_value = mock_client
        assert "enforce_limits" not in backend_components["s3_storage"]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("waldur_u", {"max_storage_limit": 10})

        quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert quota["max_size_bytes"] == 10 * 1000000000

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_missing_ceiling_is_warned_about(
        self, mock_client_class, backend_settings, backend_components, mock_client, caplog
    ):
        """An unbounded resource must not be provisioned quietly."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with caplog.at_level("WARNING"):
            backend._apply_bucket_quotas("waldur_u", {"name": "something"})

        mock_client.set_user_bucket_quota.assert_not_called()
        # The attribute keys are in the message: that is what makes a key mismatch
        # diagnosable instead of invisible.
        assert "no cap" in caplog.text.lower()
        assert "name" in caplog.text

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_user_options_reach_the_client_under_neutral_names(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The backend must not speak one flavour's spellings into a shared call.

        It used to send croit's camelCase, which the radosgw client does not read:
        the settings landed in **kwargs and were dropped without a word.
        """
        mock_client_class.return_value = mock_client
        settings = dict(backend_settings)
        settings.update(
            default_tenant="acme",
            default_placement="fast-pool",
            default_storage_class="STANDARD_IA",
        )
        backend = CephS3Backend(settings, backend_components)
        backend.client = mock_client

        resource = Mock(spec=WaldurResource)
        resource.uuid = "12345678-1234-5678-9abc-123456789abc"
        resource.name = "Test S3 Storage"

        backend._create_or_adopt_user(resource, "waldur_u")

        assert mock_client.create_user.call_args.kwargs == {
            "uid": "waldur_u",
            "name": "Test S3 Storage",
            "email": "waldur-12345678-1234-5678-9abc-123456789abc-waldur_u@invalid",
            "tenant": "acme",
            "default_placement": "fast-pool",
            "default_storage_class": "STANDARD_IA",
        }

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_an_order_with_no_attributes_still_reports_that_nothing_was_capped(
        self, mock_client_class, backend_settings, backend_components, mock_client, caplog
    ):
        """An uncapped resource has no bound on its invoice, so it must be loud.

        The warning that says so lives at the end of _apply_bucket_quotas, and a
        guard on the caller used to skip the call entirely when the order carried
        no attributes -- making the emptiest case the only silent one.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        resource = Mock(spec=WaldurResource)
        resource.uuid = "12345678-1234-5678-9abc-123456789abc"
        resource.name = "Test S3 Storage"
        resource.attributes = None

        with caplog.at_level("WARNING"):
            backend._create_s3_user(resource, "waldur_u")

        assert "no cap" in caplog.text.lower()
        mock_client.set_user_quota.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_object_ceiling_needs_no_component(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The offering has no object component, and the quota must not need one.

        Objects are neither billed nor unit-converted, so the component would only
        have carried an on/off flag -- one more way for an ordered ceiling to be
        silently inactive.
        """
        mock_client_class.return_value = mock_client
        assert "s3_objects" not in backend_components
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend._apply_bucket_quotas("waldur_u", {"max_object_limit": 5000})

        quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert quota["max_objects"] == 5000

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_list_components(
        self, mock_client_class, backend_settings, backend_components
    ):
        """Test component listing."""
        mock_client_class.return_value = Mock()
        backend = CephS3Backend(backend_settings, backend_components)

        components = backend.list_components()

        assert "s3_storage" in components
        assert "s3_user" in components

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_ping(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test backend ping."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.ping()

        assert result is True
        mock_client.ping.assert_called_once()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_diagnostics(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test backend diagnostics."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        result = backend.diagnostics()

        assert result is True
        mock_client.ping.assert_called_once()
        mock_client.list_users.assert_called_once()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_backend_declares_api_key_support(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test that the backend opts into the resource API key lifecycle."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)

        assert backend.supports_resource_api_keys is True

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_generate_resource_keys_applies_each_pair(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test that every minted pair is applied to the S3 user."""
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = []
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        keys = list(backend.generate_resource_keys("waldur_u"))

        assert len(keys) == 2
        assert mock_client.create_user_key.call_count == 2
        for key, call in zip(keys, mock_client.create_user_key.call_args_list):
            uid, access_key, secret_key = call.args
            assert uid == "waldur_u"
            assert key["client_id"] == access_key
            assert key["api_key"] == secret_key
        # Distinct pairs, and the access key follows the RadosGW/AWS convention.
        assert keys[0]["client_id"] != keys[1]["client_id"]
        assert len(keys[0]["client_id"]) == 20
        assert keys[0]["client_id"].isalnum()
        assert keys[0]["client_id"].upper() == keys[0]["client_id"]

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_creating_a_user_removes_the_auto_generated_key(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """RadosGW's auto-created key must not survive provisioning.

        Its secret never reaches Waldur, so it is a working credential that can be
        neither revealed nor rotated. It goes at creation, while it is the user's
        only key and nobody holds credentials yet.
        """
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = [
            {"user": "waldur_u", "access_key": "AUTOKEY", "secret_key": "auto"}
        ]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend.create_resource_with_id(waldur_resource, "waldur_u")

        mock_client.delete_user_key.assert_called_once_with("waldur_u", "AUTOKEY")

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_create_stamps_the_user_with_the_resource_uuid(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """The stamp is what later makes "is this user ours?" answerable.

        Written to a field no consumer controls, so an account the cluster already
        had can never carry one.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend.create_resource_with_id(waldur_resource, "waldur-u")

        assert (
            mock_client.create_user.call_args.kwargs["email"]
            == f"waldur-{waldur_resource.uuid}-waldur-u@invalid"
        )

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_an_unstamped_existing_user_is_refused(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """The takeover this guard exists to stop.

        An account the cluster already had carries no stamp. Provisioning against it
        would write the orderer's quotas onto it and mint keys the orderer can
        reveal, handing over every bucket it owns.
        """
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CephS3UserExistsError("exists")
        mock_client.get_user_info.return_value = {"uid": "backup", "name": "Backups"}
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(DuplicateResourceError):
            backend.create_resource_with_id(waldur_resource, "backup")

        mock_client.set_user_quota.assert_not_called()
        mock_client.set_user_bucket_quota.assert_not_called()
        mock_client.create_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_user_stamped_for_another_resource_is_refused(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Waldur-made is not enough; it has to be made for *this* resource.

        Otherwise a new order could adopt the orphan left by a different resource's
        failed create and inherit its data.
        """
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CephS3UserExistsError("exists")
        mock_client.get_user_info.return_value = {
            "uid": "waldur-u",
            "email": "waldur-99999999-9999-9999-9999-999999999999-waldur-u@invalid",
        }
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(DuplicateResourceError):
            backend.create_resource_with_id(waldur_resource, "waldur-u")

        mock_client.create_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_user_stamped_for_this_resource_is_adopted(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """The interrupted-create retry, which must still self-heal.

        Same resource, so the stamp matches and provisioning finishes rather than
        stranding a half-made user that every retry would trip over.
        """
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CephS3UserExistsError("exists")
        mock_client.get_user_info.return_value = {
            "uid": "waldur-u",
            "email": f"waldur-{waldur_resource.uuid}-waldur-u@invalid",
        }
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend.create_resource_with_id(waldur_resource, "waldur-u")

        assert info.backend_id == "waldur-u"
        mock_client.set_user_quota.assert_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_a_duplicate_reported_as_500_is_verified_too(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """croit reports some duplicates as 500, and that path must not be a way in."""
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CephS3APIError("user already exists")
        mock_client.get_user_info.return_value = {"uid": "backup", "name": "Backups"}
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(DuplicateResourceError):
            backend.create_resource_with_id(waldur_resource, "backup")

        mock_client.create_user_key.assert_not_called()

    def test_two_uids_of_one_resource_get_different_stamps(self):
        """Both backends reject a create whose email another user already holds.

        Measured on croit and on RGW, which answers ``EmailExists``. Without the uid
        in the stamp, re-provisioning a resource under a new id while the old user
        lingers would fail on the address rather than provision.
        """
        from waldur_site_agent_ceph_s3.backend import _ownership_stamp

        uuid = "12345678-1234-5678-9abc-123456789abc"

        assert _ownership_stamp(uuid, "waldur-a") != _ownership_stamp(uuid, "waldur-b")

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_adopting_an_existing_user_never_touches_its_keys(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """A uid this resource owns is adopted, so its live credentials must survive.

        Provisioning reaches an existing S3 user on a restore or a retry after a
        partial create. Waldur may already hold a key on it — reported before the
        earlier attempt died — so deleting "whatever was here" would cut off a
        tenant mid-flight.
        """
        mock_client_class.return_value = mock_client
        mock_client.create_user.side_effect = CephS3UserExistsError("exists")
        mock_client.get_user_info.return_value = {
            "uid": "waldur_u",
            "email": f"waldur-{waldur_resource.uuid}-waldur_u@invalid",
        }
        mock_client.list_user_keys.return_value = [
            {"user": "waldur_u", "access_key": "INUSE", "secret_key": "live"}
        ]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend.create_resource_with_id(waldur_resource, "waldur_u")

        assert info.backend_id == "waldur_u"
        mock_client.delete_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_provisioning_leaves_only_waldur_known_keys(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """The feature's invariant, asserted as an end state.

        Creating the user and minting its keys are separate calls with a test each;
        only their combination shows what survives provisioning. Every credential
        still live has to be one Waldur can reveal and rotate.
        """
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = [
            {"user": "waldur_u", "access_key": "AUTOKEY", "secret_key": "auto"}
        ]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend.create_resource_with_id(waldur_resource, "waldur_u")
        keys = list(backend.generate_resource_keys("waldur_u"))

        # Both sides of the comparison below collapse to empty if nothing is minted,
        # so without this the strongest-sounding test in the file passes while the
        # feature is gutted.
        assert len(keys) == _DEFAULT_KEY_COUNT

        applied = {call.args[1] for call in mock_client.create_user_key.call_args_list}
        deleted = {call.args[1] for call in mock_client.delete_user_key.call_args_list}
        live = ({"AUTOKEY"} | applied) - deleted
        assert live == {key["client_id"] for key in keys}

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_pruning_clears_residue_but_keeps_what_waldur_holds(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """What an adopted uid carries out of an interrupted create.

        The auto key and the orphan of an attempt that died mid-mint are live and
        unrotatable; the pair Waldur already reported is a credential a consumer may
        be using.
        """
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = [
            {"access_key": "AUTOKEY"},
            {"access_key": "ORPHAN"},
            {"access_key": "HELD"},
        ]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        backend.prune_unknown_resource_keys("waldur-u", ["HELD"])

        deleted = {
            call.args[1] for call in mock_client.delete_user_key.call_args_list
        }
        assert deleted == {"AUTOKEY", "ORPHAN"}

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_minting_is_refused_on_a_user_waldur_did_not_make(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Minting is the step that hands out a credential, so it checks too.

        The create path already refuses a foreign uid, but this one is addressed by
        backend id alone — a value that reaches it from Waldur's database rather
        than from a create this process performed.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_info.return_value = {"uid": "backup", "name": "Backups"}
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(CephS3Error):
            list(backend.generate_resource_keys("backup"))

        mock_client.create_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_pruning_is_refused_on_a_user_waldur_did_not_make(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The step that deletes credentials must be at least as careful as the one that mints.

        Minting already refuses a foreign uid. Pruning runs first and is the only
        call here that can take away a working credential, so it cannot be the
        unguarded half of the pair.
        """
        mock_client_class.return_value = mock_client
        mock_client.get_user_info.return_value = {"uid": "backup", "name": "Backups"}
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(CephS3Error):
            backend.prune_unknown_resource_keys("backup", [])

        mock_client.delete_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_generate_resource_keys_is_purely_additive(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Key generation must never delete: the auto key is already gone by now."""
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = [
            {"user": "waldur_u", "access_key": "SOMEONE_ELSES", "secret_key": "x"}
        ]
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        list(backend.generate_resource_keys("waldur_u", count=2))

        mock_client.delete_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_generate_resource_keys_keeps_its_own_new_keys(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test that freshly minted keys are not cleaned up as pre-existing ones."""
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = []
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        list(backend.generate_resource_keys("waldur_u", count=2))

        mock_client.delete_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_generate_resource_keys_is_capped_at_two(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test the two-key cap that keeps one key usable during a rotation."""
        mock_client_class.return_value = mock_client
        mock_client.list_user_keys.return_value = []
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        keys = list(backend.generate_resource_keys("waldur_u", count=5))

        assert len(keys) == 2
        assert mock_client.create_user_key.call_count == 2

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_rotate_applies_the_new_key_before_dropping_the_old(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test zero-downtime rotation: apply first, then remove the old key."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        pair = backend.rotate_resource_key("AKIAOLD", "waldur_u")

        assert pair["client_id"] != "AKIAOLD"
        mock_client.create_user_key.assert_called_once_with(
            "waldur_u", pair["client_id"], pair["api_key"]
        )
        mock_client.delete_user_key.assert_called_once_with("waldur_u", "AKIAOLD")
        create_call = mock.call.create_user_key(
            "waldur_u", pair["client_id"], pair["api_key"]
        )
        delete_call = mock.call.delete_user_key("waldur_u", "AKIAOLD")
        assert mock_client.method_calls.index(create_call) < mock_client.method_calls.index(
            delete_call
        )

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_rotation_leaves_the_sibling_key_live(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Zero downtime is what the two-key cap buys, so assert the end state.

        Call ordering says the new key lands before the old one goes; it says
        nothing about the *other* key, and a rotation that took both would leave
        the resource with a window where nothing works.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client
        first, second = list(backend.generate_resource_keys("waldur_u"))
        # What croit holds once provisioning is done. Without it a rotation that
        # swept the user's keys would delete nothing here and pass unnoticed.
        mock_client.list_user_keys.return_value = [
            {"access_key": first["client_id"]},
            {"access_key": second["client_id"]},
        ]

        rotated = backend.rotate_resource_key(first["client_id"], "waldur_u")

        applied = {call.args[1] for call in mock_client.create_user_key.call_args_list}
        deleted = {call.args[1] for call in mock_client.delete_user_key.call_args_list}
        assert applied - deleted == {rotated["client_id"], second["client_id"]}

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_rotate_drops_a_key_waldur_never_learned_about(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """A re-issued rotation must not strand the previous attempt's key.

        When a rotation's reply to Waldur is lost, the backend has already replaced
        the access key. The sweep re-rotates from the client_id Waldur still holds,
        so without pruning the intermediate key stays live forever and outside the
        lifecycle — exactly the credential this feature exists to eliminate.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client
        mock_client.list_user_keys.return_value = [
            {"access_key": "AKIAORPHAN"},  # the lost rotation's key
            {"access_key": "AKIASIBLING"},  # the resource's other key
        ]

        pair = backend.rotate_resource_key(
            "AKIAOLD", "waldur_u", known_client_ids=["AKIAOLD", "AKIASIBLING"]
        )

        mock_client.delete_user_key.assert_any_call("waldur_u", "AKIAOLD")
        mock_client.delete_user_key.assert_any_call("waldur_u", "AKIAORPHAN")
        deleted = {call.args[1] for call in mock_client.delete_user_key.call_args_list}
        # The sibling and the new key are Waldur's; they must survive.
        assert "AKIASIBLING" not in deleted
        assert pair["client_id"] not in deleted

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_rotate_prunes_nothing_when_the_known_set_is_unavailable(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Unknown must not be read as empty.

        If Waldur could not be listed, pruning "everything not in the known set"
        would delete every credential the resource has.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client
        mock_client.list_user_keys.return_value = [{"access_key": "AKIASIBLING"}]

        backend.rotate_resource_key("AKIAOLD", "waldur_u", known_client_ids=None)

        mock_client.delete_user_key.assert_called_once_with("waldur_u", "AKIAOLD")

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_rotate_failure_leaves_the_old_key_in_place(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test that a failed apply never removes the working key."""
        mock_client_class.return_value = mock_client
        mock_client.create_user_key.side_effect = CephS3APIError("boom")
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        with pytest.raises(CephS3APIError):
            backend.rotate_resource_key("AKIAOLD", "waldur_u")

        mock_client.delete_user_key.assert_not_called()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_metadata_carries_no_credentials(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Test that credentials never travel in resource metadata."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        metadata = backend.get_resource_metadata("waldur_u")

        assert "s3_credentials" not in metadata
        assert "secret_key" not in json.dumps(metadata)
        mock_client.list_user_keys.assert_not_called()
        # Non-secret connection info still reaches the user.
        assert metadata["s3_endpoint"] == "https://s3.test.croit.io"
        assert metadata["s3_region"] == "default"

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_create_with_id_uses_the_supplied_backend_id(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test the path the order processor actually calls."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend.create_resource_with_id(waldur_resource, "waldur_u")

        assert info.backend_id == "waldur_u"
        assert mock_client.create_user.call_args.kwargs["uid"] == "waldur_u"
        assert info.endpoints == [
            {"name": "S3 endpoint", "url": "https://s3.test.croit.io"}
        ]

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_create_publishes_connection_info_as_metadata(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Provisioning must publish what Getting started interpolates.

        get_resource_metadata() only runs on the reporting pass. An agent running
        just order and event processing never calls it, so connection info that
        exists only there leaves {backend_metadata_s3_endpoint} rendering as
        "undefined" on a freshly provisioned resource.
        """
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend.create_resource_with_id(waldur_resource, "waldur_u")

        assert info.backend_metadata == {
            "s3_endpoint": "https://s3.test.croit.io",
            "s3_region": "default",
            "s3_user": "waldur_u",
        }
        # Flat, so the one-level-deep template interpolation reaches every value.
        assert all(
            not isinstance(value, (dict, list))
            for value in info.backend_metadata.values()
        )
        # Credentials are resource API keys and must never ride along in metadata.
        assert "secret" not in json.dumps(info.backend_metadata).lower()

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_region_is_configurable(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """croit exposes no zonegroup, so an operator who named theirs sets it."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(
            {**backend_settings, "s3_region": "eu-north"}, backend_components
        )
        backend.client = mock_client

        info = backend.create_resource_with_id(waldur_resource, "waldur_u")

        assert info.backend_metadata["s3_region"] == "eu-north"

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_create_with_id_applies_bucket_quotas(
        self,
        mock_client_class,
        backend_settings,
        backend_components,
        mock_client,
        waldur_resource,
    ):
        """Test that safety limits are enforced on the processor's create path."""
        mock_client_class.return_value = mock_client
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client
        waldur_resource.attributes = {"storage_limit": 2, "object_limit": 10}

        backend.create_resource_with_id(waldur_resource, "waldur_u")

        quota = mock_client.set_user_bucket_quota.call_args.args[1]
        assert quota["max_size_bytes"] == 2 * 1000000000

    # --- S3 data endpoint (configured, never derived) ---

    def test_missing_s3_endpoint_is_rejected_at_construction(
        self, backend_settings, backend_components
    ):
        """A croit offering without an S3 endpoint hands out unusable credentials.

        On a real cluster the management API and RadosGW are different hosts, so
        there is nothing to derive it from: failing loudly beats publishing the
        management URL and letting a tenant discover it serves HTML.
        """
        del backend_settings["s3_endpoint"]

        with pytest.raises(ValueError, match="s3_endpoint"):
            CephS3Backend(backend_settings, backend_components)

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_endpoint_is_not_derived_from_the_api_url(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """The management URL must never leak into the published endpoint."""
        mock_client_class.return_value = mock_client
        backend_settings["api_url"] = "https://mgmt.croit.io"
        backend_settings["s3_endpoint"] = "https://s3.croit.io"
        backend = CephS3Backend(backend_settings, backend_components)
        backend.client = mock_client

        info = backend._backend_resource_info("waldur_u")

        assert info.endpoints == [{"name": "S3 endpoint", "url": "https://s3.croit.io"}]
        assert "mgmt.croit.io" not in str(info.endpoints)

    @patch("waldur_site_agent_ceph_s3.backend.CroitClient")
    def test_trailing_slash_is_normalised(
        self, mock_client_class, backend_settings, backend_components, mock_client
    ):
        """Operators write URLs both ways; the published one must be stable."""
        mock_client_class.return_value = mock_client
        backend_settings["s3_endpoint"] = "https://s3.croit.io/"
        backend = CephS3Backend(backend_settings, backend_components)

        assert backend.s3_endpoint == "https://s3.croit.io"
