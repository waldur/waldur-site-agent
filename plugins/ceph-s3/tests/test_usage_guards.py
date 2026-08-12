"""A reported figure must be one the series actually measured.

Zero is a valid usage value, so it overwrites the period's accrued total. The
plugin documents an "omit rather than report zero" property; these are the series
shapes that walked past it.
"""

from unittest.mock import Mock, patch

import pytest

from waldur_site_agent_ceph_s3.reporting import CroitUsageBackend


@pytest.fixture
def backend_settings():
    return {
        "api_url": "https://test.croit.io",
        "s3_endpoint": "https://s3.test.croit.io",
        "token": "t",
        "verify_ssl": False,
    }


@pytest.fixture
def backend_components():
    return {
        "s3_storage": {
            "accounting_type": "usage",
            "backend_name": "storage",
            "unit_factor": 1000000000,
        }
    }


@pytest.fixture
def mock_client():
    client = Mock()
    client.api_url = "https://test.croit.io/api"
    return client


@pytest.mark.parametrize(
    ("label", "datapoints"),
    [
        ("all null", [{"t": t * 180, "v": None} for t in range(8)]),
        ("single point", [{"t": 0, "v": 100}]),
        ("equal timestamps", [{"t": 0, "v": 100}, {"t": 0, "v": 100}]),
    ],
)
@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_an_unmeasurable_series_is_omitted_not_reported_as_zero(
    mock_client_class, backend_settings, backend_components, mock_client, label, datapoints
):
    """croit pads a window predating the S3 user with nulls, not an empty list.

    That is the ordinary shape for a resource created mid-month, not an exotic one:
    a -180d query on a 4-day-old user returned 1945 nulls of 2000 points.
    """
    mock_client_class.return_value = mock_client
    mock_client.get_user_storage_series.return_value = datapoints
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client

    report = backend._get_usage_report(["waldur_u"])

    assert report == {}, f"{label} was reported instead of omitted"
    assert "waldur_u" not in backend._reported_usernames


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_non_finite_reading_is_refused(
    mock_client_class, backend_settings, backend_components, mock_client
):
    """json.loads accepts bare NaN, and "nan" formats straight onto the wire.

    It passes both guards on the way: math.isclose is False so the idempotency
    check proceeds, and `nan < existing` is False so anomaly detection never fires.
    """
    mock_client_class.return_value = mock_client
    mock_client.get_user_storage_series.return_value = [
        {"t": 0, "v": float("nan")},
        {"t": 86400, "v": float("nan")},
    ]
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client

    assert backend._get_usage_report(["waldur_u"]) == {}


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_real_series_is_still_reported(
    mock_client_class, backend_settings, backend_components, mock_client
):
    """The guards must not start omitting series that did measure something."""
    mock_client_class.return_value = mock_client
    mock_client.get_user_storage_series.return_value = [
        {"t": day * 86400, "v": 100 * 10**9} for day in range(11)
    ]
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client

    report = backend._get_usage_report(["waldur_u"])

    assert report["waldur_u"]["TOTAL_ACCOUNT_USAGE"]["s3_storage"] > 0


def test_the_final_reading_is_held_to_the_window_end():
    """The last datapoint spans nothing unless the window end is supplied.

    Dropping it is the whole of the measured consolidation error: harmless at 180 s
    buckets, 2.13% at 6098 s ones. Pinning the end makes the integral correct at any
    resolution rather than only at the default maxDataPoints.
    """
    # 100 GB, sampled once a day, over an 11-day window.
    points = [{"t": day * 86400, "v": 100 * 10**9} for day in range(11)]

    without_end = CroitUsageBackend._integrate_gb_days(points[:-1], 10**9)
    with_end = CroitUsageBackend._integrate_gb_days(
        points[:-1], 10**9, period_end=10 * 86400
    )

    assert round(float(without_end)) == 900
    assert round(float(with_end)) == 1000


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_past_period_is_recomputed_from_the_series(
    mock_client_class, backend_settings, backend_components, mock_client
):
    """The current-period report stops at the last pass before rollover.

    croit still holds the series, so the month can be integrated again with both
    ends pinned -- otherwise an outage over the boundary is a permanent under-bill
    (24h out over a month end is 3.2%).
    """
    mock_client_class.return_value = mock_client
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client
    backend.timezone = "UTC"
    start, end = backend._period_bounds(2026, 6)
    mock_client.get_user_storage_series.return_value = [
        {"t": t, "v": 100 * 10**9} for t in range(start, end, 86400)
    ]

    report = backend.get_usage_report_for_period(["waldur_u"], 2026, 6)

    assert "waldur_u" in report
    mock_client.get_user_storage_series.assert_called_with("waldur_u", start, end)


def test_period_bounds_wrap_the_year():
    """December's end is January of the next year, not month 13."""
    backend = CroitUsageBackend.__new__(CroitUsageBackend)
    backend.timezone = "UTC"

    dec_start, dec_end = backend._period_bounds(2026, 12)
    jan_start, _ = backend._period_bounds(2027, 1)

    assert dec_end == jan_start


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_bucket_with_no_stats_does_not_erase_the_connection_info(
    mock_client_class, backend_settings, backend_components, mock_client
):
    """A bucket with no statistics must not cost the resource its metadata.

    A bucket croit has not measured yet answers usageSum: null, which raised into
    the broad except and returned an error dict -- and the reporting pass pushes
    that dict as backend_metadata, so Getting started loses the S3 endpoint.

    The null itself is now absorbed where the shape is known, by each client (see
    test_client_contract.test_a_bucket_without_statistics_reads_as_zero); what is
    guarded here is that the zero it normalises to still yields full metadata.
    """
    mock_client_class.return_value = mock_client
    mock_client.get_user_info.return_value = {"uid": "waldur_u"}
    mock_client.get_user_buckets.return_value = [
        {"name": "fresh", "size_bytes": 0, "num_objects": 0}
    ]
    mock_client.get_user_quota.return_value = {"bucket_quota": {}, "user_quota": {}}
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client

    metadata = backend.get_resource_metadata("waldur_u")

    assert metadata["s3_endpoint"] == "https://s3.test.croit.io"
    assert metadata["storage_summary"]["total_size_bytes"] == 0


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_key_listing_without_access_key_is_loud(
    mock_client_class, backend_settings, backend_components, mock_client, caplog
):
    """Both key sweeps fail open on a field rename, which is the worst direction.

    croit uses camelCase for usageSum/maxObjects/bucketQuota, so `accessKey` is not
    hypothetical. Skipping silently would leave the auto-generated key live and
    unknown to Waldur -- the exact credential this feature exists to eliminate.
    """
    mock_client_class.return_value = mock_client
    mock_client.list_user_keys.return_value = [{"accessKey": "AUTOKEY"}]
    backend = CroitUsageBackend(backend_settings, backend_components)
    backend.client = mock_client

    backend._remove_auto_generated_keys("waldur_u")

    mock_client.delete_user_key.assert_not_called()
    assert "no access_key" in caplog.text
