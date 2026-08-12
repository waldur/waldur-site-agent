"""The key lifecycle under partial failure.

Every shipped test drives a path where every call succeeds. The invariant the
feature exists to uphold -- a credential that works is one Waldur can rotate -- is
only interesting when something fails half-way, which is exactly what was never
exercised.
"""

from unittest.mock import Mock, patch

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_attributes import ResourceAttributes

from waldur_site_agent_ceph_s3.backend import CephS3Backend
from waldur_site_agent_ceph_s3.exceptions import CephS3APIError, CephS3UserExistsError


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
def croit():
    """A client that models croit's key set, so "live" means what the server holds.

    A bare Mock records the call that raised as though it had landed, which
    overstates the damage; counting only the applies that returned is what makes
    the assertion defensible.
    """
    client = Mock()
    client.live = set()
    client.fail_on = None

    def create_user_key(uid, access_key, secret_key):
        if client.fail_on == len(client.live) + 1:
            raise CephS3APIError("croit 503 while applying the key")
        client.live.add(access_key)

    def delete_user_key(uid, access_key):
        client.live.discard(access_key)

    def list_user_keys(uid):
        return [{"access_key": key} for key in sorted(client.live)]

    client.create_user_key.side_effect = create_user_key
    client.delete_user_key.side_effect = delete_user_key
    client.list_user_keys.side_effect = list_user_keys
    # Stamped, because these tests are about a user this agent provisioned. The
    # ones about adopting override it with the resource whose retry they describe.
    client.get_user_info.return_value = {
        "uid": "waldur_u",
        "email": "waldur-12345678-1234-5678-9abc-123456789abc-waldur_u@invalid",
    }
    return client


def stamped_for(resource, uid="waldur_u"):
    """The user an interrupted create for this resource left behind.

    Adoption is refused unless the uid carries this resource's stamp, so a test
    about a retry has to say which resource the leftover user belongs to.
    """
    return {"uid": uid, "email": f"waldur-{resource.uuid}-{uid}@invalid"}


@pytest.fixture
def waldur_resource():
    resource = Mock(spec=WaldurResource)
    resource.uuid = "12345678-1234-5678-9abc-123456789abc"
    resource.name = "Test S3 Storage"
    resource.organization = {"slug": "test-org"}
    resource.project = {"slug": "test-project"}
    resource.limits = None
    resource.attributes = ResourceAttributes.from_dict({"max_storage_limit": 100})
    return resource


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_provisioning_reports_every_key_it_applied(
    mock_client_class, backend_settings, backend_components, croit
):
    """A key live on croit must have a row in Waldur, even when provisioning dies.

    generate_resource_keys used to apply both pairs before the caller reported
    any, so a failure on the second left the first live and unknown -- and nothing
    recovers it: the sweep only revisits Updating rows, and pruning needs a
    client_id Waldur holds. With none, the orphan is permanent.
    """
    from waldur_site_agent.common import utils as common_utils

    mock_client_class.return_value = croit
    croit.fail_on = 2
    backend = CephS3Backend(backend_settings, backend_components)
    backend.client = croit

    with patch(
        "waldur_site_agent.common.utils.marketplace_resource_api_keys_report_created"
    ) as report:
        with pytest.raises(CephS3APIError):
            common_utils.provision_resource_api_keys(
                Mock(), "res-uuid-1", "waldur_u", backend
            )

    reported = {call.kwargs["body"].client_id for call in report.sync.call_args_list}

    assert croit.live == reported, (
        f"{croit.live - reported} is live on croit with no row in Waldur: "
        "a working S3 credential nobody can reveal, rotate or revoke"
    )


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_key_minted_during_the_rotation_is_never_pruned(
    mock_client_class, backend_settings, backend_components, croit
):
    """The keep-set is computed before the rotation; the key set changes during it.

    A concurrent rotation of the sibling key reports its new key to Waldur while
    this one is still running. Pruning against the stale snapshot deletes it, and
    Waldur ends up publishing a credential croit does not have -- permanently,
    because the sweep only revisits Updating rows.
    """
    mock_client_class.return_value = croit
    croit.live = {"AKIAOLD", "AKIASIB"}
    backend = CephS3Backend(backend_settings, backend_components)
    backend.client = croit

    real_delete = croit.delete_user_key.side_effect
    fired = []

    def delete_and_simulate_concurrent_rotation(uid, access_key):
        real_delete(uid, access_key)
        # The sibling's rotation lands once, between our delete and our prune --
        # not on every subsequent delete, or the prune would keep resurrecting it
        # and the race would look closed when it is not.
        if not fired:
            fired.append(True)
            croit.live.discard("AKIASIB")
            croit.live.add("AKIASIBNEW")

    croit.delete_user_key.side_effect = delete_and_simulate_concurrent_rotation

    backend.rotate_resource_key(
        "AKIAOLD", "waldur_u", known_client_ids=["AKIAOLD", "AKIASIB"]
    )

    assert "AKIASIBNEW" in croit.live, (
        "the concurrent rotation's key was pruned as an orphan; Waldur now "
        "publishes a credential croit does not have"
    )


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_an_adopted_uid_still_gets_its_ceiling(
    mock_client_class, backend_settings, backend_components, croit, waldur_resource
):
    """A retry after a partial create must not leave the resource uncapped.

    create_user succeeding and a later call failing erres the order; the retry
    adopts the existing uid and used to return success from inside the except
    block, before _apply_bucket_quotas was ever reached.
    """
    mock_client_class.return_value = croit
    croit.create_user.side_effect = CephS3UserExistsError("exists")
    croit.get_user_info.return_value = stamped_for(waldur_resource)
    backend = CephS3Backend(backend_settings, backend_components)
    backend.client = croit

    backend.create_resource_with_id(waldur_resource, "waldur_u")

    croit.set_user_quota.assert_called_once()
    assert croit.set_user_quota.call_args.args[1]["max_size_bytes"] == 100 * 1000000000


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_the_500_flavoured_exists_reply_also_gets_its_ceiling(
    mock_client_class, backend_settings, backend_components, croit, waldur_resource
):
    """croit reports a duplicate uid as a 500 as well as a 409.

    _send_with_status_retries retries POST /s3/users on 5xx, so this branch is
    reachable inside a single provisioning attempt with nobody doing anything wrong.
    """
    mock_client_class.return_value = croit
    croit.create_user.side_effect = CephS3APIError("user already exists in zonegroup")
    croit.get_user_info.return_value = stamped_for(waldur_resource)
    backend = CephS3Backend(backend_settings, backend_components)
    backend.client = croit

    info = backend.create_resource_with_id(waldur_resource, "waldur_u")

    assert info.backend_id == "waldur_u"
    croit.set_user_quota.assert_called_once()


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_an_adopted_uid_keeps_its_existing_keys(
    mock_client_class, backend_settings, backend_components, croit, waldur_resource
):
    """Re-asserting the quota must not start deleting a tenant's credentials."""
    mock_client_class.return_value = croit
    croit.live = {"AKIAINUSE"}
    croit.create_user.side_effect = CephS3UserExistsError("exists")
    croit.get_user_info.return_value = stamped_for(waldur_resource)
    backend = CephS3Backend(backend_settings, backend_components)
    backend.client = croit

    backend.create_resource_with_id(waldur_resource, "waldur_u")

    assert "AKIAINUSE" in croit.live


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_the_quota_finds_the_storage_component_under_any_key(
    mock_client_class, backend_settings, croit, waldur_resource
):
    """Usage selects on backend_name; the quota used to select on the component key.

    An operator who names the component `storage` got billing with no cap -- the
    same silent unbounded-invoice failure the quota handling exists to prevent,
    just moved somewhere the warning does not point at.
    """
    mock_client_class.return_value = croit
    backend = CephS3Backend(
        backend_settings,
        {"storage": {"backend_name": "storage", "unit_factor": 1000000000}},
    )
    backend.client = croit

    backend._apply_bucket_quotas("waldur_u", {"max_storage_limit": 7})

    assert croit.set_user_quota.call_args.args[1]["max_size_bytes"] == 7 * 1000000000


@patch("waldur_site_agent_ceph_s3.backend.CroitClient")
def test_a_ceiling_with_no_storage_component_is_warned_about(
    mock_client_class, backend_settings, croit, caplog
):
    """unit_factor lives on the component; without one there is nothing to convert.

    Sending the raw number would be a ceiling in bytes, which is worse than sending
    none -- so this path skips the quota, and the warning has to name what it looked
    for rather than only what it was given.
    """
    mock_client_class.return_value = croit
    backend = CephS3Backend(backend_settings, {})
    backend.client = croit

    backend._apply_bucket_quotas("waldur_u", {"max_storage_limit": 100})

    croit.set_user_quota.assert_not_called()
    assert "storage components []" in caplog.text
