"""Changing a resource's ceilings through an UPDATE order re-applies its quotas."""

from unittest.mock import Mock, patch
from uuid import UUID

import pytest
from waldur_api_client.models.resource import Resource as WaldurResource
from waldur_api_client.models.resource_attributes import ResourceAttributes
from waldur_api_client.models.resource_options_type_0 import ResourceOptionsType0

from waldur_site_agent_ceph_s3.backend import CephS3Backend
from waldur_site_agent_ceph_s3.exceptions import CephS3Error

from conftest import CROIT_SETTINGS, RADOSGW_SETTINGS

GB = 1000000000
RESOURCE_UUID = UUID("12345678-1234-5678-9abc-123456789abc")
COMPONENTS = {
    "s3_storage": {"accounting_type": "usage", "backend_name": "storage", "unit_factor": GB}
}
STAMP = "waldur-12345678-1234-5678-9abc-123456789abc-acme.waldur-x@invalid"


def make_backend(settings=CROIT_SETTINGS):
    target = "RadosGWClient" if settings is RADOSGW_SETTINGS else "CroitClient"
    with patch(f"waldur_site_agent_ceph_s3.backend.{target}"):
        backend = CephS3Backend(dict(settings), COMPONENTS)
    backend.client = Mock()
    backend.client.get_user_info.return_value = {"uid": "acme$waldur-x", "email": STAMP}
    return backend


def make_resource(attributes=None, options=None, backend_id="acme$waldur-x"):
    resource = WaldurResource(uuid=RESOURCE_UUID, name="x", backend_id=backend_id)
    resource.attributes = ResourceAttributes.from_dict(attributes or {})
    if options is not None:
        resource.options = ResourceOptionsType0.from_dict(options)
    return resource


def quotas(backend):
    user = backend.client.set_user_quota.call_args
    bucket = backend.client.set_user_bucket_quota.call_args
    assert user.args[0] == bucket.args[0] == "acme$waldur-x"
    assert user.args[1] == bucket.args[1]
    return user.args[1]


class TestUpdateOptions:
    def test_a_raised_ceiling_is_applied_to_both_quotas(self):
        backend = make_backend()
        resource = make_resource({"max_storage_limit": 5, "max_object_limit": 100})

        assert backend.update_resource_options(resource, {"max_storage_limit": 7}) is True

        assert quotas(backend) == {
            "enabled": True,
            "max_size_bytes": 7 * GB,
            # Kept from the order: an update changes what it names, not the rest.
            "max_objects": 100,
        }

    def test_current_options_win_over_the_order_attributes(self):
        """An earlier update already moved the ceiling; the next one builds on it."""
        backend = make_backend()
        resource = make_resource(
            {"max_storage_limit": 5, "max_object_limit": 100},
            options={"max_storage_limit": 7, "max_object_limit": 200},
        )

        backend.update_resource_options(resource, {"max_storage_limit": 9})

        assert quotas(backend) == {"enabled": True, "max_size_bytes": 9 * GB, "max_objects": 200}

    @pytest.mark.parametrize("cleared", [None, 0])
    def test_a_cleared_ceiling_is_sent_as_no_limit(self, cleared):
        """-1, never 0 and never left out: RadosGW merges a quota PUT, so a left-out
        limit would survive, and croit stores 0 as no limit while RGW caps at it."""
        backend = make_backend()
        resource = make_resource({"max_storage_limit": 5, "max_object_limit": 100})

        backend.update_resource_options(resource, {"max_object_limit": cleared})

        assert quotas(backend) == {"enabled": True, "max_size_bytes": 5 * GB, "max_objects": -1}

    def test_a_cleared_ceiling_does_not_fall_back_to_a_legacy_key(self):
        backend = make_backend()
        resource = make_resource({"storage_limit": 5, "object_limit": 100})

        backend.update_resource_options(resource, {"max_object_limit": None})

        assert quotas(backend)["max_objects"] == -1
        assert quotas(backend)["max_size_bytes"] == 5 * GB

    def test_clearing_every_ceiling_lifts_both_quotas_loudly(self, caplog):
        backend = make_backend()
        resource = make_resource({"max_storage_limit": 5})

        with caplog.at_level("WARNING"):
            backend.update_resource_options(resource, {"max_storage_limit": None})

        assert quotas(backend) == {"enabled": True, "max_size_bytes": -1, "max_objects": -1}
        assert "Removing every capacity ceiling" in caplog.text

    def test_a_foreign_user_is_refused_before_any_quota(self):
        backend = make_backend()
        backend.client.get_user_info.return_value = {"uid": "acme$waldur-x", "email": ""}

        with pytest.raises(CephS3Error, match="not provisioned by Waldur"):
            backend.update_resource_options(make_resource(), {"max_storage_limit": 7})
        backend.client.set_user_quota.assert_not_called()

    def test_a_resource_without_a_backend_id_is_refused(self):
        backend = make_backend()

        with pytest.raises(CephS3Error, match="no backend id"):
            backend.update_resource_options(
                make_resource(backend_id=""), {"max_storage_limit": 7}
            )
        backend.client.set_user_quota.assert_not_called()

    def test_radosgw_gets_the_same_neutral_quota(self):
        backend = make_backend(RADOSGW_SETTINGS)
        resource = make_resource({"max_storage_limit": 5, "max_object_limit": 100})

        backend.update_resource_options(resource, {"max_object_limit": None})

        assert quotas(backend) == {"enabled": True, "max_size_bytes": 5 * GB, "max_objects": -1}
