import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from waldur_api_client.models.user_me import UserMe

from waldur_site_agent.common.structures import Offering


def user_me_api_response(
    *,
    base_url: str = "https://waldur.example.com",
    user_uuid: Optional[UUID] = None,
    **overrides: Any,
) -> dict[str, Any]:
    """JSON body for GET /api/users/me/ that satisfies UserMe.from_dict."""
    uid = user_uuid or uuid.uuid4()
    payload: dict[str, Any] = {
        "url": f"{base_url}/api/users/{uid}/",
        "uuid": str(uid),
        "username": "test",
        "full_name": "Test User",
        "email": "test@example.com",
        "civil_number": None,
        "token": "token",
        "token_expires_at": None,
        "registration_method": "default",
        "date_joined": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        "agreement_date": None,
        "permissions": [],
        "requested_email": None,
        "affiliations": [],
        "identity_provider_name": "",
        "identity_provider_label": "",
        "identity_provider_management_url": "",
        "identity_provider_fields": [],
        "identity_source": "",
        "should_protect_user_details": False,
        "has_active_session": True,
        "has_usable_password": True,
        "ip_address": "127.0.0.1",
        "attribute_sources": {},
        "active_isds": [],
        "profile_completeness": {
            "is_complete": True,
            "missing_fields": [],
            "mandatory_fields": [],
            "enforcement_enabled": False,
        },
        "is_staff": False,
    }
    payload.update(overrides)
    UserMe.from_dict(payload)
    return payload

OFFERING = Offering(
    waldur_offering_uuid="d629d5e45567425da9cdbdc1af67b32c",
    name="example-test-00",
    waldur_api_url="https://waldur.example.com/api/",
    waldur_api_token="9e1132b9616ebfe943ddf632ca32bbb7e1109a32",
    backend_type="slurm",
    order_processing_backend="slurm",
    membership_sync_backend="slurm",
    reporting_backend="slurm",
    backend_settings={
        "default_account": "root",
        "customer_prefix": "hpc_",
        "project_prefix": "hpc_",
        "allocation_prefix": "hpc_",
        "enable_user_homedir_account_creation": True,
    },
    backend_components={
        "cpu": {
            "limit": 10,
            "measured_unit": "k-Hours",
            "unit_factor": 60000,
            "accounting_type": "limit",
            "label": "CPU",
        },
        "mem": {
            "limit": 10,
            "measured_unit": "gb-Hours",
            "unit_factor": 61440,
            "accounting_type": "usage",
            "label": "RAM",
        },
    },
)
