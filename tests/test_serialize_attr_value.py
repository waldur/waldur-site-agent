"""Tests for _serialize_attr_value used to build identity-bridge user attributes.

Regression coverage: an SDK bump turned some OfferingUser fields (affiliations,
nationalities, eduperson_assurance) into wrapped attrs models that are not
JSON-serializable. They must be unwrapped via .to_dict().
"""

import datetime
import json
from enum import Enum

from waldur_api_client.models.offering_user_user_affiliations import (
    OfferingUserUserAffiliations,
)

from waldur_site_agent.common.processors import _serialize_attr_value


def test_serializes_date_to_iso():
    assert _serialize_attr_value(datetime.date(1990, 1, 15)) == "1990-01-15"


def test_serializes_enum_to_value():
    class _Gender(Enum):
        MALE = 1

    assert _serialize_attr_value(_Gender.MALE) == 1


def test_passes_plain_value_through():
    assert _serialize_attr_value("alice@example.com") == "alice@example.com"


def test_unwraps_wrapped_attrs_model():
    affiliations = OfferingUserUserAffiliations.from_dict({"role": "student"})
    result = _serialize_attr_value(affiliations)
    assert result == {"role": "student"}
    # Must be JSON serializable (regression: SDK bump broke this).
    json.dumps(result)
