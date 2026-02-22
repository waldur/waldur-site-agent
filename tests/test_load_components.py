"""Tests for load_components_to_waldur and _build_component_kwargs."""

from unittest.mock import MagicMock, patch
from uuid import UUID

from waldur_api_client.models.billing_type_enum import BillingTypeEnum
from waldur_api_client.models.limit_period_enum import LimitPeriodEnum
from waldur_api_client.models.offering_component import OfferingComponent
from waldur_api_client.models.offering_component_request import OfferingComponentRequest
from waldur_api_client.models.update_offering_component_request import (
    UpdateOfferingComponentRequest,
)

from waldur_site_agent.common.structures import AccountingType, BackendComponent
from waldur_site_agent.common.utils import _build_component_kwargs, load_components_to_waldur


class TestBuildComponentKwargs:
    """Tests for _build_component_kwargs helper."""

    def test_extracts_known_optional_fields(self):
        component_info = {
            "measured_unit": "GB",
            "accounting_type": "usage",
            "label": "Storage",
            "description": "Block storage",
            "min_value": 0,
            "max_value": 1000,
            "max_available_limit": 500,
            "default_limit": 100,
            "article_code": "STOR-001",
            "is_boolean": False,
            "is_prepaid": True,
        }
        result = _build_component_kwargs(component_info)
        assert result == {
            "description": "Block storage",
            "min_value": 0,
            "max_value": 1000,
            "max_available_limit": 500,
            "default_limit": 100,
            "article_code": "STOR-001",
            "is_boolean": False,
            "is_prepaid": True,
        }

    def test_converts_limit_period_to_enum(self):
        component_info = {"limit_period": "month"}
        result = _build_component_kwargs(component_info)
        assert result == {"limit_period": LimitPeriodEnum.MONTH}

    def test_ignores_none_values(self):
        component_info = {
            "description": None,
            "min_value": None,
            "max_value": 100,
        }
        result = _build_component_kwargs(component_info)
        assert result == {"max_value": 100}

    def test_ignores_non_api_fields(self):
        component_info = {
            "measured_unit": "Hours",
            "unit_factor": 1.0,
            "accounting_type": "usage",
            "label": "CPU",
            "limit": 1000,
        }
        result = _build_component_kwargs(component_info)
        assert result == {}

    def test_empty_dict_returns_empty(self):
        result = _build_component_kwargs({})
        assert result == {}

    def test_zero_values_are_preserved(self):
        component_info = {"min_value": 0, "default_limit": 0}
        result = _build_component_kwargs(component_info)
        assert result == {"min_value": 0, "default_limit": 0}


def _make_offering_mock(components: list[OfferingComponent]) -> MagicMock:
    """Create a mock Waldur offering with the given components."""
    offering = MagicMock()
    offering.components = components
    return offering


def _make_existing_component(type_: str, uuid_hex: str = "aabbccdd11223344aabbccdd11223344"):
    """Create a mock OfferingComponent for an existing remote component."""
    comp = MagicMock(spec=OfferingComponent)
    comp.type_ = type_
    comp.uuid = UUID(uuid_hex)
    comp.limit_amount = 500
    return comp


class TestLoadComponentsToWaldurCreate:
    """Tests for creating new components via load_components_to_waldur."""

    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_create_offering_component"
    )
    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_retrieve"
    )
    def test_creates_component_with_extra_fields(self, mock_retrieve, mock_create):
        mock_retrieve.sync.return_value = _make_offering_mock([])

        components = {
            "cpu": BackendComponent(
                measured_unit="Hours",
                accounting_type=AccountingType.USAGE,
                label="CPU",
                min_value=1,
                max_value=1000,
                description="CPU time",
                article_code="CPU-001",
            ),
        }

        load_components_to_waldur(
            MagicMock(), "offering-uuid", "Test Offering", components
        )

        mock_create.sync_detailed.assert_called_once()
        body = mock_create.sync_detailed.call_args.kwargs["body"]
        assert isinstance(body, OfferingComponentRequest)
        assert body.min_value == 1
        assert body.max_value == 1000
        assert body.description == "CPU time"
        assert body.article_code == "CPU-001"

    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_create_offering_component"
    )
    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_retrieve"
    )
    def test_creates_component_without_extra_fields(self, mock_retrieve, mock_create):
        """Backward compatibility: configs without extra fields still work."""
        mock_retrieve.sync.return_value = _make_offering_mock([])

        components = {
            "ram": BackendComponent(
                measured_unit="GB",
                accounting_type=AccountingType.LIMIT,
                label="RAM",
            ),
        }

        load_components_to_waldur(
            MagicMock(), "offering-uuid", "Test Offering", components
        )

        mock_create.sync_detailed.assert_called_once()
        body = mock_create.sync_detailed.call_args.kwargs["body"]
        assert isinstance(body, OfferingComponentRequest)
        assert body.billing_type == BillingTypeEnum("limit")
        assert body.name == "RAM"
        assert body.measured_unit == "GB"

    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_create_offering_component"
    )
    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_retrieve"
    )
    def test_creates_component_with_limit_period(self, mock_retrieve, mock_create):
        mock_retrieve.sync.return_value = _make_offering_mock([])

        components = {
            "storage": BackendComponent(
                measured_unit="GB",
                accounting_type=AccountingType.USAGE,
                label="Storage",
                limit_period="annual",
            ),
        }

        load_components_to_waldur(
            MagicMock(), "offering-uuid", "Test Offering", components
        )

        body = mock_create.sync_detailed.call_args.kwargs["body"]
        assert body.limit_period == LimitPeriodEnum.ANNUAL


class TestLoadComponentsToWaldurUpdate:
    """Tests for updating existing components via load_components_to_waldur."""

    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_update_offering_component"
    )
    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_retrieve"
    )
    def test_updates_usage_component_with_extra_fields(self, mock_retrieve, mock_update):
        existing = _make_existing_component("cpu")
        mock_retrieve.sync.return_value = _make_offering_mock([existing])

        components = {
            "cpu": BackendComponent(
                measured_unit="Hours",
                accounting_type=AccountingType.USAGE,
                label="CPU",
                limit=200,
                max_value=5000,
                is_prepaid=True,
                description="CPU hours",
            ),
        }

        load_components_to_waldur(
            MagicMock(), "offering-uuid", "Test Offering", components
        )

        mock_update.sync_detailed.assert_called_once()
        body = mock_update.sync_detailed.call_args.kwargs["body"]
        assert isinstance(body, UpdateOfferingComponentRequest)
        assert body.max_value == 5000
        assert body.is_prepaid is True
        assert body.description == "CPU hours"
        assert body.limit_amount == 200

    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_update_offering_component"
    )
    @patch(
        "waldur_site_agent.common.utils."
        "marketplace_provider_offerings_retrieve"
    )
    def test_skips_update_for_limit_type_components(self, mock_retrieve, mock_update):
        existing = _make_existing_component("ram")
        mock_retrieve.sync.return_value = _make_offering_mock([existing])

        components = {
            "ram": BackendComponent(
                measured_unit="GB",
                accounting_type=AccountingType.LIMIT,
                label="RAM",
                max_value=512,
            ),
        }

        load_components_to_waldur(
            MagicMock(), "offering-uuid", "Test Offering", components
        )

        mock_update.sync_detailed.assert_not_called()
