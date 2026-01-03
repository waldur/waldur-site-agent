"""Order testing harness for validating order processing without live systems."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock
from uuid import uuid4

from pydantic import BaseModel
from waldur_api_client.models.order_details import OrderDetails
from waldur_api_client.models.provider_offering_details import ProviderOfferingDetails
from waldur_api_client.models.service_provider import ServiceProvider

from waldur_site_agent.backend import logger
from waldur_site_agent.common import structures, utils
from waldur_site_agent.testing.mock_backend import MockBackend
from waldur_site_agent.testing.template_engine import OrderTemplateEngine


class TestResult(BaseModel):
    """Result of an order test execution."""

    success: bool
    order_uuid: str
    order_type: str
    resource_name: Optional[str] = None
    backend_operations: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    execution_time_seconds: Optional[float] = None


class OrderTestHarness:
    """Harness for testing order processing with templates and mock backends."""

    def __init__(
        self,
        config_path: str | Path,
        template_dir: Optional[str | Path] = None,
        use_mock_backend: bool = True,
    ) -> None:
        """Initialize the order test harness.

        Args:
            config_path: Path to the agent configuration file
            template_dir: Directory containing order templates (optional)
            use_mock_backend: Whether to use mock backend instead of real backend
        """
        self.config_path = Path(config_path)
        self.use_mock_backend = use_mock_backend

        # Load configuration
        self.configuration = utils.init_configuration_from_file(str(self.config_path))

        # Set up template engine
        if template_dir is None:
            template_dir = Path(__file__).parent / "templates"
        self.template_engine = OrderTemplateEngine(template_dir)

        # Mock backend instance for testing
        self._mock_backend: Optional[MockBackend] = None

    def _get_test_backend(self, offering: structures.Offering) -> MockBackend | Any:  # noqa: ANN401
        """Get backend for testing - either mock or real backend.

        Args:
            offering: The offering configuration

        Returns:
            Backend instance for testing
        """
        if self.use_mock_backend:
            if self._mock_backend is None:
                self._mock_backend = MockBackend()
            return self._mock_backend

        # Use real backend for integration testing
        backend, _ = utils.get_backend_for_offering(offering, "order_processing_backend")
        return backend

    def _create_mock_waldur_client(self) -> MagicMock:
        """Create a mock Waldur REST client for testing."""
        mock_client = MagicMock()

        # Mock common API responses
        mock_client.timeout = 30.0
        mock_client.base_url = "https://mock.waldur.com/api/"

        # Mock offering retrieval
        ProviderOfferingDetails(
            uuid=uuid.uuid4(), name="Mock Offering", customer_uuid=uuid.uuid4(), components=[]
        )

        # Mock service provider
        ServiceProvider(uuid=uuid.uuid4(), name="Mock Provider")

        return mock_client

    def test_order_from_file(self, order_file: str | Path, **kwargs: Any) -> TestResult:  # noqa: ANN401
        """Test order processing from a JSON file.

        Args:
            order_file: Path to JSON file containing order data
            **kwargs: Additional arguments for order processing

        Returns:
            TestResult with execution details and results
        """
        order_path = Path(order_file)
        if not order_path.exists():
            return TestResult(
                success=False,
                order_uuid="",
                order_type="",
                errors=[f"Order file not found: {order_file}"],
            )

        try:
            with order_path.open() as f:
                order_data = json.load(f)
        except Exception as e:
            return TestResult(
                success=False,
                order_uuid="",
                order_type="",
                errors=[f"Failed to load order file: {e}"],
            )

        try:
            order = OrderDetails.from_dict(order_data)
        except Exception as e:
            return TestResult(
                success=False,
                order_uuid=str(order_data.get("uuid", "")),
                order_type=str(order_data.get("type", "")),
                errors=[f"Invalid order data: {e}"],
            )

        return self._test_order(order, **kwargs)

    def test_order_from_template(
        self,
        template_name: str,
        offering_uuid: Optional[str] = None,
        **template_vars: Any,  # noqa: ANN401
    ) -> TestResult:
        """Test order processing from a template.

        Args:
            template_name: Name of the template file to use
            offering_uuid: UUID of the offering to use (uses first offering if not specified)
            **template_vars: Variables to substitute in the template

        Returns:
            TestResult with execution details and results
        """
        # Get offering configuration
        if offering_uuid:
            offering = None
            for off in self.configuration.offerings:
                if off.uuid == offering_uuid:
                    offering = off
                    break
            if offering is None:
                return TestResult(
                    success=False,
                    order_uuid="",
                    order_type="",
                    errors=[f"Offering with UUID {offering_uuid} not found in configuration"],
                )
        else:
            if not self.configuration.offerings:
                return TestResult(
                    success=False,
                    order_uuid="",
                    order_type="",
                    errors=["No offerings found in configuration"],
                )
            offering = self.configuration.offerings[0]

        # Add offering context to template variables
        template_vars.setdefault("offering_uuid", offering.uuid)
        template_vars.setdefault("offering_name", offering.name)

        # Render template
        try:
            order = self.template_engine.render_template(template_name, **template_vars)
        except Exception as e:
            return TestResult(
                success=False,
                order_uuid="",
                order_type="",
                errors=[f"Failed to render template '{template_name}': {e}"],
            )

        return self._test_order(order, offering=offering)

    def _test_order(
        self, order: OrderDetails, offering: Optional[structures.Offering] = None
    ) -> TestResult:
        """Execute order processing test.

        Args:
            order: OrderDetails object to process
            offering: Offering configuration (uses first if not specified)

        Returns:
            TestResult with execution details and results
        """
        start_time = time.time()

        # Get offering if not provided
        if offering is None:
            if not self.configuration.offerings:
                return TestResult(
                    success=False,
                    order_uuid=str(order.uuid or ""),
                    order_type=str(order.type_ or ""),
                    errors=["No offerings found in configuration"],
                )
            offering = self.configuration.offerings[0]

        # Get test backend
        try:
            test_backend = self._get_test_backend(offering)
        except Exception as e:
            return TestResult(
                success=False,
                order_uuid=str(order.uuid or ""),
                order_type=str(order.type_ or ""),
                errors=[f"Failed to initialize backend: {e}"],
            )

        # Skip complex processor setup for now and do basic validation
        # TODO: Implement full processor integration when needed

        # Execute simplified order processing validation
        errors = []
        warnings = []
        backend_operations = []

        try:
            logger.info("Validating order %s (type: %s)", order.uuid, order.type_)

            # Ensure order has a UUID
            if not order.uuid:
                order.uuid = uuid4()

            # Check required fields based on order type
            if order.type_ and hasattr(order.type_, "value"):
                order_type = order.type_.value
            else:
                order_type = str(order.type_)

            if order_type == "Create":
                if not order.resource_name:
                    warnings.append("No resource name specified for CREATE order")
                if not order.marketplace_resource_uuid:
                    warnings.append("No marketplace resource UUID specified for CREATE order")

            elif order_type == "Update":
                if not order.marketplace_resource_uuid:
                    errors.append("Marketplace resource UUID required for UPDATE order")

            elif order_type == "Terminate":
                if not order.marketplace_resource_uuid:
                    errors.append("Marketplace resource UUID required for TERMINATE order")

            # For mock backend, simulate basic validation
            if isinstance(test_backend, MockBackend):
                test_backend.ping()  # Log a basic operation
                backend_operations = test_backend.get_operations_log()
                logger.info("Mock backend validation completed successfully")

            success = len(errors) == 0

        except Exception as e:
            logger.exception("Order validation failed: %s", e)
            errors.append(f"Order validation failed: {e}")
            success = False

        execution_time = time.time() - start_time

        return TestResult(
            success=success,
            order_uuid=str(order.uuid or ""),
            order_type=str(order.type_ or ""),
            resource_name=order.resource_name,
            backend_operations=backend_operations,
            errors=errors,
            warnings=warnings,
            execution_time_seconds=execution_time,
        )

    def validate_order_structure(self, order_data: dict[str, Any]) -> TestResult:
        """Validate order structure without execution.

        Args:
            order_data: Dictionary containing order data

        Returns:
            TestResult with validation results
        """
        errors = []
        warnings = []

        try:
            order = OrderDetails.from_dict(order_data)
        except Exception as e:
            return TestResult(
                success=False,
                order_uuid=str(order_data.get("uuid", "")),
                order_type=str(order_data.get("type", "")),
                errors=[f"Invalid order structure: {e}"],
            )

        # Perform additional validation
        if not order.uuid:
            warnings.append("Order UUID not specified - will be auto-generated")

        if not order.type_:
            errors.append("Order type not specified")

        # Type-specific validation
        order_type_value = str(order.type_).upper() if order.type_ else ""

        if order_type_value == "CREATE":
            if not order.resource_name:
                warnings.append("Resource name not specified for CREATE order")

        elif order_type_value == "UPDATE":
            if not order.marketplace_resource_uuid:
                errors.append("Marketplace resource UUID required for UPDATE order")

        elif order_type_value == "TERMINATE" and not order.marketplace_resource_uuid:
            errors.append("Marketplace resource UUID required for TERMINATE order")

        return TestResult(
            success=len(errors) == 0,
            order_uuid=str(order.uuid or ""),
            order_type=str(order.type_ or ""),
            resource_name=str(order.resource_name) if order.resource_name else None,
            errors=errors,
            warnings=warnings,
        )

    def list_templates(self) -> list[str]:
        """List all available order templates.

        Returns:
            List of template names
        """
        return self.template_engine.list_templates()

    def validate_template(self, template_name: str, **sample_vars: Any) -> TestResult:  # noqa: ANN401
        """Validate a template without execution.

        Args:
            template_name: Name of the template to validate
            **sample_vars: Sample variables for testing template rendering

        Returns:
            TestResult with validation results
        """
        validation_result = self.template_engine.validate_template(template_name, **sample_vars)

        return TestResult(
            success=validation_result.is_valid,
            order_uuid="",
            order_type="",
            errors=validation_result.errors,
            warnings=validation_result.warnings,
        )
