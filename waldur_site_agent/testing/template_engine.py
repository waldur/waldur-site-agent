"""Template engine for generating test orders from templates."""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound, meta
from pydantic import BaseModel
from waldur_api_client.models.order_details import OrderDetails

from waldur_site_agent.backend import logger


class TemplateValidationError(Exception):
    """Raised when template validation fails."""


class TemplateRenderError(Exception):
    """Raised when template rendering fails."""


class ValidationResult(BaseModel):
    """Result of template validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]


class OrderTemplateEngine:
    """Engine for rendering order templates with variable substitution."""

    def __init__(self, template_dir: Path | str) -> None:
        """Initialize the template engine.

        Args:
            template_dir: Directory containing template files
        """
        self.template_dir = Path(template_dir)
        self.template_dir.mkdir(parents=True, exist_ok=True)

        # Set up Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
            autoescape=False,  # Disable for JSON template compatibility  # noqa: S701
            undefined=StrictUndefined,  # Fail on undefined variables
        )

        # Add custom filters
        self.jinja_env.filters.update(
            {
                "uuid4": lambda _: str(uuid_module.uuid4()),
                "timestamp": lambda _: datetime.now().isoformat(),
                "from_json": json.loads,
                "to_json": json.dumps,
            }
        )

    def render_template(self, template_name: str, **variables: Any) -> OrderDetails:  # noqa: ANN401
        """Render template with variables into OrderDetails object.

        Args:
            template_name: Name of the template file
            **variables: Variables to substitute in the template

        Returns:
            OrderDetails object created from the rendered template

        Raises:
            TemplateNotFound: If the template file doesn't exist
            TemplateRenderError: If template rendering fails
            TemplateValidationError: If the rendered data doesn't match OrderDetails schema
        """
        try:
            template = self.jinja_env.get_template(template_name)
        except TemplateNotFound as e:
            raise TemplateNotFound(
                f"Template '{template_name}' not found in {self.template_dir}"
            ) from e
        except Exception as e:
            raise TemplateRenderError(f"Failed to load template '{template_name}': {e}") from e

        try:
            rendered = template.render(**variables)
            logger.debug("Rendered template %s: %s", template_name, rendered)
        except Exception as e:
            raise TemplateRenderError(f"Failed to render template '{template_name}': {e}") from e

        try:
            order_data = json.loads(rendered)
        except json.JSONDecodeError as e:
            raise TemplateRenderError(
                f"Template '{template_name}' produced invalid JSON: {e}"
            ) from e

        try:
            return OrderDetails.from_dict(order_data)
        except Exception as e:
            raise TemplateValidationError(
                f"Template '{template_name}' produced invalid order data: {e}"
            ) from e

    def render_template_to_dict(self, template_name: str, **variables: Any) -> dict[str, Any]:  # noqa: ANN401
        """Render template and return as dictionary without validation.

        Args:
            template_name: Name of the template file
            **variables: Variables to substitute in the template

        Returns:
            Dictionary containing the rendered template data

        Raises:
            TemplateNotFound: If the template file doesn't exist
            TemplateRenderError: If template rendering fails
        """
        try:
            template = self.jinja_env.get_template(template_name)
        except TemplateNotFound as e:
            raise TemplateNotFound(
                f"Template '{template_name}' not found in {self.template_dir}"
            ) from e

        try:
            rendered = template.render(**variables)
            return json.loads(rendered)
        except Exception as e:
            raise TemplateRenderError(f"Failed to render template '{template_name}': {e}") from e

    def list_templates(self) -> list[str]:
        """List all available template files.

        Returns:
            List of template file names relative to template directory
        """
        templates = []
        for path in self.template_dir.rglob("*.json"):
            relative_path = path.relative_to(self.template_dir)
            templates.append(str(relative_path))

        return sorted(templates)

    def validate_template(self, template_name: str, **variables: Any) -> ValidationResult:  # noqa: ANN401
        """Validate template syntax and structure.

        Args:
            template_name: Name of the template file
            **variables: Sample variables to test template rendering

        Returns:
            ValidationResult with validation status and any errors/warnings
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Check if template file exists
        template_path = self.template_dir / template_name
        if not template_path.exists():
            errors.append(f"Template file '{template_name}' does not exist")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Try to load the template
        try:
            template = self.jinja_env.get_template(template_name)
        except Exception as e:
            errors.append(f"Failed to load template: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Try to render with sample variables
        try:
            rendered = template.render(**variables)
        except Exception as e:
            errors.append(f"Failed to render template: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Try to parse as JSON
        try:
            order_data = json.loads(rendered)
        except json.JSONDecodeError as e:
            errors.append(f"Template produces invalid JSON: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        # Try to validate against OrderDetails schema
        try:
            OrderDetails.from_dict(order_data)
        except Exception as e:
            errors.append(f"Template produces invalid OrderDetails: {e}")

        # Check for common issues
        if "uuid" not in order_data:
            warnings.append("Order UUID not specified - will be auto-generated")

        if "type" not in order_data:
            errors.append("Order type not specified")

        # Validate UUID fields before OrderDetails validation
        uuid_fields = [
            "uuid",
            "offering_uuid",
            "marketplace_resource_uuid",
            "project_uuid",
            "customer_uuid",
        ]
        for field in uuid_fields:
            if order_data.get(field):
                uuid_value = str(order_data[field])
                try:
                    uuid_module.UUID(uuid_value)
                except ValueError:
                    errors.append(f"Invalid UUID format for field '{field}': {uuid_value}")

        # Check required fields for different order types
        order_type = order_data.get("type")
        if order_type == "Create":
            if "resource_name" not in order_data:
                warnings.append("Resource name not specified for CREATE order")
            if "marketplace_resource_uuid" not in order_data:
                warnings.append("Marketplace resource UUID not specified for CREATE order")
            if "offering_uuid" not in order_data:
                errors.append("Offering UUID required for CREATE order")

        elif order_type == "Update":
            if "marketplace_resource_uuid" not in order_data:
                errors.append("Marketplace resource UUID required for UPDATE order")
            if "limits" not in order_data and "attributes" not in order_data:
                warnings.append("No limits or attributes specified for UPDATE order")

        elif order_type == "Terminate":
            if "marketplace_resource_uuid" not in order_data:
                errors.append("Marketplace resource UUID required for TERMINATE order")

        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    def get_template_variables(self, template_name: str) -> set[str]:
        """Extract variable names used in a template.

        Args:
            template_name: Name of the template file

        Returns:
            Set of variable names found in the template

        Raises:
            TemplateNotFound: If the template file doesn't exist
        """
        template_path = self.template_dir / template_name
        if not template_path.exists():
            raise TemplateNotFound(f"Template '{template_name}' not found in {self.template_dir}")

        # Read template source directly
        template_source = template_path.read_text()

        # Get undeclared variables (those not provided in render context)
        ast = self.jinja_env.parse(template_source)
        return jinja2_get_template_variables(ast)


def jinja2_get_template_variables(ast: Any) -> set[str]:  # noqa: ANN401
    """Extract variable names from a Jinja2 AST.

    Args:
        ast: Jinja2 AST node

    Returns:
        Set of variable names used in the template
    """
    return meta.find_undeclared_variables(ast)
