"""Command-line interface for order testing functionality."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from waldur_site_agent.backend import logger
from waldur_site_agent.testing.order_test_harness import OrderTestHarness, TestResult
from waldur_site_agent.testing.template_engine import OrderTemplateEngine


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for order testing CLI."""
    parser = argparse.ArgumentParser(
        description="Test Waldur Site Agent order processing with templates and mock backends",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available templates
  waldur_site_test_order --list-templates

  # Validate template (use proper UUID format)
  waldur_site_test_order --template create/basic.json --validate-only \\
    --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c

  # Generate order from template (dry run)
  waldur_site_test_order --template create/with-limits.json --generate-only \\
    --var offering_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \\
    --var cpu_limit=2000 -o output.json

  # Test with config file and mock backend
  waldur_site_test_order -c config.yaml --template create/basic.json --dry-run \\
    --var resource_name=test-allocation

  # Test UPDATE order
  waldur_site_test_order -c config.yaml --template update/limits-only.json \\
    --var marketplace_resource_uuid=d629d5e4-5567-425d-a9cd-bdc1af67b32c \\
    --var resource_name=test-allocation --var new_cpu_limit=4000

  # Test from JSON file
  waldur_site_test_order -c config.yaml --order-file my_order.json
        """,
    )

    # Configuration
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Path to agent configuration file",
    )

    parser.add_argument(
        "--template-dir",
        type=str,
        help="Directory containing order templates (default: built-in templates)",
    )

    # Order sources
    order_group = parser.add_mutually_exclusive_group()
    order_group.add_argument(
        "--template",
        type=str,
        help="Template file to use for order generation",
    )

    order_group.add_argument(
        "--order-file",
        type=str,
        help="JSON file containing order data",
    )

    # Template variables
    parser.add_argument(
        "--var",
        action="append",
        dest="variables",
        help="Template variable in format key=value (can be used multiple times)",
    )

    parser.add_argument(
        "--offering-uuid",
        type=str,
        help="UUID of the offering to use (uses first offering if not specified)",
    )

    # Execution modes
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate order/template structure without execution",
    )

    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Generate order from template without execution",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run with mock backend",
    )

    parser.add_argument(
        "--use-real-backend",
        action="store_true",
        help="Use real backend instead of mock backend (be careful!)",
    )

    # Output options
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Output file for generated orders (JSON format)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output results in JSON format",
    )

    # Information commands
    parser.add_argument(
        "--list-templates",
        action="store_true",
        help="List available templates and exit",
    )

    return parser


def print_test_result(result: TestResult, verbose: bool = False, json_output: bool = False) -> None:
    """Print test result in requested format.

    Args:
        result: TestResult object
        verbose: Whether to include verbose output
        json_output: Whether to output in JSON format
    """
    if json_output:
        sys.stdout.write(json.dumps(result.model_dump(), indent=2) + "\n")
        return

    # Human-readable output
    status = "SUCCESS" if result.success else "FAILED"
    sys.stdout.write(f"Test Result: {status}\n")
    sys.stdout.write(f"Order UUID: {result.order_uuid}\n")
    sys.stdout.write(f"Order Type: {result.order_type}\n")

    if result.resource_name:
        sys.stdout.write(f"Resource Name: {result.resource_name}\n")

    if result.execution_time_seconds is not None:
        sys.stdout.write(f"Execution Time: {result.execution_time_seconds:.2f}s\n")

    if result.warnings:
        sys.stdout.write("\nWarnings:\n")
        for warning in result.warnings:
            sys.stdout.write(f"  - {warning}\n")

    if result.errors:
        sys.stdout.write("\nErrors:\n")
        for error in result.errors:
            sys.stdout.write(f"  - {error}\n")

    if verbose and result.backend_operations:
        sys.stdout.write("\nBackend Operations:\n")
        for i, op in enumerate(result.backend_operations, 1):
            sys.stdout.write(f"  {i}. {op}\n")


def main() -> None:
    """Main entry point for order testing CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logger.setLevel("DEBUG")
    else:
        logger.setLevel("INFO")

    # Handle template listing
    if args.list_templates:
        if args.template_dir:
            template_dir = Path(args.template_dir)
        else:
            # Use built-in templates
            template_dir = Path(__file__).parent / "templates"

        # For template listing, we don't need a valid config
        engine = OrderTemplateEngine(template_dir)

        templates = engine.list_templates()
        if not templates:
            sys.stdout.write("No templates found\n")
            return

        sys.stdout.write("Available templates:\n")
        for template in templates:
            sys.stdout.write(f"  - {template}\n")
        return

    # Handle template operations without config
    if args.template and (args.validate_only or args.generate_only):
        if args.template_dir:
            template_dir = Path(args.template_dir)
        else:
            template_dir = Path(__file__).parent / "templates"

        engine = OrderTemplateEngine(template_dir)

        try:
            variables = parse_template_variables(args.variables)
        except ValueError as e:
            sys.stderr.write(f"Error: {e}\n")
            sys.exit(1)

        if args.generate_only:
            # Generate order from template
            try:
                order_data = engine.render_template_to_dict(args.template, **variables)

                if args.output:
                    output_path = Path(args.output)
                    with output_path.open("w") as f:
                        json.dump(order_data, f, indent=2, default=str)
                    sys.stdout.write(f"Generated order saved to: {args.output}\n")
                else:
                    sys.stdout.write(json.dumps(order_data, indent=2, default=str) + "\n")

            except Exception as e:
                sys.stderr.write(f"Error: Failed to generate order from template: {e}\n")
                sys.exit(1)
            return

        if args.validate_only:
            validation_result = engine.validate_template(args.template, **variables)

            # Create a mock TestResult for consistency
            validation_test_result = TestResult(
                success=validation_result.is_valid,
                order_uuid="",
                order_type="",
                errors=validation_result.errors,
                warnings=validation_result.warnings,
            )

            print_test_result(
                validation_test_result, verbose=args.verbose, json_output=args.json_output
            )
            if not validation_test_result.success:
                sys.exit(1)
            return

    # Require config for other operations
    if not args.config:
        sys.stderr.write("Error: Configuration file is required (use -c/--config)\n")
        sys.exit(1)

    config_path = Path(args.config)
    if not config_path.exists():
        sys.stderr.write(f"Error: Configuration file not found: {args.config}\n")
        sys.exit(1)

    # Initialize test harness
    try:
        use_mock = not args.use_real_backend
        harness = OrderTestHarness(
            config_path=args.config,
            template_dir=args.template_dir,
            use_mock_backend=use_mock,
        )
    except Exception as e:
        sys.stderr.write(f"Error: Failed to initialize test harness: {e}\n")
        sys.exit(1)

    # Parse template variables
    try:
        variables = parse_template_variables(args.variables)
    except ValueError as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

    # Execute based on mode
    result: TestResult | None = None

    if args.template:
        # Template-based testing
        if args.generate_only:
            # Generate order from template
            try:
                order_data = harness.template_engine.render_template_to_dict(
                    args.template, **variables
                )

                if args.output:
                    output_path = Path(args.output)
                    with output_path.open("w") as f:
                        json.dump(order_data, f, indent=2, default=str)
                    sys.stdout.write(f"Generated order saved to: {args.output}\n")
                else:
                    sys.stdout.write(json.dumps(order_data, indent=2, default=str) + "\n")

            except Exception as e:
                sys.stderr.write(f"Error: Failed to generate order from template: {e}\n")
                sys.exit(1)
            return

        if args.validate_only:
            # Validate template
            result = harness.validate_template(args.template, **variables)

        else:
            # Execute order from template
            result = harness.test_order_from_template(
                template_name=args.template,
                offering_uuid=args.offering_uuid,
                **variables,
            )

    elif args.order_file:
        # File-based testing
        if args.validate_only:
            # Validate order file structure
            order_path = Path(args.order_file)
            if not order_path.exists():
                sys.stderr.write(f"Error: Order file not found: {args.order_file}\n")
                sys.exit(1)

            try:
                with order_path.open() as f:
                    order_data = json.load(f)
            except Exception as e:
                sys.stderr.write(f"Error: Failed to load order file: {e}\n")
                sys.exit(1)

            result = harness.validate_order_structure(order_data)

        else:
            # Execute order from file
            result = harness.test_order_from_file(args.order_file)

    else:
        sys.stderr.write("Error: Must specify either --template or --order-file\n")
        sys.exit(1)

    # Print results
    if result:
        print_test_result(result, verbose=args.verbose, json_output=args.json_output)

        # Exit with error code if test failed
        if not result.success:
            sys.exit(1)


def parse_template_variables(variables_list: list[str] | None) -> dict[str, Any]:
    """Parse template variables from command line format.

    Args:
        variables_list: List of variables in format ["key=value", ...]

    Returns:
        Dictionary of parsed variables

    Raises:
        ValueError: If variable format is invalid
    """
    variables: dict[str, Any] = {}

    if not variables_list:
        return variables

    for var_string in variables_list:
        if "=" not in var_string:
            raise ValueError(f"Invalid variable format '{var_string}'. Expected format: key=value")

        key, value = var_string.split("=", 1)

        # Try to parse as JSON first, fallback to string
        try:
            # Check if it looks like JSON (starts with { or [ or is quoted)
            value_stripped = value.strip()
            if (
                value_stripped.startswith(("{", "[", '"'))
                or value_stripped in ("true", "false", "null")
                or value_stripped.replace(".", "").replace("-", "").isdigit()
            ):
                variables[key] = json.loads(value)
            else:
                variables[key] = value
        except json.JSONDecodeError:
            # Not valid JSON, treat as string
            variables[key] = value

    return variables


if __name__ == "__main__":
    main()
