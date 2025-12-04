"""Historical usage loading command for SLURM backend.

This module provides a command-line interface for loading historical usage data
into Waldur from SLURM accounting records. The command requires staff user
authentication and processes usage data monthly to align with Waldur's billing model.
"""

import argparse
import datetime
import sys
from typing import Optional

from waldur_api_client import AuthenticatedClient
from waldur_api_client.api.marketplace_component_usages import (
    marketplace_component_usages_list,
    marketplace_component_usages_set_usage,
    marketplace_component_usages_set_user_usage,
)
from waldur_api_client.api.marketplace_offering_users import marketplace_offering_users_list
from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
)
from waldur_api_client.models import ComponentUsageCreateRequest, ComponentUserUsageCreateRequest
from waldur_api_client.models.component_usage_item_request import ComponentUsageItemRequest
from waldur_api_client.models.marketplace_offering_users_list_state_item import (
    MarketplaceOfferingUsersListStateItem,
)
from waldur_api_client.models.marketplace_provider_resources_list_state_item import (
    MarketplaceProviderResourcesListStateItem,
)
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import logger
from waldur_site_agent.backend import utils as backend_utils
from waldur_site_agent.common import utils
from waldur_site_agent.common.structures import Offering


def validate_staff_user(user_token: str, offering: Offering) -> None:
    """Validate that the provided token belongs to a staff user.

    Args:
        user_token: The API token to validate
        offering: Offering configuration for API connection

    Raises:
        SystemExit: If the user is not a staff user or validation fails
    """
    logger.info("Validating staff user credentials...")

    try:
        client = utils.get_client(offering.api_url, user_token, verify_ssl=offering.verify_ssl)

        current_user = utils.get_current_user_from_client(client)
        utils.print_current_user(current_user)

        if not current_user.is_staff:
            logger.error("❌ Historical usage loading requires staff user privileges")
            logger.error("Current user %s is not a staff user", current_user.username)
            sys.exit(1)

        logger.info("✓ Staff user validation successful for user: %s", current_user.username)

    except Exception as e:
        logger.error("❌ Failed to validate user credentials: %s", e)
        sys.exit(1)


def parse_date_range(start_date_str: str, end_date_str: str) -> tuple[datetime.date, datetime.date]:
    """Parse and validate start and end date strings.

    Args:
        start_date_str: Start date in YYYY-MM-DD format
        end_date_str: End date in YYYY-MM-DD format

    Returns:
        Tuple of (start_date, end_date) as datetime.date objects

    Raises:
        ValueError: If date format is invalid or end_date is before start_date
        SystemExit: If date range is unreasonably large
    """
    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error("❌ Invalid date format. Use YYYY-MM-DD format: %s", e)
        raise

    if end_date < start_date:
        msg = f"End date {end_date} cannot be before start date {start_date}"
        logger.error("❌ %s", msg)
        raise ValueError(msg)

    # Validate reasonable range (max 5 years)
    days_diff = (end_date - start_date).days
    max_days = 5 * 365  # 5 years

    if days_diff > max_days:
        logger.error(
            "❌ Date range too large (%d days). Maximum allowed is %d days (5 years)",
            days_diff,
            max_days,
        )
        sys.exit(1)

    logger.info("✓ Date range validated: %s to %s (%d days)", start_date, end_date, days_diff)
    return start_date, end_date


def find_offering_by_uuid(offerings: list[Offering], target_uuid: str) -> Optional[Offering]:
    """Find offering by UUID.

    Args:
        offerings: List of available offerings
        target_uuid: UUID to search for

    Returns:
        Offering object if found, None otherwise
    """
    return next((offering for offering in offerings if offering.uuid == target_uuid), None)


def load_historical_usage_for_month(
    offering: Offering, user_token: str, year: int, month: int, month_count: int, total_months: int
) -> None:
    """Load historical usage data for a specific month.

    Args:
        offering: Offering configuration
        user_token: Staff user API token
        year: Year to process
        month: Month to process (1-12)
        month_count: Current month number in sequence (for progress)
        total_months: Total number of months to process
    """
    logger.info(
        "📅 Processing month %d/%d: %04d-%02d for offering '%s' (%s)",
        month_count,
        total_months,
        year,
        month,
        offering.name,
        offering.uuid,
    )

    # Create API client with staff user token
    waldur_rest_client = utils.get_client(
        offering.api_url, user_token, verify_ssl=offering.verify_ssl
    )

    # Get backend for this offering - reuse existing backend creation logic
    resource_backend, _ = utils.get_backend_for_offering(offering, "reporting_backend")

    # Verify backend supports historical usage reporting
    if not hasattr(resource_backend, "get_historical_usage_report"):
        logger.error("❌ Backend does not support historical usage reporting")
        return

    # Get all resources for this offering from Waldur
    waldur_resources = marketplace_provider_resources_list.sync_all(
        client=waldur_rest_client,
        offering_uuid=[offering.uuid],
        state=[
            MarketplaceProviderResourcesListStateItem.OK,
            MarketplaceProviderResourcesListStateItem.ERRED,
        ],
    )

    # Filter resources that have backend IDs
    active_resources = [resource for resource in waldur_resources if resource.backend_id]

    if not active_resources:
        logger.info("No active resources found for offering, skipping month")
        return

    logger.info("📋 Found %d active resources to process", len(active_resources))

    # Get offering users for username mapping
    offering_users = marketplace_offering_users_list.sync_all(
        client=waldur_rest_client,
        offering_uuid=[offering.uuid],
        state=[MarketplaceOfferingUsersListStateItem.OK],
    )

    # Create username to offering user mapping
    username_to_offering_user = {user.username: user for user in offering_users}

    try:
        # Get resource backend IDs
        resource_backend_ids = [resource.backend_id for resource in active_resources]

        # Get historical usage data from backend - reuse existing method
        usage_report = resource_backend.get_historical_usage_report(
            resource_backend_ids, year, month
        )

        if not usage_report:
            logger.info("No usage data found for %04d-%02d", year, month)
            return

        logger.info(
            "📊 Processing usage data for %d accounts in offering '%s'",
            len(usage_report),
            offering.name,
        )

        # Create the usage date for the first day of the month
        usage_date = datetime.datetime(year=year, month=month, day=1)

        processed_resources = 0

        # Process each resource
        for resource in active_resources:
            backend_id = resource.backend_id

            logger.info(
                "🔍 Processing resource '%s' (backend_id: %s, uuid: %s)",
                resource.name,
                backend_id,
                resource.uuid.hex,
            )

            if backend_id not in usage_report:
                logger.warning(
                    "⚠️  No usage data for resource '%s' (%s) in offering '%s'",
                    resource.name,
                    backend_id,
                    offering.name,
                )
                continue

            account_usage = usage_report[backend_id]
            total_usage = account_usage.get("TOTAL_ACCOUNT_USAGE", {})

            if not total_usage:
                logger.warning(
                    "⚠️  No total usage for resource '%s' in offering '%s'",
                    resource.name,
                    offering.name,
                )
                continue

            # Submit resource-level usage
            _submit_resource_usage(waldur_rest_client, resource, total_usage, usage_date, offering)

            # Submit per-user usage
            user_count = 0
            for username, user_usage in account_usage.items():
                if username == "TOTAL_ACCOUNT_USAGE":
                    continue

                _submit_user_usage(
                    waldur_rest_client,
                    resource,
                    username,
                    user_usage,
                    username_to_offering_user,
                    usage_date,
                    offering,
                )
                user_count += 1

            logger.info(
                "📋 Submitted usage for resource '%s': %d users processed",
                resource.name,
                user_count,
            )
            processed_resources += 1

        logger.info(
            "✅ Completed processing %04d-%02d for offering '%s' (%d resources)",
            year,
            month,
            offering.name,
            processed_resources,
        )

    except Exception as e:
        logger.error("❌ Failed to process %04d-%02d: %s", year, month, e)
        raise


def _submit_resource_usage(
    waldur_rest_client: AuthenticatedClient,
    resource: WaldurResource,
    total_usage: dict[str, int],
    usage_date: datetime.datetime,
    offering: Offering,
) -> None:
    """Submit resource-level usage data to Waldur."""
    usage_objects = [
        ComponentUsageItemRequest(type_=component, amount=str(amount))
        for component, amount in total_usage.items()
        if amount > 0
    ]

    if not usage_objects:
        logger.info(
            "No non-zero usage for resource '%s' in offering '%s'", resource.name, offering.name
        )
        return

    request_body = ComponentUsageCreateRequest(
        usages=usage_objects,
        resource=resource.uuid,
        date=usage_date,  # Include historical date
    )

    try:
        marketplace_component_usages_set_usage.sync_detailed(
            client=waldur_rest_client, body=request_body
        )
        logger.info(
            "📤 Submitted resource usage for '%s' (uuid: %s) in offering '%s' (%s): %s",
            resource.name,
            resource.uuid.hex,
            offering.name,
            offering.uuid,
            total_usage,
        )
    except Exception as e:
        logger.error(
            "❌ Failed to submit resource usage for '%s' in offering '%s': %s",
            resource.name,
            offering.name,
            e,
        )
        raise


def _submit_user_usage(
    waldur_rest_client: AuthenticatedClient,
    resource: WaldurResource,
    username: str,
    user_usage: dict[str, int],
    username_to_offering_user: dict[str, OfferingUser],
    usage_date: datetime.datetime,
    offering: Offering,
) -> None:
    """Submit per-user usage data to Waldur."""
    logger.info(
        "👤 Processing user '%s' for resource '%s' in offering '%s'",
        username,
        resource.name,
        offering.name,
    )

    # Get component usages for this resource
    component_usages = marketplace_component_usages_list.sync_all(
        client=waldur_rest_client,
        resource_uuid=[resource.uuid.hex],
    )

    # Create component usage mapping
    component_usage_map = {usage.type_: usage for usage in component_usages}

    submitted_components = 0

    for component_type, amount in user_usage.items():
        if amount <= 0:
            continue

        component_usage = component_usage_map.get(component_type)
        if not component_usage:
            logger.warning(
                "⚠️  No component usage found for '%s' in resource '%s' (offering: '%s')",
                component_type,
                resource.name,
                offering.name,
            )
            continue

        # Create user usage request
        body = ComponentUserUsageCreateRequest(
            username=username,
            usage=str(amount),
            date=usage_date,  # Include historical date
        )

        # Add offering user URL if available
        offering_user = username_to_offering_user.get(username)
        if offering_user:
            body.user = offering_user.url
            logger.debug(
                "🔗 Linked user '%s' to offering user URL: %s", username, offering_user.url
            )
        else:
            logger.warning(
                "⚠️  No offering user found for username '%s' in offering '%s'",
                username,
                offering.name,
            )

        try:
            marketplace_component_usages_set_user_usage.sync_detailed(
                uuid=component_usage.uuid, client=waldur_rest_client, body=body
            )
            logger.info(
                "📤 Submitted user usage: '%s'/'%s' in resource '%s' (offering: '%s') - %s=%s",
                username,
                component_type,
                resource.name,
                offering.name,
                component_type,
                amount,
            )
            submitted_components += 1
        except Exception as e:
            logger.error(
                "❌ Failed to submit user usage for '%s'/'%s' in '%s' (%s): %s",
                username,
                component_type,
                resource.name,
                offering.name,
                e,
            )
            # Don't raise here - continue with other users

    if submitted_components > 0:
        logger.info(
            "✅ Completed user '%s' in resource '%s': %d components submitted",
            username,
            resource.name,
            submitted_components,
        )
    else:
        logger.warning(
            "⚠️  No usage components submitted for user '%s' in resource '%s'",
            username,
            resource.name,
        )


def main() -> None:
    """Main entry point for historical usage loading command."""
    parser = argparse.ArgumentParser(
        description="Load historical usage data from SLURM to Waldur (Staff only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load usage for entire year 2024
  waldur_site_load_historical_usage \\
    --config /etc/waldur/waldur-site-agent-config.yaml \\
    --offering-uuid 12345678-1234-1234-1234-123456789abc \\
    --user-token your-staff-token \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31

  # Load usage for specific quarter
  waldur_site_load_historical_usage \\
    --config /etc/waldur/waldur-site-agent-config.yaml \\
    --offering-uuid 12345678-1234-1234-1234-123456789abc \\
    --user-token your-staff-token \\
    --start-date 2024-01-01 \\
    --end-date 2024-03-31
        """,
    )

    parser.add_argument(
        "--config", "-c", required=True, help="Path to waldur-site-agent configuration file"
    )

    parser.add_argument(
        "--offering-uuid", "-o", required=True, help="UUID of the Waldur offering to load data for"
    )

    parser.add_argument(
        "--user-token",
        "-t",
        required=True,
        help="Waldur API token for staff user (required for historical data submission)",
    )

    parser.add_argument("--start-date", "-s", required=True, help="Start date in YYYY-MM-DD format")

    parser.add_argument("--end-date", "-e", required=True, help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    logger.info("🚀 Starting historical usage loading")
    logger.info("Configuration file: %s", args.config)
    logger.info("Offering UUID: %s", args.offering_uuid)
    logger.info("Date range: %s to %s", args.start_date, args.end_date)

    try:
        # Load configuration
        configuration = utils.load_configuration(args.config, "historical-usage-loader")

        # Find the specified offering
        offering = find_offering_by_uuid(configuration.waldur_offerings, args.offering_uuid)
        if not offering:
            logger.error("❌ Offering with UUID %s not found in configuration", args.offering_uuid)
            sys.exit(1)

        logger.info("✓ Found offering: %s", offering.name)

        # Validate staff user
        validate_staff_user(args.user_token, offering)

        # Parse and validate date range
        start_date, end_date = parse_date_range(args.start_date, args.end_date)

        # Generate monthly periods
        periods = backend_utils.generate_monthly_periods(
            start_date.year, start_date.month, end_date.year, end_date.month
        )

        total_months = len(periods)
        logger.info("📊 Will process %d months of data", total_months)

        # Process each month
        for month_count, (year, month, _, _) in enumerate(periods, 1):
            load_historical_usage_for_month(
                offering, args.user_token, year, month, month_count, total_months
            )

        logger.info("🎉 Historical usage loading completed successfully!")
        logger.info("Processed %d months from %s to %s", total_months, start_date, end_date)

    except KeyboardInterrupt:
        logger.info("⏹️  Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error("💥 Historical usage loading failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
