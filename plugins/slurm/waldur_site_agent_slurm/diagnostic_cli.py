"""CLI entry point for SLURM account diagnostics."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional

from waldur_site_agent.common import utils as common_utils
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.diagnostic_service import SlurmAccountDiagnosticService
from waldur_site_agent_slurm.diagnostics import AccountDiagnostic, DiagnosticStatus

logger = logging.getLogger(__name__)

# Maximum number of users to display in the output
MAX_USERS_DISPLAYED = 10


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for the diagnostic CLI."""
    parser = argparse.ArgumentParser(
        prog="waldur_site_diagnose_slurm_account",
        description="Diagnose SLURM account configuration against Waldur Mastermind",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Diagnose an account using default config file
  waldur_site_diagnose_slurm_account alloc_myproject_abc123

  # Diagnose with specific config file
  waldur_site_diagnose_slurm_account alloc_myproject_abc123 -c my-config.yaml

  # Output in JSON format
  waldur_site_diagnose_slurm_account alloc_myproject_abc123 --json

  # Verbose output with detailed reasoning
  waldur_site_diagnose_slurm_account alloc_myproject_abc123 --verbose
""",
    )

    parser.add_argument(
        "account_name",
        help="SLURM account name to diagnose",
    )

    parser.add_argument(
        "-c",
        "--config",
        dest="config_file_path",
        default="waldur-site-agent-config.yaml",
        help="Path to agent configuration file (default: waldur-site-agent-config.yaml)",
    )

    parser.add_argument(
        "--offering-uuid",
        help="Specific offering UUID to use (auto-detected from SLURM offerings if not specified)",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format for scripting",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Include detailed reasoning in output",
    )

    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output",
    )

    return parser


def find_slurm_offering(
    offerings: list[Offering],
    offering_uuid: Optional[str] = None,
    account_name: Optional[str] = None,
) -> Optional[Offering]:
    """Find matching SLURM offering from configuration.

    Args:
        offerings: List of configured offerings
        offering_uuid: Specific offering UUID to use (if provided)
        account_name: Account name to match against prefixes

    Returns:
        Matching Offering or None
    """
    slurm_offerings = [o for o in offerings if o.backend_type == "slurm"]

    if not slurm_offerings:
        return None

    # If specific UUID provided, find that offering
    if offering_uuid:
        for offering in slurm_offerings:
            if offering.waldur_offering_uuid == offering_uuid:
                return offering
        return None

    # If only one SLURM offering, use it
    if len(slurm_offerings) == 1:
        return slurm_offerings[0]

    # Try to match by account prefix
    if account_name:
        for offering in slurm_offerings:
            allocation_prefix = offering.backend_settings.get("allocation_prefix", "")
            if account_name.startswith(allocation_prefix):
                return offering

    # Default to first SLURM offering
    return slurm_offerings[0]


def format_tres_dict(tres_dict: Optional[dict]) -> str:
    """Format TRES dictionary as string."""
    if not tres_dict:
        return "(none)"
    return ",".join(f"{k}={v}" for k, v in sorted(tres_dict.items()))


def format_human_readable(diagnostic: AccountDiagnostic, verbose: bool = False) -> str:
    """Format diagnostic result as human-readable text.

    Args:
        diagnostic: Diagnostic result to format
        verbose: Include detailed reasoning

    Returns:
        Formatted string
    """
    lines = []
    width = 80

    # Header
    lines.append("=" * width)
    lines.append(f"SLURM Account Diagnostic: {diagnostic.account_name}")
    lines.append("=" * width)
    lines.append("")

    # SLURM Cluster Section
    lines.append("SLURM CLUSTER")
    lines.append("-" * width)
    info = diagnostic.slurm_info
    lines.append(f"  {'Account Exists:':<20} {'Yes' if info.exists else 'No'}")
    if info.exists:
        if info.description:
            lines.append(f"  {'Description:':<20} {info.description}")
        if info.organization:
            lines.append(f"  {'Organization:':<20} {info.organization}")
        lines.append(f"  {'Fairshare:':<20} {info.fairshare or '(not set)'}")
        lines.append(f"  {'QoS:':<20} {info.qos or '(not set)'}")
        lines.append(f"  {'GrpTRESMins:':<20} {format_tres_dict(info.grp_tres_mins)}")
        if info.max_tres_mins:
            lines.append(f"  {'MaxTRESMins:':<20} {format_tres_dict(info.max_tres_mins)}")
        if info.grp_tres:
            lines.append(f"  {'GrpTRES:':<20} {format_tres_dict(info.grp_tres)}")
        if info.users:
            lines.append(f"  {'Users:':<20} {', '.join(info.users[:MAX_USERS_DISPLAYED])}")
            if len(info.users) > MAX_USERS_DISPLAYED:
                remaining = len(info.users) - MAX_USERS_DISPLAYED
                lines.append(f"  {'':<20} ... and {remaining} more")
    if info.error:
        lines.append(f"  {'Error:':<20} {info.error}")
    lines.append("")

    # Waldur Mastermind Section
    lines.append("WALDUR MASTERMIND")
    lines.append("-" * width)
    waldur = diagnostic.waldur_info
    lines.append(f"  {'Resource Found:':<20} {'Yes' if waldur.exists else 'No'}")
    if waldur.exists:
        lines.append(f"  {'Resource UUID:':<20} {waldur.uuid or '(unknown)'}")
        lines.append(f"  {'Resource Name:':<20} {waldur.name or '(unknown)'}")
        lines.append(f"  {'State:':<20} {waldur.state or '(unknown)'}")
        lines.append(f"  {'Offering:':<20} {waldur.offering_name or '(unknown)'}")
        lines.append(f"  {'Project:':<20} {waldur.project_name or '(unknown)'}")
        lines.append(f"  {'Organization:':<20} {waldur.customer_name or '(unknown)'}")
        if waldur.limits:
            limits_str = ", ".join(f"{k}={v}" for k, v in waldur.limits.items() if v)
            lines.append(f"  {'Limits:':<20} {limits_str or '(none)'}")
        if waldur.downscaled:
            lines.append(f"  {'Downscaled:':<20} Yes")
        if waldur.paused:
            lines.append(f"  {'Paused:':<20} Yes")
    if waldur.error:
        lines.append(f"  {'Error:':<20} {waldur.error}")
    lines.append("")

    # Policy Section
    lines.append("SLURM POLICY")
    lines.append("-" * width)
    policy = diagnostic.policy_info
    lines.append(f"  {'Policy Found:':<20} {'Yes' if policy.exists else 'No'}")
    if policy.exists:
        lines.append(f"  {'Period:':<20} {policy.period_name or policy.period or '(unknown)'}")
        lines.append(f"  {'Limit Type:':<20} {policy.limit_type or '(unknown)'}")
        tres_billing = "Enabled" if policy.tres_billing_enabled else "Disabled"
        lines.append(f"  {'TRES Billing:':<20} {tres_billing}")
        if policy.tres_billing_weights:
            weights_str = ", ".join(
                f"{k}={v}" for k, v in policy.tres_billing_weights.items()
            )
            lines.append(f"  {'Billing Weights:':<20} {weights_str}")
        if policy.grace_ratio is not None:
            grace_pct = int(policy.grace_ratio * 100)
            grace_str = f"{policy.grace_ratio} ({grace_pct}% overconsumption allowed)"
            lines.append(f"  {'Grace Ratio:':<20} {grace_str}")
        if policy.fairshare_decay_half_life:
            lines.append(f"  {'Decay Half-Life:':<20} {policy.fairshare_decay_half_life} days")
        if policy.carryover_enabled is not None:
            lines.append(
                f"  {'Carryover:':<20} {'Enabled' if policy.carryover_enabled else 'Disabled'}"
            )
        if policy.qos_strategy:
            lines.append(f"  {'QoS Strategy:':<20} {policy.qos_strategy}")
    if policy.error:
        lines.append(f"  {'Note:':<20} {policy.error}")
    lines.append("")

    # Expected vs Actual Section
    if diagnostic.comparisons:
        lines.append("EXPECTED vs ACTUAL")
        lines.append("-" * width)
        for comp in diagnostic.comparisons:
            status_str = f"[{comp.status.value.upper()}]"
            if comp.status == DiagnosticStatus.OK:
                lines.append(f"  {status_str:<10} {comp.field}: {comp.actual} == {comp.expected}")
            else:
                lines.append(
                    f"  {status_str:<10} {comp.field}: {comp.actual} != {comp.expected}"
                )
                if verbose and comp.reason:
                    lines.append(f"             Reason: {comp.reason}")
            # Show unit conversion details if available
            if comp.waldur_unit and comp.slurm_unit and comp.unit_factor:
                unit_detail = (
                    f"Waldur: {comp.waldur_value} {comp.waldur_unit} -> "
                    f"SLURM: {comp.expected} {comp.slurm_unit} (factor: {comp.unit_factor})"
                )
                lines.append(f"             Units: {unit_detail}")
        lines.append("")

    # Unit Conversions Section (in verbose mode)
    if verbose and diagnostic.unit_conversions:
        lines.append("UNIT CONVERSIONS")
        lines.append("-" * width)
        for uc in diagnostic.unit_conversions:
            lines.append(f"  {uc.component_type}:")
            lines.append(f"    Waldur unit:     {uc.waldur_unit}")
            lines.append(f"    SLURM unit:      {uc.slurm_unit}")
            lines.append(f"    Conversion:      {uc.conversion_note}")
            lines.append(f"    Factor:          {uc.unit_factor}")
        lines.append("")

    # Remediation Commands Section
    if diagnostic.fix_commands:
        lines.append("REMEDIATION COMMANDS")
        lines.append("-" * width)
        lines.extend(f"  {cmd}" for cmd in diagnostic.fix_commands)
        lines.append("")

    # Overall Status
    status_str = diagnostic.overall_status.value.upper()
    mismatch_count = sum(
        1 for c in diagnostic.comparisons if c.status == DiagnosticStatus.MISMATCH
    )
    if mismatch_count > 0:
        lines.append(f"OVERALL: {status_str} ({mismatch_count} issue(s) found)")
    else:
        lines.append(f"OVERALL: {status_str}")
    lines.append("=" * width)

    return "\n".join(lines)


def format_json(diagnostic: AccountDiagnostic) -> str:
    """Format diagnostic result as JSON.

    Args:
        diagnostic: Diagnostic result to format

    Returns:
        JSON string
    """
    return json.dumps(diagnostic.to_dict(), indent=2)


def main() -> int:
    """Main entry point for SLURM account diagnostics.

    Returns:
        Exit code (0 for OK, 1 for issues found)
    """
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    # Load configuration
    try:
        configuration = common_utils.load_configuration(
            args.config_file_path, user_agent_suffix="diagnostics"
        )
    except FileNotFoundError:
        logger.exception("Configuration file not found: %s", args.config_file_path)
        return 1
    except Exception:
        logger.exception("Failed to load configuration")
        return 1

    # Find SLURM offering
    offering = find_slurm_offering(
        configuration.waldur_offerings,
        offering_uuid=args.offering_uuid,
        account_name=args.account_name,
    )

    if not offering:
        logger.error("No SLURM offering found in configuration")
        if args.offering_uuid:
            logger.error("Specified offering UUID: %s", args.offering_uuid)
        logger.error(
            "Available offerings: %s",
            [o.name for o in configuration.waldur_offerings],
        )
        return 1

    logger.info("Using offering: %s (%s)", offering.name, offering.waldur_offering_uuid)

    # Create clients
    try:
        waldur_client = common_utils.get_client(
            api_url=offering.waldur_api_url,
            access_token=offering.waldur_api_token,
            agent_header="waldur-site-agent-diagnostics",
            verify_ssl=offering.verify_ssl,
        )
    except Exception:
        logger.exception("Failed to create Waldur client")
        return 1

    # Create SLURM client
    slurm_tres = offering.backend_components_dict
    slurm_client = SlurmClient(slurm_tres)

    # Create diagnostic service and run diagnosis
    service = SlurmAccountDiagnosticService(
        slurm_client=slurm_client,
        waldur_client=waldur_client,
        offering=offering,
    )

    diagnostic = service.diagnose_account(args.account_name)

    # Output results
    if args.json:
        print(format_json(diagnostic))  # noqa: T201
    else:
        print(format_human_readable(diagnostic, verbose=args.verbose))  # noqa: T201

    # Return appropriate exit code
    if diagnostic.overall_status == DiagnosticStatus.OK:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
