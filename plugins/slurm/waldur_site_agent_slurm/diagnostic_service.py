"""Service for diagnosing SLURM account configuration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from waldur_api_client.api.marketplace_provider_resources import (
    marketplace_provider_resources_list,
)
from waldur_api_client.api.marketplace_slurm_periodic_usage_policies import (
    marketplace_slurm_periodic_usage_policies_list,
)
from waldur_api_client.models.resource_state import ResourceState
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.diagnostics import (
    AccountDiagnostic,
    ComparisonResult,
    ComponentUnitInfo,
    DiagnosticStatus,
    ExpectedSettings,
    PolicyInfo,
    SlurmAccountInfo,
    WaldurResourceInfo,
)

if TYPE_CHECKING:
    from waldur_api_client.client import AuthenticatedClient
    from waldur_api_client.models.resource import Resource
    from waldur_api_client.models.slurm_periodic_usage_policy import SlurmPeriodicUsagePolicy

logger = logging.getLogger(__name__)


class SlurmAccountDiagnosticService:
    """Service for diagnosing SLURM account configuration against Waldur."""

    def __init__(
        self,
        slurm_client: SlurmClient,
        waldur_client: AuthenticatedClient,
        offering: Offering,
    ) -> None:
        """Initialize the diagnostic service.

        Args:
            slurm_client: Client for interacting with SLURM cluster
            waldur_client: Authenticated client for Waldur API
            offering: Offering configuration containing backend settings
        """
        self.slurm_client = slurm_client
        self.waldur_client = waldur_client
        self.offering = offering
        self.backend_settings = offering.backend_settings
        self.backend_components = offering.backend_components_dict

    def get_slurm_account_info(self, account_name: str) -> SlurmAccountInfo:
        """Retrieve account information from local SLURM cluster.

        Args:
            account_name: Name of the SLURM account to query

        Returns:
            SlurmAccountInfo with account details from SLURM
        """
        try:
            # Check if account exists
            resource = self.slurm_client.get_resource(account_name)
            if resource is None:
                return SlurmAccountInfo(
                    exists=False,
                    name=account_name,
                    error=f"Account '{account_name}' not found in SLURM cluster",
                )

            # Get additional details
            fairshare = None
            qos = None
            limits: dict[str, dict[str, str]] = {}
            users: list[str] = []

            try:
                fairshare = self.slurm_client.get_account_fairshare(account_name)
            except BackendError as e:
                logger.warning("Failed to get fairshare for %s: %s", account_name, e)

            try:
                qos = self.slurm_client.get_current_account_qos(account_name)
            except BackendError as e:
                logger.warning("Failed to get QoS for %s: %s", account_name, e)

            try:
                limits = self.slurm_client.get_account_limits(account_name)
            except BackendError as e:
                logger.warning("Failed to get limits for %s: %s", account_name, e)

            try:
                users = self.slurm_client.list_resource_users(account_name)
            except BackendError as e:
                logger.warning("Failed to get users for %s: %s", account_name, e)

            return SlurmAccountInfo(
                exists=True,
                name=account_name,
                description=resource.description,
                organization=resource.organization,
                fairshare=fairshare,
                qos=qos,
                grp_tres_mins=limits.get("GrpTRESMins"),
                max_tres_mins=limits.get("MaxTRESMins"),
                grp_tres=limits.get("GrpTRES"),
                users=users,
            )

        except BackendError as e:
            return SlurmAccountInfo(
                exists=False,
                name=account_name,
                error=str(e),
            )

    def get_waldur_resource_info(self, account_name: str) -> WaldurResourceInfo:
        """Retrieve resource information from Waldur by backend_id.

        Args:
            account_name: SLURM account name (which is the backend_id)

        Returns:
            WaldurResourceInfo with resource details from Waldur
        """
        try:
            # Try to find resource by backend_id
            resources = marketplace_provider_resources_list.sync(
                client=self.waldur_client,
                offering_uuid=[UUID(self.offering.waldur_offering_uuid)],
                backend_id=account_name,
                state=[
                    ResourceState.OK,
                    ResourceState.ERRED,
                ],
            )

            if resources and len(resources) > 0:
                resource = resources[0]
                return self._resource_to_info(resource)

            # If not found by backend_id, try matching by expected backend_id pattern
            all_resources = marketplace_provider_resources_list.sync(
                client=self.waldur_client,
                offering_uuid=[UUID(self.offering.waldur_offering_uuid)],
                state=[
                    ResourceState.OK,
                    ResourceState.ERRED,
                ],
            )

            if all_resources:
                allocation_prefix = self.backend_settings.get("allocation_prefix", "")
                for resource in all_resources:
                    # Check if account_name matches expected backend_id
                    expected_backend_id = f"{allocation_prefix}{resource.slug}".lower()
                    if expected_backend_id == account_name.lower():
                        return self._resource_to_info(resource)

            return WaldurResourceInfo(
                exists=False,
                error=f"No Waldur resource found with backend_id '{account_name}'",
            )

        except Exception as e:
            logger.exception("Failed to fetch resource from Waldur")
            return WaldurResourceInfo(
                exists=False,
                error=f"Failed to fetch from Waldur: {e}",
            )

    def _resource_to_info(self, resource: Resource) -> WaldurResourceInfo:
        """Convert Waldur Resource to WaldurResourceInfo."""
        return WaldurResourceInfo(
            exists=True,
            uuid=str(resource.uuid) if resource.uuid else None,
            name=resource.name if not isinstance(resource.name, type(UNSET)) else None,
            state=resource.state if not isinstance(resource.state, type(UNSET)) else None,
            offering_uuid=str(resource.offering_uuid) if resource.offering_uuid else None,
            offering_name=(
                resource.offering_name
                if not isinstance(resource.offering_name, type(UNSET))
                else None
            ),
            project_uuid=str(resource.project_uuid) if resource.project_uuid else None,
            project_name=(
                resource.project_name
                if not isinstance(resource.project_name, type(UNSET))
                else None
            ),
            customer_uuid=str(resource.customer_uuid) if resource.customer_uuid else None,
            customer_name=(
                resource.customer_name
                if not isinstance(resource.customer_name, type(UNSET))
                else None
            ),
            limits=resource.limits if not isinstance(resource.limits, type(UNSET)) else None,
            backend_id=(
                resource.backend_id if not isinstance(resource.backend_id, type(UNSET)) else None
            ),
            downscaled=(
                resource.downscaled if not isinstance(resource.downscaled, type(UNSET)) else None
            ),
            paused=resource.paused if not isinstance(resource.paused, type(UNSET)) else None,
        )

    def get_policy_info(self, offering_uuid: str) -> PolicyInfo:
        """Retrieve SLURM periodic usage policy for the offering.

        Args:
            offering_uuid: UUID of the offering to get policy for

        Returns:
            PolicyInfo with policy configuration from Waldur
        """
        try:
            policies = marketplace_slurm_periodic_usage_policies_list.sync(
                client=self.waldur_client,
                scope_uuid=UUID(offering_uuid),
            )

            if not policies or len(policies) == 0:
                return PolicyInfo(
                    exists=False,
                    error="No SLURM periodic usage policy found for this offering",
                )

            # Use the first policy (typically there's one per offering)
            policy = policies[0]
            return self._policy_to_info(policy)

        except Exception as e:
            logger.exception("Failed to fetch policy from Waldur")
            return PolicyInfo(
                exists=False,
                error=f"Failed to fetch policy: {e}",
            )

    def _policy_to_info(self, policy: SlurmPeriodicUsagePolicy) -> PolicyInfo:
        """Convert Waldur SlurmPeriodicUsagePolicy to PolicyInfo."""
        component_limits = None
        if policy.component_limits_set:
            component_limits = [
                {
                    "type": cl.type_ if hasattr(cl, "type_") else None,
                    "limit": cl.limit if hasattr(cl, "limit") else None,
                }
                for cl in policy.component_limits_set
            ]

        return PolicyInfo(
            exists=True,
            uuid=str(policy.uuid) if policy.uuid else None,
            period=policy.period.value if not isinstance(policy.period, type(UNSET)) else None,
            period_name=(
                policy.period_name if not isinstance(policy.period_name, type(UNSET)) else None
            ),
            limit_type=(
                policy.limit_type.value
                if not isinstance(policy.limit_type, type(UNSET))
                else None
            ),
            tres_billing_enabled=(
                policy.tres_billing_enabled
                if not isinstance(policy.tres_billing_enabled, type(UNSET))
                else None
            ),
            tres_billing_weights=(
                policy.tres_billing_weights
                if not isinstance(policy.tres_billing_weights, type(UNSET))
                else None
            ),
            grace_ratio=(
                policy.grace_ratio if not isinstance(policy.grace_ratio, type(UNSET)) else None
            ),
            carryover_factor=(
                policy.carryover_factor
                if not isinstance(policy.carryover_factor, type(UNSET))
                else None
            ),
            carryover_enabled=(
                policy.carryover_enabled
                if not isinstance(policy.carryover_enabled, type(UNSET))
                else None
            ),
            raw_usage_reset=(
                policy.raw_usage_reset
                if not isinstance(policy.raw_usage_reset, type(UNSET))
                else None
            ),
            qos_strategy=(
                policy.qos_strategy.value
                if not isinstance(policy.qos_strategy, type(UNSET))
                else None
            ),
            component_limits=component_limits,
        )

    def calculate_expected_settings(
        self,
        waldur_info: WaldurResourceInfo,
        policy_info: PolicyInfo,
    ) -> Optional[ExpectedSettings]:
        """Calculate expected SLURM settings based on policy and resource.

        Args:
            waldur_info: Resource information from Waldur
            policy_info: Policy configuration from Waldur

        Returns:
            ExpectedSettings with calculated values, or None if calculation not possible
        """
        if not waldur_info.exists or not policy_info.exists:
            return None

        reasoning: dict[str, str] = {}
        limits: dict[str, int] = {}
        unit_info: dict[str, ComponentUnitInfo] = {}

        # Get limit type from policy
        limit_type = policy_info.limit_type or "GrpTRESMins"

        # Determine SLURM unit based on limit type
        slurm_unit = self._get_slurm_unit_for_limit_type(limit_type)

        # Calculate expected limits from policy component_limits
        if policy_info.component_limits:
            for comp_limit in policy_info.component_limits:
                comp_type = comp_limit.get("type")
                comp_value = comp_limit.get("limit")

                if comp_type and comp_value is not None:
                    # Get unit_factor and measured_unit from backend_components
                    unit_factor = 1.0
                    waldur_unit = "units"
                    if comp_type in self.backend_components:
                        comp_config = self.backend_components[comp_type]
                        unit_factor = comp_config.get("unit_factor", 1.0)
                        waldur_unit = comp_config.get("measured_unit", "units")

                    calculated_limit = int(comp_value * unit_factor)
                    limits[comp_type] = calculated_limit
                    reason = f"policy limit ({comp_value}) * unit_factor ({unit_factor})"
                    reasoning[f"limits.{comp_type}"] = f"{reason} = {calculated_limit}"

                    # Store unit conversion info
                    conv_note = (
                        f"{comp_value} {waldur_unit} -> {calculated_limit} {slurm_unit}"
                    )
                    unit_info[comp_type] = ComponentUnitInfo(
                        component_type=comp_type,
                        waldur_unit=waldur_unit,
                        waldur_value=float(comp_value),
                        slurm_unit=slurm_unit,
                        slurm_value=calculated_limit,
                        unit_factor=unit_factor,
                        conversion_note=conv_note,
                    )

        # If no policy limits, use resource limits from Waldur
        if not limits and waldur_info.limits:
            for comp_type, comp_value in waldur_info.limits.items():
                if comp_value is not None:
                    unit_factor = 1.0
                    waldur_unit = "units"
                    if comp_type in self.backend_components:
                        comp_config = self.backend_components[comp_type]
                        unit_factor = comp_config.get("unit_factor", 1.0)
                        waldur_unit = comp_config.get("measured_unit", "units")

                    calculated_limit = int(comp_value * unit_factor)
                    limits[comp_type] = calculated_limit
                    reason = f"resource limit ({comp_value}) * unit_factor ({unit_factor})"
                    reasoning[f"limits.{comp_type}"] = f"{reason} = {calculated_limit}"

                    # Store unit conversion info
                    conv_note = (
                        f"{comp_value} {waldur_unit} -> {calculated_limit} {slurm_unit}"
                    )
                    unit_info[comp_type] = ComponentUnitInfo(
                        component_type=comp_type,
                        waldur_unit=waldur_unit,
                        waldur_value=float(comp_value),
                        slurm_unit=slurm_unit,
                        slurm_value=calculated_limit,
                        unit_factor=unit_factor,
                        conversion_note=conv_note,
                    )

        # Determine expected QoS
        expected_qos = self.backend_settings.get("qos_default", "normal")
        reasoning["qos"] = f"Default QoS from backend_settings: {expected_qos}"

        # Fairshare calculation (simplified)
        fairshare = None
        if policy_info.carryover_factor:
            factor = policy_info.carryover_factor
            reasoning["fairshare"] = f"Policy-managed fairshare (carryover_factor={factor}%)"

        return ExpectedSettings(
            fairshare=fairshare,
            qos=expected_qos,
            limits=limits,
            limit_type=limit_type,
            reasoning=reasoning,
            unit_info=unit_info,
        )

    def _get_slurm_unit_for_limit_type(self, limit_type: str) -> str:
        """Get the SLURM unit based on limit type.

        Args:
            limit_type: The SLURM limit type (GrpTRESMins, MaxTRESMins, GrpTRES)

        Returns:
            Human-readable SLURM unit string
        """
        if limit_type in ("GrpTRESMins", "MaxTRESMins"):
            return "TRES-minutes"
        if limit_type == "GrpTRES":
            return "TRES"
        return "units"

    def compare_settings(
        self,
        slurm_info: SlurmAccountInfo,
        expected: ExpectedSettings,
        account_name: str,
    ) -> list[ComparisonResult]:
        """Compare actual SLURM settings with expected settings.

        Args:
            slurm_info: Actual account info from SLURM
            expected: Expected settings calculated from policy
            account_name: Account name for fix commands

        Returns:
            List of comparison results
        """
        comparisons: list[ComparisonResult] = []

        # Compare QoS
        if expected.qos:
            actual_qos = slurm_info.qos or ""
            if actual_qos == expected.qos:
                comparisons.append(
                    ComparisonResult(
                        field="qos",
                        actual=actual_qos,
                        expected=expected.qos,
                        status=DiagnosticStatus.OK,
                    )
                )
            else:
                fix_cmd = f"sacctmgr -i modify account {account_name} set qos={expected.qos}"
                comparisons.append(
                    ComparisonResult(
                        field="qos",
                        actual=actual_qos,
                        expected=expected.qos,
                        status=DiagnosticStatus.MISMATCH,
                        reason=expected.reasoning.get("qos"),
                        fix_command=fix_cmd,
                    )
                )

        # Compare fairshare if expected
        if expected.fairshare is not None:
            actual_fairshare = slurm_info.fairshare
            if actual_fairshare == expected.fairshare:
                comparisons.append(
                    ComparisonResult(
                        field="fairshare",
                        actual=actual_fairshare,
                        expected=expected.fairshare,
                        status=DiagnosticStatus.OK,
                    )
                )
            else:
                fix_cmd = (
                    f"sacctmgr -i modify account {account_name} set fairshare={expected.fairshare}"
                )
                comparisons.append(
                    ComparisonResult(
                        field="fairshare",
                        actual=actual_fairshare,
                        expected=expected.fairshare,
                        status=DiagnosticStatus.MISMATCH,
                        reason=expected.reasoning.get("fairshare"),
                        fix_command=fix_cmd,
                    )
                )

        # Compare limits
        if expected.limits:
            limit_type = expected.limit_type or "GrpTRESMins"
            actual_limits = self._get_actual_limits(slurm_info, limit_type)
            slurm_unit = self._get_slurm_unit_for_limit_type(limit_type)

            # Build expected TRES string for fix command
            expected_tres_parts = [
                f"{k}={v}" for k, v in sorted(expected.limits.items())
            ]
            expected_tres_str = ",".join(expected_tres_parts)

            for comp_type, expected_value in expected.limits.items():
                actual_value = actual_limits.get(comp_type)
                field_name = f"{limit_type}[{comp_type}]"

                if actual_value is not None:
                    try:
                        actual_int = int(actual_value)
                    except (ValueError, TypeError):
                        actual_int = None
                else:
                    actual_int = None

                # Get unit conversion info for this component
                unit_info = expected.unit_info.get(comp_type)
                waldur_value = unit_info.waldur_value if unit_info else None
                waldur_unit = unit_info.waldur_unit if unit_info else None
                unit_factor = unit_info.unit_factor if unit_info else None

                if actual_int == expected_value:
                    comparisons.append(
                        ComparisonResult(
                            field=field_name,
                            actual=actual_int,
                            expected=expected_value,
                            status=DiagnosticStatus.OK,
                            waldur_value=waldur_value,
                            waldur_unit=waldur_unit,
                            slurm_unit=slurm_unit,
                            unit_factor=unit_factor,
                        )
                    )
                else:
                    fix_cmd = (
                        f"sacctmgr -i modify account {account_name} "
                        f"set {limit_type}={expected_tres_str}"
                    )
                    comparisons.append(
                        ComparisonResult(
                            field=field_name,
                            actual=actual_int,
                            expected=expected_value,
                            status=DiagnosticStatus.MISMATCH,
                            reason=expected.reasoning.get(f"limits.{comp_type}"),
                            fix_command=fix_cmd,
                            waldur_value=waldur_value,
                            waldur_unit=waldur_unit,
                            slurm_unit=slurm_unit,
                            unit_factor=unit_factor,
                        )
                    )

        return comparisons

    def _get_actual_limits(
        self, slurm_info: SlurmAccountInfo, limit_type: str
    ) -> dict[str, Any]:
        """Get actual limits from SLURM info based on limit type."""
        if limit_type == "GrpTRESMins":
            return slurm_info.grp_tres_mins or {}
        if limit_type == "MaxTRESMins":
            return slurm_info.max_tres_mins or {}
        if limit_type == "GrpTRES":
            return slurm_info.grp_tres or {}
        return {}

    def generate_fix_commands(self, comparisons: list[ComparisonResult]) -> list[str]:
        """Generate unique SLURM remediation commands from comparisons.

        Args:
            comparisons: List of comparison results

        Returns:
            List of unique fix commands
        """
        fix_commands: list[str] = []
        seen_commands: set[str] = set()

        for comp in comparisons:
            is_mismatch = comp.status == DiagnosticStatus.MISMATCH
            if is_mismatch and comp.fix_command and comp.fix_command not in seen_commands:
                fix_commands.append(comp.fix_command)
                seen_commands.add(comp.fix_command)

        return fix_commands

    def diagnose_account(self, account_name: str) -> AccountDiagnostic:
        """Run complete diagnostic for a SLURM account.

        Args:
            account_name: Name of the SLURM account to diagnose

        Returns:
            Complete AccountDiagnostic result
        """
        # Step 1: Get SLURM account info
        slurm_info = self.get_slurm_account_info(account_name)

        # Step 2: Get Waldur resource info
        waldur_info = self.get_waldur_resource_info(account_name)

        # Step 3: Get policy info (if resource found)
        policy_info: PolicyInfo
        if waldur_info.exists and waldur_info.offering_uuid:
            policy_info = self.get_policy_info(waldur_info.offering_uuid)
        else:
            policy_info = PolicyInfo(
                exists=False,
                error="Cannot fetch policy: Waldur resource not found",
            )

        # Step 4: Calculate expected settings
        expected_settings = self.calculate_expected_settings(waldur_info, policy_info)

        # Step 5: Compare settings (if we have both SLURM info and expected settings)
        comparisons: list[ComparisonResult] = []
        fix_commands: list[str] = []
        unit_conversions: list[ComponentUnitInfo] = []

        if slurm_info.exists and expected_settings:
            comparisons = self.compare_settings(slurm_info, expected_settings, account_name)
            fix_commands = self.generate_fix_commands(comparisons)
            # Extract unit conversions from expected settings
            unit_conversions = list(expected_settings.unit_info.values())

        # Step 6: Determine overall status
        overall_status = self._determine_overall_status(slurm_info, waldur_info, comparisons)

        return AccountDiagnostic(
            account_name=account_name,
            slurm_info=slurm_info,
            waldur_info=waldur_info,
            policy_info=policy_info,
            expected_settings=expected_settings,
            comparisons=comparisons,
            fix_commands=fix_commands,
            overall_status=overall_status,
            unit_conversions=unit_conversions,
        )

    def _determine_overall_status(
        self,
        slurm_info: SlurmAccountInfo,
        waldur_info: WaldurResourceInfo,
        comparisons: list[ComparisonResult],
    ) -> DiagnosticStatus:
        """Determine overall diagnostic status."""
        # Check for errors
        if slurm_info.error or waldur_info.error:
            return DiagnosticStatus.ERROR

        # Check for missing
        if not slurm_info.exists or not waldur_info.exists:
            return DiagnosticStatus.MISSING

        # Check comparisons
        if not comparisons:
            return DiagnosticStatus.UNKNOWN

        has_mismatch = any(c.status == DiagnosticStatus.MISMATCH for c in comparisons)
        if has_mismatch:
            return DiagnosticStatus.MISMATCH

        return DiagnosticStatus.OK
