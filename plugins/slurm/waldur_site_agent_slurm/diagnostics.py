"""Data structures for SLURM account diagnostics."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class DiagnosticStatus(Enum):
    """Status of a diagnostic check."""

    OK = "ok"
    MISMATCH = "mismatch"
    MISSING = "missing"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class SlurmAccountInfo:
    """Information retrieved from local SLURM cluster."""

    exists: bool
    name: str
    description: Optional[str] = None
    organization: Optional[str] = None
    parent_account: Optional[str] = None
    fairshare: Optional[int] = None
    qos: Optional[str] = None
    grp_tres_mins: Optional[dict[str, str]] = None
    max_tres_mins: Optional[dict[str, str]] = None
    grp_tres: Optional[dict[str, str]] = None
    users: Optional[list[str]] = None
    error: Optional[str] = None


@dataclass
class WaldurResourceInfo:
    """Information retrieved from Waldur Mastermind."""

    exists: bool
    uuid: Optional[str] = None
    name: Optional[str] = None
    state: Optional[str] = None
    offering_uuid: Optional[str] = None
    offering_name: Optional[str] = None
    project_uuid: Optional[str] = None
    project_name: Optional[str] = None
    customer_uuid: Optional[str] = None
    customer_name: Optional[str] = None
    limits: Optional[dict[str, Any]] = None
    backend_id: Optional[str] = None
    downscaled: Optional[bool] = None
    paused: Optional[bool] = None
    error: Optional[str] = None


@dataclass
class PolicyInfo:
    """SLURM Periodic Usage Policy information from Waldur."""

    exists: bool
    uuid: Optional[str] = None
    period: Optional[str] = None
    period_name: Optional[str] = None
    limit_type: Optional[str] = None
    tres_billing_enabled: Optional[bool] = None
    tres_billing_weights: Optional[dict[str, float]] = None
    grace_ratio: Optional[float] = None
    carryover_factor: Optional[int] = None
    carryover_enabled: Optional[bool] = None
    raw_usage_reset: Optional[bool] = None
    qos_strategy: Optional[str] = None
    component_limits: Optional[list[dict[str, Any]]] = None
    error: Optional[str] = None


@dataclass
class ComponentUnitInfo:
    """Unit conversion information for a component."""

    component_type: str
    waldur_unit: str
    waldur_value: Optional[float] = None
    slurm_unit: str = "minutes"  # SLURM typically uses minutes for TRES
    slurm_value: Optional[int] = None
    unit_factor: float = 1.0
    conversion_note: Optional[str] = None


@dataclass
class ExpectedSettings:
    """Expected SLURM settings calculated from policy and resource."""

    fairshare: Optional[int] = None
    qos: Optional[str] = None
    limits: Optional[dict[str, int]] = None
    limit_type: Optional[str] = None
    reasoning: dict[str, str] = field(default_factory=dict)
    unit_info: dict[str, ComponentUnitInfo] = field(default_factory=dict)


@dataclass
class ComparisonResult:
    """Result of comparing actual vs expected settings."""

    field: str
    actual: Any
    expected: Any
    status: DiagnosticStatus
    reason: Optional[str] = None
    fix_command: Optional[str] = None
    # Unit conversion details
    waldur_value: Optional[float] = None
    waldur_unit: Optional[str] = None
    slurm_unit: Optional[str] = None
    unit_factor: Optional[float] = None


@dataclass
class AccountDiagnostic:
    """Complete diagnostic result for a SLURM account."""

    account_name: str
    slurm_info: SlurmAccountInfo
    waldur_info: WaldurResourceInfo
    policy_info: PolicyInfo
    expected_settings: Optional[ExpectedSettings] = None
    comparisons: list[ComparisonResult] = field(default_factory=list)
    fix_commands: list[str] = field(default_factory=list)
    overall_status: DiagnosticStatus = DiagnosticStatus.UNKNOWN
    unit_conversions: list[ComponentUnitInfo] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert diagnostic result to dictionary for JSON serialization."""
        return {
            "account_name": self.account_name,
            "overall_status": self.overall_status.value,
            "slurm_info": {
                "exists": self.slurm_info.exists,
                "name": self.slurm_info.name,
                "description": self.slurm_info.description,
                "organization": self.slurm_info.organization,
                "parent_account": self.slurm_info.parent_account,
                "fairshare": self.slurm_info.fairshare,
                "qos": self.slurm_info.qos,
                "grp_tres_mins": self.slurm_info.grp_tres_mins,
                "max_tres_mins": self.slurm_info.max_tres_mins,
                "grp_tres": self.slurm_info.grp_tres,
                "users": self.slurm_info.users,
                "error": self.slurm_info.error,
            },
            "waldur_info": {
                "exists": self.waldur_info.exists,
                "uuid": self.waldur_info.uuid,
                "name": self.waldur_info.name,
                "state": self.waldur_info.state,
                "offering_uuid": self.waldur_info.offering_uuid,
                "offering_name": self.waldur_info.offering_name,
                "project_uuid": self.waldur_info.project_uuid,
                "project_name": self.waldur_info.project_name,
                "customer_uuid": self.waldur_info.customer_uuid,
                "customer_name": self.waldur_info.customer_name,
                "limits": self.waldur_info.limits,
                "backend_id": self.waldur_info.backend_id,
                "downscaled": self.waldur_info.downscaled,
                "paused": self.waldur_info.paused,
                "error": self.waldur_info.error,
            },
            "policy_info": {
                "exists": self.policy_info.exists,
                "uuid": self.policy_info.uuid,
                "period": self.policy_info.period,
                "period_name": self.policy_info.period_name,
                "limit_type": self.policy_info.limit_type,
                "tres_billing_enabled": self.policy_info.tres_billing_enabled,
                "tres_billing_weights": self.policy_info.tres_billing_weights,
                "grace_ratio": self.policy_info.grace_ratio,
                "carryover_factor": self.policy_info.carryover_factor,
                "carryover_enabled": self.policy_info.carryover_enabled,
                "raw_usage_reset": self.policy_info.raw_usage_reset,
                "qos_strategy": self.policy_info.qos_strategy,
                "component_limits": self.policy_info.component_limits,
                "error": self.policy_info.error,
            },
            "expected_settings": (
                {
                    "fairshare": self.expected_settings.fairshare,
                    "qos": self.expected_settings.qos,
                    "limits": self.expected_settings.limits,
                    "limit_type": self.expected_settings.limit_type,
                    "reasoning": self.expected_settings.reasoning,
                }
                if self.expected_settings
                else None
            ),
            "comparisons": [
                {
                    "field": c.field,
                    "actual": c.actual,
                    "expected": c.expected,
                    "status": c.status.value,
                    "reason": c.reason,
                    "fix_command": c.fix_command,
                    "waldur_value": c.waldur_value,
                    "waldur_unit": c.waldur_unit,
                    "slurm_unit": c.slurm_unit,
                    "unit_factor": c.unit_factor,
                }
                for c in self.comparisons
            ],
            "fix_commands": self.fix_commands,
            "unit_conversions": [
                {
                    "component_type": u.component_type,
                    "waldur_unit": u.waldur_unit,
                    "waldur_value": u.waldur_value,
                    "slurm_unit": u.slurm_unit,
                    "slurm_value": u.slurm_value,
                    "unit_factor": u.unit_factor,
                    "conversion_note": u.conversion_note,
                }
                for u in self.unit_conversions
            ],
        }
