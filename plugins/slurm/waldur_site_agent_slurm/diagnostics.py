"""Data structures for SLURM account diagnostics."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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
    description: str | None = None
    organization: str | None = None
    parent_account: str | None = None
    fairshare: int | None = None
    qos: str | None = None
    grp_tres_mins: dict[str, str] | None = None
    max_tres_mins: dict[str, str] | None = None
    grp_tres: dict[str, str] | None = None
    users: list[str] | None = None
    error: str | None = None


@dataclass
class WaldurResourceInfo:
    """Information retrieved from Waldur Mastermind."""

    exists: bool
    uuid: str | None = None
    name: str | None = None
    state: str | None = None
    offering_uuid: str | None = None
    offering_name: str | None = None
    project_uuid: str | None = None
    project_name: str | None = None
    customer_uuid: str | None = None
    customer_name: str | None = None
    limits: dict[str, Any] | None = None
    backend_id: str | None = None
    downscaled: bool | None = None
    paused: bool | None = None
    error: str | None = None


@dataclass
class PolicyInfo:
    """SLURM Periodic Usage Policy information from Waldur."""

    exists: bool
    uuid: str | None = None
    period: str | None = None
    period_name: str | None = None
    limit_type: str | None = None
    tres_billing_enabled: bool | None = None
    tres_billing_weights: dict[str, float] | None = None
    grace_ratio: float | None = None
    carryover_factor: int | None = None
    carryover_enabled: bool | None = None
    raw_usage_reset: bool | None = None
    qos_strategy: str | None = None
    component_limits: list[dict[str, Any]] | None = None
    error: str | None = None


@dataclass
class ComponentUnitInfo:
    """Unit conversion information for a component."""

    component_type: str
    waldur_unit: str
    waldur_value: float | None = None
    slurm_unit: str = "minutes"  # SLURM typically uses minutes for TRES
    slurm_value: int | None = None
    unit_factor: float = 1.0
    conversion_note: str | None = None


@dataclass
class ExpectedSettings:
    """Expected SLURM settings calculated from policy and resource."""

    fairshare: int | None = None
    qos: str | None = None
    limits: dict[str, int] | None = None
    limit_type: str | None = None
    reasoning: dict[str, str] = field(default_factory=dict)
    unit_info: dict[str, ComponentUnitInfo] = field(default_factory=dict)


@dataclass
class ComparisonResult:
    """Result of comparing actual vs expected settings."""

    field: str
    actual: Any
    expected: Any
    status: DiagnosticStatus
    reason: str | None = None
    fix_command: str | None = None
    # Unit conversion details
    waldur_value: float | None = None
    waldur_unit: str | None = None
    slurm_unit: str | None = None
    unit_factor: float | None = None


@dataclass
class AccountDiagnostic:
    """Complete diagnostic result for a SLURM account."""

    account_name: str
    slurm_info: SlurmAccountInfo
    waldur_info: WaldurResourceInfo
    policy_info: PolicyInfo
    expected_settings: ExpectedSettings | None = None
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
