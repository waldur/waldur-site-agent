"""Account diagnostics must read SDK models, not assume plain dicts.

The Waldur client hands back attrs models: ``Resource.limits`` is a
``ResourceLimits`` and a policy's ``tres_billing_weights`` a
``SlurmPeriodicUsagePolicyTresBillingWeights``. Both used to be passed through
as if they were dicts and crashed the formatter with ``.items()``.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

from waldur_api_client.models.resource import Resource
from waldur_api_client.models.slurm_periodic_usage_policy_tres_billing_weights import (
    SlurmPeriodicUsagePolicyTresBillingWeights,
)
from waldur_site_agent_slurm.diagnostic_cli import format_human_readable, format_json
from waldur_site_agent_slurm.diagnostic_service import SlurmAccountDiagnosticService
from waldur_site_agent_slurm.diagnostics import AccountDiagnostic, PolicyInfo, SlurmAccountInfo


def _service() -> SlurmAccountDiagnosticService:
    offering = MagicMock()
    offering.backend_settings = {"allocation_prefix": "hpc_"}
    offering.backend_components_dict = {}
    slurm_client = MagicMock(cluster_name=None)
    return SlurmAccountDiagnosticService(slurm_client, MagicMock(), offering)


def _resource() -> Resource:
    return Resource.from_dict(
        {
            "uuid": uuid.uuid4().hex,
            "name": "alloc",
            "state": "OK",
            "limits": {"cpu": 100, "mem": 0},
            "backend_id": "hpc_alloc",
        }
    )


def _diagnostic(waldur_info, policy_info=None) -> AccountDiagnostic:
    return AccountDiagnostic(
        account_name="hpc_alloc",
        slurm_info=SlurmAccountInfo(exists=True, name="hpc_alloc"),
        waldur_info=waldur_info,
        policy_info=policy_info or PolicyInfo(exists=False),
    )


def test_resource_limits_become_a_plain_dict() -> None:
    info = _service()._resource_to_info(_resource())

    assert isinstance(info.limits, dict)
    assert info.limits == {"cpu": 100, "mem": 0}


def test_human_readable_report_lists_the_limits() -> None:
    info = _service()._resource_to_info(_resource())

    report = format_human_readable(_diagnostic(info))

    assert "cpu=100" in report


def test_json_report_serialises_the_limits() -> None:
    info = _service()._resource_to_info(_resource())

    data = json.loads(format_json(_diagnostic(info)))

    assert data["waldur_info"]["limits"] == {"cpu": 100, "mem": 0}


def test_policy_billing_weights_become_a_plain_dict() -> None:
    policy = MagicMock()
    policy.component_limits_set = []
    policy.tres_billing_weights = SlurmPeriodicUsagePolicyTresBillingWeights.from_dict(
        {"CPU": 1.0, "GRES/gpu": 10.0}
    )

    info = _service()._policy_to_info(policy)

    assert info.tres_billing_weights == {"CPU": 1.0, "GRES/gpu": 10.0}
    format_human_readable(
        _diagnostic(_service()._resource_to_info(_resource()), policy_info=info), verbose=True
    )
