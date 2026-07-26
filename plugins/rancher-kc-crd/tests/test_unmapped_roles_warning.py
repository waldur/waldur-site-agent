"""Warning coverage for role grants the translator would drop.

The translator silently drops user-roles absent from the configured
role map; the backend must surface that as a warning so a Waldur role
grant that never reaches Rancher is at least visible in the agent logs.
"""

import logging

from waldur_site_agent_rancher_kc_crd.backend import RancherKcCrdBackend


def _grants(*names: str) -> list[dict]:
    return [
        {"role_name": name, "user_uuid": None, "user_username": f"user-{i}"}
        for i, name in enumerate(names)
    ]


def test_unmapped_roles_are_warned_about(caplog):
    with caplog.at_level(logging.WARNING):
        RancherKcCrdBackend._warn_unmapped_roles(
            _grants("ingress_manage", "unmapped_role", "another_unmapped"),
            {"ingress_manage": "ingress-manage"},
            "role_map",
            "rko-alpha (uuid)",
        )
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "another_unmapped, unmapped_role" in message
    assert "role_map" in message
    assert "rko-alpha (uuid)" in message


def test_fully_mapped_roles_stay_silent(caplog):
    with caplog.at_level(logging.WARNING):
        RancherKcCrdBackend._warn_unmapped_roles(
            _grants("ingress_manage"),
            {"ingress_manage": "ingress-manage"},
            "role_map",
            "rko-alpha (uuid)",
        )
    assert len(caplog.records) == 0


def test_empty_role_names_are_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        RancherKcCrdBackend._warn_unmapped_roles(
            [{"role_name": None, "user_username": "u"}],
            {},
            "role_map",
            "scope",
        )
    assert len(caplog.records) == 0
