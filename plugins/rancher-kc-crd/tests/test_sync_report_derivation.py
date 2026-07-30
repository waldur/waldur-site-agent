"""Derivation of per-grant sync states from CR status.

Covers the state table: progressing phase -> pending, Error -> error,
Ready + confirmed member -> synced, Ready + absent member ->
missing_in_idp; unmapped roles and identifier selection.
"""

from waldur_site_agent_rancher_kc_crd.backend import _derive_grant_states

ROLE_MAP = {"ingress_manage": "ingress-manage"}


def _grants(*usernames: str) -> list[dict]:
    return [
        {"role_name": "ingress_manage", "user_username": name, "user_uuid": f"uuid-{name}"}
        for name in usernames
    ]


def _status(phase: str, synced: list[str]) -> dict:
    return {
        "phase": phase,
        "keycloakRoleBindings": [
            {"groupName": "g", "syncedMembers": [{"userIdentifier": s} for s in synced]}
        ],
    }


def test_ready_confirmed_member_is_synced() -> None:
    entries = _derive_grant_states(
        _grants("alice"), ROLE_MAP, _status("Ready", ["alice"]), "resource_project", "rp1", False
    )
    assert entries == [
        {
            "scope_type": "resource_project",
            "role_name": "ingress_manage",
            "state": "synced",
            "message": "",
            "username": "alice",
            "resource_project_uuid": "rp1",
        }
    ]


def test_ready_absent_member_is_missing_in_idp() -> None:
    entries = _derive_grant_states(
        _grants("bob"), ROLE_MAP, _status("Ready", ["alice"]), "resource_project", "rp1", False
    )
    assert entries[0]["state"] == "missing_in_idp"
    assert "identity provider" in entries[0]["message"]


def test_progressing_phase_is_pending() -> None:
    for phase in ("Pending", "Creating", "Updating", None):
        entries = _derive_grant_states(
            _grants("alice"),
            ROLE_MAP,
            _status(phase, []) if phase else {},
            "resource_project",
            "rp1",
            False,
        )
        assert entries[0]["state"] == "pending", phase


def test_error_phase_is_error_for_all_grants() -> None:
    entries = _derive_grant_states(
        _grants("alice", "bob"),
        ROLE_MAP,
        _status("Error", ["alice"]),
        "resource_project",
        "rp1",
        False,
    )
    assert [e["state"] for e in entries] == ["error", "error"]


def test_unmapped_roles_are_not_reported() -> None:
    grants = [{"role_name": "not_mapped", "user_username": "alice", "user_uuid": "u"}]
    assert (
        _derive_grant_states(
            grants, ROLE_MAP, _status("Ready", []), "resource_project", "rp1", False
        )
        == []
    )


def test_cluster_scope_reads_cluster_bindings_and_omits_rp_uuid() -> None:
    status = {
        "phase": "Ready",
        "clusterKeycloakRoleBindings": [
            {"groupName": "g", "syncedMembers": [{"userIdentifier": "alice"}]}
        ],
    }
    entries = _derive_grant_states(
        [{"role_name": "cluster_owner", "user_username": "alice", "user_uuid": "u"}],
        {"cluster_owner": "cluster-owner"},
        status,
        "resource",
        None,
        False,
    )
    assert entries[0]["state"] == "synced"
    assert entries[0]["scope_type"] == "resource"
    assert "resource_project_uuid" not in entries[0]


def test_user_uuid_identifier_mode() -> None:
    status = {
        "phase": "Ready",
        "keycloakRoleBindings": [
            {"groupName": "g", "syncedMembers": [{"userIdentifier": "uuid-alice"}]}
        ],
    }
    entries = _derive_grant_states(
        _grants("alice"), ROLE_MAP, status, "resource_project", "rp1", True
    )
    assert entries[0]["state"] == "synced"
    assert entries[0]["user_uuid"] == "uuid-alice"
    assert "username" not in entries[0]
