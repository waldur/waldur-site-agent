"""Derivation of per-grant sync states from CR status.

Covers the state table: progressing phase -> pending, Error -> error,
Ready + confirmed member -> synced, Ready + absent member ->
missing_in_idp; unmapped roles and identifier selection, including the
civil-number identity whose CR identifier never reaches the report.
"""

import logging
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest
from waldur_api_client.types import UNSET
from waldur_site_agent_rancher_kc_crd.backend import RancherKcCrdBackend, _derive_grant_states
from waldur_site_agent_rancher_kc_crd.translator import (
    LEGACY_UUID_IDENTITY,
    resolve_identity_settings,
)

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


# ---------------------------------------------------------------------
# Civil-number identity: CR identifier and report key are separate
# ---------------------------------------------------------------------

CIVIL = resolve_identity_settings({"keycloak_user_identity_source": "civil_number"})
CIVIL_ATTRIBUTE = resolve_identity_settings(
    {
        "keycloak_user_identity_source": "civil_number",
        "keycloak_user_lookup": "attribute",
        "keycloak_lookup_attribute": "personalCode",
        "keycloak_user_identity_template": "EE${value}",
    }
)


def _civil_grant(name: str, civil: Optional[str]) -> dict:
    return {
        "role_name": "ingress_manage",
        "user_username": name,
        "user_uuid": f"uuid-{name}",
        "user_civil_number": civil,
    }


def test_civil_mode_reports_by_user_uuid_never_civil_code() -> None:
    entries = _derive_grant_states(
        [_civil_grant("alice", "38001010000"), _civil_grant("bob", "49002020000")],
        ROLE_MAP,
        _status("Ready", ["38001010000"]),
        "resource_project",
        "rp1",
        CIVIL,
    )
    assert [(e["user_uuid"], e["state"]) for e in entries] == [
        ("uuid-alice", "synced"),
        ("uuid-bob", "missing_in_idp"),
    ]
    for entry in entries:
        assert "username" not in entry
        assert "38001010000" not in repr(entry)
        assert "49002020000" not in repr(entry)


def test_attribute_lookup_matches_echoed_attribute_value() -> None:
    # The operator echoes the attribute value (here the templated civil
    # code, kept verbatim since attribute lookup does not lowercase).
    entries = _derive_grant_states(
        [_civil_grant("alice", "38001010000")],
        ROLE_MAP,
        _status("Ready", ["EE38001010000"]),
        "resource_project",
        "rp1",
        CIVIL_ATTRIBUTE,
    )
    assert entries[0]["state"] == "synced"
    assert entries[0]["user_uuid"] == "uuid-alice"


def test_username_lookup_compares_lowercased_identifier_but_reports_waldur_username() -> None:
    entries = _derive_grant_states(
        [{"role_name": "ingress_manage", "user_username": "Alice", "user_uuid": "u"}],
        ROLE_MAP,
        _status("Ready", ["alice"]),
        "resource_project",
        "rp1",
        resolve_identity_settings({}),
    )
    assert entries[0]["state"] == "synced"
    assert entries[0]["username"] == "Alice"


def test_missing_civil_code_is_reported_whatever_the_phase() -> None:
    for status in (_status("Ready", []), _status("Pending", []), _status("Error", []), {}):
        entries = _derive_grant_states(
            [_civil_grant("alice", None)], ROLE_MAP, status, "resource_project", "rp1", CIVIL
        )
        assert entries == [
            {
                "scope_type": "resource_project",
                "role_name": "ingress_manage",
                "state": "missing_in_idp",
                "message": "Civil code not available for this user",
                "user_uuid": "uuid-alice",
                "resource_project_uuid": "rp1",
            }
        ]


def test_missing_username_is_still_skipped_in_username_mode() -> None:
    grants = [{"role_name": "ingress_manage", "user_username": None, "user_uuid": "u"}]
    assert (
        _derive_grant_states(
            grants, ROLE_MAP, _status("Ready", []), "resource_project", "rp1", False
        )
        == []
    )


# ---------------------------------------------------------------------
# pull_resource in civil-number mode
# ---------------------------------------------------------------------


def _backend(**settings) -> RancherKcCrdBackend:
    with patch("waldur_site_agent_rancher_kc_crd.backend.CrdClient"):
        return RancherKcCrdBackend(
            {
                "role_map": ROLE_MAP,
                "waldur_api_url": "https://waldur.example.com/api/",
                "waldur_api_token": "t",
                **settings,
            },
            {},
        )


def _sdk_user(name: str, civil: object = UNSET, legacy_sdk: bool = False) -> SimpleNamespace:
    user = SimpleNamespace(
        role_name="ingress_manage",
        user_uuid=uuid.uuid5(uuid.NAMESPACE_DNS, name),
        user_username=name,
        additional_properties={},
    )
    if legacy_sdk:
        # A waldur-api-client predating the field keeps it here.
        user.additional_properties["user_civil_number"] = civil
    else:
        user.user_civil_number = civil
    return user


def _pull(backend: RancherKcCrdBackend, users: list, synced: list[str]):
    rp = SimpleNamespace(uuid=uuid.uuid4(), name="alpha", state="OK", limits={})
    resource = SimpleNamespace(
        uuid=uuid.uuid4(), slug="rs", backend_id="c-1", customer_slug="", project_slug=""
    )
    backend.crd.get.return_value = {"status": _status("Ready", synced)}
    backend.crd.list_for_resource.return_value = []
    with patch.object(backend, "_fetch_resource_projects", return_value=[rp]):  # noqa: SIM117
        with patch.object(backend, "_fetch_resource_project_users", return_value=users):
            info = backend.pull_resource(resource)
    applied = backend.crd.apply.call_args[0][0]
    return info, applied, backend.get_membership_sync_report(resource)


def test_user_role_dict_reads_civil_number_from_attribute_or_legacy_sdk() -> None:
    to_dict = RancherKcCrdBackend._user_role_to_dict
    assert to_dict(_sdk_user("a", "38001010000"))["user_civil_number"] == "38001010000"
    assert to_dict(_sdk_user("a", "38001010000", legacy_sdk=True))["user_civil_number"] == (
        "38001010000"
    )
    assert to_dict(_sdk_user("a"))["user_civil_number"] is None
    assert to_dict(_sdk_user("a", None))["user_civil_number"] is None


def test_pull_resource_in_civil_mode_keeps_civil_codes_out_of_waldur(caplog) -> None:
    backend = _backend(keycloak_user_identity_source="civil_number")
    alice = _sdk_user("alice", "38001010000")
    bob = _sdk_user("bob")
    with caplog.at_level(logging.INFO):
        info, applied, report = _pull(backend, [alice, bob], ["38001010000", "50003030000"])

    members = applied["spec"]["keycloak"]["roleBindings"][0]["members"]
    assert members == [{"userIdentifier": "38001010000", "lookupByID": False}]
    # Confirmed members map back to Waldur UUIDs; the out-of-band one is dropped.
    assert info.users == [alice.user_uuid.hex]
    assert {(e["user_uuid"], e["state"]) for e in report} == {
        (alice.user_uuid.hex, "synced"),
        (bob.user_uuid.hex, "missing_in_idp"),
    }
    logs = "\n".join(r.getMessage() for r in caplog.records)
    assert "50003030000" not in logs
    assert "5000…00" in logs
    assert "expose_civil_number" not in logs


def test_pull_resource_warns_when_no_user_carries_a_civil_code(caplog) -> None:
    backend = _backend(keycloak_user_identity_source="civil_number")
    with caplog.at_level(logging.WARNING):
        info, applied, _report = _pull(backend, [_sdk_user("alice"), _sdk_user("bob")], [])
    warnings = [r.getMessage() for r in caplog.records if "expose_civil_number" in r.getMessage()]
    assert len(warnings) == 1
    assert applied["spec"]["keycloak"]["roleBindings"] == []
    assert info.users == []


def test_pull_resource_in_username_mode_reports_synced_identifiers_unchanged() -> None:
    backend = _backend()
    info, _applied, report = _pull(backend, [_sdk_user("alice")], ["alice", "stranger"])
    assert info.users == ["alice", "stranger"]
    assert report[0]["username"] == "alice"


def test_invalid_identity_settings_fail_backend_construction() -> None:
    with pytest.raises(ValueError, match="keycloak_lookup_attribute"):
        _backend(keycloak_user_lookup="attribute")


def test_deprecated_keycloak_use_user_id_is_warned_about(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        backend = _backend(keycloak_use_user_id=True)
    assert backend.identity == LEGACY_UUID_IDENTITY
    assert any("deprecated" in r.getMessage() for r in caplog.records)
