"""Unit tests for status_reader (pure logic, no I/O)."""

from waldur_site_agent_rancher_kc_crd import status_reader as s

# ---------------------------------------------------------------------
# extract_synced_users
# ---------------------------------------------------------------------


class TestExtractSyncedUsers:
    def test_flattens_role_bindings(self):
        status = {
            "keycloakRoleBindings": [
                {
                    "groupName": "g1",
                    "syncedMembers": [
                        {"userIdentifier": "alice"},
                        {"userIdentifier": "bob"},
                    ],
                },
                {
                    "groupName": "g2",
                    "syncedMembers": [{"userIdentifier": "carol"}],
                },
            ]
        }
        assert s.extract_synced_users(status) == {"alice", "bob", "carol"}

    def test_returns_empty_for_missing_status(self):
        assert s.extract_synced_users({}) == set()
        assert s.extract_synced_users({"phase": "Pending"}) == set()

    def test_handles_string_member_form(self):
        status = {
            "keycloakRoleBindings": [
                {"syncedMembers": ["alice", "bob"]},
            ]
        }
        assert s.extract_synced_users(status) == {"alice", "bob"}


# ---------------------------------------------------------------------
# detect_drift
# ---------------------------------------------------------------------


class TestDetectDrift:
    def test_no_drift(self):
        missing, extra = s.detect_drift({"a", "b"}, {"a", "b"})
        assert missing == set()
        assert extra == set()

    def test_missing_in_keycloak(self):
        missing, extra = s.detect_drift({"a", "b"}, {"a"})
        assert missing == {"b"}
        assert extra == set()

    def test_present_only_in_keycloak(self):
        missing, extra = s.detect_drift({"a"}, {"a", "eve"})
        assert missing == set()
        assert extra == {"eve"}


# ---------------------------------------------------------------------
# build_backend_metadata
# ---------------------------------------------------------------------


class TestBuildBackendMetadata:
    def test_minimal_status(self):
        status = {
            "phase": "Ready",
            "rancherProjectId": "p-abc",
            # Even when a stale CR carries namespaceName from operator
            # 0.2.x, build_backend_metadata must not surface it.
            "namespaceName": "team-alpha",
        }
        md = s.build_backend_metadata(status)
        assert md["phase"] == "Ready"
        assert md["rancher_project_id"] == "p-abc"
        assert "namespace_name" not in md
        # No noisy nulls.
        assert "failing_conditions" not in md

    def test_failing_conditions_surfaced(self):
        status = {
            "phase": "Error",
            "conditions": [
                {"type": "RancherBindingsReady", "status": "True"},
                {
                    "type": "KeycloakGroupsReady",
                    "status": "False",
                    "reason": "AuthFailed",
                    "message": "401 from Keycloak",
                },
            ],
        }
        md = s.build_backend_metadata(status)
        assert md["failing_conditions"] == [
            {
                "type": "KeycloakGroupsReady",
                "status": "False",
                "reason": "AuthFailed",
                "message": "401 from Keycloak",
            }
        ]

    def test_role_bindings_summary(self):
        status = {
            "keycloakRoleBindings": [
                {
                    "groupName": "c_x_admin",
                    "keycloakGroupId": "kg-1",
                    "rancherBindingId": "rb-1",
                    "memberCount": 3,
                    "syncedMembers": ["a", "b", "c"],
                }
            ]
        }
        md = s.build_backend_metadata(status)
        assert md["role_bindings"] == [
            {
                "group_name": "c_x_admin",
                "keycloak_group_id": "kg-1",
                "rancher_binding_id": "rb-1",
                "member_count": 3,
            }
        ]

    def test_empty_status_returns_empty_dict(self):
        assert s.build_backend_metadata({}) == {}


# ---------------------------------------------------------------------
# is_terminal_failure
# ---------------------------------------------------------------------


class TestIsTerminalFailure:
    def test_returns_none_when_phase_not_error(self):
        assert s.is_terminal_failure({"phase": "Ready"}) is None
        assert s.is_terminal_failure({"phase": "Pending"}) is None

    def test_returns_aggregated_messages_when_failed(self):
        status = {
            "phase": "Error",
            "conditions": [
                {"status": "False", "message": "Rancher 500"},
                {"status": "False", "message": "Keycloak 401"},
            ],
        }
        assert s.is_terminal_failure(status) == "Rancher 500; Keycloak 401"

    def test_fallback_message_when_no_condition_messages(self):
        assert (
            s.is_terminal_failure({"phase": "Error"})
            == "Operator reports Error phase"
        )
