"""Backend tests with the LdapClient and Waldur SDK fully mocked.

These exercise pull_resource end-to-end through the translator and
the LdapClient surface, asserting the right add / remove calls are
made for given Waldur fixtures. They run without a live LDAP server
or Waldur API.
"""

from __future__ import annotations

import uuid as uuid_lib
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from waldur_site_agent_ldap_roles.backend import LdapRolesBackend

from waldur_site_agent.backend.exceptions import BackendError

# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _make_resource(
    uid_hex: str = "deadbeefdeadbeefdeadbeefdeadbeef", slug: str = "cluster1"
) -> MagicMock:
    r = MagicMock()
    r.uuid = uuid_lib.UUID(uid_hex)
    r.slug = slug
    r.customer_slug = "acme"
    r.project_slug = "alpha"
    return r


def _make_rp(uid_hex: str, name: str = "RP-1") -> MagicMock:
    rp = MagicMock()
    rp.uuid = uuid_lib.UUID(uid_hex)
    rp.name = name
    return rp


def _make_user_role(
    role: str, username: str, user_uuid_hex: Optional[str] = None
) -> MagicMock:
    u = MagicMock()
    u.role_name = role
    u.user_username = username
    u.user_uuid = uuid_lib.UUID(user_uuid_hex) if user_uuid_hex else None
    return u


def _make_settings(**overrides: Any) -> dict:
    base = {
        "waldur_api_url": "https://waldur.example.com/api/",
        "waldur_api_token": "test-token",
        "resource_role_map": {"RESOURCE.ADMIN": "admin"},
        "resource_project_role_map": {
            "RESOURCE_PROJECT.ADMIN": "admin",
            "RESOURCE_PROJECT.MEMBER": "member",
        },
        "resource_group_template": "${resource_slug}_${role_name}",
        "resource_project_group_template": (
            "${resource_slug}_${rp_uuid_short}_${role_name}"
        ),
        "membership_type": "memberUid",
        "managed_by_tag": "waldur-site-agent",
        "ldap": {
            "uri": "ldap://example",
            "bind_dn": "cn=admin",
            "bind_password": "x",
            "base_dn": "dc=example,dc=com",
        },
    }
    base.update(overrides)
    return base


class _FakeLdap:
    """In-memory stand-in for LdapClient.

    Tracks groups, group memberships, and the set of "known" users so
    tests can assert on the post-sync state and on what calls were made.
    """

    def __init__(self, known_users: set[str]) -> None:
        self.known_users = set(known_users)
        self.groups: dict[str, set[str]] = {}  # group_name -> members
        self.descriptions: dict[str, list[str]] = {}  # group_name -> description values
        self.calls: list[tuple[str, tuple]] = []

    def _record(self, name: str, *args: Any) -> None:
        self.calls.append((name, args))

    def ping(self) -> bool:
        return True

    def user_exists(self, username: str) -> bool:
        self._record("user_exists", username)
        return username in self.known_users

    def group_exists(self, group_name: str) -> bool:
        return group_name in self.groups

    def create_project_group(
        self, group_name: str, extra_attributes: Optional[dict] = None
    ) -> int:
        self._record("create_project_group", group_name, extra_attributes)
        self.groups.setdefault(group_name, set())
        description = (extra_attributes or {}).get("description")
        self.descriptions.setdefault(group_name, [description] if description else [])
        return 12345

    def get_group_descriptions(self, group_name: str) -> Optional[list[str]]:
        if group_name not in self.groups:
            return None
        return list(self.descriptions.get(group_name, []))

    def add_group_description(self, group_name: str, value: str) -> None:
        self._record("add_group_description", group_name, value)
        values = self.descriptions.setdefault(group_name, [])
        if value not in values:
            values.append(value)

    def find_groups_by_description(self, value: str) -> list[str]:
        return sorted(name for name, values in self.descriptions.items() if value in values)

    def add_foreign_group(
        self, group_name: str, members: set[str], descriptions: Optional[list[str]] = None
    ) -> None:
        """Seed a group this backend did not create."""
        self.groups[group_name] = set(members)
        self.descriptions[group_name] = list(descriptions or [])

    def list_group_members(
        self, group_name: str, membership_type: str = "memberUid"
    ) -> list[str]:
        self._record("list_group_members", group_name, membership_type)
        return sorted(self.groups.get(group_name, set()))

    def add_user_to_group(
        self, group_name: str, username: str, membership_type: str = "memberUid"
    ) -> None:
        self._record("add_user_to_group", group_name, username, membership_type)
        self.groups.setdefault(group_name, set()).add(username)

    def remove_user_from_group(
        self, group_name: str, username: str, membership_type: str = "memberUid"
    ) -> None:
        self._record("remove_user_from_group", group_name, username, membership_type)
        self.groups.get(group_name, set()).discard(username)


@pytest.fixture
def fake_ldap():
    return _FakeLdap(known_users={"alice", "bob", "carol"})


@pytest.fixture
def backend(fake_ldap):
    """Backend with the LdapClient swapped for a fake.

    Patches LdapClient at the module where the backend imports it so
    the constructor uses the fake. The Waldur SDK is patched per-test.
    """
    with patch("waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap):
        b = LdapRolesBackend(_make_settings(), backend_components={})
        # Override waldur_client check — pull_resource bails when None.
        b.waldur_client = MagicMock()
        yield b


# ---------------------------------------------------------------------
# pull_resource
# ---------------------------------------------------------------------


@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list_users_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resources_list_users_list")
class TestPullResource:
    def test_creates_groups_for_both_scopes(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        backend,
        fake_ldap,
    ):
        # Resource-scope role: alice is RESOURCE.ADMIN on the resource.
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice"),
        ]
        # One ResourceProject with bob as RESOURCE_PROJECT.MEMBER.
        rp = _make_rp("abcd1234deadbeef0000000000000000", "RP-1")
        mock_rp_list.sync_all.return_value = [rp]
        mock_rp_users.sync_all.return_value = [
            _make_user_role("RESOURCE_PROJECT.MEMBER", "bob"),
        ]

        result = backend.pull_resource(_make_resource())

        assert result is not None
        assert "cluster1_admin" in fake_ldap.groups
        assert fake_ldap.groups["cluster1_admin"] == {"alice"}
        assert "cluster1_abcd1234_member" in fake_ldap.groups
        assert fake_ldap.groups["cluster1_abcd1234_member"] == {"bob"}
        # Synced users are reported back.
        assert set(result.users) == {"alice", "bob"}

    def test_idempotent(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        backend,
        fake_ldap,
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())
        # Reset call log; second pull should not add or remove.
        fake_ldap.calls.clear()
        backend.pull_resource(_make_resource())

        adds = [c for c in fake_ldap.calls if c[0] == "add_user_to_group"]
        removes = [c for c in fake_ldap.calls if c[0] == "remove_user_from_group"]
        assert adds == []
        assert removes == []

    def test_user_removed_from_waldur_is_removed_from_group(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        backend,
        fake_ldap,
    ):
        # First cycle: alice + bob.
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice"),
            _make_user_role("RESOURCE.ADMIN", "bob"),
        ]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())
        assert fake_ldap.groups["cluster1_admin"] == {"alice", "bob"}

        # Second cycle: only alice.
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice"),
        ]
        backend.pull_resource(_make_resource())
        assert fake_ldap.groups["cluster1_admin"] == {"alice"}

    def test_skips_user_not_in_ldap(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        backend,
        fake_ldap,
    ):
        # "dave" is not in fake_ldap.known_users.
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice"),
            _make_user_role("RESOURCE.ADMIN", "dave"),
        ]
        mock_rp_list.sync_all.return_value = []

        result = backend.pull_resource(_make_resource())

        assert fake_ldap.groups["cluster1_admin"] == {"alice"}
        # dave is NOT in the synced users count.
        assert "dave" not in result.users
        assert "alice" in result.users

    def test_no_role_maps_skips_api_calls(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        fake_ldap,
    ):
        # Backend with empty role maps.
        with patch(
            "waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap
        ):
            b = LdapRolesBackend(
                _make_settings(resource_role_map={}, resource_project_role_map={}),
                backend_components={},
            )
            b.waldur_client = MagicMock()

        result = b.pull_resource(_make_resource())

        assert result is not None
        assert result.users == []
        # Neither SDK call was made — both scopes gated on role map.
        mock_resource_users.sync_all.assert_not_called()
        mock_rp_list.sync_all.assert_not_called()

    def test_no_waldur_client_returns_none(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        fake_ldap,
    ):
        with patch(
            "waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap
        ):
            b = LdapRolesBackend(_make_settings(), backend_components={})
            # Force the no-client branch.
            b.waldur_client = None

        assert b.pull_resource(_make_resource()) is None

    def test_role_outside_role_map_is_ignored(
        self,
        mock_resource_users,
        mock_rp_list,
        mock_rp_users,
        backend,
        fake_ldap,
    ):
        mock_resource_users.sync_all.return_value = [
            _make_user_role("UNMAPPED_ROLE", "alice"),
            _make_user_role("RESOURCE.ADMIN", "bob"),
        ]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())
        assert fake_ldap.groups == {"cluster1_admin": {"bob"}}


# ---------------------------------------------------------------------
# membership_type=member (DN-based)
# ---------------------------------------------------------------------


@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list_users_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resources_list_users_list")
def test_membership_type_member_passed_through(
    mock_resource_users, mock_rp_list, mock_rp_users, fake_ldap
):
    """When membership_type=member, every group op uses 'member'.

    The backend doesn't transform usernames itself; it forwards the
    membership_type to the client which decides whether to write a
    DN or a uid value. The test asserts the parameter is plumbed.
    """
    with patch("waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap):
        b = LdapRolesBackend(_make_settings(membership_type="member"), backend_components={})
        b.waldur_client = MagicMock()

    mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
    mock_rp_list.sync_all.return_value = []

    b.pull_resource(_make_resource())

    list_calls = [c for c in fake_ldap.calls if c[0] == "list_group_members"]
    assert list_calls and all(c[1][1] == "member" for c in list_calls)
    add_calls = [c for c in fake_ldap.calls if c[0] == "add_user_to_group"]
    assert add_calls and all(c[1][2] == "member" for c in add_calls)


# ---------------------------------------------------------------------
# add_user / remove_user route through pull_resource
# ---------------------------------------------------------------------


@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list_users_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resources_list_users_list")
class TestUserMutations:
    def test_add_user_calls_pull_resource(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend
    ):
        mock_resource_users.sync_all.return_value = []
        mock_rp_list.sync_all.return_value = []
        with patch.object(backend, "pull_resource", wraps=backend.pull_resource) as spy:
            ok = backend.add_user(_make_resource(), "alice")
        assert ok is True
        spy.assert_called_once()

    def test_remove_user_calls_pull_resource(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend
    ):
        mock_resource_users.sync_all.return_value = []
        mock_rp_list.sync_all.return_value = []
        with patch.object(backend, "pull_resource", wraps=backend.pull_resource) as spy:
            ok = backend.remove_user(_make_resource(), "alice")
        assert ok is True
        spy.assert_called_once()


def test_skips_the_flat_team_diff():
    """Membership is ResourceProject-scoped; the processor must not diff it against the team."""
    assert LdapRolesBackend.skip_resource_team_diff is True


# ---------------------------------------------------------------------
# Group ownership and revocation
# ---------------------------------------------------------------------

MARKER = "managed_by=waldur-site-agent;resource=deadbeefdeadbeefdeadbeefdeadbeef"
OTHER_MARKER = "managed_by=waldur-site-agent;resource=" + "0" * 32


def _build_backend(fake_ldap: _FakeLdap, **overrides: Any) -> LdapRolesBackend:
    with patch("waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap):
        b = LdapRolesBackend(_make_settings(**overrides), backend_components={})
    b.waldur_client = MagicMock()
    return b


@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list_users_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resources_list_users_list")
class TestOwnershipAndRevocation:
    def test_created_group_carries_the_ownership_marker(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())

        assert fake_ldap.descriptions["cluster1_admin"] == [MARKER]

    def test_revoking_the_last_holder_of_a_role_empties_its_group(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        mock_resource_users.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        # Emptied, not deleted: the GID must not be handed to the next group.
        assert fake_ldap.groups["cluster1_admin"] == set()

    def test_deleted_resource_project_has_its_groups_emptied(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = []
        mock_rp_list.sync_all.return_value = [_make_rp("abcd1234deadbeef0000000000000000")]
        mock_rp_users.sync_all.return_value = [
            _make_user_role("RESOURCE_PROJECT.MEMBER", "bob"),
            _make_user_role("RESOURCE_PROJECT.ADMIN", "carol"),
        ]
        backend.pull_resource(_make_resource())
        assert fake_ldap.groups["cluster1_abcd1234_member"] == {"bob"}

        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        assert fake_ldap.groups["cluster1_abcd1234_member"] == set()
        assert fake_ldap.groups["cluster1_abcd1234_admin"] == set()

    def test_role_removed_from_the_map_has_its_group_emptied(
        self, mock_resource_users, mock_rp_list, mock_rp_users, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        _build_backend(fake_ldap).pull_resource(_make_resource())

        _build_backend(fake_ldap, resource_role_map={"RESOURCE.OPERATOR": "admin"}).pull_resource(
            _make_resource()
        )

        assert fake_ldap.groups["cluster1_admin"] == set()

    def test_unmarked_existing_group_is_left_untouched(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        fake_ldap.add_foreign_group("cluster1_admin", {"root-admin"})
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        result = backend.pull_resource(_make_resource())

        assert fake_ldap.groups["cluster1_admin"] == {"root-admin"}
        assert fake_ldap.descriptions["cluster1_admin"] == []
        assert "alice" not in result.users

    def test_group_owned_by_another_resource_is_left_untouched(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        fake_ldap.add_foreign_group("cluster1_admin", {"bob"}, [OTHER_MARKER])
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())
        assert fake_ldap.groups["cluster1_admin"] == {"bob"}

        # Nor does this resource's revocation pass reach it.
        mock_resource_users.sync_all.return_value = []
        backend.pull_resource(_make_resource())
        assert fake_ldap.groups["cluster1_admin"] == {"bob"}

    def test_group_marked_by_hand_is_adopted(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        fake_ldap.add_foreign_group("cluster1_admin", {"root-admin"}, ["by hand", MARKER])
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())

        assert fake_ldap.groups["cluster1_admin"] == {"alice"}

    def test_failed_waldur_read_changes_nothing(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        mock_rp_list.sync_all.side_effect = RuntimeError("Waldur returned 503")
        with pytest.raises(RuntimeError):
            backend.pull_resource(_make_resource())

        assert fake_ldap.groups["cluster1_admin"] == {"alice"}


# ---------------------------------------------------------------------
# Group object classes vs membership_type
# ---------------------------------------------------------------------


def _client_settings(fake_ldap: _FakeLdap, **overrides: Any) -> dict:
    """The ldap settings the backend hands to LdapClient."""
    with patch(
        "waldur_site_agent_ldap_roles.backend.LdapClient", return_value=fake_ldap
    ) as client_cls:
        LdapRolesBackend(_make_settings(**overrides), backend_components={})
    return client_cls.call_args.args[0]


def test_member_mode_defaults_to_group_of_names(fake_ldap):
    settings = _client_settings(fake_ldap, membership_type="member")
    assert settings["project_group_object_classes"] == ["groupOfNames", "top"]


def test_member_uid_mode_keeps_the_client_default(fake_ldap):
    assert "project_group_object_classes" not in _client_settings(fake_ldap)


def test_member_mode_accepts_group_of_names_alongside_posix_group(fake_ldap):
    classes = ["groupOfNames", "posixGroup", "top"]
    ldap = {**_make_settings()["ldap"], "project_group_object_classes": classes}
    settings = _client_settings(fake_ldap, membership_type="member", ldap=ldap)
    assert settings["project_group_object_classes"] == classes


@pytest.mark.parametrize(
    ("membership_type", "classes"),
    [("member", ["posixGroup", "top"]), ("memberUid", ["groupOfNames", "top"])],
)
def test_classes_without_the_membership_attribute_are_refused(
    fake_ldap, membership_type, classes
):
    ldap = {**_make_settings()["ldap"], "project_group_object_classes": classes}
    with pytest.raises(BackendError, match="project_group_object_classes"):
        _client_settings(fake_ldap, membership_type=membership_type, ldap=ldap)


# ---------------------------------------------------------------------
# Per-grant membership sync report
# ---------------------------------------------------------------------

RP_UUID = "abcd1234deadbeef0000000000000000"


def _states(report: Optional[list[dict]]) -> dict[tuple, str]:
    """(user, scope_type, role_name) -> state, for compact assertions."""
    assert report is not None
    return {
        (e.get("username") or e.get("user_uuid"), e["scope_type"], e["role_name"]): e["state"]
        for e in report
    }


@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list_users_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resource_projects_list")
@patch("waldur_site_agent_ldap_roles.backend.marketplace_provider_resources_list_users_list")
class TestMembershipSyncReport:
    def test_no_report_before_a_pull(self, mock_resource_users, mock_rp_list, mock_rp_users, backend):
        assert backend.get_membership_sync_report(_make_resource()) is None

    def test_reports_each_mapped_grant(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice"),
            _make_user_role("RESOURCE.UNMAPPED", "bob"),
        ]
        mock_rp_list.sync_all.return_value = [_make_rp(RP_UUID)]
        mock_rp_users.sync_all.return_value = [
            _make_user_role("RESOURCE_PROJECT.MEMBER", "bob"),
            _make_user_role("RESOURCE_PROJECT.MEMBER", "dave"),  # no LDAP entry
        ]

        result = backend.pull_resource(_make_resource())
        report = backend.get_membership_sync_report(_make_resource())

        # The unmapped role reaches no group and is not reported at all.
        assert _states(report) == {
            ("alice", "resource", "RESOURCE.ADMIN"): "synced",
            ("bob", "resource_project", "RESOURCE_PROJECT.MEMBER"): "synced",
            ("dave", "resource_project", "RESOURCE_PROJECT.MEMBER"): "missing_in_idp",
        }
        by_user = {e["username"]: e for e in report}
        assert "resource_project_uuid" not in by_user["alice"]
        assert by_user["bob"]["resource_project_uuid"] == RP_UUID
        assert by_user["dave"]["message"].startswith("No LDAP entry")
        assert sorted(result.users) == ["alice", "bob"]

    def test_unowned_group_reports_an_error(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        fake_ldap.add_foreign_group("cluster1_admin", {"root-admin"})
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())

        (entry,) = backend.get_membership_sync_report(_make_resource())
        assert entry["state"] == "error"
        assert "cluster1_admin" in entry["message"]
        assert "not managed by the agent" in entry["message"]

    def test_failed_add_reports_an_error(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        with patch.object(
            fake_ldap, "add_user_to_group", side_effect=BackendError("insufficientAccessRights")
        ):
            result = backend.pull_resource(_make_resource())

        (entry,) = backend.get_membership_sync_report(_make_resource())
        assert entry["state"] == "error"
        assert "insufficientAccessRights" in entry["message"]
        assert result.users == []

    def test_revoked_grant_leaves_the_report(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        mock_resource_users.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        # An empty report, not None: Waldur replaces the rows, dropping alice's.
        assert backend.get_membership_sync_report(_make_resource()) == []

    def test_failed_pull_clears_the_previous_report(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []
        backend.pull_resource(_make_resource())

        mock_rp_list.sync_all.side_effect = RuntimeError("Waldur returned 503")
        with pytest.raises(RuntimeError):
            backend.pull_resource(_make_resource())

        assert backend.get_membership_sync_report(_make_resource()) is None

    def test_uuid_lookup_reports_user_uuid(
        self, mock_resource_users, mock_rp_list, mock_rp_users, fake_ldap
    ):
        user_uuid = "11111111111111111111111111111111"
        mock_resource_users.sync_all.return_value = [
            _make_user_role("RESOURCE.ADMIN", "alice", user_uuid)
        ]
        mock_rp_list.sync_all.return_value = []
        b = _build_backend(fake_ldap, lookup_by_user_uuid=True)

        b.pull_resource(_make_resource())

        (entry,) = b.get_membership_sync_report(_make_resource())
        assert entry["user_uuid"] == user_uuid
        assert "username" not in entry

    def test_failed_user_lookup_reports_an_error_not_a_missing_user(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        with patch.object(fake_ldap, "user_exists", side_effect=BackendError("timed out")):
            backend.pull_resource(_make_resource())

        (entry,) = backend.get_membership_sync_report(_make_resource())
        assert entry["state"] == "error"
        assert "timed out" in entry["message"]

    def test_group_is_created_with_its_marker_in_one_write(
        self, mock_resource_users, mock_rp_list, mock_rp_users, backend, fake_ldap
    ):
        mock_resource_users.sync_all.return_value = [_make_user_role("RESOURCE.ADMIN", "alice")]
        mock_rp_list.sync_all.return_value = []

        backend.pull_resource(_make_resource())

        assert ("create_project_group", ("cluster1_admin", {"description": MARKER})) in (
            fake_ldap.calls
        )
        assert not [c for c in fake_ldap.calls if c[0] == "add_group_description"]


# ---------------------------------------------------------------------
# Order processing: create and terminate
# ---------------------------------------------------------------------


class TestResourceLifecycle:
    def test_create_returns_the_backend_id_without_touching_ldap(self, backend, fake_ldap):
        info = backend.create_resource_with_id(_make_resource(), "cluster1", {})

        assert info.backend_id == "cluster1"
        assert fake_ldap.calls == []

    def test_terminate_empties_only_the_resource_groups(self, backend, fake_ldap):
        fake_ldap.add_foreign_group("cluster1_admin", {"alice"}, [MARKER])
        fake_ldap.add_foreign_group("cluster1_abcd1234_member", {"bob"}, [MARKER])
        fake_ldap.add_foreign_group("other_admin", {"carol"}, [OTHER_MARKER])
        fake_ldap.add_foreign_group("unmarked", {"dave"})

        assert backend.delete_resource(_make_resource()) is None

        assert fake_ldap.groups["cluster1_admin"] == set()
        assert fake_ldap.groups["cluster1_abcd1234_member"] == set()
        assert fake_ldap.groups["other_admin"] == {"carol"}
        assert fake_ldap.groups["unmarked"] == {"dave"}

    def test_terminate_raises_when_the_groups_cannot_be_listed(self, backend, fake_ldap):
        with patch.object(
            fake_ldap, "find_groups_by_description", side_effect=BackendError("down")
        ), pytest.raises(BackendError, match="down"):
            backend.delete_resource(_make_resource())

    def test_terminate_raises_when_a_member_cannot_be_removed(self, backend, fake_ldap):
        fake_ldap.add_foreign_group("cluster1_admin", {"alice"}, [MARKER])

        with patch.object(
            fake_ldap, "remove_user_from_group", side_effect=BackendError("denied")
        ), pytest.raises(BackendError, match="cluster1_admin"):
            backend.delete_resource(_make_resource())
