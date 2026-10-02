"""Decision logic for Waldur-authoritative reconciliation, with no LDAP in sight.

Every interesting case lives in build_desired/classify, so these are plain
functions over a dataclass and a dict.
"""

from types import SimpleNamespace

import pytest
from waldur_api_client.types import UNSET

from waldur_site_agent_ldap.reconcile import (
    Outcome,
    SkipReason,
    build_desired,
    classify,
    index_by_mail,
    index_by_uid_number,
)

DEFAULTS = {"default_home_base": "/home", "default_login_shell": "/bin/bash"}


def offering_user(**overrides):
    """An OfferingUser-shaped object; the code only ever getattrs it."""
    attrs = {
        "uuid": "ou-1",
        "username": "jsmith",
        "uidnumber": 10001,
        "primarygroup": 20001,
        "home_directory": "/home/jsmith",
        "login_shell": "/bin/bash",
        "user_first_name": "John",
        "user_last_name": "Smith",
        "user_email": "john@example.com",
        "user_username": "cuid-123",
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


def ldap_entry(**overrides):
    """An ldap3 entry_attributes_as_dict, which lists every value."""
    entry = {
        "uid": ["jsmith"],
        "uidNumber": [10001],
        "gidNumber": [20001],
        "homeDirectory": ["/home/jsmith"],
        "loginShell": ["/bin/bash"],
        "cn": ["John Smith"],
        "mail": ["john@example.com"],
        "givenName": ["John"],
        "sn": ["Smith"],
    }
    entry.update(overrides)
    return entry


class TestBuildDesired:
    def test_takes_every_value_from_waldur(self):
        desired, reason = build_desired(offering_user(), **DEFAULTS)
        assert reason is None
        assert (desired.username, desired.uid_number, desired.gid_number) == (
            "jsmith",
            10001,
            20001,
        )
        assert desired.home_directory == "/home/jsmith"
        assert desired.login_shell == "/bin/bash"

    def test_no_username_is_not_an_error(self):
        desired, reason = build_desired(offering_user(username=""), **DEFAULTS)
        assert desired is None
        assert reason == SkipReason.NO_USERNAME

    def test_unset_ids_are_distinguished_from_missing_ones(self):
        # UNSET means the server never sent the fields: an offering-wide problem.
        desired, reason = build_desired(
            offering_user(uidnumber=UNSET, primarygroup=UNSET), **DEFAULTS
        )
        assert desired is None
        assert reason == SkipReason.IDS_UNSET

    def test_null_ids_are_a_per_account_problem(self):
        desired, reason = build_desired(
            offering_user(uidnumber=None, primarygroup=None), **DEFAULTS
        )
        assert desired is None
        assert reason == SkipReason.IDS_MISSING

    def test_a_single_missing_id_is_still_missing(self):
        desired, reason = build_desired(offering_user(primarygroup=None), **DEFAULTS)
        assert desired is None
        assert reason == SkipReason.IDS_MISSING

    @pytest.mark.parametrize("empty", [None, "", UNSET])
    def test_home_and_shell_fall_back_to_defaults(self, empty):
        desired, _ = build_desired(
            offering_user(home_directory=empty, login_shell=empty), **DEFAULTS
        )
        assert desired.home_directory == "/home/jsmith"
        assert desired.login_shell == "/bin/bash"

    def test_ids_never_fall_back(self):
        # The whole point of the mode: a locally invented id would reintroduce
        # the dual allocation it exists to remove.
        desired, reason = build_desired(offering_user(uidnumber=None), **DEFAULTS)
        assert desired is None

    def test_waldur_username_only_carried_when_an_attribute_is_configured(self):
        desired, _ = build_desired(offering_user(), **DEFAULTS)
        assert desired.waldur_username is None
        desired, _ = build_desired(
            offering_user(), waldur_username_attribute="employeeNumber", **DEFAULTS
        )
        assert desired.waldur_username == "cuid-123"

    def test_common_name_falls_back_to_the_login_name(self):
        desired, _ = build_desired(
            offering_user(user_first_name="", user_last_name=""), **DEFAULTS
        )
        assert desired.common_name == "jsmith"


class TestClassify:
    def desired(self, **overrides):
        entry, _ = build_desired(offering_user(**overrides), **DEFAULTS)
        return entry

    def test_case_1_create_when_absent(self):
        decision = classify(self.desired(), None)
        assert decision.outcome == Outcome.CREATE
        assert decision.duplicate_mail_owner is None

    def test_case_2_noop_when_identical(self):
        assert classify(self.desired(), ldap_entry()).outcome == Outcome.NOOP

    def test_case_3_update_profile_and_home(self):
        decision = classify(
            self.desired(),
            ldap_entry(homeDirectory=["/old/jsmith"], mail=["stale@example.com"]),
        )
        assert decision.outcome == Outcome.UPDATE
        assert decision.updates["homeDirectory"] == "/home/jsmith"
        assert decision.updates["mail"] == "john@example.com"
        assert "uidNumber" not in decision.updates

    def test_case_4_drift_on_uid(self):
        decision = classify(self.desired(), ldap_entry(uidNumber=[999]))
        assert decision.outcome == Outcome.DRIFT
        assert decision.diff == {"uidNumber": (999, 10001)}

    def test_case_4_drift_on_gid(self):
        decision = classify(self.desired(), ldap_entry(gidNumber=[999]))
        assert decision.outcome == Outcome.DRIFT
        assert decision.diff == {"gidNumber": (999, 20001)}

    def test_drift_outranks_a_profile_difference(self):
        # Never quietly rewrite the profile of an entry whose identity is wrong.
        decision = classify(
            self.desired(), ldap_entry(uidNumber=[999], mail=["stale@example.com"])
        )
        assert decision.outcome == Outcome.DRIFT
        assert decision.updates == {}

    def test_case_5_uid_taken_by_another_entry(self):
        decision = classify(self.desired(), None, uid_owner="someone_else")
        assert decision.outcome == Outcome.UID_TAKEN
        assert decision.uid_taken_by == "someone_else"

    def test_drifted_uid_is_not_adopted_onto_another_entrys_uid(self):
        # The account exists but its uidNumber drifted, and the value Waldur wants
        # is already on somebody else. Renumbering onto it would leave two accounts
        # sharing every file they own, and LDAP would not stop us -- so this is
        # refused outright rather than handed to on_posix_mismatch.
        decision = classify(
            self.desired(), ldap_entry(uidNumber=[999]), uid_owner="someone_else"
        )
        assert decision.outcome == Outcome.UID_TAKEN
        assert decision.uid_taken_by == "someone_else"

    def test_drift_still_reported_when_the_wanted_uid_is_free(self):
        # Guard against the collision check swallowing ordinary drift.
        decision = classify(self.desired(), ldap_entry(uidNumber=[999]), uid_owner=None)
        assert decision.outcome == Outcome.DRIFT
        assert decision.diff == {"uidNumber": (999, 10001)}

    def test_uid_owner_pointing_at_this_entry_is_not_a_collision(self):
        # The index maps uidNumber -> uid, so an entry that already holds the
        # wanted UID resolves to itself. Only a gid drift remains.
        decision = classify(
            self.desired(), ldap_entry(gidNumber=[999]), uid_owner="jsmith"
        )
        assert decision.outcome == Outcome.DRIFT
        assert decision.diff == {"gidNumber": (999, 20001)}

    def test_gid_only_drift_is_not_blocked_by_an_unrelated_uid_owner(self):
        # Nothing is renumbering the uidNumber here, so a uid_owner is irrelevant.
        decision = classify(
            self.desired(), ldap_entry(gidNumber=[999]), uid_owner="someone_else"
        )
        assert decision.outcome == Outcome.DRIFT
        assert decision.diff == {"gidNumber": (999, 20001)}

    def test_case_5_outranks_creation(self):
        decision = classify(
            self.desired(), None, uid_owner="someone_else", mail_owner="someone_else"
        )
        assert decision.outcome == Outcome.UID_TAKEN

    def test_our_own_uid_is_not_a_collision(self):
        decision = classify(self.desired(), None, uid_owner="jsmith")
        assert decision.outcome == Outcome.CREATE

    def test_case_6_duplicate_mail_warns_but_creates(self):
        decision = classify(self.desired(), None, mail_owner="jsmith_staff")
        assert decision.outcome == Outcome.CREATE
        assert decision.duplicate_mail_owner == "jsmith_staff"

    def test_missing_attribute_reads_as_absent_not_as_a_value(self):
        # ldap3 returns [] for a present-but-empty attribute.
        decision = classify(self.desired(), ldap_entry(homeDirectory=[]))
        assert decision.outcome == Outcome.UPDATE
        assert decision.updates == {"homeDirectory": "/home/jsmith"}

    def test_waldur_username_attribute_is_synced_when_configured(self):
        entry, _ = build_desired(
            offering_user(), waldur_username_attribute="employeeNumber", **DEFAULTS
        )
        decision = classify(entry, ldap_entry(), waldur_username_attribute="employeeNumber")
        assert decision.updates == {"employeeNumber": "cuid-123"}


class TestIndexes:
    def test_index_by_uid_number(self):
        users = {"a": ldap_entry(uidNumber=[1]), "b": ldap_entry(uidNumber=[2])}
        assert index_by_uid_number(users) == {1: "a", 2: "b"}

    def test_entries_without_a_uid_number_are_skipped(self):
        assert index_by_uid_number({"a": ldap_entry(uidNumber=[])}) == {}

    def test_index_by_mail_is_case_insensitive(self):
        users = {"a": ldap_entry(mail=["John@Example.com"])}
        assert index_by_mail(users) == {"john@example.com": "a"}


class TestRenameOrCollision:
    """A taken UID is a rename only through the Waldur-username key, with the uidNumber."""

    ATTR = "employeeNumber"

    def desired(self, **overrides):
        entry, _ = build_desired(
            offering_user(username="jsmith2", home_directory="/home/jsmith2", **overrides),
            **DEFAULTS,
            waldur_username_attribute=self.ATTR,
        )
        return entry

    def classify(self, desired, directory, key_owners, uid_owner="jsmith"):
        return classify(
            desired,
            directory.get(desired.username),
            uid_owner=uid_owner,
            waldur_username_attribute=self.ATTR,
            key_owners=key_owners,
            directory=directory,
        )

    def test_the_key_under_another_name_with_the_same_uid_is_a_rename(self):
        directory = {"jsmith": ldap_entry(employeeNumber=["cuid-123"])}
        decision = self.classify(self.desired(), directory, ["jsmith"])
        assert decision.outcome == Outcome.RENAME
        assert decision.uid_taken_by == "jsmith"

    def test_mail_alone_is_not_evidence(self):
        directory = {"jsmith": ldap_entry()}  # same mail, no key
        decision = self.classify(self.desired(), directory, [])
        assert decision.outcome == Outcome.UID_TAKEN
        assert "no entry carries" in decision.reason

    def test_another_key_on_the_holder_is_a_collision(self):
        directory = {"jsmith": ldap_entry(employeeNumber=["someone-else"])}
        assert self.classify(self.desired(), directory, []).outcome == Outcome.UID_TAKEN

    def test_two_entries_with_the_key_is_a_collision(self):
        directory = {
            "jsmith": ldap_entry(employeeNumber=["cuid-123"]),
            "other": ldap_entry(uid=["other"], uidNumber=[777], employeeNumber=["cuid-123"]),
        }
        decision = self.classify(self.desired(), directory, ["jsmith", "other"])
        assert decision.outcome == Outcome.UID_TAKEN
        assert "all carry" in decision.reason

    def test_the_key_on_an_entry_with_another_uid_is_a_collision(self):
        directory = {
            "jsmith": ldap_entry(),
            "old": ldap_entry(uid=["old"], uidNumber=[555], employeeNumber=["cuid-123"]),
        }
        decision = self.classify(self.desired(), directory, ["old"])
        assert decision.outcome == Outcome.UID_TAKEN
        assert "uidNumber" in decision.reason

    def test_the_key_elsewhere_with_the_uid_free_still_refuses(self):
        directory = {"old": ldap_entry(uid=["old"], uidNumber=[555], employeeNumber=["cuid-123"])}
        decision = self.classify(self.desired(), directory, ["old"], uid_owner=None)
        assert decision.outcome == Outcome.UID_TAKEN

    def test_without_the_attribute_it_is_a_collision(self):
        entry, _ = build_desired(offering_user(username="jsmith2"), **DEFAULTS)
        decision = classify(entry, None, uid_owner="jsmith", key_owners=None, directory={})
        assert decision.outcome == Outcome.UID_TAKEN
        assert "not configured" in decision.reason

    def test_a_matching_entry_is_stamped(self):
        directory = {"jsmith2": ldap_entry(uid=["jsmith2"], homeDirectory=["/home/jsmith2"])}
        decision = self.classify(self.desired(), directory, [], uid_owner="jsmith2")
        assert decision.outcome == Outcome.UPDATE
        assert decision.updates == {"employeeNumber": "cuid-123"}

    def test_a_changed_waldur_username_is_restamped_on_an_id_match(self):
        directory = {
            "jsmith2": ldap_entry(
                uid=["jsmith2"], homeDirectory=["/home/jsmith2"], employeeNumber=["old-cuid"]
            )
        }
        decision = self.classify(self.desired(), directory, [], uid_owner="jsmith2")
        assert decision.outcome == Outcome.UPDATE
        assert decision.updates == {"employeeNumber": "cuid-123"}
        assert decision.restamped_from == "old-cuid"

    def test_an_old_value_that_is_another_current_accounts_key_is_a_conflict(self):
        directory = {
            "jsmith2": ldap_entry(
                uid=["jsmith2"], homeDirectory=["/home/jsmith2"], employeeNumber=["old-cuid"]
            )
        }
        decision = classify(
            self.desired(),
            directory["jsmith2"],
            uid_owner="jsmith2",
            waldur_username_attribute=self.ATTR,
            key_owners=[],
            directory=directory,
            current_keys={"cuid-123", "old-cuid"},
        )
        assert decision.outcome == Outcome.KEY_CONFLICT
        assert "old-cuid" in decision.reason

    def test_another_key_on_a_drifted_entry_is_a_conflict(self):
        directory = {
            "jsmith2": ldap_entry(uid=["jsmith2"], uidNumber=[999], employeeNumber=["other"])
        }
        decision = self.classify(self.desired(), directory, [], uid_owner=None)
        assert decision.outcome == Outcome.KEY_CONFLICT

    @pytest.mark.parametrize(
        "entry",
        [
            {"description": ["waldur-site-agent:disabled"]},  # would be re-enabled
            {"cn": ["Someone Else"]},  # would be updated
        ],
        ids=["reenable", "update"],
    )
    def test_another_current_accounts_key_blocks_every_write(self, entry):
        directory = {
            "jsmith2": ldap_entry(uid=["jsmith2"], employeeNumber=["other-cuid"], **entry)
        }
        decision = classify(
            self.desired(),
            directory["jsmith2"],
            uid_owner="jsmith2",
            waldur_username_attribute=self.ATTR,
            key_owners=[],
            directory=directory,
            current_keys={"cuid-123", "other-cuid"},
        )
        assert decision.outcome == Outcome.KEY_CONFLICT

    def test_the_key_is_not_copied_onto_a_second_entry(self):
        directory = {
            "jsmith2": ldap_entry(uid=["jsmith2"], homeDirectory=["/home/jsmith2"]),
            "jsmith": ldap_entry(uidNumber=[555], employeeNumber=["cuid-123"]),
        }
        decision = self.classify(self.desired(), directory, ["jsmith"], uid_owner="jsmith2")
        assert decision.outcome == Outcome.NOOP

    def test_a_drifted_entry_is_not_stamped(self):
        directory = {"jsmith2": ldap_entry(uid=["jsmith2"], uidNumber=[999])}
        decision = self.classify(self.desired(), directory, [], uid_owner=None)
        assert decision.outcome == Outcome.DRIFT
