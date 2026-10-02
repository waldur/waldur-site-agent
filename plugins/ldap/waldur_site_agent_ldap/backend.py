"""LDAP username management backend for Waldur Site Agent."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from pydantic import ValidationError as PydanticValidationError
from waldur_api_client.api.marketplace_offering_users import marketplace_offering_users_list
from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.models.offering_user_field_enum import OfferingUserFieldEnum
from waldur_api_client.types import UNSET
from waldur_site_agent_ldap_client import EntryExistsError, EntryMissingError, LdapClient
from waldur_site_agent_ldap_client.client import normalize_dn

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.backends import (
    LIVE_OFFERING_USER_STATES,
    AbstractUsernameManagementBackend,
)
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_ldap import project_groups, reconcile
from waldur_site_agent_ldap.email_sender import WelcomeEmailSender
from waldur_site_agent_ldap.schemas import (
    AccountSource,
    DeparturePolicy,
    LdapSettingsSchema,
    MissingPosixIdsPolicy,
    PosixMismatchPolicy,
)

# Parent DNs claimed, per process, by offerings that did not say which
# offerings the parent lists. One agent process serves one configuration, so
# this is where two offerings pointing at one parent can be noticed.
_PARENT_CLAIMS: dict[str, dict[str, frozenset[str]]] = {}
_WARNED_SETTINGS: set[str] = set()
_WARNED_PARENTS: set[tuple[str, frozenset[frozenset[str]]]] = set()
_WARNED_NO_RENAME_KEY: list[bool] = []


_WARNED_HIDDEN_USERNAMES: set[str] = set()


def _warn_once_about_hidden_waldur_usernames(offering_key: str, offering: object) -> None:
    """The key is the Waldur username; say once per offering when Waldur does not send it."""
    if offering_key in _WARNED_HIDDEN_USERNAMES:
        return
    _WARNED_HIDDEN_USERNAMES.add(offering_key)
    logger.warning(
        "Renames cannot be recognised on %s: the offering does not expose Waldur "
        "usernames (user_username comes back empty), so no key is written and every "
        "renamed account is reported as a collision. Expose usernames in the offering's "
        "user attribute configuration.",
        getattr(offering, "name", offering_key),
    )


def _warn_once_about_renames_without_a_key() -> None:
    """Renames need waldur_username_attribute; say once when it is not configured."""
    if _WARNED_NO_RENAME_KEY:
        return
    _WARNED_NO_RENAME_KEY.append(True)
    logger.warning(
        "waldur_username_attribute is not configured, so accounts renamed in Waldur "
        "cannot be recognised: their entries are reported as UID collisions and left "
        "alone. Set it (employeeNumber is the usual choice) to have renames followed."
    )


def _warn_about_shared_parents(offering_uuid: str, parents: list) -> None:
    """Warn once when offerings disagree about which offerings a shared parent lists.

    Each offering's agent reconciles the parent against its own *effective* set
    -- ``offering_uuids``, or just itself when unset. Two offerings pointing at
    one DN with different sets (both unset, or only one of them set) take out
    of the entry the groups the other adds, and it flaps every cycle.
    """
    me = project_groups.normalize_uuid(offering_uuid)
    for parent in parents:
        if not isinstance(parent, dict) or not parent.get("dn"):
            continue
        key = normalize_dn(str(parent["dn"]))
        effective = frozenset(
            project_groups.normalize_uuid(u) for u in parent.get("offering_uuids") or []
        ) or frozenset({me})
        claims = _PARENT_CLAIMS.setdefault(key, {})
        claims[me] = effective
        distinct = frozenset(claims.values())
        if len(distinct) > 1 and (key, distinct) not in _WARNED_PARENTS:
            _WARNED_PARENTS.add((key, distinct))
            logger.warning(
                "Parent entry %s is configured differently on offerings %s: each agent "
                "would remove the project groups the others add. Give it the same "
                "parents[].offering_uuids, listing all of them, on every offering.",
                parent["dn"],
                ", ".join(sorted(claims)),
            )


#: Recorded in an entry's description while a rename is between its modrdn
#: and the removal of the old name's memberships, so a failure in between is
#: finished on the next cycle instead of leaving the old name in groups.
PENDING_RENAME_PREFIX = "waldur-site-agent:renamed-from="

# Usernames the last account pass of each offering could not reconcile cleanly
# (a UID or key conflict, unadopted drift, a failure). Per process, because core
# builds a fresh backend for the profile sync and for the offering reconcile.
_CONFLICTED_ACCOUNTS: dict[str, set[str]] = {}


def _pending_token(old_username: str) -> str:
    return f"{PENDING_RENAME_PREFIX}{old_username}"


def _pending_old_name(value: str) -> Optional[str]:
    if value.startswith(PENDING_RENAME_PREFIX):
        return value[len(PENDING_RENAME_PREFIX) :] or None
    return None


@dataclass
class _PassState:
    """The in-memory directory one account pass works against, kept current as it writes."""

    existing: dict
    uid_index: dict
    mail_index: dict
    conflicted: set
    # Waldur usernames of every account in this batch: a key holding one of
    # them belongs to that account, never to be rewritten for another.
    current_keys: set = field(default_factory=set)
    counts: dict = field(
        default_factory=lambda: dict.fromkeys(
            ("created", "updated", "renamed", "skipped", "conflicts"), 0
        )
    )


class LdapUsernameBackend(AbstractUsernameManagementBackend):
    """Username management backend that provisions POSIX users in LDAP.

    Creates POSIX user entries (with personal groups unless ``personal_groups`` is
    off), writes the provider's project groups when ``project_groups`` is enabled,
    and handles user lifecycle in an LDAP directory. It does not manage SSH keys:
    a user's keys are not exposed on the offering-user list this backend reads,
    so it never sees them (#18).
    """

    def __init__(
        self,
        backend_settings: dict | None = None,
        offering: Optional[Offering] = None,
    ) -> None:
        """Initialize LDAP username backend from backend_settings."""
        super().__init__(backend_settings, offering)
        ldap_settings = (backend_settings or {}).get("ldap", {})
        if not ldap_settings:
            msg = (
                "LDAP settings are required for the LDAP username management backend. "
                "Add an 'ldap' section to backend_settings."
            )
            raise BackendError(msg)
        # Validate here rather than relying on core: validate_backend_settings_with_
        # plugin_schema keys on backend_type, and the realistic deployment sets
        # backend_type: slurm, whose schema is extra="allow" — so this plugin's
        # schema is never reached and every key passes unchecked. Validate for the
        # side effect only; the raw dict stays in use, so nothing downstream sees
        # pydantic enum instances where it expects strings.
        try:
            LdapSettingsSchema(**ldap_settings)
        except PydanticValidationError as e:
            msg = f"Invalid LDAP backend settings: {e}"
            raise BackendError(msg) from e
        self.client = LdapClient(ldap_settings)
        self.account_source = ldap_settings.get("account_source", AccountSource.LDAP.value)
        self.on_missing_posix_ids = ldap_settings.get(
            "on_missing_posix_ids", MissingPosixIdsPolicy.ERROR.value
        )
        self.on_posix_mismatch = ldap_settings.get(
            "on_posix_mismatch", PosixMismatchPolicy.REPORT.value
        )
        self.username_format = ldap_settings.get("username_format", "first_initial_lastname")
        # Unset follows the authority: a directory Waldur owns can be rebuilt from
        # Waldur, so a departed user's entry goes; a directory that named and
        # numbered its own accounts keeps them, as it always has.
        remove_user_on_deactivate = ldap_settings.get("remove_user_on_deactivate")
        self.remove_user_on_deactivate = (
            self.waldur_authoritative
            if remove_user_on_deactivate is None
            else bool(remove_user_on_deactivate)
        )
        on_departure = ldap_settings.get("on_departure")
        if on_departure is None:
            on_departure = (
                DeparturePolicy.DISABLE.value
                if self.waldur_authoritative
                else DeparturePolicy.DELETE.value
            )
        self.on_departure = str(on_departure)
        self.access_groups = ldap_settings.get("access_groups", [])
        self.personal_groups = ldap_settings.get("personal_groups", True) is not False
        self.project_groups = ldap_settings.get("project_groups") or {}
        if self.project_groups.get("enabled") and offering is not None:
            _warn_about_shared_parents(
                getattr(offering, "uuid", ""), self.project_groups.get("parents") or []
            )
        self.generate_vpn_password = ldap_settings.get("generate_vpn_password", False)
        self.waldur_username_attr = ldap_settings.get("waldur_username_attribute") or ""
        self._key_index: dict[str, list[str]] = {}

        welcome_email_settings = ldap_settings.get("welcome_email")
        self.email_sender = (
            WelcomeEmailSender(welcome_email_settings) if welcome_email_settings else None
        )

        # Set per instance rather than overriding the base class attribute with a
        # property: one class serves both directions, so the answer depends on this
        # offering's configuration.
        self.is_username_authoritative = not self.waldur_authoritative

        if self.waldur_authoritative:
            self._warn_about_ignored_settings(ldap_settings)

    @property
    def waldur_authoritative(self) -> bool:
        """Whether Waldur owns the username and POSIX ids for this offering."""
        return self.account_source == AccountSource.WALDUR.value

    def _warn_about_ignored_settings(self, ldap_settings: dict) -> None:
        """Say plainly which settings stop applying, and which hazard remains.

        Once per offering and process: core builds a fresh backend every cycle,
        and a startup warning repeated every few minutes stops being read.
        """
        key = str(getattr(self.offering, "uuid", "") or id(self))
        if key in _WARNED_SETTINGS:
            return
        _WARNED_SETTINGS.add(key)
        if "uid_range_start" in ldap_settings or "uid_range_end" in ldap_settings:
            logger.warning(
                "account_source is 'waldur', so uid_range_start/uid_range_end are "
                "ignored for user accounts: UIDs come from the offering user."
            )
        if self.offering is not None and not any(
            getattr(self.offering, name, "")
            for name in ("order_processing_backend", "membership_sync_backend")
        ):
            # No resource backend runs for this offering, so nothing allocates
            # group GIDs from the directory's range.
            return
        logger.warning(
            "account_source is 'waldur': user UIDs and primary GIDs come from Waldur, "
            "but *project* group GIDs are still allocated from gid_range %s-%s by the "
            "resource backend. Those ranges must not overlap the offering's POSIX ID "
            "pool - LDAP does not enforce gidNumber uniqueness, so an overlap silently "
            "yields two groups sharing a GID.",
            self.client.gid_range_start,
            self.client.gid_range_end,
        )

    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Check if a user already exists in LDAP.

        Searches by email first, then by Waldur username.
        """
        if self.waldur_authoritative:
            # Waldur named the account; there is nothing to look up. Deliberately
            # not the email-first search below: that can match an unrelated entry
            # and then serve its UID, and its fallback matches user_username (the
            # Waldur account name), which is a different value from the POSIX one.
            return getattr(offering_user, "username", None) or None

        email = getattr(offering_user, "user_email", None)
        if email:
            user_data = self.client.search_user_by_email(email)
            if user_data and user_data.get("uid"):
                uid_values = user_data["uid"]
                username = uid_values[0] if isinstance(uid_values, list) else uid_values
                logger.info(
                    "Found existing LDAP user %s for email %s",
                    username,
                    email,
                )
                return username

        waldur_username = getattr(offering_user, "user_username", None)
        if waldur_username and self.client.user_exists(waldur_username):
            logger.info("Found existing LDAP user by Waldur username %s", waldur_username)
            return waldur_username

        return None

    def generate_username(self, offering_user: OfferingUser) -> str:
        """Generate a username and create the POSIX user in LDAP."""
        if self.waldur_authoritative:
            # Unreachable in a correct setup - core skips username generation for a
            # non-authoritative backend - but minting a name here would contradict
            # the one Waldur holds, so fail loudly rather than quietly.
            msg = (
                "account_source is 'waldur': this backend does not generate usernames. "
                "Set the offering's username_generation_policy to something other than "
                "'service_provider', or set account_source to 'ldap'."
            )
            raise BackendError(msg)

        first_name = getattr(offering_user, "user_first_name", "") or ""
        last_name = getattr(offering_user, "user_last_name", "") or ""
        email = getattr(offering_user, "user_email", "") or ""
        waldur_username = getattr(offering_user, "user_username", "") or ""

        username = self._generate_username_string(first_name, last_name, offering_user)
        if not username:
            raise BackendError(
                f"Cannot generate username for offering user {offering_user.uuid}: "
                "insufficient user data (need first_name/last_name or user_username)"
            )

        # Ensure uniqueness (expand first name prefix before numeric suffix)
        first_name_clean = self._normalize_name(first_name)
        last_name_clean = self._normalize_name(last_name)
        username = self._ensure_unique_username(username, first_name_clean, last_name_clean)

        # Prepare optional fields
        password = None
        if self.generate_vpn_password:
            password = LdapClient.generate_random_password()

        # Create the POSIX user in LDAP
        # Optionally store the Waldur username (e.g. CUID) in a configured attribute
        extra_attributes = {}
        if self.waldur_username_attr and waldur_username:
            extra_attributes[self.waldur_username_attr] = waldur_username

        uid_number = self.client.create_user(
            username=username,
            first_name=first_name,
            last_name=last_name,
            email=email,
            password=password,
            extra_attributes=extra_attributes or None,
        )

        # Add user to configured access groups
        for group_config in self.access_groups:
            group_name = group_config["name"]
            membership_type = group_config.get("attribute", "memberUid")
            try:
                self.client.add_user_to_group(group_name, username, membership_type)
            except BackendError:
                logger.exception(
                    "Failed to add user %s to access group %s",
                    username,
                    group_name,
                )

        logger.info(
            "Created LDAP user %s for offering user %s",
            username,
            offering_user.uuid,
        )

        # Send welcome email (non-blocking — failure is logged but does not abort)
        if self.email_sender and email:
            self.email_sender.send_welcome_email(
                recipient_email=email,
                username=username,
                vpn_password=password or "",
                first_name=first_name,
                last_name=last_name,
                email=email,
                home_directory=f"{self.client.default_home_base}/{username}",
                login_shell=self.client.default_login_shell,
                uid_number=str(uid_number),
            )

        return username

    def sync_user_profiles(self, offering_users: list[OfferingUser]) -> None:
        """Bring the directory into line with Waldur.

        Two very different jobs behind one name, because this is the only hook
        core calls with the *full* offering-user list on every cycle — including
        accounts already in OK, which the username-generation path never sees.

        Under ``account_source: ldap`` it does what it always did: refresh profile
        attributes on entries the agent previously created. Under
        ``account_source: waldur`` it is the whole provisioning path — create,
        update, and report drift.
        """
        if self.waldur_authoritative:
            self._reconcile_from_waldur(offering_users)
        else:
            self._sync_profiles_legacy(offering_users)

    def reconcile_offering(self, waldur_rest_client: AuthenticatedClient) -> None:
        """Write the provider's project groups and their parent memberships.

        Core calls this once per periodic cycle, after sync_user_profiles (so a
        group's new members already have entries), and also when the offering
        has no offering users left -- the parents still need cleaning then.

        A failure to read Waldur leaves the directory untouched, and so does an
        empty answer (see ProjectGroupReconciler.run).
        """
        if not self.project_groups.get("enabled"):
            return
        offering_uuid = getattr(self.offering, "uuid", None)
        if not offering_uuid:
            logger.error("project_groups is enabled, but the offering UUID is unknown")
            return
        try:
            items = project_groups.fetch_provider_project_groups(
                waldur_rest_client, offering_uuid
            )
        except Exception:
            logger.exception(
                "Could not read the project groups from Waldur; LDAP project groups "
                "were left unchanged this cycle"
            )
            return
        reconciler = project_groups.ProjectGroupReconciler(
            self.client,
            self.project_groups,
            offering_uuid,
            require_user_entries=self.waldur_authoritative,
            key_attribute=self.waldur_username_attr if self.waldur_authoritative else "",
            excluded_members=_CONFLICTED_ACCOUNTS.get(self._offering_key()),
        )
        try:
            report = reconciler.run([project_groups.ProjectGroup.from_api(i) for i in items])
        except BackendError:
            logger.exception("LDAP project group reconcile failed")
            return
        logger.info(
            "LDAP project groups: %d created, %d kept, %d marked, %d renumbered, "
            "%d member updates, %d parent updates, %d conflicts, %d skipped, %d failed",
            report.created,
            report.kept,
            report.marked,
            report.renumbered,
            report.member_updates,
            report.parent_updates,
            report.conflicts,
            report.skipped,
            report.failed,
        )

    def _reconcile_from_waldur(self, offering_users: list[OfferingUser]) -> None:
        """Converge the directory on Waldur's usernames and POSIX ids.

        Reads the directory once in total, not once per user: every LdapClient
        method opens and unbinds its own connection, so per-account lookups would
        cost thousands of binds a cycle on a large offering.

        Renames go first, so an account created or adopted later in the batch
        is classified against the directory they leave behind. Accounts this
        pass could not reconcile cleanly (a conflict, a failure) are remembered,
        so the project-group pass does not name them.
        """
        conflicted: set[str] = set()
        _CONFLICTED_ACCOUNTS[self._offering_key()] = conflicted
        if not offering_users:
            return

        existing = self.client.list_users()
        state = _PassState(
            existing=existing,
            uid_index=reconcile.index_by_uid_number(existing),
            mail_index=reconcile.index_by_mail(existing),
            conflicted=conflicted,
        )
        # Waldur username -> entries carrying it: the key renames are found by.
        self._key_index = reconcile.index_by_attribute(existing, self.waldur_username_attr)

        work: list[reconcile.DesiredEntry] = []
        unset_accounts = 0
        for offering_user in offering_users:
            account_state = offering_user.state
            if account_state and account_state not in LIVE_OFFERING_USER_STATES:
                # An account Waldur is tearing down, or has already torn down,
                # must not be converged back into existence by this loop; the
                # deletion states are handed to release_users by core instead.
                # An unset state means the field was never requested, which says
                # nothing about the account, so it is reconciled as before.
                logger.debug(
                    "Offering user %s is in state %s, not reconciling",
                    offering_user.username,
                    account_state,
                )
                continue
            desired, reason = reconcile.build_desired(
                offering_user,
                default_home_base=self.client.default_home_base,
                default_login_shell=self.client.default_login_shell,
                waldur_username_attribute=self.waldur_username_attr,
            )
            if desired is None:
                if reason == reconcile.SkipReason.IDS_UNSET:
                    # Offering-wide, not per-account: counted and reported once below.
                    unset_accounts += 1
                elif reason == reconcile.SkipReason.NO_USERNAME:
                    logger.debug(
                        "Offering user %s has no username yet, skipping",
                        getattr(offering_user, "uuid", "?"),
                    )
                else:
                    state.counts["skipped"] += 1
                    self._report_missing_ids(offering_user)
                continue
            work.append(desired)

        state.current_keys = {d.waldur_username for d in work if d.waldur_username}
        if self.waldur_username_attr and work and not state.current_keys:
            _warn_once_about_hidden_waldur_usernames(self._offering_key(), self.offering)

        later = []
        for desired in work:
            decision = self._classify(desired, state)
            if decision.outcome == reconcile.Outcome.RENAME:
                self._reconcile_one(desired, decision, state)
            else:
                later.append(desired)
        for desired in later:
            self._reconcile_one(desired, self._classify(desired, state), state)

        # Step (iii) of renames an earlier cycle could not finish.
        self._finish_pending_renames(existing)

        if unset_accounts:
            logger.error(
                "Waldur returned no POSIX attributes for any of %d offering users on %s. "
                "The server may predate the POSIX attribute API, or the agent may not be "
                "requesting those fields. No accounts were provisioned this cycle.",
                unset_accounts,
                self.offering.name if self.offering else "?",
            )
        counts = state.counts
        logger.info(
            "LDAP reconcile: %d created, %d updated, %d renamed, %d skipped, %d conflicts",
            counts["created"],
            counts["updated"],
            counts["renamed"],
            counts["skipped"],
            counts["conflicts"],
        )

    def _offering_key(self) -> str:
        return str(getattr(self.offering, "uuid", "") or id(self))

    def _reconcile_one(
        self, desired: reconcile.DesiredEntry, decision: reconcile.Decision, state: _PassState
    ) -> None:
        """Act on one account's decision and keep the in-memory directory current."""
        if decision.outcome == reconcile.Outcome.UID_TAKEN and not self.waldur_username_attr:
            _warn_once_about_renames_without_a_key()
        try:
            if decision.outcome == reconcile.Outcome.RENAME:
                self._rename(str(decision.uid_taken_by), desired, state)
                state.counts["renamed"] += 1
                # The entry now sits under the new name; whatever else differs
                # (home directory, profile, a parked state) is an ordinary update.
                decision = self._classify(desired, state)
            try:
                outcome = self._apply(decision, desired)
            except EntryExistsError:
                # Another writer (the STOMP event for this account, or a sibling
                # offering's agent) created it after our bulk read. Read it and
                # treat it as an existing entry.
                entry = self.client.search_user(desired.username)
                if entry is None:
                    raise
                logger.info(
                    "LDAP user %s was created concurrently by another writer; "
                    "reconciling the existing entry",
                    desired.username,
                )
                state.existing[desired.username] = entry
                decision = self._classify(desired, state)
                outcome = self._apply(decision, desired)
        except BackendError:
            logger.exception("Failed to reconcile LDAP account %s", desired.username)
            state.counts["skipped"] += 1
            state.conflicted.add(desired.username)
            return

        adopted = (
            outcome == reconcile.Outcome.DRIFT
            and self.on_posix_mismatch == PosixMismatchPolicy.ADOPT.value
        )
        if outcome == reconcile.Outcome.CREATE:
            state.counts["created"] += 1
            # Keep the in-memory indexes honest so a second account in the same
            # batch cannot be handed a UID this one just took.
            state.uid_index.setdefault(desired.uid_number, desired.username)
            state.existing.setdefault(desired.username, {"uid": [desired.username]})
            state.existing[desired.username]["uidNumber"] = [desired.uid_number]
            if self.waldur_username_attr and desired.waldur_username:
                self._key_index.setdefault(desired.waldur_username, []).append(desired.username)
        elif outcome in (reconcile.Outcome.UPDATE, reconcile.Outcome.REENABLE) or adopted:
            state.counts["updated"] += 1
            if adopted:
                # The entry now carries Waldur's ids; the indexes have to say so.
                for uid_number, holder in list(state.uid_index.items()):
                    if holder == desired.username:
                        del state.uid_index[uid_number]
                state.uid_index[desired.uid_number] = desired.username
                entry = state.existing.setdefault(desired.username, {"uid": [desired.username]})
                entry["uidNumber"] = [desired.uid_number]
                entry["gidNumber"] = [desired.gid_number]
        elif outcome in (
            reconcile.Outcome.DRIFT,
            reconcile.Outcome.UID_TAKEN,
            reconcile.Outcome.KEY_CONFLICT,
        ):
            state.counts["conflicts"] += 1
            state.conflicted.add(desired.username)

    def _classify(self, desired: reconcile.DesiredEntry, state: _PassState) -> reconcile.Decision:
        key_owners = None
        if self.waldur_username_attr:
            key_owners = list(self._key_index.get(desired.waldur_username or "", []))
        return reconcile.classify(
            desired,
            state.existing.get(desired.username),
            uid_owner=state.uid_index.get(desired.uid_number),
            mail_owner=state.mail_index.get(desired.email.lower()) if desired.email else None,
            waldur_username_attribute=self.waldur_username_attr,
            key_owners=key_owners,
            directory=state.existing,
            current_keys=state.current_keys,
        )

    def _is_this_account(self, entry: Optional[dict], desired: reconcile.DesiredEntry) -> bool:
        """Whether an entry read live is this account: its uidNumber and its key."""
        if not entry:
            return False
        if reconcile.attr(entry, "uidNumber") is None:
            return False
        if int(str(reconcile.attr(entry, "uidNumber"))) != desired.uid_number:
            return False
        if self.waldur_username_attr:
            key = reconcile.attr(entry, self.waldur_username_attr)
            return key is not None and str(key) == desired.waldur_username
        return True

    def _rename(
        self, old_username: str, desired: reconcile.DesiredEntry, state: _PassState
    ) -> None:
        """Move the entry to the new name in an order a crash at any point can resume.

        (i)   Record the old name on the entry as pending, add the new name to
              every groups_ou membership the old one has, and rename the
              personal group. Nothing has lost access; a failure here leaves
              the entry keyed under the old name and the rename is retried.
        (ii)  modrdn ``uid=<old>`` to ``uid=<new>``.
        (iii) Remove the old name's memberships, then the pending record. A
              failure here leaves the record on the renamed entry, and the next
              cycle finishes the cleanup from it (see _finish_pending_renames).

        The old entry is re-read live first: the bulk read may be minutes old,
        and the rename must still be of this account's entry.
        """
        new_username = desired.username
        entry = self.client.search_user(old_username)
        if entry is None:
            fresh = self.client.search_user(new_username)
            if not self._is_this_account(fresh, desired):
                msg = f"LDAP user {old_username} disappeared before it could be renamed"
                raise EntryMissingError(msg)
            logger.info(
                "LDAP user %s was renamed to %s concurrently by another writer",
                old_username,
                new_username,
            )
            self._renamed_in_memory(old_username, new_username, fresh or {}, desired, state)
            self._finish_rename(new_username, old_username)
            return
        if not self._is_this_account(entry, desired):
            msg = f"LDAP user {old_username} changed since it was read; not renaming it"
            raise BackendError(msg)

        # Step (i).
        self.client.add_user_attribute_value(
            old_username, "description", _pending_token(old_username)
        )
        memberships = self._group_memberships(old_username)
        personal_group_renamed = self._rename_personal_group(old_username, new_username)
        # A retried step (i) finds some of these done already.
        held = set(self._group_memberships(new_username))
        for group_name, membership_type in memberships:
            target = group_name
            if personal_group_renamed and group_name == old_username:
                target = new_username
            if (target, membership_type) not in held:
                self.client.add_user_to_group(target, new_username, membership_type)

        # Step (ii).
        moved_by_us = True
        try:
            self.client.rename_user(old_username, new_username)
        except (EntryMissingError, EntryExistsError):
            fresh = self.client.search_user(new_username)
            if not self._is_this_account(fresh, desired):
                raise
            logger.info(
                "LDAP user %s was renamed to %s concurrently by another writer",
                old_username,
                new_username,
            )
            moved_by_us = False
            entry = fresh or {}
        self._renamed_in_memory(old_username, new_username, entry, desired, state)

        # Step (iii).
        self._finish_rename(new_username, old_username)
        if moved_by_us:
            logger.warning(
                "Renamed LDAP user %s to %s (uid %d): Waldur renamed the account. Paths "
                "that embed the old name (a home directory, say) are not moved by the agent.",
                old_username,
                new_username,
                desired.uid_number,
            )

    def _renamed_in_memory(
        self,
        old_username: str,
        new_username: str,
        entry: dict,
        desired: reconcile.DesiredEntry,
        state: _PassState,
    ) -> None:
        moved = dict(entry or {})
        moved["uid"] = [new_username]
        state.existing.pop(old_username, None)
        state.existing[new_username] = moved
        state.uid_index[desired.uid_number] = new_username
        for owners in self._key_index.values():
            owners[:] = [new_username if o == old_username else o for o in owners]

    def _finish_rename(self, username: str, old_username: str) -> None:
        """Step (iii): drop the old name's memberships, then the pending record.

        A name another entry has meanwhile taken is left alone: its memberships
        are that account's now (the agent gives a new account its access groups
        itself).
        """
        if self.client.search_user(old_username) is not None:
            logger.info(
                "LDAP user %s exists again; leaving its memberships to it and clearing the "
                "pending rename record on %s",
                old_username,
                username,
            )
        else:
            for group_name, membership_type in self._group_memberships(old_username):
                self.client.remove_user_from_group(group_name, old_username, membership_type)
            if self.personal_groups and self.client.group_exists(old_username):
                # Left behind if the personal group was recreated or could not be
                # renamed; an orphan carrying the account's old name.
                logger.warning(
                    "LDAP group %s is left from before %s was renamed; remove it by hand "
                    "once nothing uses it",
                    old_username,
                    username,
                )
        self.client.remove_user_attribute_value(
            username, "description", _pending_token(old_username)
        )

    def _finish_pending_renames(self, existing: dict) -> None:
        """Retry step (iii) for every entry still carrying a pending rename record."""
        for username, entry in existing.items():
            for value in entry.get("description") or []:
                old_username = _pending_old_name(str(value))
                if not old_username or old_username == username:
                    # Not a record, or a rename not past step (ii) yet: the
                    # rename itself retries that.
                    continue
                try:
                    self._finish_rename(username, old_username)
                    logger.info("Finished the rename of LDAP user %s to %s", old_username, username)
                except BackendError:
                    logger.exception(
                        "Could not finish the rename of LDAP user %s to %s; retrying next cycle",
                        old_username,
                        username,
                    )

    def _rename_personal_group(self, old_username: str, new_username: str) -> bool:
        """Rename the personal group with the account; True when it now has the new name."""
        if not self.personal_groups:
            return False
        if not self.client.group_exists(old_username):
            # Already moved, by us earlier or by another writer, or never there.
            return self.client.group_exists(new_username)
        if self.client.group_exists(new_username):
            logger.warning(
                "LDAP group %s already exists; the personal group %s was not renamed",
                new_username,
                old_username,
            )
            return False
        try:
            self.client.rename_group(old_username, new_username)
        except (EntryMissingError, EntryExistsError):
            if not self.client.group_exists(new_username):
                raise
            logger.info(
                "LDAP group %s was renamed to %s concurrently by another writer",
                old_username,
                new_username,
            )
        return True

    def _report_missing_ids(self, offering_user: OfferingUser) -> None:
        """Complain, or not, about an account Waldur holds no ids for."""
        if self.on_missing_posix_ids == MissingPosixIdsPolicy.SKIP.value:
            logger.debug(
                "Offering user %s has no POSIX ids, skipping",
                getattr(offering_user, "username", "?"),
            )
            return
        logger.error(
            "Offering user %s has no UID/primary GID in Waldur, so no LDAP account was "
            "created. Attach a POSIX ID pool to the service provider, or enable POSIX "
            "accounts on the offering.",
            getattr(offering_user, "username", "?"),
        )

    def _apply(  # noqa: PLR0911
        self, decision: reconcile.Decision, desired: reconcile.DesiredEntry
    ) -> reconcile.Outcome:
        """Carry out one decision. Returns the outcome actually acted on."""
        if decision.outcome == reconcile.Outcome.NOOP:
            return decision.outcome

        if decision.outcome == reconcile.Outcome.UID_TAKEN:
            logger.error(
                "UID %d is already held by LDAP user %s, so %s was left unchanged%s. "
                "Resolve the collision in the directory or re-point the allocation "
                "in Waldur.",
                desired.uid_number,
                decision.uid_taken_by,
                desired.username,
                f" (not taken as a rename: {decision.reason})"
                if decision.reason
                else "",
            )
            return decision.outcome

        if decision.outcome == reconcile.Outcome.KEY_CONFLICT:
            logger.error(
                "LDAP user %s was left unchanged: %s. The entry belongs to someone else, "
                "or its %s is wrong; resolve it in the directory.",
                desired.username,
                decision.reason,
                self.waldur_username_attr,
            )
            return decision.outcome

        if decision.outcome == reconcile.Outcome.DRIFT:
            return self._apply_drift(decision, desired)

        if decision.outcome == reconcile.Outcome.REENABLE:
            # Same DN, same uid: the person is back on the identity Waldur kept
            # reserved for them. Wake the entry rather than fail on "exists".
            self.client.enable_user(desired.username, desired.login_shell)
            self._add_to_access_groups(desired.username)
            if decision.updates:
                self.client.update_user_attributes(desired.username, decision.updates)
                self._log_restamp(decision, desired)
            logger.info(
                "Re-enabled LDAP user %s (uid %d): restored shell %s, dropped expiry and "
                "marker, re-added access groups",
                desired.username,
                desired.uid_number,
                desired.login_shell,
            )
            return decision.outcome

        if decision.outcome == reconcile.Outcome.CREATE:
            if decision.duplicate_mail_owner:
                logger.warning(
                    "LDAP user %s already carries the address %s; creating %s alongside it",
                    decision.duplicate_mail_owner,
                    desired.email,
                    desired.username,
                )
            self._create_from_desired(desired)
            return decision.outcome

        self.client.update_user_attributes(desired.username, decision.updates)
        self._log_restamp(decision, desired)
        logger.info(
            "Updated LDAP user %s: %s",
            desired.username,
            ", ".join(sorted(decision.updates)),
        )
        return decision.outcome

    def _log_restamp(self, decision: reconcile.Decision, desired: reconcile.DesiredEntry) -> None:
        if not decision.restamped_from:
            return
        logger.info(
            "Re-stamped %s on LDAP user %s: %s -> %s (the account's Waldur username changed)",
            self.waldur_username_attr,
            desired.username,
            decision.restamped_from,
            desired.waldur_username,
        )
        owners = self._key_index.get(decision.restamped_from, [])
        if desired.username in owners:
            owners.remove(desired.username)
        self._key_index.setdefault(desired.waldur_username or "", []).append(desired.username)

    def _apply_drift(
        self, decision: reconcile.Decision, desired: reconcile.DesiredEntry
    ) -> reconcile.Outcome:
        """Handle an entry whose POSIX ids disagree with Waldur's."""
        summary = ", ".join(
            f"{name}: {actual} in LDAP, {wanted} in Waldur"
            for name, (actual, wanted) in sorted(decision.diff.items())
        )
        if self.on_posix_mismatch == PosixMismatchPolicy.FAIL.value:
            msg = f"POSIX identity mismatch for LDAP user {desired.username} ({summary})"
            raise BackendError(msg)
        if self.on_posix_mismatch != PosixMismatchPolicy.ADOPT.value:
            logger.error(
                "POSIX identity mismatch for LDAP user %s (%s). Nothing was changed - "
                "renumbering a live account orphans the files it owns. Set "
                "on_posix_mismatch to 'adopt' once the filesystem has been reconciled.",
                desired.username,
                summary,
            )
            return decision.outcome

        self.client.set_user_posix_attributes(
            desired.username,
            uid_number=desired.uid_number,
            gid_number=desired.gid_number,
            home_directory=desired.home_directory,
            login_shell=desired.login_shell,
        )
        # The personal group carries the primary GID too; leaving it behind would
        # make the account a member of a group that no longer matches its gidNumber.
        if not self.personal_groups:
            logger.warning(
                "Adopted Waldur POSIX ids for LDAP user %s (%s). Files owned by the old "
                "ids need chown-ing to the new ones.",
                desired.username,
                summary,
            )
            return decision.outcome
        try:
            self.client.set_group_gid(desired.username, desired.gid_number)
        except BackendError:
            logger.exception(
                "Adopted Waldur ids for LDAP user %s, but its personal group could not "
                "be updated; the group still holds the old GID",
                desired.username,
            )
        logger.warning(
            "Adopted Waldur POSIX ids for LDAP user %s (%s). Files owned by the old ids "
            "need chown-ing to the new ones.",
            desired.username,
            summary,
        )
        return decision.outcome

    def _create_from_desired(self, desired: reconcile.DesiredEntry) -> None:
        """Create the account, its access-group memberships and its welcome mail."""
        password = LdapClient.generate_random_password() if self.generate_vpn_password else None
        extra_attributes = {}
        if self.waldur_username_attr and desired.waldur_username:
            extra_attributes[self.waldur_username_attr] = desired.waldur_username

        self.client.create_user_with_ids(
            username=desired.username,
            first_name=desired.first_name,
            last_name=desired.last_name,
            email=desired.email,
            uid_number=desired.uid_number,
            gid_number=desired.gid_number,
            home_directory=desired.home_directory,
            login_shell=desired.login_shell,
            password=password,
            extra_attributes=extra_attributes or None,
        )

        for group_config in self.access_groups:
            group_name = group_config["name"]
            membership_type = group_config.get("attribute", "memberUid")
            try:
                self.client.add_user_to_group(group_name, desired.username, membership_type)
            except BackendError:
                logger.exception(
                    "Failed to add user %s to access group %s", desired.username, group_name
                )

        if self.email_sender and desired.email:
            self.email_sender.send_welcome_email(
                recipient_email=desired.email,
                username=desired.username,
                vpn_password=password or "",
                first_name=desired.first_name,
                last_name=desired.last_name,
                email=desired.email,
                home_directory=desired.home_directory,
                login_shell=desired.login_shell,
                uid_number=str(desired.uid_number),
            )

    def _sync_profiles_legacy(self, offering_users: list[OfferingUser]) -> None:
        """Update user attributes in LDAP from Waldur profiles."""
        for offering_user in offering_users:
            username = getattr(offering_user, "username", None)
            if not username:
                continue

            if not self.client.user_exists(username):
                logger.warning(
                    "LDAP user %s not found during profile sync, skipping",
                    username,
                )
                continue

            first_name = getattr(offering_user, "user_first_name", None)
            last_name = getattr(offering_user, "user_last_name", None)
            email = getattr(offering_user, "user_email", None)
            waldur_username = getattr(offering_user, "user_username", None)

            updates = {}
            if first_name:
                updates["givenName"] = first_name
            if last_name:
                updates["sn"] = last_name
            if first_name and last_name:
                updates["cn"] = f"{first_name} {last_name}"
            if email:
                updates["mail"] = email
            if waldur_username and self.waldur_username_attr:
                updates[self.waldur_username_attr] = waldur_username

            if updates:
                try:
                    self.client.update_user_attributes(username, updates)
                except BackendError:
                    logger.exception(
                        "Failed to sync profile for LDAP user %s",
                        username,
                    )

    def deactivate_users(self, usernames: set[str]) -> None:
        """Deactivate users no longer in the offering."""
        for username in usernames:
            if not self.client.user_exists(username):
                logger.info("LDAP user %s already absent, skipping deactivation", username)
                continue

            if self.remove_user_on_deactivate:
                try:
                    # Remove from all access groups first
                    for group_config in self.access_groups:
                        group_name = group_config["name"]
                        membership_type = group_config.get("attribute", "memberUid")
                        try:
                            self.client.remove_user_from_group(
                                group_name, username, membership_type
                            )
                        except BackendError:
                            logger.debug(
                                "User %s not in group %s, skipping removal",
                                username,
                                group_name,
                            )
                    self.client.delete_user(username)
                    logger.info("Deleted LDAP user %s", username)
                except BackendError:
                    logger.exception("Failed to delete LDAP user %s", username)
            else:
                logger.info(
                    "LDAP user %s deactivated from offering but retained in directory",
                    username,
                )

    def release_users(
        self,
        offering_users: list[OfferingUser],
        waldur_rest_client: AuthenticatedClient,
    ) -> None:
        """Delete the entries of people who no longer hold any account on the directory.

        Core hands over the offering users of *this* offering whose access ended.
        That is not enough to act on: one directory serves every offering of the
        provider, so the entry stays as long as the same person still holds a
        live account under the same username on any of them -- a restricted
        account included, since restriction is a suspension, not a departure.
        The decision is made against Waldur, never against the directory, and
        any doubt (a failed lookup, an account without a username) keeps the
        entry. Keeping an entry that is still read elsewhere is a success;
        failing to check, or failing to delete, raises after the whole batch so
        core does not acknowledge the deletion to Waldur and retries next cycle.
        """
        if not self.remove_user_on_deactivate:
            for offering_user in offering_users:
                logger.info(
                    "LDAP user %s left offering %s but is retained in the directory: "
                    "remove_user_on_deactivate is off",
                    getattr(offering_user, "username", "?"),
                    self.offering.name if self.offering else "?",
                )
            return

        failed: list[str] = []
        for offering_user in offering_users:
            username = getattr(offering_user, "username", None)
            if not username:
                continue
            try:
                holder = self._live_account_elsewhere(offering_user, waldur_rest_client)
            except Exception:
                logger.exception(
                    "Could not check whether %s still holds an account on another "
                    "offering; keeping the LDAP entry",
                    username,
                )
                failed.append(username)
                continue
            if holder is not None:
                logger.info(
                    "Keeping LDAP user %s: still holds a %s account on offering %s",
                    username,
                    holder.state,
                    holder.offering_name,
                )
                continue

            entry = self.client.search_user(username)
            if entry is None:
                logger.info("LDAP user %s already absent, nothing to release", username)
                continue
            try:
                if self.on_departure == DeparturePolicy.DELETE.value:
                    self._delete_account(username)
                    logger.info(
                        "Deleted LDAP user %s and its personal group: no live account "
                        "remains on any offering of the provider",
                        username,
                    )
                elif LdapClient.is_disabled_by_agent(entry):
                    logger.info("LDAP user %s is already disabled", username)
                else:
                    self._disable_account(username)
                    logger.info(
                        "Disabled LDAP user %s (no-login shell, shadowExpire=1, groups dropped): "
                        "no live account remains on any offering of the provider; the entry "
                        "and its ids stay reserved",
                        username,
                    )
            except BackendError:
                logger.exception("Failed to release LDAP user %s", username)
                failed.append(username)
                continue
        if failed:
            msg = f"Could not release LDAP accounts: {', '.join(sorted(failed))}"
            raise BackendError(msg)

    def _live_account_elsewhere(
        self, offering_user: OfferingUser, waldur_rest_client: AuthenticatedClient
    ) -> Optional[OfferingUser]:
        """The offering user that still keeps this person's entry alive, if any.

        Every offering of one provider that shares the directory reads the same
        username through the provider-wide account, so "same person, same
        provider, same username" is exactly the set of accounts behind this one
        entry. A differently-named account on a sibling offering is a separate
        entry and does not count. The offering user handed in is its own sibling:
        if Waldur still lists *it* as live, the removal was for one project of
        several and the entry stays.
        """
        user_uuid = getattr(offering_user, "user_uuid", UNSET)
        provider_uuid = getattr(offering_user, "customer_uuid", UNSET)
        if user_uuid is UNSET or provider_uuid is UNSET:
            msg = (
                f"Offering user {offering_user.username} carries no user_uuid/customer_uuid; "
                "the provider-wide check cannot run"
            )
            raise BackendError(msg)
        siblings = marketplace_offering_users_list.sync_all(
            client=waldur_rest_client,
            user_uuid=user_uuid,
            provider_uuid=provider_uuid,
            field=[
                OfferingUserFieldEnum.UUID,
                OfferingUserFieldEnum.USERNAME,
                OfferingUserFieldEnum.STATE,
                OfferingUserFieldEnum.IS_RESTRICTED,
                OfferingUserFieldEnum.OFFERING_UUID,
                OfferingUserFieldEnum.OFFERING_NAME,
            ],
        )
        for sibling in siblings:
            if sibling.username != offering_user.username:
                continue
            if sibling.state in LIVE_OFFERING_USER_STATES:
                return sibling
        return None

    def _delete_account(self, username: str) -> None:
        """Drop access-group memberships, then the entry and its personal group.

        Project group memberships are not touched here: the resource backend
        removes those as it removes the SLURM association, before core calls
        release_users at all.
        """
        self._remove_from_access_groups(username)
        self.client.delete_user(username)

    def _disable_account(self, username: str) -> None:
        """Park the entry: drop every group membership it still has, then disable it.

        Unlike delete, this also sweeps project groups: the entry survives, so a
        leftover membership would keep granting group access to a parked
        account. A membership that cannot be dropped fails the release rather
        than being logged past: disabling the entry while a group still lists it
        would acknowledge a teardown that left the person's access in place.
        """
        self._remove_from_access_groups(username)
        for group_name, membership_type in self._group_memberships(username):
            if group_name == username:
                continue  # the personal group stays with the entry
            self.client.remove_user_from_group(group_name, username, membership_type)
        self.client.disable_user(username)

    def _group_memberships(self, username: str) -> list[tuple[str, str]]:
        """The groups_ou memberships to sweep; none when that OU is legitimately absent.

        Without personal groups the OU is only needed for access groups, so a
        directory without one is a valid layout rather than a misconfiguration.
        """
        if not self.personal_groups and not self.client.groups_container_exists():
            logger.debug(
                "%s does not exist; no group memberships to drop", self.client.groups_ou
            )
            return []
        return self.client.find_group_memberships(username)

    def _remove_from_access_groups(self, username: str) -> None:
        """Drop the configured access-group memberships, failing if one survives.

        A membership the user never had is not a failure: the client returns
        normally when the attribute is already absent, so anything raised here
        is a group that still grants access to an account being released.
        """
        for group_config in self.access_groups:
            group_name = group_config["name"]
            membership_type = group_config.get("attribute", "memberUid")
            self.client.remove_user_from_group(group_name, username, membership_type)

    def _add_to_access_groups(self, username: str) -> None:
        for group_config in self.access_groups:
            group_name = group_config["name"]
            membership_type = group_config.get("attribute", "memberUid")
            try:
                self.client.add_user_to_group(group_name, username, membership_type)
            except BackendError:
                logger.exception("Failed to add user %s to access group %s", username, group_name)

    def _generate_username_string(
        self,
        first_name: str,
        last_name: str,
        offering_user: OfferingUser,
    ) -> str:
        """Generate a username string based on the configured format."""
        if self.username_format == "waldur_username":
            raw = getattr(offering_user, "user_username", "") or ""
            return self._sanitize_posix_username(raw)

        first_name_clean = self._normalize_name(first_name)
        last_name_clean = self._normalize_name(last_name)

        if not first_name_clean or not last_name_clean:
            raw = getattr(offering_user, "user_username", "") or ""
            return self._sanitize_posix_username(raw)

        fi = first_name_clean[0]
        formats = {
            "first_initial_lastname": f"{fi}{last_name_clean}",
            "first_letter_full_lastname": f"{fi}.{last_name_clean}",
            "firstname_dot_lastname": f"{first_name_clean}.{last_name_clean}",
            "firstname_lastname": f"{first_name_clean}{last_name_clean}",
        }
        return formats.get(self.username_format, f"{fi}{last_name_clean}").lower()

    def _ensure_unique_username(
        self,
        base_username: str,
        first_name: str = "",
        last_name: str = "",
    ) -> str:
        """Ensure the username is unique.

        Resolution order:
        1. Try the base username as-is.
        2. Expand the first-name prefix (e.g. j.smith -> jo.smith -> joh.smith).
        3. Fall back to a numeric suffix (e.g. j.smith2, j.smith3).
        """
        if not self.client.user_exists(base_username):
            return base_username

        # Try expanding the first-name prefix when format uses a separator
        if first_name and last_name and "." in base_username:
            for length in range(2, len(first_name) + 1):
                candidate = f"{first_name[:length]}.{last_name}".lower()
                if candidate != base_username and not self.client.user_exists(candidate):
                    logger.info(
                        "Username %s taken, using expanded prefix %s",
                        base_username,
                        candidate,
                    )
                    return candidate

        # Numeric suffix fallback
        for i in range(2, 1000):
            candidate = f"{base_username}{i}"
            if not self.client.user_exists(candidate):
                logger.info(
                    "Username %s taken, using %s",
                    base_username,
                    candidate,
                )
                return candidate

        raise BackendError(f"Cannot find unique username based on {base_username}")

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize a name for use in a POSIX username.

        Removes diacritics, non-ASCII characters, and special characters.
        """
        # Decompose unicode characters and strip combining marks (accents)
        normalized = unicodedata.normalize("NFD", name)
        ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
        # Keep only alphanumeric characters
        return re.sub(r"[^a-zA-Z0-9]", "", ascii_name)

    @staticmethod
    def _sanitize_posix_username(raw: str) -> str:
        """Sanitize a raw string into a valid POSIX username.

        Strips diacritics, keeps only [a-z0-9._-], removes leading
        non-alpha characters, and truncates to 32 characters.
        """
        normalized = unicodedata.normalize("NFD", raw)
        ascii_str = normalized.encode("ascii", "ignore").decode("ascii").lower()
        # Keep POSIX-safe characters: alphanumeric, dot, underscore, hyphen
        sanitized = re.sub(r"[^a-z0-9._-]", "", ascii_str)
        # Username must start with a letter or underscore
        sanitized = re.sub(r"^[^a-z_]+", "", sanitized)
        # Truncate to 32 chars (POSIX limit)
        return sanitized[:32]
