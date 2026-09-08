"""LDAP username management backend for Waldur Site Agent."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

from pydantic import ValidationError as PydanticValidationError
from waldur_api_client.models.offering_user import OfferingUser

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_ldap import reconcile
from waldur_site_agent_ldap.client import LdapClient
from waldur_site_agent_ldap.email_sender import WelcomeEmailSender
from waldur_site_agent_ldap.schemas import (
    AccountSource,
    LdapSettingsSchema,
    MissingPosixIdsPolicy,
    PosixMismatchPolicy,
)


class LdapUsernameBackend(AbstractUsernameManagementBackend):
    """Username management backend that provisions POSIX users in LDAP.

    Creates POSIX user entries with personal groups and handles user lifecycle in
    an LDAP directory. It does not manage SSH keys: a user's keys are not exposed
    on the offering-user list this backend reads, so it never sees them (#18).
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
        self.remove_user_on_deactivate = ldap_settings.get("remove_user_on_deactivate", False)
        self.access_groups = ldap_settings.get("access_groups", [])
        self.generate_vpn_password = ldap_settings.get("generate_vpn_password", False)
        self.waldur_username_attr = ldap_settings.get("waldur_username_attribute", "")

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
        """Say plainly which settings stop applying, and which hazard remains."""
        if "uid_range_start" in ldap_settings or "uid_range_end" in ldap_settings:
            logger.warning(
                "account_source is 'waldur', so uid_range_start/uid_range_end are "
                "ignored for user accounts: UIDs come from the offering user."
            )
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

    def _reconcile_from_waldur(self, offering_users: list[OfferingUser]) -> None:
        """Converge the directory on Waldur's usernames and POSIX ids.

        Reads the directory twice in total, not twice per user: every LdapClient
        method opens and unbinds its own connection, so per-account lookups would
        cost thousands of binds a cycle on a large offering.
        """
        if not offering_users:
            return

        existing = self.client.list_users()
        uid_index = reconcile.index_by_uid_number(existing)
        mail_index = reconcile.index_by_mail(existing)

        created = updated = skipped = conflicts = 0
        unset_accounts = 0

        for offering_user in offering_users:
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
                    skipped += 1
                    self._report_missing_ids(offering_user)
                continue

            decision = reconcile.classify(
                desired,
                existing.get(desired.username),
                uid_owner=uid_index.get(desired.uid_number),
                mail_owner=mail_index.get(desired.email.lower()) if desired.email else None,
                waldur_username_attribute=self.waldur_username_attr,
            )
            try:
                outcome = self._apply(decision, desired)
            except BackendError:
                logger.exception("Failed to reconcile LDAP account %s", desired.username)
                skipped += 1
                continue
            if outcome == reconcile.Outcome.CREATE:
                created += 1
                # Keep the in-memory indexes honest so a second account in the
                # same batch cannot be handed a UID this one just took.
                uid_index.setdefault(desired.uid_number, desired.username)
            elif outcome == reconcile.Outcome.UPDATE:
                updated += 1
            elif outcome in (reconcile.Outcome.DRIFT, reconcile.Outcome.UID_TAKEN):
                conflicts += 1

        if unset_accounts:
            logger.error(
                "Waldur returned no POSIX attributes for any of %d offering users on %s. "
                "The server may predate the POSIX attribute API, or the agent may not be "
                "requesting those fields. No accounts were provisioned this cycle.",
                unset_accounts,
                self.offering.name if self.offering else "?",
            )
        logger.info(
            "LDAP reconcile: %d created, %d updated, %d skipped, %d conflicts",
            created,
            updated,
            skipped,
            conflicts,
        )

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

    def _apply(
        self, decision: reconcile.Decision, desired: reconcile.DesiredEntry
    ) -> reconcile.Outcome:
        """Carry out one decision. Returns the outcome actually acted on."""
        if decision.outcome == reconcile.Outcome.NOOP:
            return decision.outcome

        if decision.outcome == reconcile.Outcome.UID_TAKEN:
            logger.error(
                "UID %d is already held by LDAP user %s, so %s was left unchanged. "
                "Resolve the collision in the directory or re-point the allocation "
                "in Waldur.",
                desired.uid_number,
                decision.uid_taken_by,
                desired.username,
            )
            return decision.outcome

        if decision.outcome == reconcile.Outcome.DRIFT:
            return self._apply_drift(decision, desired)

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
        logger.info(
            "Updated LDAP user %s: %s",
            desired.username,
            ", ".join(sorted(decision.updates)),
        )
        return decision.outcome

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
