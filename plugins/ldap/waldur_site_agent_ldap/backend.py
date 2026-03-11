"""LDAP username management backend for Waldur Site Agent."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

from waldur_api_client.models.offering_user import OfferingUser

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.common.structures import Offering
from waldur_site_agent_ldap.client import LdapClient
from waldur_site_agent_ldap.email_sender import WelcomeEmailSender


class LdapUsernameBackend(AbstractUsernameManagementBackend):
    """Username management backend that provisions POSIX users in LDAP.

    Creates POSIX user entries with personal groups, manages SSH keys,
    and handles user lifecycle in an LDAP directory.
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
        self.client = LdapClient(ldap_settings)
        self.username_format = ldap_settings.get("username_format", "first_initial_lastname")
        self.remove_user_on_deactivate = ldap_settings.get("remove_user_on_deactivate", False)
        self.access_groups = ldap_settings.get("access_groups", [])
        self.generate_vpn_password = ldap_settings.get("generate_vpn_password", False)

        welcome_email_settings = ldap_settings.get("welcome_email")
        self.email_sender = (
            WelcomeEmailSender(welcome_email_settings) if welcome_email_settings else None
        )

    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Check if a user already exists in LDAP.

        Searches by email first, then by Waldur username.
        """
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
        first_name = getattr(offering_user, "user_first_name", "") or ""
        last_name = getattr(offering_user, "user_last_name", "") or ""
        email = getattr(offering_user, "user_email", "") or ""

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
        uid_number = self.client.create_user(
            username=username,
            first_name=first_name,
            last_name=last_name,
            email=email,
            password=password,
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

            updates = {}
            if first_name:
                updates["givenName"] = first_name
            if last_name:
                updates["sn"] = last_name
            if first_name and last_name:
                updates["cn"] = f"{first_name} {last_name}"
            if email:
                updates["mail"] = email

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
