"""LDAP client for managing POSIX users and groups."""

from __future__ import annotations

import secrets
import string
from typing import Optional

from ldap3 import (
    ALL,
    MODIFY_ADD,
    MODIFY_DELETE,
    MODIFY_REPLACE,
    SUBTREE,
    Connection,
    Server,
)
from ldap3.core.exceptions import LDAPException

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError


class LdapClient:
    """Client for LDAP directory operations.

    Manages POSIX users and groups for HPC site integration.
    """

    def __init__(self, settings: dict) -> None:
        """Initialize LDAP client from settings dict."""
        self.uri = settings["uri"]
        self.bind_dn = settings["bind_dn"]
        self.bind_password = settings["bind_password"]
        self.base_dn = settings["base_dn"]
        self.people_ou = settings.get("people_ou", "ou=People")
        self.groups_ou = settings.get("groups_ou", "ou=Groups")
        self.uid_range_start = settings.get("uid_range_start", 10000)
        self.uid_range_end = settings.get("uid_range_end", 65000)
        self.gid_range_start = settings.get("gid_range_start", 10000)
        self.gid_range_end = settings.get("gid_range_end", 65000)
        self.default_login_shell = settings.get("default_login_shell", "/bin/bash")
        self.default_home_base = settings.get("default_home_base", "/home")
        self.user_object_classes = settings.get(
            "user_object_classes",
            [
                "inetOrgPerson",
                "ldapPublicKey",
                "organizationalPerson",
                "person",
                "posixAccount",
                "top",
            ],
        )
        self.user_group_object_classes = settings.get(
            "user_group_object_classes",
            [
                "groupOfNames",
                "nsMemberOf",
                "organizationalUnit",
                "posixGroup",
                "top",
            ],
        )
        self.project_group_object_classes = settings.get(
            "project_group_object_classes",
            ["posixGroup", "top"],
        )
        self.use_starttls = settings.get("use_starttls", False)

    def _connect(self) -> Connection:
        """Create and return a bound LDAP connection."""
        try:
            server = Server(self.uri, get_info=ALL)
            conn = Connection(
                server,
                user=self.bind_dn,
                password=self.bind_password,
                auto_bind=True,
            )
            if self.use_starttls:
                conn.start_tls()
            return conn
        except LDAPException as e:
            raise BackendError(f"Failed to connect to LDAP server {self.uri}: {e}") from e

    def ping(self) -> bool:
        """Check if LDAP server is reachable."""
        try:
            conn = self._connect()
            conn.unbind()
            return True
        except BackendError:
            return False

    @property
    def _people_dn(self) -> str:
        return f"{self.people_ou},{self.base_dn}"

    @property
    def _groups_dn(self) -> str:
        return f"{self.groups_ou},{self.base_dn}"

    def _user_dn(self, username: str) -> str:
        return f"uid={username},{self._people_dn}"

    def _group_dn(self, group_name: str) -> str:
        return f"cn={group_name},{self._groups_dn}"

    # ---- Search operations ----

    def search_user(self, username: str) -> Optional[dict]:
        """Search for a user by uid."""
        conn = self._connect()
        try:
            conn.search(
                self._people_dn,
                f"(uid={username})",
                search_scope=SUBTREE,
                attributes=["uid", "uidNumber", "gidNumber", "cn", "mail", "sshPublicKey"],
            )
            if conn.entries:
                entry = conn.entries[0]
                return entry.entry_attributes_as_dict
            return None
        except LDAPException as e:
            raise BackendError(f"LDAP search for user {username} failed: {e}") from e
        finally:
            conn.unbind()

    def search_user_by_email(self, email: str) -> Optional[dict]:
        """Search for a user by email address."""
        conn = self._connect()
        try:
            conn.search(
                self._people_dn,
                f"(mail={email})",
                search_scope=SUBTREE,
                attributes=["uid", "uidNumber", "gidNumber", "cn", "mail"],
            )
            if conn.entries:
                entry = conn.entries[0]
                return entry.entry_attributes_as_dict
            return None
        except LDAPException as e:
            raise BackendError(f"LDAP search by email {email} failed: {e}") from e
        finally:
            conn.unbind()

    def user_exists(self, username: str) -> bool:
        """Check if a user exists in LDAP."""
        return self.search_user(username) is not None

    def group_exists(self, group_name: str) -> bool:
        """Check if a group exists in LDAP."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(cn={group_name})",
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            return len(conn.entries) > 0
        except LDAPException as e:
            raise BackendError(f"LDAP search for group {group_name} failed: {e}") from e
        finally:
            conn.unbind()

    def get_group_gid(self, group_name: str) -> Optional[int]:
        """Get the gidNumber of a group."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(cn={group_name})",
                search_scope=SUBTREE,
                attributes=["gidNumber"],
            )
            if conn.entries:
                return int(conn.entries[0].gidNumber.value)
            return None
        except LDAPException as e:
            raise BackendError(f"Failed to get GID for group {group_name}: {e}") from e
        finally:
            conn.unbind()

    # ---- ID allocation ----

    def _get_used_ids(self, attribute: str, search_base: str) -> set[int]:
        """Collect all used IDs of a given attribute type."""
        conn = self._connect()
        try:
            conn.search(
                search_base,
                f"({attribute}=*)",
                search_scope=SUBTREE,
                attributes=[attribute],
            )
            used = set()
            for entry in conn.entries:
                val = getattr(entry, attribute).value
                if val is not None:
                    used.add(int(val))
            return used
        except LDAPException as e:
            raise BackendError(f"Failed to enumerate {attribute} values: {e}") from e
        finally:
            conn.unbind()

    def get_next_uid(self) -> int:
        """Find the next available UID in the configured range."""
        used_uids = self._get_used_ids("uidNumber", self._people_dn)
        for uid in range(self.uid_range_start, self.uid_range_end + 1):
            if uid not in used_uids:
                return uid
        raise BackendError(
            f"No available UIDs in range {self.uid_range_start}-{self.uid_range_end}"
        )

    def get_next_gid(self) -> int:
        """Find the next available GID in the configured range."""
        used_gids = self._get_used_ids("gidNumber", self._groups_dn)
        for gid in range(self.gid_range_start, self.gid_range_end + 1):
            if gid not in used_gids:
                return gid
        raise BackendError(
            f"No available GIDs in range {self.gid_range_start}-{self.gid_range_end}"
        )

    # ---- User operations ----

    def create_user(
        self,
        username: str,
        first_name: str,
        last_name: str,
        email: str,
        ssh_public_key: Optional[str] = None,
        password: Optional[str] = None,
        description: Optional[str] = None,
    ) -> int:
        """Create a POSIX user and their personal group in LDAP.

        Returns the allocated UID.
        """
        if self.user_exists(username):
            raise BackendError(f"User {username} already exists in LDAP")

        uid_number = self.get_next_uid()
        gid_number = self.get_next_gid()

        # Create the personal group first
        self._create_group_entry(
            group_name=username,
            gid_number=gid_number,
            object_classes=self.user_group_object_classes,
            extra_attributes={
                "memberUid": username,
                "ou": "groups",
            },
            member_dn=self._user_dn(username),
        )

        # Create the user entry
        full_name = f"{first_name} {last_name}".strip() or username
        attributes = {
            "objectClass": self.user_object_classes,
            "uid": username,
            "cn": full_name,
            "givenName": first_name or username,
            "sn": last_name or username,
            "uidNumber": uid_number,
            "gidNumber": gid_number,
            "homeDirectory": f"{self.default_home_base}/{username}",
            "loginShell": self.default_login_shell,
            "mail": email,
        }

        if ssh_public_key:
            attributes["sshPublicKey"] = ssh_public_key

        if password:
            attributes["userPassword"] = password

        if description:
            attributes["description"] = description

        conn = self._connect()
        try:
            user_dn = self._user_dn(username)
            success = conn.add(user_dn, attributes=attributes)
            if not success:
                raise BackendError(f"Failed to create LDAP user {username}: {conn.result}")
            logger.info("Created LDAP user %s with UID %d", username, uid_number)
            return uid_number
        except LDAPException as e:
            raise BackendError(f"Failed to create LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    def delete_user(self, username: str) -> None:
        """Delete a user and their personal group from LDAP."""
        conn = self._connect()
        try:
            # Delete user entry
            user_dn = self._user_dn(username)
            conn.delete(user_dn)
            logger.info("Deleted LDAP user entry %s", username)

            # Delete personal group
            group_dn = self._group_dn(username)
            conn.delete(group_dn)
            logger.info("Deleted LDAP personal group for %s", username)
        except LDAPException as e:
            raise BackendError(f"Failed to delete LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    def update_user_attributes(self, username: str, attributes: dict) -> None:
        """Update attributes of an existing LDAP user."""
        conn = self._connect()
        try:
            user_dn = self._user_dn(username)
            changes = {}
            for attr_name, attr_value in attributes.items():
                if attr_value is not None:
                    changes[attr_name] = [(MODIFY_REPLACE, [attr_value])]
            if changes:
                success = conn.modify(user_dn, changes)
                if not success:
                    raise BackendError(f"Failed to update LDAP user {username}: {conn.result}")
                logger.info(
                    "Updated LDAP user %s attributes: %s", username, list(attributes.keys())
                )
        except LDAPException as e:
            raise BackendError(f"Failed to update LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    # ---- Group operations ----

    def _create_group_entry(
        self,
        group_name: str,
        gid_number: int,
        object_classes: list[str],
        extra_attributes: Optional[dict] = None,
        member_dn: Optional[str] = None,
    ) -> None:
        """Create a POSIX group entry."""
        conn = self._connect()
        try:
            group_dn = self._group_dn(group_name)
            attributes = {
                "objectClass": object_classes,
                "cn": group_name,
                "gidNumber": gid_number,
            }
            if extra_attributes:
                attributes.update(extra_attributes)
            # groupOfNames requires at least one member attribute
            if "groupOfNames" in object_classes and member_dn:
                attributes["member"] = member_dn
            success = conn.add(group_dn, attributes=attributes)
            if not success:
                raise BackendError(f"Failed to create LDAP group {group_name}: {conn.result}")
            logger.info("Created LDAP group %s with GID %d", group_name, gid_number)
        except LDAPException as e:
            raise BackendError(f"Failed to create LDAP group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def create_project_group(self, group_name: str) -> int:
        """Create a project POSIX group.

        Returns the allocated GID.
        """
        if self.group_exists(group_name):
            gid = self.get_group_gid(group_name)
            logger.info("LDAP project group %s already exists with GID %s", group_name, gid)
            return gid or 0

        gid_number = self.get_next_gid()
        self._create_group_entry(
            group_name=group_name,
            gid_number=gid_number,
            object_classes=self.project_group_object_classes,
        )
        return gid_number

    def delete_group(self, group_name: str) -> None:
        """Delete a group from LDAP."""
        conn = self._connect()
        try:
            group_dn = self._group_dn(group_name)
            conn.delete(group_dn)
            logger.info("Deleted LDAP group %s", group_name)
        except LDAPException as e:
            raise BackendError(f"Failed to delete LDAP group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def add_user_to_group(
        self,
        group_name: str,
        username: str,
        membership_type: str = "memberUid",
    ) -> None:
        """Add a user to a group.

        Args:
            group_name: Name of the target group.
            username: Username to add.
            membership_type: Either "memberUid" (UID-based) or "member" (DN-based).
        """
        conn = self._connect()
        try:
            group_dn = self._group_dn(group_name)
            value = self._user_dn(username) if membership_type == "member" else username

            success = conn.modify(
                group_dn,
                {membership_type: [(MODIFY_ADD, [value])]},
            )
            if not success:
                result_desc = conn.result.get("description", "")
                # Attribute already exists is not an error
                if "attributeOrValueExists" not in result_desc:
                    raise BackendError(
                        f"Failed to add {username} to group {group_name}: {conn.result}"
                    )
            logger.info("Added user %s to LDAP group %s", username, group_name)
        except LDAPException as e:
            raise BackendError(f"Failed to add {username} to group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def remove_user_from_group(
        self,
        group_name: str,
        username: str,
        membership_type: str = "memberUid",
    ) -> None:
        """Remove a user from a group."""
        conn = self._connect()
        try:
            group_dn = self._group_dn(group_name)
            value = self._user_dn(username) if membership_type == "member" else username

            success = conn.modify(
                group_dn,
                {membership_type: [(MODIFY_DELETE, [value])]},
            )
            if not success:
                result_desc = conn.result.get("description", "")
                if "noSuchAttribute" not in result_desc:
                    raise BackendError(
                        f"Failed to remove {username} from group {group_name}: {conn.result}"
                    )
            logger.info("Removed user %s from LDAP group %s", username, group_name)
        except LDAPException as e:
            raise BackendError(f"Failed to remove {username} from group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def is_user_in_group(
        self,
        group_name: str,
        username: str,
        membership_type: str = "memberUid",
    ) -> bool:
        """Check if a user is a member of a group."""
        conn = self._connect()
        try:
            if membership_type == "member":
                filter_str = f"(&(cn={group_name})(member={self._user_dn(username)}))"
            else:
                filter_str = f"(&(cn={group_name})(memberUid={username}))"

            conn.search(self._groups_dn, filter_str, search_scope=SUBTREE, attributes=["cn"])
            return len(conn.entries) > 0
        except LDAPException as e:
            raise BackendError(
                f"Failed to check membership of {username} in {group_name}: {e}"
            ) from e
        finally:
            conn.unbind()

    @staticmethod
    def generate_random_password(length: int = 16) -> str:
        """Generate a random password for VPN access."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return "".join(secrets.choice(alphabet) for _ in range(length))
