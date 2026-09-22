"""LDAP client for managing POSIX users and groups."""

from __future__ import annotations

import secrets
import string
from typing import Optional, Union

from ldap3 import (
    ALL,
    MODIFY_ADD,
    MODIFY_DELETE,
    MODIFY_REPLACE,
    SUBTREE,
    Connection,
    Server,
)
from ldap3.core.exceptions import LDAPException, LDAPInvalidDnError
from ldap3.utils.conv import escape_filter_chars
from ldap3.utils.dn import escape_rdn, parse_dn

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

#: Written into ``description`` by disable_user, so a reconcile can tell an
#: account the agent parked from one an operator disabled by hand.
DISABLED_MARKER = "waldur-site-agent:disabled"
NOLOGIN_SHELL = "/usr/sbin/nologin"


def _first(value: Union[list, tuple, str, int, None]) -> Union[str, int, None]:
    """First element of an ldap3 attribute value, which may be a list or a scalar.

    ``entry_attributes_as_dict`` returns a list for every attribute, but a
    single-valued attribute that was never set comes back as an empty list, and
    some strategies hand back a bare scalar. Normalising here keeps every caller
    from repeating the same three-way check.
    """
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


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
        # groupOfNames requires at least one member. Such groups are created with
        # this DN as a stand-in, and it takes the last real member's place on
        # removal, so revoking access never needs an empty group. It is never
        # read back as a user, which is why it must not be a uid= DN.
        self.empty_group_member_dn = (
            settings.get("empty_group_member_dn") or f"cn=nobody,{self.base_dn}"
        )
        if _extract_uid_from_dn(self.empty_group_member_dn):
            raise BackendError(
                f"empty_group_member_dn {self.empty_group_member_dn!r} must not be a "
                "uid= DN: it would be read back as a group member"
            )

    # Attributes every user search asks for. The reconciler diffs the POSIX
    # projection as well as the profile, so homeDirectory/loginShell/givenName/sn
    # have to come back too — without them a drift check cannot tell a match from
    # a mismatch.
    USER_ATTRIBUTES = (
        "uid",
        "uidNumber",
        "gidNumber",
        "cn",
        "mail",
        "homeDirectory",
        "loginShell",
        "givenName",
        "sn",
        # Departure bookkeeping: an account parked by disable_user carries the
        # marker in description, shadowExpire=1 and the shadowAccount class.
        "description",
        "shadowExpire",
        "objectClass",
    )

    def _build_server(self) -> Server:
        """Construct the ldap3 Server. Overridden in tests to inject a strategy."""
        return Server(self.uri, get_info=ALL)

    def _connect(self) -> Connection:
        """Create and return a bound LDAP connection."""
        try:
            server = self._build_server()
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
        return f"uid={escape_rdn(username)},{self._people_dn}"

    def _group_dn(self, group_name: str) -> str:
        return f"cn={escape_rdn(group_name)},{self._groups_dn}"

    # ---- Search operations ----

    def search_user(self, username: str) -> Optional[dict]:
        """Search for a user by uid."""
        conn = self._connect()
        try:
            conn.search(
                self._people_dn,
                f"(uid={escape_filter_chars(username)})",
                search_scope=SUBTREE,
                attributes=list(self.USER_ATTRIBUTES),
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
                f"(mail={escape_filter_chars(email)})",
                search_scope=SUBTREE,
                attributes=list(self.USER_ATTRIBUTES),
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

    def list_users(self) -> dict:
        """Every user entry under people_ou, keyed by uid.

        One search instead of one per account. Every other method on this class
        opens its own connection and unbinds it, so a per-user reconcile over a
        few hundred accounts would open well over a thousand binds each cycle.
        The reconciler reads this once and diffs in memory.
        """
        conn = self._connect()
        try:
            conn.search(
                self._people_dn,
                "(uid=*)",
                search_scope=SUBTREE,
                attributes=list(self.USER_ATTRIBUTES),
            )
            users = {}
            for entry in conn.entries:
                attrs = entry.entry_attributes_as_dict
                uid = _first(attrs.get("uid"))
                if uid:
                    users[uid] = attrs
            return users
        except LDAPException as e:
            raise BackendError(f"Failed to list LDAP users: {e}") from e
        finally:
            conn.unbind()

    def list_groups(self) -> dict:
        """Every group under groups_ou as ``{cn: gidNumber}``. See list_users."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                "(cn=*)",
                search_scope=SUBTREE,
                attributes=["cn", "gidNumber"],
            )
            groups = {}
            for entry in conn.entries:
                attrs = entry.entry_attributes_as_dict
                cn = _first(attrs.get("cn"))
                gid = _first(attrs.get("gidNumber"))
                if cn is not None and gid is not None:
                    groups[cn] = int(gid)
            return groups
        except LDAPException as e:
            raise BackendError(f"Failed to list LDAP groups: {e}") from e
        finally:
            conn.unbind()

    def search_user_by_uid_number(self, uid_number: int) -> Optional[dict]:
        """Find the entry holding a given uidNumber, if any.

        Used to detect a UID already taken by an unrelated account before
        creating a new one with it.
        """
        conn = self._connect()
        try:
            conn.search(
                self._people_dn,
                f"(uidNumber={escape_filter_chars(str(uid_number))})",
                search_scope=SUBTREE,
                attributes=list(self.USER_ATTRIBUTES),
            )
            if conn.entries:
                return conn.entries[0].entry_attributes_as_dict
            return None
        except LDAPException as e:
            raise BackendError(f"LDAP search for uidNumber {uid_number} failed: {e}") from e
        finally:
            conn.unbind()

    def group_exists(self, group_name: str) -> bool:
        """Check if a group exists in LDAP."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(cn={escape_filter_chars(group_name)})",
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
                f"(cn={escape_filter_chars(group_name)})",
                search_scope=SUBTREE,
                attributes=["gidNumber"],
            )
            if not conn.entries:
                return None
            # A plain groupOfNames has no gidNumber.
            gid = _first(conn.entries[0].entry_attributes_as_dict.get("gidNumber"))
            return int(gid) if gid is not None else None
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
        password: Optional[str] = None,
        extra_attributes: Optional[dict[str, str]] = None,
    ) -> int:
        """Create a POSIX user and their personal group in LDAP.

        Args:
            username: POSIX username for the new account.
            first_name: User's first name.
            last_name: User's last name.
            email: User's email address.
            password: Optional cleartext password to set.
            extra_attributes: Additional LDAP attributes to set on the user entry
                (e.g. ``{"employeeNumber": "cuid123"}``).

        Returns the allocated UID.
        """
        if self.user_exists(username):
            raise BackendError(f"User {username} already exists in LDAP")

        uid_number = self.get_next_uid()
        gid_number = self.get_next_gid()

        # Create the personal group first
        group_extra_attrs: dict[str, str] = {"memberUid": username}
        self._create_group_entry(
            group_name=username,
            gid_number=gid_number,
            object_classes=self.user_group_object_classes,
            extra_attributes=group_extra_attrs,
            member_dn=self._user_dn(username),
        )

        # Create the user entry
        full_name = f"{first_name} {last_name}".strip() or username
        attributes: dict[str, object] = {
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

        if password:
            attributes["userPassword"] = password

        if extra_attributes:
            attributes.update(extra_attributes)

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

    def create_user_with_ids(
        self,
        username: str,
        first_name: str,
        last_name: str,
        email: str,
        uid_number: int,
        gid_number: int,
        home_directory: str,
        login_shell: str,
        password: Optional[str] = None,
        extra_attributes: Optional[dict] = None,
    ) -> None:
        """Create a POSIX user from externally-supplied ids.

        The Waldur-authoritative counterpart of :meth:`create_user`: every value
        is given rather than allocated, so ``get_next_uid``/``get_next_gid`` are
        never consulted. Kept separate so the legacy allocator path is untouched.

        The personal group has to exist before the user entry (``groupOfNames``
        needs a member, and the account needs its primary group), so if the user
        add then fails we delete a group we had just created rather than leaving
        it orphaned — a failure the original create path leaves behind.
        """
        group_state = self.ensure_group(
            group_name=username,
            gid_number=gid_number,
            object_classes=self.user_group_object_classes,
            extra_attributes={"memberUid": username},
            member_dn=self._user_dn(username),
        )
        if group_state == "conflict":
            raise BackendError(
                f"LDAP group {username} already exists with a different gidNumber than "
                f"the {gid_number} Waldur assigned; refusing to create the account."
            )

        full_name = f"{first_name} {last_name}".strip() or username
        attributes: dict = {
            "objectClass": self.user_object_classes,
            "uid": username,
            "cn": full_name,
            "givenName": first_name or username,
            "sn": last_name or username,
            "uidNumber": uid_number,
            "gidNumber": gid_number,
            "homeDirectory": home_directory,
            "loginShell": login_shell,
            "mail": email,
        }
        if password:
            attributes["userPassword"] = password
        if extra_attributes:
            attributes.update(extra_attributes)

        try:
            self._add_user_entry(username, attributes)
        except BackendError:
            # The group had to exist before the user entry; if the entry could not
            # be added, do not leave the group we just made behind as an orphan.
            if group_state == "created":
                self._rollback_group(username)
            raise
        logger.info(
            "Created LDAP user %s with UID %d and GID %d from Waldur",
            username,
            uid_number,
            gid_number,
        )

    def _add_user_entry(self, username: str, attributes: dict) -> None:
        """Add one user entry, raising BackendError on any failure."""
        conn = self._connect()
        try:
            success = conn.add(self._user_dn(username), attributes=attributes)
            if not success:
                msg = f"Failed to create LDAP user {username}: {conn.result}"
                raise BackendError(msg)
        except LDAPException as e:
            raise BackendError(f"Failed to create LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    def _rollback_group(self, group_name: str) -> None:
        """Best-effort removal of a personal group whose user entry failed to add."""
        try:
            self.delete_group(group_name)
            logger.info("Rolled back orphaned LDAP group %s", group_name)
        except BackendError:
            logger.exception(
                "Failed to roll back LDAP group %s after the user entry could not be "
                "created; it may need removing by hand",
                group_name,
            )

    def set_user_posix_attributes(
        self,
        username: str,
        uid_number: Optional[int] = None,
        gid_number: Optional[int] = None,
        home_directory: Optional[str] = None,
        login_shell: Optional[str] = None,
    ) -> None:
        """Replace whichever POSIX attributes are supplied on an existing entry."""
        attributes: dict = {}
        if uid_number is not None:
            attributes["uidNumber"] = uid_number
        if gid_number is not None:
            attributes["gidNumber"] = gid_number
        if home_directory is not None:
            attributes["homeDirectory"] = home_directory
        if login_shell is not None:
            attributes["loginShell"] = login_shell
        if attributes:
            self.update_user_attributes(username, attributes)

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

    def disable_user(self, username: str, login_shell: str = NOLOGIN_SHELL) -> None:
        """Park an account: keep the entry and its ids, make it unusable.

        Sets ``loginShell`` to a no-login shell, adds the ``shadowAccount`` class
        with ``shadowExpire: 1`` (an expiry in the past, which sssd honours with
        ``ldap_account_expire_policy = shadow``), and records the marker in
        ``description`` so a later reconcile can tell an account the agent parked
        from one an operator disabled by hand. Idempotent.
        """
        entry = self.search_user(username)
        if entry is None:
            raise BackendError(f"Cannot disable LDAP user {username}: entry not found")
        changes: dict = {
            "loginShell": [(MODIFY_REPLACE, [login_shell])],
            "shadowExpire": [(MODIFY_REPLACE, ["1"])],
        }
        classes = {str(c) for c in entry.get("objectClass") or []}
        if "shadowAccount" not in classes:
            changes["objectClass"] = [(MODIFY_ADD, ["shadowAccount"])]
        descriptions = {str(d) for d in entry.get("description") or []}
        if DISABLED_MARKER not in descriptions:
            changes["description"] = [(MODIFY_ADD, [DISABLED_MARKER])]
        conn = self._connect()
        try:
            if not conn.modify(self._user_dn(username), changes):
                raise BackendError(f"Failed to disable LDAP user {username}: {conn.result}")
            logger.info("Disabled LDAP user %s", username)
        except LDAPException as e:
            raise BackendError(f"Failed to disable LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    def enable_user(self, username: str, login_shell: str) -> None:
        """Undo disable_user: restore the shell, drop the expiry and the marker.

        The ``shadowAccount`` class is left in place; it is harmless without
        ``shadowExpire`` and removing an auxiliary class is a schema-sensitive
        write for no gain.
        """
        entry = self.search_user(username)
        if entry is None:
            raise BackendError(f"Cannot enable LDAP user {username}: entry not found")
        changes: dict = {"loginShell": [(MODIFY_REPLACE, [login_shell])]}
        if entry.get("shadowExpire"):
            changes["shadowExpire"] = [(MODIFY_DELETE, [])]
        descriptions = {str(d) for d in entry.get("description") or []}
        if DISABLED_MARKER in descriptions:
            changes["description"] = [(MODIFY_DELETE, [DISABLED_MARKER])]
        conn = self._connect()
        try:
            if not conn.modify(self._user_dn(username), changes):
                raise BackendError(f"Failed to enable LDAP user {username}: {conn.result}")
            logger.info("Re-enabled LDAP user %s", username)
        except LDAPException as e:
            raise BackendError(f"Failed to enable LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    @staticmethod
    def is_disabled_by_agent(entry: Optional[dict]) -> bool:
        """Whether a user entry (as returned by search_user/list_users) was parked by the agent."""
        if not entry:
            return False
        return DISABLED_MARKER in {str(d) for d in entry.get("description") or []}

    def find_groups_with_member(self, username: str) -> list[str]:
        """Names of every group under groups_ou listing ``username`` as memberUid."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(memberUid={escape_filter_chars(username)})",
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            names = []
            for entry in conn.entries:
                cn = _first(entry.entry_attributes_as_dict.get("cn"))
                if cn:
                    names.append(str(cn))
            return names
        except LDAPException as e:
            raise BackendError(f"Failed to list groups of {username}: {e}") from e
        finally:
            conn.unbind()

    # ---- Group operations ----

    def _create_group_entry(
        self,
        group_name: str,
        gid_number: Optional[int],
        object_classes: list[str],
        extra_attributes: Optional[dict] = None,
        member_dn: Optional[str] = None,
    ) -> None:
        """Create a group entry; ``gid_number`` is None for a group without posixGroup."""
        conn = self._connect()
        try:
            group_dn = self._group_dn(group_name)
            attributes: dict = {
                "objectClass": object_classes,
                "cn": group_name,
            }
            if gid_number is not None:
                attributes["gidNumber"] = gid_number
            if extra_attributes:
                attributes.update(extra_attributes)
            # groupOfNames requires at least one member attribute
            if member_dn and "groupofnames" in {c.lower() for c in object_classes}:
                attributes["member"] = member_dn
            success = conn.add(group_dn, attributes=attributes)
            if not success:
                raise BackendError(f"Failed to create LDAP group {group_name}: {conn.result}")
            logger.info("Created LDAP group %s (GID %s)", group_name, gid_number)
        except LDAPException as e:
            raise BackendError(f"Failed to create LDAP group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def ensure_group(
        self,
        group_name: str,
        gid_number: int,
        object_classes: list,
        extra_attributes: Optional[dict] = None,
        member_dn: Optional[str] = None,
    ) -> str:
        """Create the group if absent; report what was found if it is already there.

        Returns ``"created"``, ``"exists"`` when a group of that name already holds
        the wanted GID, or ``"conflict"`` when it holds a different one. Unlike
        :meth:`_create_group_entry` this is safe to call on every reconcile pass.
        """
        existing_gid = self.get_group_gid(group_name) if self.group_exists(group_name) else None
        if existing_gid is not None:
            return "exists" if existing_gid == gid_number else "conflict"
        self._create_group_entry(
            group_name=group_name,
            gid_number=gid_number,
            object_classes=object_classes,
            extra_attributes=extra_attributes,
            member_dn=member_dn,
        )
        return "created"

    def set_group_gid(self, group_name: str, gid_number: int) -> None:
        """Replace a group's gidNumber."""
        conn = self._connect()
        try:
            success = conn.modify(
                self._group_dn(group_name),
                {"gidNumber": [(MODIFY_REPLACE, [gid_number])]},
            )
            if not success:
                raise BackendError(
                    f"Failed to set gidNumber on LDAP group {group_name}: {conn.result}"
                )
            logger.info("Set LDAP group %s gidNumber to %d", group_name, gid_number)
        except LDAPException as e:
            raise BackendError(f"Failed to set gidNumber on group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def create_project_group(
        self, group_name: str, extra_attributes: Optional[dict] = None
    ) -> Optional[int]:
        """Create a project group with ``project_group_object_classes``.

        Returns the GID, or None when the classes carry no gidNumber (a plain
        groupOfNames). A groupOfNames is created with ``empty_group_member_dn``
        as its member, since it cannot exist without one. ``extra_attributes``
        are written in the same add, so a group never exists without them.
        """
        if self.group_exists(group_name):
            gid = self.get_group_gid(group_name)
            logger.info("LDAP project group %s already exists with GID %s", group_name, gid)
            return gid

        classes = {c.lower() for c in self.project_group_object_classes}
        gid_number = self.get_next_gid() if "posixgroup" in classes else None
        self._create_group_entry(
            group_name=group_name,
            gid_number=gid_number,
            object_classes=self.project_group_object_classes,
            extra_attributes=extra_attributes,
            member_dn=self.empty_group_member_dn,
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
            result_desc = "" if success else conn.result.get("description", "")
            if membership_type == "member" and result_desc == "objectClassViolation":
                # The last member of a groupOfNames, in a group that lacks the
                # stand-in (created by hand, or before it existed). Swap the
                # stand-in in within one modify, so the group is never empty.
                success = conn.modify(
                    group_dn,
                    {
                        "member": [
                            (MODIFY_ADD, [self.empty_group_member_dn]),
                            (MODIFY_DELETE, [value]),
                        ]
                    },
                )
                result_desc = "" if success else conn.result.get("description", "")
            if not success and "noSuchAttribute" not in result_desc:
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
                filter_str = (
                    f"(&(cn={escape_filter_chars(group_name)})"
                    f"(member={escape_filter_chars(self._user_dn(username))}))"
                )
            else:
                filter_str = (
                    f"(&(cn={escape_filter_chars(group_name)})"
                    f"(memberUid={escape_filter_chars(username)}))"
                )

            conn.search(self._groups_dn, filter_str, search_scope=SUBTREE, attributes=["cn"])
            return len(conn.entries) > 0
        except LDAPException as e:
            raise BackendError(
                f"Failed to check membership of {username} in {group_name}: {e}"
            ) from e
        finally:
            conn.unbind()

    def list_group_members(
        self,
        group_name: str,
        membership_type: str = "memberUid",
    ) -> list[str]:
        """List the members of a group as a list of usernames.

        For ``member`` (DN-based) groups, the leading ``uid=`` RDN is
        extracted so the returned list is comparable to ``memberUid``
        groups in the calling code. Members that are not ``uid=`` DNs —
        ``empty_group_member_dn``, nested groups — are not users and are
        left out, so callers never try to remove them.
        """
        conn = self._connect()
        try:
            attr = "member" if membership_type == "member" else "memberUid"
            conn.search(
                self._groups_dn,
                f"(cn={escape_filter_chars(group_name)})",
                search_scope=SUBTREE,
                attributes=[attr],
            )
            if not conn.entries:
                return []
            raw = getattr(conn.entries[0], attr).value
            if raw is None:
                return []
            values = raw if isinstance(raw, list) else [raw]
            if membership_type == "member":
                return [uid for uid in (_extract_uid_from_dn(v) for v in values if v) if uid]
            return [str(v) for v in values if v]
        except LDAPException as e:
            raise BackendError(
                f"Failed to list members of group {group_name}: {e}"
            ) from e
        finally:
            conn.unbind()

    # ---- Group ownership markers ----
    #
    # A directory is often shared between writers: this agent's plugins, other
    # provisioning, administrators. Plugins that reconcile a group's full member
    # list record which groups are theirs in ``description`` so they never strip
    # the members of a group someone else created.

    def get_group_descriptions(self, group_name: str) -> Optional[list[str]]:
        """Return a group's description values, or None if the group does not exist."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(cn={escape_filter_chars(group_name)})",
                search_scope=SUBTREE,
                attributes=["description"],
            )
            if not conn.entries:
                return None
            values = conn.entries[0].entry_attributes_as_dict.get("description") or []
            return [str(v) for v in values]
        except LDAPException as e:
            raise BackendError(f"Failed to read description of group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def add_group_description(self, group_name: str, value: str) -> None:
        """Add a description value to a group, keeping the values it already holds.

        Idempotent without relying on the server: not every directory rejects
        a duplicate value with ``attributeOrValueExists``.
        """
        if value in (self.get_group_descriptions(group_name) or []):
            return
        conn = self._connect()
        try:
            success = conn.modify(
                self._group_dn(group_name),
                {"description": [(MODIFY_ADD, [value])]},
            )
            if not success and "attributeOrValueExists" not in conn.result.get("description", ""):
                raise BackendError(
                    f"Failed to add description to LDAP group {group_name}: {conn.result}"
                )
        except LDAPException as e:
            raise BackendError(f"Failed to add description to group {group_name}: {e}") from e
        finally:
            conn.unbind()

    def find_groups_by_description(self, value: str) -> list[str]:
        """Names of the groups holding ``value`` among their description values."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                f"(description={escape_filter_chars(value)})",
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            names = []
            for entry in conn.entries:
                cn = _first(entry.entry_attributes_as_dict.get("cn"))
                if cn:
                    names.append(str(cn))
            return sorted(names)
        except LDAPException as e:
            raise BackendError(f"Failed to search groups by description: {e}") from e
        finally:
            conn.unbind()

    @staticmethod
    def generate_random_password(length: int = 16) -> str:
        """Generate a random password for VPN access."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return "".join(secrets.choice(alphabet) for _ in range(length))


def _extract_uid_from_dn(dn: str) -> str:
    r"""Extract the unescaped uid RDN value from a DN string.

    Returns the empty string if the DN doesn't start with a uid RDN or
    cannot be parsed. Used by list_group_members so the result of a
    DN-based group lookup is comparable to a memberUid lookup — which
    needs the value unescaped: ``_user_dn`` escapes usernames with
    ``escape_rdn``, so ``a,b`` is stored as ``uid=a\,b``.
    """
    try:
        components = parse_dn(dn)
    except LDAPInvalidDnError:
        return ""
    if not components:
        return ""
    attribute, value, _ = components[0]
    if attribute.strip().lower() != "uid":
        return ""
    return _unescape_dn_value(value)


def _unescape_dn_value(value: str) -> str:
    """Undo RFC 4514 escaping, which ldap3's parse_dn leaves in place.

    A backslash is followed either by the escaped character or by two hex
    digits of a UTF-8 byte; directories may return either form.
    """
    out = bytearray()
    i = 0
    while i < len(value):
        if value[i] == "\\" and i + 1 < len(value):
            pair = value[i + 1 : i + 3]
            if len(pair) == 2 and all(c in string.hexdigits for c in pair):  # noqa: PLR2004
                out.append(int(pair, 16))
                i += 3
            else:
                out.extend(value[i + 1].encode())
                i += 2
            continue
        out.extend(value[i].encode())
        i += 1
    return out.decode("utf-8", errors="replace")
