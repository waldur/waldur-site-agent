"""LDAP client for managing POSIX users and groups."""

from __future__ import annotations

import contextlib
import secrets
import string
from typing import Optional, Union

from ldap3 import (
    ALL,
    BASE,
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


class EntryExistsError(BackendError):
    """An add found the entry already there -- usually another writer got there first."""


class EntryMissingError(BackendError):
    """The entry to change is gone -- usually another writer moved or removed it first."""


class ContainerMissingError(BackendError):
    """The base of a search does not exist."""


class ValueConflictError(BackendError):
    """A modify added a value already present, or deleted one already gone."""


def _result_description(conn: Connection) -> str:
    return str((conn.result or {}).get("description", ""))


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
        # Off: accounts carry the primary GID they are given and no group entry
        # is written for them, so groups_ou is only needed for access groups.
        self.personal_groups = settings.get("personal_groups", True) is not False
        # Read back with every user search: it is compared on every cycle (a
        # profile diff) and it is the key a rename is found by.
        self.waldur_username_attribute = settings.get("waldur_username_attribute") or ""
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

    #: Page size for bulk searches. A server's size limit then caps a page, not
    #: the whole read.
    SEARCH_PAGE_SIZE = 500

    def _search_all(
        self, conn: Connection, search_base: str, search_filter: str, attributes: list[str]
    ) -> list[tuple[str, dict]]:
        """Every entry under ``search_base`` matching the filter, paged, or an error.

        A bulk read that stopped early -- a size, time or administrative limit,
        an ACL that hid part of the tree -- must abort the caller, not hand it a
        shorter list: a reconcile acting on a partial read would treat what it
        did not see as absent. Only a final ``success`` result counts. Every
        attribute comes back as a list, as ``entry_attributes_as_dict`` gives it.
        """
        response = conn.extend.standard.paged_search(
            search_base,
            search_filter,
            search_scope=SUBTREE,
            attributes=attributes,
            paged_size=self.SEARCH_PAGE_SIZE,
            generator=False,
        )
        description = _result_description(conn)
        if description == "noSuchObject":
            msg = f"{search_base} does not exist"
            raise ContainerMissingError(msg)
        if description != "success":
            msg = (
                f"Search under {search_base} for {search_filter} did not complete "
                f"({description or conn.result}); nothing was acted on"
            )
            raise BackendError(msg)
        entries = []
        for item in response or []:
            if item.get("type") != "searchResEntry":
                continue
            attrs = {
                name: value if isinstance(value, list) else [value]
                for name, value in (item.get("attributes") or {}).items()
            }
            entries.append((item["dn"], attrs))
        return entries

    def _user_attributes(self) -> list[str]:
        attributes = list(self.USER_ATTRIBUTES)
        if self.waldur_username_attribute and self.waldur_username_attribute not in attributes:
            attributes.append(self.waldur_username_attribute)
        return attributes

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
                attributes=self._user_attributes(),
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
                attributes=self._user_attributes(),
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
            users = {}
            for _, attrs in self._search_all(
                conn, self._people_dn, "(uid=*)", self._user_attributes()
            ):
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
            groups = {}
            for _, attrs in self._search_all(conn, self._groups_dn, "(cn=*)", ["cn", "gidNumber"]):
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
                attributes=self._user_attributes(),
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
            used = set()
            for _, attrs in self._search_all(conn, search_base, f"({attribute}=*)", [attribute]):
                for val in attrs.get(attribute) or []:
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

        With ``personal_groups`` off no group is looked at or written: the
        account carries ``gid_number`` as its primary GID and nothing else.
        """
        group_state = "skipped"
        if self.personal_groups:
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
        except EntryExistsError:
            # Another writer created the account between our read and our add,
            # and it may rely on the group we made: keep it.
            raise
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
                if _result_description(conn) == "entryAlreadyExists":
                    raise EntryExistsError(msg)
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
        """Delete a user and, when personal groups are kept, their personal group."""
        conn = self._connect()
        try:
            # Delete user entry
            user_dn = self._user_dn(username)
            conn.delete(user_dn)
            logger.info("Deleted LDAP user entry %s", username)

            if not self.personal_groups:
                # A group named after the user is not this account's to remove.
                return

            # Delete personal group
            group_dn = self._group_dn(username)
            conn.delete(group_dn)
            logger.info("Deleted LDAP personal group for %s", username)
        except LDAPException as e:
            raise BackendError(f"Failed to delete LDAP user {username}: {e}") from e
        finally:
            conn.unbind()

    def _rename_entry(self, dn: str, new_rdn: str) -> None:
        conn = self._connect()
        try:
            if not conn.modify_dn(dn, new_rdn, delete_old_dn=True):
                msg = f"Failed to rename LDAP entry {dn}: {conn.result}"
                description = _result_description(conn)
                if description == "noSuchObject":
                    raise EntryMissingError(msg)
                if description == "entryAlreadyExists":
                    raise EntryExistsError(msg)
                raise BackendError(msg)
        except LDAPException as e:
            raise BackendError(f"Failed to rename LDAP entry {dn}: {e}") from e
        finally:
            conn.unbind()

    def rename_user(self, old_username: str, new_username: str) -> None:
        """Rename an account's entry (modrdn); uidNumber and everything else stay."""
        self._rename_entry(self._user_dn(old_username), f"uid={escape_rdn(new_username)}")
        logger.info("Renamed LDAP user %s to %s", old_username, new_username)

    def rename_group(self, old_name: str, new_name: str) -> None:
        """Rename a group in groups_ou (modrdn); gidNumber and members stay."""
        self._rename_entry(self._group_dn(old_name), f"cn={escape_rdn(new_name)}")
        logger.info("Renamed LDAP group %s to %s", old_name, new_name)

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

    def add_user_attribute_value(self, username: str, attribute: str, value: str) -> None:
        """Add one value to a multi-valued attribute of an account; present already is fine."""
        with contextlib.suppress(ValueConflictError):
            self.modify_entry(self._user_dn(username), {attribute: [(MODIFY_ADD, [value])]})

    def remove_user_attribute_value(self, username: str, attribute: str, value: str) -> None:
        """Remove one value from an attribute of an account; absent already is fine."""
        with contextlib.suppress(ValueConflictError):
            self.modify_entry(self._user_dn(username), {attribute: [(MODIFY_DELETE, [value])]})

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
        """Names of every group under groups_ou listing ``username`` as memberUid.

        Only the posixGroup style. Use ``find_group_memberships`` to sweep an
        account out of every group it is in, whichever way it is listed.
        """
        return [
            group_name
            for group_name, membership_type in self.find_group_memberships(username)
            if membership_type == "memberUid"
        ]

    def find_group_memberships(self, username: str) -> list[tuple[str, str]]:
        """Every group under groups_ou listing ``username``, and how it lists them.

        Both styles are swept: ``memberUid`` (posixGroup, a bare name) and
        ``member`` (groupOfNames, the full DN). A sweep that knew only the first
        would leave a departing account inside every DN-style group it belongs
        to while reporting the release as done. The membership type travels with
        the name because it is what ``remove_user_from_group`` needs to drop it.
        A group listing the user both ways is returned once per style, so both
        are removed.
        """
        conn = self._connect()
        try:
            memberships = []
            for membership_type, value in (
                ("memberUid", username),
                ("member", self._user_dn(username)),
            ):
                # A wrong groups_ou raises here rather than reading as "no
                # groups": a sweep that looked at nothing must not report success.
                for _, attrs in self._search_all(
                    conn,
                    self._groups_dn,
                    f"({membership_type}={escape_filter_chars(value)})",
                    ["cn"],
                ):
                    cn = _first(attrs.get("cn"))
                    if cn:
                        memberships.append((str(cn), membership_type))
        except ContainerMissingError as e:
            msg = f"Cannot list the groups of {username}: {self._groups_dn} does not exist"
            raise BackendError(msg) from e
        except LDAPException as e:
            raise BackendError(f"Failed to list groups of {username}: {e}") from e
        finally:
            conn.unbind()
        return memberships

    def _groups_container_exists(self) -> bool:
        """Whether the groups container itself is present."""
        conn = self._connect()
        try:
            conn.search(
                self._groups_dn,
                "(objectClass=*)",
                search_scope=SUBTREE,
                attributes=["cn"],
            )
            return bool(conn.entries)
        except LDAPException:
            return False
        finally:
            conn.unbind()

    def groups_container_exists(self) -> bool:
        """Whether groups_ou exists; it is optional without personal groups."""
        return self._groups_container_exists()

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
                msg = f"Failed to create LDAP group {group_name}: {conn.result}"
                if _result_description(conn) == "entryAlreadyExists":
                    raise EntryExistsError(msg)
                raise BackendError(msg)
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
        try:
            self._create_group_entry(
                group_name=group_name,
                gid_number=gid_number,
                object_classes=object_classes,
                extra_attributes=extra_attributes,
                member_dn=member_dn,
            )
        except EntryExistsError:
            # Created between the check and the add, by another writer.
            existing_gid = self.get_group_gid(group_name)
            logger.info("LDAP group %s was created concurrently; using it", group_name)
            return "exists" if existing_gid == gid_number else "conflict"
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

    # ---- Entries addressed by DN ----
    #
    # Project groups and the entries that list them (a cluster's groupOfNames,
    # say) live outside groups_ou, wherever the operator's layout puts them, so
    # they are read and written by full DN rather than by name.

    def user_dn(self, username: str) -> str:
        """The DN an account of this name has under people_ou."""
        return self._user_dn(username)

    def dn_under(self, rdn_value: str, ou: str, rdn_attribute: str = "cn") -> str:
        """The DN of ``<rdn_attribute>=<rdn_value>`` in ``ou`` under the base DN."""
        return f"{rdn_attribute}={escape_rdn(rdn_value)},{ou},{self.base_dn}"

    def read_entry(self, dn: str, attributes: list[str]) -> Optional[dict]:
        """One entry's attributes, or None when it does not exist."""
        conn = self._connect()
        try:
            conn.search(dn, "(objectClass=*)", search_scope=BASE, attributes=attributes)
            if conn.entries:
                return conn.entries[0].entry_attributes_as_dict
            return None
        except LDAPException as e:
            raise BackendError(f"Failed to read LDAP entry {dn}: {e}") from e
        finally:
            conn.unbind()

    def list_entries(self, ou: str, search_filter: str, attributes: list[str]) -> dict:
        """Every entry under ``ou`` matching the filter, as ``{dn: attributes}``.

        One search for the whole container, so a reconcile reads it once per
        cycle. Raises when ``ou`` itself is missing: an empty answer from a
        container that does not exist would otherwise look like "nothing there
        yet" and every write that follows would fail one at a time. An empty
        ``ou`` searches the whole base DN.
        """
        search_base = f"{ou},{self.base_dn}" if ou else self.base_dn
        conn = self._connect()
        try:
            return dict(self._search_all(conn, search_base, search_filter, attributes))
        except LDAPException as e:
            raise BackendError(f"Failed to list entries under {search_base}: {e}") from e
        finally:
            conn.unbind()

    def gid_holders(self) -> dict[int, list[str]]:
        """Every gidNumber anywhere under the base DN, with the DNs holding it.

        Users' primary GIDs included. LDAP does not enforce gidNumber
        uniqueness, so this is what a writer checks before giving a GID out.
        """
        conn = self._connect()
        try:
            holders: dict[int, list[str]] = {}
            for dn, attrs in self._search_all(conn, self.base_dn, "(gidNumber=*)", ["gidNumber"]):
                gid = _first(attrs.get("gidNumber"))
                if gid is not None:
                    holders.setdefault(int(gid), []).append(dn)
            return holders
        except LDAPException as e:
            raise BackendError(f"Failed to enumerate gidNumber values: {e}") from e
        finally:
            conn.unbind()

    def add_entry(self, dn: str, attributes: dict) -> None:
        """Add one entry, raising BackendError on any failure."""
        conn = self._connect()
        try:
            if not conn.add(dn, attributes=attributes):
                msg = f"Failed to create LDAP entry {dn}: {conn.result}"
                if _result_description(conn) == "entryAlreadyExists":
                    raise EntryExistsError(msg)
                raise BackendError(msg)
        except LDAPException as e:
            raise BackendError(f"Failed to create LDAP entry {dn}: {e}") from e
        finally:
            conn.unbind()

    def modify_entry(self, dn: str, changes: dict) -> None:
        """Apply an ldap3 changes dict to one entry in a single modify."""
        conn = self._connect()
        try:
            if not conn.modify(dn, changes):
                msg = f"Failed to modify LDAP entry {dn}: {conn.result}"
                if _result_description(conn) in ("attributeOrValueExists", "noSuchAttribute"):
                    raise ValueConflictError(msg)
                raise BackendError(msg)
        except LDAPException as e:
            raise BackendError(f"Failed to modify LDAP entry {dn}: {e}") from e
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
            # noSuchAttribute: the user is not a member. noSuchObject: the group
            # itself is absent -- a typo in the config, or one an operator has
            # not created yet. Either way the membership this call exists to
            # remove does not exist, and an account being released must not be
            # held back by it.
            #
            # Unless the whole container is missing: then every group answers
            # noSuchObject, every sweep comes back empty, and tolerating it
            # would let a release report success having removed nothing.
            if (
                not success
                and "noSuchObject" in result_desc
                and not self._groups_container_exists()
            ):
                msg = (
                    f"Failed to remove {username} from group {group_name}: the groups "
                    f"container {self._groups_dn} does not exist"
                )
                raise BackendError(msg)
            tolerated = ("noSuchAttribute", "noSuchObject")
            if not success and not any(desc in result_desc for desc in tolerated):
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
            names = []
            for _, attrs in self._search_all(
                conn, self._groups_dn, f"(description={escape_filter_chars(value)})", ["cn"]
            ):
                cn = _first(attrs.get("cn"))
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


def uid_from_dn(dn: str) -> str:
    """The username a ``uid=`` DN names, or the empty string for any other DN."""
    return _extract_uid_from_dn(dn)


def normalize_dn(dn: str) -> str:
    """A DN in a form two spellings of the same entry compare equal in.

    Attribute names and values are lowercased and escapes undone, so
    ``CN=Proj,OU=projects`` and ``cn=proj, ou=projects`` match: directories
    hand back DNs in whatever case they were written with.
    """
    try:
        components = parse_dn(dn, strip=True)
    except LDAPInvalidDnError:
        return dn.strip().lower()
    return ",".join(
        f"{attribute.strip().lower()}={_unescape_dn_value(value).strip().lower()}"
        for attribute, value, _ in components
    )
