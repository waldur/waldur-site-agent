"""Pure decision logic for Waldur-authoritative LDAP reconciliation.

Nothing here touches a directory or an API. :func:`build_desired` turns an
offering user into the entry Waldur says should exist, and :func:`classify`
compares that against what the directory currently holds. Keeping both free of
I/O is what makes the interesting cases — a UID already taken by somebody else,
an entry whose ids drifted, a second account sharing an email — testable without
an LDAP server.

The caller is responsible for acting on the returned :class:`Decision`; see
``LdapUsernameBackend._reconcile_from_waldur``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.types import Unset

# Attributes carrying the account's POSIX identity, as opposed to its profile.
# A disagreement here is a drift; a disagreement anywhere else is just stale data.
POSIX_ATTRIBUTES = ("uidNumber", "gidNumber")


class SkipReason(str, Enum):
    """Why an offering user cannot be reconciled this cycle."""

    # Waldur has not assigned a POSIX login name yet (account still REQUESTED,
    # or the service provider has not set one). Ordinary and transient.
    NO_USERNAME = "no_username"
    # The server did not return the POSIX fields at all. Almost always means the
    # Mastermind predates them, or the agent is not requesting them — an
    # offering-wide condition, so the caller logs it once rather than per user.
    IDS_UNSET = "ids_unset"
    # The server returned them as null: no PosixIdPool resolves for the offering,
    # or enable_posix_account is off. A per-account condition.
    IDS_MISSING = "ids_missing"


class Outcome(str, Enum):
    """What should happen to one account."""

    CREATE = "create"  # nothing in the directory yet, and the UID is free
    NOOP = "noop"  # directory already agrees
    UPDATE = "update"  # profile / home / shell differ; safe to rewrite
    DRIFT = "drift"  # uidNumber or gidNumber differ; policy decides
    UID_TAKEN = "uid_taken"  # a different entry already holds this UID


@dataclass
class DesiredEntry:
    """The directory entry Waldur says should exist for one offering user."""

    username: str
    uid_number: int
    gid_number: int
    home_directory: str
    login_shell: str
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    waldur_username: Optional[str] = None

    @property
    def common_name(self) -> str:
        """The cn to write: full name when we have one, else the login name."""
        return f"{self.first_name} {self.last_name}".strip() or self.username


@dataclass
class Decision:
    """The verdict for one account, plus everything needed to act on or report it."""

    outcome: Outcome
    # Attribute -> value, for Outcome.UPDATE. Ready to hand to update_user_attributes.
    updates: dict = field(default_factory=dict)
    # Attribute -> (actual, desired), for Outcome.DRIFT. Reporting only.
    diff: dict = field(default_factory=dict)
    # uid of the entry already holding the wanted uidNumber, for Outcome.UID_TAKEN.
    uid_taken_by: Optional[str] = None
    # uid of another entry carrying the same email. A warning, never a blocker:
    # one person legitimately holds several accounts in a shared directory.
    duplicate_mail_owner: Optional[str] = None


def _text(value: object) -> str:
    """An OfferingUser string field as a plain str, treating UNSET/None as empty."""
    if value is None or isinstance(value, Unset):
        return ""
    return str(value)


def _number(value: object) -> Optional[int]:
    """An OfferingUser integer field as an int, or None when absent."""
    if value is None or isinstance(value, Unset):
        return None
    if isinstance(value, int):
        return value
    return int(str(value))


def attr(entry: Optional[dict], name: str) -> Optional[object]:
    """One attribute of an ldap3 ``entry_attributes_as_dict``, unwrapped.

    ldap3 hands back a list for every attribute; an attribute that is present but
    empty comes back as ``[]``, which must read as absent rather than as a value.
    """
    if not entry:
        return None
    value = entry.get(name)
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _attr_text(entry: Optional[dict], name: str) -> str:
    value = attr(entry, name)
    return "" if value is None else str(value)


def _attr_number(entry: Optional[dict], name: str) -> Optional[int]:
    value = attr(entry, name)
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except ValueError:
        return None


def build_desired(
    offering_user: OfferingUser,
    *,
    default_home_base: str,
    default_login_shell: str,
    waldur_username_attribute: str = "",
) -> tuple[Optional[DesiredEntry], Optional[SkipReason]]:
    """Turn an offering user into the entry that should exist, or say why not.

    Exactly one of the two return values is set. ``home_directory`` and
    ``login_shell`` fall back to the configured defaults when Waldur holds none —
    unlike the ids, a local default for those is harmless. The ids never fall
    back: inventing one here is precisely the dual-allocation this mode exists to
    remove.
    """
    username = _text(getattr(offering_user, "username", None))
    if not username:
        return None, SkipReason.NO_USERNAME

    raw_uid = getattr(offering_user, "uidnumber", None)
    raw_gid = getattr(offering_user, "primarygroup", None)
    # UNSET (field never came back) and None (came back null) mean different
    # things to an operator, so they are reported differently.
    if isinstance(raw_uid, Unset) and isinstance(raw_gid, Unset):
        return None, SkipReason.IDS_UNSET

    uid_number = _number(raw_uid)
    gid_number = _number(raw_gid)
    if uid_number is None or gid_number is None:
        return None, SkipReason.IDS_MISSING

    home_directory = _text(getattr(offering_user, "home_directory", None))
    if not home_directory:
        home_directory = f"{default_home_base.rstrip('/')}/{username}"
    login_shell = _text(getattr(offering_user, "login_shell", None)) or default_login_shell

    waldur_username = _text(getattr(offering_user, "user_username", None))
    return (
        DesiredEntry(
            username=username,
            uid_number=uid_number,
            gid_number=gid_number,
            home_directory=home_directory,
            login_shell=login_shell,
            first_name=_text(getattr(offering_user, "user_first_name", None)),
            last_name=_text(getattr(offering_user, "user_last_name", None)),
            email=_text(getattr(offering_user, "user_email", None)),
            waldur_username=(
                waldur_username if waldur_username and waldur_username_attribute else None
            ),
        ),
        None,
    )


def classify(
    desired: DesiredEntry,
    actual: Optional[dict],
    *,
    uid_owner: Optional[str] = None,
    mail_owner: Optional[str] = None,
    waldur_username_attribute: str = "",
) -> Decision:
    """Compare the wanted entry against the directory and decide what to do.

    ``actual`` is the existing entry for ``desired.username``, or None.
    ``uid_owner`` is the uid of whichever *other* entry already holds
    ``desired.uid_number``; ``mail_owner`` likewise for the email address. Both
    are resolved by the caller from its bulk read.

    ``uid_owner`` blocks two distinct cases: creating an account on a taken UID,
    and renumbering a drifted account onto one.
    """
    if actual is None:
        # Case 5 outranks creation: taking a UID that belongs to somebody else
        # would break that account, and no policy makes that acceptable.
        if uid_owner is not None and uid_owner != desired.username:
            return Decision(outcome=Outcome.UID_TAKEN, uid_taken_by=uid_owner)
        # Case 6 does not block — a shared directory legitimately holds more than
        # one account per person — but it is worth saying out loud.
        return Decision(
            outcome=Outcome.CREATE,
            duplicate_mail_owner=(
                mail_owner if mail_owner and mail_owner != desired.username else None
            ),
        )

    # Case 4: the POSIX identity itself disagrees. Reported as a diff and left to
    # policy — rewriting a live uidNumber orphans every file the account owns.
    diff = {}
    actual_uid = _attr_number(actual, "uidNumber")
    actual_gid = _attr_number(actual, "gidNumber")
    if actual_uid != desired.uid_number:
        diff["uidNumber"] = (actual_uid, desired.uid_number)
    if actual_gid != desired.gid_number:
        diff["gidNumber"] = (actual_gid, desired.gid_number)
    if diff:
        # A drifted uidNumber cannot be adopted onto a value another entry already
        # holds. LDAP does not enforce uidNumber uniqueness, so the write would
        # succeed and leave two accounts sharing ownership of every file -- the
        # same reason the creation path refuses it above, and no policy makes it
        # acceptable either.
        if "uidNumber" in diff and uid_owner is not None and uid_owner != desired.username:
            return Decision(outcome=Outcome.UID_TAKEN, uid_taken_by=uid_owner)
        return Decision(outcome=Outcome.DRIFT, diff=diff)

    # Case 3: everything else is cheap and safe to bring into line.
    wanted = [
        ("homeDirectory", desired.home_directory),
        ("loginShell", desired.login_shell),
        ("cn", desired.common_name),
        ("mail", desired.email),
        ("givenName", desired.first_name or desired.username),
        ("sn", desired.last_name or desired.username),
    ]
    if waldur_username_attribute and desired.waldur_username:
        wanted.append((waldur_username_attribute, desired.waldur_username))
    updates = {name: value for name, value in wanted if value and _attr_text(actual, name) != value}

    if updates:
        return Decision(outcome=Outcome.UPDATE, updates=updates)
    return Decision(outcome=Outcome.NOOP)


def index_by_uid_number(users: dict) -> dict[int, str]:
    """``{uidNumber: uid}`` over a bulk :meth:`LdapClient.list_users` result."""
    index: dict[int, str] = {}
    for uid, entry in users.items():
        number = _attr_number(entry, "uidNumber")
        if number is not None:
            index.setdefault(number, uid)
    return index


def index_by_mail(users: dict) -> dict[str, str]:
    """``{mail: uid}`` over a bulk :meth:`LdapClient.list_users` result."""
    index: dict[str, str] = {}
    for uid, entry in users.items():
        mail = _attr_text(entry, "mail")
        if mail:
            index.setdefault(mail.lower(), uid)
    return index
