"""Provider project groups: Waldur's groups written into the directory.

Waldur keeps one group per project at a service provider, with a name and a GID
it allocated and the usernames of the project's members. Every offering of the
provider sees the same groups, so several agents writing one directory write
each group once, with the same GID, and converge on the same members.

The agent never allocates a GID here and never deletes a group entry: a GID is
reserved for as long as files may carry it, and Waldur owns that decision.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import httpx
from ldap3 import MODIFY_ADD, MODIFY_DELETE, MODIFY_REPLACE
from waldur_api_client.client import AuthenticatedClient
from waldur_site_agent_ldap_client import EntryExistsError, LdapClient, ValueConflictError
from waldur_site_agent_ldap_client.client import normalize_dn, uid_from_dn

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

PROJECT_GROUPS_PATH = "/api/marketplace-service-provider-project-groups/"
PAGE_SIZE = 100
# A provider with more groups than this many pages would be its own problem; the
# bound only stops a server that keeps answering with a next link.
MAX_PAGES = 10000


def fetch_provider_project_groups(
    waldur_rest_client: AuthenticatedClient, offering_uuid: str
) -> list[dict]:
    """Every project group of the service provider that owns ``offering_uuid``.

    All of them, in use or not: the reconcile needs the unused ones too, to
    know which DNs it may take back out of a parent entry. Raw HTTP over the
    agent's authenticated client, because the generated API client does not
    carry this endpoint yet; keep it to this one function so it can be swapped.
    """
    http = waldur_rest_client.get_httpx_client()
    url = PROJECT_GROUPS_PATH
    params: Optional[dict] = {
        "provider_offering_uuid": offering_uuid,
        "page_size": PAGE_SIZE,
        # Oldest first: a group created mid-fetch lands on the last page
        # instead of shifting every page boundary after it.
        "o": "created",
    }
    groups: list[dict] = []
    counts: set[str] = set()
    for _ in range(MAX_PAGES):
        response = http.get(url, params=params)
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            msg = f"Unexpected project group listing: {type(page).__name__}"
            raise BackendError(msg)
        groups.extend(page)
        if response.headers.get("X-Result-Count") is not None:
            counts.add(response.headers["X-Result-Count"])
        next_link = response.links.get("next")
        if not next_link or not next_link.get("url"):
            _check_complete(groups, counts)
            return groups
        # The next link carries the query string already. It is followed only
        # to the server the agent was configured for: a link elsewhere would be
        # sent the agent's token.
        url, params = _same_origin(http.base_url, next_link["url"]), None
    msg = f"Listing project groups did not finish within {MAX_PAGES} pages"
    raise BackendError(msg)


def _same_origin(base_url: httpx.URL, link: str) -> str:
    target = httpx.URL(link)
    if target.is_relative_url:
        return link
    if (target.scheme, target.host, target.port) != (base_url.scheme, base_url.host, base_url.port):
        where = target.copy_with(query=None)
        msg = f"Refusing to follow a next-page link to another server: {where}"
        raise BackendError(msg)
    return link


def _check_complete(groups: list[dict], counts: set[str]) -> None:
    """Refuse a listing that changed while it was being paged through.

    A group created mid-fetch can shift the page boundaries, so one group is
    seen twice and another not at all -- and a group missing from the listing
    is one whose DN the reconcile takes out of the parents. Better to skip a
    cycle than to act on that.
    """
    unique = {str(g.get("uuid") or g.get("name")) for g in groups}
    if len(counts) > 1 or (counts and len(unique) != int(next(iter(counts)))):
        msg = (
            f"The project group listing changed while it was read ({len(unique)} "
            f"distinct groups, result counts {sorted(counts)}); retrying next cycle"
        )
        raise BackendError(msg)


def normalize_uuid(value: object) -> str:
    """UUIDs compare equal with or without dashes, in either case."""
    return str(value).replace("-", "").lower()


@dataclass
class ProjectGroup:
    """The part of a Waldur project group the directory needs."""

    name: str
    gid: Optional[int]
    members: list[str]
    offering_uuids: set[str]
    customer_slug: str = ""

    @classmethod
    def from_api(cls, item: dict) -> ProjectGroup:
        """Parse one item of the endpoint's response."""
        gid = item.get("gid")
        return cls(
            name=str(item.get("name") or ""),
            gid=int(gid) if gid is not None else None,
            members=sorted({str(m) for m in item.get("members") or [] if m}),
            offering_uuids={
                normalize_uuid(offering["uuid"])
                for offering in item.get("offerings") or []
                if isinstance(offering, dict) and offering.get("uuid")
            },
            customer_slug=str(item.get("customer_slug") or ""),
        )


@dataclass
class ReconcileReport:
    """What one reconcile pass did, for the summary line."""

    created: int = 0
    kept: int = 0
    renumbered: int = 0
    conflicts: int = 0
    skipped: int = 0
    failed: int = 0
    member_updates: int = 0
    parent_updates: int = 0
    description_updates: int = 0


def _first(value: object) -> object:
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _values(entry: dict, attribute: str) -> list[str]:
    """An attribute's values as strings, whatever case the directory spells it in."""
    for name, value in entry.items():
        if name.lower() == attribute.lower():
            if value is None:
                return []
            values = value if isinstance(value, (list, tuple)) else [value]
            return [str(v) for v in values if v is not None and v != ""]
    return []


def _plain(value: object) -> str:
    """A setting as its plain string.

    Core dumps a validated settings block with enum instances in it, and
    ``str()`` of a str-mixin enum is its qualified name, not its value.
    """
    if value is None:
        return ""
    return str(getattr(value, "value", value))


def _operations(to_add: list[str], to_remove: list[str]) -> list:
    operations = []
    if to_add:
        operations.append((MODIFY_ADD, to_add))
    if to_remove:
        operations.append((MODIFY_DELETE, to_remove))
    return operations


def _needs_a_member(attribute: str, object_classes: list[str]) -> bool:
    """Whether emptying ``attribute`` would leave an entry its schema rejects."""
    classes = {c.lower() for c in object_classes}
    if attribute.lower() == "member":
        return "groupofnames" in classes
    if attribute.lower() == "uniquemember":
        return "groupofuniquenames" in classes
    return False


def _is_group_of_names(object_classes: list[str]) -> bool:
    return "groupofnames" in {c.lower() for c in object_classes}


_WARNED_BARE_TEMPLATES: set[str] = set()
_WARNED_MANAGED_MARKER: set[bool] = set()


def _template_pattern(template: str) -> Optional[re.Pattern]:
    """A pattern matching any value rendered from ``template``; None if it cannot.

    A template that is nothing but ``{slug}`` matches every value, so values
    written from it cannot be told from the operator's own.
    """
    if not template or "{slug}" not in template:
        return None
    prefix, _, suffix = template.partition("{slug}")
    if not prefix and not suffix:
        return None
    return re.compile(re.escape(prefix) + r".+" + re.escape(suffix))


def _warn_once_about_managed_marker() -> None:
    if _WARNED_MANAGED_MARKER:
        return
    _WARNED_MANAGED_MARKER.add(True)
    logger.warning(
        "project_groups.managed_marker is set but ignored: project groups are no longer "
        "marked. Remove the setting; existing marker values in the directory are left "
        "as they are"
    )


def _warn_once_about_bare_organization_template(template: str) -> None:
    if template in _WARNED_BARE_TEMPLATES:
        return
    _WARNED_BARE_TEMPLATES.add(template)
    logger.warning(
        "project_groups.organization_description is %r: with no text around {slug} "
        "the agent cannot tell its value from other descriptions, so it adds the "
        "current slug but never removes an old one. Use e.g. 'organization={slug}' "
        "to have a changed slug replaced.",
        template,
    )


class ProjectGroupReconciler:
    """Converge the project-group OU and its parent entries on Waldur's groups."""

    def __init__(
        self,
        client: LdapClient,
        settings: dict,
        offering_uuid: str,
        require_user_entries: bool = False,
        key_attribute: str = "",
        excluded_members: Optional[set[str]] = None,
    ) -> None:
        """Read the ``project_groups`` settings block as a raw dict.

        ``require_user_entries``: list only members whose account has an entry
        under people_ou. Set when this agent writes the accounts itself, so a
        member whose account it could not write this cycle (a UID conflict, a
        failed add) is not named in a group either; it joins once the account
        exists. DN members are always held to this -- a DN must name an entry.

        ``key_attribute``: with it, a member's entry must also carry a Waldur
        username key, which the account pass writes only on an entry it matched
        to its account. ``excluded_members``: usernames the account pass could
        not reconcile this cycle (a UID or key conflict, unadopted drift); an
        entry under such a name may be someone else's and is not named.
        """
        self.client = client
        self.require_user_entries = require_user_entries
        self.key_attribute = key_attribute
        self.excluded_members = set(excluded_members or ())
        self.ou = settings.get("ou") or "ou=projects"
        self.object_classes = list(settings.get("object_classes") or ["top", "posixGroup"])
        self.member_attribute = _plain(settings.get("member_attribute")) or "memberUid"
        self.add_only = _plain(settings.get("membership")) == "add_only"
        self.adopt_gid = _plain(settings.get("on_gid_mismatch")) == "adopt"
        self.parents = list(settings.get("parents") or [])
        if settings.get("managed_marker"):
            _warn_once_about_managed_marker()
        self.organization_template = settings.get("organization_description") or ""
        self._organization_pattern = _template_pattern(self.organization_template)
        if self.organization_template and self._organization_pattern is None:
            _warn_once_about_bare_organization_template(self.organization_template)
        self.offering_uuid = normalize_uuid(offering_uuid)
        self._ou_suffix = "," + normalize_dn(f"{self.ou},{client.base_dn}")

    def _in_project_ou(self, key: str) -> bool:
        return key.endswith(self._ou_suffix)

    def run(self, groups: list[ProjectGroup]) -> ReconcileReport:
        """One pass: groups first, then the parent entries that list them.

        A fixed number of bulk reads of the directory -- the project OU, every
        gidNumber under the base DN, every posixGroup under it, and the people
        OU when members are written as DNs -- however many groups there are.

        An empty listing writes nothing at all. The endpoint lists every group
        the provider has ever had, so "none" means a broken answer far more often
        than an empty provider, and acting on it would strip every parent entry.
        """
        report = ReconcileReport()
        if not groups:
            logger.warning(
                "Waldur listed no project groups for this provider; LDAP project groups "
                "and parent entries were left unchanged"
            )
            return report

        existing = {
            normalize_dn(dn): (dn, attrs)
            for dn, attrs in self.client.list_entries(
                self.ou,
                "(cn=*)",
                ["cn", "gidNumber", "objectClass", "description", self.member_attribute],
            ).items()
        }
        holders = {
            gid: {normalize_dn(dn) for dn in dns}
            for gid, dns in self.client.gid_holders().items()
        }
        # Same-cn posixGroups elsewhere in the directory: NSS would see two
        # groups of one name, so such a name counts as held.
        name_holders: dict[str, list[str]] = {}
        for dn, attrs in self.client.list_entries(
            "", "(objectClass=posixGroup)", ["cn"]
        ).items():
            if self._in_project_ou(normalize_dn(dn)):
                continue
            for cn in _values(attrs, "cn"):
                name_holders.setdefault(cn.lower(), []).append(dn)
        user_names: Optional[set[str]] = None
        if self.member_attribute == "member" or self.require_user_entries:
            user_names = {
                uid
                for uid, entry in self.client.list_users().items()
                if not self.key_attribute or _values(entry, self.key_attribute)
            } - self.excluded_members

        # The groups this pass reconciled in the directory. Only these are added
        # to a parent; a group Waldur lists but that could not be written (its
        # GID or name held elsewhere) is not.
        managed: dict[str, tuple[str, ProjectGroup]] = {}
        # Groups Waldur lists without a GID: their DNs in a parent are left as
        # they are, since Waldur cannot yet say anything about them.
        untouchable: set[str] = set()
        seen: set[str] = set()
        # DNs a parent may lose: groups Waldur lists. An entry under the project
        # OU that Waldur does not list -- hand-made, or a group of an offering
        # since moved to another provider -- is never removed.
        removable: set[str] = set()
        for group in groups:
            if not group.name:
                report.skipped += 1
                continue
            dn = self.client.dn_under(group.name, self.ou)
            key = normalize_dn(dn)
            removable.add(key)
            if group.gid is None:
                logger.warning(
                    "Project group %s has no GID in Waldur (no pool could supply one); "
                    "not written to LDAP",
                    group.name,
                )
                untouchable.add(key)
                report.skipped += 1
                continue
            if key in seen:
                logger.error(
                    "Waldur lists project group %s twice; only the first is written", group.name
                )
                report.conflicts += 1
                continue
            seen.add(key)
            context = _GroupContext(
                existing.get(key), holders, name_holders.get(group.name.lower(), []), user_names
            )
            try:
                self._reconcile_group(group, group.gid, dn, key, context, managed, report)
            except BackendError:
                logger.exception("Failed to reconcile LDAP project group %s", group.name)
                report.failed += 1
                # A failure this cycle says nothing about the group: its DN stays
                # in the parents as it is until a cycle gets through.
                untouchable.add(key)

        for parent in self.parents:
            try:
                if self._reconcile_parent(parent, managed, removable - untouchable):
                    report.parent_updates += 1
            except BackendError:
                logger.exception("Failed to update parent entry %s", parent.get("dn"))
                report.failed += 1
        return report

    # ---- One group ----

    def _reconcile_group(
        self,
        group: ProjectGroup,
        gid: int,
        dn: str,
        key: str,
        context: _GroupContext,
        managed: dict[str, tuple[str, ProjectGroup]],
        report: ReconcileReport,
    ) -> None:
        holders = context.holders
        others = sorted(holders.get(gid, set()) - {key})

        if context.entry is None:
            if others or context.same_name:
                logger.error(
                    "Project group %s (GID %d) was not created: %s. Resolve the collision "
                    "in the directory, or set the group's GID in Waldur to a free value.",
                    group.name,
                    gid,
                    "; ".join(
                        ([f"GID {gid} is held by {', '.join(others)}"] if others else [])
                        + (
                            [f"posixGroup {', '.join(context.same_name)} has the same name"]
                            if context.same_name
                            else []
                        )
                    ),
                )
                report.conflicts += 1
                return
            try:
                self.client.add_entry(dn, self._new_group_attributes(group, context.user_names))
            except EntryExistsError:
                # Another agent on the same directory created it after our bulk
                # read: carry on with it as an existing entry.
                fresh = self.client.read_entry(
                    dn, ["cn", "gidNumber", "objectClass", "description", self.member_attribute]
                )
                if fresh is None:
                    raise
                logger.info(
                    "LDAP project group %s was created concurrently; reconciling it", group.name
                )
                context.entry = (dn, fresh)
            else:
                holders.setdefault(gid, set()).add(key)
                managed[key] = (dn, group)
                report.created += 1
                logger.info(
                    "Created LDAP project group %s (GID %d, %d members)",
                    group.name,
                    gid,
                    len(group.members),
                )
                return
        if context.entry is None:
            return

        entry_dn, attrs = context.entry
        if context.same_name:
            logger.warning(
                "LDAP project group %s shares its name with posixGroup %s; NSS sees two "
                "groups named %s",
                entry_dn,
                ", ".join(context.same_name),
                group.name,
            )
        raw_gid = _first(_values(attrs, "gidNumber"))
        current_gid = int(str(raw_gid)) if raw_gid not in (None, "") else None
        if current_gid == gid:
            report.kept += 1
        elif self.adopt_gid and not others:
            self.client.modify_entry(entry_dn, {"gidNumber": [(MODIFY_REPLACE, [gid])]})
            if current_gid is not None:
                holders.get(current_gid, set()).discard(key)
            holders.setdefault(gid, set()).add(key)
            report.renumbered += 1
            logger.warning(
                "Renumbered LDAP group %s from %s to Waldur's GID %d. Files owned by the "
                "old GID need chgrp-ing to the new one.",
                entry_dn,
                current_gid,
                gid,
            )
        else:
            # The GID stays as it is -- renumbering orphans files -- but the
            # group is still Waldur's project group: members and parents follow.
            if self.adopt_gid:
                reason = f"GID {gid} is held by {', '.join(others)}"
            else:
                reason = (
                    "on_gid_mismatch is 'report'; renumbering a group orphans the files it "
                    "owns. Set the GID in Waldur to the directory's, or set 'adopt' after "
                    "a filesystem chgrp"
                )
            logger.error(
                "LDAP group %s has gidNumber %s, Waldur's project group has %d; the GID "
                "was left unchanged (%s). Members and parent entries are still reconciled.",
                entry_dn,
                current_gid,
                gid,
                reason,
            )
            report.conflicts += 1

        managed[key] = (entry_dn, group)
        if self._sync_organization_description(entry_dn, attrs, group):
            report.description_updates += 1
        if self._sync_members(entry_dn, attrs, group, context.user_names):
            report.member_updates += 1

    def _organization_value(self, group: ProjectGroup) -> str:
        """The description value naming the group's organization, or ''."""
        if not self.organization_template or not group.customer_slug:
            return ""
        return self.organization_template.replace("{slug}", group.customer_slug)

    def _sync_organization_description(self, dn: str, attrs: dict, group: ProjectGroup) -> bool:
        """Add the organization value, replacing a stale one; True when written.

        Only a value matching the template's literal text is ever removed:
        everything else in ``description`` is the operator's. A group whose
        project is gone keeps what it has.
        """
        desired = self._organization_value(group)
        if not desired:
            return False
        current = _values(attrs, "description")
        stale = []
        if self._organization_pattern is not None:
            stale = [
                value
                for value in current
                if value != desired
                and self._organization_pattern.fullmatch(value)
            ]
        to_add = [] if desired in current else [desired]
        if not stale and not to_add:
            return False
        self.client.modify_entry(dn, {"description": _operations(to_add, stale)})
        logger.info(
            "LDAP group %s: organization description set to %r%s",
            dn,
            desired,
            f", replacing {', '.join(map(repr, stale))}" if stale else "",
        )
        return True

    def _member_values(self, group: ProjectGroup, user_names: Optional[set[str]]) -> list[str]:
        # A member must name an entry: an account Waldur lists before this agent
        # has written it (still being created, or held back by a conflict)
        # joins on a later cycle.
        present = [u for u in group.members if user_names is None or u in user_names]
        if len(present) != len(group.members):
            logger.info(
                "Project group %s: not listing %s, no LDAP entry yet",
                group.name,
                ", ".join(u for u in group.members if u not in present),
            )
        if self.member_attribute != "member":
            return present
        return [self.client.user_dn(username) for username in present]

    def _new_group_attributes(self, group: ProjectGroup, user_names: Optional[set[str]]) -> dict:
        attributes: dict = {
            "objectClass": self.object_classes,
            "cn": group.name,
            "gidNumber": group.gid,
        }
        organization = self._organization_value(group)
        if organization:
            attributes["description"] = [organization]
        members = self._member_values(group, user_names)
        if members:
            attributes[self.member_attribute] = members
        if _is_group_of_names(self.object_classes) and not attributes.get("member"):
            # groupOfNames cannot exist without a member.
            attributes["member"] = [self.client.empty_group_member_dn]
        return attributes

    def _sync_members(
        self, dn: str, attrs: dict, group: ProjectGroup, user_names: Optional[set[str]]
    ) -> bool:
        """Make the group's members match Waldur's; True when something was written."""
        attribute = self.member_attribute
        current = _values(attrs, attribute)
        desired = self._member_values(group, user_names)
        if attribute == "member":
            current_by_key = {normalize_dn(v): v for v in current}
            desired_by_key = {normalize_dn(v): v for v in desired}
            to_add = [v for k, v in desired_by_key.items() if k not in current_by_key]
            # Only account DNs are Waldur's to remove: a nested group or the
            # stand-in member was put there by someone else, for their reasons.
            to_remove = [
                v
                for k, v in current_by_key.items()
                if k not in desired_by_key and uid_from_dn(v)
            ]
        else:
            to_add = [v for v in desired if v not in set(current)]
            to_remove = [v for v in current if v not in set(desired)]
        if self.add_only:
            to_remove = []
        return self._modify_members(dn, attribute, attrs, current, to_add, to_remove, group.name)

    def _modify_members(
        self,
        dn: str,
        attribute: str,
        attrs: dict,
        current: list[str],
        to_add: list[str],
        to_remove: list[str],
        label: str,
    ) -> bool:
        """One modify; a groupOfNames emptied by it gets the stand-in in the same write."""
        if (
            to_remove
            and len(to_remove) == len(current)
            and not to_add
            and _needs_a_member(attribute, _values(attrs, "objectClass"))
        ):
            to_add = [self.client.empty_group_member_dn]
        if not to_add and not to_remove:
            return False
        try:
            self.client.modify_entry(dn, {attribute: _operations(to_add, to_remove)})
        except ValueConflictError:
            # Another writer changed the same values since our read: re-read,
            # keep only what is still to do, and apply that once.
            fresh = self.client.read_entry(dn, [attribute, "objectClass"])
            if fresh is None:
                raise

            def key(value: str) -> str:
                return normalize_dn(value) if attribute.lower() == "member" else value

            present = {key(v) for v in _values(fresh, attribute)}
            to_add = [v for v in to_add if key(v) not in present]
            to_remove = [v for v in to_remove if key(v) in present]
            if not to_add and not to_remove:
                logger.info("%s: already changed by another writer", label)
                return False
            self.client.modify_entry(dn, {attribute: _operations(to_add, to_remove)})
        logger.info(
            "%s: added %s, removed %s",
            label,
            ", ".join(to_add) or "nothing",
            ", ".join(to_remove) or "nothing",
        )
        return True

    # ---- Parent entries ----

    def _reconcile_parent(
        self,
        parent: dict,
        managed: dict[str, tuple[str, ProjectGroup]],
        removable: set[str],
    ) -> bool:
        """List the DN of every managed group whose project uses one of the parent's offerings.

        A DN is only ever removed if it lies under the project OU and is in
        ``removable``: a group Waldur lists (with a GID). It then goes when the
        group is not wanted here -- no resource on these offerings, or not
        written because its GID or name is held. Everything else, including
        hand-made groups under the project OU and groups Waldur no longer
        lists, stays.
        """
        parent_dn = parent["dn"]
        attribute = parent.get("attribute") or "member"
        offerings = {normalize_uuid(u) for u in parent.get("offering_uuids") or []} or {
            self.offering_uuid
        }
        desired = {
            key: dn for key, (dn, group) in managed.items() if group.offering_uuids & offerings
        }
        entry = self.client.read_entry(parent_dn, [attribute, "objectClass"])
        if entry is None:
            msg = f"parent entry {parent_dn} does not exist"
            raise BackendError(msg)
        current_values = _values(entry, attribute)
        current = {normalize_dn(v): v for v in current_values}
        to_add = [dn for key, dn in desired.items() if key not in current]
        to_remove = [
            value
            for key, value in current.items()
            if key not in desired and self._in_project_ou(key) and key in removable
        ]
        return self._modify_members(
            parent_dn,
            attribute,
            entry,
            current_values,
            to_add,
            to_remove,
            f"Parent entry {parent_dn}",
        )


@dataclass
class _GroupContext:
    """What the bulk reads say about one group's name and GID."""

    entry: Optional[tuple[str, dict]]
    holders: dict[int, set[str]]
    same_name: list[str]
    user_names: Optional[set[str]]
