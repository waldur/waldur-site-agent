"""LiteLLM backend: manages virtual keys on a LiteLLM proxy.

This is the management (order-processing) backend. Token usage is reported by the
separate ``litellm-usage`` reporting backend, so ``_get_usage_report`` here returns
nothing.

The proxy stores keys sha256-hashed and hands out the plaintext exactly once, in the
``/key/generate`` response. The agent therefore never holds a key after provisioning
(Waldur does, encrypted) and addresses every later operation by the key's alias — the
slot name ``<resource_backend_id>-<n>`` this module assigns — resolving it to the hash
the proxy needs through ``/key/list``.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterator
from typing import Optional

from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import DEFAULT_RESOURCE_KEY_COUNT, backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo
from waldur_site_agent.common import WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES

from .client import (
    META_RESOURCE,
    META_USERNAME,
    LiteLLMBackendError,
    LiteLLMClient,
    LiteLLMEnterpriseFeatureError,
)
from .openwebui import ROLE_ACTIVE, ROLE_DISABLED, OpenWebUIClient, OpenWebUIError

logger = logging.getLogger(__name__)

# Inference resources get two keys so one can be rotated with no downtime. Taken from
# the core, because common.utils reconciles the count against what Waldur already
# holds and so has to agree with this module on the target.
_DEFAULT_KEY_COUNT = DEFAULT_RESOURCE_KEY_COUNT

# Waldur limit names this backend mirrors onto the key as a metering backstop, and the
# LiteLLM field each maps to. Waldur remains the enforcement authority (report -> pause
# -> block); these only stop a burst between two reporting cycles from outrunning it.
_LIMIT_FIELDS = {
    "token_cost": "max_budget",
    "tpm": "tpm_limit",
    "rpm": "rpm_limit",
}

# How long the proxy's internal-user listing stays reusable, in seconds. ``/user/list``
# cannot be filtered by metadata, so establishing which users belong to one resource
# means walking every internal user on the proxy -- and the membership processor pulls
# one resource at a time, which without this would repeat that walk per resource per
# pass. Half the membership-sync period for the same reason the usage cache uses half
# the report period: it covers the whole of one pass and expires before the next.
_USER_CACHE_TTL = WALDUR_SITE_AGENT_MEMBERSHIP_SYNC_PERIOD_MINUTES * 60 * 0.5

# Account provisioning modes for the chat surface.
#
# ``sso``: Open WebUI is fronted by an identity provider (the same one as Waldur), so
# an account materializes on first login with the address the IdP asserts. The agent
# creates nothing -- there is no password to mint, transport or store -- and only ever
# revokes. This is the target state and the default.
#
# ``managed_password``: the agent creates the account itself with an operator-supplied
# temporary password, mirroring what an admin does by hand today. It exists because
# that is where most deployments start, not because it is good: Waldur has no encrypted
# channel to hand a password to an end user, so the same initial secret is used for
# every account and the person is expected to change it on first login.
PROVISIONING_SSO = "sso"
PROVISIONING_MANAGED_PASSWORD = "managed_password"  # noqa: S105 - a mode name, not a secret


class LiteLLMBackend(backends.BaseBackend):
    """Provisions LiteLLM virtual keys from Waldur orders."""

    # LiteLLM mints each key and returns the plaintext once; the agent applies it and
    # only then reports it to Waldur (encrypted) via the provider endpoints. A resource
    # owns many keys, each a "<backend_id>-<n>" alias.
    supports_resource_api_keys = True

    def __init__(
        self, backend_settings: dict, backend_components: dict[str, dict]
    ) -> None:
        """Initialize the backend from offering settings and components."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "litellm"

        api_url = backend_settings.get("api_url")
        if not api_url:
            msg = "LiteLLM backend requires 'api_url' in backend_settings"
            raise BackendError(msg)
        self.api_url = str(api_url).rstrip("/")

        # Optional per-offering allowlist pushed onto every key the offering mints.
        # An empty list means "every model the proxy serves"; LiteLLM treats an absent
        # `models` that way, so it is only sent when non-empty.
        models = backend_settings.get("models") or []
        self.models = [str(model) for model in models]
        self.budget_duration = backend_settings.get("budget_duration")
        self.default_tpm_limit = backend_settings.get("tpm_limit")
        self.default_rpm_limit = backend_settings.get("rpm_limit")

        # The limits of the resource currently being provisioned, as
        # ``(backend_id, limits)``. The core mints the keys in a separate call
        # (``generate_resource_keys``) that carries only the backend id, so the limits
        # read in ``_provision`` are parked here for it. Same process, same order, a
        # few calls apart; anything that misses this window is repaired by
        # ``sync_resource_limits`` on the next cycle.
        self._pending_limits: Optional[tuple[str, dict]] = None

        self.litellm_client = LiteLLMClient(backend_settings)

        # Optional chat surface. Absent settings mean "API only": the offering sells
        # keys and nothing here touches Open WebUI, which is why this is a nested block
        # rather than a set of top-level settings -- an offering that does not sell chat
        # should not have to name empty values for it.
        openwebui_settings = backend_settings.get("openwebui") or {}
        self.openwebui_settings = dict(openwebui_settings)
        self.openwebui_client = (
            OpenWebUIClient(self.openwebui_settings) if self.openwebui_settings else None
        )
        self.openwebui_url = str(self.openwebui_settings.get("url") or "").rstrip("/")
        self.account_provisioning = str(
            self.openwebui_settings.get("account_provisioning") or PROVISIONING_SSO
        )
        self.initial_password = self.openwebui_settings.get("initial_password")
        if (
            self.openwebui_client
            and self.account_provisioning == PROVISIONING_MANAGED_PASSWORD
            and not self.initial_password
        ):
            msg = (
                "Open WebUI account_provisioning is 'managed_password' but no "
                "'initial_password' is set; the agent has no other way to give the "
                "person a credential they can sign in with"
            )
            raise BackendError(msg)
        # Whether losing access demotes the account or deletes it. Demotion is the
        # default: it ends access just as completely (Open WebUI refuses a 'pending'
        # account) while keeping the person's chat history, so re-adding them later
        # restores what they had instead of handing back an empty product.
        self.remove_chat_accounts = bool(self.openwebui_settings.get("delete_accounts_on_removal"))

        # ``/user/list`` results, cached for part of a pass -- see _USER_CACHE_TTL.
        self._users_cache: Optional[tuple[float, dict]] = None

    # --- health / introspection -------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the LiteLLM proxy is reachable and its key database is live."""
        if self.litellm_client.ping():
            return True
        if raise_exception:
            msg = "LiteLLM backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log backend configuration and report reachability."""
        logger.info("=== LiteLLM backend diagnostics ===")
        logger.info("Proxy URL: %s", self.api_url)
        logger.info("Model allowlist: %s", self.models or "<all models>")
        logger.info(
            "Key backstops: budget_duration=%s tpm=%s rpm=%s",
            self.budget_duration,
            self.default_tpm_limit,
            self.default_rpm_limit,
        )
        logger.info("Components: %s", list(self.backend_components.keys()))
        if self.openwebui_client is None:
            logger.info("Chat surface: not configured (API-only offering)")
        else:
            logger.info(
                "Chat surface: %s (provisioning=%s, removal=%s, reachable=%s)",
                self.openwebui_client.api_url,
                self.account_provisioning,
                "delete" if self.remove_chat_accounts else "disable",
                self.openwebui_client.ping(),
            )
        return self.ping(raise_exception=False)

    def list_components(self) -> list:
        """Return the configured component names (token meters)."""
        return list(self.backend_components.keys())

    # --- key lookup -------------------------------------------------------------

    @staticmethod
    def _alias_prefix(resource_backend_id: str) -> str:
        return f"{resource_backend_id}-"

    @staticmethod
    def _slot_pattern(resource_backend_id: str) -> re.Pattern:
        """Match only this resource's own slots.

        The pattern is anchored on both ends and the number is the whole tail, never a
        prefix: matching by prefix alone lets resource ``proj`` capture ``proj-extra-1``,
        so a pause or a terminate would fan out across resource boundaries.
        """
        return re.compile(rf"^{re.escape(resource_backend_id)}-\d+$")

    def _resource_keys(self, resource_backend_id: str) -> dict:
        """Return ``{alias: record}`` for every key the resource owns.

        ``/key/list`` narrows by substring server-side, which is not precise enough on
        its own, so the exact slot pattern is applied to what comes back.
        """
        pattern = self._slot_pattern(resource_backend_id)
        found = {}
        for record in self.litellm_client.list_keys(self._alias_prefix(resource_backend_id)):
            alias = record.get("key_alias")
            if alias and pattern.match(alias):
                found[alias] = record
        return found

    @staticmethod
    def _token(record: dict) -> Optional[str]:
        """Return the sha256 handle LiteLLM addresses a key by."""
        return record.get("token") or record.get("token_id")

    def list_resource_client_ids(self, resource_backend_id: str) -> list:
        """Return the client-ids (aliases) of every key a resource owns."""
        return sorted(self._resource_keys(resource_backend_id))

    def _resource_is_paused(self, records: dict) -> bool:
        """A resource is paused when it owns keys and every one of them is blocked.

        Used so a key minted onto — or re-applied to — a paused resource lands blocked
        rather than silently un-pausing it and serving traffic past its quota.
        """
        if not records:
            return False
        return all(bool(record.get("blocked")) for record in records.values())

    # --- provisioning -----------------------------------------------------------

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        del waldur_resource, user_context  # no prerequisite setup

    def _client_id(self, waldur_resource: WaldurResource) -> str:
        return str(getattr(waldur_resource.uuid, "hex", waldur_resource.uuid))

    def _provision(self, backend_id: str, waldur_resource: WaldurResource) -> BackendResourceInfo:
        """Register the resource without minting keys.

        The core generates the keys separately (``generate_resource_keys``) and pushes
        each to Waldur, so nothing secret travels in ``backend_metadata``.
        """
        limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        # Handed to the keys minted right after this, so a resource does not spend its
        # first cycle on the offering-wide defaults alone.
        self._pending_limits = (backend_id, limits)
        logger.info("Registered LiteLLM resource %s", waldur_resource.uuid)
        return BackendResourceInfo(
            backend_id=backend_id,
            limits=limits,
            backend_metadata={},
            # The chat URL is the offering's, not the proxy's, so it is only surfaced
            # when the operator has actually named one -- otherwise the portal would
            # advertise a chat surface that does not exist for this offering.
            endpoints=self._endpoints(),
        )

    def _endpoints(self) -> list:
        """Access endpoints to surface on the resource in the portal."""
        endpoints = [{"name": "OpenAI API", "url": f"{self.api_url}/v1"}]
        if self.openwebui_url:
            endpoints.append({"name": "Chat", "url": self.openwebui_url})
        return endpoints

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Register the resource under the agent-supplied backend id.

        This is the method the order processor actually calls (it derives the
        backend_id from the resource and passes it in), so provisioning lives here.
        """
        del user_context
        return self._provision(resource_backend_id, waldur_resource)

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Register the resource with backend_id = resource UUID (direct calls/tests)."""
        del user_context
        return self._provision(self._client_id(waldur_resource), waldur_resource)

    def _pull_backend_resource(self, resource_backend_id: str) -> Optional[BackendResourceInfo]:
        """Report the resource as existing when it owns at least one key.

        The order processor calls ``pull_resource`` before (re)creating a resource that
        already carries a ``backend_id``. Without this the base goes through
        ``UnknownClient`` (always ``None``) and the processor re-provisions a resource
        that is already there.
        """
        if self._resource_keys(resource_backend_id):
            try:
                # The membership processor diffs this against the project team to work
                # out who to add and who has gone stale.
                users = self.list_resource_users(resource_backend_id)
            except BackendError:
                # Existence is decided by the keys alone, so a failed ``/user/list``
                # must not read as "resource missing": ``pull_resource`` swallows the
                # error and the order processor answers ``None`` by re-creating a live
                # resource, minting a second full set of keys for it. An empty member
                # list only makes the next membership pass re-add the members, which
                # is idempotent.
                logger.exception(
                    "Unable to list the users of resource %s; reporting it as existing "
                    "with no members rather than as missing",
                    resource_backend_id,
                )
                users = []
            return BackendResourceInfo(backend_id=resource_backend_id, users=users)
        return None

    def recreate_missing_resource(self, waldur_resource: WaldurResource) -> bool:
        """No automatic recreation.

        The agent does not keep key material — LiteLLM hashes it and Waldur holds the
        only copy — so a key that vanished from the proxy cannot be restored, only
        replaced. Replacement is portal-driven (rotate mints a new one and reports it),
        so a forced sync reports state instead of minting behind the user's back.
        """
        backend_id = waldur_resource.backend_id
        if backend_id and not self._resource_keys(backend_id):
            logger.warning(
                "Resource %s has no keys on the LiteLLM proxy; rotate from the portal "
                "to mint a replacement.",
                waldur_resource.uuid,
            )
        return False

    # --- key management (agent-driven, proxy-minted) ----------------------------

    def generate_resource_keys(
        self, resource_backend_id: str, count: int = _DEFAULT_KEY_COUNT
    ) -> Iterator[dict]:
        """Mint ``count`` new keys, yielding each as soon as the proxy accepts it.

        Aliases are ``<resource_backend_id>-<n>``, numbered past whatever the resource
        already has so a re-run never collides with a live key — and LiteLLM rejects a
        duplicate alias outright, so a colliding slot would fail the whole cycle rather
        than quietly overwrite.

        On a paused resource the new keys are minted ``blocked``: a live key added to a
        paused resource un-pauses it in practice and serves traffic past its quota.

        Yields rather than returning a list so the caller reports each key before the
        next is minted. Minting all of them first strands any key created before a
        mid-loop failure — live at the proxy, with no row in Waldur to rotate it by.
        """
        existing = self._resource_keys(resource_backend_id)
        blocked = self._resource_is_paused(existing)
        limits = self._take_pending_limits(resource_backend_id)
        prefix = self._alias_prefix(resource_backend_id)
        produced = 0
        slot = 1
        while produced < count:
            alias = f"{prefix}{slot}"
            slot += 1
            if alias in existing:
                continue
            response = self._mint(alias, blocked=blocked, limits=limits)
            produced += 1
            logger.info("Generated LiteLLM key %s (blocked=%s)", alias, blocked)
            yield {"client_id": alias, "api_key": response["key"]}

    def _take_pending_limits(self, resource_backend_id: str) -> dict:
        """Consume the limits parked by ``_provision`` for this resource, if any."""
        pending = self._pending_limits
        if pending is None or pending[0] != resource_backend_id:
            return {}
        self._pending_limits = None
        return pending[1]

    def _backstop_fields(self, limits: dict) -> dict:
        """The full target state of a key's backstop fields for these limits.

        Every field is always present, ``None`` where there is to be no cap, because
        this is a reconciliation target and not a patch. Emitting only the fields the
        resource currently carries makes the backstop one-way: drop ``tpm`` from a
        resource's limits and the old ``tpm_limit`` stays on every key forever,
        throttling it by a limit Waldur no longer holds and which no later cycle can
        clear. A ``None`` here is sent to ``/key/update`` and removes the cap.

        The resource's own limit wins; the offering-wide default is what a field falls
        back to, so clearing a resource limit returns the key to the offering default
        rather than to no cap at all.
        """
        # What a field falls back to when the resource does not set that limit. There
        # is no offering-wide budget, so max_budget falls back to no cap.
        defaults = {
            "max_budget": None,
            "tpm_limit": self.default_tpm_limit,
            "rpm_limit": self.default_rpm_limit,
        }
        fields: dict = {}
        for name, field in _LIMIT_FIELDS.items():
            value = limits.get(name)
            fields[field] = defaults[field] if value is None else value
        # Only meaningful alongside a budget, and a stale duration on a key that no
        # longer has one would outlive the budget it belonged to.
        fields["budget_duration"] = (
            self.budget_duration if fields["max_budget"] is not None else None
        )
        return fields

    def _limits_from_records(self, records: dict) -> dict:
        """Read the backstop back off the keys the proxy already holds.

        A rotation mints a replacement without ever seeing Waldur's limits -- it is
        handed a client_id, not a resource -- so without this the new key comes back
        with no ``max_budget`` while its siblings keep theirs. ``sync_resource_limits``
        would repair it, but only on the next membership_sync pass, leaving an
        uncapped key in circulation for up to a reporting period. The existing keys
        are the authority that is actually in hand here.
        """
        limits: dict = {}
        for record in records.values():
            if not isinstance(record, dict):
                continue
            for name, field in _LIMIT_FIELDS.items():
                value = record.get(field)
                if name not in limits and value is not None:
                    limits[name] = value
        return limits

    def _mint(self, alias: str, *, blocked: bool, limits: Optional[dict] = None) -> dict:
        """Mint one key with the offering's allowlist and the rate backstops applied.

        The resource's own limits win over the offering-wide defaults: a key minted
        without them would carry no ``max_budget`` at all until the resource's limits
        were next edited, which is exactly the burst the backstop exists to stop.
        """
        fields = self._backstop_fields(limits or {})
        return self.litellm_client.generate_key(
            alias,
            models=self.models,
            blocked=blocked,
            max_budget=fields["max_budget"],
            budget_duration=fields["budget_duration"],
            tpm_limit=fields["tpm_limit"],
            rpm_limit=fields["rpm_limit"],
        )

    def rotate_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        known_client_ids: Optional[list] = None,
    ) -> str:
        """Replace one key's material, keeping its alias, and return the new plaintext.

        The resource's other keys are untouched, so rotation is zero-downtime. The
        alias is a stable slot, so only the secret is returned.

        ``/key/{key}/regenerate`` does this in place but is enterprise-gated, so on an
        open-source proxy the fallback is delete-then-mint under the same alias — the
        delete is what frees the alias, which LiteLLM requires to be globally unique.

        ``known_client_ids`` is accepted and ignored: a rotation reuses an existing
        alias rather than allocating a new one, so a lost reply leaves no key behind
        for Waldur to lose track of.
        """
        del known_client_ids
        records = self._resource_keys(resource_backend_id)
        record = records.get(client_id)
        if record is None:
            # The slot is gone from the proxy (removed out-of-band, or a rotation that
            # died between the delete and the mint). Re-create it, honouring the
            # resource's pause state so this cannot resurrect a paused resource.
            logger.warning("Key %s is missing from the proxy; minting a replacement", client_id)
            return self._mint(
                client_id,
                blocked=self._resource_is_paused(records),
                limits=self._limits_from_records(records),
            )["key"]

        token = self._token(record)
        if not token:
            msg = f"LiteLLM returned no token handle for key {client_id}"
            raise LiteLLMBackendError(msg)

        try:
            new_key = self.litellm_client.regenerate_key(token)
        except LiteLLMEnterpriseFeatureError:
            logger.info(
                "/key/regenerate is enterprise-only on this proxy; rotating %s by "
                "delete-and-mint",
                client_id,
            )
        else:
            logger.info("Rotated LiteLLM key %s in place", client_id)
            return new_key

        # Pause state is read before the delete: afterwards this key is gone from the
        # listing, and a resource whose only other key is blocked would otherwise look
        # unpaused and get a live replacement.
        blocked = self._resource_is_paused(records)
        # Same reason the pause state is read here: after the delete the record is
        # gone, and with it the only in-hand copy of this key's backstop.
        limits = self._limits_from_records({client_id: record})
        self.litellm_client.delete_keys([token])
        new_key = self._mint(client_id, blocked=blocked, limits=limits)["key"]
        logger.info("Rotated LiteLLM key %s by delete-and-mint (blocked=%s)", client_id, blocked)
        return new_key

    def prune_unknown_resource_keys(self, resource_backend_id: str, keep: list) -> None:
        """Drop the resource's keys that Waldur does not hold.

        Runs at provisioning, before anything is minted, so the only keys in scope are
        the residue of an interrupted earlier create: live at the proxy with no row in
        Waldur, which means nothing could ever rotate or revoke them.

        The core only calls this with a set it could actually read — ``None`` there
        means unknown and nothing is pruned — so an empty ``keep`` here is a genuine
        "Waldur holds none of these", not a failed lookup.
        """
        keep_set = set(keep)
        doomed = [
            token
            for alias, record in self._resource_keys(resource_backend_id).items()
            if alias not in keep_set and (token := self._token(record))
        ]
        if not doomed:
            return
        logger.info(
            "Pruning %d LiteLLM key(s) of resource %s that Waldur does not hold",
            len(doomed),
            resource_backend_id,
        )
        self.litellm_client.delete_keys(doomed)

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: object) -> None:
        """Remove every one of the resource's keys from the proxy.

        Mirrors :meth:`pause_resource` on the key that cannot be addressed: without the
        hash there is no way to delete it, and it keeps serving after the resource is
        gone from Waldur — an orphan nothing can ever rotate or revoke. Every other key
        is still deleted (one bad record must not strand the rest), and the failure is
        then raised so the terminate order errs instead of reporting a clean removal.
        """
        del kwargs
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning("No backend_id for resource %s; nothing to delete", waldur_resource.uuid)
            return
        # Members first, keys second. A member left behind after the keys are gone is
        # the dangerous residue: they can still chat (that traffic never touched this
        # resource's keys) and their spend accrues against a resource Waldur has
        # terminated. A key left behind while the members are gone only fails closed.
        self._remove_all_users(backend_id)

        tokens = []
        orphans = []
        for alias, record in self._resource_keys(backend_id).items():
            token = self._token(record)
            if token:
                tokens.append(token)
            else:
                logger.error(
                    "LiteLLM returned no token handle for key %s; it cannot be deleted "
                    "and will keep serving after resource %s is gone",
                    alias,
                    waldur_resource.uuid,
                )
                orphans.append(alias)
        self.litellm_client.delete_keys(tokens)
        logger.info(
            "Deleted %d LiteLLM key(s) for resource %s", len(tokens), waldur_resource.uuid
        )
        if orphans:
            msg = (
                f"LiteLLM key(s) {', '.join(orphans)} of resource {backend_id} have no "
                "token handle and could not be deleted"
            )
            raise LiteLLMBackendError(msg)

    def _remove_all_users(self, resource_backend_id: str) -> None:
        """Revoke every member of a resource that is going away.

        Failures are logged and do not stop the deletion: the keys still have to go,
        and a terminate that aborts halfway leaves a live resource in Waldur's past.
        """
        try:
            owned = self._resource_user_map(resource_backend_id)
        except BackendError:
            # The keys still have to go. Raising here would abort ``delete_resource``
            # before the key-deletion loop, so a listing failure would leave every key
            # of a terminated resource serving.
            logger.exception(
                "Unable to list the users of resource %s; continuing with key deletion",
                resource_backend_id,
            )
            return
        if not owned:
            return
        for email in sorted(owned.values()):
            try:
                self._revoke_chat_access(email, permanent=True)
            except BackendError:
                logger.exception("Unable to revoke chat access for %s", email)
        try:
            self.litellm_client.delete_users(sorted(owned.values()))
        except BackendError:
            logger.exception(
                "Unable to delete the LiteLLM users of resource %s", resource_backend_id
            )
        else:
            logger.info(
                "Removed %d user(s) from resource %s", len(owned), resource_backend_id
            )
        self._invalidate_users_cache()

    # --- state transitions ------------------------------------------------------

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Block every key of the resource.

        Reports success only when nothing is left serving. The return value is the
        processor's only success signal (it logs "Pausing is successfully completed"
        on True and nothing retries within the cycle), so reporting True while one key
        of the pair still answers would announce an enforced quota that is not
        enforced. Every key is still attempted — one failure must not strand the rest.

        A key the proxy no longer holds is not a failure: it cannot serve traffic, so
        the resource is paused as far as this is concerned.
        """
        records = self._resource_keys(resource_backend_id)
        if not records:
            logger.warning(
                "Resource %s owns no keys on the LiteLLM proxy; nothing to pause",
                resource_backend_id,
            )
            return False

        # Both surfaces, because blocking the keys only closes the API one. Chat runs
        # on Open WebUI's shared key, which no per-resource block can reach.
        paused = self._set_chat_access_for_resource(resource_backend_id, enabled=False)
        for alias, record in records.items():
            token = self._token(record)
            if not token:
                # Without the hash there is no way to address the key, and it is still
                # serving — the same outcome as a failed block.
                logger.error("LiteLLM returned no token handle for key %s; cannot pause it", alias)
                paused = False
                continue
            try:
                if not self.litellm_client.block(token):
                    logger.info("Key %s is already gone from the proxy", alias)
            except LiteLLMBackendError:
                # A swallowed pause silently defeats quota enforcement — the over-limit
                # key keeps serving — so this is an error, not a quiet warning.
                logger.exception("Unable to pause (block) key %s", alias)
                paused = False
        return paused

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Unblock every key of the resource.

        The mirror of :meth:`pause_resource`, and asymmetric with it on purpose: a
        pause asks "is anything still serving?", a restore asks "is everything serving
        again?". So a key the proxy has lost fails a restore while it satisfies a
        pause — the resource comes back with fewer working keys than Waldur holds, and
        only a portal-driven rotation can replace one.
        """
        records = self._resource_keys(resource_backend_id)
        if not records:
            logger.warning(
                "Resource %s owns no keys on the LiteLLM proxy; nothing to restore",
                resource_backend_id,
            )
            return False

        restored = self._set_chat_access_for_resource(resource_backend_id, enabled=True)
        for alias, record in records.items():
            token = self._token(record)
            if not token:
                logger.warning("LiteLLM returned no token handle for key %s", alias)
                restored = False
                continue
            try:
                if not self.litellm_client.unblock(token):
                    logger.warning("Key %s is gone from the proxy and cannot be restored", alias)
                    restored = False
            except LiteLLMBackendError as exc:
                logger.warning("Unable to restore (unblock) key %s: %s", alias, exc)
                restored = False
        return restored

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """A key has no partial-capacity state; block it as the safe interpretation."""
        return self.pause_resource(resource_backend_id)

    # --- limits -----------------------------------------------------------------

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple:
        # Limits are held and enforced by Waldur; nothing to read back from the proxy.
        del waldur_resource
        return {}, {}

    def set_resource_limits(self, resource_backend_id: str, limits: dict) -> None:
        """Mirror the resource's limits onto its keys as a metering backstop.

        Waldur stays the enforcement authority (report -> pause -> block); this only
        keeps a burst between two reporting cycles from outrunning the metering. Each
        key carries the resource's full budget rather than a share of it: the keys are
        alternatives for one consumer, not separate allowances, and splitting the
        budget would throttle a consumer using a single key to half its entitlement.

        Unlike the base implementation, ``unit_factor`` is deliberately not applied.
        The reporting side does not apply it either -- usage rows come back in the
        proxy's own units -- so both directions speak Waldur units and a limit can
        never end up measured differently from the usage it is compared against.
        Honouring it here alone would produce exactly that mismatch. The base also
        casts to ``int``, which is wrong for ``max_budget``: it is a float on the
        proxy, and rounding a fractional budget down is a silent loss.
        """
        fields = self._backstop_fields(limits)
        for alias, record in self._resource_keys(resource_backend_id).items():
            token = self._token(record)
            if not token:
                continue
            if all(record.get(field) == value for field, value in fields.items()):
                # Reconciliation runs every membership-sync cycle; writing values the
                # key already carries would be one /key/update per key per cycle.
                continue
            try:
                self.litellm_client.update_key(token, fields)
            except LiteLLMBackendError as exc:
                # The backstop failing is not the same as enforcement failing: Waldur
                # still pauses on the reported usage, so this does not fail the order.
                logger.warning("Unable to apply limits to key %s: %s", alias, exc)

    def sync_resource_limits(
        self, waldur_resource: WaldurResource, waldur_rest_client: AuthenticatedClient
    ) -> None:
        """Push Waldur's limits onto the resource's keys.

        The base implementation reconciles the other way — it pulls the backend's
        limits into Waldur — which is meaningless here: the proxy holds no limits of
        its own, so every cycle would read an empty set and log "No limits found in
        the backend".

        Waldur is the authority, so the reconciliation runs from Waldur outwards. It
        is what keeps a key minted before a limit change, or one re-minted by a
        rotation, carrying the resource's current backstop.
        """
        del waldur_rest_client
        backend_id = waldur_resource.backend_id
        if not backend_id:
            return
        # An empty limit set is reconciled too, rather than skipped: it is how a
        # resource whose limits were cleared gets its keys back to the offering
        # defaults. Skipping it would leave the removed caps in place indefinitely.
        limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        self.set_resource_limits(backend_id, limits)

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Report whether any of the resource's keys is currently serving."""
        records = self._resource_keys(resource_backend_id)
        return {
            "backend_type": self.backend_type,
            "active": any(not record.get("blocked") for record in records.values()),
        }

    # --- users and chat access --------------------------------------------------
    #
    # A LiteLLM *user* is not a credential and does not gate anything. It exists so
    # that chat traffic can be billed: Open WebUI authenticates every upstream call
    # with one shared virtual key and forwards the signed-in person's address, which
    # the proxy stamps onto the request's ``user_id`` -- after authentication, without
    # looking the address up. Two consequences shape everything below.
    #
    # First, the user record is an *attribution* record. Budgets, rate limits and model
    # allowlists written onto it are not consulted for chat traffic, because that
    # request authenticated as the shared key and was checked against the shared key's
    # owner. So this plugin writes nothing onto the user but identity and ownership.
    # A ``max_budget`` here would read as an enforced cap that enforces nothing at all.
    # Enforcement stays where it already is -- Waldur meters, pauses, and the
    # agent blocks the keys and demotes the chat account.
    #
    # Second, removing the LiteLLM user does not end chat access. An address the proxy
    # has never seen is not rejected; it accrues daily spend rows against a user row
    # that does not exist. Revocation therefore has to happen in Open WebUI, which is
    # why every removal path here touches both systems.

    @staticmethod
    def _email_of(record: dict) -> str:
        """The address a managed user is keyed by, normalized for comparison."""
        return str(record.get("user_id") or "").strip().lower()

    @staticmethod
    def _user_metadata(record: dict) -> dict:
        metadata = record.get("metadata")
        return metadata if isinstance(metadata, dict) else {}

    def _managed_users(self) -> dict:
        """Return ``{email: record}`` for every internal user on the proxy.

        The whole listing rather than one resource's slice, because ``/user/list``
        cannot filter on metadata and ownership only lives there. It is cached for part
        of a pass so the per-resource walk does not repeat it.
        """
        now = time.monotonic()
        cached = self._users_cache
        if cached is not None and now - cached[0] < _USER_CACHE_TTL:
            return cached[1]
        users = {}
        for record in self.litellm_client.list_users():
            email = self._email_of(record)
            if email:
                users[email] = record
        self._users_cache = (now, users)
        return users

    def _invalidate_users_cache(self) -> None:
        """Drop the cached listing after a write that changes it.

        Without this an add followed by a pull inside the same pass would read the
        pre-add listing and report the new member as still missing, which the processor
        would answer by adding them again.
        """
        self._users_cache = None

    def _resource_user_map(self, resource_backend_id: str) -> dict:
        """Return ``{offering_username: email}`` for the users this resource owns.

        Keyed by offering username because that is the currency the membership
        processor diffs in: it compares what this returns against the project team's
        offering usernames. The email is what the proxy is addressed by, so both halves
        of the mapping are needed and both are read back out of the user's metadata
        rather than recomputed -- a person can change their address in Waldur, and the
        record on the proxy is the one that says which address is actually billing.
        """
        found = {}
        for email, record in self._managed_users().items():
            metadata = self._user_metadata(record)
            if metadata.get(META_RESOURCE) != resource_backend_id:
                continue
            username = metadata.get(META_USERNAME)
            if username:
                found[str(username)] = email
        return found

    def list_resource_users(self, resource_backend_id: str) -> list:
        """Offering usernames of the people currently provisioned for the resource."""
        return sorted(self._resource_user_map(resource_backend_id))

    def add_users_to_resource(
        self, waldur_resource: WaldurResource, user_ids: set, **kwargs: object
    ) -> set:
        """Provision each new member on the proxy and, if configured, in Open WebUI.

        ``user_ids`` are offering usernames; the addresses come alongside them in
        ``user_emails``, which the membership processor builds from the offering users.
        A member with no address cannot be provisioned at all -- the email *is* the
        identifier both systems agree on -- so they are skipped loudly rather than
        provisioned under something that would never receive their usage.
        """
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning("No backend_id for resource %s; cannot add users", waldur_resource.uuid)
            return set()
        if not user_ids:
            logger.info("No new users to add")
            return set()

        # The processor threads these through as plain dicts; ``**kwargs: object`` is
        # the base signature, so they are narrowed here rather than trusted.
        user_emails = kwargs.get("user_emails")
        user_emails = user_emails if isinstance(user_emails, dict) else {}
        user_attributes = kwargs.get("user_attributes")
        user_attributes = user_attributes if isinstance(user_attributes, dict) else {}
        added = set()
        for username in sorted(user_ids):
            # ``user_emails`` is only built by the membership processor; the order
            # processor and the service/course-account syncs pass the addresses inside
            # ``user_attributes`` instead. Reading only the former skipped every member
            # on those paths, so a CREATE order provisioned nobody until the next
            # membership pass -- and never, in order-only mode.
            attributes = user_attributes.get(username)
            attributes = attributes if isinstance(attributes, dict) else {}
            raw_email = user_emails.get(username) or attributes.get("email") or ""
            email = str(raw_email).strip().lower()
            if not email:
                logger.error(
                    "Offering user %s has no email address; LiteLLM and Open WebUI are "
                    "married by email, so they cannot be added to resource %s",
                    username,
                    backend_id,
                )
                continue
            attributes = user_attributes.get(username)
            attributes = attributes if isinstance(attributes, dict) else {}
            full_name = str(attributes.get("full_name") or username)
            try:
                if not self._ensure_litellm_user(email, username, full_name, backend_id):
                    continue
                self._ensure_chat_account(email, full_name)
            except BackendError:
                logger.exception(
                    "Unable to add user %s (%s) to resource %s", username, email, backend_id
                )
                continue
            added.add(username)
        if added:
            self._invalidate_users_cache()
        return added

    def _ensure_litellm_user(
        self, email: str, username: str, full_name: str, backend_id: str
    ) -> bool:
        """Create or adopt the proxy-side user record. False means "not ours to touch".

        A LiteLLM user id is global, and here it is an email address, so one person has
        exactly one record on the proxy no matter how many resources they hold. That is
        the per-person model's load-bearing constraint: usage arriving under an address
        can only be billed to one resource, so a second resource claiming an address
        another one already owns is **refused**, not taken over. Stealing it would move
        the first resource's chat billing onto the second one silently, and the person
        would keep using the chat throughout.

        A record with no owner stamped on it is adopted rather than refused: it is
        either a user an admin added by hand before Waldur managed this offering, or one
        left behind by an interrupted add, and in both cases the alternative is a
        resource that can never provision its own member.
        """
        metadata = {META_RESOURCE: backend_id, META_USERNAME: username}
        existing = self.litellm_client.get_user(email)
        if existing is None:
            self.litellm_client.create_user(
                email,
                user_email=email,
                user_alias=full_name,
                metadata=metadata,
            )
            logger.info("Created LiteLLM user %s for resource %s", email, backend_id)
            return True

        owner = self._user_metadata(existing).get(META_RESOURCE)
        if owner and owner != backend_id:
            logger.error(
                "LiteLLM user %s is already owned by resource %s; refusing to move it "
                "to %s. A person's usage can only be billed to one resource, so the "
                "chat surface supports one entitlement per person -- terminate the "
                "other resource first.",
                email,
                owner,
                backend_id,
            )
            return False

        # Re-stamped on every add, not only on adoption: the offering username can
        # change (Waldur regenerates it under some policies), and the membership diff
        # is keyed on it, so a stale one would report the member as absent forever.
        self.litellm_client.update_user(
            email, {"user_email": email, "user_alias": full_name, "metadata": metadata}
        )
        logger.info("Adopted existing LiteLLM user %s for resource %s", email, backend_id)
        return True

    def _ensure_chat_account(
        self, email: str, full_name: Optional[str] = None, *, create_missing: bool = True
    ) -> None:
        """Make sure the person can sign in to the chat surface, if there is one.

        Under ``sso`` no account is created: the identity provider does that on first
        login. What still runs is the *re-enable*, because a previous removal demoted
        the account and an SSO login into a demoted account does not restore it.

        ``create_missing=False`` re-enables only. Restore uses it: an account that is
        not there was not paused by this plugin, and restore has no display name to
        create one under -- it knows the offering username, not the person's name.
        """
        if self.openwebui_client is None:
            return
        account = self.openwebui_client.find_user(email)
        if account is None:
            if not create_missing:
                logger.info("No Open WebUI account for %s; nothing to re-enable", email)
                return
            if self.account_provisioning != PROVISIONING_MANAGED_PASSWORD:
                logger.info(
                    "No Open WebUI account for %s; it will be created on first sign-in "
                    "through the identity provider",
                    email,
                )
                return
            self.openwebui_client.create_user(
                email, str(full_name or email), str(self.initial_password), role=ROLE_ACTIVE
            )
            logger.info("Created Open WebUI account for %s", email)
            return

        if str(account.get("role")) == ROLE_DISABLED:
            self.openwebui_client.set_role(account, ROLE_ACTIVE)
            logger.info("Re-enabled the Open WebUI account of %s", email)

    def remove_users_from_resource(
        self, waldur_resource: WaldurResource, usernames: set, **kwargs: object
    ) -> list:
        """Revoke a member's LiteLLM record and their chat access.

        Both halves matter and in this order they are both required: deleting only the
        LiteLLM user leaves the person chatting through the shared key with their usage
        landing on rows nothing reconciles, and demoting only the chat account leaves a
        billing record for someone who is no longer entitled.
        """
        del kwargs
        backend_id = waldur_resource.backend_id
        if not usernames:
            logger.info("No users to remove")
            return []

        owned = self._resource_user_map(backend_id) if backend_id else {}
        removed = []
        for username in sorted(usernames):
            email = owned.get(username)
            if email is None:
                # Already absent from the proxy. Reporting it as removed is correct --
                # the desired end state holds -- and reporting a failure instead would
                # have the processor retry it on every pass forever.
                logger.info(
                    "No LiteLLM user for %s on resource %s; nothing to remove",
                    username,
                    backend_id,
                )
                removed.append(username)
                continue
            try:
                self._revoke_chat_access(email, permanent=True)
                self.litellm_client.delete_users([email])
            except BackendError:
                logger.exception(
                    "Unable to remove user %s (%s) from resource %s", username, email, backend_id
                )
                continue
            logger.info("Removed user %s (%s) from resource %s", username, email, backend_id)
            removed.append(username)
        if removed:
            self._invalidate_users_cache()
        return removed

    def _revoke_chat_access(self, email: str, *, permanent: bool = False) -> None:
        """End one person's access to the chat surface.

        Demotion by default, deletion only when the offering asks for it *and* the
        entitlement is actually gone -- ``permanent`` is what says so, and only the
        member-removal and terminate paths pass it. A pause is a temporary state, so
        it always demotes however ``delete_accounts_on_removal`` is set: deleting there
        would destroy the person's conversations over an unpaid invoice, and under
        ``sso`` restore creates nothing back, so the loss would be permanent.

        Both forms end access equally; demotion additionally keeps the person's
        conversations so a re-add gives back what they had.

        A missing account is not an error. Under ``sso`` a person may never have signed
        in, and there is nothing to revoke for someone who never had access.
        """
        if self.openwebui_client is None:
            return
        account = self.openwebui_client.find_user(email)
        if account is None:
            return
        role = str(account.get("role") or "")
        if role not in (ROLE_ACTIVE, ROLE_DISABLED):
            # An admin (or any other elevated role) can also be a project member. This
            # plugin only ever grants ``user``, so anything else was set outside it and
            # is not ours to take away: demoting it would lock the operator out of the
            # admin API -- possibly with the very token this agent authenticates with --
            # and restore only ever promotes back to ``user``, so the loss is permanent.
            logger.info(
                "Open WebUI account of %s has role %s; leaving it untouched", email, role
            )
            return
        if permanent and self.remove_chat_accounts:
            self.openwebui_client.delete_user(str(account.get("id")))
            logger.info("Deleted the Open WebUI account of %s", email)
            return
        self.openwebui_client.set_role(account, ROLE_DISABLED)
        logger.info("Disabled the Open WebUI account of %s", email)

    def _set_chat_access_for_resource(self, resource_backend_id: str, *, enabled: bool) -> bool:
        """Enable or disable the chat surface for every member of one resource.

        Called from pause and restore. Blocking the resource's keys does **not** stop
        its members chatting: that traffic authenticates as Open WebUI's own shared key,
        which this plugin neither owns nor may block -- blocking it would cut off every
        other tenant on the proxy. So a paused resource has to be paused person by
        person, on the chat side.
        """
        if self.openwebui_client is None:
            return True
        succeeded = True
        try:
            emails = sorted(self._resource_user_map(resource_backend_id).values())
        except BackendError:
            # Raising would abort pause/restore before a single key is blocked, and in
            # the membership pass it would also skip the rest of the per-resource work.
            # Report the failure instead and let the caller block the keys anyway.
            logger.exception(
                "Unable to list the users of resource %s; chat access is unchanged",
                resource_backend_id,
            )
            return False
        for email in emails:
            try:
                if enabled:
                    self._ensure_chat_account(email, create_missing=False)
                else:
                    self._revoke_chat_access(email)
            except (BackendError, OpenWebUIError):
                # A pause that half-worked still leaves someone spending past their
                # quota, so this is an error and the caller reports the pause as failed.
                logger.exception(
                    "Unable to %s chat access for %s",
                    "restore" if enabled else "revoke",
                    email,
                )
                succeeded = False
        return succeeded

    # --- usage reporting --------------------------------------------------------

    def _get_usage_report(self, resource_backend_ids: list) -> dict:
        """Usage is reported by the separate ``litellm-usage`` reporting backend."""
        del resource_backend_ids
        return {}
