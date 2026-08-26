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
from collections.abc import Iterator
from typing import Optional

from waldur_api_client.client import AuthenticatedClient
from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import DEFAULT_RESOURCE_KEY_COUNT, backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .client import LiteLLMBackendError, LiteLLMClient, LiteLLMEnterpriseFeatureError

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
            endpoints=[{"name": "OpenAI API", "url": f"{self.api_url}/v1"}],
        )

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
            return BackendResourceInfo(backend_id=resource_backend_id)
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

        paused = True
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

        restored = True
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

    # --- usage reporting --------------------------------------------------------

    def _get_usage_report(self, resource_backend_ids: list) -> dict:
        """Usage is reported by the separate ``litellm-usage`` reporting backend."""
        del resource_backend_ids
        return {}
