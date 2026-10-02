"""Envoy AI Gateway backend: manages API keys as Kubernetes Secret entries.

This is the management (order-processing) backend. Token usage is reported by a separate
reporting backend that reads the usage sink (usage warehouse), so ``_get_usage_report``
here returns nothing.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Iterator
from typing import Optional

from waldur_api_client.models.resource import Resource as WaldurResource

from waldur_site_agent.backend import DEFAULT_RESOURCE_KEY_COUNT, backends
from waldur_site_agent.backend.exceptions import BackendError
from waldur_site_agent.backend.structures import BackendResourceInfo

from .client import KEY_ACTIVE, KEY_PAUSED, EnvoyAIGatewayBackendError, EnvoyAIGatewayClient

logger = logging.getLogger(__name__)

# The agent owns key generation. OpenAI-style "sk-" prefix; token_urlsafe yields
# ~1.3 chars per entropy byte.
_KEY_PREFIX = "sk-"
_KEY_ENTROPY_BYTES = 32
# Inference resources get two keys so one can be rotated with no downtime. Taken from
# the core, because common.utils reconciles the count against what Waldur already holds
# and so has to agree with this module on the target.
_DEFAULT_KEY_COUNT = DEFAULT_RESOURCE_KEY_COUNT


def _generate_key() -> str:
    return f"{_KEY_PREFIX}{secrets.token_urlsafe(_KEY_ENTROPY_BYTES)}"


class EnvoyAIGatewayBackend(backends.BaseBackend):
    """Provisions Envoy AI Gateway API keys from Waldur orders."""

    # The agent generates each key, applies it to the Secret, then reports it to
    # Waldur (encrypted) via the provider endpoints. A resource has many keys,
    # each a "<backend_id>-<n>" Secret entry.
    supports_resource_api_keys = True
    # One Secret entry is one key, so each can be minted, paused, resumed and
    # deleted on its own. Limits are Waldur's to enforce (report -> pause), exactly as
    # for the resource, so applying them stores nothing here.
    supports_resource_api_key_lifecycle = True

    def __init__(
        self, backend_settings: dict[str, object], backend_components: dict[str, dict]
    ) -> None:
        """Initialize the backend from offering settings and components."""
        super().__init__(backend_settings, backend_components)
        self.backend_type = "envoy"

        self.gateway_url = backend_settings.get("gateway_url")
        if not self.gateway_url:
            msg = "Envoy AI Gateway backend requires 'gateway_url' in backend_settings"
            raise BackendError(msg)
        self.gateway_url = str(self.gateway_url).rstrip("/")

        self.gateway_client = EnvoyAIGatewayClient(backend_settings)

    # --- health / introspection -------------------------------------------------

    def ping(self, raise_exception: bool = False) -> bool:
        """Check the Kubernetes api-key Secret is reachable."""
        if self.gateway_client.ping():
            return True
        if raise_exception:
            msg = "Envoy AI Gateway backend is not available"
            raise BackendError(msg)
        return False

    def diagnostics(self) -> bool:
        """Log backend configuration and report reachability."""
        logger.info("=== Envoy AI Gateway backend diagnostics ===")
        logger.info("Gateway URL: %s", self.gateway_url)
        logger.info("Namespace: %s", self.gateway_client.namespace)
        logger.info(
            "Secrets: active=%s blocked=%s",
            self.gateway_client.apikey_secret,
            self.gateway_client.blocked_secret,
        )
        logger.info("Components: %s", list(self.backend_components.keys()))
        return self.ping(raise_exception=False)

    def list_components(self) -> list[str]:
        """Return the configured component names (token meters)."""
        return list(self.backend_components.keys())

    # --- provisioning -----------------------------------------------------------

    def _pre_create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> None:
        del waldur_resource, user_context  # no prerequisite setup

    def _client_id(self, waldur_resource: WaldurResource) -> str:
        return str(getattr(waldur_resource.uuid, "hex", waldur_resource.uuid))

    def _provision(self, client_id: str, waldur_resource: WaldurResource) -> BackendResourceInfo:
        """Register the resource without minting keys.

        The agent generates the keys separately (generate_resource_keys) and the
        core pushes them to Waldur; keys are no longer carried in backend_metadata.
        Limits are held and enforced by Waldur (report -> pause).
        """
        limits = waldur_resource.limits.to_dict() if waldur_resource.limits else {}
        logger.info("Registered Envoy AI Gateway resource %s", waldur_resource.uuid)
        return BackendResourceInfo(
            backend_id=client_id,
            limits=limits,
            backend_metadata={},
            endpoints=[{"name": "OpenAI API", "url": f"{self.gateway_url}/v1"}],
        )

    # --- key management (agent-owned) -------------------------------------------

    @staticmethod
    def _key_prefix(resource_backend_id: str) -> str:
        return f"{resource_backend_id}-"

    def list_resource_client_ids(self, resource_backend_id: str) -> list[str]:
        """Return the client-ids of every key a resource owns (active or blocked)."""
        return self.gateway_client.list_client_ids(resource_backend_id)

    def _resource_is_paused(
        self, resource_backend_id: str, key_states: dict[str, str], exclude: str = ""
    ) -> bool:
        """Whether the resource itself is paused.

        ``pause_resource`` records it with a marker. A resource paused before the
        marker existed is still recognised the old way: it owns keys and none of them
        are active. Keys paused on their own account are left out of that: they say
        nothing about the resource. Used so a key added or re-applied on a paused
        resource lands in the blocked Secret rather than silently un-pausing it.
        """
        if self.gateway_client.is_resource_marked_paused(resource_backend_id):
            return True
        states = [
            state
            for client_id, state in key_states.items()
            if client_id != exclude and state != KEY_PAUSED
        ]
        if not states:
            return False
        return KEY_ACTIVE not in states

    def _free_client_ids(self, resource_backend_id: str, taken: set[str]) -> Iterator[str]:
        prefix = self._key_prefix(resource_backend_id)
        n = 1
        while True:
            client_id = f"{prefix}{n}"
            n += 1
            if client_id not in taken:
                yield client_id

    def generate_resource_keys(
        self, resource_backend_id: str, count: int = _DEFAULT_KEY_COUNT
    ) -> Iterator[dict]:
        """Generate ``count`` new keys, applying and yielding one at a time.

        Client-ids are ``<resource_backend_id>-<n>``, continuing past any keys the
        resource already has so a re-run never collides with an existing one. New keys
        land in the active Secret, except on a paused resource where they land
        blocked (adding a live key to a paused resource would bypass its pause).

        Yields rather than returns so the caller reports each key before the next is
        applied; applying all of them first strands any key minted before a failure,
        live in the Secret with no row in Waldur.
        """
        key_states = self.gateway_client.key_states(resource_backend_id)
        blocked = self._resource_is_paused(resource_backend_id, key_states)
        free = self._free_client_ids(resource_backend_id, set(key_states))
        for _ in range(count):
            client_id = next(free)
            api_key = _generate_key()
            self.gateway_client.provision_key(client_id, api_key, blocked=blocked)
            logger.info(
                "Generated Envoy AI Gateway key %s (blocked=%s)", client_id, blocked
            )
            yield {"client_id": client_id, "api_key": api_key}

    def rotate_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        known_client_ids: Optional[list[str]] = None,
    ) -> str:
        """Generate a new value for one key and apply it, revoking the previous one.

        The other keys are untouched, so rotation is zero-downtime. Returns the new
        key value for the caller to report to Waldur; the client_id is a stable slot
        here, so it does not change.

        ``known_client_ids`` is accepted and ignored: a rotation overwrites the value
        in an existing Secret entry rather than creating a new one, so a lost reply
        leaves nothing behind for Waldur to lose track of.
        """
        del known_client_ids
        api_key = _generate_key()
        key_states = self.gateway_client.key_states(resource_backend_id)
        if key_states.get(client_id) == KEY_PAUSED:
            # Waldur rotates only an OK or Erred key and settles it OK. A key still
            # paused here is the residue of a pause whose acknowledgement failed, and
            # must serve again once Waldur shows it OK.
            self.gateway_client.rotate_key(client_id, api_key)
            self._lift_key_pause(client_id, resource_backend_id, key_states)
        elif not self.gateway_client.rotate_key(client_id, api_key):
            # The key had no entry in either Secret (lost / never applied). Re-apply
            # it, but honour the resource's pause state: blind-provisioning to the
            # active Secret would resurrect a paused resource's key.
            self.gateway_client.provision_key(
                client_id,
                api_key,
                blocked=self._resource_is_paused(
                    resource_backend_id, key_states, exclude=client_id
                ),
            )
        logger.info("Rotated Envoy AI Gateway key %s", client_id)
        return api_key

    # --- per-key lifecycle ------------------------------------------------------

    @staticmethod
    def _refuse_model_allowlist(allowed_models: Optional[list[str]]) -> None:
        """Fail a command that would restrict a key to some models.

        The gateway authenticates a key but routes every model to it alike; there is
        no per-key model rule to write. Accepting the list would show the key in
        Waldur as restricted while it can call every model.
        """
        if allowed_models:
            msg = (
                "The Envoy AI Gateway cannot restrict a single key to some models; "
                "clear the key's model list"
            )
            raise BackendError(msg)

    def mint_resource_key(
        self,
        resource_backend_id: str,
        reserved_client_ids: list[str],
        limits: Optional[dict] = None,
        allowed_models: Optional[list[str]] = None,
    ) -> dict:
        """Add one key to the resource, as its next free ``<backend_id>-<n>`` slot.

        A slot Waldur still holds — a deleted key's included — is skipped even though
        its Secret entry is gone, because usage is attributed by client_id. Like
        ``generate_resource_keys``, the key lands blocked on a paused resource.
        ``limits`` is not stored: Waldur enforces limits from reported usage.
        """
        del limits
        self._refuse_model_allowlist(allowed_models)
        key_states = self.gateway_client.key_states(resource_backend_id)
        blocked = self._resource_is_paused(resource_backend_id, key_states)
        taken = set(key_states) | set(reserved_client_ids)
        client_id = next(self._free_client_ids(resource_backend_id, taken))
        api_key = _generate_key()
        self.gateway_client.provision_key(client_id, api_key, blocked=blocked)
        logger.info("Minted Envoy AI Gateway key %s (blocked=%s)", client_id, blocked)
        return {"client_id": client_id, "api_key": api_key}

    def pause_resource_key(self, client_id: str, resource_backend_id: str) -> None:
        """Hold one key back in the blocked Secret, under its own pause.

        The resource's other keys keep serving, and a resource restore leaves this one
        paused. A key with no entry at all already cannot authenticate, so there is
        nothing to hold back; resuming it later fails and a rotation re-applies it.
        """
        del resource_backend_id
        if not self.gateway_client.pause_key(client_id):
            logger.warning(
                "Envoy AI Gateway key %s has no Secret entry; it cannot serve, "
                "so it counts as paused",
                client_id,
            )
            return
        logger.info("Paused Envoy AI Gateway key %s", client_id)

    def resume_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        limits: Optional[dict] = None,
        allowed_models: Optional[list[str]] = None,
    ) -> None:
        """Lift a key's own pause; on a paused resource it stays blocked with its siblings."""
        del limits
        self._refuse_model_allowlist(allowed_models)
        key_states = self.gateway_client.key_states(resource_backend_id)
        state = key_states.get(client_id)
        if state is None:
            msg = (
                f"Envoy AI Gateway key {client_id} has no Secret entry to resume; "
                "rotate it to apply a new value"
            )
            raise BackendError(msg)
        if state != KEY_PAUSED:
            # Resumed already: a replay of a resume whose acknowledgement was lost.
            logger.info("Envoy AI Gateway key %s is not paused; nothing to resume", client_id)
            return
        self._lift_key_pause(client_id, resource_backend_id, key_states)
        logger.info("Resumed Envoy AI Gateway key %s", client_id)

    def _lift_key_pause(
        self, client_id: str, resource_backend_id: str, key_states: dict[str, str]
    ) -> None:
        blocked = self._resource_is_paused(resource_backend_id, key_states, exclude=client_id)
        self.gateway_client.resume_key(client_id, blocked=blocked)

    def delete_resource_key(self, client_id: str, resource_backend_id: str) -> None:
        """Remove one key's entry from both Secrets, whichever state it was in."""
        del resource_backend_id
        self.gateway_client.deprovision_key(client_id, strict=True)
        logger.info("Deleted Envoy AI Gateway key %s", client_id)

    def update_resource_key(
        self,
        client_id: str,
        resource_backend_id: str,
        limits: Optional[dict] = None,
        allowed_models: Optional[list[str]] = None,
    ) -> None:
        """Accept a key's limits; there is nothing to push, since Waldur enforces them.

        An update settles the key OK in Waldur, so the key must serve afterwards: one
        left paused by a pause whose acknowledgement failed has its pause lifted, and
        one with no entry at all is an error rather than an OK key that cannot work.
        """
        del limits
        self._refuse_model_allowlist(allowed_models)
        key_states = self.gateway_client.key_states(resource_backend_id)
        state = key_states.get(client_id)
        if state is None:
            msg = (
                f"Envoy AI Gateway key {client_id} has no Secret entry; "
                "rotate it to apply a new value"
            )
            raise BackendError(msg)
        if state == KEY_PAUSED:
            self._lift_key_pause(client_id, resource_backend_id, key_states)

    def create_resource_with_id(
        self,
        waldur_resource: WaldurResource,
        resource_backend_id: str,
        user_context: Optional[dict] = None,
    ) -> BackendResourceInfo:
        """Provision a key using the agent-supplied backend id as the client_id.

        This is the method the site-agent order processor actually calls (it generates the
        backend_id from the resource and passes it in), so the provisioning lives here.
        """
        del user_context
        return self._provision(resource_backend_id, waldur_resource)

    def create_resource(
        self, waldur_resource: WaldurResource, user_context: Optional[dict] = None
    ) -> BackendResourceInfo:
        """Provision a key with client_id = resource UUID (direct calls / tests)."""
        del user_context
        return self._provision(self._client_id(waldur_resource), waldur_resource)

    def _pull_backend_resource(self, resource_backend_id: str) -> Optional[BackendResourceInfo]:
        """Report the resource as existing when it owns at least one key.

        The order processor calls ``pull_resource`` before (re)creating a resource
        that already has a ``backend_id``. Without this override the base goes
        through ``UnknownClient`` (always ``None``), so the processor would treat an
        existing resource as missing and re-provision. Key existence lives in the
        Secrets, so consult those.
        """
        if self.list_resource_client_ids(resource_backend_id):
            return BackendResourceInfo(backend_id=resource_backend_id)
        return None

    def recreate_missing_resource(self, waldur_resource: WaldurResource) -> bool:
        """No automatic recreation.

        In the agent-generates model the agent does not hold prior key values
        (Waldur stores them encrypted), so it cannot silently restore a lost key.
        Restoration is portal-driven (rotate re-generates and re-applies), so a
        forced sync only reports state — it never regenerates keys behind the
        user's back.
        """
        client_id = waldur_resource.backend_id
        if client_id and not self.list_resource_client_ids(client_id):
            logger.warning(
                "Resource %s has no keys in the Secrets; rotate from the portal to re-apply.",
                waldur_resource.uuid,
            )
        return False

    def delete_resource(self, waldur_resource: WaldurResource, **kwargs: object) -> None:
        """Remove every one of the resource's keys from both Secrets."""
        del kwargs
        backend_id = waldur_resource.backend_id
        if not backend_id:
            logger.warning("No backend_id for resource %s; nothing to delete", waldur_resource.uuid)
            return
        for client_id in self.list_resource_client_ids(backend_id):
            self.gateway_client.deprovision_key(client_id)
        self.gateway_client.mark_resource_paused(backend_id, False)
        logger.info("Deprovisioned Envoy AI Gateway keys for resource %s", waldur_resource.uuid)

    # --- state transitions ------------------------------------------------------

    def pause_resource(self, resource_backend_id: str) -> bool:
        """Block every key of the resource (move active -> blocked) and record the pause."""
        try:
            self.gateway_client.mark_resource_paused(resource_backend_id, True)
        except EnvoyAIGatewayBackendError:
            # Blocking the keys below still enforces the pause; the marker only
            # keeps keys added later from going live.
            logger.exception("Unable to record the pause of resource %s", resource_backend_id)
        blocked_any = False
        for client_id in self.list_resource_client_ids(resource_backend_id):
            try:
                blocked_any = self.gateway_client.block(client_id) or blocked_any
            except EnvoyAIGatewayBackendError:
                # A swallowed pause silently defeats quota enforcement (the over-limit
                # key keeps serving), so surface it at ERROR rather than a quiet warning.
                logger.exception("Unable to pause (block) key %s", client_id)
        return blocked_any

    def restore_resource(self, resource_backend_id: str) -> bool:
        """Unblock every key of the resource (move blocked -> active)."""
        restored_any = False
        for client_id in self.list_resource_client_ids(resource_backend_id):
            try:
                restored_any = self.gateway_client.unblock(client_id) or restored_any
            except EnvoyAIGatewayBackendError as exc:
                logger.warning("Unable to restore (unblock) key %s: %s", client_id, exc)
        try:
            self.gateway_client.mark_resource_paused(resource_backend_id, False)
        except EnvoyAIGatewayBackendError as exc:
            logger.warning("Unable to clear the pause of resource %s: %s", resource_backend_id, exc)
        return restored_any

    def downscale_resource(self, resource_backend_id: str) -> bool:
        """A key has no partial-capacity state; block it as the safe interpretation."""
        return self.pause_resource(resource_backend_id)

    # --- limits -----------------------------------------------------------------

    def _collect_resource_limits(
        self, waldur_resource: WaldurResource
    ) -> tuple[dict[str, int], dict[str, int]]:
        # Limits live in Waldur, not the gateway; nothing to collect for the backend.
        del waldur_resource
        return {}, {}

    def set_resource_limits(self, resource_backend_id: str, limits: dict[str, int]) -> None:
        """No-op: token/cost limits are enforced by Waldur (report -> pause), not the gateway.

        Kept because the order processor calls it on limit-change orders; there is nothing to
        push, since the gateway only tracks whether a key exists.
        """
        del resource_backend_id, limits

    def get_resource_metadata(self, resource_backend_id: str) -> dict:
        """Return whether any of the resource's keys is currently active."""
        active = any(
            self.gateway_client.is_active(client_id)
            for client_id in self.list_resource_client_ids(resource_backend_id)
        )
        return {
            "backend_type": self.backend_type,
            "active": active,
        }

    # --- usage reporting --------------------------------------------------------

    def _get_usage_report(self, resource_backend_ids: list[str]) -> dict:
        """Usage is reported by the separate reporting backend (usage warehouse)."""
        del resource_backend_ids
        return {}
