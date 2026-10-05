"""Kubernetes client for Envoy AI Gateway api-key Secrets.

API keys live as ``clientID: key`` entries in a Kubernetes Secret that the Envoy Gateway
``SecurityPolicy`` reads. To support pause/restore using only the ``client_id``, entries are
moved between an *active* Secret and a *blocked* Secret rather than deleted.

A key is kept out of the active Secret for one of two independent reasons, and the blocked
Secret records which: a resource-wide pause stores it as ``<client_id>``, a pause of that key
alone as ``<client_id>.paused``. Restoring the resource moves only the former back, so it never
un-pauses a key that was paused on its own. A resource-wide pause is also recorded on its own,
as a ``<resource_backend_id>.resource-paused`` entry, so it is known even when the resource has no
key left to block.
"""

from __future__ import annotations

import base64
import logging
import re
from typing import Optional

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

from waldur_site_agent.backend.exceptions import BackendError

logger = logging.getLogger(__name__)

DEFAULT_APIKEY_SECRET = "envoy-ai-gateway-apikeys"  # noqa: S105  # Secret *name*, not a credential
HTTP_NOT_FOUND = 404
# Suffix of a blocked-Secret entry held back by a pause of that key alone. The gateway never
# reads the blocked Secret, so the entry name is free to carry the reason.
KEY_PAUSED_SUFFIX = ".paused"
# Suffix of the blocked-Secret marker recording that a resource itself is paused.
RESOURCE_PAUSED_SUFFIX = ".resource-paused"

# Where a key's entry currently lives.
KEY_ACTIVE = "active"
KEY_BLOCKED = "blocked"  # held back by a resource-wide pause
KEY_PAUSED = "paused"  # held back by a pause of this key alone


class EnvoyAIGatewayBackendError(BackendError):
    """Error raised for Envoy AI Gateway / Kubernetes API failures."""


def _merge_patch_api_client() -> k8s_client.ApiClient:
    """Build an ApiClient that PATCHes with strategic merge semantics.

    The generated client picks the first offered patch content type,
    ``application/json-patch+json``, which expects an array of RFC 6902 operations and
    rejects the dict bodies used here. A dedicated instance is used rather than
    ``ApiClient.get_default()`` because that singleton is shared process-wide.
    """
    api_client = k8s_client.ApiClient()
    api_client.set_default_header("Content-Type", "application/strategic-merge-patch+json")
    return api_client


class EnvoyAIGatewayClient:
    """Manages api-key Secret entries for the Envoy AI Gateway."""

    def __init__(
        self,
        backend_settings: dict,
        core_api: Optional[k8s_client.CoreV1Api] = None,
    ) -> None:
        """Initialize the client.

        Args:
            backend_settings: Offering ``backend_settings`` (namespace, secret names, kubeconfig).
            core_api: Optional pre-built CoreV1Api (injected in tests).
        """
        namespace = backend_settings.get("namespace")
        if not namespace:
            msg = "Envoy AI Gateway backend requires 'namespace' in backend_settings"
            raise EnvoyAIGatewayBackendError(msg)
        self.namespace = namespace
        self.apikey_secret = backend_settings.get("apikey_secret", DEFAULT_APIKEY_SECRET)
        self.blocked_secret = (
            backend_settings.get("blocked_secret") or f"{self.apikey_secret}-blocked"
        )

        # In tests core_api is injected and kube config is never loaded. In-cluster/dev we load the
        # config once and build the API client from it.
        if core_api is not None:
            self.core_api = core_api
        else:
            self._load_config(backend_settings)
            self.core_api = k8s_client.CoreV1Api(_merge_patch_api_client())

    @staticmethod
    def _load_config(backend_settings: dict) -> None:
        kubeconfig_path = backend_settings.get("kubeconfig_path")
        kube_context = backend_settings.get("kube_context")
        try:
            if kubeconfig_path or kube_context:
                # Explicit kubeconfig/context (local/dev): pin the target cluster so we never
                # fall back to the ambient current-context, which may be a remote cluster.
                k8s_config.load_kube_config(config_file=kubeconfig_path, context=kube_context)
            else:
                k8s_config.load_incluster_config()
        except Exception as exc:
            msg = f"Failed to load Kubernetes config: {exc}"
            raise EnvoyAIGatewayBackendError(msg) from exc

    # --- low-level Secret operations -------------------------------------------

    def _patch(self, secret_name: str, body: dict) -> None:
        try:
            self.core_api.patch_namespaced_secret(secret_name, self.namespace, body)
        except ApiException as exc:
            msg = f"Failed to patch Secret {secret_name}: {exc}"
            raise EnvoyAIGatewayBackendError(msg) from exc

    def _read_data(self, secret_name: str) -> dict:
        try:
            secret = self.core_api.read_namespaced_secret(secret_name, self.namespace)
        except ApiException as exc:
            if exc.status == HTTP_NOT_FOUND:
                return {}
            msg = f"Failed to read Secret {secret_name}: {exc}"
            raise EnvoyAIGatewayBackendError(msg) from exc
        return secret.data or {}

    def _read_value(self, secret_name: str, client_id: str) -> Optional[str]:
        raw = self._read_data(secret_name).get(client_id)
        return base64.b64decode(raw).decode() if raw else None

    @staticmethod
    def _paused_entry(client_id: str) -> str:
        return f"{client_id}{KEY_PAUSED_SUFFIX}"

    # --- semantic operations ---------------------------------------------------

    def ping(self) -> bool:
        """Return True if the active api-key Secret is readable."""
        try:
            self.core_api.read_namespaced_secret(self.apikey_secret, self.namespace)
            return True
        except Exception:
            # Not just ApiException: connection/DNS failures raise urllib3/OS errors
            # that must not escape a health check that returns a bool.
            logger.exception("Envoy AI Gateway ping failed")
            return False

    def provision_key(self, client_id: str, api_key: str, *, blocked: bool = False) -> None:
        """Add ``client_id: api_key`` to a Secret.

        Defaults to the active Secret. Pass ``blocked=True`` to add it to the
        blocked Secret instead — a key added to a paused resource must land blocked,
        or the add would silently un-pause the resource and bypass quota enforcement.
        """
        secret_name = self.blocked_secret if blocked else self.apikey_secret
        self._patch(secret_name, {"stringData": {client_id: api_key}})

    def _remove_from_secret(self, secret_name: str, client_id: str, *, required: bool) -> None:
        try:
            self._patch(secret_name, {"data": {client_id: None}})
        except EnvoyAIGatewayBackendError:
            if required:
                raise
            logger.warning("Best-effort removal of %s from %s failed", client_id, secret_name)

    def deprovision_key(self, client_id: str, *, strict: bool = False) -> None:
        """Remove the client from both Secrets, whichever pause holds it.

        Removal from the active Secret must succeed — it gates authentication, and
        swallowing a failure here would report a successful terminate while the key
        stays live. Removal from the blocked Secret is best-effort when the whole
        resource goes, and required with ``strict`` — deleting one key of a resource
        that lives on, where a blocked entry left behind comes back live on the next
        resource restore.
        """
        self._remove_from_secret(self.apikey_secret, client_id, required=True)
        try:
            self._patch(
                self.blocked_secret,
                {"data": {client_id: None, self._paused_entry(client_id): None}},
            )
        except EnvoyAIGatewayBackendError:
            if strict:
                raise
            logger.warning(
                "Best-effort removal of %s from %s failed", client_id, self.blocked_secret
            )

    def rotate_key(self, client_id: str, new_key: str) -> bool:
        """Replace the client's key value in-place, revoking the previous key.

        Overwrites the entry under the same ``client_id`` in whichever Secret currently
        holds it (active, or blocked when the resource is paused) so the old value stops
        working without changing the active/blocked/paused state. Returns False if the
        client has no entry in either Secret.
        """
        blocked = self._read_data(self.blocked_secret)
        paused_entry = self._paused_entry(client_id)
        if self._read_value(self.apikey_secret, client_id) is not None:
            self._patch(self.apikey_secret, {"stringData": {client_id: new_key}})
            # A live key is authoritative; a blocked copy beside it is residue of an
            # interrupted move and still holds the revoked value, which a later pause
            # and resume (or a resource restore) would bring back.
            if blocked.get(client_id) or blocked.get(paused_entry):
                self._patch(
                    self.blocked_secret, {"data": {client_id: None, paused_entry: None}}
                )
            return True
        if blocked.get(paused_entry):
            # The key's own pause is authoritative; a plain copy beside it is race
            # residue that would otherwise keep the revoked value.
            self._patch(
                self.blocked_secret,
                {"stringData": {paused_entry: new_key}, "data": {client_id: None}},
            )
            return True
        if blocked.get(client_id):
            self._patch(self.blocked_secret, {"stringData": {client_id: new_key}})
            return True
        return False

    def block(self, client_id: str) -> bool:
        """Move the client from the active Secret to the blocked Secret.

        Write the blocked copy first (so the key's value is never lost), then clear
        the active copy. If the clear fails the key is still live, so roll the
        blocked copy back — the key must never be left present in both Secrets — and
        surface the error rather than reporting a successful pause.
        """
        value = self._read_value(self.apikey_secret, client_id)
        if value is None:
            return False
        self._patch(self.blocked_secret, {"stringData": {client_id: value}})
        try:
            self._patch(self.apikey_secret, {"data": {client_id: None}})
        except EnvoyAIGatewayBackendError:
            self._remove_from_secret(self.blocked_secret, client_id, required=False)
            raise
        return True

    def unblock(self, client_id: str) -> bool:
        """Move the client from the blocked Secret back to the active Secret.

        Mirror of :meth:`block`: write the active copy first, then clear the blocked
        copy; if the clear fails, roll the active copy back so the key stays blocked
        (fail closed) rather than living in both Secrets.
        """
        blocked = self._read_data(self.blocked_secret)
        if not blocked.get(client_id):
            return False
        if blocked.get(self._paused_entry(client_id)):
            # The key is also paused on its own — a plain copy beside it is the
            # residue of a pause that raced a resource pause. The key's own pause
            # wins; the next pause of the key clears the residue.
            return False
        value = base64.b64decode(blocked[client_id]).decode()
        self._patch(self.apikey_secret, {"stringData": {client_id: value}})
        try:
            self._patch(self.blocked_secret, {"data": {client_id: None}})
        except EnvoyAIGatewayBackendError:
            self._remove_from_secret(self.apikey_secret, client_id, required=False)
            raise
        return True

    @staticmethod
    def _resource_marker(resource_backend_id: str) -> str:
        return f"{resource_backend_id}{RESOURCE_PAUSED_SUFFIX}"

    def is_resource_marked_paused(self, resource_backend_id: str) -> bool:
        """Whether a resource-wide pause is recorded for the resource."""
        marker = self._resource_marker(resource_backend_id)
        return bool(self._read_data(self.blocked_secret).get(marker))

    def mark_resource_paused(self, resource_backend_id: str, paused: bool) -> None:
        """Record or clear a resource-wide pause, independently of its keys.

        Inferring the pause from the keys fails when there is no key to block — every
        key deleted or paused on its own — and a key added then would go live on a
        paused resource. The membership sync pauses or restores every resource on every
        cycle, so the Secret is written only when the marker actually changes.
        """
        marker = self._resource_marker(resource_backend_id)
        present = bool(self._read_data(self.blocked_secret).get(marker))
        if paused and not present:
            self._patch(self.blocked_secret, {"stringData": {marker: "paused"}})
        elif not paused and present:
            self._patch(self.blocked_secret, {"data": {marker: None}})

    def pause_key(self, client_id: str) -> bool:
        """Hold one key back on its own account, whatever the resource's state.

        The key moves to the blocked Secret as ``<client_id>.paused``, from the active
        Secret or from a plain blocked entry left by a resource-wide pause. Within the
        blocked Secret the move is one patch, so there is never a moment in which a
        resource restore could pick the key up. Out of the active Secret it mirrors
        :meth:`block`: the paused copy is written first, and rolled back if the active
        copy cannot be cleared, so the key is never both live and recorded as paused.

        Returns False when the client has no entry anywhere; True once it is paused,
        including when it already was.
        """
        paused_entry = self._paused_entry(client_id)
        blocked = self._read_data(self.blocked_secret)
        if blocked.get(paused_entry):
            # Already paused. A plain copy beside it would be un-paused by the next
            # resource restore, so it goes. An active copy left by an interrupted move
            # is what the gateway honours, so its value — possibly rotated since —
            # replaces the paused one before it is cleared.
            live = self._read_value(self.apikey_secret, client_id)
            body: dict = {"data": {client_id: None}} if blocked.get(client_id) else {}
            if live is not None:
                body["stringData"] = {paused_entry: live}
            if body:
                self._patch(self.blocked_secret, body)
            self._patch(self.apikey_secret, {"data": {client_id: None}})
            return True
        if blocked.get(client_id):
            live = self._read_value(self.apikey_secret, client_id)
            held = live if live is not None else base64.b64decode(blocked[client_id]).decode()
            self._patch(
                self.blocked_secret,
                {"stringData": {paused_entry: held}, "data": {client_id: None}},
            )
            # A live copy can sit beside the blocked one: a restore caught half-way,
            # or a failed rollback. Left there, the key is acknowledged paused and
            # keeps serving, and nothing ever replays a settled pause.
            self._patch(self.apikey_secret, {"data": {client_id: None}})
            return True
        value = self._read_value(self.apikey_secret, client_id)
        if value is None:
            return False
        self._patch(self.blocked_secret, {"stringData": {paused_entry: value}})
        try:
            self._patch(self.apikey_secret, {"data": {client_id: None}})
        except EnvoyAIGatewayBackendError:
            self._remove_from_secret(self.blocked_secret, paused_entry, required=False)
            raise
        return True

    def resume_key(self, client_id: str, *, blocked: bool) -> bool:
        """Lift a key's own pause.

        With ``blocked`` (the resource itself is paused) the key only changes reason:
        it stays in the blocked Secret as a plain entry, so the resource restore brings
        it back with its siblings. Otherwise it returns to the active Secret, mirroring
        :meth:`unblock` — the active copy is written first and rolled back if the
        paused copy cannot be cleared, so the key stays held (fail closed).

        Returns False when the key is not paused on its own account.
        """
        paused_entry = self._paused_entry(client_id)
        value = self._read_value(self.blocked_secret, paused_entry)
        if value is None:
            return False
        if blocked:
            self._patch(
                self.blocked_secret,
                {"stringData": {client_id: value}, "data": {paused_entry: None}},
            )
            return True
        self._patch(self.apikey_secret, {"stringData": {client_id: value}})
        try:
            self._patch(self.blocked_secret, {"data": {paused_entry: None}})
        except EnvoyAIGatewayBackendError:
            self._remove_from_secret(self.apikey_secret, client_id, required=False)
            raise
        return True

    def is_active(self, client_id: str) -> bool:
        """Return True if the client currently has an entry in the active Secret."""
        return self._read_value(self.apikey_secret, client_id) is not None

    def exists(self, client_id: str) -> bool:
        """Return True if the client has an entry in the active or the blocked Secret."""
        blocked = self._read_data(self.blocked_secret)
        return (
            self._read_value(self.apikey_secret, client_id) is not None
            or bool(blocked.get(client_id))
            or bool(blocked.get(self._paused_entry(client_id)))
        )

    def key_states(self, resource_backend_id: str) -> dict[str, str]:
        """Map each client-id the resource owns to where its entry lives.

        Client-ids are ``<resource_backend_id>-<n>``. Matching is an **exact**
        ``<backend_id>-<digits>`` pattern, not a prefix: a prefix match would let
        resource ``proj`` capture the keys of a sibling ``proj-extra`` (whose
        client-ids ``proj-extra-0`` start with ``proj-``), so pause/restore/delete
        would fan out across resource boundaries.

        A key found in more than one place is the residue of an interrupted move. An
        active entry wins, since the gateway honours it whatever else exists; between
        the two blocked entries the key's own pause wins, since a resource restore must
        not treat the key as its own to bring back.
        """
        pattern = re.compile(
            rf"^({re.escape(resource_backend_id)}-\d+)({re.escape(KEY_PAUSED_SUFFIX)})?$"
        )
        states: dict[str, str] = {}
        for entry in self._read_data(self.blocked_secret):
            match = pattern.match(entry)
            if match:
                client_id, paused = match.groups()
                if paused or states.get(client_id) != KEY_PAUSED:
                    states[client_id] = KEY_PAUSED if paused else KEY_BLOCKED
        for entry in self._read_data(self.apikey_secret):
            match = pattern.match(entry)
            if match and not match.group(2):
                states[match.group(1)] = KEY_ACTIVE
        return dict(sorted(states.items()))

    def list_client_ids(self, resource_backend_id: str) -> list[str]:
        """Return the client-ids a resource owns across both Secrets, paused ones included."""
        return list(self.key_states(resource_backend_id))
