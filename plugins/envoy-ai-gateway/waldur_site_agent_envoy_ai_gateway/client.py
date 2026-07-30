"""Kubernetes client for Envoy AI Gateway api-key Secrets.

API keys live as ``clientID: key`` entries in a Kubernetes Secret that the Envoy Gateway
``SecurityPolicy`` reads. To support pause/restore using only the ``client_id``, entries are
moved between an *active* Secret and a *blocked* Secret rather than deleted.
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

    def _read_value(self, secret_name: str, client_id: str) -> Optional[str]:
        try:
            secret = self.core_api.read_namespaced_secret(secret_name, self.namespace)
        except ApiException as exc:
            if exc.status == HTTP_NOT_FOUND:
                return None
            msg = f"Failed to read Secret {secret_name}: {exc}"
            raise EnvoyAIGatewayBackendError(msg) from exc
        raw = (secret.data or {}).get(client_id)
        return base64.b64decode(raw).decode() if raw else None

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

    def deprovision_key(self, client_id: str) -> None:
        """Remove the client from both Secrets.

        Removal from the active Secret must succeed — it gates authentication, and
        swallowing a failure here would report a successful terminate while the key
        stays live. Removal from the blocked Secret is best-effort.
        """
        self._remove_from_secret(self.apikey_secret, client_id, required=True)
        self._remove_from_secret(self.blocked_secret, client_id, required=False)

    def rotate_key(self, client_id: str, new_key: str) -> bool:
        """Replace the client's key value in-place, revoking the previous key.

        Overwrites the entry under the same ``client_id`` in whichever Secret currently
        holds it (active, or blocked when the resource is paused) so the old value stops
        working without changing the active/blocked state. Returns False if the client
        has no entry in either Secret.
        """
        if self._read_value(self.apikey_secret, client_id) is not None:
            self._patch(self.apikey_secret, {"stringData": {client_id: new_key}})
            return True
        if self._read_value(self.blocked_secret, client_id) is not None:
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
        value = self._read_value(self.blocked_secret, client_id)
        if value is None:
            return False
        self._patch(self.apikey_secret, {"stringData": {client_id: value}})
        try:
            self._patch(self.blocked_secret, {"data": {client_id: None}})
        except EnvoyAIGatewayBackendError:
            self._remove_from_secret(self.apikey_secret, client_id, required=False)
            raise
        return True

    def is_active(self, client_id: str) -> bool:
        """Return True if the client currently has an entry in the active Secret."""
        return self._read_value(self.apikey_secret, client_id) is not None

    def exists(self, client_id: str) -> bool:
        """Return True if the client has an entry in the active or the blocked Secret."""
        return (
            self._read_value(self.apikey_secret, client_id) is not None
            or self._read_value(self.blocked_secret, client_id) is not None
        )

    def _list_secret_keys(self, secret_name: str) -> list[str]:
        try:
            secret = self.core_api.read_namespaced_secret(secret_name, self.namespace)
        except ApiException as exc:
            if exc.status == HTTP_NOT_FOUND:
                return []
            msg = f"Failed to read Secret {secret_name}: {exc}"
            raise EnvoyAIGatewayBackendError(msg) from exc
        return list((secret.data or {}).keys())

    def list_client_ids(self, resource_backend_id: str) -> list[str]:
        """Return the client-ids a resource owns across both Secrets.

        Client-ids are ``<resource_backend_id>-<n>``. Matching is an **exact**
        ``<backend_id>-<digits>`` pattern, not a prefix: a prefix match would let
        resource ``proj`` capture the keys of a sibling ``proj-extra`` (whose
        client-ids ``proj-extra-0`` start with ``proj-``), so pause/restore/delete
        would fan out across resource boundaries.
        """
        pattern = re.compile(rf"^{re.escape(resource_backend_id)}-\d+$")
        found: set[str] = set()
        for secret_name in (self.apikey_secret, self.blocked_secret):
            found.update(key for key in self._list_secret_keys(secret_name) if pattern.match(key))
        return sorted(found)
