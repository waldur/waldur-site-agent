"""Thin wrapper around ``CustomObjectsApi`` for ManagedRancherProject CRs.

Handles the three operations the backend needs: apply (server-side
patch with ``force=True``, falls back to create on 404), get (returns
None when missing), and delete. No list/watch in v1 — the
membership-sync model is poll-Waldur, push-CR.
"""

import logging
from typing import Any, Optional

from kubernetes import client as k8s
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

from .translator import CRD_API_VERSION, CRD_PLURAL

logger = logging.getLogger(__name__)

_GROUP, _VERSION = CRD_API_VERSION.split("/", 1)
_HTTP_NOT_FOUND = 404
_HTTP_CONFLICT = 409
_APPLY_MAX_RETRIES = 5


class CrdClient:
    """K8s client for ManagedRancherProject CRDs scoped to one namespace."""

    def __init__(
        self,
        namespace: str,
        kubeconfig_path: Optional[str] = None,
        context: Optional[str] = None,
    ) -> None:
        """Initialize with a namespace and optional kubeconfig path/context.

        If ``kubeconfig_path`` is None we try in-cluster config first,
        then fall back to the default kubeconfig location.
        """
        if kubeconfig_path:
            k8s_config.load_kube_config(config_file=kubeconfig_path, context=context)
        else:
            try:
                k8s_config.load_incluster_config()
            except k8s_config.ConfigException:
                k8s_config.load_kube_config(context=context)

        self.namespace = namespace
        self.api = k8s.CustomObjectsApi()

    def get(self, name: str) -> Optional[dict[str, Any]]:
        """Fetch a CR by name; return ``None`` if it doesn't exist."""
        try:
            return self.api.get_namespaced_custom_object(
                group=_GROUP,
                version=_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == _HTTP_NOT_FOUND:
                return None
            raise

    def apply(self, body: dict[str, Any]) -> dict[str, Any]:
        """Idempotent create-or-update via GET-then-PUT (replace).

        Why not patch: the kubernetes Python client uses JSON Merge
        Patch (RFC 7396) for CRDs by default, which recursively merges
        nested maps — keys absent from the patch are kept. That breaks
        the desired-state pattern (removing a quota key in Waldur
        wouldn't remove it from the CR). Replace fixes that.

        Status preservation: the operator writes its .status via the
        ``/status`` subresource (separate API endpoint), so PUT on
        ``/managedrancherprojects/<name>`` does NOT clobber it as long
        as the CRD has ``subresources: { status: {} }`` (it does).

        Concurrency: status subresource updates still bump the parent
        object's resourceVersion, so a status write between our GET
        and PUT yields a 409. Retry up to ``_APPLY_MAX_RETRIES`` with
        a fresh GET each time.
        """
        name = body["metadata"]["name"]
        for attempt in range(_APPLY_MAX_RETRIES):
            existing = self.get(name)
            if existing is None:
                return self.api.create_namespaced_custom_object(
                    group=_GROUP,
                    version=_VERSION,
                    namespace=self.namespace,
                    plural=CRD_PLURAL,
                    body=body,
                )
            send = dict(body)
            send["metadata"] = dict(body.get("metadata") or {})
            send["metadata"]["resourceVersion"] = existing["metadata"][
                "resourceVersion"
            ]
            try:
                return self.api.replace_namespaced_custom_object(
                    group=_GROUP,
                    version=_VERSION,
                    namespace=self.namespace,
                    plural=CRD_PLURAL,
                    name=name,
                    body=send,
                )
            except ApiException as e:
                if e.status != _HTTP_CONFLICT or attempt == _APPLY_MAX_RETRIES - 1:
                    raise
                logger.debug("apply() 409 on %s, retrying (attempt %s)", name, attempt + 1)
        msg = f"apply() exhausted retries for {name}"
        raise RuntimeError(msg)

    def delete(self, name: str) -> bool:
        """Delete a CR by name; return True if a delete was issued, False if absent."""
        try:
            self.api.delete_namespaced_custom_object(
                group=_GROUP,
                version=_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == _HTTP_NOT_FOUND:
                return False
            raise
        return True

    def list_for_resource(self, resource_uuid: str) -> list[dict[str, Any]]:
        """List CRs in this namespace tagged with a given Waldur resource UUID.

        Used by the membership-sync orphan-pruning path: after applying
        the CRs for a resource's *current* ResourceProjects, anything
        else matching the label is a stale CR for an RP that no longer
        exists in Waldur and should be deleted.

        CRs created before the label-emitting translator change won't be
        matched — that is intentional: deleting an unlabelled CR could
        affect resources this client doesn't own. The translator always
        applies the label on the next sync, so existing CRs self-heal
        and become eligible for pruning from then on.
        """
        try:
            result = self.api.list_namespaced_custom_object(
                group=_GROUP,
                version=_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                label_selector=f"waldur.io/resource-uuid={resource_uuid}",
            )
            return result.get("items", [])
        except ApiException as e:
            if e.status == _HTTP_NOT_FOUND:
                return []
            raise
