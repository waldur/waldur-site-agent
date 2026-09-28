"""Kubernetes client for ManagedNamespace custom resources."""

from typing import Optional

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

from waldur_site_agent.backend import logger
from waldur_site_agent.backend.exceptions import BackendError

MNS_API_GROUP = "provisioning.hpc.ut.ee"
MNS_API_VERSION = "v1"
MNS_PLURAL = "managednamespaces"

HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409


class K8sUtNamespaceClient:
    """Client for managing ManagedNamespace custom resources on Kubernetes."""

    def __init__(self, backend_settings: dict) -> None:
        """Initialize Kubernetes client."""
        kubeconfig_path = backend_settings.get("kubeconfig_path")
        try:
            if kubeconfig_path:
                k8s_config.load_kube_config(config_file=kubeconfig_path)
            else:
                k8s_config.load_incluster_config()
        except Exception as e:
            raise BackendError(f"Failed to load Kubernetes config: {e}") from e

        self.api_client = k8s_client.ApiClient()
        self.custom_api = k8s_client.CustomObjectsApi(self.api_client)
        self.cr_namespace = backend_settings.get("cr_namespace", "waldur-system")

        logger.info(
            "Initialized K8s client for ManagedNamespace CRs in namespace %s",
            self.cr_namespace,
        )

    def ping(self) -> bool:
        """Check Kubernetes cluster connectivity."""
        try:
            version_api = k8s_client.VersionApi(self.api_client)
            version_api.get_code()
            return True
        except Exception as e:
            logger.error(f"Failed to ping Kubernetes cluster: {e}")
            return False

    def create_managed_namespace(self, name: str, spec: dict) -> dict:
        """Create a ManagedNamespace custom resource."""
        body = {
            "apiVersion": f"{MNS_API_GROUP}/{MNS_API_VERSION}",
            "kind": "ManagedNamespace",
            "metadata": {"name": name, "namespace": self.cr_namespace},
            "spec": spec,
        }
        try:
            result = self.custom_api.create_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
                body=body,
            )
            logger.info("Created ManagedNamespace CR: %s", name)
            return result
        except ApiException as e:
            raise BackendError(f"Failed to create ManagedNamespace {name}: {e}") from e

    def get_managed_namespace(self, name: str) -> Optional[dict]:
        """Get a ManagedNamespace custom resource by name."""
        try:
            return self.custom_api.get_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == HTTP_NOT_FOUND:
                return None
            raise BackendError(f"Failed to get ManagedNamespace {name}: {e}") from e

    def list_managed_namespaces(self) -> list[dict]:
        """List all ManagedNamespace custom resources."""
        try:
            result = self.custom_api.list_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
            )
            return result.get("items", [])
        except ApiException as e:
            raise BackendError(f"Failed to list ManagedNamespaces: {e}") from e

    def patch_managed_namespace(self, name: str, patch: dict) -> dict:
        """Patch a ManagedNamespace custom resource."""
        try:
            result = self.custom_api.patch_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
                name=name,
                body=patch,
            )
            logger.info("Patched ManagedNamespace CR: %s", name)
            return result
        except ApiException as e:
            raise BackendError(f"Failed to patch ManagedNamespace {name}: {e}") from e

    def replace_managed_namespace(self, name: str, obj: dict) -> dict:
        """Replace a ManagedNamespace CR with *obj*.

        *obj* must carry the ``metadata.resourceVersion`` it was read with. Unlike
        :meth:`patch_managed_namespace` (a JSON merge patch, which never checks whether
        the object changed since it was read), this is a full PUT: the API server
        rejects it with 409 if *obj*'s resourceVersion is stale. Callers doing a
        read-modify-write need this -- a merge patch alone silently lets a concurrent
        writer's update be clobbered (confirmed live: report and membership_sync modes
        both call _get_usage_report() independently, with no coordination between
        them).

        Raises:
            ApiException: With ``status == 409`` on a stale resourceVersion -- callers
                should re-read and retry, not treat this as a hard failure.
        """
        try:
            result = self.custom_api.replace_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
                name=name,
                body=obj,
            )
            logger.info("Replaced ManagedNamespace CR: %s", name)
            return result
        except ApiException as e:
            if e.status == HTTP_CONFLICT:
                raise
            raise BackendError(f"Failed to replace ManagedNamespace {name}: {e}") from e

    def delete_managed_namespace(self, name: str) -> None:
        """Delete a ManagedNamespace custom resource."""
        try:
            self.custom_api.delete_namespaced_custom_object(
                group=MNS_API_GROUP,
                version=MNS_API_VERSION,
                namespace=self.cr_namespace,
                plural=MNS_PLURAL,
                name=name,
            )
            logger.info("Deleted ManagedNamespace CR: %s", name)
        except ApiException as e:
            if e.status == HTTP_NOT_FOUND:
                logger.warning("ManagedNamespace %s not found, skipping deletion", name)
                return
            raise BackendError(f"Failed to delete ManagedNamespace {name}: {e}") from e
