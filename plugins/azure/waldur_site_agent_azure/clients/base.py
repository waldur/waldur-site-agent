"""Shared credential handling and error translation for the Azure clients.

The ``azure.*`` imports are not deferred: this package is imported only when an
offering names the ``azure`` backend, so the memory the SDK costs falls only on
the deployments that use Azure.
"""

from __future__ import annotations

import functools
from http import HTTPStatus
from typing import Any, Callable, Optional, TypeVar

from azure.core.exceptions import AzureError
from azure.core.polling import LROPoller
from azure.identity import ClientSecretCredential

T = TypeVar("T")


class AzureClientError(Exception):
    """Raised when a call to Azure fails.

    ``status_code`` carries the HTTP status when Azure supplied one, so callers
    can tell "this machine is gone" from "Azure would not answer" — the two must
    not lead to the same decision.
    """

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        """Record the message and, when Azure gave one, the HTTP status."""
        super().__init__(message)
        self.status_code = status_code

    @property
    def not_found(self) -> bool:
        """Whether Azure answered that the object does not exist."""
        return self.status_code == HTTPStatus.NOT_FOUND


def _as_client_error(exc: AzureError) -> AzureClientError:
    """Restate an SDK failure in the plugin's own exception type."""
    return AzureClientError(str(exc), status_code=getattr(exc, "status_code", None))


class _TranslatingPoller:
    """A long-running operation whose wait fails the way its start would.

    An ``azure-mgmt`` ``begin_*`` call returns before Azure has done the work;
    the failure usually arrives later, out of ``result()``. Wrapping only the
    call that starts the operation therefore catches the rarer half: a delete
    that 404s while being awaited would reach the backend as an ``azure.core``
    exception, past every ``except AzureClientError`` that exists to tolerate an
    object already gone.
    """

    def __init__(self, poller: Any) -> None:
        """Bind to the poller whose waiting is being translated."""
        self._poller = poller

    def result(self, *args: Any, **kwargs: Any) -> Any:
        """Wait for the operation, restating an SDK failure as ``AzureClientError``."""
        try:
            return self._poller.result(*args, **kwargs)
        except AzureError as exc:
            raise _as_client_error(exc) from exc

    def wait(self, *args: Any, **kwargs: Any) -> Any:
        """Wait without reading the result, translating the same way."""
        try:
            return self._poller.wait(*args, **kwargs)
        except AzureError as exc:
            raise _as_client_error(exc) from exc

    def __getattr__(self, name: str) -> Any:
        """Defer everything else — ``status``, ``done`` — to the real poller."""
        return getattr(self._poller, name)


def wrap_azure_errors(func: Callable[..., T]) -> Callable[..., T]:
    """Translate SDK failures into ``AzureClientError``.

    Every call site would otherwise repeat the same try/except, and the agent
    would leak ``azure.core`` exception types into the core's error handling.
    A poller is returned wrapped, so that the wait it exists for is covered too.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        try:
            result = func(*args, **kwargs)
        except AzureError as exc:
            raise _as_client_error(exc) from exc
        if isinstance(result, LROPoller):
            return _TranslatingPoller(result)  # type: ignore[return-value]
        return result

    return wrapper


class AzureCredentials:
    """The service principal a set of clients authenticates with."""

    def __init__(
        self, subscription_id: str, tenant_id: str, client_id: str, client_secret: str
    ) -> None:
        """Store the subscription and service principal identifiers."""
        self.subscription_id = subscription_id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    @functools.cached_property
    def credential(self) -> ClientSecretCredential:
        """Return the SDK credential object, built once per set of credentials."""
        return ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )


class BaseAzureClient:
    """Common construction for the per-service clients."""

    def __init__(self, credentials: AzureCredentials) -> None:
        """Bind the client to a set of credentials."""
        self.credentials = credentials

    @property
    def subscription_id(self) -> str:
        """Return the subscription the client operates on."""
        return self.credentials.subscription_id
