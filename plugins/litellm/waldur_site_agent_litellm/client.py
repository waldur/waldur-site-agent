"""HTTP client for the LiteLLM proxy key-management API.

LiteLLM stores virtual keys sha256-hashed, so the plaintext ``sk-…`` is readable
exactly once, in the ``/key/generate`` response. Every later call therefore addresses
a key by its ``key_alias`` (which the agent chooses and which is stable) or by the
hash LiteLLM returns as ``token``. This client never caches plaintext.

It also manages LiteLLM *users*, which exist for a different reason than keys: see the
"users" section below.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from waldur_site_agent.backend.exceptions import BackendError

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0
# /key/list caps the page size; 100 keeps the number of round trips low without
# tripping it.
KEY_LIST_PAGE_SIZE = 100
# /user/list caps page_size at 100 server-side; asking for more is a 422.
USER_LIST_PAGE_SIZE = 100
# A defensive stop so a paging bug cannot spin forever against a live proxy.
MAX_PAGES = 1000
# Substrings LiteLLM uses when the entity asked about simply is not there. It answers
# 400 rather than 404 on some of these paths, so the status code alone cannot tell an
# absent user from a malformed request -- see ``_request``.
_MISSING_ENTITY_MARKERS = ("does not exist", "not found", "no user found")

# Keys the plugin writes into a LiteLLM user's ``metadata``. The proxy has no field of
# its own for "who does this belong to", and a user_id here is an email address, which
# says nothing about which resource is paying for it -- so ownership is recorded here.
# They live in the client rather than in either backend because both backends read
# them: the management one stamps them, the reporting one resolves usage through them.
META_RESOURCE = "waldur_resource"
META_USERNAME = "waldur_username"


def verify_ssl_setting(backend_settings: dict) -> bool:
    """Read ``verify_ssl``, defaulting to on.

    ``.get("verify_ssl", True)`` only covers an absent key: a bare ``verify_ssl:`` in
    the YAML is *present* and ``None``, which ``bool()`` reads as False and silently
    turns certificate verification off. Verification is not something to lose to a
    stray colon, so only an explicit false value disables it.
    """
    verify = backend_settings.get("verify_ssl")
    return True if verify is None else bool(verify)


def timeout_setting(backend_settings: dict) -> float:
    """Read ``timeout``, defaulting when it is absent or explicitly null.

    Same trap as ``verify_ssl``: a present-but-null value reaches ``float(None)`` and
    raises, taking the backend down at construction over an empty config line.
    """
    timeout = backend_settings.get("timeout")
    return DEFAULT_TIMEOUT if timeout is None else float(timeout)


class LiteLLMBackendError(BackendError):
    """Error raised for LiteLLM management API failures."""


class LiteLLMEnterpriseFeatureError(LiteLLMBackendError):
    """Raised when the proxy gates the requested endpoint behind a licence.

    Separate from the generic error so a caller can fall back to an
    OSS-available path instead of failing the operation. ``/key/{key}/regenerate``
    is the one this plugin actually hits.
    """


class LiteLLMClient:
    """Manages virtual keys on a LiteLLM proxy."""

    def __init__(self, backend_settings: dict) -> None:
        """Initialize the client from the offering's ``backend_settings``."""
        api_url = backend_settings.get("api_url")
        if not api_url:
            msg = "LiteLLM backend requires 'api_url' in backend_settings"
            raise LiteLLMBackendError(msg)
        api_token = backend_settings.get("api_token")
        if not api_token:
            msg = "LiteLLM backend requires 'api_token' (master or admin key) in backend_settings"
            raise LiteLLMBackendError(msg)

        self.api_url = str(api_url).rstrip("/")
        self.api_token = str(api_token)
        self.verify_ssl = verify_ssl_setting(backend_settings)
        self.timeout = timeout_setting(backend_settings)
        # One pooled client for the instance rather than one per call: a single
        # membership-sync pass makes a /key/list request per page and a /key/update,
        # /key/block or /key/delete per key, and a fresh client would pay a TCP and
        # TLS handshake for each of them. Same shape as the nextcloud and mup clients.
        self.session = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers=self._headers(),
        )

    # --- transport --------------------------------------------------------------

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def close(self) -> None:
        """Release the pooled connections."""
        self.session.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        none_on_404: bool = False,
        none_on_missing_entity: bool = False,
    ) -> Any:  # noqa: ANN401 - the proxy returns both dicts and lists
        """Perform one request, translating every failure into a backend error.

        ``none_on_404`` maps LiteLLM's "Key not found." onto ``None`` so callers can
        treat an absent key as a state rather than a failure — the block/unblock/info
        paths all run against a key that may legitimately have been removed at the
        proxy.

        ``none_on_missing_entity`` extends that to the paths where the proxy reports an
        absent object with a 400 instead: ``/user/info`` is the one this plugin hits.
        Without it, provisioning a user for the first time — the case where the lookup
        is *expected* to find nothing — would raise instead of falling through to the
        create. It is opt-in per call rather than global because on a write path the
        same prose means the operation failed, and swallowing that would report a
        create or a delete that never happened.
        """
        url = f"{self.api_url}{path}"
        try:
            response = self.session.request(method, url, params=params, json=json_body)
            if none_on_404 and response.status_code == httpx.codes.NOT_FOUND:
                return None
            if none_on_missing_entity and self._is_missing_entity(response):
                return None
            self._raise_for_enterprise(response, path)
            response.raise_for_status()
            return response.json()
        except LiteLLMBackendError:
            raise
        except httpx.HTTPStatusError as exc:
            msg = f"LiteLLM {path} failed: {exc.response.status_code} {exc.response.text}"
            raise LiteLLMBackendError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"LiteLLM {path} request error: {exc}"
            raise LiteLLMBackendError(msg) from exc
        except ValueError as exc:
            # response.json() raises ValueError on a non-JSON body behind a 2xx.
            msg = f"LiteLLM {path} returned invalid JSON: {exc}"
            raise LiteLLMBackendError(msg) from exc

    @staticmethod
    def _is_missing_entity(response: httpx.Response) -> bool:
        """True when the response says the object asked about does not exist."""
        if response.is_success:
            return False
        if response.status_code not in (httpx.codes.BAD_REQUEST, httpx.codes.NOT_FOUND):
            return False
        try:
            body = response.text.lower()
        except Exception:  # pragma: no cover - httpx keeps the body in memory
            return False
        return any(marker in body for marker in _MISSING_ENTITY_MARKERS)

    @staticmethod
    def _raise_for_enterprise(response: httpx.Response, path: str) -> None:
        """Turn a licence refusal into :class:`LiteLLMEnterpriseFeatureError`.

        The proxy does not use one status for this: ``/key/{key}/regenerate`` answers
        500, ``/global/spend/report`` 400 and ``/key/spend/report`` 403, all carrying
        the same "You must be a LiteLLM Enterprise user" prose. Matching on the text
        is therefore the only reliable discriminator, and it has to happen before
        ``raise_for_status`` so the caller sees the specific type.

        The word "enterprise" alone, not that plus "license". The two errors are
        asymmetric: read a licence refusal as a generic failure and rotation breaks
        outright on every open-source proxy, while reading some other failure as a
        licence refusal only sends rotation down the delete-and-mint path, which is
        what an OSS proxy uses anyway. So the loose match is the safe one, and it
        cannot fire on a success.
        """
        if response.is_success:
            return
        try:
            body = response.text
        except Exception:  # pragma: no cover - httpx keeps the body in memory
            return
        if "enterprise" in body.lower():
            msg = f"LiteLLM {path} is gated behind an enterprise licence"
            raise LiteLLMEnterpriseFeatureError(msg)

    # --- health -----------------------------------------------------------------

    def ping(self) -> bool:
        """Return True when the proxy reports itself healthy with a live database.

        The key API is useless without the database — virtual keys live there — so a
        proxy answering ``db: "Not connected"`` is treated as down rather than up.

        Authenticated like every other call: ``/health/readiness`` is open on a stock
        proxy, but one configured to require a key answers 401, and an unauthenticated
        probe would report the backend permanently down.
        """
        try:
            response = self.session.get(f"{self.api_url}/health/readiness")
            if response.status_code != httpx.codes.OK:
                return False
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            logger.exception("LiteLLM readiness check failed")
            return False
        if not isinstance(payload, dict):
            return False
        return payload.get("db") == "connected"

    # --- keys -------------------------------------------------------------------

    def generate_key(
        self,
        key_alias: str,
        *,
        models: Optional[list] = None,
        blocked: bool = False,
        max_budget: Optional[float] = None,
        budget_duration: Optional[str] = None,
        tpm_limit: Optional[int] = None,
        rpm_limit: Optional[int] = None,
    ) -> dict:
        """Mint a key and return the proxy's response.

        No ``key`` is sent, so LiteLLM generates the material itself. That keeps the
        plugin working against a proxy with ``disable_custom_api_keys`` set, and
        avoids the agent inventing secrets for a system that only stores hashes.

        No ``duration`` is sent either: the keys do not expire, because Waldur owns
        the resource lifecycle and an expiry it does not know about would revoke
        access behind its back.
        """
        body: dict = {"key_alias": key_alias}
        if models:
            body["models"] = list(models)
        if blocked:
            body["blocked"] = True
        if max_budget is not None:
            body["max_budget"] = max_budget
        if budget_duration is not None:
            body["budget_duration"] = budget_duration
        if tpm_limit is not None:
            body["tpm_limit"] = tpm_limit
        if rpm_limit is not None:
            body["rpm_limit"] = rpm_limit

        payload = self._request("POST", "/key/generate", json_body=body)
        if not isinstance(payload, dict) or not payload.get("key"):
            msg = f"LiteLLM /key/generate returned no key for alias {key_alias}"
            raise LiteLLMBackendError(msg)
        return payload

    def list_keys(self, alias_prefix: str) -> list:
        """Return the full key records whose alias contains ``alias_prefix``.

        ``substring_matching`` narrows the query server-side, which keeps this from
        paging the proxy's whole key table. It is only a narrowing: the match is a
        substring, so ``res1-`` also returns ``res1-extra-1``. The caller applies the
        exact slot pattern — see
        :meth:`~waldur_site_agent_litellm.backend.LiteLLMBackend.list_resource_client_ids`.
        """
        keys: list = []
        page = 1
        while page <= MAX_PAGES:
            payload = self._request(
                "GET",
                "/key/list",
                params={
                    "key_alias": alias_prefix,
                    "substring_matching": "true",
                    "return_full_object": "true",
                    "page": page,
                    "size": KEY_LIST_PAGE_SIZE,
                },
            )
            if not isinstance(payload, dict):
                break
            batch = payload.get("keys") or []
            keys.extend(item for item in batch if isinstance(item, dict))
            # An empty page ends the walk whatever the metadata claims -- the same guard
            # the usage walk carries. ``total_pages`` counts the whole match, and a key
            # deleted between two requests leaves the proxy reporting a page that is no
            # longer there; trusting the count alone spends the rest of MAX_PAGES
            # fetching nothing. A page that carried nothing has nothing after it either.
            if not batch:
                break
            total_pages = payload.get("total_pages") or 0
            if page >= total_pages:
                break
            page += 1
        return keys

    def get_key(self, token: str) -> Optional[dict]:
        """Return one key's record by hash, or None when the proxy has no such key."""
        payload = self._request(
            "GET", "/key/info", params={"key": token}, none_on_404=True
        )
        if not isinstance(payload, dict):
            return None
        info = payload.get("info")
        return info if isinstance(info, dict) else payload

    def update_key(self, token: str, fields: dict) -> None:
        """Apply ``fields`` to one key, addressed by hash."""
        body = dict(fields)
        body["key"] = token
        self._request("POST", "/key/update", json_body=body)

    def block(self, token: str) -> bool:
        """Block one key. Returns False when the proxy no longer holds it."""
        payload = self._request(
            "POST", "/key/block", json_body={"key": token}, none_on_404=True
        )
        return payload is not None

    def unblock(self, token: str) -> bool:
        """Unblock one key. Returns False when the proxy no longer holds it."""
        payload = self._request(
            "POST", "/key/unblock", json_body={"key": token}, none_on_404=True
        )
        return payload is not None

    def delete_keys(self, tokens: list) -> None:
        """Delete keys by hash. A no-op for an empty list."""
        if not tokens:
            return
        self._request("POST", "/key/delete", json_body={"keys": list(tokens)})

    def regenerate_key(self, token: str) -> str:
        """Rotate a key in place, keeping its alias, and return the new plaintext.

        Raises :class:`LiteLLMEnterpriseFeatureError` on a proxy without a licence —
        this endpoint is enterprise-only, so on the open-source build the caller has
        to fall back to delete-then-generate.
        """
        payload = self._request("POST", f"/key/{token}/regenerate", json_body={})
        if not isinstance(payload, dict) or not payload.get("key"):
            msg = "LiteLLM /key/regenerate returned no key"
            raise LiteLLMBackendError(msg)
        return str(payload["key"])

    # --- users ------------------------------------------------------------------
    #
    # A LiteLLM *user* is a different object from a virtual *key*. Keys carry the API
    # surface; users exist so chat traffic arriving on Open WebUI's single shared key
    # can be attributed to a person, because the proxy resolves the forwarded
    # ``X-OpenWebUI-User-Email`` header to ``user_id`` verbatim. That is why every user
    # here is created with ``user_id`` set to the person's email address rather than to
    # a UUID: Open WebUI mints its own opaque ids and offers no way to set one, and
    # LiteLLM declined to match on the ``user_email`` column (BerriAI/litellm#21927), so
    # the email is the only identifier the two systems can agree on.

    def create_user(
        self,
        user_id: str,
        *,
        user_email: str,
        user_alias: Optional[str] = None,
        models: Optional[list] = None,
        max_budget: Optional[float] = None,
        budget_duration: Optional[str] = None,
        tpm_limit: Optional[int] = None,
        rpm_limit: Optional[int] = None,
        max_parallel_requests: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        """Create one internal user and return the proxy's response.

        ``auto_create_key`` is sent as false, against LiteLLM's own default of true. A
        key minted here would be a live credential that Waldur never sees: the agent
        does not keep key material, so it could not report it, and nothing in the
        portal could then rotate or revoke it. Every key this plugin is responsible for
        comes from ``generate_resource_keys`` and is reported to Waldur in the same
        breath — see the module docstring.
        """
        body: dict = {
            "user_id": user_id,
            "user_email": user_email,
            "user_role": "internal_user",
            "auto_create_key": False,
        }
        if user_alias:
            body["user_alias"] = user_alias
        if models:
            body["models"] = list(models)
        if max_budget is not None:
            body["max_budget"] = max_budget
        if budget_duration is not None:
            body["budget_duration"] = budget_duration
        if tpm_limit is not None:
            body["tpm_limit"] = tpm_limit
        if rpm_limit is not None:
            body["rpm_limit"] = rpm_limit
        if max_parallel_requests is not None:
            body["max_parallel_requests"] = max_parallel_requests
        if metadata is not None:
            body["metadata"] = dict(metadata)

        payload = self._request("POST", "/user/new", json_body=body)
        if not isinstance(payload, dict):
            msg = f"LiteLLM /user/new returned no user record for {user_id}"
            raise LiteLLMBackendError(msg)
        return payload

    def get_user(self, user_id: str) -> Optional[dict]:
        """Return one user's record, or None when the proxy has no such user.

        ``/user/info`` wraps the row in ``user_info`` on some versions and returns it
        flat on others; both shapes are unwrapped here so callers see one.

        A proxy that answers 400 rather than 404 for an unknown user is treated the
        same way. LiteLLM is not consistent about which it uses, and reading "no such
        user" as a transport failure would turn every first-time provision into an
        error instead of a create.
        """
        payload = self._request(
            "GET", "/user/info", params={"user_id": user_id}, none_on_404=True,
            none_on_missing_entity=True,
        )
        if not isinstance(payload, dict):
            return None
        info = payload.get("user_info")
        return info if isinstance(info, dict) else payload

    def update_user(self, user_id: str, fields: dict) -> None:
        """Apply ``fields`` to one user."""
        body = dict(fields)
        body["user_id"] = user_id
        self._request("POST", "/user/update", json_body=body)

    def delete_users(self, user_ids: list) -> None:
        """Delete users by id. A no-op for an empty list."""
        if not user_ids:
            return
        self._request("POST", "/user/delete", json_body={"user_ids": list(user_ids)})

    def list_users(self, role: str = "internal_user") -> list:
        """Return every user record with the given role.

        Used to rebuild the email-to-resource map from the proxy itself, so the
        reporting backend — which never sees Waldur's team lists — can attribute chat
        usage without the management backend having to hand anything over.
        """
        users: list = []
        page = 1
        while page <= MAX_PAGES:
            payload = self._request(
                "GET",
                "/user/list",
                params={"role": role, "page": page, "page_size": USER_LIST_PAGE_SIZE},
            )
            if not isinstance(payload, dict):
                break
            batch = payload.get("users") or []
            users.extend(item for item in batch if isinstance(item, dict))
            # Same guard as the key walk: an empty page ends it whatever the metadata
            # says, so a proxy that never clears ``total_pages`` cannot spin MAX_PAGES
            # requests inside one pass.
            if not batch:
                break
            total_pages = payload.get("total_pages") or 0
            if page >= total_pages:
                break
            page += 1
        return users
