"""HTTP client for the Open WebUI admin API.

Open WebUI is the chat surface in front of the same LiteLLM proxy. It holds its own
user table, and the two systems are married by **email**: Open WebUI mints an opaque id
of its own that cannot be set from outside, and it forwards the signed-in person's
address as ``X-OpenWebUI-User-Email`` on every upstream call, which the proxy resolves
straight onto ``user_id``. So the email is the only identifier both ends can agree on,
and it is what this client addresses accounts by.

Why the agent has to touch Open WebUI at all
--------------------------------------------
Every request Open WebUI makes upstream carries **one shared virtual key**, and LiteLLM
applies the forwarded header *after* authentication — it overwrites ``user_id`` for
attribution and re-checks nothing. A person's LiteLLM budget and rate limits were
already evaluated against the shared key's owner, and an email the proxy has never seen
is not rejected: it simply accrues daily spend rows against a user row that does not
exist. Deleting or blocking someone's LiteLLM user therefore does **not** end their
chat access. Only Open WebUI can do that, which is why removal runs here.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from waldur_site_agent.backend.exceptions import BackendError

from .client import timeout_setting, verify_ssl_setting

logger = logging.getLogger(__name__)

# The admin user listing is fixed at Open WebUI's own PAGE_ITEM_COUNT; the query is
# narrowed server-side by email, so one page is enough in practice and the walk exists
# only for the case where an address is a substring of several others.
MAX_PAGES = 100

# Open WebUI's role for an account that exists but may not use the product — what the
# UI shows as "pending approval". Removing access by demotion rather than deletion
# keeps the person's chat history and makes a re-add reversible.
ROLE_DISABLED = "pending"
ROLE_ACTIVE = "user"


class OpenWebUIError(BackendError):
    """Error raised for Open WebUI admin API failures."""


class OpenWebUIClient:
    """Manages user accounts on an Open WebUI instance."""

    def __init__(self, settings: dict) -> None:
        """Initialize the client from the offering's ``openwebui`` settings block."""
        api_url = settings.get("api_url")
        if not api_url:
            msg = "Open WebUI integration requires 'api_url' in the openwebui settings"
            raise OpenWebUIError(msg)
        api_token = settings.get("api_token")
        if not api_token:
            msg = "Open WebUI integration requires 'api_token' in the openwebui settings"
            raise OpenWebUIError(msg)

        self.api_url = str(api_url).rstrip("/")
        self.api_token = str(api_token)
        self.verify_ssl = verify_ssl_setting(settings)
        self.timeout = timeout_setting(settings)

        self.session = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
            },
        )

    # --- transport --------------------------------------------------------------

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
    ) -> Any:  # noqa: ANN401 - the API returns dicts, lists and bare booleans
        """Perform one request, translating every failure into a backend error."""
        url = f"{self.api_url}{path}"
        try:
            response = self.session.request(method, url, params=params, json=json_body)
            if none_on_404 and response.status_code == httpx.codes.NOT_FOUND:
                return None
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            msg = f"Open WebUI {path} failed: {exc.response.status_code} {exc.response.text}"
            raise OpenWebUIError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"Open WebUI {path} request error: {exc}"
            raise OpenWebUIError(msg) from exc
        except ValueError as exc:
            msg = f"Open WebUI {path} returned invalid JSON: {exc}"
            raise OpenWebUIError(msg) from exc

    def ping(self) -> bool:
        """Return True when the instance answers its health endpoint."""
        try:
            response = self.session.get(f"{self.api_url}/health")
        except httpx.HTTPError:
            logger.exception("Open WebUI health check failed")
            return False
        return response.status_code == httpx.codes.OK

    # --- users ------------------------------------------------------------------

    @staticmethod
    def _normalize(email: str) -> str:
        """Open WebUI lowercases every address it stores, so comparisons must too."""
        return email.strip().lower()

    def find_user(self, email: str) -> Optional[dict]:
        """Return the account with this exact address, or None.

        There is no get-by-email endpoint. ``GET /api/v1/users/`` takes a free-text
        ``query`` which Open WebUI matches as a *substring* against name and email, so
        the result is a candidate list that still has to be filtered exactly here —
        matching loosely would let ``ann@x.org`` resolve to ``joann@x.org`` and hand
        one person's account to another.
        """
        wanted = self._normalize(email)
        page = 1
        while page <= MAX_PAGES:
            payload = self._request("GET", "/api/v1/users/", params={"query": wanted, "page": page})
            if not isinstance(payload, dict):
                # Older builds answer with a bare list. Anything else is a shape this
                # client cannot read, and returning ``None`` would spell it "no such
                # account" -- turning every revoke into a silent no-op while the person
                # keeps chatting, and every managed-password add into 400 EMAIL_TAKEN.
                if isinstance(payload, list):
                    payload = {"users": payload, "total": len(payload)}
                else:
                    msg = (
                        f"Unexpected payload of type {type(payload).__name__} from "
                        "GET /api/v1/users/"
                    )
                    raise OpenWebUIError(msg)
            users = payload.get("users") or []
            for record in users:
                if not isinstance(record, dict):
                    continue
                if self._normalize(str(record.get("email", ""))) == wanted:
                    return record
            if not users:
                return None
            # ``total`` counts the whole match, not the page, so it is the only way to
            # know whether another page exists; an empty page above ends the walk
            # regardless, so a wrong count cannot spin this loop.
            if page * len(users) >= int(payload.get("total") or 0):
                return None
            page += 1
        return None

    def create_user(self, email: str, name: str, password: str, role: str = ROLE_ACTIVE) -> dict:
        """Create one account and return its record.

        ``POST /api/v1/auths/add`` is not idempotent — it answers 400 ``EMAIL_TAKEN``
        for an address that already exists — so callers look the account up first. That
        is a lookup-then-create race in principle, but the caller treats a taken email
        as "already provisioned", which is the same end state.
        """
        payload = self._request(
            "POST",
            "/api/v1/auths/add",
            json_body={
                "name": name,
                "email": self._normalize(email),
                "password": password,
                "role": role,
            },
        )
        if not isinstance(payload, dict):
            msg = f"Open WebUI /api/v1/auths/add returned no account for {email}"
            raise OpenWebUIError(msg)
        return payload

    def set_role(self, account: dict, role: str) -> None:
        """Change one account's role, addressed by Open WebUI's own id.

        The whole account is sent, not just the role, because ``/update`` is a
        *replace* and not a patch: on every released Open WebUI its ``UserUpdateForm``
        declares ``role``, ``name``, ``email`` and ``profile_image_url`` as required
        (they became optional only after v0.6.34, on ``main``), and the handler writes
        all four unconditionally. A body carrying just the role is a 422 there, and on
        a build that accepts it the other three would be overwritten with nulls.

        So the values are echoed back from the record the caller already looked up,
        which is also what keeps a name or an address the person changed themselves
        from being clobbered.
        """
        self._request(
            "POST",
            f"/api/v1/users/{account['id']}/update",
            json_body={
                "role": role,
                "name": str(account.get("name") or ""),
                "email": self._normalize(str(account.get("email") or "")),
                "profile_image_url": str(account.get("profile_image_url") or "/user.png"),
            },
        )

    def delete_user(self, user_id: str) -> bool:
        """Delete one account. Returns False when it is already gone.

        Note that ``main`` answers ``200 true`` for an id it has never held rather than
        404, so the 404 branch is defensive: it covers builds that do signal a missing
        account, and either way the caller treats "already absent" as removed.
        """
        return self._request("DELETE", f"/api/v1/users/{user_id}", none_on_404=True) is not None
