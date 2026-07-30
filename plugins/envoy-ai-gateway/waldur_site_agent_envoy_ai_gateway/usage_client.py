"""HTTP client for a usage warehouse."""

from __future__ import annotations

import logging
from typing import Optional

import httpx

from waldur_site_agent.backend.exceptions import BackendError

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


class EnvoyUsageBackendError(BackendError):
    """Error raised for usage warehouse API failures."""


class EnvoyUsageClient:
    """Reads per-client_id token usage from the usage warehouse."""

    def __init__(
        self, api_url: str, api_token: Optional[str] = None, timeout: float = DEFAULT_TIMEOUT
    ) -> None:
        """Initialize the client.

        Args:
            api_url: usage warehouse base URL (e.g. http://usage-warehouse:9000).
            api_token: Optional bearer token.
            timeout: Per-request timeout in seconds.
        """
        self.api_url = api_url.rstrip("/")
        self.api_token = api_token
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"} if self.api_token else {}

    def ping(self) -> bool:
        """Return True if the warehouse health endpoint responds 200."""
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(f"{self.api_url}/health", headers=self._headers())
                return response.status_code == httpx.codes.OK
        except httpx.HTTPError:
            logger.exception("usage warehouse ping failed")
            return False

    def get_usage(
        self, client_ids: list[str], from_month: str, to_month: str
    ) -> list[dict]:
        """Return per-client_id usage rows for the [from_month, to_month] range.

        Months are ``YYYY-MM``. Each row is ``{client_id, input_tokens, output_tokens, ...}``.
        """
        params: list[tuple[str, str]] = [("from", from_month), ("to", to_month)]
        params.extend(("client_id", client_id) for client_id in client_ids)
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(
                    f"{self.api_url}/usage-month", params=params, headers=self._headers()
                )
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as exc:
            msg = (
                "usage warehouse /usage-month failed: "
                f"{exc.response.status_code} {exc.response.text}"
            )
            raise EnvoyUsageBackendError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"usage warehouse /usage-month request error: {exc}"
            raise EnvoyUsageBackendError(msg) from exc
        except ValueError as exc:
            # response.json() raises ValueError (json.JSONDecodeError) on a non-JSON 200.
            msg = f"usage warehouse /usage-month returned invalid JSON: {exc}"
            raise EnvoyUsageBackendError(msg) from exc
        return payload.get("usage", []) if isinstance(payload, dict) else []
