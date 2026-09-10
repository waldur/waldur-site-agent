"""Range queries against a Prometheus-compatible time series database.

Deliberately not a Prometheus client library: one endpoint is needed,
``/api/v1/query_range``, and the core carries httpx already. Anything speaking
that API works — Prometheus, VictoriaMetrics, Mimir, Thanos — because the reply
shape is what this reads, not the product behind it.
"""

from typing import Dict, List, Optional

import httpx


class PromQLError(Exception):
    """A range query could not be answered."""


class PromQLClient:
    """A single range query against a Prometheus-compatible HTTP API."""

    def __init__(
        self,
        url: str,
        timeout: int = 30,
        verify_ssl: bool = True,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
    ) -> None:
        """Build a client for the database at ``url``.

        Args:
            url: base URL, with or without a trailing slash.
            timeout: seconds to wait for a reply.
            verify_ssl: whether to verify the server certificate.
            username: for basic auth, alongside ``password``.
            password: for basic auth, alongside ``username``.
            token: bearer token, used instead of basic auth when both are given.
        """
        self.url = url.rstrip("/")
        headers = {"Authorization": f"Bearer {token}"} if token else None
        auth = (username, password) if username and password and not token else None
        self.session = httpx.Client(
            timeout=timeout,
            transport=httpx.HTTPTransport(verify=verify_ssl, retries=3),
            headers=headers,
            auth=auth,
        )

    def query_range(
        self, query: str, start: int, end: int, step: str
    ) -> List[Dict]:
        """Evaluate ``query`` across a window.

        Returns:
            The API's ``data.result`` — one entry per series, each carrying
            ``metric`` labels and ``values`` as ``[unix seconds, "value"]`` pairs.

        Raises:
            PromQLError: on a transport failure, a non-200 reply, or a body whose
                ``status`` is not ``success``. A partial answer is not returned as
                if it were whole: usage read from half a database is a wrong
                invoice rather than a small one.
        """
        try:
            response = self.session.get(
                f"{self.url}/api/v1/query_range",
                params={"query": query, "start": start, "end": end, "step": step},
            )
        except httpx.HTTPError as e:
            raise PromQLError(f"Range query failed: {e}") from e

        if response.status_code != httpx.codes.OK:
            raise PromQLError(
                f"Range query returned {response.status_code}: {response.text[:200]}"
            )

        payload = response.json()
        if payload.get("status") != "success":
            raise PromQLError(
                "Range query was not successful: "
                f"{payload.get('error') or payload.get('status')}"
            )

        return payload.get("data", {}).get("result", [])

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()
