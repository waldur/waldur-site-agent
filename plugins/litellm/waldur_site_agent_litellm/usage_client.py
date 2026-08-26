"""HTTP client for LiteLLM's spend/usage API.

Reads ``GET /user/daily/activity``, LiteLLM's pre-aggregated daily spend table.

The obvious endpoint for this is ``/global/spend/report?group_by=api_key``, but it —
and ``/key/spend/report`` — are enterprise-gated, so neither works on an open-source
proxy. ``/user/daily/activity`` is not gated, is already aggregated per day (so this
never walks per-request rows), and carries each key's ``key_alias`` alongside its
hash, which removes the need for a separate hash-to-alias map.
"""

from __future__ import annotations

import logging
from typing import Optional, Union

import httpx

from waldur_site_agent.backend.exceptions import BackendError

from .client import timeout_setting, verify_ssl_setting

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0
DAILY_PAGE_SIZE = 100
# A defensive stop so a paging bug cannot spin forever against a live proxy.
MAX_PAGES = 1000


class LiteLLMUsageBackendError(BackendError):
    """Error raised for LiteLLM spend API failures."""


class LiteLLMUsageClient:
    """Reads per-key token usage and spend from a LiteLLM proxy."""

    def __init__(self, backend_settings: dict) -> None:
        """Initialize the client from the offering's ``backend_settings``."""
        api_url = backend_settings.get("api_url")
        if not api_url:
            msg = "LiteLLM usage backend requires 'api_url' in backend_settings"
            raise LiteLLMUsageBackendError(msg)
        api_token = backend_settings.get("api_token")
        if not api_token:
            msg = "LiteLLM usage backend requires 'api_token' in backend_settings"
            raise LiteLLMUsageBackendError(msg)

        self.api_url = str(api_url).rstrip("/")
        self.api_token = str(api_token)
        self.verify_ssl = verify_ssl_setting(backend_settings)
        self.timeout = timeout_setting(backend_settings)
        # One pooled client for the instance: a month of usage is walked a page at a
        # time, and a fresh client per page would pay a TCP and TLS handshake for each.
        self.session = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers=self._headers(),
        )

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_token}"}

    def close(self) -> None:
        """Release the pooled connections."""
        self.session.close()

    def ping(self) -> bool:
        """Return True when the proxy is healthy and its spend database is live.

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
            logger.exception("LiteLLM usage ping failed")
            return False
        return isinstance(payload, dict) and payload.get("db") == "connected"

    def _fetch_page(self, start_date: str, end_date: str, page: int) -> dict:
        params: dict[str, Union[str, int]] = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "page_size": DAILY_PAGE_SIZE,
            # The proxy can otherwise omit the day still in progress, which would
            # report a month's usage as zero for every cycle until the day rolls over.
            "include_current_utc_day": "true",
        }
        try:
            response = self.session.get(f"{self.api_url}/user/daily/activity", params=params)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPStatusError as exc:
            msg = (
                "LiteLLM /user/daily/activity failed: "
                f"{exc.response.status_code} {exc.response.text}"
            )
            raise LiteLLMUsageBackendError(msg) from exc
        except httpx.HTTPError as exc:
            msg = f"LiteLLM /user/daily/activity request error: {exc}"
            raise LiteLLMUsageBackendError(msg) from exc
        except ValueError as exc:
            msg = f"LiteLLM /user/daily/activity returned invalid JSON: {exc}"
            raise LiteLLMUsageBackendError(msg) from exc
        return payload if isinstance(payload, dict) else {}

    def get_usage_rows(self, start_date: str, end_date: str) -> list:
        """Return one usage row per (day, key) in the inclusive ``YYYY-MM-DD`` range.

        Each row is ``{"key_alias", "input_tokens", "output_tokens", "token_cost"}``.
        Rows are *not* aggregated here — a key appears once per day it was used, and
        the caller sums them.

        Only ``breakdown.api_keys`` is read. The same numbers also appear under
        ``breakdown.models`` and ``breakdown.model_groups``, split by model and by
        model group respectively; walking more than one of those breakdowns would
        count the same spend two or three times.
        """
        rows: list = []
        page = 1
        while page <= MAX_PAGES:
            payload = self._fetch_page(start_date, end_date, page)
            results = payload.get("results") or []
            for result in results:
                if not isinstance(result, dict):
                    continue
                breakdown = result.get("breakdown") or {}
                api_keys = breakdown.get("api_keys") or {}
                for record in api_keys.values():
                    row = self._to_row(record)
                    if row is not None:
                        rows.append(row)
            # An empty page ends the walk whatever the metadata claims. The metadata
            # alone is not a safe stop: a proxy that leaves ``has_more`` set, or reports
            # it without a ``total_pages``, never satisfies the condition below, and the
            # loop would spend all MAX_PAGES requests inside one reporting pass. A page
            # that carried nothing has nothing after it either.
            if not results:
                break
            metadata = payload.get("metadata") or {}
            total_pages = metadata.get("total_pages") or 0
            if not metadata.get("has_more") and page >= total_pages:
                break
            page += 1
        return rows

    @staticmethod
    def _to_row(record: object) -> Optional[dict]:
        """Map one ``api_keys`` breakdown entry onto a usage row.

        Keyed by alias rather than by the hash the breakdown is indexed on: a rotation
        that goes through delete-and-mint changes the hash while keeping the alias, so
        attributing by hash would split one slot's month across two identities.
        """
        if not isinstance(record, dict):
            return None
        alias = (record.get("metadata") or {}).get("key_alias")
        if not alias:
            # A key deleted before the sweep, or one minted outside the agent, has no
            # alias to attribute usage to. Counting it against nothing is correct.
            return None
        metrics = record.get("metrics") or {}
        return {
            "key_alias": str(alias),
            "input_tokens": int(metrics.get("prompt_tokens") or 0),
            "output_tokens": int(metrics.get("completion_tokens") or 0),
            "token_cost": float(metrics.get("spend") or 0.0),
        }
