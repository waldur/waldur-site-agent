"""Pagination utilities for Waldur API calls."""

import logging
import sys
from typing import Callable, TypeVar

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

from waldur_api_client.types import Response

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


def get_all_paginated(
    api_function: Callable[P, Response[list[T]]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> list[T]:
    """Get all items from a paginated API endpoint.

    This function handles pagination automatically by making multiple API calls
    until all items are retrieved. It uses the Link header from responses to
    determine when to stop pagination.

    Args:
        api_function: The API function to call (e.g., marketplace_resources_list.sync_detailed)
        *args: Additional arguments to pass to the API function
        **kwargs: Additional arguments to pass to the API function

    Returns:
        List of all items from all pages

    Example:
        from waldur_api_client.api.marketplace_resources import marketplace_resources_list
        from waldur_site_agent.common.pagination import get_all_paginated

        resources = get_all_paginated(
            marketplace_resources_list.sync_detailed,
            client,
            offering="uuid-here",
        )
    """
    all_items: list[T] = []
    page = 1
    kwargs.setdefault("page_size", 100)

    while True:
        try:
            kwargs["page"] = page
            response = api_function(*args, **kwargs)
            if not response.parsed:
                break

            all_items.extend(response.parsed)

            # Check Link header for next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            page += 1

        except Exception:
            logger.exception("Failed to fetch page %d", page)
            break

    logger.debug("Retrieved %d items across %d page(s)", len(all_items), page - 1)
    return all_items
