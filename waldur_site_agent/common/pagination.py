"""Pagination utilities for Waldur API calls."""

import logging
from typing import Callable

from waldur_api_client import AuthenticatedClient

logger = logging.getLogger(__name__)


def get_all_paginated(
    api_function: Callable[..., list],
    client: AuthenticatedClient,
    page_size: int = 100,
    **kwargs,  # noqa: ANN003
) -> list:
    """Get all items from a paginated API endpoint.

    This function handles pagination automatically by making multiple API calls
    until all items are retrieved.

    Args:
        api_function: The API function to call (e.g., marketplace_resources_list.sync)
        client: Authenticated API client
        page_size: Number of items per page (default: 100)
        **kwargs: Additional arguments to pass to the API function

    Returns:
        List of all items from all pages

    Example:
        from waldur_api_client.api.marketplace_resources import marketplace_resources_list
        from waldur_site_agent.common.pagination import get_all_paginated

        resources = get_all_paginated(
            marketplace_resources_list.sync,
            client,
            offering="uuid-here",
            page_size=50
        )
    """
    all_items = []
    page = 1

    while True:
        try:
            response = api_function(client=client, page=page, page_size=page_size, **kwargs)

            if not response:
                break

            all_items.extend(response)

            # If we got less than page_size, we've reached the end
            if len(response) < page_size:
                break

            page += 1

        except Exception:
            logger.exception("Failed to fetch page %d", page)
            break

    logger.debug("Retrieved %d items across %d page(s)", len(all_items), page - 1)
    return all_items
