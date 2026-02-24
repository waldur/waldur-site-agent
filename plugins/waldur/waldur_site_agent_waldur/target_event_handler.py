"""Handlers for events from Waldur B (target instance).

ORDER handler: when a target order reaches a terminal state (DONE, ERRED,
CANCELED, REJECTED), finds the matching source order on Waldur A and updates
its state.

OFFERING_USER handler: when a target offering user is created or updated with
a username, triggers a full username sync from Waldur B to Waldur A so the
username is propagated instantly without waiting for the next polling cycle.
"""

from __future__ import annotations

import json
import logging
from uuid import UUID

import stomp.utils
from waldur_api_client.api.marketplace_orders import (
    marketplace_orders_list,
    marketplace_orders_set_state_done,
    marketplace_orders_set_state_erred,
)
from waldur_api_client.models.order_error_details_request import OrderErrorDetailsRequest
from waldur_api_client.models.order_state import OrderState
from waldur_api_client.types import UNSET

from waldur_site_agent.common.structures import Offering
from waldur_site_agent.common.utils import get_client

logger = logging.getLogger(__name__)

TERMINAL_STATES = {OrderState.DONE, OrderState.ERRED, OrderState.CANCELED, OrderState.REJECTED}


def make_target_order_handler(
    source_offering: Offering,
):
    """Create a STOMP handler for ORDER events from Waldur B.

    Uses closure to capture source_offering for creating a Waldur A client.
    The returned function has the standard STOMP handler signature:
    ``(frame, offering, user_agent) -> None``

    Args:
        source_offering: The source Waldur offering (Waldur A).

    Returns:
        Handler function for STOMP ORDER messages from Waldur B.
    """

    def handler(
        frame: stomp.utils.Frame,
        target_offering: Offering,
        user_agent: str,
    ) -> None:
        """Process ORDER event from Waldur B and update source order on Waldur A."""
        del target_offering  # Not used — source_offering from closure is used instead
        message = json.loads(frame.body)
        order_uuid = message.get("order_uuid", "")
        order_state = message.get("order_state", "")

        if not order_uuid:
            logger.debug("Received ORDER event without order_uuid, skipping")
            return

        # Only process terminal states
        if order_state not in {s.value for s in TERMINAL_STATES}:
            logger.debug(
                "Target order %s in non-terminal state %s, skipping",
                order_uuid,
                order_state,
            )
            return

        logger.info(
            "Target order %s reached terminal state %s, updating source order",
            order_uuid,
            order_state,
        )

        try:
            source_client = get_client(
                source_offering.api_url,
                source_offering.api_token,
                user_agent,
                verify_ssl=source_offering.verify_ssl,
            )

            # Find the source order whose backend_id matches the target order UUID.
            # The source order processor sets backend_id = target_order_uuid when
            # the async creation starts (see _process_create_order in processors.py).
            executing_orders = marketplace_orders_list.sync_all(
                client=source_client,
                offering_uuid=UUID(source_offering.uuid),
                state=[OrderState.EXECUTING],
            )

            source_order = next(
                (
                    o
                    for o in (executing_orders or [])
                    if not isinstance(o.backend_id, type(UNSET))
                    and o.backend_id == order_uuid
                ),
                None,
            )

            if not source_order:
                logger.debug(
                    "No EXECUTING source order found with backend_id=%s",
                    order_uuid,
                )
                return

            if order_state == OrderState.DONE.value:
                marketplace_orders_set_state_done.sync_detailed(
                    uuid=source_order.uuid,
                    client=source_client,
                )
                logger.info(
                    "Source order %s marked as DONE (target order %s completed)",
                    source_order.uuid,
                    order_uuid,
                )
            else:
                marketplace_orders_set_state_erred.sync_detailed(
                    uuid=source_order.uuid,
                    client=source_client,
                    body=OrderErrorDetailsRequest(
                        error_message=f"Target order {order_uuid} state: {order_state}",
                    ),
                )
                logger.info(
                    "Source order %s marked as ERRED (target order %s state: %s)",
                    source_order.uuid,
                    order_uuid,
                    order_state,
                )
        except Exception:
            logger.exception(
                "Failed to update source order for target order %s",
                order_uuid,
            )

    return handler


def make_target_offering_user_handler(
    source_offering: Offering,
    backend,  # WaldurBackend — not type-hinted to avoid circular import
):
    """Create a STOMP handler for OFFERING_USER events from Waldur B.

    When a Waldur B offering user is created or updated with a username,
    triggers a full sync of offering user usernames from B to A via
    ``backend.sync_offering_user_usernames``.

    Args:
        source_offering: The source Waldur offering (Waldur A).
        backend: The WaldurBackend instance.

    Returns:
        Handler function for STOMP OFFERING_USER messages from Waldur B.
    """

    def handler(
        frame: stomp.utils.Frame,
        target_offering: Offering,
        user_agent: str,
    ) -> None:
        """Process OFFERING_USER event from Waldur B and sync to Waldur A."""
        del target_offering  # Not used — source_offering from closure is used instead
        message = json.loads(frame.body)
        action = message.get("action", "")
        username = message.get("username", "")
        offering_user_uuid = message.get("offering_user_uuid", "")

        if action not in ("create", "update"):
            logger.debug(
                "Offering user %s action %s is not create/update, skipping",
                offering_user_uuid,
                action,
            )
            return

        if not username:
            logger.debug(
                "Offering user %s has no username, skipping",
                offering_user_uuid,
            )
            return

        logger.info(
            "Offering user %s %sd with username %s, triggering username sync",
            offering_user_uuid,
            action,
            username,
        )

        try:
            source_client = get_client(
                source_offering.api_url,
                source_offering.api_token,
                user_agent,
                verify_ssl=source_offering.verify_ssl,
            )

            updated = backend.sync_offering_user_usernames(
                waldur_a_offering_uuid=source_offering.uuid,
                waldur_rest_client=source_client,
            )

            if updated:
                logger.info("Username sync completed: usernames updated on Waldur A")
            else:
                logger.debug("Username sync completed: no changes needed on Waldur A")
        except Exception:
            logger.exception(
                "Failed to sync offering user usernames after event for %s",
                offering_user_uuid,
            )

    return handler
