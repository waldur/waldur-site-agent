"""Username management backend using Waldur Identity Bridge.

Pushes user profiles from Waldur A to Waldur B via the Identity Bridge API
(POST /api/identity-bridge/) to ensure users exist before membership sync.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from waldur_api_client import AuthenticatedClient
from waldur_api_client.models.offering_user import OfferingUser
from waldur_api_client.types import UNSET

from waldur_site_agent.backend.backends import AbstractUsernameManagementBackend

if TYPE_CHECKING:
    from waldur_site_agent.common.structures import Offering

logger = logging.getLogger(__name__)


def _get_waldur_username(offering_user: OfferingUser) -> str | None:
    """Extract Waldur A username (user_username) from offering user."""
    val = offering_user.user_username
    return val if val and not isinstance(val, type(UNSET)) else None


def _extract_attributes(offering_user: OfferingUser) -> dict:
    """Map OfferingUser fields to identity bridge payload."""
    attrs: dict = {}

    # Direct field mappings (OfferingUser field -> identity bridge field)
    field_map = {
        "user_first_name": "first_name",  # Added in Mastermind Stage 0
        "user_last_name": "last_name",  # Added in Mastermind Stage 0
        "user_email": "email",
        "user_organization": "organization",
        "user_affiliations": "affiliations",
        "user_phone_number": "phone_number",
        "user_civil_number": "civil_number",
        "user_personal_title": "personal_title",
        "user_place_of_birth": "place_of_birth",
        "user_country_of_residence": "country_of_residence",
        "user_nationality": "nationality",
        "user_nationalities": "nationalities",
        "user_organization_country": "organization_country",
        "user_organization_type": "organization_type",
        "user_eduperson_assurance": "eduperson_assurance",
        "user_identity_source": "identity_source",
    }
    for ou_field, ib_field in field_map.items():
        val = getattr(offering_user, ou_field, UNSET)
        if val and not isinstance(val, type(UNSET)):
            attrs[ib_field] = val

    # Gender needs special handling (GenderEnum -> int)
    gender = getattr(offering_user, "user_gender", UNSET)
    if gender is not None and not isinstance(gender, type(UNSET)):
        attrs["gender"] = gender.value if hasattr(gender, "value") else gender

    # birth_date needs ISO format
    birth_date = getattr(offering_user, "user_birth_date", UNSET)
    if birth_date is not None and not isinstance(birth_date, type(UNSET)):
        attrs["birth_date"] = (
            birth_date.isoformat() if hasattr(birth_date, "isoformat") else str(birth_date)
        )

    return attrs


class WaldurIdentityBridgeUsernameBackend(AbstractUsernameManagementBackend):
    """Username management backend that syncs users via Waldur Identity Bridge.

    Creates/updates users on Waldur B via POST /api/identity-bridge/
    and generates usernames from the Waldur A username (user_username).
    """

    def __init__(
        self,
        backend_settings: dict | None = None,
        offering: Optional[Offering] = None,
    ) -> None:
        super().__init__(backend_settings, offering)
        target_api_url = (self.backend_settings.get("target_api_url") or "").rstrip("/")
        target_api_token = self.backend_settings.get("target_api_token") or ""
        self.identity_bridge_source = self.backend_settings.get("identity_bridge_source", "")
        self._http_client = AuthenticatedClient(
            base_url=target_api_url,
            token=target_api_token,
        )
        self._previous_offering_usernames: set[str] | None = None
        self._log_attribute_config()

    def _log_attribute_config(self) -> None:
        """Fetch and log offering user attribute config from Waldur A for diagnostics.

        Calls GET /api/marketplace-provider-offerings/{uuid}/user-attribute-config/
        on Waldur A (source) to show which user attributes are exposed. This helps
        admins debug what data will be available for identity bridge sync.
        """
        if not self.offering:
            logger.warning("No offering context — cannot fetch attribute config")
            return
        try:
            waldur_a_client = AuthenticatedClient(
                base_url=self.offering.waldur_api_url.rstrip("/"),
                token=self.offering.waldur_api_token,
            )
            response = waldur_a_client.get_httpx_client().get(
                f"/api/marketplace-provider-offerings/{self.offering.waldur_offering_uuid}"
                f"/user-attribute-config/",
            )
            response.raise_for_status()
            config = response.json()
            exposed_fields = config.get("exposed_fields", [])
            is_default = config.get("is_default", True)
            logger.info(
                "Offering %s attribute config — exposed fields: %s (is_default: %s)",
                self.offering.name,
                exposed_fields,
                is_default,
            )
            # Warn about fields identity bridge needs but aren't exposed
            desired = {"username", "first_name", "last_name", "email"}
            exposed = set(exposed_fields)
            missing = desired - exposed
            if missing:
                logger.warning(
                    "Identity bridge recommended fields NOT exposed: %s. "
                    "These won't be available for user sync.",
                    sorted(missing),
                )
        except Exception:
            logger.exception("Failed to fetch attribute config from Waldur A")

    def get_username(self, offering_user: OfferingUser) -> Optional[str]:
        """Return existing user_username as local username."""
        return _get_waldur_username(offering_user)

    def generate_username(self, offering_user: OfferingUser) -> str:
        """Push user to identity bridge, return user_username as local username."""
        self._push_user_to_identity_bridge(offering_user)
        return offering_user.user_username or ""

    def sync_user_profiles(self, offering_users: list[OfferingUser]) -> None:
        """Batch push all offering user profiles to Waldur B."""
        if not self.identity_bridge_source:
            logger.warning("identity_bridge_source not configured, skipping profile sync")
            return

        current_usernames: set[str] = set()
        for ou in offering_users:
            waldur_username = _get_waldur_username(ou)
            if not waldur_username:
                continue
            current_usernames.add(waldur_username)
            try:
                self._push_user_to_identity_bridge(ou)
            except Exception:
                logger.exception(
                    "Failed to push user %s to identity bridge", waldur_username
                )

        # Deactivate users that disappeared since last sync cycle
        if self._previous_offering_usernames is not None:
            stale = self._previous_offering_usernames - current_usernames
            if stale:
                self.deactivate_users(stale)
        self._previous_offering_usernames = current_usernames

    def deactivate_users(self, usernames: set[str]) -> None:
        """Remove departed users from identity bridge."""
        for username in usernames:
            try:
                self._remove_user_from_identity_bridge(username)
            except Exception:
                logger.exception(
                    "Failed to deactivate user %s via identity bridge", username
                )

    def _push_user_to_identity_bridge(self, offering_user: OfferingUser) -> dict:
        """POST /api/identity-bridge/ on Waldur B."""
        waldur_username = _get_waldur_username(offering_user)
        payload: dict = {
            "username": waldur_username,
            "source": self.identity_bridge_source,
        }
        payload.update(_extract_attributes(offering_user))
        response = self._http_client.get_httpx_client().post(
            "/api/identity-bridge/",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def _remove_user_from_identity_bridge(self, username: str) -> dict:
        """POST /api/identity-bridge/remove/ on Waldur B."""
        payload = {"username": username, "source": self.identity_bridge_source}
        response = self._http_client.get_httpx_client().post(
            "/api/identity-bridge/remove/",
            json=payload,
        )
        response.raise_for_status()
        return response.json()
