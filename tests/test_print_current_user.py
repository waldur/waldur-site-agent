"""Regression tests for print_current_user against the /api/users/me schema."""

from waldur_api_client.models.me_permission import MePermission
from waldur_api_client.models.user_me import UserMe

from waldur_site_agent.common.utils import print_current_user


class TestPrintCurrentUser:
    """print_current_user must only touch fields present in the trimmed
    MePermission projection returned by /api/users/me (WAL-8015)."""

    def test_non_staff_user_with_permissions(self):
        """Logging permissions of a non-staff user must not raise."""
        user = UserMe(
            username="site-agent",
            full_name="Site Agent",
            is_staff=False,
            permissions=[
                MePermission(
                    role_name="CUSTOMER.OWNER",
                    scope_type="customer",
                    scope_name="Test customer",
                )
            ],
        )

        print_current_user(user)

    def test_staff_user(self):
        """Staff users short-circuit before permission logging."""
        user = UserMe(username="admin", full_name="Admin", is_staff=True)

        print_current_user(user)

    def test_user_without_permissions(self):
        """A user with no permissions logs the fallback message."""
        user = UserMe(username="plain", full_name="Plain", is_staff=False, permissions=[])

        print_current_user(user)
