import unittest

from waldur_api_client.models.offering_user_state import OfferingUserState
from waldur_api_client.models.project_user import ProjectUser


class TestProjectUserNoneState(unittest.TestCase):
    def test_project_user_with_none_offering_state(self):
        """Test that ProjectUser.from_dict correctly handles None offering_user_state."""
        # This simulates the data returned from the API where offering_user_state can be None
        api_response_data = {
            "url": "https://api.example.com/user/123",
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "username": "test_user",
            "full_name": "Test User",
            "role": "admin",
            "expiration_time": None,
            "offering_user_username": "test_offering_user",
            "offering_user_state": None,  # This is a valid None value
            "email": "test@example.com",
        }

        # This should work without errors since offering_user_state is Optional
        project_user = ProjectUser.from_dict(api_response_data)

        # Verify that the None value is preserved
        self.assertIsNone(project_user.offering_user_state)
        self.assertEqual(project_user.username, "test_user")
        self.assertEqual(project_user.full_name, "Test User")

    def test_project_user_with_valid_offering_state(self):
        """Test that ProjectUser.from_dict works correctly with a valid offering_user_state."""
        api_response_data = {
            "url": "https://api.example.com/user/123",
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "username": "test_user",
            "full_name": "Test User",
            "role": "admin",
            "expiration_time": None,
            "offering_user_username": "test_offering_user",
            "offering_user_state": "OK",  # Valid state
            "email": "test@example.com",
        }

        # This should work without errors
        project_user = ProjectUser.from_dict(api_response_data)
        self.assertEqual(project_user.offering_user_state, OfferingUserState.OK)


if __name__ == "__main__":
    unittest.main()
