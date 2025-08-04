"""Tests for pagination utility functions."""

from unittest.mock import Mock

from waldur_site_agent.common.pagination import get_all_paginated


class TestPagination:
    """Test cases for pagination utilities."""

    def test_get_all_paginated_single_page(self):
        """Test pagination with single page response."""
        # Mock API function
        mock_api_function = Mock()
        mock_items = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_api_function.return_value = mock_items

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client, test_param="test_value")

        # Should return all items
        assert result == mock_items

        # Should call API function once with correct parameters
        mock_api_function.assert_called_once_with(
            client=mock_client, page=1, page_size=100, test_param="test_value"
        )

    def test_get_all_paginated_multiple_pages(self):
        """Test pagination with multiple pages."""
        # Mock API function to simulate pagination
        mock_api_function = Mock()

        # First page: full page (100 items)
        first_page = [{"id": i} for i in range(100)]
        # Second page: partial page (50 items) - triggers end condition
        second_page = [{"id": i} for i in range(100, 150)]

        mock_api_function.side_effect = [first_page, second_page]

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(
            mock_api_function, mock_client, page_size=100, offering="test-uuid"
        )

        # Should return all 150 items
        assert len(result) == 150
        assert result == first_page + second_page

        # Should make 2 API calls
        assert mock_api_function.call_count == 2

        # Verify call parameters
        calls = mock_api_function.call_args_list
        assert calls[0][1] == {
            "client": mock_client,
            "page": 1,
            "page_size": 100,
            "offering": "test-uuid",
        }
        assert calls[1][1] == {
            "client": mock_client,
            "page": 2,
            "page_size": 100,
            "offering": "test-uuid",
        }

    def test_get_all_paginated_empty_response(self):
        """Test pagination with empty response."""
        # Mock API function
        mock_api_function = Mock()
        mock_api_function.return_value = []

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client)

        # Should return empty list
        assert result == []

        # Should call API function once
        mock_api_function.assert_called_once()

    def test_get_all_paginated_none_response(self):
        """Test pagination with None response."""
        # Mock API function
        mock_api_function = Mock()
        mock_api_function.return_value = None

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client)

        # Should return empty list
        assert result == []

        # Should call API function once
        mock_api_function.assert_called_once()

    def test_get_all_paginated_api_error(self):
        """Test pagination with API error."""
        # Mock API function to raise exception
        mock_api_function = Mock()
        mock_api_function.side_effect = Exception("API Error")

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client)

        # Should return empty list on error
        assert result == []

        # Should call API function once before failing
        mock_api_function.assert_called_once()

    def test_get_all_paginated_custom_page_size(self):
        """Test pagination with custom page size."""
        # Mock API function
        mock_api_function = Mock()
        mock_items = [{"id": i} for i in range(25)]
        mock_api_function.return_value = mock_items

        # Mock client
        mock_client = Mock()

        # Test the function with custom page size
        result = get_all_paginated(mock_api_function, mock_client, page_size=50)

        # Should return all items
        assert result == mock_items

        # Should call with custom page size
        mock_api_function.assert_called_once_with(client=mock_client, page=1, page_size=50)

    def test_get_all_paginated_full_page_exactly(self):
        """Test pagination when response exactly matches page size."""
        # Mock API function
        mock_api_function = Mock()

        # First page: exactly 50 items
        first_page = [{"id": i} for i in range(50)]
        # Second page: empty (triggers end condition)
        second_page = []

        mock_api_function.side_effect = [first_page, second_page]

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client, page_size=50)

        # Should return all 50 items
        assert len(result) == 50
        assert result == first_page

        # Should make 2 API calls (second call gets empty response)
        assert mock_api_function.call_count == 2
