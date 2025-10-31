"""Tests for pagination utility functions."""

from unittest.mock import Mock

from waldur_api_client.types import Response

from waldur_site_agent.common.pagination import get_all_paginated


class TestPagination:
    """Test cases for pagination utilities."""

    def test_get_all_paginated_single_page(self):
        """Test pagination with single page response."""
        # Mock API function
        mock_api_function = Mock()
        mock_items = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_response = Mock(spec=Response)
        mock_response.parsed = mock_items
        mock_response.headers = {"Link": ""}
        mock_api_function.return_value = mock_response

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client, test_param="test_value")

        # Should return all items
        assert result == mock_items

        # Should call API function once with correct parameters
        mock_api_function.assert_called_once_with(
            mock_client, page=1, page_size=100, test_param="test_value"
        )

    def test_get_all_paginated_multiple_pages(self):
        """Test pagination with multiple pages using Link header."""
        # Mock API function to simulate pagination
        mock_api_function = Mock()

        # First page: full page with next link
        first_page = [{"id": i} for i in range(100)]
        first_response = Mock(spec=Response)
        first_response.parsed = first_page
        first_response.headers = {"Link": '<http://example.com/api/resources/?page=2>; rel="next"'}

        # Second page: partial page without next link
        second_page = [{"id": i} for i in range(100, 150)]
        second_response = Mock(spec=Response)
        second_response.parsed = second_page
        second_response.headers = {"Link": ""}

        mock_api_function.side_effect = [first_response, second_response]

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
            "page": 1,
            "page_size": 100,
            "offering": "test-uuid",
        }
        assert calls[1][1] == {
            "page": 2,
            "page_size": 100,
            "offering": "test-uuid",
        }

    def test_get_all_paginated_empty_response(self):
        """Test pagination with empty response."""
        # Mock API function
        mock_api_function = Mock()
        mock_response = Mock(spec=Response)
        mock_response.parsed = []
        mock_response.headers = {"Link": ""}
        mock_api_function.return_value = mock_response

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client)

        # Should return empty list
        assert result == []

        # Should call API function once
        mock_api_function.assert_called_once()

    def test_get_all_paginated_none_response(self):
        """Test pagination with None parsed response."""
        # Mock API function
        mock_api_function = Mock()
        mock_response = Mock(spec=Response)
        mock_response.parsed = None
        mock_response.headers = {"Link": ""}
        mock_api_function.return_value = mock_response

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
        mock_response = Mock(spec=Response)
        mock_response.parsed = mock_items
        mock_response.headers = {"Link": ""}
        mock_api_function.return_value = mock_response

        # Mock client
        mock_client = Mock()

        # Test the function with custom page size
        result = get_all_paginated(mock_api_function, mock_client, page_size=50)

        # Should return all items
        assert result == mock_items

        # Should call with custom page size
        mock_api_function.assert_called_once_with(mock_client, page=1, page_size=50)

    def test_get_all_paginated_full_page_exactly(self):
        """Test pagination when response exactly matches page size with Link header."""
        # Mock API function
        mock_api_function = Mock()

        # First page: exactly 50 items with next link
        first_page = [{"id": i} for i in range(50)]
        first_response = Mock(spec=Response)
        first_response.parsed = first_page
        first_response.headers = {"Link": '<http://example.com/api/resources/?page=2>; rel="next"'}

        # Second page: empty (no more items)
        second_response = Mock(spec=Response)
        second_response.parsed = []
        second_response.headers = {"Link": ""}

        mock_api_function.side_effect = [first_response, second_response]

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client, page_size=50)

        # Should return all 50 items
        assert len(result) == 50
        assert result == first_page

        # Should make 2 API calls (second call gets empty response)
        assert mock_api_function.call_count == 2

    def test_get_all_paginated_link_header_pagination(self):
        """Test that pagination stops when Link header has no next."""
        # Mock API function
        mock_api_function = Mock()

        # Page with 100 items but no next link (last page)
        page_items = [{"id": i} for i in range(100)]
        response = Mock(spec=Response)
        response.parsed = page_items
        response.headers = {"Link": '<http://example.com/api/resources/?page=1>; rel="first"'}
        mock_api_function.return_value = response

        # Mock client
        mock_client = Mock()

        # Test the function
        result = get_all_paginated(mock_api_function, mock_client, page_size=100)

        # Should return all items
        assert len(result) == 100
        assert result == page_items

        # Should make only 1 API call (no next link means stop)
        mock_api_function.assert_called_once()

    def test_get_all_paginated_default_page_size(self):
        """Test that default page size is 100."""
        # Mock API function
        mock_api_function = Mock()
        mock_response = Mock(spec=Response)
        mock_response.parsed = []
        mock_response.headers = {"Link": ""}
        mock_api_function.return_value = mock_response

        # Mock client
        mock_client = Mock()

        # Test the function without specifying page_size
        get_all_paginated(mock_api_function, mock_client)

        # Should call with default page_size of 100
        mock_api_function.assert_called_once_with(mock_client, page=1, page_size=100)
