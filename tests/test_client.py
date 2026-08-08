"""Tests for pyspark_fingrid.client.FingridApiClient."""

from unittest.mock import Mock, patch

import pytest

from pyspark_fingrid.client import FingridApiClient
from pyspark_fingrid.exceptions import FingridApiError, FingridRateLimitError


@pytest.fixture
def client():
    return FingridApiClient("test-key")


class TestFetchMetadata:
    @patch("requests.get")
    def test_successful_metadata_fetch(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"nameEn": "Test Dataset"}
        mock_get.return_value = mock_response

        result = client.fetch_metadata(192)

        assert result == {"nameEn": "Test Dataset"}
        mock_get.assert_called_once_with(
            "https://data.fingrid.fi/api/datasets/192",
            headers={"x-api-key": "test-key"},
            params={},
            timeout=60,
        )

    @patch("requests.get")
    def test_failed_metadata_fetch_returns_none(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        assert client.fetch_metadata(192) is None

    @patch("requests.get")
    def test_metadata_fetch_network_exception_raises_api_error(self, mock_get, client):
        import requests

        mock_get.side_effect = requests.ConnectionError("boom")

        with pytest.raises(FingridApiError, match="Error fetching metadata"):
            client.fetch_metadata(192)

    @patch("time.sleep")
    @patch("requests.get")
    def test_metadata_rate_limit_exhausted_raises_rate_limit_error(self, mock_get, mock_sleep, client):
        rate_limited = Mock()
        rate_limited.status_code = 429
        rate_limited.headers = {}
        mock_get.return_value = rate_limited

        with pytest.raises(FingridRateLimitError):
            client.fetch_metadata(192)


class TestFetchData:
    @patch("requests.get")
    def test_successful_data_fetch(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"test": "record"}],
            "pagination": {"total": 1},
        }
        mock_get.return_value = mock_response

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        assert result == [{"test": "record"}]
        mock_get.assert_called_once()

    @patch("time.sleep")
    @patch("requests.get")
    def test_rate_limit_retry(self, mock_get, mock_sleep, client):
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {}

        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"data": [{"test": "record"}]}

        mock_get.side_effect = [rate_limited_response, success_response]

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        assert result == [{"test": "record"}]
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(5.0)  # initial_backoff_seconds default

    @patch("time.sleep")
    @patch("requests.get")
    def test_rate_limit_exhausts_retries_raises(self, mock_get, mock_sleep, client):
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {}
        mock_get.return_value = rate_limited_response

        with pytest.raises(FingridRateLimitError):
            client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        assert mock_get.call_count == client.max_retries
        assert mock_sleep.call_count == client.max_retries - 1

    @patch("time.sleep")
    @patch("requests.get")
    def test_rate_limit_respects_retry_after_header(self, mock_get, mock_sleep, client):
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429
        rate_limited_response.headers = {"Retry-After": "3"}

        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"data": [{"test": "record"}]}

        mock_get.side_effect = [rate_limited_response, success_response]

        client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        mock_sleep.assert_called_once_with(3.0)

    @patch("requests.get")
    def test_api_error_raises(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(FingridApiError, match="500"):
            client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

    @patch("requests.get")
    def test_invalid_response_structure_raises(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"invalid": "structure"}
        mock_get.return_value = mock_response

        with pytest.raises(FingridApiError, match="Unexpected response structure"):
            client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

    @patch("requests.get")
    def test_pagination_fetches_all_pages(self, mock_get, client):
        page1 = Mock()
        page1.status_code = 200
        page1.json.return_value = {
            "data": [{"value": 1}, {"value": 2}],
            "pagination": {"total": 5, "currentPage": 1, "lastPage": 3},
        }
        page2 = Mock()
        page2.status_code = 200
        page2.json.return_value = {
            "data": [{"value": 3}, {"value": 4}],
            "pagination": {"total": 5, "currentPage": 2, "lastPage": 3},
        }
        page3 = Mock()
        page3.status_code = 200
        page3.json.return_value = {
            "data": [{"value": 5}],
            "pagination": {"total": 5, "currentPage": 3, "lastPage": 3},
        }
        mock_get.side_effect = [page1, page2, page3]

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z", page_size=2)

        assert result == [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}, {"value": 5}]
        assert mock_get.call_count == 3
        called_pages = [call.kwargs["params"]["page"] for call in mock_get.call_args_list]
        assert called_pages == [1, 2, 3]

    @patch("requests.get")
    def test_pagination_stops_without_pagination_metadata(self, mock_get, client):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"data": [{"value": 1}]}  # no 'pagination' key at all
        mock_get.return_value = response

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z", page_size=2)

        assert result == [{"value": 1}]
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_pagination_respects_max_pages_safety_cap(self, mock_get, client):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [{"value": 1}, {"value": 2}],
            "pagination": {"total": 1000000},
        }
        mock_get.return_value = response

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z", page_size=2, max_pages=3)

        assert len(result) == 6  # 3 pages * 2 records
        assert mock_get.call_count == 3

    @patch("requests.get")
    def test_no_data_returns_empty_list(self, mock_get, client):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"data": [], "pagination": {"total": 0}}
        mock_get.return_value = response

        result = client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")
        assert result == []

    @patch("requests.get")
    def test_calls_throttle_before_each_page(self, mock_get, client):
        """Every outbound request, including pagination pages, must be rate limited."""
        page1 = Mock()
        page1.status_code = 200
        page1.json.return_value = {"data": [{"value": 1}], "pagination": {"total": 2, "lastPage": 2}}
        page2 = Mock()
        page2.status_code = 200
        page2.json.return_value = {"data": [{"value": 2}], "pagination": {"total": 2, "lastPage": 2}}
        mock_get.side_effect = [page1, page2]

        with patch.object(client._rate_limiter, "wait") as mock_wait:
            client.fetch_data(192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z", page_size=1)

        assert mock_wait.call_count == 2  # once per page
