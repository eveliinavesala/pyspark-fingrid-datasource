"""
Tests for the main data reader orchestration (read_fingrid_data).

HTTP/pagination/retry behavior is tested in test_client.py; option parsing
and validation is tested in test_options.py. This module tests only the
orchestration read_fingrid_data() does on top of those: wiring a
FingridApiClient, transforming records, and building a DataFrame.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from pyspark_fingrid.exceptions import FingridApiError, FingridRateLimitError
from pyspark_fingrid.reader import list_available_datasets, read_fingrid_data


class TestReadFingridData:
    """Test the main read_fingrid_data function."""

    def test_invalid_inputs(self):
        """Test validation of invalid inputs."""
        with pytest.raises(ValueError, match="api_key is required"):
            read_fingrid_data("", 192)

        with pytest.raises(ValueError, match="dataset_id must be an integer"):
            read_fingrid_data("test-key", "192")

        with pytest.raises(ValueError, match="Dataset 999 not supported"):
            read_fingrid_data("test-key", 999)

    @patch("pyspark_fingrid.reader.FingridApiClient")
    @patch("pyspark.sql.SparkSession.getActiveSession")
    def test_successful_data_read(self, mock_spark_session, mock_client_cls):
        """Test successful data reading and DataFrame creation."""
        mock_spark = Mock()
        mock_spark_session.return_value = mock_spark
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df

        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.return_value = {"nameEn": "Test Dataset", "unitEn": "MW", "updateCadenceEn": "3 min"}
        mock_client.fetch_data.return_value = [
            {
                "datasetId": 192,
                "startTime": "2024-07-24T12:00:00.000Z",
                "endTime": "2024-07-24T12:03:00.000Z",
                "value": 6789.5,
            }
        ]

        result = read_fingrid_data("test-key", 192)

        mock_client_cls.assert_called_once_with("test-key")
        mock_client.fetch_metadata.assert_called_once_with(192)
        mock_client.fetch_data.assert_called_once()
        mock_spark.createDataFrame.assert_called_once()
        assert result == mock_df

    @patch("pyspark_fingrid.reader.FingridApiClient")
    def test_no_data_returned(self, mock_client_cls):
        """Test handling when the API has no data for the range."""
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.return_value = None
        mock_client.fetch_data.return_value = []

        result = read_fingrid_data("test-key", 192)
        assert result is None

    @patch("pyspark_fingrid.reader.FingridApiClient")
    def test_api_error_returns_none(self, mock_client_cls):
        """Test that a failed fetch (e.g. exhausted retries) is reported and returns None
        rather than propagating, matching this function's existing None-on-failure contract."""
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.return_value = None
        mock_client.fetch_data.side_effect = FingridApiError("boom")

        result = read_fingrid_data("test-key", 192)
        assert result is None

    @patch("pyspark_fingrid.reader.FingridApiClient")
    def test_metadata_rate_limit_does_not_abort_the_read(self, mock_client_cls):
        """Metadata is best-effort: even if it's rate limited, data fetching should proceed."""
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.side_effect = FingridRateLimitError("rate limited")
        mock_client.fetch_data.return_value = []  # short-circuit before DataFrame creation

        result = read_fingrid_data("test-key", 192)

        mock_client.fetch_data.assert_called_once()
        assert result is None

    @patch("pyspark_fingrid.reader.FingridApiClient")
    def test_metadata_generic_api_error_does_not_abort_the_read(self, mock_client_cls):
        """Metadata is best-effort even on a non-rate-limit failure (e.g. a network
        error), not just on FingridRateLimitError specifically."""
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.side_effect = FingridApiError("network error")
        mock_client.fetch_data.return_value = []  # short-circuit before DataFrame creation

        result = read_fingrid_data("test-key", 192)

        mock_client.fetch_data.assert_called_once()
        assert result is None

    @patch("pyspark_fingrid.reader.FingridApiClient")
    @patch("pyspark.sql.SparkSession.getActiveSession")
    def test_no_spark_session(self, mock_spark_session, mock_client_cls):
        """Test handling when no Spark session is available."""
        mock_spark_session.return_value = None
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.return_value = None
        mock_client.fetch_data.return_value = [{"datasetId": 192, "startTime": None, "endTime": None, "value": 1.0}]

        result = read_fingrid_data("test-key", 192)
        assert result is None

    @patch("pyspark_fingrid.reader.FingridApiClient")
    def test_default_time_range_uses_utc(self, mock_client_cls):
        """Test that the default (no start/end given) time window is computed in UTC.

        Regression test: previously this used naive datetime.now() (local time)
        while formatting it with a trailing 'Z' (UTC marker), which silently
        produced the wrong window on any machine not running in UTC.
        """
        mock_client = mock_client_cls.return_value
        mock_client.fetch_metadata.return_value = None
        mock_client.fetch_data.return_value = []  # short-circuit before DataFrame creation

        before = datetime.now(timezone.utc)
        read_fingrid_data("test-key", 192)
        after = datetime.now(timezone.utc)

        assert mock_client.fetch_data.called
        _, called_start, called_end = mock_client.fetch_data.call_args[0][:3]

        called_end_dt = datetime.strptime(called_end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        called_start_dt = datetime.strptime(called_start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # The "now" used to build the window must fall within [before, after] in UTC.
        assert before - timedelta(seconds=1) <= called_end_dt <= after + timedelta(seconds=1)
        assert (called_end_dt - called_start_dt) == timedelta(minutes=30)


class TestListAvailableDatasets:
    """Test list_available_datasets convenience function."""

    @patch("pyspark_fingrid.schemas.registry.FingridSchemaRegistry.list_available_datasets")
    def test_list_available_datasets(self, mock_list):
        list_available_datasets()
        mock_list.assert_called_once()
