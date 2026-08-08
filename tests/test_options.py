"""Tests for pyspark_fingrid.options.FingridReadOptions."""

from datetime import datetime, timedelta, timezone

import pytest

from pyspark_fingrid.exceptions import FingridConfigError
from pyspark_fingrid.options import FingridReadOptions


class TestFromDict:
    """Option parsing for the format("fingrid") DataSource path."""

    def test_valid_options(self):
        opts = FingridReadOptions.from_dict(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-25T00:00:00Z",
                "partitionDays": "2",
                "pageSize": "500",
            }
        )
        assert opts.api_key == "test-key"
        assert opts.dataset_id == 192
        assert opts.partition_days == 2
        assert opts.page_size == 500
        assert opts.start_time_str == "2024-07-24T00:00:00Z"
        assert opts.end_time_str == "2024-07-25T00:00:00Z"

    def test_missing_api_key(self):
        with pytest.raises(FingridConfigError, match="apiKey"):
            FingridReadOptions.from_dict({"datasetId": "192"})

    def test_missing_dataset_id(self):
        with pytest.raises(FingridConfigError, match="datasetId"):
            FingridReadOptions.from_dict({"apiKey": "test-key"})

    def test_non_integer_dataset_id(self):
        with pytest.raises(FingridConfigError, match="must be an integer"):
            FingridReadOptions.from_dict({"apiKey": "test-key", "datasetId": "not-a-number"})

    def test_invalid_partition_days(self):
        with pytest.raises(FingridConfigError, match="partitionDays"):
            FingridReadOptions.from_dict({"apiKey": "test-key", "datasetId": "192", "partitionDays": "0"})

    def test_end_before_start(self):
        with pytest.raises(FingridConfigError, match="endTime.*after.*startTime"):
            FingridReadOptions.from_dict(
                {
                    "apiKey": "test-key",
                    "datasetId": "192",
                    "startTime": "2024-07-25T00:00:00Z",
                    "endTime": "2024-07-24T00:00:00Z",
                }
            )

    def test_defaults_to_last_30_minutes_in_utc(self):
        opts = FingridReadOptions.from_dict({"apiKey": "test-key", "datasetId": "192"})
        assert opts.start_time.tzinfo is not None
        assert opts.end_time.tzinfo is not None
        assert (opts.end_time - opts.start_time) == pytest.approx(timedelta(minutes=30), abs=timedelta(seconds=2))

    def test_config_error_is_a_value_error(self):
        """Backward compatibility: existing `except ValueError` call sites must keep working."""
        with pytest.raises(ValueError):
            FingridReadOptions.from_dict({})


class TestFromArgs:
    """Option parsing for the legacy read_fingrid_data() path."""

    def test_valid_args(self):
        opts = FingridReadOptions.from_args("test-key", 192, "2024-07-24T00:00:00Z", "2024-07-25T00:00:00Z")
        assert opts.api_key == "test-key"
        assert opts.dataset_id == 192
        assert opts.start_time_str == "2024-07-24T00:00:00Z"
        assert opts.end_time_str == "2024-07-25T00:00:00Z"

    def test_missing_api_key(self):
        with pytest.raises(FingridConfigError, match="api_key is required"):
            FingridReadOptions.from_args("", 192)

    def test_dataset_id_must_be_a_real_int_not_a_string(self):
        """Unlike from_dict (Spark options are always strings so casting is
        expected), from_args is a direct Python call and keeps the
        stricter pre-existing check."""
        with pytest.raises(FingridConfigError, match="dataset_id must be an integer"):
            FingridReadOptions.from_args("test-key", "192")

    def test_defaults_to_last_30_minutes_in_utc(self):
        opts = FingridReadOptions.from_args("test-key", 192)
        assert (opts.end_time - opts.start_time) == pytest.approx(timedelta(minutes=30), abs=timedelta(seconds=2))

    def test_uses_current_utc_time_as_end_when_omitted(self):
        before = datetime.now(timezone.utc)
        opts = FingridReadOptions.from_args("test-key", 192)
        after = datetime.now(timezone.utc)
        assert before - timedelta(seconds=1) <= opts.end_time <= after + timedelta(seconds=1)
