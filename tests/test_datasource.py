"""
Tests for the FingridDataSource / FingridDataSourceReader
(Python Data Source API v2 implementation).

Option parsing/validation is delegated to FingridReadOptions and is
covered by test_options.py; this module focuses on DataSource-specific
behavior: schema resolution, partitioning, and per-partition reads.
"""

import pickle
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StructType

from pyspark_fingrid.client import FingridApiClient
from pyspark_fingrid.datasource import (
    FingridDataSource,
    FingridDataSourceReader,
    FingridPartition,
    register,
)
from pyspark_fingrid.exceptions import FingridApiError


class TestFingridDataSourceName:
    """Test basic DataSource identity."""

    def test_name(self):
        assert FingridDataSource.name() == "fingrid"


class TestFingridDataSourceSchema:
    """Test schema resolution from options."""

    def test_schema_for_valid_dataset(self):
        ds = FingridDataSource({"apiKey": "test-key", "datasetId": "192"})
        schema = ds.schema()
        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert field_names == ['startTime', 'endTime', 'production_mw', 'datasetId']

    def test_schema_missing_dataset_id(self):
        ds = FingridDataSource({"apiKey": "test-key"})
        with pytest.raises(ValueError, match="datasetId"):
            ds.schema()

    def test_schema_non_integer_dataset_id(self):
        ds = FingridDataSource({"apiKey": "test-key", "datasetId": "not-a-number"})
        with pytest.raises(ValueError, match="must be an integer"):
            ds.schema()

    def test_schema_unregistered_dataset_id(self):
        ds = FingridDataSource({"apiKey": "test-key", "datasetId": "999"})
        with pytest.raises(ValueError, match="Dataset 999 not supported"):
            ds.schema()

    def test_reader_returns_reader_instance(self):
        ds = FingridDataSource({"apiKey": "test-key", "datasetId": "192"})
        schema = ds.schema()
        reader = ds.reader(schema)
        assert isinstance(reader, FingridDataSourceReader)


class TestFingridDataSourceReaderValidation:
    """Test option validation in the reader constructor (delegated to FingridReadOptions)."""

    def _schema(self):
        return FingridDataSource({"apiKey": "test-key", "datasetId": "192"}).schema()

    def test_missing_api_key(self):
        with pytest.raises(ValueError, match="apiKey"):
            FingridDataSourceReader({"datasetId": "192"}, self._schema())

    def test_missing_dataset_id(self):
        with pytest.raises(ValueError, match="datasetId"):
            FingridDataSourceReader({"apiKey": "test-key"}, self._schema())

    def test_unsupported_dataset_id(self):
        with pytest.raises(ValueError, match="Dataset 999 not supported"):
            FingridDataSourceReader({"apiKey": "test-key", "datasetId": "999"}, self._schema())

    def test_invalid_partition_days(self):
        with pytest.raises(ValueError, match="partitionDays"):
            FingridDataSourceReader(
                {"apiKey": "test-key", "datasetId": "192", "partitionDays": "0"},
                self._schema(),
            )

    def test_end_before_start(self):
        with pytest.raises(ValueError, match="endTime.*after.*startTime"):
            FingridDataSourceReader(
                {
                    "apiKey": "test-key",
                    "datasetId": "192",
                    "startTime": "2024-07-25T00:00:00Z",
                    "endTime": "2024-07-24T00:00:00Z",
                },
                self._schema(),
            )

    def test_defaults_to_last_30_minutes_in_utc(self):
        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())
        assert reader.read_options.start_time.tzinfo is not None
        assert reader.read_options.end_time.tzinfo is not None
        delta = reader.read_options.end_time - reader.read_options.start_time
        assert delta.total_seconds() == pytest.approx(1800, abs=2)


class TestFingridDataSourceReaderPartitions:
    """Test that the requested time range is split correctly into partitions."""

    def _schema(self):
        return FingridDataSource({"apiKey": "test-key", "datasetId": "192"}).schema()

    def test_single_day_range_yields_one_partition(self):
        reader = FingridDataSourceReader(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-24T12:00:00Z",
            },
            self._schema(),
        )
        partitions = reader.partitions()
        assert len(partitions) == 1
        assert partitions[0].start_time == "2024-07-24T00:00:00Z"
        assert partitions[0].end_time == "2024-07-24T12:00:00Z"

    def test_multi_day_range_splits_by_partition_days(self):
        reader = FingridDataSourceReader(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-27T00:00:00Z",
                "partitionDays": "1",
            },
            self._schema(),
        )
        partitions = reader.partitions()
        assert len(partitions) == 3
        assert partitions[0].start_time == "2024-07-24T00:00:00Z"
        assert partitions[0].end_time == "2024-07-25T00:00:00Z"
        assert partitions[1].start_time == "2024-07-25T00:00:00Z"
        assert partitions[1].end_time == "2024-07-26T00:00:00Z"
        assert partitions[2].start_time == "2024-07-26T00:00:00Z"
        assert partitions[2].end_time == "2024-07-27T00:00:00Z"

    def test_custom_partition_days(self):
        reader = FingridDataSourceReader(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-28T00:00:00Z",
                "partitionDays": "2",
            },
            self._schema(),
        )
        partitions = reader.partitions()
        assert len(partitions) == 2
        assert partitions[0].start_time == "2024-07-24T00:00:00Z"
        assert partitions[0].end_time == "2024-07-26T00:00:00Z"
        assert partitions[1].start_time == "2024-07-26T00:00:00Z"
        assert partitions[1].end_time == "2024-07-28T00:00:00Z"

    def test_partial_final_partition_is_clamped_to_end_time(self):
        reader = FingridDataSourceReader(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-25T06:00:00Z",
                "partitionDays": "1",
            },
            self._schema(),
        )
        partitions = reader.partitions()
        assert len(partitions) == 2
        assert partitions[1].start_time == "2024-07-25T00:00:00Z"
        assert partitions[1].end_time == "2024-07-25T06:00:00Z"


class TestFingridDataSourceReaderRead:
    """Test that read() fetches (with pagination) and transforms records per partition."""

    def _schema(self):
        return FingridDataSource({"apiKey": "test-key", "datasetId": "192"}).schema()

    @patch("pyspark_fingrid.datasource.FingridApiClient")
    def test_read_transforms_records_into_tuples(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.fetch_data.return_value = [
            {
                'datasetId': 192,
                'startTime': '2024-07-24T00:00:00.000Z',
                'endTime': '2024-07-24T00:03:00.000Z',
                'value': 6789.5,
            }
        ]

        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())
        partition = FingridPartition("2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        rows = list(reader.read(partition))

        assert len(rows) == 1
        row = rows[0]
        # Order must match schema field order: startTime, endTime, production_mw, datasetId
        assert row[2] == 6789.5
        assert row[3] == 192
        assert isinstance(row[0], datetime)

        # A fresh client is constructed per partition (this runs on an
        # executor), using the reader's api_key.
        mock_client_cls.assert_called_once_with("test-key")

        # Verify the partition's own start/end were passed through, not the full range.
        mock_client.fetch_data.assert_called_once_with(
            192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z", page_size=20000
        )

    @patch("pyspark_fingrid.datasource.FingridApiClient")
    def test_read_skips_records_that_fail_transformation(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.fetch_data.return_value = [
            {'datasetId': 192, 'startTime': None, 'endTime': None, 'value': 'not-a-number'},
            {
                'datasetId': 192,
                'startTime': '2024-07-24T00:00:00.000Z',
                'endTime': '2024-07-24T00:03:00.000Z',
                'value': 100.0,
            },
        ]

        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())
        partition = FingridPartition("2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        rows = list(reader.read(partition))

        # Both records "succeed" here because parse_float/parse_timestamp are lenient
        # (return None rather than raising), so this really checks read() doesn't choke
        # on partially-null data rather than on hard failures.
        assert len(rows) == 2

    @patch("pyspark_fingrid.datasource.FingridApiClient")
    def test_read_returns_nothing_when_no_data(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.fetch_data.return_value = []

        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())
        partition = FingridPartition("2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        rows = list(reader.read(partition))
        assert rows == []

    @patch("pyspark_fingrid.datasource.FingridApiClient")
    def test_read_propagates_api_errors(self, mock_client_cls):
        """Unlike the legacy reader (which swallows failures and returns None),
        a partition read failure should surface as a task failure rather than
        silently producing an incomplete result."""
        mock_client = mock_client_cls.return_value
        mock_client.fetch_data.side_effect = FingridApiError("boom")

        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())
        partition = FingridPartition("2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        with pytest.raises(FingridApiError):
            list(reader.read(partition))


class TestRegister:
    """Test the register() convenience function."""

    def test_register_calls_spark_data_source_register(self):
        spark = Mock()
        register(spark)
        spark.dataSource.register.assert_called_once_with(FingridDataSource)


class TestSerialization:
    """PySpark's Python Data Source API requires DataSource/DataSourceReader
    instances to be picklable, since the reader is serialized and shipped to
    executors (https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
    - "Serialization Requirement"). This is guarded explicitly because it's
    easy to accidentally break: e.g. caching a FingridApiClient as a reader
    attribute in __init__ (instead of constructing it fresh inside read())
    would silently break this, since the client's RateLimiter holds a
    threading.Lock, which pickle cannot serialize.
    """

    def _schema(self):
        return FingridDataSource({"apiKey": "test-key", "datasetId": "192"}).schema()

    def test_reader_is_picklable(self):
        reader = FingridDataSourceReader(
            {
                "apiKey": "test-key",
                "datasetId": "192",
                "startTime": "2024-07-24T00:00:00Z",
                "endTime": "2024-07-25T00:00:00Z",
            },
            self._schema(),
        )

        restored = pickle.loads(pickle.dumps(reader))

        assert restored.read_options.dataset_id == 192
        assert restored.read_options.api_key == "test-key"

    def test_partition_is_picklable(self):
        partition = FingridPartition("2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        restored = pickle.loads(pickle.dumps(partition))

        assert restored.start_time == "2024-07-24T00:00:00Z"
        assert restored.end_time == "2024-07-24T01:00:00Z"

    def test_reader_holds_no_client_instance_before_read(self):
        """The reader must not construct/store a FingridApiClient (or
        anything holding a lock) as instance state - only read() may do
        that, locally, on the executor. This is the property that makes
        the pickle tests above pass; asserted directly so a future change
        that reintroduces a stored client fails fast with a clear message
        rather than a cryptic pickling error.
        """
        reader = FingridDataSourceReader({"apiKey": "test-key", "datasetId": "192"}, self._schema())

        for value in vars(reader).values():
            assert not isinstance(value, FingridApiClient), (
                "FingridDataSourceReader must not store a FingridApiClient as "
                "instance state (breaks pickling via its RateLimiter's Lock); "
                "construct it inside read() instead"
            )
