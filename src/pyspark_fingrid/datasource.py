"""
Fingrid PySpark DataSource (Python Data Source API).

Implements the modern ``pyspark.sql.datasource.DataSource`` /
``DataSourceReader`` interface (added in PySpark 4.0) so Fingrid data can be
read the idiomatic Spark way::

    df = (
        spark.read.format("fingrid")
        .option("apiKey", "your-api-key")
        .option("datasetId", 192)
        .option("startTime", "2024-07-24T00:00:00Z")
        .option("endTime", "2024-07-25T00:00:00Z")
        .load()
    )

Unlike the legacy ``read_fingrid_data`` helper (still available for backward
compatibility), this reads data per-partition on executors rather than
building the full result set as a Python list on the driver, and it splits
the requested time range into day-sized partitions so pages of data can be
fetched in parallel.

Both this module and reader.py build on the same FingridApiClient,
FingridReadOptions, and transform_records() - there is one implementation
of "call the API," "parse/validate options," and "transform a record,"
shared by both entry points.
"""

from collections.abc import Iterator, Sequence
from datetime import timedelta
from typing import Any

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from .client import FingridApiClient
from .options import FingridReadOptions, format_timestamp
from .schemas import FingridSchemaRegistry
from .transform import transform_records


def register(spark) -> None:
    """
    Register the "fingrid" format with a SparkSession.

    Args:
        spark: An active SparkSession.

    Example:
        >>> from pyspark_fingrid import register
        >>> register(spark)
        >>> df = (
        ...     spark.read.format("fingrid")
        ...     .option("apiKey", "your-api-key")
        ...     .option("datasetId", 192)
        ...     .load()
        ... )
    """
    spark.dataSource.register(FingridDataSource)


class FingridPartition(InputPartition):
    """A single time-range slice of a Fingrid dataset read.

    Splitting by time range (rather than reading the whole range in one
    request) lets Spark fetch and process slices in parallel across
    executors, and keeps any single request's payload bounded.
    """

    def __init__(self, start_time: str, end_time: str):
        self.start_time = start_time
        self.end_time = end_time


class FingridDataSource(DataSource):
    """PySpark DataSource for reading Fingrid open data datasets.

    Options:
        apiKey (str, required): Fingrid API key (https://data.fingrid.fi/en/).
        datasetId (int, required): Fingrid dataset ID (must be registered in
            the schema registry, e.g. 192, 336, 363).
        startTime (str, optional): ISO-8601 start time, e.g.
            "2024-07-24T00:00:00Z". Defaults to 30 minutes before now (UTC).
        endTime (str, optional): ISO-8601 end time. Defaults to now (UTC).
        partitionDays (int, optional): Size, in days, of each partition's
            time slice. Defaults to 1.
        pageSize (int, optional): Records requested per API page within a
            partition. Defaults to 20000.
    """

    @classmethod
    def name(cls) -> str:
        return "fingrid"

    def schema(self) -> StructType:
        dataset_id = FingridReadOptions.from_dict(self.options).dataset_id
        schema_handler = FingridSchemaRegistry.get_schema(dataset_id)
        return schema_handler.get_schema()

    def reader(self, schema: StructType) -> "FingridDataSourceReader":
        return FingridDataSourceReader(self.options, schema)


class FingridDataSourceReader(DataSourceReader):
    """Reader that fetches Fingrid data per time-range partition."""

    def __init__(self, options: dict[str, str], schema: StructType):
        self.read_options = FingridReadOptions.from_dict(options)
        self.schema = schema
        # Validates the dataset is supported and gives us the transform logic.
        self.schema_handler = FingridSchemaRegistry.get_schema(self.read_options.dataset_id)

    def partitions(self) -> Sequence[InputPartition]:
        """Split the requested [startTime, endTime) range into day-sized slices."""
        slices: list[FingridPartition] = []
        slice_start = self.read_options.start_time
        step = timedelta(days=self.read_options.partition_days)

        while slice_start < self.read_options.end_time:
            slice_end = min(slice_start + step, self.read_options.end_time)
            slices.append(
                FingridPartition(
                    start_time=format_timestamp(slice_start),
                    end_time=format_timestamp(slice_end),
                )
            )
            slice_start = slice_end

        return slices

    def read(self, partition: "FingridPartition") -> Iterator[tuple[Any, ...]]:
        """Fetch and transform one partition's worth of data.

        Runs on the executor. Uses the same paginating FingridApiClient as
        the legacy reader, so a partition's records are still fully
        retrieved across API pages, but only for that partition's slice of
        the overall time range.

        Unlike the legacy reader (which prints and returns None on a failed
        fetch), a FingridApiError here propagates and fails the Spark task,
        so a partition that couldn't be fetched surfaces as a job failure
        rather than silently missing rows in the result.
        """
        client = FingridApiClient(self.read_options.api_key)
        records = client.fetch_data(
            self.read_options.dataset_id,
            partition.start_time,
            partition.end_time,
            page_size=self.read_options.page_size,
        )

        if not records:
            return

        field_names = [f.name for f in self.schema.fields]
        for row in transform_records(self.schema_handler, records):
            yield tuple(row[name] for name in field_names)
