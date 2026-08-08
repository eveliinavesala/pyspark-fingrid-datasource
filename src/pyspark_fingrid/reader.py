"""
Main data reader for Fingrid datasets.

This module provides the primary interface for reading Fingrid data
with dataset-specific schemas and transformations. It's an orchestration
layer only: HTTP/pagination/rate-limiting live in `client.py`, option
parsing/validation/defaulting lives in `options.py`, and per-record
transform-and-skip logic lives in `transform.py`. This module wires those
together and turns the result into a PySpark DataFrame.
"""

import logging

from pyspark.sql import DataFrame

from .client import FingridApiClient
from .exceptions import FingridApiError
from .options import FingridReadOptions
from .schemas import FingridSchemaRegistry
from .transform import transform_records

logger = logging.getLogger(__name__)


def read_fingrid_data(
    api_key: str, dataset_id: int, start_time: str | None = None, end_time: str | None = None
) -> DataFrame | None:
    """
    Read Fingrid data with dataset-specific typing and schema.

    This function fetches data from the Fingrid API and returns a PySpark DataFrame
    with dataset-specific column names and types based on the registered schema.

    Args:
        api_key: Your Fingrid API key (get from https://data.fingrid.fi/en/)
        dataset_id: Dataset ID (must be registered in schema registry)
        start_time: ISO format "2024-07-24T00:00:00Z" (optional, defaults to last 30 min)
        end_time: ISO format "2024-07-24T06:00:00Z" (optional, defaults to now)

    Returns:
        DataFrame or None: PySpark DataFrame with dataset-specific schema, or None if
        there was no data for the requested range or all records failed to transform.

    Raises:
        FingridConfigError: If api_key/dataset_id/time range are invalid (subclasses
            ValueError, so existing `except ValueError` callers keep working).
        FingridSchemaError: If dataset_id has no registered schema (subclasses ValueError).
        FingridApiError: If the Fingrid API request fails.
        FingridRateLimitError: If retries are exhausted while rate limited.

    Example:
        >>> df = read_fingrid_data("your-api-key", 192)
        >>> df.printSchema()
        root
         |-- startTime: timestamp (nullable = true)
         |-- endTime: timestamp (nullable = true)
         |-- production_mw: double (nullable = true)
         |-- datasetId: integer (nullable = true)
    """
    read_options = FingridReadOptions.from_args(api_key, dataset_id, start_time, end_time)

    try:
        schema_handler = FingridSchemaRegistry.get_schema(read_options.dataset_id)
    except ValueError as e:
        print(f"❌ {e}")
        print("\nAvailable datasets:")
        FingridSchemaRegistry.list_available_datasets()
        raise

    print(f"📊 Reading dataset {read_options.dataset_id}: {schema_handler.get_description()}")
    print(f"⏰ Time range: {read_options.start_time_str} to {read_options.end_time_str}")

    client = FingridApiClient(read_options.api_key)

    try:
        metadata = client.fetch_metadata(read_options.dataset_id)
    except FingridApiError as e:
        # Metadata is best-effort: a failure here (rate limited, network error,
        # ...) shouldn't abort the read - only the actual data fetch below does.
        logger.warning("Could not fetch metadata: %s", e)
        metadata = None

    if metadata:
        schema_handler.set_metadata(metadata)
        print(f"📋 Name: {schema_handler.get_name()}")
        print(f"📏 Unit: {schema_handler.get_unit()}")
        print(f"🔄 Update frequency: {schema_handler.get_update_frequency()}")

    try:
        records = client.fetch_data(read_options.dataset_id, read_options.start_time_str, read_options.end_time_str)
    except FingridApiError as e:
        print(f"❌ Failed to fetch data: {e}")
        return None

    if not records:
        print("   ⚠️  No data returned")
        return None

    print(f"🔄 Transforming data using {schema_handler.__class__.__name__}")
    rows = list(transform_records(schema_handler, records))

    if not rows:
        print("❌ No valid records after transformation")
        return None

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if not spark:
            raise RuntimeError("No active Spark session found")

        df = spark.createDataFrame(rows, schema_handler.get_schema())
        print("✅ DataFrame created with dataset-specific schema!")
        return df

    except Exception as e:
        print(f"❌ Failed to create DataFrame: {e}")
        return None


def list_available_datasets() -> None:
    """
    List all available datasets with descriptions.

    This is a convenience function that delegates to the schema registry.
    """
    FingridSchemaRegistry.list_available_datasets()
