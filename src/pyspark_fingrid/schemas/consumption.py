"""
Schema implementation for Dataset 363: Electricity Consumption.

This module defines the schema and transformation logic for Finland's
electricity consumption data with detailed metadata.
"""

from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .base import FingridDatasetSchema


class ElectricityConsumptionSchema(FingridDatasetSchema):
    """
    Schema for Dataset 363: Electricity Consumption.

    This dataset contains electricity consumption data with additional
    metadata including time series type, resolution, and measurement details.
    """

    def get_schema(self) -> StructType:
        """
        Return the PySpark schema for electricity consumption data.

        Returns:
            StructType: Schema with consumption and metadata fields
        """
        return StructType(
            [
                StructField("startTime", TimestampType(), True),
                StructField("endTime", TimestampType(), True),
                StructField("consumption_kwh", DoubleType(), True),
                StructField("datasetId", IntegerType(), True),
                StructField("time_series_type", StringType(), True),
                StructField("resolution", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("read_timestamp", TimestampType(), True),
                StructField("measurement_count", IntegerType(), True),
            ]
        )

    def transform_record(self, raw_record: dict[str, Any]) -> Row:
        """
        Transform raw API record to consumption Row.

        Args:
            raw_record: Raw record from Fingrid API

        Returns:
            Row: PySpark Row with consumption schema
        """
        additional_json = raw_record.get("additionalJson", {})

        return Row(
            startTime=self.parse_timestamp(raw_record.get("startTime")),
            endTime=self.parse_timestamp(raw_record.get("endTime")),
            consumption_kwh=self.parse_float(raw_record.get("value")),
            datasetId=self.parse_int(raw_record.get("datasetId", self.dataset_id)),
            time_series_type=additional_json.get("TimeSeriesType"),
            resolution=additional_json.get("Res"),
            unit=additional_json.get("Uom"),
            read_timestamp=self.parse_timestamp(additional_json.get("ReadTS")),
            measurement_count=self.parse_int(additional_json.get("Count")),
        )

    def get_description(self) -> str:
        """
        Get description of this dataset.

        Returns:
            str: Human-readable description
        """
        return "Electricity consumption data with detailed metadata (KWH), updated hourly"
