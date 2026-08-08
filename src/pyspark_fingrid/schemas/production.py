"""
Schema implementation for Dataset 192: Electricity Production in Finland.

This module defines the schema and transformation logic for Finland's
real-time electricity production data.
"""

from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from .base import FingridDatasetSchema


class ElectricityProductionSchema(FingridDatasetSchema):
    """
    Schema for Dataset 192: Electricity Production in Finland.

    This dataset contains real-time electricity production data in MW,
    updated every 3 minutes based on Fingrid's operation control system.
    """

    def get_schema(self) -> StructType:
        """
        Return the PySpark schema for electricity production data.

        Returns:
            StructType: Schema with production_mw as the main value column
        """
        return StructType(
            [
                StructField("startTime", TimestampType(), True),
                StructField("endTime", TimestampType(), True),
                StructField("production_mw", DoubleType(), True),
                StructField("datasetId", IntegerType(), True),
            ]
        )

    def transform_record(self, raw_record: dict[str, Any]) -> Row:
        """
        Transform raw API record to electricity production Row.

        Args:
            raw_record: Raw record from Fingrid API

        Returns:
            Row: PySpark Row with electricity production schema
        """
        return Row(
            startTime=self.parse_timestamp(raw_record.get("startTime")),
            endTime=self.parse_timestamp(raw_record.get("endTime")),
            production_mw=self.parse_float(raw_record.get("value")),
            datasetId=self.parse_int(raw_record.get("datasetId", self.dataset_id)),
        )

    def get_description(self) -> str:
        """
        Get description of this dataset.

        Returns:
            str: Human-readable description
        """
        return "Real-time electricity production in Finland (MW), updated every 3 minutes"
