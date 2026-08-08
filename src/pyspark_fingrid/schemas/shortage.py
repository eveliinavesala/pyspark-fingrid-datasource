"""
Schema implementation for Dataset 336: Electricity Shortage Status.

This module defines the schema and transformation logic for Finland's
electricity shortage status monitoring system.
"""

from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .base import FingridDatasetSchema


class ElectricityShortageStatusSchema(FingridDatasetSchema):
    """
    Schema for Dataset 336: Electricity Shortage Status.

    This dataset contains the current electricity shortage status in Finland,
    updated every 3 minutes. Status codes indicate the severity level.
    """

    # Status code mappings
    STATUS_CODES = {
        0: "Normal",
        1: "Electricity shortage possible",
        2: "High risk of electricity shortage",
        3: "Electricity shortage",
    }

    def get_schema(self) -> StructType:
        """
        Return the PySpark schema for electricity shortage status.

        Returns:
            StructType: Schema with shortage status and description
        """
        return StructType(
            [
                StructField("startTime", TimestampType(), True),
                StructField("endTime", TimestampType(), True),
                StructField("shortage_status", IntegerType(), True),
                StructField("shortage_status_description", StringType(), True),
                StructField("datasetId", IntegerType(), True),
            ]
        )

    def transform_record(self, raw_record: dict[str, Any]) -> Row:
        """
        Transform raw API record to shortage status Row.

        Args:
            raw_record: Raw record from Fingrid API

        Returns:
            Row: PySpark Row with shortage status schema
        """
        status_code = self.parse_int(raw_record.get("value"))
        status_description = self.STATUS_CODES.get(status_code, "Unknown") if status_code is not None else None

        return Row(
            startTime=self.parse_timestamp(raw_record.get("startTime")),
            endTime=self.parse_timestamp(raw_record.get("endTime")),
            shortage_status=status_code,
            shortage_status_description=status_description,
            datasetId=self.parse_int(raw_record.get("datasetId", self.dataset_id)),
        )

    def get_description(self) -> str:
        """
        Get description of this dataset.

        Returns:
            str: Human-readable description with status codes
        """
        return "Electricity shortage status (0=Normal, 1=Possible, 2=High Risk, 3=Shortage), updated every 3 minutes"
