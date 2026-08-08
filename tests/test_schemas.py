"""
Tests for Fingrid dataset schemas.

This module contains unit tests for the schema implementations
and registry functionality.
"""

from datetime import datetime

import pytest
from pyspark.sql.types import (
    StructType,
)

from pyspark_fingrid.schemas import (
    ElectricityConsumptionSchema,
    ElectricityProductionSchema,
    ElectricityShortageStatusSchema,
    FingridSchemaRegistry,
)


class TestElectricityProductionSchema:
    """Test electricity production schema (dataset 192)."""

    def test_schema_structure(self):
        """Test that schema has correct structure."""
        schema_handler = ElectricityProductionSchema(192)
        schema = schema_handler.get_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        expected_fields = ["startTime", "endTime", "production_mw", "datasetId"]
        assert field_names == expected_fields

    def test_transform_record(self):
        """Test record transformation."""
        schema_handler = ElectricityProductionSchema(192)

        raw_record = {
            "datasetId": 192,
            "startTime": "2024-07-24T12:00:00.000Z",
            "endTime": "2024-07-24T12:03:00.000Z",
            "value": 6789.5,
        }

        row = schema_handler.transform_record(raw_record)

        assert row.datasetId == 192
        assert row.production_mw == 6789.5
        assert isinstance(row.startTime, datetime)
        assert isinstance(row.endTime, datetime)

    def test_description(self):
        """Test dataset description."""
        schema_handler = ElectricityProductionSchema(192)
        description = schema_handler.get_description()

        assert "electricity production" in description.lower()
        assert "MW" in description
        assert "3 minutes" in description


class TestElectricityShortageStatusSchema:
    """Test electricity shortage status schema (dataset 336)."""

    def test_schema_structure(self):
        """Test that schema has correct structure."""
        schema_handler = ElectricityShortageStatusSchema(336)
        schema = schema_handler.get_schema()

        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "startTime",
            "endTime",
            "shortage_status",
            "shortage_status_description",
            "datasetId",
        ]
        assert field_names == expected_fields

    def test_status_code_mapping(self):
        """Test status code to description mapping."""
        schema_handler = ElectricityShortageStatusSchema(336)

        test_cases = [
            (0, "Normal"),
            (1, "Electricity shortage possible"),
            (2, "High risk of electricity shortage"),
            (3, "Electricity shortage"),
            (99, "Unknown"),  # Invalid code
        ]

        for status_code, expected_description in test_cases:
            raw_record = {
                "datasetId": 336,
                "startTime": "2024-07-24T12:00:00.000Z",
                "endTime": "2024-07-24T12:03:00.000Z",
                "value": status_code,
            }

            row = schema_handler.transform_record(raw_record)
            assert row.shortage_status == status_code
            assert row.shortage_status_description == expected_description


class TestElectricityConsumptionSchema:
    """Test electricity consumption schema (dataset 363)."""

    def test_schema_structure(self):
        """Test that schema has correct structure."""
        schema_handler = ElectricityConsumptionSchema(363)
        schema = schema_handler.get_schema()

        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "startTime",
            "endTime",
            "consumption_kwh",
            "datasetId",
            "time_series_type",
            "resolution",
            "unit",
            "read_timestamp",
            "measurement_count",
        ]
        assert field_names == expected_fields

    def test_transform_record_with_additional_json(self):
        """Test record transformation with additionalJson."""
        schema_handler = ElectricityConsumptionSchema(363)

        raw_record = {
            "datasetId": 363,
            "startTime": "2024-07-24T12:00:00.000Z",
            "endTime": "2024-07-24T13:00:00.000Z",
            "value": 3502751.77,
            "additionalJson": {
                "TimeSeriesType": "CTT_SUM_CONS_ACP",
                "Res": "PT1H",
                "Uom": "KWH",
                "ReadTS": "2024-07-24T12:00:00Z",
                "Value": "3502751.77",
                "Count": "3600745",
            },
        }

        row = schema_handler.transform_record(raw_record)

        assert row.consumption_kwh == 3502751.77
        assert row.time_series_type == "CTT_SUM_CONS_ACP"
        assert row.resolution == "PT1H"
        assert row.unit == "KWH"
        assert row.measurement_count == 3600745
        assert isinstance(row.read_timestamp, datetime)


class TestFingridSchemaRegistry:
    """Test schema registry functionality."""

    def test_get_registered_schema(self):
        """Test getting a registered schema."""
        schema_handler = FingridSchemaRegistry.get_schema(192)
        assert isinstance(schema_handler, ElectricityProductionSchema)
        assert schema_handler.dataset_id == 192

    def test_get_unregistered_schema(self):
        """Test getting an unregistered schema raises ValueError."""
        with pytest.raises(ValueError, match="Dataset 999 not supported"):
            FingridSchemaRegistry.get_schema(999)

    def test_is_registered(self):
        """Test is_registered reports registered and unregistered ids correctly."""
        assert FingridSchemaRegistry.is_registered(192) is True
        assert FingridSchemaRegistry.is_registered(999) is False

    def test_get_available_datasets(self):
        """Test get_available_datasets returns all registered dataset ids."""
        available = FingridSchemaRegistry.get_available_datasets()
        assert 192 in available
        assert 336 in available
        assert 363 in available
