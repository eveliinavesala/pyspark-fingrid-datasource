"""Tests for pyspark_fingrid.transform.transform_records."""

from unittest.mock import Mock

from pyspark_fingrid.transform import transform_records


class TestTransformRecords:
    def test_yields_transformed_rows(self):
        schema_handler = Mock()
        schema_handler.transform_record.side_effect = lambda r: r["value"] * 2

        records = [{"value": 1}, {"value": 2}, {"value": 3}]
        result = list(transform_records(schema_handler, records))

        assert result == [2, 4, 6]

    def test_skips_records_that_fail_to_transform(self, caplog):
        schema_handler = Mock()

        def transform(record):
            if record["value"] == "bad":
                raise ValueError("cannot parse")
            return record["value"]

        schema_handler.transform_record.side_effect = transform

        records = [{"value": 1}, {"value": "bad"}, {"value": 3}]
        result = list(transform_records(schema_handler, records))

        assert result == [1, 3]

    def test_empty_input_yields_nothing(self):
        schema_handler = Mock()
        result = list(transform_records(schema_handler, []))
        assert result == []

    def test_all_records_failing_yields_nothing(self):
        schema_handler = Mock()
        schema_handler.transform_record.side_effect = ValueError("bad")

        result = list(transform_records(schema_handler, [{"a": 1}, {"a": 2}]))
        assert result == []
