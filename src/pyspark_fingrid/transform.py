"""
Shared record-transformation helper.

Both `read_fingrid_data()` and `FingridDataSourceReader.read()` need to turn
a list of raw API records into schema Rows, skipping (and logging) any
record that fails to transform rather than failing the whole batch over one
bad record. This is the single implementation of that policy.
"""

import logging
from collections.abc import Iterable, Iterator
from typing import Any

from pyspark.sql import Row

logger = logging.getLogger(__name__)


def transform_records(schema_handler: Any, records: Iterable[dict]) -> Iterator[Row]:
    """Yield a transformed `Row` for each record that transforms
    successfully. Records that raise during transformation are skipped and
    logged as a warning, rather than aborting the whole read.
    """
    for raw_record in records:
        try:
            yield schema_handler.transform_record(raw_record)
        except Exception as e:
            logger.warning("Skipping record that failed to transform: %s", e)
            continue
