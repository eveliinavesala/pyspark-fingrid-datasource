"""
PySpark Fingrid Data Source

A Python package for reading Finnish electricity market data from Fingrid's API
into PySpark DataFrames with dataset-specific schemas and proper typing.

Main functions:
    read_fingrid_data: Read data with dataset-specific schema
    list_available_datasets: Show all supported datasets

Example:
    >>> from pyspark_fingrid import read_fingrid_data
    >>> df = read_fingrid_data("your-api-key", 192)
    >>> df.show()
"""

from .datasource import FingridDataSource, FingridDataSourceReader, register
from .exceptions import (
    FingridApiError,
    FingridConfigError,
    FingridError,
    FingridRateLimitError,
    FingridSchemaError,
)
from .reader import list_available_datasets, read_fingrid_data
from .schemas import (
    ElectricityConsumptionSchema,
    ElectricityProductionSchema,
    ElectricityShortageStatusSchema,
    FingridDatasetSchema,
    FingridSchemaRegistry,
)

__version__ = "0.2.0"
__authors__ = ["Chanukya Pekala", "Eveliina Vesala"]
__author__ = "Chanukya Pekala, Eveliina Vesala"
__email__ = "chanukya.pekala@gmail.com, eveliina.ves@gmail.com"

__all__ = [
    'read_fingrid_data',
    'list_available_datasets',
    'FingridDataSource',
    'FingridDataSourceReader',
    'register',
    'FingridSchemaRegistry',
    'FingridDatasetSchema',
    'ElectricityProductionSchema',
    'ElectricityShortageStatusSchema',
    'ElectricityConsumptionSchema',
    'FingridError',
    'FingridConfigError',
    'FingridSchemaError',
    'FingridApiError',
    'FingridRateLimitError',
]
