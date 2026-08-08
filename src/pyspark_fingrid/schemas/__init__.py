"""
Fingrid dataset schemas package.

This package contains all dataset-specific schema implementations
and the schema registry for managing them.
"""

from .base import FingridDatasetSchema
from .consumption import ElectricityConsumptionSchema
from .production import ElectricityProductionSchema
from .registry import FingridSchemaRegistry
from .shortage import ElectricityShortageStatusSchema

# Auto-register all implemented schemas
FingridSchemaRegistry.register_schema(192, ElectricityProductionSchema)
FingridSchemaRegistry.register_schema(336, ElectricityShortageStatusSchema)
FingridSchemaRegistry.register_schema(363, ElectricityConsumptionSchema)

__all__ = [
    "ElectricityConsumptionSchema",
    "ElectricityProductionSchema",
    "ElectricityShortageStatusSchema",
    "FingridDatasetSchema",
    "FingridSchemaRegistry",
]
