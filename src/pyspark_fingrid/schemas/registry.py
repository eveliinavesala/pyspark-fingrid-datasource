"""
Schema registry for managing dataset-specific schema implementations.

This module provides a central registry to map dataset IDs to their
corresponding schema implementation classes.
"""

from .base import FingridDatasetSchema


class FingridSchemaRegistry:
    """
    Registry to map dataset IDs to their schema implementations.

    This class manages the mapping between Fingrid dataset IDs and their
    corresponding schema implementation classes.
    """

    _schemas: dict[int, type[FingridDatasetSchema]] = {}

    @classmethod
    def register_schema(cls, dataset_id: int, schema_class: type[FingridDatasetSchema]) -> None:
        """
        Register a new dataset schema implementation.

        Args:
            dataset_id: Fingrid dataset ID
            schema_class: Schema implementation class
        """
        cls._schemas[dataset_id] = schema_class
        print(f"✅ Registered schema for dataset {dataset_id}: {schema_class.__name__}")

    @classmethod
    def get_schema(cls, dataset_id: int) -> FingridDatasetSchema:
        """
        Get schema implementation for a dataset ID.

        Args:
            dataset_id: Fingrid dataset ID

        Returns:
            FingridDatasetSchema: Schema implementation instance

        Raises:
            ValueError: If dataset ID is not registered
        """
        if dataset_id not in cls._schemas:
            available = list(cls._schemas.keys())
            raise ValueError(f"Dataset {dataset_id} not supported. Available datasets: {available}")

        schema_class = cls._schemas[dataset_id]
        return schema_class(dataset_id)

    @classmethod
    def is_registered(cls, dataset_id: int) -> bool:
        """
        Check if a dataset ID is registered.

        Args:
            dataset_id: Fingrid dataset ID

        Returns:
            bool: True if registered, False otherwise
        """
        return dataset_id in cls._schemas

    @classmethod
    def get_available_datasets(cls) -> list[int]:
        """
        Get list of all registered dataset IDs.

        Returns:
            List[int]: List of available dataset IDs
        """
        return list(cls._schemas.keys())

    @classmethod
    def list_available_datasets(cls) -> None:
        """Print all available datasets with descriptions."""
        print("📋 Available Datasets:")
        if not cls._schemas:
            print("   No datasets registered yet.")
            return

        for dataset_id, schema_class in cls._schemas.items():
            try:
                schema_instance = schema_class(dataset_id)
                description = schema_instance.get_description()
                print(f"   {dataset_id}: {description}")
            except Exception as e:
                print(f"   {dataset_id}: Error getting description - {e}")

    @classmethod
    def unregister_schema(cls, dataset_id: int) -> bool:
        """
        Unregister a dataset schema.

        Args:
            dataset_id: Fingrid dataset ID

        Returns:
            bool: True if unregistered, False if not found
        """
        if dataset_id in cls._schemas:
            del cls._schemas[dataset_id]
            print(f"🗑️  Unregistered schema for dataset {dataset_id}")
            return True
        return False

    @classmethod
    def clear_registry(cls) -> None:
        """Clear all registered schemas."""
        cls._schemas.clear()
        print("🗑️  Cleared all registered schemas")
