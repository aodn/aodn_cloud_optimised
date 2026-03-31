from .bin import create_aws_registry_dataset

# Start with core functionality
__all__ = ["create_aws_registry_dataset"]

try:
    # Attempt to load the dev-only module
    from .lib import DataQuery

    __all__.append("DataQuery")
except (ImportError, ModuleNotFoundError):
    # Define a guard function that only raises an error if called
    def DataQuery(*args, **kwargs):
        """
        Placeholder for DataQuery when dev dependencies are missing.
        """
        raise ImportError(
            "DataQuery is only available when installed with dev dependencies. "
            "Please run: poetry install --with dev"
        )
