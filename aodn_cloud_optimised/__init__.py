from .bin import create_aws_registry_dataset

# Start with core functionality
__all__ = ["create_aws_registry_dataset"]

try:
    # Attempt to load the dev-only module
    from .lib import DataQuery

    __all__.append("DataQuery")
except (ImportError, ModuleNotFoundError):
    class _DataQueryProxy:
        """
        Placeholder for DataQuery when dev dependencies are missing.

        Any attempt to call this object or access its attributes will raise
        an ImportError with guidance on how to install the dev dependencies.
        """

        _ERROR = ImportError(
            "DataQuery is only available when installed with dev dependencies. "
            "Please run: poetry install --with dev"
        )

        def __getattr__(self, name):
            raise self._ERROR

        def __call__(self, *args, **kwargs):
            raise self._ERROR

    DataQuery = _DataQueryProxy()
