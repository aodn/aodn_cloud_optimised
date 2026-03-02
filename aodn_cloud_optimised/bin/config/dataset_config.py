import json
import pathlib
import typing

import pydantic

import aodn_cloud_optimised.config.dataset

from .csv_config_model import CSVConfigModel
from .parquet_schema_transformation import ParquetSchemaTransformation
from .run_settings import RunSettings
from .zarr_schema_transformation import ZarrSchemaTransformation


class DatasetConfig(pydantic.BaseModel):

    model_config = pydantic.ConfigDict(validate_by_name=True, validate_by_alias=True)

    dataset_name: str
    run_settings: RunSettings
    schema_transformation: dict | None = pydantic.Field(
        default=None,
        description="Schema transformation settings depending on cloud_optimised_format. "
        "Should match ZarrSchemaTransformation if format is 'zarr', "
        "or ParquetSchemaTransformation if format is 'parquet'.",
    )
    dataset_schema: dict[str, typing.Any] = pydantic.Field(
        ...,
        description="Schema definition of the input dataset",
        # Allow serialize and de-serialize of internally used `schema` field name
        # In practice `dataset_schema` is just `schema` in json
        # Remember to `.model_dump(by_alias=True)` to serialize to `schema``
        validation_alias="schema",
        serialization_alias="schema",
    )
    cloud_optimised_format: typing.Literal["zarr", "parquet"]
    metadata_uuid: str | None = pydantic.Field(default=None)
    # Nested CSV configuration validator
    csv_config: CSVConfigModel | None = pydantic.Field(
        default=None,
        description=(
            "Configuration for reading CSV files. "
            "Only one of pandas_read_csv_config or polars_read_csv_config "
            "can be provided inside this object."
        ),
    )

    @pydantic.model_validator(mode="after")
    def validate_schema_transformation_type(self) -> typing.Self:
        """Validate that schema_transformation matches the cloud_optimised_format.

        Ensures that ParquetSchemaTransformation is used for parquet format and
        ZarrSchemaTransformation is used for zarr format.

        :return: The validated model instance
        :rtype: typing.Self
        :raises ValidationError: If schema_transformation does not match the format
        """
        if self.cloud_optimised_format == "parquet":
            ParquetSchemaTransformation.model_validate(self.schema_transformation)
        if self.cloud_optimised_format == "zarr":
            if self.schema_transformation is not None:
                combined_data = {
                    **self.schema_transformation,
                    "dataset_name": self.dataset_name,
                    "dataset_schema": self.dataset_schema,  # inject here
                }
                ZarrSchemaTransformation.model_validate(combined_data)
        return self

    @pydantic.model_validator(mode="after")
    def validate_parquet_partition_time_varname_in_schema(self) -> typing.Self:
        """Validate that partitioning time_varname exists in dataset schema.

        For parquet datasets with time-based partitioning, ensures that the
        time_varname specified in partitioning configuration is present in the
        dataset schema keys.

        :return: The validated model instance
        :rtype: typing.Self
        :raises ValueError: If time_varname is not present in dataset_schema
        """
        if (
            isinstance(self.schema_transformation, ParquetSchemaTransformation)
            and self.schema_transformation.partitioning
        ):
            keys_in_schema = set(self.dataset_schema.keys())

            for part in self.schema_transformation.partitioning:
                if not isinstance(part, dict):
                    continue
                time_extent = part.get("time_extent")
                if time_extent:
                    time_varname = time_extent.get("time_varname")
                    if time_varname not in keys_in_schema:
                        raise ValueError(
                            f"'time_varname' '{time_varname}' in partitioning "
                            f"is not present in dataset_schema keys."
                        )
        return self

    @pydantic.model_validator(mode="after")
    def validate_parquet_partition_spatial_varnames_in_schema(self) -> typing.Self:
        """Validate that partitioning spatial variable names exist in dataset schema.

        For parquet datasets with spatial partitioning, ensures that the lat_varname
        and lon_varname specified in partitioning configuration are present in the
        dataset schema keys.

        :return: The validated model instance
        :rtype: typing.Self
        :raises ValueError: If spatial varnames are missing in dataset_schema
        """
        if (
            isinstance(self.schema_transformation, ParquetSchemaTransformation)
            and self.schema_transformation.partitioning
        ):
            keys_in_schema = set(self.dataset_schema.keys())

            for part in self.schema_transformation.partitioning:
                if not isinstance(part, dict):
                    continue
                spatial_extent = part.get("spatial_extent")
                if spatial_extent:
                    lat_varname = spatial_extent.get("lat_varname")
                    lon_varname = spatial_extent.get("lon_varname")

                    missing_vars = [
                        varname
                        for varname in (lat_varname, lon_varname)
                        if varname not in keys_in_schema
                    ]
                    if missing_vars:
                        raise ValueError(
                            f"The following spatial_extent varnames are missing in dataset_schema keys: {missing_vars}"
                        )
        return self

    # TODO: if we want to test for the existence of the PLACEHOLDER in the aws_opendata_regristry cloud_optimised_creation
    # we should add this key in the class definition. However, having this placeholder doesn't cause problem to the dataset
    # creation
    @pydantic.model_validator(mode="after")
    def validate_no_manual_fill_placeholders(self) -> typing.Self:
        """Validate that no manual fill placeholders remain in the configuration.

        Recursively checks all string values in the model to ensure that no
        placeholder text requiring manual completion remains in the configuration.

        :return: The validated model instance
        :rtype: typing.Self
        :raises ValueError: If any placeholder values are found
        """
        PLACEHOLDER = "FILL UP MANUALLY - CHECK DOCUMENTATION"

        def check_recursive(value, path=""):
            if isinstance(value, str):
                if value.strip() == PLACEHOLDER:
                    raise ValueError(
                        f'Placeholder value found at "{path or "root"}": {PLACEHOLDER}'
                    )
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_recursive(v, f"{path}.{k}" if path else k)
            elif isinstance(value, list):
                for idx, item in enumerate(value):
                    check_recursive(item, f"{path}[{idx}]")
            elif isinstance(value, pydantic.BaseModel):
                check_recursive(value.model_dump(), path)

        check_recursive(self.model_dump())
        return self

    @classmethod
    def _load_dataset_config_json(
        cls,
        config_file_path: pathlib.Path,
    ) -> dict:
        """Load and parse a dataset configuration JSON file.

        Validates that the file exists and has a .json extension before loading.

        :param config_file_path: Path to the JSON configuration file
        :type config_file_path: pathlib.Path
        :return: Parsed JSON configuration as a dictionary
        :rtype: dict
        :raises FileNotFoundError: If the config file does not exist
        :raises ValueError: If the config file does not have a .json extension
        """
        # Validate config file
        if not config_file_path.is_file():
            raise FileNotFoundError(
                f"config file must be a file; did not find config file @ `{config_file_path}`"
            )
        elif config_file_path.suffix != ".json":
            raise ValueError("config file must be a json file")

        return json.loads(config_file_path.read_text())

    @classmethod
    def _merge_dicts(
        cls,
        parent: dict,
        child: dict,
    ):
        """Merge two dictionaries, giving priority to the child dictionary.

        Recursively merges nested dictionaries. The child dictionary's values
        override those of the parent. Special handling: 'schema' key in child
        completely replaces parent schema instead of merging.

        :param parent: The parent dictionary to be merged into
        :type parent: dict
        :param child: The child dictionary whose values take precedence
        :type child: dict
        :return: The merged dictionary with child's values taking precedence
        :rtype: dict
        """
        for key, value in child.items():
            if key == "schema" and value:  # dont merge child and parent schema
                parent[key] = value
            elif isinstance(value, dict) and key in parent:
                parent[key] = cls._merge_dicts(parent[key], value)
            else:
                parent[key] = value
        return parent

    @classmethod
    def load_from_cloud_optimised_directory(
        cls,
        config_file: str,
    ) -> typing.Self:
        """Load a dataset configuration from the module's config directory.

        Loads the specified JSON configuration file from the package's dataset
        config directory. If the configuration specifies a parent_config, loads
        and merges it with the child configuration. Supports one level of hierarchy.

        :param config_file: Name of the JSON configuration file (e.g., 'argo.json')
        :type config_file: str
        :return: Validated DatasetConfig instance
        :rtype: typing.Self
        :raises FileNotFoundError: If the config file does not exist
        :raises ValueError: If the config file is invalid
        """
        # Extract the module dataset config path
        module_dataset_config_dir = pathlib.Path(
            aodn_cloud_optimised.config.dataset.__path__[0]
        ).absolute()

        # Load the dataset_config and its parent
        dataset_config = cls._load_dataset_config_json(
            config_file_path=module_dataset_config_dir / config_file
        )

        # If there is a parent config, load and merge it
        # Assumes only one level of hierarchy
        if dataset_config.get("parent_config") is not None:
            parent_dataset_config = cls._load_dataset_config_json(
                config_file_path=module_dataset_config_dir
                / dataset_config.get("parent_config")
            )
            dataset_config = cls._merge_dicts(
                parent=parent_dataset_config, child=dataset_config
            )

        # Validate
        return DatasetConfig.model_validate(dataset_config)

    @classmethod
    def load_from_path(
        cls,
        config_file_path: pathlib.Path,
    ) -> typing.Self:
        """Load a dataset configuration from an explicit file path.

        Loads and validates a dataset configuration JSON file from the specified
        path. Does not handle parent config hierarchy.

        :param config_file_path: Full path to the JSON configuration file
        :type config_file_path: pathlib.Path
        :return: Validated DatasetConfig instance
        :rtype: typing.Self
        :raises FileNotFoundError: If the config file does not exist
        :raises ValueError: If the config file is invalid
        """
        dataset_config = cls._load_dataset_config_json(
            config_file_path=config_file_path,
        )
        return DatasetConfig.model_validate(dataset_config)
