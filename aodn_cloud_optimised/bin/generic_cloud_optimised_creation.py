#!/usr/bin/env python3

"""
Runner to generate cloud-optimised datasets from S3 files, based on a dataset config JSON.

Example usage:
  generic_cloud_optimised_creation --config mooring_hourly_timeseries_delayed_qc
  generic_cloud_optimised_creation --config satellite_chlorophylla_gsm_1day_aqua --json-overwrite '{"run_settings": {"cluster": {"mode": null}, "raise_error": true}, "clear_existing_data": false}'  # useful for single file processing on prefect
  generic_cloud_optimised_creation --config satellite_chlorophylla_gsm_1day_aqua --json-overwrite '{"run_settings": {"cluster": {"mode": null}, "raise_error": true},  "clear_existing_data": false, "force_previous_parquet_deletion": true }' # useful for parquet dataset to overwrite existing matching input files already processed
"""

import argparse
import json
import logging
from logging.config import valid_ident
import re
import sys
import warnings
from pathlib import Path, PurePosixPath
from typing import Any, List, Literal, Optional, Union
from urllib.parse import urlparse

import numpy as np
from pydantic import (
    BaseModel,
    Field,
    StrictBool,
    ValidationError,
    field_validator,
    model_validator,
)

from aodn_cloud_optimised.lib import clusterLib
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls

logger = logging.getLogger(__name__)


# ---------- Pydantic schema for validating the config ----------
class ClusterConfig(BaseModel):
    """Cluster processing configuration.

    Attributes:
        mode: Cluster mode (local, coiled, ec2, or None).
        restart_every_path: Restart cluster after processing each path.
    """

    mode: Optional[clusterLib.ClusterMode] | None = Field(
        default=None,
        description=f"The mode for the cluster. Must be one of: {[m.value for m in clusterLib.ClusterMode]} or null.",
    )
    restart_every_path: bool = Field(
        default=False,
        description="Whether to restart the cluster after each path is processed.",
    )


class PathConfig(BaseModel):
    """Input path configuration.

    Attributes:
        s3_uri: S3 URI as a POSIX path string.
        filter: List of regex patterns to filter files.
        year_range: Year filter: None, one year, or a two-year inclusive range, or a list of exclusive years to process.
    """

    s3_uri: str
    filter: List[str] = Field(
        default_factory=list,
        description="List of regular expression patterns used to filter matching files.",
    )
    year_range: Optional[List[int]] = Field(
        default=None,
        description="Must be None (no filtering), a single year [YYYY], a two-year range [YYYY, YYYY], or a list of exclusive years to process [YYYY, YYYY, YYYY]",
    )

    @field_validator("year_range", mode="after")
    def validate_year_range(cls, v: Optional[List[int]]) -> Optional[List[int]]:
        if v is None or len(v) == 0:
            return None  # No year filtering

        # Validate all items are int
        if not all(isinstance(year, int) for year in v):
            raise ValueError("year_range must be a list of integers")
        # If one year, return as single-item list
        if len(v) == 1:
            return v

        if len(v) == 2:
            start, end = v
            if start > end:
                raise ValueError("year_range start year must be <= end year")
            elif start == end:
                raise ValueError("year_range start year must be != end year")
            # Return inclusive range list
            return list(range(start, end + 1))

        # More than 2 years, treat as explicit list
        # Validate sorted ascending and unique
        if sorted(v) != v:
            raise ValueError("year_range list must be sorted in ascending order")
        if len(set(v)) != len(v):
            raise ValueError("year_range list must contain unique years")

        return v

    @field_validator("s3_uri", mode="after")
    def validate_s3_uri(cls, v: str) -> str:
        if not isinstance(v, str):
            raise TypeError("s3_uri must be a string")

        if v.startswith("s3://"):
            parsed = urlparse(v)
            if not parsed.netloc:
                raise ValueError("s3_uri must include a bucket name after 's3://'")
            if not parsed.path or parsed.path == "/":
                raise ValueError(
                    "s3_uri must include a valid key path after the bucket"
                )
            try:
                PurePosixPath(parsed.path.lstrip("/"))
            except Exception as e:
                raise ValueError(f"s3_uri key path is not a valid POSIX path: {e}")
        else:
            # Validate as a relative POSIX path (e.g. "IMOS/SRS/...")
            try:
                PurePosixPath(v)
            except Exception as e:
                raise ValueError(f"s3_uri is not a valid relative POSIX path: {e}")

        return v

    @field_validator("filter", mode="after")
    def validate_regex(cls, v):
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex: {pattern} ({e})")
        return v


class WorkerOptions(BaseModel):
    """Worker configuration for Coiled clusters.

    Attributes:
        nthreads: Number of threads per worker.
        memory_limit: Memory limit per worker (e.g., "64GB").
    """

    nthreads: int
    memory_limit: str  # "64GB", etc.


class CoiledClusterOptions(BaseModel):
    """Configuration options for a Coiled cluster.

    Attributes:
        n_workers: List of two integers specifying the minimum and maximum number of workers (e.g., [25, 150]).
        scheduler_vm_types: VM type to use for the Coiled cluster scheduler.
        worker_vm_types: VM type to use for Coiled cluster workers.
        allow_ingress_from: IP or CIDR block allowed to access the cluster.
        compute_purchase_option: AWS compute purchase option (e.g., "on_demand", "spot").
        worker_options: Configuration for individual Coiled cluster workers.
    """

    n_workers: List[int] = Field(
        ..., description="List of integers: min and max workers (e.g., [25, 150])."
    )
    scheduler_vm_types: str
    worker_vm_types: str
    allow_ingress_from: str
    compute_purchase_option: str
    worker_options: WorkerOptions


class Ec2ClusterOptions(BaseModel):
    """Configuration options for a EC2 cluster.

    Attributes:
        n_workers: integer specifying the number of workers (e.g., 25).
        scheduler_instance_type: VM type to use for the Coiled cluster scheduler.
        worker_instance_type: VM type to use for Coiled cluster workers.
        security: boolean.
        docker_image: url as string
        worker_options: Configuration for individual Coiled cluster workers.
    """

    n_workers: int = Field(..., description="integer of workers  (e.g., 25).")
    scheduler_instance_type: str
    worker_instance_type: str
    security: bool
    docker_image: str


class Ec2AdaptOptions(BaseModel):
    """Configuration options for a EC2 cluster workers.

    Attributes:
    """

    minimum: int = Field(..., description="integer of minimum workers  (e.g., 25).")
    maximum: int = Field(..., description="integer of maximum workers  (e.g., 25).")


class RunSettings(BaseModel):
    """Run configuration for processing.

    Attributes:
        paths: List of dataset path configurations.
        cluster: Cluster execution settings.
        clear_existing_data: Clear ALL existing Cloud Optimised data before run.
        force_previous_parquet_deletion: Force deletion of previous Parquet files matching an input file.
        raise_error: Raise errors and exit instead of simply logging.
        suffix: File suffix for input file matching.
        exclude: Optional regex to exclude files.
        optimised_bucket_name: Optional override for the destination S3 bucket where cloud-optimised output will be written.
        root_prefix_cloud_optimised_path: Optional override for the root key prefix inside the optimised bucket.
        bucket_raw_default_name: Optional override for the default raw data input bucket.
            If provided, it must match the bucket in any `s3_uri` that begins with 's3://'.
        batch_size: Optional maximum number of files to process in a single batch. Must be a positive integer.
        coiled_cluster_options: Optional configuration block for Coiled clusters.
            Required only when cluster mode is set to 'coiled'.


    Notes:
        If `s3_uri` starts with 's3://', and `bucket_raw_default_name` is also provided,
        the bucket in the URI must match `bucket_raw_default_name`, or validation will fail.

        When `cluster.mode` is set to "coiled", `coiled_cluster_options` must be provided,
        otherwise a validation error will be raised.
    """

    paths: List[PathConfig] = Field(
        ..., description="List of input S3 path configs to process."
    )
    cluster: ClusterConfig = Field(
        default_factory=ClusterConfig, description="Settings for cluster configuration."
    )
    clear_existing_data: bool = Field(
        default=False,
        description="If True, clear previously optimised data before processing.",
    )
    force_previous_parquet_deletion: bool = Field(
        default=False,
        description="If True, force deletion of previously generated Parquet files for matching input file.",
    )
    raise_error: StrictBool = Field(
        default=False,
        description="If True, raise errors for every logger.Error instead of continuing silently.",
    )
    suffix: str = Field(
        default=".nc",
        description="Suffix used to identify relevant input files (e.g., '.nc').",
    )
    exclude: Optional[str] = None
    optimised_bucket_name: Optional[str] = Field(
        default=None,
        description="Override the default cloud optimised S3 bucket name.",
    )
    root_prefix_cloud_optimised_path: Optional[str] = Field(
        default=None,
        description="Override the default root prefix path for the cloud-optimised output.",
    )
    bucket_raw_default_name: Optional[str] = Field(
        default=None,
        description="Override the input s3 bucket name where the input files are located.",
    )
    batch_size: Optional[int] = Field(
        default=None,
        description="Maximum number of files to process in a batch (must be a positive integer).",
        ge=1,
    )
    coiled_cluster_options: Optional[CoiledClusterOptions] = Field(
        default=None,
        description="Configuration options required when cluster.mode is 'coiled'. Ignored otherwise.",
    )
    ec2_cluster_options: Optional[Ec2ClusterOptions] = Field(
        default=None,
        description="Configuration options required when cluster.mode is 'ec2'. Ignored otherwise.",
    )
    ec2_adapt_options: Optional[Ec2AdaptOptions] = Field(
        default=None,
        description="Configuration min/max wokersoptions required when cluster.mode is 'ec2'. Ignored otherwise.",
    )

    @model_validator(mode="after")
    def validate_bucket_consistency(self) -> "RunSettings":
        for path in self.paths:
            if path.s3_uri.startswith("s3://"):
                parsed = urlparse(path.s3_uri)
                bucket_from_uri = parsed.netloc
                if (
                    self.bucket_raw_default_name is not None
                    and bucket_from_uri != self.bucket_raw_default_name
                ):
                    raise ValueError(
                        f"`bucket_raw_default_name` ('{self.bucket_raw_default_name}') does not match the bucket in s3_uri ('{bucket_from_uri}')."
                    )
        return self

    @model_validator(mode="after")
    def validate_cluster_opts(self) -> "RunSettings":
        # Validate_ coiled options if mode is coiled
        if (
            self.cluster.mode == clusterLib.ClusterMode.COILED
            and self.coiled_cluster_options is None
        ):
            raise ValueError(
                "coiled_cluster_options must be provided when cluster.mode is 'coiled'"
            )
        elif self.cluster.mode == clusterLib.ClusterMode.EC2 and (
            self.ec2_cluster_options is None or self.ec2_adapt_options is None
        ):
            raise ValueError(
                "ec2_cluster_options must be provided when cluster.mode is 'ec2'"
            )

        return self


class DatasetConfig(BaseModel):
    dataset_name: str
    run_settings: RunSettings  # replace with concrete type
    schema: dict[str, Any]
    vars_incompatible_with_region: Optional[list[str]] = None
    gattrs_to_variables: Optional[Union[dict[str, dict[str, Any]], list[str]]] = None
    dimensions: dict[str, dict[str, Any]] | None = None
    cloud_optimised_format: Literal["zarr", "parquet"]
    partition_keys: Optional[list[str]] = None
    time_extent: Optional[dict[str, str]] = None
    spatial_extent: Optional[dict[str, Any]] = None
    dataset_gattrs: Optional[dict[str, Any]] = None
    dataset_sort_by: Optional[list[str]] = None

    @model_validator(mode="after")
    def validate_dataset_sortby(self) -> "DatasetConfig":
        if self.dataset_sort_by:
            valid_names = {
                dim["name"] for dim in self.dimensions.values() if "name" in dim
            }
            for var in self.dataset_sort_by:
                if var not in valid_names:
                    raise ValueError(
                        f"{var} does not exist in the dimensions list and can't be used to sort the dataset"
                    )
        return self

    @model_validator(mode="after")
    def validate_dimensions_config(self) -> "DatasetConfig":
        if not self.dimensions:
            return self  # Nothing to check

        # Check how many dimensions have "append_dim" set
        has_append_flag = [
            (dim_key, props.get("append_dim"))
            for dim_key, props in self.dimensions.items()
            if "append_dim" in props
        ]

        if has_append_flag:
            # Ensure exactly one is True
            true_dims = [k for k, v in has_append_flag if v is True]
            if len(true_dims) != 1:
                raise ValueError(
                    f"Exactly one dimension must have 'append_dim: true'. Found: {true_dims}"
                )
        else:
            # No dimension has an append_dim key
            warnings.warn(
                f"{self.dataset_name}\n"
                "No 'append_dim' key was found in any dimension config. "
                "will default to using dimensions[\"time\"]. Consider adding 'append_dim: true' "
                "to one dimension explicitly for clarity.",
                stacklevel=1,
            )

        return self

    # TODO: if we want to test for the existence of the PLACEHOLDER in the aws_opendata_regristry cloud_optimised_creation
    # we should add this key in the class definition. However, having this placeholder doesn't cause problem to the dataset
    # creation
    @model_validator(mode="after")
    def validate_no_manual_fill_placeholders(self) -> "DatasetConfig":
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
            elif isinstance(value, BaseModel):
                check_recursive(value.model_dump(), path)

        check_recursive(self.model_dump())
        return self

    @model_validator(mode="after")
    def validate_vars_incompatible_with_region(self) -> "DatasetConfig":
        if self.vars_incompatible_with_region:
            missing = [
                var
                for var in self.vars_incompatible_with_region
                if var not in self.schema
            ]
            if missing:
                raise ValueError(
                    f"The following vars_incompatible_with_region are not defined in schema configuration or mispelled: {missing}"
                )
        return self

    @model_validator(mode="after")
    def validate_gattrs_to_variable_dimensions(self) -> "DatasetConfig":
        if self.gattrs_to_variables:
            if self.cloud_optimised_format == "zarr":
                valid_dimension_names = {
                    dim_def["name"] for dim_def in self.dimensions.values()
                }
                invalid_entries = []
                for key, var_def in self.gattrs_to_variables.items():
                    dim_name = var_def.get("dimensions")
                    if dim_name not in valid_dimension_names:
                        invalid_entries.append((key, dim_name))

                    def is_valid_dtype(attr_type) -> bool:
                        try:
                            np.dtype(attr_type)
                            return True
                        except TypeError:
                            return False

                    def not_implemented_dtype(attr_type) -> bool:
                        if np.dtype(attr_type).kind != "U":
                            raise ValueError(
                                f"{attr_type} to convert a global attribute to a variable is currently not implemented in this library due to dask issues and data loss. Use string or <U type instead"
                            )
                        else:
                            return False

                    attr_type = var_def.get("dtype", None)
                    if not is_valid_dtype(attr_type) or attr_type is None:
                        raise ValueError(
                            f"{attr_type} for variable {key} is not a valid type"
                        )

                    not_implemented_dtype(attr_type)

                if invalid_entries:
                    raise ValueError(
                        f"The following gattrs_to_variables entries refer to undefined dimensions: "
                        + ", ".join(f"{k} (dimension: {d})" for k, d in invalid_entries)
                    )
            if self.cloud_optimised_format == "parquet":
                for var in self.gattrs_to_variables:
                    var_def = self.schema.get(var)
                    numeric_types = {
                        "int",
                        "int32",
                        "int64",
                        "float",
                        "float32",
                        "float64",
                        "number",
                    }

                    if (
                        not var_def
                        or var_def.get("type") not in {"string"} | numeric_types
                    ):
                        raise ValueError(
                            f"schema['{var}'] must be of type 'string' or a numeric type "
                            f"(e.g. {sorted(numeric_types)}), required by gattrs_to_variables"
                        )
        return self

    @model_validator(mode="after")
    def validate_parquet_config(self) -> "DatasetConfig":
        if self.cloud_optimised_format == "parquet":
            if not self.partition_keys:
                raise ValueError("partition_keys must be defined for parquet format")

            # timestamp partition check
            if "timestamp" in self.partition_keys:
                # Validate time_extent block exists
                if not self.time_extent:
                    raise ValueError(
                        "time_extent must be defined when 'timestamp' is a partition key"
                    )
                time_var = self.time_extent.get("time")
                if time_var not in self.schema:
                    raise ValueError(
                        f"time_extent 'time' value '{time_var}' is not defined in schema"
                    )

                # Validate timestamp partition period
                valid_periods = {"M", "Q", "Y", "h", "min", "s", "ms", "us", "ns"}
                period = self.time_extent.get("partition_timestamp_period")
                if period not in valid_periods:
                    raise ValueError(
                        f"Invalid partition_timestamp_period '{period}'. Must be one of {sorted(valid_periods)}"
                    )

                # Validate timestamp exists in schema
                timestamp_def = self.schema.get("timestamp")
                if not timestamp_def:
                    raise ValueError("'timestamp' must be defined in schema")
                if timestamp_def.get("type") != "int64":
                    raise ValueError("schema['timestamp'] must have type 'int64'")

            # polygon partition check
            if "polygon" in self.partition_keys:
                if not self.spatial_extent:
                    raise ValueError(
                        "spatial_extent must be defined when 'polygon' is a partition key"
                    )

                lat_var = self.spatial_extent.get("lat")
                lon_var = self.spatial_extent.get("lon")
                resolution = self.spatial_extent.get("spatial_resolution")

                missing = [v for v in (lat_var, lon_var) if v not in self.schema]
                if missing:
                    raise ValueError(
                        f"The following spatial_extent variables are not defined in schema: {missing}"
                    )

                if not isinstance(resolution, int) or resolution >= 50:
                    raise ValueError(
                        "spatial_extent.spatial_resolution must be an integer less than 50"
                    )

                polygon_def = self.schema.get("polygon")
                if not polygon_def or polygon_def.get("type") != "string":
                    raise ValueError(
                        "schema['polygon'] must be defined with type 'string'"
                    )
        return self


def load_config_and_validate(config_filename: str) -> DatasetConfig:
    """Load and validate a dataset configuration.

    This function loads a dataset configuration file and validates it against the
    `DatasetConfig` Pydantic model. If `config_filename` is a full path to an
    existing file, it is used directly. Otherwise, it is assumed to be a filename
    located in the default config directory:
    `../config/dataset/` relative to this file.

    Args:
        config_filename: The name of the configuration file or a full path to one.

    Returns:
        A validated `DatasetConfig` instance.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        pydantic.ValidationError: If the configuration is invalid.
    """
    config_path = resolve_dataset_config_path(config_filename)
    dataset_config = load_dataset_config(str(config_path))
    return DatasetConfig.model_validate(dataset_config)


def json_update(base: dict, updates: dict) -> dict:
    """Recursively update nested dictionaries."""
    for k, v in updates.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = json_update(base[k], v)
        else:
            base[k] = v
    return base


def collect_files(
    path_cfg: PathConfig, suffix: str, exclude: Optional[str], bucket_raw: Optional[str]
) -> List[str]:
    """Collect files from an S3 bucket using suffix and optional regex filtering.

    Args:
        path_cfg: Configuration object including the S3 URI and optional regex filters.
        suffix: File suffix to filter by, e.g., '.nc'. Set to None to disable suffix filtering.
        exclude: Optional regex string to exclude files.
        bucket_raw: Required if `path_cfg.s3_uri` is not a full S3 URI.

    Returns:
        List of matching file keys (paths) as strings.
    """
    s3_uri = path_cfg.s3_uri

    if s3_uri.startswith("s3://"):
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
    else:
        if not bucket_raw:
            raise ValueError(
                "bucket_raw must be provided when s3_uri is not a full S3 URI."
            )
        bucket = bucket_raw
        prefix = s3_uri

    prefix = str(PurePosixPath(prefix))  # normalise path

    # matching_files = s3_ls(bucket, prefix, suffix=suffix, exclude=exclude)
    matching_files = s3_ls(bucket, prefix, suffix=None, exclude=exclude)

    for pattern in path_cfg.filter or []:
        logger.info(f"Filtering files with regex pattern: {pattern}")
        regex = re.compile(pattern)
        matching_files = [f for f in matching_files if regex.search(f)]
        if matching_files == []:
            raise ValueError(
                f"No files matching {pattern} under {s3_uri}. Modify regexp filter or path in configuration file. Abort"
            )

        logger.info(f"Matched {len(matching_files)} files")

    return matching_files


def join_s3_uri(base_uri: str, *parts: str) -> str:
    if base_uri.startswith("s3://"):
        parsed = urlparse(base_uri)
        bucket = parsed.netloc
        key = PurePosixPath(parsed.path.lstrip("/"), *parts)
        return f"s3://{bucket}/{key}"
    else:
        return str(PurePosixPath(base_uri, *parts))


def resolve_dataset_config_path(config_arg: str) -> str:
    """Resolve dataset config path from a given argument.

    If `config_arg` is an existing file path, return it as-is.
    Otherwise, treat it as a base name (with or without `.json`)
    and look it up in `aodn_cloud_optimised.config.dataset`.

    Args:
        config_arg: The CLI config argument, either a path or a name.

    Returns:
        The path to the config file as a string.

    Raises:
        FileNotFoundError: If the resolved config file does not exist.
    """
    config_path = Path(config_arg)
    if not config_path.is_file():
        # Fall back to the default relative config path
        # Ensure .json extension if missing
        if not config_arg.endswith(".json"):
            config_arg += ".json"

        config_path = Path(__file__).parents[1] / "config" / "dataset" / config_arg

    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    return str(config_path)


def main():
    parser = argparse.ArgumentParser(
        description="Run cloud-optimised creation using config."
    )
    parser.add_argument(
        "--config", required=False, help="JSON filename in config/dataset/"
    )
    parser.add_argument(
        "--json-overwrite",
        type=str,
        help='JSON string to override config fields. Example:  \'{"run_settings": {"cluster": {"mode": null}, "raise_error": true}}\' ',
    )

    args = parser.parse_args()

    try:
        config = load_config_and_validate(args.config)
    except ValidationError as e:
        print(f"❌ Validation error in config file:\n{e}")
        sys.exit(1)

    overwrite_dict: dict[str, Any] = {}
    if args.json_overwrite:
        overwrite = args.json_overwrite
        if overwrite.startswith("@"):
            with open(overwrite[1:], "r") as f:
                overwrite_dict = json.load(f)
        else:
            overwrite_dict = json.loads(overwrite)

        config_updated = json_update(config.model_dump(), overwrite_dict)
        config = DatasetConfig.model_validate(config_updated)

    bucket_raw = (
        config.run_settings.bucket_raw_default_name
        or load_variable_from_config("BUCKET_RAW_DEFAULT")
    )
    bucket_optimised = (
        config.run_settings.optimised_bucket_name
        or load_variable_from_config("BUCKET_OPTIMISED_DEFAULT")
    )
    root_prefix = (
        config.run_settings.root_prefix_cloud_optimised_path
        or load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH")
    )

    dataset_config_path = resolve_dataset_config_path(args.config)
    dataset_config = load_dataset_config(
        dataset_config_path
    )  # not using config.model_dump() as it retains only the validated objects.

    # If restart_every_path is True, run one cloud_optimised_creation per path/year
    if config.run_settings.cluster.restart_every_path:
        clear_flag_added = False
        for path_cfg in config.run_settings.paths:
            year_values = path_cfg.year_range or [None]
            if year_values == [None]:
                matching_files = collect_files(
                    path_cfg,
                    suffix=config.run_settings.suffix,
                    exclude=config.run_settings.exclude,
                    bucket_raw=bucket_raw,
                )

                if not matching_files:
                    warnings.warn(f"No files matched for path {path_cfg.s3_uri}")
                    continue

                result = cloud_optimised_creation(
                    matching_files,
                    dataset_config=dataset_config,
                    handler_class=None,
                    clear_existing_data=(
                        config.run_settings.clear_existing_data and not clear_flag_added
                    ),
                    force_previous_parquet_deletion=config.run_settings.force_previous_parquet_deletion,
                    cluster_mode=config.run_settings.cluster.mode
                    or clusterLib.ClusterMode.NONE.value,
                    optimised_bucket_name=bucket_optimised,
                    root_prefix_cloud_optimised_path=root_prefix,
                    raise_error=config.run_settings.raise_error,
                )
                if config.run_settings.clear_existing_data:
                    clear_flag_added = True
                if not result and config.run_settings.raise_error:
                    sys.exit(1)

            else:
                for year in year_values:
                    s3_uri = (
                        join_s3_uri(path_cfg.s3_uri, str(year))
                        if year
                        else path_cfg.s3_uri
                    )

                    path_cfg_year = path_cfg.model_copy()
                    path_cfg_year.s3_uri = s3_uri

                    matching_files = collect_files(
                        path_cfg_year,
                        suffix=config.run_settings.suffix,
                        exclude=config.run_settings.exclude,
                        bucket_raw=bucket_raw,
                    )

                    if not matching_files:
                        warnings.warn(f"No files matched for path {s3_uri}")
                        continue

                    result = cloud_optimised_creation(
                        matching_files,
                        dataset_config=dataset_config,
                        handler_class=None,
                        clear_existing_data=(
                            config.run_settings.clear_existing_data
                            and not clear_flag_added
                        ),
                        force_previous_parquet_deletion=config.run_settings.force_previous_parquet_deletion,
                        cluster_mode=config.run_settings.cluster.mode
                        or clusterLib.ClusterMode.NONE.value,
                        optimised_bucket_name=bucket_optimised,
                        root_prefix_cloud_optimised_path=root_prefix,
                        raise_error=config.run_settings.raise_error,
                    )
                    if config.run_settings.clear_existing_data:
                        clear_flag_added = True
                    if not result and config.run_settings.raise_error:
                        sys.exit(1)

    else:
        # Collect all files from all paths / years and run once
        all_files = []
        for path_cfg in config.run_settings.paths:
            year_values = path_cfg.year_range or [None]

            if year_values == [None]:
                files_found = collect_files(
                    path_cfg,
                    suffix=config.run_settings.suffix,
                    exclude=config.run_settings.exclude,
                    bucket_raw=bucket_raw,
                )
                all_files.extend(files_found)

            else:
                for year in year_values:
                    s3_uri = (
                        join_s3_uri(path_cfg.s3_uri, str(year))
                        if year
                        else path_cfg.s3_uri
                    )

                    path_cfg_year = path_cfg.model_copy()
                    path_cfg_year.s3_uri = s3_uri
                    files_found = collect_files(
                        path_cfg_year,
                        suffix=config.run_settings.suffix,
                        exclude=config.run_settings.exclude,
                        bucket_raw=bucket_raw,
                    )
                    all_files.extend(files_found)

        all_files = list(dict.fromkeys(all_files))  # Deduplicate

        if not all_files:
            warnings.warn("No files matched any path/filter combination.")
            sys.exit(1)

        result = cloud_optimised_creation(
            all_files,
            dataset_config=dataset_config,
            handler_class=None,
            clear_existing_data=config.run_settings.clear_existing_data,
            force_previous_parquet_deletion=config.run_settings.force_previous_parquet_deletion,
            cluster_mode=config.run_settings.cluster.mode
            or clusterLib.ClusterMode.NONE.value,
            optimised_bucket_name=bucket_optimised,
            root_prefix_cloud_optimised_path=root_prefix,
            raise_error=config.run_settings.raise_error,
        )
        if not result and config.run_settings.raise_error:
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
