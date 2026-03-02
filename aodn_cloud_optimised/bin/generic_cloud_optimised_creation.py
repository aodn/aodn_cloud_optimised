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
import re
import sys
import warnings
from pathlib import Path, PurePosixPath
from typing import Any, List, Optional
from urllib.parse import urlparse

from pydantic import ValidationError

from aodn_cloud_optimised.lib import clusterLib
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.s3Tools import boto3_from_opts_dict, s3_ls

logger = logging.getLogger(__name__)
from aodn_cloud_optimised.bin.config import DatasetConfig, PathConfig


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


def collect_files(
    path_cfg: PathConfig,
    suffix: Optional[str],
    exclude: Optional[str],
    bucket_raw: Optional[str],
    s3_client_opts: Optional[dict] = None,
) -> List[str]:
    """Collect dataset paths from S3 based on dataset type.

    Supports:
      - 'files': lists and filters regular files (e.g., NetCDF, CSV)
      - 'parquet': handles both single Parquet files and Hive-partitioned datasets
      - 'zarr': returns the Zarr store path directly

    Args:
        path_cfg: Configuration object including type, S3 URI, and optional regex filters.
        suffix: File suffix to filter by, e.g., '.nc'. Set to None to disable suffix filtering.
        exclude: Optional regex string to exclude files.
        bucket_raw: Required if `path_cfg.s3_uri` is not a full S3 URI.
        s3_client_opts: Optional dict with boto3 S3 client options.

    Returns:
        List of dataset paths (files or root URIs) as strings.
    """
    dataset_type = getattr(path_cfg, "type", "files")  # default value
    s3_uri = path_cfg.s3_uri.rstrip("/")

    # ---------------------------------------------------------------------
    # Handle 'file' collection (NetCDF, CSV)
    # ---------------------------------------------------------------------
    if dataset_type == "files":
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

        matching_files = s3_ls(
            bucket,
            prefix,
            suffix=None,
            exclude=exclude,
            s3_client_opts=s3_client_opts,
        )

        for pattern in path_cfg.filter or []:
            logger.info(f"Filtering files with regex pattern: {pattern}")
            regex = re.compile(pattern)
            matching_files = [f for f in matching_files if regex.search(f)]
            if not matching_files:
                raise ValueError(
                    f"No files matching {pattern} under {s3_uri}. Modify regexp filter or path in configuration file. Abort"
                )

            logger.info(f"Matched {len(matching_files)} files")

        return matching_files

    # ---------------------------------------------------------------------
    # Handle 'parquet' (single Parquet file or Hive-partitioned dataset)
    # ---------------------------------------------------------------------
    elif dataset_type == "parquet":
        # No filters
        return [s3_uri]

    # ---------------------------------------------------------------------
    # Handle 'zarr' (Zarr store)
    # ---------------------------------------------------------------------
    elif dataset_type == "zarr":
        raise ValueError("zarr store as an input dataset is not yet implemented")
        # return [s3_uri]

    # Unsupported type
    else:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")


def json_update(base: dict, updates: dict) -> dict:
    """Recursively update nested dictionaries."""
    for k, v in updates.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = json_update(base[k], v)
        else:
            base[k] = v
    return base


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
        "-c", "--config", required=False, help="JSON filename in config/dataset/"
    )
    parser.add_argument(
        "-o",
        "--json-overwrite",
        type=str,
        help='JSON string to override config fields. Example:  \'{"run_settings": {"cluster": {"mode": null}, "raise_error": true}}\' ',
    )

    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Use integration testing bucket instead of the default optimised bucket.",
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

    # Set bucket values
    s3_bucket_opts = config.run_settings.s3_bucket_opts or {}

    bucket_raw = (
        s3_bucket_opts.get("input_data", {}).get("bucket")
        or config.run_settings.bucket_raw_default_name
        or load_variable_from_config("BUCKET_RAW_DEFAULT")
    )

    bucket_optimised = (
        s3_bucket_opts.get("output_data", {}).get("bucket")
        or config.run_settings.optimised_bucket_name
        or load_variable_from_config("BUCKET_OPTIMISED_DEFAULT")
    )

    root_prefix = (
        config.run_settings.root_prefix_cloud_optimised_path
        or load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH")
    )

    # Override for test mode
    if args.test:
        bucket_optimised = load_variable_from_config(
            "BUCKET_INTEGRATION_TESTING_OPTIMISED_DEFAULT"
        )
        root_prefix = load_variable_from_config(
            "ROOT_PREFIX_CLOUD_OPTIMISED_INTEGRATION_TESTING_PATH"
        )

    s3_fs_common_opts = config.run_settings.s3_fs_common_opts
    s3_client_opts = boto3_from_opts_dict(s3_fs_common_opts)

    # overwrite the above with specific options if set for the input bucket
    if (
        config.run_settings.s3_bucket_opts
        and "input_data" in config.run_settings.s3_bucket_opts
        and config.run_settings.s3_bucket_opts["input_data"].get("s3_fs_opts")
    ):
        s3_fs_opts_input = config.run_settings.s3_bucket_opts["input_data"][
            "s3_fs_opts"
        ]
        s3_client_opts = boto3_from_opts_dict(s3_fs_opts_input)

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
                    s3_client_opts=s3_client_opts,
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
                        s3_client_opts=s3_client_opts,
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
                    s3_client_opts=s3_client_opts,
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
                        s3_client_opts=s3_client_opts,
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
