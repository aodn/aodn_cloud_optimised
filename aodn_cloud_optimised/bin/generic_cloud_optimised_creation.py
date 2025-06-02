#!/usr/bin/env python3

"""
Runner to generate cloud-optimised datasets from S3 files, based on a dataset config JSON.

Example usage:
  generic_cloud_optimised_creation --config mooring_hourly_timeseries_delayed_qc
  generic_cloud_optimised_creation --config satellite_chlorophylla_gsm_1day_aqua --json-overwrite '{"run_settings": {"cluster": {"mode": null}, "raise_error": true}}'
"""

import argparse
import json
import re
import sys
import warnings
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, ValidationError, field_validator, StrictBool

from aodn_cloud_optimised.lib import clusterLib
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


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


class DatasetConfig(BaseModel):
    """Dataset processing configuration.

    Attributes:
        dataset_name: Dataset identifier.
        run_settings: Processing run settings.
    """

    dataset_name: str
    run_settings: RunSettings


# ---------- Main logic ----------
def load_config(config_filename: str) -> DatasetConfig:
    config_path = Path(__file__).parents[1] / "config" / "dataset" / config_filename
    with config_path.open() as f:
        raw = f.read()
    return DatasetConfig.model_validate_json(raw)


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

    matching_files = s3_ls(bucket, prefix, suffix=suffix, exclude=exclude)

    for pattern in path_cfg.filter or []:
        regex = re.compile(pattern)
        matching_files = [f for f in matching_files if regex.search(f)]

    return matching_files


def join_s3_uri(base_uri: str, *parts: str) -> str:
    if base_uri.startswith("s3://"):
        parsed = urlparse(base_uri)
        bucket = parsed.netloc
        key = PurePosixPath(parsed.path.lstrip("/"), *parts)
        return f"s3://{bucket}/{key}"
    else:
        return str(PurePosixPath(base_uri, *parts))


def main():
    parser = argparse.ArgumentParser(
        description="Run cloud-optimised creation using config."
    )
    parser.add_argument(
        "--config", required=True, help="JSON filename in config/dataset/"
    )
    parser.add_argument(
        "--json-overwrite",
        type=str,
        help='JSON string to override config fields. Example:  \'{"run_settings": {"cluster": {"mode": null}, "raise_error": true}}\' ',
    )
    args = parser.parse_args()

    try:
        config = load_config(f"{args.config}.json")
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

    bucket_raw = load_variable_from_config("BUCKET_RAW_DEFAULT")
    bucket_optimised = load_variable_from_config("BUCKET_OPTIMISED_DEFAULT")
    root_prefix = load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH")
    dataset_config_path = str(
        files("aodn_cloud_optimised.config.dataset").joinpath(f"{args.config}.json")
    )
    dataset_config = load_dataset_config(dataset_config_path)

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
