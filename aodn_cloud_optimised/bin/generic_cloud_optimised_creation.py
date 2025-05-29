#!/usr/bin/env python3

"""
Runner to generate cloud-optimised datasets from S3 files, based on a dataset config JSON.

Example usage:
  python cloud_optimised_runner.py --config mooring_hourly_timeseries_delayed_qc.json
"""

import argparse
import re
import sys
import warnings
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import List, Optional

import pydantic
from pydantic import BaseModel, Field, field_validator

from aodn_cloud_optimised.lib import clusterLib
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


# ---------- Pydantic schema for validating the config ----------
class ClusterConfig(BaseModel):
    mode: Optional[str] = None
    restart_every_path: bool = False


class PathConfig(BaseModel):
    s3_uri: str
    filter: Optional[List[str]] = Field(default_factory=list)
    year_range: Optional[List[int]] = None

    @field_validator("year_range")
    def validate_year_range(cls, v: Optional[List[int]]) -> Optional[List[int]]:
        if v is None or len(v) == 0:
            return None  # No year filtering

        if not (1 <= len(v) <= 2):
            raise ValueError("year_range must contain 1 or 2 integers")

        # If one year, return as single-item list
        if len(v) == 1:
            return v

        # If two years, return inclusive range list
        start, end = v
        if start > end:
            raise ValueError("year_range start year must be <= end year")

        return list(range(start, end + 1))

    @field_validator("filter")
    def validate_regex(cls, v):
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex: {pattern} ({e})")
        return v


class RunSettings(BaseModel):
    paths: List[PathConfig]
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)
    clear_existing_data: bool = False
    force_previous_parquet_deletion: bool = False
    raise_error: bool = False
    suffix: str = ".nc"
    exclude: Optional[str] = None


class DatasetConfig(BaseModel):
    dataset_name: str
    run_settings: RunSettings


# ---------- Main logic ----------
def load_config(config_filename: str) -> DatasetConfig:
    config_path = Path(__file__).parents[1] / "config" / "dataset" / config_filename
    with config_path.open() as f:
        raw = f.read()
    return DatasetConfig.model_validate_json(raw)


def collect_files(
    path_cfg: PathConfig, suffix: str, exclude: Optional[str], bucket_raw: str
) -> List[str]:
    matching_files = s3_ls(bucket_raw, path_cfg.s3_uri, suffix=suffix, exclude=exclude)

    for pattern in path_cfg.filter or []:
        regex = re.compile(pattern)
        matching_files = [f for f in matching_files if regex.search(f)]

    return matching_files


def main():
    parser = argparse.ArgumentParser(
        description="Run cloud-optimised creation using config."
    )
    parser.add_argument(
        "--config", required=True, help="JSON filename in config/dataset/"
    )
    args = parser.parse_args()

    try:
        config = load_config(f"{args.config}.json")
    except pydantic.ValidationError as e:
        print(f"❌ Validation error in config file:\n{e}")
        sys.exit(1)

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
                    if year:
                        s3_uri = str(PurePosixPath(path_cfg.s3_uri) / str(year))
                    else:
                        s3_uri = path_cfg.s3_uri

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
                    if year:
                        s3_uri = str(PurePosixPath(path_cfg.s3_uri) / str(year))
                    else:
                        s3_uri = path_cfg.s3_uri

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
