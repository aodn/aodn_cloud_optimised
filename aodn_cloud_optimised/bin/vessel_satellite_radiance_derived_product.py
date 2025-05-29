#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path
from typing import List, Optional
import re

from pydantic import BaseModel, Field, ValidationError, field_validator


class ClusterConfig(BaseModel):
    mode: Optional[str] = None
    restart_every_path: bool = False


class PathConfig(BaseModel):
    s3_uri: str
    filter: Optional[List[str]] = Field(default_factory=list)
    year_range: list[int] | None = None

    @field_validator("year_range")
    def validate_year_range(cls, v):
        # Allow None or empty list
        if v is None or len(v) == 0:
            return v
        if not (1 <= len(v) <= 2):
            raise ValueError("year_range must contain 1 or 2 years if specified")
        return v

    @field_validator("filter")
    def validate_filters_are_regex(cls, v):
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {pattern} ({e})")
        return v


class RunSettings(BaseModel):
    paths: List[PathConfig]
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)
    clear_existing_data: bool = False


class DatasetConfig(BaseModel):
    dataset_name: str
    run_settings: RunSettings


def load_config(config_path: Path) -> DatasetConfig:
    with open(config_path, "r") as f:
        raw = json.load(f)
    return DatasetConfig(**raw)


def run_command(args: List[str]):
    print(f"Running command: {' '.join(args)}")
    subprocess.run(args, check=True)


def main():
    script_path = Path(__file__)
    config_filename = script_path.stem + ".json"
    repo_root = script_path.parents[1]
    config_path = repo_root / "config" / "dataset" / config_filename

    try:
        config = load_config(config_path)
    except ValidationError as e:
        print("Configuration validation error:")
        print(e)
        return

    dataset_config_file = config_path.name
    proc_config = config.run_settings
    clear_flag_added = False

    if proc_config.cluster.restart_every_path:
        for path_config in proc_config.paths:
            year_values = path_config.year_range or [None]

            for year in year_values:
                s3_uri = (
                    os.path.join(path_config.s3_uri, str(year))
                    if year
                    else path_config.s3_uri
                )

                command = [
                    "generic_cloud_optimised_creation",
                    "--dataset-config",
                    dataset_config_file,
                    "--path",
                    s3_uri,
                ]

                if path_config.filter:
                    for filter_val in path_config.filter:
                        command += ["--filter", filter_val]

                if proc_config.clear_existing_data and not clear_flag_added:
                    command.append("--clear-existing-data")
                    clear_flag_added = True

                if proc_config.cluster.mode:
                    command += ["--cluster-mode", proc_config.cluster.mode]

                run_command(command)
    else:
        # Run one command with all paths and filters combined
        command = [
            "generic_cloud_optimised_creation",
            "--dataset-config",
            dataset_config_file,
        ]

        if proc_config.clear_existing_data:
            command.append("--clear-existing-data")

        if proc_config.cluster.mode:
            command += ["--cluster-mode", proc_config.cluster.mode]

        for path_config in proc_config.paths:
            year_values = path_config.year_range or [None]

            for year in year_values:
                s3_uri = (
                    os.path.join(path_config.s3_uri, str(year))
                    if year
                    else path_config.s3_uri
                )
                command += ["--path", s3_uri]

                if path_config.filter:
                    for filter_val in path_config.filter:
                        command += ["--filter", filter_val]

        run_command(command)


if __name__ == "__main__":
    main()
