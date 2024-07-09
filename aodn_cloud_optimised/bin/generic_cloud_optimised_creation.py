#!/usr/bin/env python3
"""
Script to process S3 paths and create cloud-optimized datasets.

This script allows you to specify S3 paths and various options to process
datasets and create cloud-optimised versions. It provides filtering options
and supports different cluster modes.

Usage Examples:
  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' \
  --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json' \
  --clear-existing-data --cluster-mode 'remote'

  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' \
  --dataset-config 'anmn_ctd_ts_fv01.json'

  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' \
  --dataset-config 'radar_TurquoiseCoast_velocity_hourly_average_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'

Arguments:
  --paths: List of S3 paths to process. Example: 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'
  --filters: Optional filter strings to apply on the S3 paths. Example: '_hourly-timeseries_' 'FV02'
  --suffix: Optional suffix used by s3_ls to filter S3 objects. Default is .nc. Example: '.nc'
  --dataset-config: Path to the dataset config JSON file. Example: 'mooring_hourly_timeseries_delayed_qc.json'
  --clear-existing-data: Flag to clear existing data. Default is False.
  --force-previous-parquet-deletion: Flag to force the search of previous equivalent parquet file created. Much slower. Default is False. Only for Parquet processing.
  --cluster-mode: Cluster mode to use. Options: 'local' or 'remote'. Default is 'local'.
  --optimised-bucket-name: Bucket name where cloud optimised object will be created. Default is the value of BUCKET_OPTIMISED_DEFAULT from the config.
  --root-prefix-cloud-optimised-path: Prefix value for the root location of the cloud optimised objects. Default is the value of ROOT_PREFIX_CLOUD_OPTIMISED_PATH from the config.
  --bucket-raw: Bucket name where input object files will be searched for. Default is the value of BUCKET_RAW_DEFAULT from the config.

"""

import argparse
from importlib.resources import files

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_variable_from_config,
    load_dataset_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    parser = argparse.ArgumentParser(
        description="Process S3 paths and create cloud-optimized datasets.",
        epilog="Examples:\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' --dataset-config 'anmn_ctd_ts_fv01.json'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' --dataset-config 'radar_TurquoiseCoast_velocity_hourly_average_delayed_qc.json' --clear-existing-data --cluster-mode 'remote'\n",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--paths",
        nargs="+",
        required=True,
        help="List of S3 paths to process. Example: 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'",
    )
    parser.add_argument(
        "--filters",
        nargs="*",
        default=[],
        help="Optional filter strings to apply on the S3 paths. Example: '_hourly-timeseries_' 'FV02'",
    )
    parser.add_argument(
        "--suffix",
        default=".nc",
        help="Optional suffix used by s3_ls to filter S3 objects. Default is .nc. Example: '.nc'",
    )
    parser.add_argument(
        "--dataset-config",
        required=True,
        help="Path to the dataset config JSON file. Example: 'mooring_hourly_timeseries_delayed_qc.json'",
    )
    parser.add_argument(
        "--clear-existing-data",
        action="store_true",
        help="Flag to clear existing data. Default is False.",
    )
    parser.add_argument(
        "--force-previous-parquet-deletion",
        action="store_true",
        help="Flag to force the search of previous equivalent parquet file created. Much slower. Default is False. "
        "Only for Parquet processing.",
    )
    parser.add_argument(
        "--cluster-mode",
        default="local",
        choices=["local", "remote"],
        help="Cluster mode to use. Options: 'local' or 'remote'. Default is 'local'.",
    )

    parser.add_argument(
        "--optimised-bucket-name",
        default=load_variable_from_config("BUCKET_OPTIMISED_DEFAULT"),
        help=f"Bucket name where cloud optimised object will be created. "
        f"Default is '{load_variable_from_config('BUCKET_OPTIMISED_DEFAULT')}'",
    )

    parser.add_argument(
        "--root-prefix-cloud-optimised-path",
        default=load_variable_from_config("ROOT_PREFIX_CLOUD_OPTIMISED_PATH"),
        help=f"Prefix value for the root location of the cloud optimised objects, such as "
        f"s3://optimised-bucket-name/root-prefix-cloud-optimised-path/... "
        f"Default is '{load_variable_from_config('ROOT_PREFIX_CLOUD_OPTIMISED_PATH')}'",
    )

    parser.add_argument(
        "--bucket-raw",
        default=load_variable_from_config("BUCKET_RAW_DEFAULT"),
        help=f"Bucket name where input object files will be searched for. "
        f"Default is '{load_variable_from_config('BUCKET_RAW_DEFAULT')}'",
    )

    args = parser.parse_args()

    bucket_raw_value = args.bucket_raw

    # Gather S3 paths
    nc_obj_ls = []
    for path in args.paths:
        nc_obj_ls += s3_ls(bucket_raw_value, path, suffix=args.suffix)

    # Apply filters
    for filter_str in args.filters:
        nc_obj_ls = [s for s in nc_obj_ls if filter_str in s]

    if not nc_obj_ls:
        raise ValueError("No files found matching the specified criteria.")

    # Load dataset config
    dataset_config_path = args.dataset_config
    dataset_config = load_dataset_config(
        str(files("aodn_cloud_optimised.config.dataset").joinpath(dataset_config_path))
    )

    # Call cloud_optimised_creation
    cloud_optimised_creation(
        nc_obj_ls,
        dataset_config=dataset_config,
        handler_class=None,
        clear_existing_data=args.clear_existing_data,
        force_previous_parquet_deletion=args.force_previous_parquet_deletion,
        cluster_mode=args.cluster_mode,
        optimised_bucket_name=args.optimised_bucket_name,
        root_prefix_cloud_optimised_path=args.root_prefix_cloud_optimised_path,
    )


if __name__ == "__main__":
    main()
