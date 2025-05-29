#!/usr/bin/env python3
"""
Script to process S3 paths and create cloud-optimised datasets.

This script allows you to specify S3 paths and various options to process
datasets and create cloud-optimised versions. It provides filtering options
and supports different cluster modes.

Usage Examples:
  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'
  --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json'
  --clear-existing-data --cluster-mode 'coiled'

  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD'
  --dataset-config 'mooring_ctd_delayed_qc.json'

  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024'
  --dataset-config 'radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json' --clear-existing-data --cluster-mode 'coiled'

Arguments:
  --paths: List of S3 paths to process. Example: 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'
  --filters: Optional filter strings to apply on the S3 paths. Example: '_hourly-timeseries_' 'FV02'
  --suffix: Optional suffix used by s3_ls to filter S3 objects. Default is .nc. Example: '.nc'
  --dataset-config: Path to the dataset config JSON file. Example: 'mooring_hourly_timeseries_delayed_qc.json'
  --clear-existing-data: Flag to clear existing data. Default is False.
  --force-previous-parquet-deletion: Flag to force the search of previous equivalent parquet file created. Much slower.
                                     Default is False. Only for Parquet processing.
  --cluster-mode: Cluster mode to use. Options: 'local' or 'coiled'. Default is 'local'.
  --optimised-bucket-name: Bucket name where cloud optimised object will be created. Default is the value of
                           BUCKET_OPTIMISED_DEFAULT from the config.
  --root-prefix-cloud-optimised-path: Prefix value for the root location of the cloud optimised objects. Default is the
                                      value of ROOT_PREFIX_CLOUD_OPTIMISED_PATH from the config.
  --bucket-raw: Bucket name where input object files will be searched for. Default is the value of BUCKET_RAW_DEFAULT
                from the config.

"""

import argparse
import sys
import warnings
from importlib.resources import files
import re

from aodn_cloud_optimised.lib import clusterLib
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    parser = argparse.ArgumentParser(
        description="Process S3 paths and create cloud-optimized datasets.",
        epilog="Examples:\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' --filters '_hourly-timeseries_' 'FV02' --dataset-config 'mooring_hourly_timeseries_delayed_qc.json' --clear-existing-data --cluster-mode 'coiled'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' --dataset-config 'mooring_ctd_delayed_qc.json'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' --dataset-config 'radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json' --clear-existing-data --cluster-mode 'coiled'\n",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--paths",
        nargs="+",
        required=True,
        help="List of S3 paths to process. Example: 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA'",
    )
    parser.add_argument(
        "--filter",
        nargs="*",
        default=None,
        help="Optional list of filters, one per path, joined with ':'. "
        "Example: '_hourly-timeseries_:FV02' '_daily-timeseries_'",
    )

    parser.add_argument(
        "--suffix",
        default=".nc",
        help="Optional suffix used by s3_ls to filter S3 objects. Default is .nc. Example: '.nc'",
    )

    parser.add_argument(
        "--exclude",
        default=None,
        help="Optional string to exclude files listed by s3_ls to filter S3 objects. '_FV01_'",
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

    cluster_options = [mode.value for mode in clusterLib.ClusterMode]
    parser.add_argument(
        "--cluster-mode",
        default=clusterLib.ClusterMode.NONE.value,
        choices=cluster_options,
        help=f"Cluster mode to use. Options: {cluster_options}. Default is None.",
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

    parser.add_argument(
        "--raise-error",
        action="store_true",
        help="Flag to exit the code on the first error. Default is False.",
    )
    args = parser.parse_args()

    if args.filter and len(args.filter) != len(args.paths):
        parser.error(
            "Number of --filters must match number of --paths or be omitted entirely."
        )
    nc_obj_ls = []

    for i, path in enumerate(args.paths):
        # Get object list for this path
        objs = s3_ls(
            args.bucket_raw,
            path,
            # suffix and exclude removed since no longer used
        )

        # Apply path-specific regex filters if provided
        if args.filter:
            regex_pattern = args.filter[i]
            regex = re.compile(regex_pattern)
            objs = [s for s in objs if regex.search(s)]

    nc_obj_ls.extend(objs)

    # Deduplicate
    nc_obj_ls = list(dict.fromkeys(nc_obj_ls))

    if not nc_obj_ls:
        warnings.warn("No files found matching the specified criteria.")
        return False

    # Load dataset config
    dataset_config_path = args.dataset_config
    dataset_config = load_dataset_config(
        str(files("aodn_cloud_optimised.config.dataset").joinpath(dataset_config_path))
    )

    # Call cloud_optimised_creation
    res = cloud_optimised_creation(
        nc_obj_ls,
        dataset_config=dataset_config,
        handler_class=None,
        clear_existing_data=args.clear_existing_data,
        force_previous_parquet_deletion=args.force_previous_parquet_deletion,
        cluster_mode=args.cluster_mode,
        optimised_bucket_name=args.optimised_bucket_name,
        root_prefix_cloud_optimised_path=args.root_prefix_cloud_optimised_path,
        raise_error=args.raise_error,
    )

    sys.exit(0 if res else 1)
