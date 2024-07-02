#!/usr/bin/env python3
import argparse
import importlib.resources
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
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/PA' --filters '_hourly-timeseries_' 'FV02' --dataset-config 'anmn_hourly_timeseries.json' --clear-existing-data --cluster-mode 'remote'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ANMN/NSW' 'IMOS/ANMN/QLD' --dataset-config 'anmn_ctd_ts_fv01.json'\n"
        "  generic_cloud_optimised_creation --paths 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2024' --dataset-config 'acorn_gridded_qc_turq.json' --clear-existing-data --cluster-mode 'remote'\n",
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
        default="",
        help="Optional suffix used by s3_ls to filter S3 objects. Example: '.nc'",
    )
    parser.add_argument(
        "--dataset-config",
        required=True,
        help="Path to the dataset config JSON file. Example: 'anmn_hourly_timeseries.json'",
    )
    parser.add_argument(
        "--clear-existing-data",
        action="store_true",
        help="Flag to clear existing data. Default is False.",
    )
    parser.add_argument(
        "--force-previous-parquet-deletion",
        action="store_true",
        help="Flag to force the search of previous equivalent parquet file created. Much slower. Default is False.",
    )
    parser.add_argument(
        "--cluster-mode",
        default="local",
        choices=["local", "remote"],
        help="Cluster mode to use. Options: 'local' or 'remote'. Default is 'local'.",
    )

    args = parser.parse_args()

    BUCKET_RAW_DEFAULT = load_variable_from_config("BUCKET_RAW_DEFAULT")

    # Gather S3 paths
    nc_obj_ls = []
    for path in args.paths:
        nc_obj_ls += s3_ls(BUCKET_RAW_DEFAULT, path, suffix=args.suffix)

    # Apply filters
    for filter_str in args.filters:
        nc_obj_ls = [s for s in nc_obj_ls if filter_str in s]

    # Load dataset config
    dataset_config_path = args.dataset_config
    dataset_config = load_dataset_config(
        str(
            importlib.resources.path(
                "aodn_cloud_optimised.config.dataset", dataset_config_path
            )
        )
    )

    # Call cloud_optimised_creation
    cloud_optimised_creation(
        nc_obj_ls,
        dataset_config=dataset_config,
        handler_class=None,
        clear_existing_data=args.clear_existing_data,
        force_previous_parquet_deletion=args.force_previous_parquet_deletion,
        cluster_mode=args.cluster_mode,
    )


if __name__ == "__main__":
    main()
