#!/usr/bin/env python3
"""
Script to generate a dataset configuration from a NetCDF file stored in S3.

This script performs the following tasks:

1. Generates a JSON schema from the NetCDF file located in the S3 bucket.
2. Reads and merges the validation schema template with the generated schema.
3. Populates the dataset configuration with additional metadata including dataset name, metadata UUID, logger name,
   cloud-optimised format, cluster options, and batch size.
4. Writes the dataset configuration to the module path as a JSON file. READY TO BE ADDED TO GITHUB
5. Optionally, fills up the AWS registry with Geonetwork metadata if a UUID is provided.

Usage:
    cloud_optimised_create_dataset_config -f <NetCDF file object key> -c <cloud optimised format> -d <dataset name> [-b <S3 bucket name>] [-u <Geonetwork Metadata UUID>]

Arguments:
    -f, --file: Object key for the NetCDF file (required).
    -b, --bucket: S3 bucket name (optional, defaults to the value from config).
    -c, --cloud-format: Cloud optimised format, either "zarr" or "parquet" (required).
    -u, --uuid: Geonetwork Metadata UUID (optional).
    -d, --dataset-name: Name of the dataset (required, no spaces or underscores).

Example:
    cloud_optimised_create_dataset_config \
        -f IMOS/SOOP/SOOP-TRV/VMQ9273_Solander/By_Cruise/Cruise_START-20100225T073727Z_END-20100225T131607Z/chlorophyll/IMOS_SOOP-TRV_B_20100225T073727Z_VMQ9273_FV01_END-20100225T131607Z.nc \
        -d vessel_trv_realtime_qc \
        -u 8af21108-c535-43bf-8dab-c1f45a26088c \
        -c parquet
"""

import os
import argparse
from aodn_cloud_optimised.lib.config import load_variable_from_config, merge_dicts
from aodn_cloud_optimised.lib.schema import generate_json_schema_from_s3_netcdf
from aodn_cloud_optimised.bin.create_aws_registry_dataset import (
    populate_dataset_config_with_geonetwork_metadata,
)

import json
import importlib.resources
import uuid
from collections import OrderedDict
import importlib.util

import re


def validate_dataset_name(value):
    if not isinstance(value, str):
        raise argparse.ArgumentTypeError("Dataset name must be a string.")
    if " " in value:
        raise argparse.ArgumentTypeError("Dataset name must not contain spaces.")
    if "_" not in value:
        raise argparse.ArgumentTypeError(
            "Dataset name must contain at least one underscore."
        )
    return value


def validate_uuid(value):
    try:
        uuid.UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid UUID format.")
    return value


def generate_template_value(schema):
    schema_type = schema["type"]

    if schema_type == "string":
        return "FILL UP MANUALLY - CHECK DOCUMENTATION"
    elif schema_type == "integer":
        return 0
    elif schema_type == "boolean":
        return False
    elif schema_type == "array":
        return [generate_template_value(schema["items"])]
    elif schema_type == "object":
        return OrderedDict(
            (key, generate_template_value(subschema))
            for key, subschema in schema.get("properties", {}).items()
        )

        # return {key: generate_template_value(subschema) for key, subschema in schema.get('properties', {}).items()}
    else:
        return None


def generate_template(schema):
    template = {}
    for key, subschema in schema["properties"].items():
        template[key] = generate_template_value(subschema)
    return template


def main():
    """
    Script to generate a dataset configuration from a NetCDF file stored in S3.

    This script performs the following tasks:

    1. Generates a JSON schema from the NetCDF file located in the S3 bucket.
    2. Reads and merges the validation schema template with the generated schema.
    3. Populates the dataset configuration with additional metadata including dataset name, metadata UUID, logger name,
       cloud-optimised format, cluster options, and batch size.
    4. Writes the dataset configuration to the module path as a JSON file. READY TO BE ADDED TO GITHUB
    5. Optionally, fills up the AWS registry with Geonetwork metadata if a UUID is provided.

    Usage:
        cloud_optimised_create_dataset_config -f <NetCDF file object key> -c <cloud optimised format> -d <dataset name> [-b <S3 bucket name>] [-u <Geonetwork Metadata UUID>]

    Arguments:
        -f, --file: Object key for the NetCDF file (required).
        -b, --bucket: S3 bucket name (optional, defaults to the value from config).
        -c, --cloud-format: Cloud optimised format, either "zarr" or "parquet" (required).
        -u, --uuid: Geonetwork Metadata UUID (optional).
        -d, --dataset-name: Name of the dataset (required, no spaces or underscores).

    Example:
        cloud_optimised_create_dataset_config \
            -f IMOS/SOOP/SOOP-TRV/VMQ9273_Solander/By_Cruise/Cruise_START-20100225T073727Z_END-20100225T131607Z/chlorophyll/IMOS_SOOP-TRV_B_20100225T073727Z_VMQ9273_FV01_END-20100225T131607Z.nc \
            -d vessel_trv_realtime_qc \
            -u 8af21108-c535-43bf-8dab-c1f45a26088c \
            -c parquet
    """
    # Load the default BUCKET_RAW_DEFAULT
    default_bucket = load_variable_from_config("BUCKET_RAW_DEFAULT")

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Generate JSON schema from S3 NetCDF file."
    )
    parser.add_argument(
        "-f", "--file", required=True, help="Object key for the NetCDF file."
    )
    parser.add_argument(
        "-b",
        "--bucket",
        required=False,
        default=default_bucket,
        help="S3 bucket name. Default is the value from config.",
    )
    parser.add_argument(
        "-c",
        "--cloud-format",
        required=True,
        choices=["zarr", "parquet"],
        help="Cloud optimised format",
    )
    parser.add_argument(
        "-u",
        "--uuid",
        required=False,
        type=validate_uuid,
        help="Geonetwork Metadata UUID",
    )
    parser.add_argument(
        "-d",
        "--dataset-name",
        required=True,
        type=validate_dataset_name,
        help="Name of the dataset (no spaces or underscores)",
    )

    # Parse arguments
    args = parser.parse_args()
    obj_key = args.file
    bucket = args.bucket

    # Construct the S3 file path
    nc_file = os.path.join("s3://", bucket, obj_key)

    # Generate JSON schema from the NetCDF file
    temp_file_path = generate_json_schema_from_s3_netcdf(nc_file)

    with open(temp_file_path, "r") as file:
        dataset_config_schema = json.load(file)
    os.remove(temp_file_path)

    dataset_config = {"schema": dataset_config_schema}

    # Define the path to the validation schema file
    json_validation_path = str(
        importlib.resources.files("aodn_cloud_optimised")
        .joinpath("config")
        .joinpath(f"schema_validation_{args.cloud_format}.json")
    )

    # Read the validation schema from the file
    with open(json_validation_path, "r") as f:
        validation_schema = json.load(f)

    # Generate the template based on the validation schema
    template = generate_template(validation_schema)

    dataset_config = merge_dicts(template, dataset_config)

    # default values
    dataset_config["dataset_name"] = args.dataset_name
    dataset_config["metadata_uuid"] = args.uuid
    dataset_config["logger_name"] = args.dataset_name
    dataset_config["cloud_optimised_format"] = args.cloud_format
    dataset_config["cluster_options"] = {
        "n_workers": [1, 20],
        "scheduler_vm_types": "t3.small",
        "worker_vm_types": "t3.medium",
        "allow_ingress_from": "me",
        "compute_purchase_option": "spot_with_fallback",
        "worker_options": {"nthreads": 4, "memory_limit": "8GB"},
    }
    dataset_config["batch_size"] = 5

    if args.cloud_format == "parquet":
        # default partition keys
        dataset_config["partition_keys"] = ["timestamp", "polygon"]

        dataset_config["schema"]["timestamp"] = {"type": "int64"}
        dataset_config["schema"]["polygon"] = {"type": "string"}
        dataset_config["schema"]["filename"] = {"type": "string"}

    module_name = "aodn_cloud_optimised"
    spec = importlib.util.find_spec(module_name)
    module_path = spec.submodule_search_locations[0]

    # write json config to module path
    with open(f"{module_path}/config/dataset/{args.dataset_name}.json", "w") as f:
        json.dump(dataset_config, f, indent=2)

    # fill up aws registry with GN3 uuid
    if args.uuid:
        populate_dataset_config_with_geonetwork_metadata(f"{args.dataset_name}.json")


if __name__ == "__main__":
    main()
