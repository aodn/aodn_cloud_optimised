#!/usr/bin/env python3

import argparse
import importlib
from typing import Optional

from pydantic import (
    ValidationError,
)

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
    load_config_and_validate,
    resolve_dataset_config_path,
)
from aodn_cloud_optimised.lib.common import list_dataset_config
from aodn_cloud_optimised.lib.CommonHandler import _get_generic_handler_class
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)


def main(
    json_files: Optional[list[str]] = None,
    optimised_bucket_name: Optional[str] = None,
    root_prefix_cloud_optimised_path: Optional[str] = None,
):
    """
    Validate and apply metadata updates to cloud-optimised datasets (Zarr or Parquet).

    This function validates each JSON configuration file, loads the corresponding dataset
    configuration, and applies metadata updates using the appropriate handler.

    Parameters
    ----------
    json_files : list[str] or None, optional
        Specific JSON files to process. If None, all config files in `config_dir` are used.
        example ["satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.json"]
    optimised_bucket_name : str or None, optional
        Bucket containing the cloud-optimised datasets. Uses the configured default
        when omitted.
    root_prefix_cloud_optimised_path : str or None, optional
        Root prefix containing the cloud-optimised datasets. Uses the configured
        default when omitted.

    Returns
    -------
    int
        0 if no files were found; otherwise, None is returned after processing.

    Steps
    -----
    1. List or receive the config JSON files.
    2. Validate each file against its schema.
    3. Load the dataset configuration.
    4. Identify the cloud-optimised format (`zarr` or `parquet`).
    5. Use the appropriate handler to update metadata.
    6. Log and continue on validation or processing errors.
    """
    if json_files is None:
        json_files = list_dataset_config()

    if not json_files:
        print(f"ℹ️ No JSON files")
        return 0

    if optimised_bucket_name is None:
        optimised_bucket_name = load_variable_from_config("BUCKET_OPTIMISED_DEFAULT")
    if root_prefix_cloud_optimised_path is None:
        root_prefix_cloud_optimised_path = load_variable_from_config(
            "ROOT_PREFIX_CLOUD_OPTIMISED_PATH"
        )

    for json_file in json_files:
        try:
            load_config_and_validate(json_file)
        except ValidationError as e:
            print(f"\n❌ Validation failed in: {json_file}")
            print("─" * 80)
            print(e)
            print("─" * 80)
            continue
        except Exception as e:
            print(f"\n❌ Error reading {json_file}: {e}")
            continue

        dataset_config_path = resolve_dataset_config_path(json_file)
        dataset_config = load_dataset_config(
            dataset_config_path
        )  # not using config.model_dump() as it retains only the validated objects.

        cloud_optimised_format = dataset_config.get("cloud_optimised_format")

        handler_class_name = dataset_config.get("handler_class", None)
        if handler_class_name is not None:
            module = importlib.import_module(
                f"aodn_cloud_optimised.lib.{handler_class_name}"
            )
            handler_class = getattr(module, handler_class_name)
        else:
            handler_class = _get_generic_handler_class(dataset_config)

        if cloud_optimised_format == "parquet":
            handler = handler_class(
                optimised_bucket_name=optimised_bucket_name,
                root_prefix_cloud_optimised_path=root_prefix_cloud_optimised_path,
                dataset_config=dataset_config,
            )
            try:
                handler._add_metadata_sidecar()
            except Exception as err:
                print(f"{json_file} - Error while updating metadata.\n{err}")

        elif cloud_optimised_format == "zarr":
            handler = handler_class(
                optimised_bucket_name=optimised_bucket_name,
                root_prefix_cloud_optimised_path=root_prefix_cloud_optimised_path,
                dataset_config=dataset_config,
            )
            try:
                handler._update_metadata()
            except Exception as err:
                print(f"{json_file} - Error while updating metadata.\n{err}")
        else:
            raise ValueError(f"{cloud_optimised_format} not supported")


def cli_main():
    parser = argparse.ArgumentParser(
        description="Validate and apply metadata updates to cloud-optimised datasets."
    )
    parser.add_argument(
        "dataset_name",
        nargs="?",
        default=None,
        help="Optional dataset name (must finish with .json and exist).",
    )
    args = parser.parse_args()

    if args.dataset_name is not None:
        if not args.dataset_name.endswith(".json"):
            parser.error("The dataset name must finish with '.json'")

        valid_datasets = list_dataset_config()
        if args.dataset_name not in valid_datasets:
            parser.error(
                f"'{args.dataset_name}' is not within the available dataset configurations."
            )

        main(json_files=[args.dataset_name])
    else:
        main(json_files=None)


if __name__ == "__main__":
    cli_main()
