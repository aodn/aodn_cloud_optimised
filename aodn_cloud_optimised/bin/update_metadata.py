#!/usr/bin/env python3


from typing import Optional

from pydantic import (
    ValidationError,
)

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
    load_config_and_validate,
    resolve_dataset_config_path,
)
from aodn_cloud_optimised.lib.common import list_dataset_config
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
)
from aodn_cloud_optimised.lib.GenericParquetHandler import (
    GenericHandler as ParquetHandler,
)
from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler as ZarrHandler


def main(
    json_files: Optional[list[str]] = None,
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

        if cloud_optimised_format == "parquet":
            parquetHandler = ParquetHandler(
                optimised_bucket_name=load_variable_from_config(
                    "BUCKET_OPTIMISED_DEFAULT"
                ),
                root_prefix_cloud_optimised_path=load_variable_from_config(
                    "ROOT_PREFIX_CLOUD_OPTIMISED_PATH"
                ),
                dataset_config=dataset_config,
            )
            try:
                parquetHandler._add_metadata_sidecar()
            except Exception as err:
                print(f"{json_file} - Error while updating metadata.\n{err}")

        elif cloud_optimised_format == "zarr":
            zarrHandler = ZarrHandler(
                optimised_bucket_name=load_variable_from_config(
                    "BUCKET_OPTIMISED_DEFAULT"
                ),
                root_prefix_cloud_optimised_path=load_variable_from_config(
                    "ROOT_PREFIX_CLOUD_OPTIMISED_PATH"
                ),
                dataset_config=dataset_config,
            )
            try:
                zarrHandler._update_metadata()
            except Exception as err:
                print(f"{json_file} - Error while updating metadata.\n{err}")
        else:
            raise ValueError(f"{cloud_optimised_format} not supported")


if __name__ == "__main__":
    main()
