#!/usr/bin/env python3
import importlib.resources

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.config import (
    load_variable_from_config,
    load_dataset_config,
)
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    BUCKET_RAW_DEFAULT = load_variable_from_config("BUCKET_RAW_DEFAULT")
    nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, "IMOS/SOOP/SOOP-XBT/REALTIME/")

    dataset_config = load_dataset_config(
        str(
            importlib.resources.path(
                "aodn_cloud_optimised.config.dataset", "soop_xbt_nrt.json"
            )
        )
    )
    cloud_optimised_creation(
        nc_obj_ls, dataset_config=dataset_config, clear_existing_data=True
    )


if __name__ == "__main__":
    main()
