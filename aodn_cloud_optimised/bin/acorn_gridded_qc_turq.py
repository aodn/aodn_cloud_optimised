#!/usr/bin/env python3
import importlib.resources

from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation_loop

from aodn_cloud_optimised.lib.config import load_variable_from_config, load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')
    nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ACORN/gridded_1h-avg-current-map_QC/TURQ/2023')

    dataset_config = load_dataset_config(str(importlib.resources.path("aodn_cloud_optimised.config.dataset", "acorn_gridded_qc_turq.json")))

    # First zarr creation
    cloud_optimised_creation_loop([nc_obj_ls[0]],
                                  dataset_config=dataset_config,
                                  reprocess=True
                                  )

    # append to zarr
    cloud_optimised_creation_loop(nc_obj_ls[1:],
                                  dataset_config=dataset_config
                                  )
    # rechunking
    GenericHandler(input_object_key=nc_obj_ls[0],
                   dataset_config=dataset_config,
                   ).rechunk()


if __name__ == "__main__":
    main()
