#!/usr/bin/env python3
import importlib.resources

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation_loop
from aodn_cloud_optimised.lib.config import load_variable_from_config, load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():
    BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')
    nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, 'Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'Bureau_of_Meteorology/WAVE-BUOYS/REALTIME/') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'Deakin_University/WAVE-BUOYS/REALTIME')

    dataset_config = load_dataset_config(str(importlib.resources.path("aodn_cloud_optimised.config.dataset", "ardc_wave_nrt.json")))

    cloud_optimised_creation_loop(nc_obj_ls,
                                  dataset_config=dataset_config
                                  )


if __name__ == "__main__":
    main()
