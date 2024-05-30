#!/usr/bin/env python3
import importlib.resources

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation_loop
from aodn_cloud_optimised.lib.config import load_variable_from_config, load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():

    BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')

    nc_obj_ls = s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ANMN/NSW') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ANMN/PA') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ANMN/QLD') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ANMN/SA') + \
                s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/ANMN/WA')

    # Aqualogger
    temperature_logger_ts_fv01_ls = [s for s in nc_obj_ls if ('/Temperature/' in s) and ('FV01' in s)]
    dataset_config = load_dataset_config(str(importlib.resources.path("aodn_cloud_optimised.config.dataset", "anmn_temperature_logger_ts_fv01.json")))

    cloud_optimised_creation_loop(temperature_logger_ts_fv01_ls,
                                  dataset_config=dataset_config)


if __name__ == "__main__":
    main()
