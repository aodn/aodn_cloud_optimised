#!/usr/bin/env python3
import importlib.resources

from aodn_cloud_optimised.lib.ArgoHandler import ArgoHandler
from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation_loop, cloud_optimised_creation
from aodn_cloud_optimised.lib.config import load_variable_from_config, load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls


def main():

    dataset_config = load_dataset_config(str(importlib.resources.path("aodn_cloud_optimised.config.dataset", "aatams_sattag_meop_qc_ctd.json")))

    # test file with timestamp issues and deletion of previous parquet objects
    #cloud_optimised_creation('IMOS/Argo/dac/incois/2902093/2902093_prof.nc',
    #                 dataset_config=dataset_config,
    #                 handler_class=ArgoHandler,
    #                 force_old_pq_del=True)

    # Lots of ram usage
    #cloud_optimised_creation('IMOS/Argo/dac/coriolis/3902120/3902120_prof.nc',
    #                 dataset_config=dataset_config,
    #                 handler_class=ArgoHandler,
    #                 force_old_pq_del=True)

    BUCKET_RAW_DEFAULT = load_variable_from_config('BUCKET_RAW_DEFAULT')

    #organisations = ["kordi", "csiro", "bodc", "csio", "incois", "jma", "coriolis", "aoml", "nmdis", "meds", "kma"]
    # organisations = ["nmdis", "meds", "kma"]

    # for org in organisations:
    argo_core_ls = s3_ls(BUCKET_RAW_DEFAULT, 'IMOS/AATAMS/satellite_tagging/MEOP_QC_CTD')

    cloud_optimised_creation_loop(argo_core_ls,
                                      dataset_config=dataset_config,
                                      handler_class=ArgoHandler)


if __name__ == "__main__":
    main()
