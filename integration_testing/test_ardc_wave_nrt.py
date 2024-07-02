#!/usr/bin/env python3
import importlib.resources
import os
import unittest

import pandas as pd
import pyarrow.dataset as pds
import pyarrow.parquet as pq

from aodn_cloud_optimised.lib.CommonHandler import cloud_optimised_creation
from aodn_cloud_optimised.lib.ParquetDataQuery import *
from aodn_cloud_optimised.lib.config import (
    load_variable_from_config,
    load_dataset_config,
)
from aodn_cloud_optimised.lib.s3Tools import delete_objects_in_prefix


class TestIntegrationArdcWaveNrt(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        nc_obj_ls = [
            "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2022/DOT-WA_20221106_ALBANY_RT_WAVE-PARAMETERS_monthly.nc",
            "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2022/DOT-WA_20221201_ALBANY_RT_WAVE-PARAMETERS_monthly.nc",
            "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2023/DOT-WA_20230101_ALBANY_RT_WAVE-PARAMETERS_monthly.nc",
            "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2023/DOT-WA_20230201_ALBANY_RT_WAVE-PARAMETERS_monthly.nc",
            "Department_of_Transport-Western_Australia/WAVE-BUOYS/REALTIME/WAVE-PARAMETERS/ALBANY/2023/DOT-WA_20230302_ALBANY_RT_WAVE-PARAMETERS_monthly.nc",
        ]

        dataset_config = load_dataset_config(
            str(
                importlib.resources.path(
                    "aodn_cloud_optimised.config.dataset", "ardc_wave_nrt.json"
                )
            )
        )

        delete_objects_in_prefix(
            load_variable_from_config("BUCKET_INTEGRATION_TESTING_OPTIMISED_DEFAULT"),
            os.path.join(
                load_variable_from_config(
                    "ROOT_PREFIX_CLOUD_OPTIMISED_INTEGRATION_TESTING_PATH"
                ),
                (
                    dataset_config["dataset_name"]
                    + "."
                    + dataset_config["cloud_optimised_format"]
                ),
            ),
        )

        cloud_optimised_creation(
            nc_obj_ls,
            dataset_config=dataset_config,
            raw_bucket_name=load_variable_from_config(
                "BUCKET_INTEGRATION_TESTING_RAW_DEFAULT"
            ),
            optimised_bucket_name=load_variable_from_config(
                "BUCKET_INTEGRATION_TESTING_OPTIMISED_DEFAULT"
            ),
            root_prefix_cloud_optimised_path=load_variable_from_config(
                "ROOT_PREFIX_CLOUD_OPTIMISED_INTEGRATION_TESTING_PATH"
            ),
        )

    def test_ardc_wave_nrt(self):
        dataset_config = load_dataset_config(
            str(
                importlib.resources.path(
                    "aodn_cloud_optimised.config.dataset", "ardc_wave_nrt.json"
                )
            )
        )

        dataset_path = os.path.join(
            load_variable_from_config("BUCKET_INTEGRATION_TESTING_OPTIMISED_DEFAULT"),
            load_variable_from_config(
                "ROOT_PREFIX_CLOUD_OPTIMISED_INTEGRATION_TESTING_PATH"
            ),
            (
                dataset_config["dataset_name"]
                + "."
                + dataset_config["cloud_optimised_format"]
            ),
        )

        dname = f"s3://{dataset_path}"
        parquet_ds = pq.ParquetDataset(dname, partitioning="hive")

        unique_partition_value = query_unique_value(parquet_ds, "site_name")
        self.assertEqual(unique_partition_value, {"Albany"})

        df = pd.read_parquet(dname, engine="pyarrow")
        self.assertAlmostEqual(2.427, df.WHTH.mean(), delta=0.1)

    @classmethod
    def tearDownClass(cls):

        dataset_config = load_dataset_config(
            str(
                importlib.resources.path(
                    "aodn_cloud_optimised.config.dataset", "ardc_wave_nrt.json"
                )
            )
        )

        delete_objects_in_prefix(
            load_variable_from_config("BUCKET_INTEGRATION_TESTING_OPTIMISED_DEFAULT"),
            os.path.join(
                load_variable_from_config(
                    "ROOT_PREFIX_CLOUD_OPTIMISED_INTEGRATION_TESTING_PATH"
                ),
                (
                    dataset_config["dataset_name"]
                    + "."
                    + dataset_config["cloud_optimised_format"]
                ),
            ),
        )


if __name__ == "__main__":
    unittest.main()
