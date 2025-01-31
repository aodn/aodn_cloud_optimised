import json
import os
import unittest
from unittest.mock import patch

import boto3
import numpy as np
import pandas as pd
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from numpy.testing import assert_array_equal

from aodn_cloud_optimised.lib.ArgoHandler import ArgoHandler
from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC = os.path.join(ROOT_DIR, "resources", "ct64-M001-09_prof.nc")

DATASET_CONFIG = os.path.join(
    ROOT_DIR, "resources", "animal_ctd_satellite_relay_tagging_delayed_qc.json"
)


@mock_aws
class TestArgoHandler(unittest.TestCase):
    def setUp(self):
        self.BUCKET_OPTIMISED_NAME = "imos-data-lab-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = "testing"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket="imos-data")
        self.s3.create_bucket(Bucket=self.BUCKET_OPTIMISED_NAME)

        # create moto server; needed for s3fs and parquet
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=5555)

        # TODO: use it for patching?
        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://127.0.0.1:5555/",
                "region_name": "us-east-1",
            },
        )

        self.server.start()

        # Make the "imos-data" bucket public
        public_policy_imos_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::imos-data/*",
                }
            ],
        }

        public_policy_cloud_optimised_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.BUCKET_OPTIMISED_NAME}/*",
                }
            ],
        }

        self.s3.put_bucket_policy(
            Bucket="imos-data", Policy=json.dumps(public_policy_imos_data)
        )

        self.s3.put_bucket_policy(
            Bucket=self.BUCKET_OPTIMISED_NAME,
            Policy=json.dumps(public_policy_cloud_optimised_data),
        )

        # Copy files to the mock S3 bucket
        self._upload_to_s3(
            "imos-data", f"good_nc_meop/{os.path.basename(TEST_FILE_NC)}", TEST_FILE_NC
        )
        self.dataset_meop_netcdf_config = load_dataset_config(DATASET_CONFIG)

        self.handler_nc_meop_file = ArgoHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=self.dataset_meop_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
        )

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def tearDown(self):
        # Delete all objects and buckets in the mock S3
        bucket_list = self.s3.list_buckets()["Buckets"]
        for bucket in bucket_list:
            bucket_name = bucket["Name"]
            objects = self.s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            for obj in objects:
                self.s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            self.s3.delete_bucket(Bucket=bucket_name)

        self.server.stop()

    def test_parquet_nc_meop(self):
        nc_obj_ls = s3_ls("imos-data", "good_nc_meop")

        # 1st pass
        with patch.object(self.handler_nc_meop_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_meop_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        with patch.object(self.handler_nc_meop_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_meop_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_name = self.dataset_meop_netcdf_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertEqual(parquet_dataset["filename"][0], os.path.basename(TEST_FILE_NC))
        self.assertEqual(
            parquet_dataset["species"][0], "Southern ellie"
        )  # important to check. gatts to varatts and we modified to source xarray by converting back from pandas

        # this checks that bad_timestamps values are cleaned
        self.assertEqual(parquet_dataset["timestamp"][0], 1262304000.0)
        self.assertEqual(
            parquet_dataset["JULD"][0], pd.Timestamp("2010-07-10 23:09:59")
        )

        # Assert the expected values in the Parquet dataset
        self.assertIn("PSAL_ADJUSTED", parquet_dataset.columns)
        self.assertIn("TEMP_ADJUSTED", parquet_dataset.columns)
        assert_array_equal(
            np.unique(parquet_dataset.PLATFORM_NUMBER.values), np.array([19866])
        )


if __name__ == "__main__":
    unittest.main()
