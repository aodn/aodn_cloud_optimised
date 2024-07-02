import json
import os
import unittest

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
TEST_FILE_NC = os.path.join(ROOT_DIR, "resources", "2902093_prof.nc")
TEST_FILE_BAD_GEOM_NC = os.path.join(ROOT_DIR, "resources", "5905017_prof.nc")

DATASET_CONFIG = os.path.join(ROOT_DIR, "resources", "argo_core.json")


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
            "imos-data", f"good_nc_argo/{os.path.basename(TEST_FILE_NC)}", TEST_FILE_NC
        )
        self._upload_to_s3(
            "imos-data",
            f"bad_geom_argo/{os.path.basename(TEST_FILE_BAD_GEOM_NC)}",
            TEST_FILE_BAD_GEOM_NC,
        )

        self.dataset_argo_netcdf_config = load_dataset_config(DATASET_CONFIG)

        self.handler_nc_argo_file = ArgoHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=self.dataset_argo_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
        )

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def tearDown(self):
        self.server.stop()

    def test_parquet_nc_argo_handler(self):  # , MockS3FileSystem):
        nc_obj_ls = s3_ls("imos-data", "good_nc_argo")
        # with patch('s3fs.S3FileSystem', lambda anon, client_kwargs: s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555/"})):
        # MockS3FileSystem.return_value = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555"})

        # with mock_aws(aws_credentials):

        # 1st pass
        self.handler_nc_argo_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        # TODO: Not a big big deal breaker, but got an issue which should be fixed in the try except only for the unittest
        #       2024-07-01 16:04:54,721 - INFO - GenericParquetHandler.py:824 - delete_existing_matching_parquet - No files to delete: GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
        self.handler_nc_argo_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_name = self.dataset_argo_netcdf_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        self.assertEqual(parquet_dataset["filename"][0], os.path.basename(TEST_FILE_NC))

        # this checks that bad_timestamps values are cleaned
        self.assertEqual(parquet_dataset["timestamp"][0], 1356998400.0)
        self.assertEqual(
            parquet_dataset["JULD"][0], pd.Timestamp("2013-02-26 03:15:00")
        )

        # Assert the expected values in the Parquet dataset
        self.assertIn("PSAL_ADJUSTED", parquet_dataset.columns)
        self.assertIn("TEMP_ADJUSTED", parquet_dataset.columns)
        assert_array_equal(
            np.unique(parquet_dataset.PLATFORM_NUMBER.values), np.array([2902093])
        )

    def test_parquet_nc_argo_bad_geom_handler(self):
        nc_obj_ls = s3_ls("imos-data", "bad_geom_argo")
        # with patch('s3fs.S3FileSystem', lambda anon, client_kwargs: s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555/"})):
        # MockS3FileSystem.return_value = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555"})

        # with mock_aws(aws_credentials):

        # 1st pass
        self.handler_nc_argo_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_name = self.dataset_argo_netcdf_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {"endpoint_url": "http://127.0.0.1:5555"}
            },
        )

        # TODO: check the values are correct, why are the first 10 values removed? forgot! To investigate!
        #     JULD = "2016-01-07 22:06:34", "2016-01-17 15:35:41", "2016-01-27 09:41:07",
        #     "2016-02-06 03:32:09", "2016-02-15 22:51:46", "2016-02-25 17:19:12",
        #     "2016-03-06 12:20:15", "2016-03-16 07:49:21", "2016-03-26 01:49:47",
        #     "2016-04-04 21:19:44", "2016-04-14 15:09:26", "2016-04-24 11:00:24",
        #     "2016-05-04 06:12:07", "2016-05-14 01:40:30", "2016-05-23 20:29:18",
        # Assert the expected values in the Parquet dataset
        self.assertEqual(parquet_dataset["timestamp"][0], 1451606400)
        self.assertEqual(
            parquet_dataset["JULD"][0], pd.Timestamp("2016-05-23 20:29:18")
        )

        # Assert the expected values in the Parquet dataset
        self.assertIn("PSAL_ADJUSTED", parquet_dataset.columns)
        self.assertIn("TEMP_ADJUSTED", parquet_dataset.columns)


if __name__ == "__main__":
    unittest.main()
