import json
import logging
import os
import unittest
from io import StringIO
from unittest.mock import patch

import boto3
import pytest
import s3fs
import xarray as xr
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler
from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import s3_ls

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
filenames = [
    "IMOS_OceanCurrent_HV_20110901T000000Z_GSLA_FV02_NRT.nc",
    "IMOS_OceanCurrent_HV_20110902T000000Z_GSLA_FV02_NRT.nc",
    "IMOS_OceanCurrent_HV_20240101T000000Z_GSLA_FV02_NRT.nc",
    "IMOS_OceanCurrent_HV_20240102T000000Z_GSLA_FV02_NRT.nc",
]

TEST_FILE_NC_GSLA = [
    os.path.join(ROOT_DIR, "resources", file_name) for file_name in filenames
]

DATASET_CONFIG_NC_GSLA_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "model_sea_level_anomaly_gridded_realtime.json",
)


@pytest.fixture(scope="function")
def mock_aws_server():
    with mock_aws():
        yield


@mock_aws
class TestGenericZarrHandler(unittest.TestCase):
    def setUp(self):
        # TODO: remove this abomination for unittesting. but it works. Only for zarr !
        os.environ["RUNNING_UNDER_UNITTEST"] = "true"

        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "imos-data-lab-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = "testing"
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket="imos-data")
        self.s3.create_bucket(Bucket=self.BUCKET_OPTIMISED_NAME)

        # create moto server; needed for s3fs and parquet
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=5555)

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

        for test_file in TEST_FILE_NC_GSLA:
            self._upload_to_s3(
                "imos-data", f"gsla/{os.path.basename(test_file)}", test_file
            )

        dataset_gsla_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_GSLA_JSON)
        self.handler_nc_gsla_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_gsla_netcdf_config,
            # clear_existing_data=True,
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
        del os.environ["RUNNING_UNDER_UNITTEST"]

    # TODO: find a solution to patch s3fs properly and not relying on changing the s3fs values in the code
    def test_zarr_nc_handler(self):
        """
        A scpecific test to see how the code handles dealing with multiple files being NetCDF3 and NetCDF4.
        There are currently (as of August 2024, some issues with the latest xarray version.
        see https://github.com/pydata/xarray/issues/8909
        """
        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        nc_obj_ls = s3_ls("imos-data", "gsla")
        with patch.object(self.handler_nc_gsla_file, "s3_fs", new=self.s3_fs):
            self.handler_nc_gsla_file.to_cloud_optimised(nc_obj_ls)

        # Get captured logs
        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "Successfully Concatenating files together" in log
                for log in captured_logs
            )
        )

        dataset_config = load_dataset_config(DATASET_CONFIG_NC_GSLA_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.zarr/"

        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)
        self.assertEqual(ds.dims["TIME"], 4)


if __name__ == "__main__":
    unittest.main()
