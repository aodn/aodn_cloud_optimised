import json
import os
import unittest

import boto3
import numpy as np
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
    "IMOS_ACORN_V_20240101T000000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T010000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T020000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T030000Z_TURQ_FV01_1-hour-avg.nc",
]

TEST_FILE_NC_ACORN = [
    os.path.join(ROOT_DIR, "resources", file_name) for file_name in filenames
]
# TEST_FILE_NC_ACORN_1, TEST_FILE_NC_ACORN_2, TEST_FILE_NC_ACORN_3, TEST_FILE_NC_ACORN_4 = TEST_FILE_NC_ACORN

DATASET_CONFIG_NC_ACORN_JSON = os.path.join(
    ROOT_DIR, "resources", "acorn_gridded_qc_turq.json"
)


@pytest.fixture(scope="function")
def mock_aws_server():
    with mock_aws():
        yield


@mock_aws
class TestGenericZarrHandler(unittest.TestCase):
    def setUp(self):

        # Create a mock S3 service
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

        for test_file in TEST_FILE_NC_ACORN:
            self._upload_to_s3(
                "imos-data", f"acorn/{os.path.basename(test_file)}", test_file
            )

        dataset_acorn_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_JSON)
        self.handler_nc_acorn_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_acorn_netcdf_config,
            # clear_existing_data=True,
            cluster_mode="local",
        )

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def tearDown(self):
        self.server.stop()

    # TODO: find a solution to patch s3fs properly and not relying on changing the s3fs values in the code
    # @patch('s3fs.S3FileSystem')
    def test_parquet_nc_acorn_handler(self):  # , MockS3FileSystem):
        nc_obj_ls = s3_ls("imos-data", "acorn")
        # with patch('s3fs.S3FileSystem', lambda anon, client_kwargs: s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555/"})):
        # MockS3FileSystem.return_value = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": "http://127.0.0.1:5555"})

        # with mock_aws(aws_credentials):

        # 1st pass
        # 2024-07-02 11:16:16,538 - INFO - GenericZarrHandler.py:381 - publish_cloud_optimised_fileset_batch - Writing data to new Zarr dataset
        # 2024-07-02 11:16:19,366 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>
        self.handler_nc_acorn_file.to_cloud_optimised(nc_obj_ls)

        # 2nd pass, process the same file a second time. Should be overwritten in ONE region slice
        # 2024-07-02 11:16:21,649 - INFO - GenericZarrHandler.py:303 - publish_cloud_optimised_fileset_batch - Duplicate values of TIME
        # 2024-07-02 11:16:21,650 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(0, 4, None)}, Matching Indexes in new ds: [0 1 2 3]
        # 2024-07-02 11:16:22,573 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>
        self.handler_nc_acorn_file.to_cloud_optimised(nc_obj_ls)

        # 3rd pass, create a non-contiguous list of files to reprocess. TWO region slices should happen. Look in the log
        # output of the unittest as it's hard to test!
        # output should be
        # 2024-07-02 11:16:24,774 - INFO - GenericZarrHandler.py:276 - publish_cloud_optimised_fileset_batch - append data to existing Zarr
        # 2024-07-02 11:16:24,837 - INFO - GenericZarrHandler.py:303 - publish_cloud_optimised_fileset_batch - Duplicate values of TIME
        # 2024-07-02 11:16:24,839 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(0, 1, None)}, Matching Indexes in new ds: [0]
        # 2024-07-02 11:16:25,905 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(2, 4, None)}, Matching Indexes in new ds: [1 2]
        # 2024-07-02 11:16:26,631 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>
        nc_obj_ls_non_contiguous = nc_obj_ls[0:1] + nc_obj_ls[2:4]
        self.handler_nc_acorn_file.to_cloud_optimised(nc_obj_ls_non_contiguous)

        # read zarr
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.zarr/"

        # TODO: calling open_zarr in the unitest is crazy finiky. Sometimes it works sometimes it doesnt
        #       ValueError: The future belongs to a different loop than the one specified as the loop argument
        #       the only way is to run it multiple times. Could be a local machine issue
        #       Also debugging and trying to load open_zarr in debug doesnt work... However it's possible to do a
        #       print(np.nanmax(ds.UCUR.values)) to get value to write unittests

        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)
        self.assertEqual(ds.UCUR.standard_name, "eastward_sea_water_velocity")

        np.testing.assert_almost_equal(
            np.nanmax(ds.UCUR.values),
            0.69455004,
            decimal=3,
            err_msg="Maximum value in UCUR is not as expected.",
        )


if __name__ == "__main__":
    unittest.main()