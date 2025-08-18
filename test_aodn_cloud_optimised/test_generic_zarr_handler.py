import copy
import json
import logging
import os
import unittest
from io import StringIO
from unittest.mock import patch

import boto3
import numpy as np
import pytest
import s3fs
import xarray as xr
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.GenericZarrHandler import GenericHandler
from aodn_cloud_optimised.lib.s3Tools import get_free_local_port, s3_ls

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
filenames_acorn_turq = [
    "IMOS_ACORN_V_20240101T000000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T010000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T020000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T030000Z_TURQ_FV01_1-hour-avg.nc",
]

TEST_FILE_NC_ACORN_TURQ = [
    os.path.join(ROOT_DIR, "resources", file_name) for file_name in filenames_acorn_turq
]

DATASET_CONFIG_NC_ACORN_TURQ_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json",
)


# Same thing but for North West Shelf. Some files have a bad grid. Will test the bad file to be removed
filenames_acorn_nwa = [
    "IMOS_ACORN_V_20220312T013000Z_NWA_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20220312T003000Z_NWA_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20220312T183000Z_NWA_FV01_1-hour-avg.nc",  # Bad grid, not consistent
]

TEST_FILE_NC_ACORN_NWA = [
    os.path.join(ROOT_DIR, "resources", file_name) for file_name in filenames_acorn_nwa
]

DATASET_CONFIG_NC_ACORN_NWA_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "radar_NorthWestShelf_velocity_hourly_averaged_delayed_qc.json",
)


@pytest.fixture(scope="function")
def mock_aws_server():
    with mock_aws():
        yield


@mock_aws
class TestGenericZarrHandler(unittest.TestCase):
    def setUp(self):
        # TODO: remove this abomination for unittesting. but it works. Only for zarr !
        # os.environ["RUNNING_UNDER_UNITTEST"] = "true"

        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "imos-data-lab-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = "testing"

        # create moto server; needed for s3fs and parquet
        self.port = get_free_local_port()
        os.environ["MOTO_PORT"] = str(self.port)
        self.endpoint_ip = "127.0.0.1"
        self.server = ThreadedMotoServer(ip_address=self.endpoint_ip, port=self.port)

        self.server.start()

        self.s3_client_opts_common = {
            "service_name": "s3",
            "region_name": "us-east-1",
            "endpoint_url": f"http://{self.endpoint_ip}:{self.port}",
        }
        self.s3 = boto3.client(**self.s3_client_opts_common)

        self.s3.create_bucket(Bucket="imos-data")
        self.s3.create_bucket(Bucket=self.BUCKET_OPTIMISED_NAME)

        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": f"http://{self.endpoint_ip}:{self.port}/",
                "region_name": "us-east-1",
            },
        )

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
        for test_file in TEST_FILE_NC_ACORN_TURQ:
            self._upload_to_s3(
                "imos-data", f"acorn/turq/{os.path.basename(test_file)}", test_file
            )

        dataset_acorn_turq_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_ACORN_TURQ_JSON
        )
        self.handler_nc_acorn_turq_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_acorn_turq_netcdf_config,
            # clear_existing_data=True,
            cluster_mode="local",
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        # Copy files to the mock S3 bucket
        for test_file in TEST_FILE_NC_ACORN_NWA:
            self._upload_to_s3(
                "imos-data", f"acorn/nwa/{os.path.basename(test_file)}", test_file
            )

        dataset_acorn_nwa_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_ACORN_NWA_JSON
        )
        self.handler_nc_acorn_nwa_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_acorn_nwa_netcdf_config,
            # clear_existing_data=True,
            cluster_mode="local",
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        dataset_acorn_nwa_netcdf_config_mod = copy.deepcopy(
            dataset_acorn_nwa_netcdf_config
        )
        self.mod_title = "modified_title"
        dataset_acorn_nwa_netcdf_config_mod["schema_transformation"][
            "global_attributes"
        ]["set"]["title"] = self.mod_title
        dataset_acorn_nwa_netcdf_config_mod["schema_transformation"][
            "global_attributes"
        ]["delete"] = ["time_coverage_start", "time_coverage_end"]
        self.handler_nc_acorn_nwa_mod_metadata = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_acorn_nwa_netcdf_config_mod,
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
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
        # del os.environ["RUNNING_UNDER_UNITTEST"]

    # TODO: find a solution to patch s3fs properly and not relying on changing the s3fs values in the code
    def test_zarr_nc_acorn_turq_handler(self):
        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        nc_obj_ls = s3_ls("imos-data", "acorn/turq")

        # TODO: capture logging from handler as done in the test_generic_parquet_handler for SOOP SST.
        #       Then test that we're getting to correct logging messages
        # 1st pass
        # 2024-07-02 11:16:16,538 - INFO - GenericZarrHandler.py:381 - publish_cloud_optimised_fileset_batch - Writing data to new Zarr dataset
        # 2024-07-02 11:16:19,366 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>

        self.handler_nc_acorn_turq_file.to_cloud_optimised(nc_obj_ls)

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any("Writing data to a new Zarr store at" in log for log in captured_logs)
        )
        # 2nd pass, process the same file a second time. Should be overwritten in ONE region slice
        # 2024-07-02 11:16:21,649 - INFO - GenericZarrHandler.py:303 - publish_cloud_optimised_fileset_batch - Duplicate values of TIME
        # 2024-07-02 11:16:21,650 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(0, 4, None)}, Matching Indexes in new ds: [0 1 2 3]
        # 2024-07-02 11:16:22,573 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>

        self.handler_nc_acorn_turq_file.to_cloud_optimised(nc_obj_ls)

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "Batch 1, Region 1 - Overwriting Zarr dataset in region: {'TIME': slice(0, 4, None)}, with matching indexes in the new dataset: [0 1 2 3]"
                in log
                for log in captured_logs
            )
        )

        # 3rd pass, create a non-contiguous list of files to reprocess. TWO region slices should happen. Look in the log
        # output of the unittest as it's hard to test!
        # output should be
        # 2024-07-02 11:16:24,774 - INFO - GenericZarrHandler.py:276 - publish_cloud_optimised_fileset_batch - append data to existing Zarr
        # 2024-07-02 11:16:24,837 - INFO - GenericZarrHandler.py:303 - publish_cloud_optimised_fileset_batch - Duplicate values of TIME
        # 2024-07-02 11:16:24,839 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(0, 1, None)}, Matching Indexes in new ds: [0]
        # 2024-07-02 11:16:25,905 - INFO - GenericZarrHandler.py:353 - publish_cloud_optimised_fileset_batch - Overwriting Zarr dataset in Region: {'TIME': slice(2, 4, None)}, Matching Indexes in new ds: [1 2]
        # 2024-07-02 11:16:26,631 - INFO - GenericZarrHandler.py:391 - publish_cloud_optimised_fileset_batch - Batch 1 processed and written to <fsspec.mapping.FSMap object at 0x78166762b730>
        nc_obj_ls_non_contiguous = nc_obj_ls[0:1] + nc_obj_ls[2:4]
        self.handler_nc_acorn_turq_file.to_cloud_optimised(nc_obj_ls_non_contiguous)

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "Batch 1, Region 2 - Overwriting Zarr dataset in region: {'TIME': slice(2, 4, None)}, with matching indexes in the new dataset: [1 2]"
                in log
                for log in captured_logs
            )
        )

        # read zarr
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_TURQ_JSON)
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

    def test_zarr_nc_acorn_nwa_handler(self):
        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        nc_obj_ls = s3_ls("imos-data", "acorn/nwa")

        # TODO: capture logging from handler
        self.handler_nc_acorn_nwa_file.to_cloud_optimised(nc_obj_ls)

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "Detected issue with variable 'LATITUDE': Inconsistent grid." in log
                for log in captured_logs
            )
        )
        self.assertTrue(
            any(
                "LATITUDE in <File-like object S3FileSystem, imos-data/acorn/nwa/IMOS_ACORN_V_20220312T183000Z_NWA_FV01_1-hour-avg.nc> is NOT consistent with reference values."
                in log
                for log in captured_logs
            )
        )
        self.assertTrue(
            any(
                "[<File-like object S3FileSystem, imos-data/acorn/nwa/IMOS_ACORN_V_20220312T183000Z_NWA_FV01_1-hour-avg.nc>]"
                in log
                for log in captured_logs
            )
        )
        self.assertTrue(
            any("Writing data to a new Zarr store at" in log for log in captured_logs)
        )

        # read zarr
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_NWA_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.zarr/"

        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)
        self.assertEqual(ds.UCUR.standard_name, "eastward_sea_water_velocity")

        expected = np.array(
            ["2022-03-12T00:29:59.999993088", "2022-03-12T01:30:00.000000000"],
            dtype="datetime64[ns]",
        )
        assert np.allclose(
            ds.TIME.values.astype("datetime64[ns]").astype(float),
            expected.astype(float),
            atol=1e-9,
        ), f"TIME values are not as expected: {expected}"

        ### modification of metadata
        self.handler_nc_acorn_nwa_mod_metadata._update_metadata()
        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)
        self.assertEqual(ds.title, self.mod_title)
        self.assertNotIn("time_coverage_start", ds.attrs)
        self.assertNotIn("time_coverage_end", ds.attrs)
        #################################

        ### test delete data ##############
        ################

        filename_to_delete = filenames_acorn_nwa[1]

        ## 1) we check that the data we re about to delete is not full of NaNs
        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)

        # Get the dimension that filename is attached to
        time_dim = ds["filename"].dims[0]

        # Find the index/indices for this filename
        filenames = ds["filename"].compute().values
        matching_idx = np.where(filenames == filename_to_delete)[0]

        # Select UCUR only for those time steps
        ucur_selected = ds["UCUR"].isel({time_dim: matching_idx}).compute().values

        # Assert they are all NaN
        assert not np.isnan(
            ucur_selected
        ).all(), (
            f"UCUR for {filenames_acorn_nwa[1]} are already fully NaN. Not possible"
        )

        ## 2) delete the data
        self.handler_nc_acorn_nwa_mod_metadata.delete_cloud_optimised_data(
            filename="test"
        )
        self.handler_nc_acorn_nwa_mod_metadata.delete_cloud_optimised_data(
            filename=filename_to_delete
        )

        ## 3) check the data was deleted
        ds = xr.open_zarr(self.s3_fs.get_mapper(dname), consolidated=True)
        time_dim = ds["filename"].dims[0]
        filenames = ds["filename"].compute().values
        matching_idx = np.where(filenames == filename_to_delete)[0]
        ucur_selected = ds["UCUR"].isel({time_dim: matching_idx}).compute().values

        # Assert they are all NaN
        assert np.isnan(
            ucur_selected
        ).all(), f"UCUR for {filenames_acorn_nwa[1]} not fully NaN"


if __name__ == "__main__":
    unittest.main()
