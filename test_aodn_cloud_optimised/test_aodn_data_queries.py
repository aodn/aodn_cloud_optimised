import json
import os
import unittest
from unittest.mock import patch

import boto3
import pandas as pd
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from shapely.geometry import Polygon

from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.DataQuery import GetAodn
from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.s3Tools import get_free_local_port, s3_ls

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC_ARDC = os.path.join(
    ROOT_DIR, "resources", "BOM_20240301_CAPE-SORELL_RT_WAVE-PARAMETERS_monthly.nc"
)

TEST_FILE_NC_SOOP_SST = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-SST_MT_20110101T000000Z_9HA2479_FV01_C-20120528T071958Z.nc",
)


DATASET_CONFIG_NC_ARDC_JSON = os.path.join(
    ROOT_DIR, "resources", "wave_buoy_realtime_nonqc.json"
)

DATASET_CONFIG_NC_SOOP_SST_JSON = os.path.join(
    ROOT_DIR, "resources", "vessel_sst_delayed_qc.json"
)


@mock_aws
class TestGenericHandler(unittest.TestCase):
    def setUp(self):
        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "aodn-cloud-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = ""
        # create moto server; needed for s3fs and parquet
        self.port = get_free_local_port()
        self.endpoint_ip = "127.0.0.1"
        self.server = ThreadedMotoServer(ip_address=self.endpoint_ip, port=self.port)

        self.server.start()

        self.s3_client_opts = {
            "service_name": "s3",
            "region_name": "us-east-1",
            "endpoint_url": f"http://{self.endpoint_ip}:{self.port}",
        }
        self.s3 = boto3.client(**self.s3_client_opts)
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

        # give all permissions to read on Action. Very important
        public_policy_cloud_optimised_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:*",
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
            "imos-data",
            f"good_nc_ardc/{os.path.basename(TEST_FILE_NC_ARDC)}",
            TEST_FILE_NC_ARDC,
        )

        self._upload_to_s3(
            "imos-data",
            f"good_nc_soop_sst/{os.path.basename(TEST_FILE_NC_SOOP_SST)}",
            TEST_FILE_NC_SOOP_SST,
        )

        dataset_ardc_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ARDC_JSON)
        self.handler_nc_ardc_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_ardc_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
            s3_client_opts=self.s3_client_opts,
        )

        dataset_soop_sst_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_SOOP_SST_JSON
        )
        self.handler_nc_soop_sst_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_soop_sst_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
            s3_client_opts=self.s3_client_opts,
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

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", "us-east-1")
    def test_parquet_queries(self):
        """Creating 2 Parquet dataset and then use the GetAodn Class to query data"""
        with patch(
            "aodn_cloud_optimised.lib.DataQuery.ENDPOINT_URL",
            f"http://{self.endpoint_ip}:{self.port}",
        ):
            # dataset 1
            nc_obj_ls = s3_ls("imos-data", "good_nc_ardc")
            with patch.object(self.handler_nc_ardc_file, "s3_fs", new=self.s3_fs):
                self.handler_nc_ardc_file.to_cloud_optimised([nc_obj_ls[0]])

            # dataset 2
            nc_obj_ls = s3_ls("imos-data", "good_nc_soop_sst")
            with patch.object(self.handler_nc_soop_sst_file, "s3_fs", new=self.s3_fs):
                self.handler_nc_soop_sst_file.to_cloud_optimised([nc_obj_ls[0]])

            aodn_instance = GetAodn()

            # test metadata and available dataset
            aodn_meta = aodn_instance.get_metadata()
            self.assertEqual(
                ["vessel_sst_delayed_qc.parquet", "wave_buoy_realtime_nonqc.parquet"],
                list(aodn_meta.metadata_catalog().keys()),
            )

            # test fuzzysearch
            res = aodn_meta.find_datasets_with_attribute(
                "temp", target_key="standard_name"
            )
            self.assertEqual(["vessel_sst_delayed_qc.parquet"], res)

            # test temporal extents
            res = aodn_instance.get_dataset(
                "vessel_sst_delayed_qc.parquet"
            ).get_temporal_extent()

            self.assertEqual(
                (
                    pd.Timestamp("2011-01-01 00:00:00").floor("min"),
                    pd.Timestamp("2011-01-01 21:59:00").floor("min"),
                ),
                (
                    res[0].floor("min"),
                    res[1].floor("min"),
                ),
            )

            # test spatial extent
            expected_polygon_0 = Polygon(
                [(160, -25), (170, -25), (170, -15), (160, -15), (160, -25)]
            )

            res = aodn_instance.get_dataset(
                "vessel_sst_delayed_qc.parquet"
            ).get_spatial_extent()

            self.assertEqual(expected_polygon_0, res.geoms[0])


if __name__ == "__main__":
    unittest.main()
