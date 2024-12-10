import json
import logging
import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch, MagicMock

import boto3
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import main
from aodn_cloud_optimised.lib.clusterLib import ClusterMode

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

filenames = [
    "IMOS_ACORN_V_20240101T000000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T010000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T020000Z_TURQ_FV01_1-hour-avg.nc",
    "IMOS_ACORN_V_20240101T030000Z_TURQ_FV01_1-hour-avg.nc",
]

TEST_FILE_NC_ACORN = [
    os.path.join(ROOT_DIR, "resources", file_name) for file_name in filenames
]

DATASET_CONFIG_NC_ACORN_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json",
)


@mock_aws
class TestGenericCloudOptimisedCreation(unittest.TestCase):
    def setUp(self):
        # TODO: remove this abomination for unittesting. but it works. Only for zarr !
        os.environ["RUNNING_UNDER_UNITTEST"] = "true"

        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "optimised-bucket"
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
        for test_file in TEST_FILE_NC_ACORN:
            self._upload_to_s3(
                "imos-data",
                f"IMOS/ACORN/gridded_1h-avg-current-map_QC/{os.path.basename(test_file)}",
                test_file,
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

    @patch("argparse.ArgumentParser.parse_args")
    def test_main(self, mock_parse_args):
        # Prepare mock arguments
        mock_parse_args.return_value = MagicMock(
            paths=["IMOS/ACORN/gridded_1h-avg-current-map_QC"],
            filters=["TURQ"],
            suffix=".nc",
            dataset_config=DATASET_CONFIG_NC_ACORN_JSON,
            clear_existing_data=True,
            force_previous_parquet_deletion=False,
            cluster_mode=ClusterMode.LOCAL,
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path="testing",
            bucket_raw="imos-data",
        )

        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        try:
            # Run main function
            main()

            # Get captured logs
            log_handler.flush()
            captured_logs = log_stream.getvalue().strip().split("\n")

            # Validate logs (add more specific assertions based on your logging format)
            self.assertTrue(
                any("Cluster dask dashboard" in log for log in captured_logs)
            )
            self.assertTrue(any("Processing batch 1" in log for log in captured_logs))
            self.assertTrue(
                any(
                    "Batch 1 successfully published to Zarr store" in log
                    for log in captured_logs
                )
            )
            self.assertFalse(any("ERROR" in log for log in captured_logs))

        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__


if __name__ == "__main__":
    unittest.main()
