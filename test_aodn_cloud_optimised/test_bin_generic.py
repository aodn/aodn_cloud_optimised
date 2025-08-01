import json
import logging
import os
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

import boto3
import s3fs
from botocore import UNSIGNED
from botocore.client import Config as BotoConfig  # avoid conflict with json config
from botocore.session import get_session
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
    DatasetConfig,
    main,
)
from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.s3Tools import get_free_local_port

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
        self.port = get_free_local_port()
        os.environ["MOTO_PORT"] = str(self.port)
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port)

        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": f"http://127.0.0.1:{self.port}/",
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

    def test_main_with_config_and_json_overwrite(self):
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_JSON)
        config_validated = DatasetConfig.model_validate(dataset_config)

        def _mock_boto3_client(service_name, *args, **kwargs):
            if service_name == "s3":
                session = get_session()
                return session.create_client(
                    "s3",
                    endpoint_url=f"http://127.0.0.1:{self.port}",
                    region_name="us-east-1",
                    config=BotoConfig(signature_version=UNSIGNED),
                )
            raise NotImplementedError(f"Unhandled boto3 service: {service_name}")

        with (
            patch(
                "aodn_cloud_optimised.bin.generic_cloud_optimised_creation.load_config_and_validate",
                new=lambda _: config_validated,
            ),
            patch("argparse.ArgumentParser.parse_args") as mock_parse_args,
            patch("sys.exit") as mock_sys_exit,
            patch(
                "aodn_cloud_optimised.lib.s3Tools.boto3.client", new=_mock_boto3_client
            ),
        ):
            mock_parse_args.return_value = MagicMock(
                # config="radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc",
                config=DATASET_CONFIG_NC_ACORN_JSON,
                json_overwrite=json.dumps(
                    {
                        "run_settings": {
                            "cluster": {"mode": "local"},
                            "raise_error": False,
                            "force_previous_parquet_deletion": False,
                            "clear_existing_data": False,
                            "paths": [
                                {
                                    "s3_uri": "s3://imos-data/IMOS/ACORN/gridded_1h-avg-current-map_QC",
                                    "filter": ["TURQ"],
                                },
                            ],
                            "optimised_bucket_name": self.BUCKET_OPTIMISED_NAME,
                            "root_prefix_cloud_optimised_path": self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
                        }
                    }
                ),
            )

            log_stream = StringIO()
            log_handler = logging.StreamHandler(log_stream)
            logger = logging.getLogger()
            logger.addHandler(log_handler)

            try:
                # Run main function
                main()

                log_handler.flush()
                captured_logs = log_stream.getvalue().strip().split("\n")

                assert any("Cluster dask dashboard" in log for log in captured_logs)
                assert any("Processing batch 1" in log for log in captured_logs)
                assert any(
                    "successfully published to Zarr store" in log
                    for log in captured_logs
                )
                assert not any("ERROR" in log for log in captured_logs)

                mock_sys_exit.assert_called_with(0)

            finally:
                logger.removeHandler(log_handler)


if __name__ == "__main__":
    unittest.main()
