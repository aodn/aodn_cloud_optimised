import json
import os
import unittest

import boto3
import pyarrow as pa
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.s3Tools import get_free_local_port, s3_ls
from aodn_cloud_optimised.lib.schema import (
    create_pyarrow_schema,
    create_pyarrow_schema_from_dict,
    generate_json_schema_from_s3_netcdf,
    generate_json_schema_var_from_netcdf,
)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC_ANMN = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc",
)

TEST_FILE_NC_ANMN_SCHEMA = TEST_FILE_NC_ANMN.replace(".nc", ".schema")


@mock_aws()
class TestNetCDFSchemaGeneration(unittest.TestCase):
    def setUp(self):
        # Create a mock S3 service
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "imos-data"
        self.s3.create_bucket(Bucket=self.bucket_name)

        # Make the "imos-data" bucket public
        public_policy_imos_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                }
            ],
        }

        self.s3.put_bucket_policy(
            Bucket=self.bucket_name, Policy=json.dumps(public_policy_imos_data)
        )

        # Copy files to the mock S3 bucket
        self._upload_to_s3(
            self.bucket_name,
            f"good_nc_anmn/{os.path.basename(TEST_FILE_NC_ANMN)}",
            TEST_FILE_NC_ANMN,
        )

        self.nc_s3_path = s3_ls("imos-data", "good_nc_anmn")[0]

        self.port = get_free_local_port()
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port)
        self.server.start()
        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": f"http://127.0.0.1:{self.port}/",
                "region_name": "us-east-1",
            },
        )

        # Define some sample data for testing
        self.schema_list = [
            "temperature: double",
            "humidity: float",
            "pressure: int32",
            "timestamp: timestamp[ns]",
            "location: string",
        ]
        self.schema_dict = {
            "temperature": {"type": "double"},
            "humidity": {"type": "float"},
            "pressure": {"type": "int32"},
            "timestamp": {
                "type": "timestamp[ns]",
                "standard_name": "",
                "long_name": "",
            },
            "location": {"type": "string"},
        }

        self.sub_schema_strings = ["filename: string", "site_code: string"]

    def tearDown(self):
        self.server.stop()

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def test_create_schema_from_dict(self):
        # Test pyarrow_schema creation from pyarrow_schema strings
        result_schema = create_pyarrow_schema_from_dict(self.schema_dict)

        # Check if the result pyarrow_schema is an instance of PyArrow pyarrow_schema
        self.assertIsInstance(result_schema, pa.Schema)

        # Check if the pyarrow_schema fields match the expected fields
        expected_fields = [
            pa.field("temperature", pa.float64()),
            pa.field("humidity", pa.float32()),
            pa.field("pressure", pa.int32()),
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("location", pa.string()),
        ]
        self.assertEqual(result_schema, pa.schema(expected_fields))

    def test_create_schema(self):
        expected_fields = [
            pa.field("temperature", pa.float64()),
            pa.field("humidity", pa.float32()),
            pa.field("pressure", pa.int32()),
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("location", pa.string()),
        ]

        result_schema = create_pyarrow_schema(self.schema_dict)
        self.assertEqual(result_schema, pa.schema(expected_fields))

        result_schema = create_pyarrow_schema(self.schema_list)
        self.assertEqual(result_schema, pa.schema(expected_fields))

    def test_generate_json_schema_from_s3_netcdf(self):
        def assert_file_contents_equal(file1_path, file2_path):
            with open(file1_path, "r") as f1, open(file2_path, "r") as f2:
                content1 = f1.read()
                content2 = f2.read()
                assert json.loads(content1) == json.loads(
                    content2
                ), f"File contents do not match: {file1_path} != {file2_path}"

        try:
            loaded_schema_file = generate_json_schema_from_s3_netcdf(
                self.nc_s3_path, s3_fs=self.s3_fs, cloud_format="parquet"
            )
            assert_file_contents_equal(loaded_schema_file, TEST_FILE_NC_ANMN_SCHEMA)

        finally:
            os.remove(loaded_schema_file)

    def test_generate_json_schema_var_from_netcdf(self):
        json_expected = {
            "TEMP": {
                "type": "float",
                "ancillary_variables": "TEMP_quality_control",
                "long_name": "sea_water_temperature",
                "standard_name": "sea_water_temperature",
                "units": "degrees_Celsius",
                "valid_max": 40.0,
                "valid_min": -2.5,
            }
        }

        # Test from a local file
        json_output = generate_json_schema_var_from_netcdf(TEST_FILE_NC_ANMN, "TEMP")
        self.assertEqual(json_output, json.dumps(json_expected, indent=2))

        # Test from a s3fs file
        json_output = generate_json_schema_var_from_netcdf(
            self.s3_fs.open(self.nc_s3_path), "TEMP", s3_fs=self.s3_fs
        )
        self.assertEqual(json_output, json.dumps(json_expected, indent=2))

        # Test from a s3 file
        json_output = generate_json_schema_var_from_netcdf(
            self.nc_s3_path, "TEMP", s3_fs=self.s3_fs
        )
        self.assertEqual(json_output, json.dumps(json_expected, indent=2))


if __name__ == "__main__":
    unittest.main()
