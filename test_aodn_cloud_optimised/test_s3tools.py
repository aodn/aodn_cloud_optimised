import json
import unittest

import boto3
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.s3Tools import (
    s3_ls,
    create_fileset,
    split_s3_path,
    delete_objects_in_prefix,
    prefix_exists,
)


@mock_aws()
class TestS3Ls(unittest.TestCase):
    def setUp(self):

        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=5555)
        self.server.start()

        # Create a mock S3 service
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-bucket"
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
        self.s3.put_object(
            Bucket=self.bucket_name, Key="prefix/file1.nc", Body=""
        )  # empty file
        self.s3.put_object(
            Bucket=self.bucket_name, Key="prefix/file2.nc", Body=""
        )  # empty file

    def tearDown(self):
        self.server.stop()

    def test_s3_ls(self):
        result = s3_ls(self.bucket_name, "prefix")

        self.assertEqual(
            result,
            ["s3://test-bucket/prefix/file1.nc", "s3://test-bucket/prefix/file2.nc"],
        )

    def test_split(self):
        bucket_name, key = split_s3_path("s3://test-bucket/prefix/file1.nc")
        self.assertEqual(bucket_name, "test-bucket")
        self.assertEqual(key, "prefix/file1.nc")

    def test_prefix_exists(self):
        self.assertTrue(prefix_exists("s3://test-bucket/prefix/file1.nc"))

    def test_delete_objects_in_prefix(self):
        delete_objects_in_prefix("test-bucket", "prefix")
        result = s3_ls(self.bucket_name, "prefix")
        self.assertListEqual(result, [])

    def test_create_fileset(self):
        fileset = create_fileset("s3://test-bucket/prefix/file1.nc")
        self.assertEqual(fileset[0].path, "test-bucket/prefix/file1.nc")


if __name__ == "__main__":
    unittest.main()
