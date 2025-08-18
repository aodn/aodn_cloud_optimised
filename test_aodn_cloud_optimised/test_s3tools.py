import json
import unittest
from unittest.mock import MagicMock, Mock, patch

import boto3
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.s3Tools import (
    create_fileset,
    delete_objects_in_prefix,
    get_free_local_port,
    prefix_exists,
    s3_ls,
    split_s3_path,
)


@mock_aws()
class TestS3Tools(unittest.TestCase):
    def setUp(self):
        self.port = get_free_local_port()
        self.server = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port)
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

    def test_s3_ls_exclude(self):
        result = s3_ls(self.bucket_name, "prefix", exclude="e2")

        self.assertEqual(
            result,
            ["s3://test-bucket/prefix/file1.nc"],
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

    @patch("s3fs.S3FileSystem")
    def test_create_fileset(self, MockS3FileSystem):
        MockS3FileSystem.return_value = s3fs.S3FileSystem(
            anon=False, client_kwargs={"endpoint_url": f"http://127.0.0.1:{self.port}/"}
        )

        fileset = create_fileset("s3://test-bucket/prefix/file1.nc")
        self.assertEqual(fileset[0].path, "test-bucket/prefix/file1.nc")

    @patch.object(s3fs.S3FileSystem, "open")
    def test_create_fileset(self, mock_open):
        # Prepare a list of mock objects for each file path
        mock_files = []
        s3_paths = [
            "s3://test-bucket/prefix/file1.nc",
            "s3://test-bucket/prefix/file2.nc",
        ]

        for s3_path in s3_paths:
            mock_file = Mock()
            mock_file.path = s3_path
            mock_files.append(mock_file)

        # Configure mock_open to return the appropriate mock file for each call
        mock_open.side_effect = mock_files

        # Call the function under test
        fileset = create_fileset(s3_paths)

        # Assert that each file in the fileset has the correct path
        for idx, file_obj in enumerate(fileset):
            self.assertEqual(file_obj.path, s3_paths[idx])


if __name__ == "__main__":
    unittest.main()
