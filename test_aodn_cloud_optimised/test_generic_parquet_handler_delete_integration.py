import logging
import unittest

import boto3
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.s3Tools import get_free_local_port


@mock_aws
class TestGenericParquetHandlerDeleteIntegration(unittest.TestCase):
    def setUp(self):
        self.bucket = "optimised-bucket"
        self.dataset_key_root = "prefix/dataset.parquet"
        self.dataset_uri = f"s3://{self.bucket}/{self.dataset_key_root}"

        self.port = get_free_local_port()
        self.endpoint_ip = "127.0.0.1"
        self.server = ThreadedMotoServer(ip_address=self.endpoint_ip, port=self.port)
        self.server.start()

        self.s3_client_opts_common = {
            "service_name": "s3",
            "region_name": "us-east-1",
            "endpoint_url": f"http://{self.endpoint_ip}:{self.port}",
        }
        self.s3 = boto3.client(**self.s3_client_opts_common)
        self.s3.create_bucket(Bucket=self.bucket)

        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": f"http://{self.endpoint_ip}:{self.port}/",
                "region_name": "us-east-1",
            },
        )

        self._write_mock_hive_dataset()

        self.handler = GenericHandler.__new__(GenericHandler)
        self.handler.logger = logging.getLogger(__name__)
        self.handler.cloud_optimised_output_path = self.dataset_uri
        self.handler.s3_fs_output = self.s3_fs

    def tearDown(self):
        objects = self.s3.list_objects_v2(Bucket=self.bucket).get("Contents", [])
        for obj in objects:
            self.s3.delete_object(Bucket=self.bucket, Key=obj["Key"])
        self.s3.delete_bucket(Bucket=self.bucket)
        self.server.stop()

    def _write_mock_hive_dataset(self):
        partitions = [
            ("platform=A", "a.nc-0.parquet", [1, 2]),
            ("platform=B", "b.nc-1.parquet", [3, 4]),
        ]

        for partition_dir, parquet_name, values in partitions:
            table = pa.table({"value": values})
            parquet_uri = f"{self.dataset_uri}/{partition_dir}/{parquet_name}"
            pq.write_table(table, parquet_uri, filesystem=self.s3_fs)

    def test_mock_dataset_files_are_bucket_prefixed(self):
        ds = pds.dataset(
            source=f"{self.bucket}/{self.dataset_key_root}",
            partitioning="hive",
            filesystem=self.s3_fs,
        )

        self.assertEqual(len(ds.files), 2)
        self.assertTrue(
            all(
                file.startswith(f"{self.bucket}/{self.dataset_key_root}/")
                for file in ds.files
            )
        )

    def test_list_dataset_bucket_with_real_pyarrow_dataset(self):
        bucket, keys = self.handler.list_dataset_bucket()

        expected_keys = [
            f"{self.dataset_key_root}/platform=A/a.nc-0.parquet",
            f"{self.dataset_key_root}/platform=B/b.nc-1.parquet",
        ]
        self.assertEqual(bucket, self.bucket)
        self.assertEqual(sorted(keys), sorted(expected_keys))


if __name__ == "__main__":
    unittest.main()
