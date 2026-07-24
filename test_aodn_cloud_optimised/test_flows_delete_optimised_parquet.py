"""
Unit tests for aodn_cloud_optimised.flows.delete_optimised_parquet.

The Prefect tasks are tested directly (bypassing the flow runner) so that
moto can intercept all boto3 calls without needing a live Prefect server.
The flow-level validation is tested via simple mocking.

``prefect.get_run_logger`` is patched to a standard Python logger so that
task functions can run outside a live Prefect context.
"""

import logging
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws

# ---------------------------------------------------------------------------
# Guard: skip the whole module if the optional 'flows' extras are not present.
# ---------------------------------------------------------------------------
prefect = cloudpathlib = None
try:
    import cloudpathlib
    import prefect
except ImportError:
    pass


@contextmanager
def _mock_prefect_logger():
    """Patch prefect.get_run_logger to return a standard Python logger."""
    logger = logging.getLogger("test_flows")
    with patch(
        "aodn_cloud_optimised.flows.delete_optimised_parquet.prefect.get_run_logger",
        return_value=logger,
    ):
        yield logger


@unittest.skipUnless(cloudpathlib and prefect, "flows extras not installed")
class TestListDatasetBucket(unittest.TestCase):
    """Tests for the list_dataset_bucket task."""

    @mock_aws
    def test_returns_all_objects_under_prefix(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            list_dataset_bucket,
        )

        bucket = "test-optimised-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)

        keys = [
            "dataset.parquet/year=2023/file_A-0.parquet",
            "dataset.parquet/year=2023/file_A-1.parquet",
            "dataset.parquet/year=2024/file_B-0.parquet",
        ]
        for key in keys:
            s3.put_object(Bucket=bucket, Key=key, Body=b"")

        client = cloudpathlib.S3Client(boto3_session=boto3.Session())
        root = client.S3Path(f"s3://{bucket}/dataset.parquet/")

        with _mock_prefect_logger():
            result = list_dataset_bucket.fn(root)

        self.assertEqual(len(result), len(keys))
        result_uris = sorted(p.as_uri() for p in result)
        expected_uris = sorted(f"s3://{bucket}/{k}" for k in keys)
        self.assertEqual(result_uris, expected_uris)

    @mock_aws
    def test_returns_empty_list_for_missing_prefix(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            list_dataset_bucket,
        )

        bucket = "test-optimised-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)

        client = cloudpathlib.S3Client(boto3_session=boto3.Session())
        root = client.S3Path(f"s3://{bucket}/nonexistent.parquet/")

        with _mock_prefect_logger():
            result = list_dataset_bucket.fn(root)
        self.assertEqual(result, [])


@unittest.skipUnless(cloudpathlib and prefect, "flows extras not installed")
class TestFindMatchedDeleteS3Paths(unittest.TestCase):
    """Tests for the find_matched_delete_s3_paths task."""

    def _make_paths(self, uris: list[str]) -> list:
        client = cloudpathlib.S3Client()
        return [client.S3Path(uri) for uri in uris]

    def test_matches_correct_files(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            find_matched_delete_s3_paths,
        )

        all_uris = [
            "s3://bucket/ds.parquet/year=2023/file_A-0.parquet",
            "s3://bucket/ds.parquet/year=2023/file_A-1.parquet",
            "s3://bucket/ds.parquet/year=2024/file_B-0.parquet",
            "s3://bucket/ds.parquet/year=2024/file_C-0.parquet",
        ]
        paths = self._make_paths(all_uris)

        with _mock_prefect_logger():
            matched = find_matched_delete_s3_paths.fn(
                dataset_s3_paths=paths,
                delete_file_names=["file_A", "file_B"],
            )

        matched_uris = sorted(p.as_uri() for p in matched)
        expected = sorted(
            [
                "s3://bucket/ds.parquet/year=2023/file_A-0.parquet",
                "s3://bucket/ds.parquet/year=2023/file_A-1.parquet",
                "s3://bucket/ds.parquet/year=2024/file_B-0.parquet",
            ]
        )
        self.assertEqual(matched_uris, expected)

    def test_no_matches_returns_empty(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            find_matched_delete_s3_paths,
        )

        paths = self._make_paths(
            ["s3://bucket/ds.parquet/year=2023/file_C-0.parquet"]
        )
        with _mock_prefect_logger():
            matched = find_matched_delete_s3_paths.fn(
                dataset_s3_paths=paths,
                delete_file_names=["file_A"],
            )
        self.assertEqual(matched, [])

    def test_regex_special_chars_in_filename_are_escaped(self):
        """Filenames with regex metacharacters must not cause errors."""
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            find_matched_delete_s3_paths,
        )

        paths = self._make_paths(
            ["s3://bucket/ds.parquet/year=2023/file.name-0.parquet"]
        )
        # "file.name" contains a dot which is a regex metachar
        with _mock_prefect_logger():
            matched = find_matched_delete_s3_paths.fn(
                dataset_s3_paths=paths,
                delete_file_names=["file.name"],
            )
        self.assertEqual(len(matched), 1)


@unittest.skipUnless(cloudpathlib and prefect, "flows extras not installed")
class TestDeleteMatchedFiles(unittest.TestCase):
    """Tests for the delete_matched_files task."""

    def _make_paths(self, bucket: str, keys: list[str]) -> list:
        client = cloudpathlib.S3Client()
        return [client.S3Path(f"s3://{bucket}/{k}") for k in keys]

    @mock_aws
    def test_dryrun_does_not_delete(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            delete_matched_files,
        )

        bucket = "test-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="ds.parquet/file_A-0.parquet", Body=b"")

        client = cloudpathlib.S3Client(boto3_session=boto3.Session())
        root = client.S3Path(f"s3://{bucket}/ds.parquet/")
        matched = self._make_paths(bucket, ["ds.parquet/file_A-0.parquet"])

        with _mock_prefect_logger():
            delete_matched_files.fn(
                dataset_s3_path=root,
                matched_delete_s3_paths=matched,
                pause_flow_run=False,
                dryrun=True,
            )

        # Object must still exist
        response = s3.list_objects_v2(Bucket=bucket)
        self.assertEqual(len(response.get("Contents", [])), 1)

    @mock_aws
    def test_deletes_files_when_not_dryrun(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            delete_matched_files,
        )

        bucket = "test-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        for i in range(3):
            s3.put_object(
                Bucket=bucket, Key=f"ds.parquet/file_A-{i}.parquet", Body=b""
            )

        client = cloudpathlib.S3Client(boto3_session=boto3.Session())
        root = client.S3Path(f"s3://{bucket}/ds.parquet/")
        matched = self._make_paths(
            bucket,
            [f"ds.parquet/file_A-{i}.parquet" for i in range(3)],
        )

        with _mock_prefect_logger():
            delete_matched_files.fn(
                dataset_s3_path=root,
                matched_delete_s3_paths=matched,
                pause_flow_run=False,
                dryrun=False,
            )

        response = s3.list_objects_v2(Bucket=bucket)
        self.assertEqual(response.get("KeyCount", 0), 0)

    @mock_aws
    def test_no_matched_files_is_noop(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            delete_matched_files,
        )

        bucket = "test-bucket"
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="ds.parquet/keep-0.parquet", Body=b"")

        client = cloudpathlib.S3Client(boto3_session=boto3.Session())
        root = client.S3Path(f"s3://{bucket}/ds.parquet/")

        with _mock_prefect_logger():
            delete_matched_files.fn(
                dataset_s3_path=root,
                matched_delete_s3_paths=[],
                pause_flow_run=False,
                dryrun=False,
            )

        # Original object untouched
        response = s3.list_objects_v2(Bucket=bucket)
        self.assertEqual(len(response.get("Contents", [])), 1)


@unittest.skipUnless(cloudpathlib and prefect, "flows extras not installed")
class TestDeleteOptimisedParquetFlow(unittest.TestCase):
    """Tests for the delete_optimised_parquet flow-level validation."""

    def _make_s3path(self, uri: str) -> "cloudpathlib.S3Path":
        return cloudpathlib.S3Client().S3Path(uri)

    def test_wrong_bucket_raises(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            delete_optimised_parquet,
        )

        mock_block = MagicMock()
        mock_block.bucket_name = "correct-bucket"

        with patch("prefect_aws.S3Bucket.load", return_value=mock_block):
            with self.assertRaises(RuntimeError, msg="wrong bucket should raise"):
                delete_optimised_parquet.fn(
                    dataset_s3_path=self._make_s3path(
                        "s3://wrong-bucket/ds.parquet/"
                    ),
                    delete_file_names=["file_A"],
                )

    def test_non_parquet_suffix_raises(self):
        from aodn_cloud_optimised.flows.delete_optimised_parquet import (
            delete_optimised_parquet,
        )

        mock_block = MagicMock()
        mock_block.bucket_name = "correct-bucket"

        with patch("prefect_aws.S3Bucket.load", return_value=mock_block):
            with self.assertRaises(RuntimeError, msg="non-parquet suffix should raise"):
                delete_optimised_parquet.fn(
                    dataset_s3_path=self._make_s3path(
                        "s3://correct-bucket/ds.zarr/"
                    ),
                    delete_file_names=["file_A"],
                )


if __name__ == "__main__":
    unittest.main()
