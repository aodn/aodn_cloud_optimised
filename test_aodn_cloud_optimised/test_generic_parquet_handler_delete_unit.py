import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler


class TestGenericParquetDeleteHelpers(unittest.TestCase):
    def setUp(self):
        self.handler = GenericHandler.__new__(GenericHandler)
        self.handler.logger = logging.getLogger(__name__)
        self.handler.cloud_optimised_output_path = (
            "s3://optimised-bucket/prefix/dataset.parquet"
        )
        self.handler.s3_fs_output = object()

    def test_list_dataset_bucket_returns_bucket_and_keys(self):
        files = [
            "optimised-bucket/prefix/dataset.parquet/a.nc-0.parquet",
            "optimised-bucket/prefix/dataset.parquet/b.nc-1.parquet",
        ]
        with patch(
            "aodn_cloud_optimised.lib.GenericParquetHandler.pds.dataset",
            return_value=SimpleNamespace(files=files),
        ) as mock_dataset:
            bucket, keys = self.handler.list_dataset_bucket()

        self.assertEqual(bucket, "optimised-bucket")
        self.assertEqual(
            keys,
            [
                "prefix/dataset.parquet/a.nc-0.parquet",
                "prefix/dataset.parquet/b.nc-1.parquet",
            ],
        )
        mock_dataset.assert_called_once_with(
            source="optimised-bucket/prefix/dataset.parquet",
            partitioning="hive",
            filesystem=self.handler.s3_fs_output,
        )

    def test_list_dataset_bucket_raises_indexerror_for_empty_dataset(self):
        with patch(
            "aodn_cloud_optimised.lib.GenericParquetHandler.pds.dataset",
            return_value=SimpleNamespace(files=[]),
        ):
            with self.assertRaises(IndexError):
                self.handler.list_dataset_bucket()

    def test_list_dataset_bucket_reraises_storage_errors(self):
        with patch(
            "aodn_cloud_optimised.lib.GenericParquetHandler.pds.dataset",
            side_effect=FileNotFoundError("missing dataset"),
        ):
            with self.assertRaises(FileNotFoundError):
                self.handler.list_dataset_bucket()

    def test_find_matched_keys_matches_single_filename_default_pattern(self):
        keys = [
            "bucket/prefix/a.nc-0.parquet",
            "bucket/prefix/a.nc-not-number.parquet",
            "bucket/prefix/other.nc-3.parquet",
        ]
        matched = self.handler.find_matched_keys(keys=keys, filenames=["a.nc"])
        self.assertEqual(matched, ["bucket/prefix/a.nc-0.parquet"])

    def test_find_matched_keys_matches_multiple_filenames(self):
        keys = [
            "bucket/prefix/a.nc-0.parquet",
            "bucket/prefix/b.nc-2.parquet",
            "bucket/prefix/c.nc-3.parquet",
        ]
        matched = self.handler.find_matched_keys(keys=keys, filenames=["a.nc", "c.nc"])
        self.assertEqual(
            matched,
            ["bucket/prefix/a.nc-0.parquet", "bucket/prefix/c.nc-3.parquet"],
        )

    def test_find_matched_keys_escapes_filename_regex_characters(self):
        keys = [
            "bucket/prefix/file.name+v1.nc-0.parquet",
            "bucket/prefix/fileXnameYv1.nc-0.parquet",
        ]
        matched = self.handler.find_matched_keys(
            keys=keys, filenames=["file.name+v1.nc"]
        )
        self.assertEqual(matched, ["bucket/prefix/file.name+v1.nc-0.parquet"])

    def test_find_matched_keys_returns_empty_when_no_match(self):
        keys = ["bucket/prefix/a.nc-0.parquet", "bucket/prefix/b.nc-2.parquet"]
        matched = self.handler.find_matched_keys(keys=keys, filenames=["missing.nc"])
        self.assertEqual(matched, [])

    def test_find_matched_keys_supports_custom_pattern_template(self):
        keys = ["bucket/prefix/a.nc-v1.parquet", "bucket/prefix/a.nc-0.parquet"]
        matched = self.handler.find_matched_keys(
            keys=keys,
            filenames=["a.nc"],
            pattern_template=r"-v\d+\.parquet$",
        )
        self.assertEqual(matched, ["bucket/prefix/a.nc-v1.parquet"])

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_delete_matched_keys_no_matches_exits_early(self, mock_client_factory):
        self.handler.delete_matched_keys(bucket="bucket", matched_keys=[], dryrun=False)
        mock_client_factory.assert_not_called()

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_delete_matched_keys_dryrun_exits_early(self, mock_client_factory):
        self.handler.delete_matched_keys(
            bucket="bucket",
            matched_keys=["k1"],
            dryrun=True,
        )
        mock_client_factory.assert_not_called()

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_delete_matched_keys_deletes_single_batch(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client.delete_objects.return_value = {"Errors": []}
        mock_client_factory.return_value = mock_client

        self.handler.delete_matched_keys(
            bucket="bucket",
            matched_keys=["k1", "k2"],
            dryrun=False,
        )

        mock_client_factory.assert_called_once_with("s3")
        mock_client.delete_objects.assert_called_once()
        call_kwargs = mock_client.delete_objects.call_args.kwargs
        self.assertEqual(call_kwargs["Bucket"], "bucket")
        self.assertEqual(
            call_kwargs["Delete"]["Objects"], [{"Key": "k1"}, {"Key": "k2"}]
        )
        self.assertTrue(call_kwargs["Delete"]["Quiet"])

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_delete_matched_keys_chunks_in_batches_of_1000(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client.delete_objects.return_value = {"Errors": []}
        mock_client_factory.return_value = mock_client

        keys = [f"k{i}" for i in range(1001)]
        self.handler.delete_matched_keys(
            bucket="bucket",
            matched_keys=keys,
            dryrun=False,
        )

        self.assertEqual(mock_client.delete_objects.call_count, 2)
        first_call = mock_client.delete_objects.call_args_list[0].kwargs["Delete"][
            "Objects"
        ]
        second_call = mock_client.delete_objects.call_args_list[1].kwargs["Delete"][
            "Objects"
        ]
        self.assertEqual(len(first_call), 1000)
        self.assertEqual(len(second_call), 1)

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_delete_matched_keys_raises_on_s3_errors(self, mock_client_factory):
        mock_client = MagicMock()
        mock_client.delete_objects.return_value = {
            "Errors": [{"Key": "k1", "Code": "AccessDenied"}]
        }
        mock_client_factory.return_value = mock_client

        with self.assertRaises(RuntimeError):
            self.handler.delete_matched_keys(
                bucket="bucket",
                matched_keys=["k1"],
                dryrun=False,
            )

    def test_delete_existing_matching_parquet_requires_exactly_one_input(self):
        with self.assertRaises(ValueError):
            self.handler.delete_existing_matching_parquet()
        with self.assertRaises(ValueError):
            self.handler.delete_existing_matching_parquet(
                filename="a.nc", filenames=["a.nc"]
            )

    def test_delete_existing_matching_parquet_with_filename_calls_delete_flow(self):
        self.handler.list_dataset_bucket = MagicMock(
            return_value=("bucket", ["k1", "k2"])
        )
        self.handler.find_matched_keys = MagicMock(return_value=["k1"])
        self.handler.delete_matched_keys = MagicMock()

        self.handler.delete_existing_matching_parquet(filename="a.nc")

        self.handler.list_dataset_bucket.assert_called_once_with()
        self.handler.find_matched_keys.assert_called_once_with(
            keys=["k1", "k2"], filenames=["a.nc"]
        )
        self.handler.delete_matched_keys.assert_called_once_with(
            bucket="bucket",
            matched_keys=["k1"],
            dryrun=False,
        )

    def test_delete_existing_matching_parquet_with_filenames_calls_delete_flow(self):
        self.handler.list_dataset_bucket = MagicMock(
            return_value=("bucket", ["k1", "k2"])
        )
        self.handler.find_matched_keys = MagicMock(return_value=["k1", "k2"])
        self.handler.delete_matched_keys = MagicMock()

        self.handler.delete_existing_matching_parquet(filenames=["a.nc", "b.nc"])

        self.handler.find_matched_keys.assert_called_once_with(
            keys=["k1", "k2"], filenames=["a.nc", "b.nc"]
        )
        self.handler.delete_matched_keys.assert_called_once_with(
            bucket="bucket",
            matched_keys=["k1", "k2"],
            dryrun=False,
        )

    def test_delete_existing_matching_parquet_skips_delete_when_no_matches(self):
        self.handler.list_dataset_bucket = MagicMock(
            return_value=("bucket", ["k1", "k2"])
        )
        self.handler.find_matched_keys = MagicMock(return_value=[])
        self.handler.delete_matched_keys = MagicMock()

        self.handler.delete_existing_matching_parquet(filename="a.nc")

        self.handler.delete_matched_keys.assert_not_called()

    def test_delete_existing_matching_parquet_noop_when_dataset_missing(self):
        self.handler.list_dataset_bucket = MagicMock(
            side_effect=FileNotFoundError("dataset not found")
        )
        self.handler.find_matched_keys = MagicMock()
        self.handler.delete_matched_keys = MagicMock()

        self.handler.delete_existing_matching_parquet(filename="a.nc")

        self.handler.find_matched_keys.assert_not_called()
        self.handler.delete_matched_keys.assert_not_called()


if __name__ == "__main__":
    unittest.main()
