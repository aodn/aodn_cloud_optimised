import os
import shutil
import tempfile
import unittest
import yaml
from unittest.mock import patch, MagicMock

import pandas as pd
from numpy.testing import assert_array_equal

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.config import load_dataset_config

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC = os.path.join(
    ROOT_DIR, "resources", "IMOS_SOOP-XBT_T_20240218T141800Z_IX28_FV00_ID_9797539.nc"
)

CONFIG_JSON = os.path.join(ROOT_DIR, "resources", "soop_xbt_nrt.json")


class TestGenericHandler(unittest.TestCase):
    @patch(
        "aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj"
    )
    def setUp(self, mock_get_s3_raw_obj):
        # Create a temporary directory
        self.tmp_dir = tempfile.mkdtemp()

        # Copy the test NetCDF file to the temporary directory
        self.tmp_nc_path = os.path.join(self.tmp_dir, os.path.basename(TEST_FILE_NC))
        shutil.copy(TEST_FILE_NC, self.tmp_nc_path)

        dataset_config = load_dataset_config(CONFIG_JSON)

        dataset_config_no_schema = {
            "dataset_name": "dummy_table_name",
            "cloud_optimised_format": "parquet",
            "metadata_uuid": "35234913-aa3c-48ec-b9a4-77f822f66ef8",
            "gattrs_to_variables": ["XBT_line", "ship_name", "Callsign", "imo_number"],
            "partition_keys": ["XBT_line", "timestamp"],
            "time_extent": {"time": "TIME", "partition_timestamp_period": "M"},
            "spatial_extent": {
                "lat": "LATITUDE",
                "lon": "LONGITUDE",
                "spatial_resolution": 5,
            },
            "schema": {},
            "dataset_gattrs": {"title": "SOOP XBT NRT"},
            "force_old_pq_del": False,
        }

        self.handler = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config_no_schema,
            force_old_pq_del=False,
        )

        self.handler_with_schema = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config,
        )

        # modify the path of the parquet dataset output
        self.handler.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_dataset_name"
        )
        self.handler_with_schema.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_dataset_name"
        )

        # Create a mock object for xr.open_dataset
        self.mock_open_dataset = MagicMock()

    # test method inherited from super
    @patch(
        "aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj"
    )
    def test_get_s3_raw_obj(self, mock_get_s3_raw_obj):
        with patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client"):
            mock_get_s3_raw_obj.return_value = self.tmp_nc_path
            tmp_filepath = self.handler.get_s3_raw_obj()
            self.assertEqual(tmp_filepath, self.tmp_nc_path)

    @patch(
        "aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj"
    )
    def test_data_to_df_ds(self, mock_get_s3_raw_obj):
        # Configure the mock object to return the path of the copied NetCDF file
        mock_get_s3_raw_obj.return_value = self.tmp_nc_path

        # Call the preprocess_data method
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Assert that ds.site_code is equal to the expected value
        assert_array_equal(ds.XBT_line, "IX28")

    def test_add_columns_df_and_bad_timestamps(self):
        # with patch.object(self.handler_no_schema, 'get_partition_parameters_data', return_value=["XBT_line"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Call the method to add columns to the DataFrame
        result_df = self.handler._add_timestamp_df(df)
        result_df = self.handler._add_columns_df(result_df, ds)

        # now we call the next function to remove the bad timestamp values ( which does also a reindexing)
        result_df = self.handler._rm_bad_timestamp_df(result_df)
        self.assertEqual(result_df["timestamp"][0], 1706745600.0)
        self.assertEqual(result_df.index.name, "DEPTH")
        self.assertEqual(
            result_df.filename[0],
            "IMOS_SOOP-XBT_T_20240218T141800Z_IX28_FV00_ID_9797539.nc",
        )

    def test_create_data_parquet_with_mocked_parameters_and_with_schema(self):
        # Mock the return value of self.get_partition_parameters_data()
        # with patch.object(self.handler, 'get_partition_parameters_data', return_value=["XBT_line"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler_with_schema.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        self.handler_with_schema.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler_with_schema.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        self.assertNotIn(
            "DUMMY_VAR_NOT_IN", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertIn("TEMP", parquet_dataset.columns)

        self.assertEqual(parquet_dataset["timestamp"][0], 1706745600.0)
        self.assertEqual(
            parquet_dataset["TIME"][0], pd.Timestamp("2024-02-18 14:18:00")
        )

    def test_push_metadata_aws_registry(self):

        # Load the registry config from the JSON file
        expected_yaml_data = yaml.dump(
            self.handler_with_schema.dataset_config["aws_opendata_registry"]
        )

        with patch("boto3.client") as mock_client:
            mock_s3_client = MagicMock()
            mock_client.return_value = mock_s3_client

            self.handler_with_schema.push_metadata_aws_registry()

            # Assert that put_object was called with the correct parameters
            # expected_key = os.path.join(self.tmp_dir, 'dummy_dataset_name/soop_xbt_nrt.yaml')
            expected_key = os.path.join("parquet/loz_test/soop_xbt_nrt.yaml")

            mock_s3_client.put_object.assert_called_once_with(
                Bucket=self.handler_with_schema.optimised_bucket_name,
                Key=expected_key,
                Body=expected_yaml_data.encode("utf-8"),
            )

    def tearDown(self):
        # Remove the temporary directory and its contents
        shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    unittest.main()
