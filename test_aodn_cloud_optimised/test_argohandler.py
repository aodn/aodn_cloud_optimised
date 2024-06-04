import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal

from aodn_cloud_optimised.lib.ArgoHandler import ArgoHandler
from aodn_cloud_optimised.lib.config import load_dataset_config

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC = os.path.join(ROOT_DIR, "resources", "2902093_prof.nc")
TEST_FILE_BAD_GEOM_NC = os.path.join(ROOT_DIR, "resources", "5905017_prof.nc")

CONFIG_JSON = os.path.join(ROOT_DIR, "resources", "argo_core.json")


class TestGenericHandler(unittest.TestCase):
    @patch("aodn_cloud_optimised.lib.ArgoHandler.ArgoHandler.get_s3_raw_obj")
    def setUp(self, mock_get_s3_raw_obj):
        # Create a temporary directory
        self.tmp_dir = tempfile.mkdtemp()

        # Copy the test NetCDF file to the temporary directory
        self.tmp_nc_path = os.path.join(self.tmp_dir, os.path.basename(TEST_FILE_NC))
        shutil.copy(TEST_FILE_NC, self.tmp_nc_path)

        self.tmp_nc_bad_geom_path = os.path.join(
            self.tmp_dir, os.path.basename(TEST_FILE_BAD_GEOM_NC)
        )
        shutil.copy(TEST_FILE_BAD_GEOM_NC, self.tmp_nc_bad_geom_path)

        dataset_config = load_dataset_config(CONFIG_JSON)

        self.handler = ArgoHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config,
        )

        self.handler_bad_geom = ArgoHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_bad_geom_path),
            dataset_config=dataset_config,
        )
        # modify the path of the parquet dataset output
        self.handler.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_table_name"
        )
        self.handler_bad_geom.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_table_name"
        )

        # Create a mock object for xr.open_dataset
        self.mock_open_dataset = MagicMock()

    # test method inherited from super
    @patch("aodn_cloud_optimised.lib.ArgoHandler.ArgoHandler.get_s3_raw_obj")
    def test_get_s3_raw_obj(self, mock_get_s3_raw_obj):
        with patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client"):
            mock_get_s3_raw_obj.return_value = self.tmp_nc_path
            tmp_filepath = self.handler.get_s3_raw_obj()
            self.assertEqual(tmp_filepath, self.tmp_nc_path)

    @patch("aodn_cloud_optimised.lib.ArgoHandler.ArgoHandler.get_s3_raw_obj")
    def test_data_to_df_ds(self, mock_get_s3_raw_obj):
        # Configure the mock object to return the path of the copied NetCDF file
        mock_get_s3_raw_obj.return_value = self.tmp_nc_path

        # Call the preprocess_data method
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Assert that ds.site_code is equal to the expected value
        assert_array_equal(np.unique(ds.PLATFORM_NUMBER.values), np.array([2902093]))

    def test_add_columns_df_and_bad_timestamps(self):
        # with patch.object(self.handler_no_schema, 'get_partition_parameters_data', return_value=["site_code"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Call the method to add columns to the DataFrame
        result_df = self.handler._add_timestamp_df(df)
        result_df = self.handler._add_columns_df(result_df, ds)

        # Check if the column are added with the correct values
        self.assertEqual(result_df["filename"][0], os.path.basename(self.tmp_nc_path))
        self.assertEqual(
            result_df["timestamp"][0], -9223372036.854776
        )  # This is a NAN value but all good!

        # now we call the next function to remove the bad timestamp values ( which does also a reindexing)
        result_df = self.handler._rm_bad_timestamp_df(result_df)
        self.assertEqual(result_df["timestamp"][0], 1356998400.0)

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_create_data_parquet(self, mock_boto3_client):
        # Set up mock return values and inputs
        mock_s3_client = mock_boto3_client.return_value
        # Mock the return value of s3.download_file to simulate file download
        mock_s3_client.download_file.return_value = self.tmp_nc_path

        # Call the get_s3_raw_obj method (which should now use the mocked behavior)
        self.handler.get_s3_raw_obj()

        self.handler.tmp_input_file = self.tmp_nc_path  # overwrite value in handler

        # Mock the return value of self.get_partition_parameters_data()
        # with patch.object(self.handler_no_schema, 'get_partition_parameters_data', return_value=["PLATFORM_NUMBER"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        self.handler.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        self.assertEqual(parquet_dataset["timestamp"][0], 1356998400.0)
        self.assertEqual(
            parquet_dataset["JULD"][0], pd.Timestamp("2013-02-26 03:15:00")
        )

        # Assert the expected values in the Parquet dataset
        self.assertNotIn(
            "PSAL_ADJUSTED", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertIn("TEMP_ADJUSTED", parquet_dataset.columns)

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_create_data_parquet_bad_geom(self, mock_boto3_client):
        # Set up mock return values and inputs
        mock_s3_client = mock_boto3_client.return_value
        # Mock the return value of s3.download_file to simulate file download
        mock_s3_client.download_file.return_value = self.tmp_nc_bad_geom_path

        # Call the get_s3_raw_obj method (which should now use the mocked behavior)
        self.handler_bad_geom.get_s3_raw_obj()

        self.handler_bad_geom.tmp_input_file = (
            self.tmp_nc_bad_geom_path
        )  # overwrite value in handler

        # Mock the return value of self.get_partition_parameters_data()
        # with patch.object(self.handler_no_schema, 'get_partition_parameters_data', return_value=["PLATFORM_NUMBER"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler_bad_geom.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        self.handler_bad_geom.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler_bad_geom.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        self.assertEqual(parquet_dataset["timestamp"][0], 1356998400.0)
        self.assertEqual(
            parquet_dataset["JULD"][0], pd.Timestamp("2013-02-26 03:15:00")
        )

        # Assert the expected values in the Parquet dataset
        self.assertNotIn(
            "PSAL_ADJUSTED", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertIn("TEMP_ADJUSTED", parquet_dataset.columns)

    def tearDown(self):
        # Remove the temporary directory and its contents
        shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    unittest.main()
