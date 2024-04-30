import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pandas as pd
from numpy.testing import assert_array_equal

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.config import load_dataset_config

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC = os.path.join(ROOT_DIR, 'resources', 'BOM_20240301_CAPE-SORELL_RT_WAVE-PARAMETERS_monthly.nc')

CONFIG_JSON = os.path.join(ROOT_DIR, 'resources', 'ardc_wave_nrt.json')


class TestGenericHandler(unittest.TestCase):

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj")
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
            "gattrs_to_variables": ["site_name", "water_depth", "wmo_id"],
            "partition_keys": ["site_name", "timestamp"],
            "time_extent": {
                "time": "TIME",
                "partition_timestamp_period": "M"
            },
            "spatial_extent": {
                "lat": "LATITUDE",
                "lon": "LONGITUDE",
                "spatial_resolution": 5
            },
            "schema": {},
            "dataset_gattrs": {
                "title": "ARDC glider"
            },
            "metadata_uuid": "b12b3-123bb-iijww",
            "force_old_pq_del": False
        }

        self.handler = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config_no_schema,
            force_old_pq_del=False
        )

        self.handler_with_schema = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config
        )

        # modify the path of the parquet dataset output
        self.handler.cloud_optimised_output_path = os.path.join(self.tmp_dir, "dummy_dataset_name")
        self.handler_with_schema.cloud_optimised_output_path = os.path.join(self.tmp_dir, "dummy_dataset_name")

        # Create a mock object for xr.open_dataset
        self.mock_open_dataset = MagicMock()

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj")
    def test_get_s3_raw_obj(self, mock_get_s3_raw_obj):
        with patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client"):
            mock_get_s3_raw_obj.return_value = self.tmp_nc_path
            tmp_filepath = self.handler.get_s3_raw_obj()
            self.assertEqual(tmp_filepath, self.tmp_nc_path)

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler.get_s3_raw_obj")
    def test_data_to_df_ds(self, mock_get_s3_raw_obj):
        # Configure the mock object to return the path of the copied NetCDF file
        mock_get_s3_raw_obj.return_value = self.tmp_nc_path

        # Call the preprocess_data method
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Assert that ds.site_code is equal to the expected value
        assert_array_equal(ds.site_name, 'Cape Sorell')

    def test_add_columns_df_and_bad_timestamps(self):
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Call the method to add columns to the DataFrame
        result_df = self.handler._add_timestamp_df(df)
        result_df = self.handler._add_columns_df(result_df, ds)

        # now we call the next function to remove the bad timestamp values ( which does also a reindexing)
        result_df = self.handler._rm_bad_timestamp_df(result_df)
        self.assertEqual(result_df['timestamp'][0], 1709251200.0)

    def test_create_data_parquet_with_mocked_parameters_and_with_schema(self):
        # Mock the return value of self.get_partition_parameters_data()
        #with patch.object(self.handler, 'get_partition_parameters_data', return_value=["site_name"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler_with_schema.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        self.handler_with_schema.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler_with_schema.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        self.assertNotIn('DUMMY_VAR_NOT_IN', parquet_dataset.columns)  # make sure the variable is removed
        self.assertIn('WHTH', parquet_dataset.columns)

        self.assertEqual(parquet_dataset['timestamp'][0], 1709251200.0)
        self.assertEqual(parquet_dataset['TIME'][0], pd.Timestamp('2024-03-01 01:30:00'))

        # Testing the metadata sidecar file
        # Reading the metadata file of the dataset (at the root)
        parquet_meta_file_path = os.path.join(self.handler_with_schema.cloud_optimised_output_path, '_common_metadata')
        parquet_meta = pa.parquet.read_schema(parquet_meta_file_path)
        import json
        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {key.decode('utf-8'): json.loads(value.decode('utf-8').replace("'", '"')) for key, value in
                        parquet_meta.metadata.items()}

        self.assertEqual(decoded_meta['dataset_metadata']['metadata_uuid'], '2807f3aa-4db0-4924-b64b-354ae8c10b58')
        self.assertEqual(decoded_meta['dataset_metadata']['title'],'ARDC')

    def tearDown(self):
        # Remove the temporary directory and its contents
        shutil.rmtree(self.tmp_dir)


if __name__ == '__main__':
    unittest.main()
