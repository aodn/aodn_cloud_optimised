import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from shapely.geometry import Polygon
from shapely import wkb

from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.config import load_dataset_config

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc",
)
DUMMY_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nan")
DUMMY_NC_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nc")
TEST_CSV_FILE = os.path.join(
    ROOT_DIR, "resources", "A69-1105-135_107799906_130722039.csv"
)
CONFIG_CSV_JSON = os.path.join(ROOT_DIR, "resources", "aatams_acoustic_tagging.json")


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

        self.tmp_dummy_file_path = os.path.join(
            self.tmp_dir, os.path.basename(DUMMY_FILE)
        )
        shutil.copy(DUMMY_FILE, self.tmp_dummy_file_path)

        self.tmp_dummy_nc_path = os.path.join(
            self.tmp_dir, os.path.basename(DUMMY_NC_FILE)
        )
        shutil.copy(DUMMY_NC_FILE, self.tmp_dummy_nc_path)

        dataset_config = {
            "dataset_name": "dummy",
            "cloud_optimised_format": "parquet",
            "metadata_uuid": "a681fdba-c6d9-44ab-90b9-113b0ed03536",
            "gattrs_to_variables": ["site_code"],
            "partition_keys": ["site_code", "timestamp", "polygon"],
            "time_extent": {"time": "TIME", "partition_timestamp_period": "Q"},
            "spatial_extent": {
                "lat": "LATITUDE",
                "lon": "LONGITUDE",
                "spatial_resolution": 5,
            },
            "schema": {
                "TIMESERIES": {"type": "int32"},
                "LATITUDE": {"type": "double"},
                "LONGITUDE": {"type": "double", "axis": "X"},
                "NOMINAL_DEPTH": {"type": "float", "standard_name": "depth"},
                "TEMP": {"type": "float", "standard_name": "sea_water_temperature"},
                "DUMMY1": {"type": "float"},
                "timestamp": {"type": "int64"},
                "site_code": {"type": "string"},
                "filename": {"type": "string"},
                "polygon": {"type": "string"},
                "TIME": {"type": "timestamp[ns]"},
            },
            "dataset_gattrs": {"title": "dummy"},
            "force_old_pq_del": True,
        }

        self.handler = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_nc_path),
            dataset_config=dataset_config,
        )

        self.handler_dummy_file = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_dummy_file_path),
            dataset_config=dataset_config,
        )

        self.handler_dummy_nc_file = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_dummy_nc_path),
            dataset_config=dataset_config,
        )

        # modify the path of the parquet dataset output
        self.handler.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_dataset_name"
        )

        self.tmp_csv_path = os.path.join(self.tmp_dir, os.path.basename(TEST_CSV_FILE))
        shutil.copy(TEST_CSV_FILE, self.tmp_csv_path)
        dataset_csv_config = load_dataset_config(CONFIG_CSV_JSON)
        self.handler_csv_file = GenericHandler(
            raw_bucket_name="dummy_raw_bucket",
            optimised_bucket_name="dummy_optimised_bucket",
            input_object_key=os.path.basename(self.tmp_csv_path),
            dataset_config=dataset_csv_config,
        )
        self.handler_csv_file.cloud_optimised_output_path = os.path.join(
            self.tmp_dir, "dummy_dataset_name"
        )

        # Create a mock object for xr.open_dataset
        # self.mock_open_dataset = MagicMock()

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
        self.assertEqual(ds.site_code, "SYD140")

    def test_add_columns_df(self):
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        # Call the method to add columns such as timestamp, sitecode ... to the DataFrame
        result_df = self.handler._add_timestamp_df(df)
        result_df = self.handler._add_columns_df(result_df, ds)

        # Check if the column are added with the correct values
        self.assertIn("site_code", result_df.columns)
        self.assertEqual(result_df["site_code"].iloc[0], "SYD140")
        self.assertEqual(
            result_df["filename"].iloc[0], os.path.basename(self.tmp_nc_path)
        )
        self.assertEqual(result_df["timestamp"].iloc[0], 1617235200.0)

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_create_data_parquet(self, mock_boto3_client):
        # Set up mock return values and inputs
        mock_s3_client = mock_boto3_client.return_value
        # Mock the return value of s3.download_file to simulate file download
        mock_s3_client.download_file.return_value = self.tmp_nc_path

        # Call the get_s3_raw_obj method (which should now use the mocked behavior)
        self.handler.get_s3_raw_obj()

        self.handler.tmp_input_file = self.tmp_nc_path  # overwrite value in handler

        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler.preprocess_data(self.tmp_nc_path)
        df, ds = next(generator)

        self.handler.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # The following section shows how the created polygon variable can be used to perform data queries. this adds significant overload, but is worth it
        df["converted_polygon"] = df["polygon"].apply(
            lambda x: wkb.loads(bytes.fromhex(x))
        )

        # Define the predefined polygon
        predefined_polygon_coords_out = [(150, -40), (155, -40), (155, -45), (150, -45)]
        predefined_polygon_coords_in = [(150, -32), (155, -32), (155, -45), (150, -45)]

        predefined_polygon_out = Polygon(predefined_polygon_coords_out)
        predefined_polygon_in = Polygon(predefined_polygon_coords_in)

        df_unique_polygon = df["converted_polygon"].unique()[0]
        self.assertFalse(df_unique_polygon.intersects(predefined_polygon_out))
        self.assertTrue(df_unique_polygon.intersects(predefined_polygon_in))

        # Assert the expected values in the Parquet dataset
        self.assertNotIn(
            "TEMP_quality_control", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertIn("TEMP", parquet_dataset.columns)

        # Testing the metadata sidecar file
        # Reading the metadata file of the dataset (at the root)
        parquet_meta_file_path = os.path.join(
            self.handler.cloud_optimised_output_path, "_common_metadata"
        )
        parquet_meta = pa.parquet.read_schema(parquet_meta_file_path)

        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(decoded_meta["LONGITUDE"]["axis"], "X")
        self.assertEqual(decoded_meta["NOMINAL_DEPTH"]["standard_name"], "depth")

        # alternative way to access the metadata
        # Create a dictionary where keys are the names and values are the elements
        schema_dict = {obj.name: obj for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"].metadata.get(b"standard_name"), b"sea_water_temperature"
        )
        # other way to access the metadata
        schema_dict = {obj.name: obj.metadata for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"][b"standard_name"], b"sea_water_temperature"
        )

    def test_create_csv_data_parquet(self):
        # Mock the return value of self.get_partition_parameters_data()
        # with patch.object(self.handler_no_schema, 'get_partition_parameters_data', return_value=["site_code"]) as mock_get_params:
        # Convert the Dataset to DataFrame using preprocess_data
        generator = self.handler_csv_file.preprocess_data(self.tmp_csv_path)
        df, ds = next(generator)

        self.handler_csv_file.publish_cloud_optimised(df, ds)

        # Read the Parquet dataset
        parquet_file_path = self.handler_csv_file.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        # For example, assert that the 'site_code' column is present and contains the expected value
        self.assertIn("station_name", parquet_dataset.columns)

        # Testing the metadata sidecar file

    @patch("aodn_cloud_optimised.lib.GenericParquetHandler.boto3.client")
    def test_handler_main_function(self, mock_boto3_client):
        # Set up mock return values and inputs
        mock_s3_client = mock_boto3_client.return_value
        # Mock the return value of s3.download_file to simulate file download
        mock_s3_client.download_file.return_value = self.tmp_nc_path
        self.handler.tmp_input_file = self.tmp_nc_path
        # Call the get_s3_raw_obj method (which should now use the mocked behavior)
        self.handler.to_cloud_optimised()

        # Read the Parquet dataset
        parquet_file_path = self.handler.cloud_optimised_output_path
        parquet_dataset = pd.read_parquet(parquet_file_path)

        # Assert the expected values in the Parquet dataset
        self.assertNotIn("station_name", parquet_dataset.columns)
        self.assertAlmostEqual(parquet_dataset["TEMP"][0], 13.2773, delta=1e-2)

    def test_dummies(self):
        with self.assertRaises(ValueError):
            self.handler_dummy_file.to_cloud_optimised()

        with self.assertRaises(TypeError):
            self.handler_dummy_nc_file.to_cloud_optimised()

    def tearDown(self):
        # Remove the temporary directory and its contents
        shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    unittest.main()
