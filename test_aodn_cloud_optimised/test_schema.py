import json
import unittest
from unittest.mock import patch, mock_open, MagicMock

import pyarrow as pa
import xarray as xr

from aodn_cloud_optimised.lib.schema import (
    generate_pyarrow_schema_from_s3_netcdf,
    create_pyarrow_schema_from_list,
    create_pyrarrow_schema_from_dict,
    create_pyarrow_schema,
    generate_json_schema_from_s3_netcdf,
)


class TestNetCDFSchemaGeneration(unittest.TestCase):
    def setUp(self):
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
        self.sub_schema = create_pyarrow_schema_from_list(self.sub_schema_strings)

    @patch("aodn_cloud_optimised.lib.schema.xr.open_dataset")
    @patch("aodn_cloud_optimised.lib.schema.s3fs.S3FileSystem")
    def test_generate_pyarrow_schema_from_s3_netcdf(
        self, mock_s3fs, mock_xr_open_dataset
    ):
        # Mock S3 file access and Xarray open_dataset function
        mock_s3 = MagicMock()
        mock_s3.open.return_value = mock_open(read_data=b"dummy_data").return_value
        mock_s3fs.return_value = mock_s3

        # Mock Xarray open_dataset function to return a dummy dataset. We really don't care at this stage of the values
        dummy_dataset = xr.Dataset(
            {
                "temperature": xr.DataArray([1, 2, 3]),
                "humidity": xr.DataArray([4, 5, 6]),
                "pressure": xr.DataArray([7, 8, 9]),
                "timestamp": xr.DataArray([10, 11, 12]),
                "location": xr.DataArray(["A", "B", "C"]),
            }
        )
        mock_xr_open_dataset.return_value = dummy_dataset

        # Test the function with mocked S3 file access
        result_schema = generate_pyarrow_schema_from_s3_netcdf(
            "dummy_s3_path", self.sub_schema
        )

        # Check if the result pyarrow_schema is an instance of PyArrow pyarrow_schema
        self.assertIsInstance(result_schema, pa.Schema)

        # You can add more assertions based on your expected behavior

    def test_create_schema_from_list(self):
        # Test pyarrow_schema creation from pyarrow_schema strings
        result_schema = create_pyarrow_schema_from_list(self.schema_list)

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

    def test_create_schema_from_dict(self):
        # Test pyarrow_schema creation from pyarrow_schema strings
        result_schema = create_pyrarrow_schema_from_dict(self.schema_dict)

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

    @patch("aodn_cloud_optimised.lib.schema.xr.open_dataset")
    @patch("aodn_cloud_optimised.lib.schema.s3fs.S3FileSystem")
    def test_generate_json_schema_from_s3_netcdf(self, mock_s3fs, mock_xr_open_dataset):
        # Mock the S3 file system
        mock_s3 = MagicMock()
        mock_s3fs.return_value = mock_s3

        # Mock the open_dataset method to return a dataset
        mock_dataset = MagicMock()
        mock_xr_open_dataset.return_value = mock_dataset

        # Define mock dataset variables and coordinates
        mock_dataset.variables = {
            "lon": MagicMock(dtype="float32", attrs={"units": "degrees_east"}),
            "lat": MagicMock(dtype="float32", attrs={"units": "degrees_north"}),
            "time": MagicMock(
                dtype="datetime64[ns]", attrs={"units": "seconds since 1970-01-01"}
            ),
        }
        mock_dataset.coords = {}

        # Call the function under test
        temp_file_path = generate_json_schema_from_s3_netcdf(
            "s3://your-bucket/path/to/file.nc"
        )

        # Load the generated JSON schema
        with open(temp_file_path, "r") as json_file:
            loaded_schema = json.load(json_file)

        # Assert the content of the loaded schema
        expected_schema = {
            "lon": {"type": "float", "units": "degrees_east"},
            "lat": {"type": "float", "units": "degrees_north"},
            "time": {"type": "timestamp[ns]", "units": "seconds since 1970-01-01"},
        }
        self.assertEqual(loaded_schema, expected_schema)


if __name__ == "__main__":
    unittest.main()
