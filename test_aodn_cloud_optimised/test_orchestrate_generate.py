"""Unit tests for aodn_cloud_optimised.bin.orchestrate.generate.

Functions are called directly as plain Python — no Prefect task/flow machinery,
no server, no parameter type validation.
"""

import copy
import unittest
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.bin.config.model.dataset_config import DatasetConfig
from aodn_cloud_optimised.bin.orchestrate.generate import (
    generate,
    optimise,
)

# ---------------------------------------------------------------------------
# Minimal config payload reused across tests
# ---------------------------------------------------------------------------
_MINIMAL_RUN_SETTINGS = {"paths": [{"s3_uri": "s3://imos-data/IMOS/", "type": "files"}]}

_MINIMAL_PARQUET_DATA = {
    "dataset_name": "test_parquet_dataset",
    "cloud_optimised_format": "parquet",
    "schema": {
        "TIME": {"type": "string", "units": "days since 1950-01-01"},
        "LATITUDE": {"type": "float64", "units": "degrees_north"},
        "LONGITUDE": {"type": "float64", "units": "degrees_east"},
    },
    "run_settings": _MINIMAL_RUN_SETTINGS,
    "schema_transformation": {
        "partitioning": [
            {
                "source_variable": "timestamp",
                "type": "time_extent",
                "time_extent": {"time_varname": "TIME", "partition_period": "M"},
            },
            {
                "source_variable": "polygon",
                "type": "spatial_extent",
                "spatial_extent": {
                    "lat_varname": "LATITUDE",
                    "lon_varname": "LONGITUDE",
                    "spatial_resolution": 5,
                },
            },
        ],
        "add_variables": {
            "timestamp": {
                "source": "@partitioning:time_extent",
                "schema": {"type": "int64", "units": "1"},
            },
            "polygon": {
                "source": "@partitioning:spatial_extent",
                "schema": {"type": "string", "units": "1"},
            },
        },
    },
}


# ---------------------------------------------------------------------------
# optimise task
# ---------------------------------------------------------------------------
class TestOptimiseTask(unittest.TestCase):
    def test_calls_to_cloud_optimised_with_file_list(self):
        mock_handler = MagicMock()
        file_list = ["s3://bucket/a.nc", "s3://bucket/b.nc"]

        optimise(mock_handler, file_list)

        mock_handler.to_cloud_optimised.assert_called_once_with(file_list)

    def test_passes_empty_list_to_handler(self):
        mock_handler = MagicMock()

        optimise(mock_handler, [])

        mock_handler.to_cloud_optimised.assert_called_once_with([])


# ---------------------------------------------------------------------------
# generate function
# ---------------------------------------------------------------------------
class TestGenerate(unittest.TestCase):
    def test_generate_calls_handler_with_collected_files(self):
        """End-to-end: generate() wires config → collect → handler → optimise."""
        dataset_config = DatasetConfig.model_validate(
            copy.deepcopy(_MINIMAL_PARQUET_DATA)
        )
        s3_files = ["s3://raw/a.nc", "s3://raw/b.nc"]

        mock_collector = MagicMock()
        mock_collector.collect.return_value = s3_files

        with patch(
            "aodn_cloud_optimised.lib.GenericParquetHandler.GenericHandler"
        ) as mock_handler_class:
            generate(
                dataset_config=dataset_config,
                file_collector=mock_collector,
                optimised_bucket_name="optimised-bucket",
                root_prefix_cloud_optimised_path="cloud_optimised",
            )

        mock_collector.collect.assert_called_once()
        mock_handler_class.return_value.to_cloud_optimised.assert_called_once_with(
            s3_files
        )


if __name__ == "__main__":
    unittest.main()
