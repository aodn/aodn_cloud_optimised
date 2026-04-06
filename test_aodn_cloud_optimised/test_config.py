import copy
import json
import os
import pathlib
import shutil
import tempfile
import unittest

import pydantic
import yaml

import aodn_cloud_optimised.config.dataset
from aodn_cloud_optimised.bin.config.model.dataset_config import DatasetConfig
from aodn_cloud_optimised.lib.config import (
    load_dataset_config,
    load_variable_from_config,
    load_variable_from_file,
)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

DATASET_CONFIG_NC_ACORN_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json",
)

# ---------------------------------------------------------------------------
# Minimal valid config fixtures used to test DatasetConfig validators in
# isolation, without depending on the completeness of any specific repo config
# file. Parquet requires polygon/timestamp partitions; zarr requires
# var_template_shape and at least one dimension with append_dim=True.
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

_MINIMAL_ZARR_DATA = {
    "dataset_name": "test_zarr_dataset",
    "cloud_optimised_format": "zarr",
    "schema": {
        "TIME": {"type": "float64", "units": "days since 1950-01-01"},
    },
    "run_settings": _MINIMAL_RUN_SETTINGS,
    "schema_transformation": {
        "var_template_shape": "TIME",
        "dimensions": {
            "time": {"name": "TIME", "chunk": 100, "append_dim": True},
        },
    },
}


class TestLoadVariableFromFile(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Create test JSON file
        self.json_file_path = os.path.join(self.temp_dir, "test_data.json")
        json_data = {"var1": "value1", "var2": "value2"}
        with open(self.json_file_path, "w") as json_file:
            json.dump(json_data, json_file)

        # Create test YAML file
        self.yaml_file_path = os.path.join(self.temp_dir, "test_data.yaml")
        yaml_data = {"var3": "value3", "var4": "value4"}
        with open(self.yaml_file_path, "w") as yaml_file:
            yaml.dump(yaml_data, yaml_file)

    def tearDown(self):
        # Remove the temporary directory and its contents
        shutil.rmtree(self.temp_dir)

    def test_load_variable_from_file_json(self):
        variable_name = "var1"
        value = load_variable_from_file(self.json_file_path, variable_name)
        self.assertEqual(value, "value1")

    def test_load_variable_from_file_yaml(self):
        variable_name = "var4"
        value = load_variable_from_file(self.yaml_file_path, variable_name)
        self.assertEqual(value, "value4")

    def test_load_variable_from_file_variable_not_found(self):
        variable_name = "var5"
        with self.assertRaises(KeyError):
            load_variable_from_file(self.json_file_path, variable_name)

    def test_load_variable_from_file_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            load_variable_from_file(
                os.path.join(self.temp_dir, "non_existent_file.json"), "var1"
            )

    def test_load_parent_child_config(self):
        dataset_acorn_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ACORN_JSON)
        cloud_optimised_format = dataset_acorn_netcdf_config["cloud_optimised_format"]
        self.assertEqual(
            "zarr", cloud_optimised_format
        )  # attribute only found in parent record


class TestDatasetConfigMergeDicts(unittest.TestCase):
    """Unit tests for DatasetConfig._merge_dicts class method."""

    def test_basic_merge_non_overlapping_keys(self):
        result = DatasetConfig._merge_dicts({"a": 1, "b": 2}, {"c": 3})
        self.assertEqual(result, {"a": 1, "b": 2, "c": 3})

    def test_child_overrides_parent_for_same_key(self):
        result = DatasetConfig._merge_dicts(
            {"key": "parent", "other": "keep"}, {"key": "child"}
        )
        self.assertEqual(result["key"], "child")
        self.assertEqual(result["other"], "keep")

    def test_nested_dicts_are_merged_recursively(self):
        result = DatasetConfig._merge_dicts(
            {"nested": {"x": 1, "y": 2}},
            {"nested": {"y": 99, "z": 3}},
        )
        self.assertEqual(result["nested"], {"x": 1, "y": 99, "z": 3})

    def test_schema_key_in_child_replaces_parent_entirely(self):
        """The 'schema' key must not be merged — child schema replaces parent schema."""
        result = DatasetConfig._merge_dicts(
            {"schema": {"old_var": {"type": "int64"}}},
            {"schema": {"new_var": {"type": "string"}}},
        )
        self.assertNotIn("old_var", result["schema"])
        self.assertIn("new_var", result["schema"])

    def test_empty_child_returns_parent_unchanged(self):
        result = DatasetConfig._merge_dicts({"a": 1}, {})
        self.assertEqual(result, {"a": 1})

    def test_empty_parent_becomes_child(self):
        result = DatasetConfig._merge_dicts({}, {"a": 1})
        self.assertEqual(result, {"a": 1})


class TestDatasetConfigLoadJson(unittest.TestCase):
    """Unit tests for DatasetConfig._load_dataset_config_json class method."""

    def setUp(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_missing_file_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            DatasetConfig._load_dataset_config_json(self.temp_dir / "missing.json")

    def test_non_json_extension_raises_value_error(self):
        txt_file = self.temp_dir / "config.txt"
        txt_file.write_text('{"key": "value"}')
        with self.assertRaises(ValueError):
            DatasetConfig._load_dataset_config_json(txt_file)

    def test_valid_json_is_loaded_as_dict(self):
        json_file = self.temp_dir / "config.json"
        data = {"dataset_name": "test", "cloud_optimised_format": "parquet"}
        json_file.write_text(json.dumps(data))
        result = DatasetConfig._load_dataset_config_json(json_file)
        self.assertEqual(result["dataset_name"], "test")


class TestDatasetConfigCustomValidators(unittest.TestCase):
    """Tests for the custom business-logic validators on DatasetConfig."""

    def test_valid_parquet_config(self):
        config = DatasetConfig.model_validate(copy.deepcopy(_MINIMAL_PARQUET_DATA))
        self.assertEqual(config.cloud_optimised_format, "parquet")

    def test_valid_zarr_config(self):
        config = DatasetConfig.model_validate(copy.deepcopy(_MINIMAL_ZARR_DATA))
        self.assertEqual(config.cloud_optimised_format, "zarr")

    def test_placeholder_in_top_level_field_raises(self):
        data = copy.deepcopy(_MINIMAL_PARQUET_DATA)
        data["metadata_uuid"] = "FILL UP MANUALLY - CHECK DOCUMENTATION"
        with self.assertRaises(pydantic.ValidationError):
            DatasetConfig.model_validate(data)

    def test_placeholder_nested_in_schema_raises(self):
        data = copy.deepcopy(_MINIMAL_PARQUET_DATA)
        data["schema"]["TIME"]["units"] = "FILL UP MANUALLY - CHECK DOCUMENTATION"
        with self.assertRaises(pydantic.ValidationError):
            DatasetConfig.model_validate(data)

    def test_parquet_schema_transformation_missing_required_partitions_raises(self):
        """ParquetSchemaTransformation requires 'polygon' and 'timestamp' in partitioning."""
        data = copy.deepcopy(_MINIMAL_PARQUET_DATA)
        data["schema_transformation"] = {
            "partitioning": [{"source_variable": "platform_code"}]
        }
        with self.assertRaises(pydantic.ValidationError):
            DatasetConfig.model_validate(data)

    def test_zarr_var_template_shape_absent_from_schema_raises(self):
        """var_template_shape must reference a variable that exists in the schema."""
        data = copy.deepcopy(_MINIMAL_ZARR_DATA)
        data["schema_transformation"]["var_template_shape"] = "NONEXISTENT_VAR"
        with self.assertRaises(pydantic.ValidationError):
            DatasetConfig.model_validate(data)

    def test_zarr_schema_transformation_none_skips_zarr_validation(self):
        """None schema_transformation is valid for zarr and skips ZarrSchemaTransformation checks."""
        data = copy.deepcopy(_MINIMAL_ZARR_DATA)
        data["schema_transformation"] = None
        config = DatasetConfig.model_validate(data)
        self.assertIsNone(config.schema_transformation)

    def test_dataset_schema_serialises_under_schema_alias(self):
        """dataset_schema must round-trip through its 'schema' JSON alias."""
        config = DatasetConfig.model_validate(copy.deepcopy(_MINIMAL_PARQUET_DATA))
        dumped = config.model_dump(by_alias=True)
        self.assertIn("schema", dumped)
        self.assertNotIn("dataset_schema", dumped)


class TestDatasetConfigLoadFromPath(unittest.TestCase):
    """Unit tests for DatasetConfig.load_from_path class method."""

    def setUp(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def _write_json(self, filename, data):
        path = self.temp_dir / filename
        path.write_text(json.dumps(data))
        return path

    def test_load_parquet_config(self):
        path = self._write_json("parquet.json", _MINIMAL_PARQUET_DATA)
        config = DatasetConfig.load_from_path(path)
        self.assertEqual(config.dataset_name, "test_parquet_dataset")
        self.assertEqual(config.cloud_optimised_format, "parquet")

    def test_load_zarr_config(self):
        path = self._write_json("zarr.json", _MINIMAL_ZARR_DATA)
        config = DatasetConfig.load_from_path(path)
        self.assertEqual(config.dataset_name, "test_zarr_dataset")
        self.assertEqual(config.cloud_optimised_format, "zarr")

    def test_missing_file_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            DatasetConfig.load_from_path(self.temp_dir / "missing.json")

    def test_non_json_extension_raises_value_error(self):
        path = self.temp_dir / "config.yaml"
        path.write_text(json.dumps(_MINIMAL_PARQUET_DATA))
        with self.assertRaises(ValueError):
            DatasetConfig.load_from_path(path)

    def test_invalid_config_raises_validation_error(self):
        invalid_data = {
            "dataset_name": "bad",
            "cloud_optimised_format": "csv",
            "schema": {},
        }
        path = self._write_json("invalid.json", invalid_data)
        with self.assertRaises(pydantic.ValidationError):
            DatasetConfig.load_from_path(path)


class TestDatasetConfigLoadFromCloudOptimisedDirectory(unittest.TestCase):
    """Unit tests for DatasetConfig.load_from_cloud_optimised_directory class method."""

    def test_load_parquet_config(self):
        config = DatasetConfig.load_from_cloud_optimised_directory(
            "vessel_sst_delayed_qc.json"
        )
        self.assertEqual(config.cloud_optimised_format, "parquet")
        self.assertEqual(config.dataset_name, "vessel_sst_delayed_qc")

    def test_load_zarr_config(self):
        config = DatasetConfig.load_from_cloud_optimised_directory(
            "model_sea_level_anomaly_gridded_realtime.json"
        )
        self.assertEqual(config.cloud_optimised_format, "zarr")
        self.assertEqual(
            config.dataset_name, "model_sea_level_anomaly_gridded_realtime"
        )

    def test_missing_config_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            DatasetConfig.load_from_cloud_optimised_directory(
                "nonexistent_dataset.json"
            )

    def test_load_config_with_parent_merges_fields(self):
        """A child config with parent_config should inherit fields absent from the child."""
        config = DatasetConfig.load_from_cloud_optimised_directory(
            "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json"
        )
        # cloud_optimised_format lives in the parent config, not the child
        self.assertEqual(config.cloud_optimised_format, "zarr")


# Config files that intentionally contain "FILL UP MANUALLY" placeholders and
# are expected to fail DatasetConfig validation — excluded from the compat sweep.
_PLACEHOLDER_CONFIGS = {
    # dataset_template.json uses // comments and is a human-readable template,
    # not a loadable dataset config.
    "dataset_template.json",
    "aggregated_kelp_nonqc.json",
    "aggregated_seabird_nonqc.json",
    "aggregated_seagrass_nonqc.json",
    "amsa_vessel_tracking.json",
    "diver_photoquadrat_score_qc.json",
    "mooring_wave_timeseries_delayed_qc.json",
    "radar_wave_delayed_qc_no_I_J_version_main.json",
    "satellite_ghrsst_l3c_4hour_himawari8.json",
    "satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_southernocean.json",
    "satellite_nanoplankton_fraction_oc3_1day_aqua.json",
    "satellite_optical_water_type_1day_snpp.json",
    "satellite_picoplankton_fraction_oc3_1day_aqua.json",
    "satellite_sst_1day_aqua.json",
    "satellite_sst_1day_snpp.json",
    "station_wireless_sensor_network_delayec_qc.json",
}


class TestDatasetConfigBackwardCompatibility(unittest.TestCase):
    """Smoke-tests every production config file in the module's dataset config
    directory.  For each loadable config we verify that:

    1. DatasetConfig.load_from_cloud_optimised_directory succeeds (no
       regression in validators as the model evolves).
    2. The typed model agrees with the legacy load_dataset_config dict on the
       three fields that define a dataset's identity: dataset_name,
       cloud_optimised_format, and the set of schema variable names.
    """

    @classmethod
    def setUpClass(cls):
        cls.config_dir = pathlib.Path(
            aodn_cloud_optimised.config.dataset.__path__[0]
        ).absolute()
        # Collect all filenames that are referenced as a parent_config by
        # another config so we can exclude them from the sweep — parent-only
        # configs intentionally omit 'schema' (children supply it).
        cls.parent_config_filenames: set[str] = set()
        for config_path in cls.config_dir.glob("*.json"):
            try:
                config_data = json.loads(config_path.read_text())
                if isinstance(config_data, dict) and "parent_config" in config_data:
                    cls.parent_config_filenames.add(config_data["parent_config"])
            except (json.JSONDecodeError, OSError):
                pass  # malformed files handled elsewhere

    def _loadable_configs(self):
        return sorted(
            config_path
            for config_path in self.config_dir.glob("*.json")
            if config_path.name not in _PLACEHOLDER_CONFIGS
            and config_path.name not in self.parent_config_filenames
        )

    def test_all_configs_load_without_error(self):
        for config_path in self._loadable_configs():
            with self.subTest(config=config_path.name):
                DatasetConfig.load_from_cloud_optimised_directory(config_path.name)

    def test_new_loader_agrees_with_legacy_loader(self):
        """dataset_name, cloud_optimised_format, and schema keys must match
        between DatasetConfig and the legacy load_dataset_config dict."""
        for config_path in self._loadable_configs():
            with self.subTest(config=config_path.name):
                legacy = load_dataset_config(str(config_path))
                new = DatasetConfig.load_from_cloud_optimised_directory(
                    config_path.name
                ).model_dump(by_alias=True)

                # Check all keys from legacy are present in the new
                for key in legacy.keys():
                    self.assertIn(key, new)

                # Check the core values
                self.assertEqual(new["dataset_name"], legacy["dataset_name"])
                self.assertEqual(
                    new["cloud_optimised_format"], legacy["cloud_optimised_format"]
                )
                self.assertEqual(
                    set(new["schema"].keys()),
                    set(legacy["schema"].keys()),
                )


if __name__ == "__main__":
    unittest.main()
