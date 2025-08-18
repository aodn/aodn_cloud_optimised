import json
import os
import shutil
import tempfile
import unittest

import yaml

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


if __name__ == "__main__":
    unittest.main()
