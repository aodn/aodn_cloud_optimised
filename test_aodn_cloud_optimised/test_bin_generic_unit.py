import unittest
from unittest.mock import patch, mock_open
from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import DatasetConfig
from pydantic import ValidationError
import json


class TestDatasetConfigValidation(unittest.TestCase):
    def setUp(self):
        self.base_valid_config = {
            "dataset_name": "example_dataset",
            "run_settings": {
                "paths": [
                    {
                        "s3_uri": "s3://my-bucket/data/",
                        "filter": [],
                        "year_range": [2022],
                    }
                ],
                "cluster": {"mode": "local", "restart_every_path": False},
                "clear_existing_data": False,
                "raise_error": False,
                "suffix": ".nc",
                "bucket_raw_default_name": "",
            },
        }

    def test_valid_config_with_bucket_raw_default(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["bucket_raw_default_name"] = "my-bucket"
        self.assertIsInstance(DatasetConfig.model_validate(config), DatasetConfig)

    def test_valid_config_with_relative_s3_uri(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["paths"][0]["s3_uri"] = "IMOS/data/"
        config["run_settings"]["bucket_raw_default_name"] = "imos-bucket"
        self.assertIsInstance(DatasetConfig.model_validate(config), DatasetConfig)

    def test_invalid_mismatched_bucket_raw_default_name(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["bucket_raw_default_name"] = "other-bucket"
        with self.assertRaises(ValidationError) as ctx:
            DatasetConfig.model_validate(config)
        self.assertIn("does not match the bucket in s3_uri", str(ctx.exception))

    def test_missing_required_fields(self):
        with self.assertRaises(ValidationError):
            DatasetConfig.model_validate({})

    def test_invalid_regex_pattern(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["paths"][0]["filter"] = ["[a-z"]
        with self.assertRaises(ValidationError):
            DatasetConfig.model_validate(config)

    def test_invalid_year_range_unsorted(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["paths"][0]["year_range"] = [2023, 2020, 2021]
        with self.assertRaises(ValidationError):
            DatasetConfig.model_validate(config)

    def test_invalid_year_range_duplicate(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["paths"][0]["year_range"] = [2020, 2020]
        with self.assertRaises(ValidationError):
            DatasetConfig.model_validate(config)

    def test_optional_overrides(self):
        config = self.base_valid_config.copy()
        config["run_settings"]["optimised_bucket_name"] = "custom-bucket"
        config["run_settings"]["root_prefix_cloud_optimised_path"] = "custom/prefix"
        result = DatasetConfig.model_validate(config)
        self.assertEqual(result.run_settings.optimised_bucket_name, "custom-bucket")
        self.assertEqual(
            result.run_settings.root_prefix_cloud_optimised_path, "custom/prefix"
        )


if __name__ == "__main__":
    unittest.main()
