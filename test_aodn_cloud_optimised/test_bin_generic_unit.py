import unittest
from unittest.mock import patch

from pydantic import ValidationError

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
    DatasetConfig,
    PathConfig,
)


class TestDatasetConfigValidation(unittest.TestCase):
    def setUp(self):
        self.base_valid_config = {
            "dataset_name": "example_dataset",
            "cloud_optimised_format": "zarr",  # required
            "metadata_uuid": "123b1lkjh",
            "schema": {
                "TEMP": {"type": "float32"},  # minimal valid schema
            },
            "dimensions": {
                "TIME": {"name": "TIME"},  # required for Zarr
            },
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

    def test_coiled_mode_missing_options(self):
        config = self.base_valid_config.copy()
        config["cloud_optimised_format"] = "zarr"
        config["dimensions"] = {"TIME": {"name": "TIME"}}
        config["schema"] = {}
        config["run_settings"]["cluster"]["mode"] = "coiled"
        with self.assertRaises(ValidationError) as ctx:
            DatasetConfig.model_validate(config)
        self.assertIn("coiled_cluster_options must be provided", str(ctx.exception))

    def test_vars_incompatible_with_region_valid(self):
        config = self.base_valid_config.copy()
        config["schema"] = {"TEMP": {"type": "float32"}}
        config["vars_incompatible_with_region"] = ["TEMP"]
        config["cloud_optimised_format"] = "zarr"
        config["dimensions"] = {"TIME": {"name": "TIME"}}  # Required for Zarr
        self.assertIsInstance(DatasetConfig.model_validate(config), DatasetConfig)

    def test_vars_incompatible_with_region_invalid(self):
        config = self.base_valid_config.copy()
        config["schema"] = {"TEMP": {"type": "float32"}}

        config["schema_transformation"] = {}
        config["schema_transformation"]["vars_incompatible_with_region"] = [
            "WAVELENTGH"
        ]
        config["cloud_optimised_format"] = "zarr"
        config["schema_transformation"]["dimensions"] = {"TIME": {"name": "TIME"}}
        with self.assertRaises(ValidationError) as ctx:
            DatasetConfig.model_validate(config)
        # self.assertIn("not defined in schema", str(ctx.exception))


class TestPathConfigFilter(unittest.TestCase):
    """Tests for PathConfig.filter normalisation and collect_files regex behaviour.

    Regression: the pydantic validator normalises filter to a plain string, so
    the consumer must treat it as a single pattern — not iterate over it character
    by character (which was the bug).
    """

    def _make_path_config(self, filter_val):
        return PathConfig(s3_uri="IMOS/ANMN/", filter=filter_val)

    def test_filter_list_normalised_to_string(self):
        """A single-element list should be stored as a plain string."""
        p = self._make_path_config([".*\\.nc"])
        self.assertEqual(p.filter, ".*\\.nc")
        self.assertIsInstance(p.filter, str)

    def test_filter_empty_list_normalised_to_empty_string(self):
        p = self._make_path_config([])
        self.assertFalse(p.filter)

    def test_filter_none_normalised_to_empty_string(self):
        p = self._make_path_config(None)
        self.assertFalse(p.filter)

    def test_filter_string_passthrough(self):
        p = self._make_path_config(".*\\.nc")
        self.assertEqual(p.filter, ".*\\.nc")

    def test_filter_list_more_than_one_element_raises(self):
        with self.assertRaises(ValidationError):
            self._make_path_config([".*\\.nc", ".*\\.csv"])

    def test_collect_files_applies_filter_as_whole_pattern(self):
        """Regression: filter must be used as a single regex, not iterated char-by-char.

        Before the fix, '.*\\.nc' was iterated as ['.', '*', '\\', '.', 'n', 'c'],
        causing re.error: nothing to repeat at position 0 on '*'.
        """
        from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
            collect_files,
        )

        path_cfg = self._make_path_config([".*\\.nc"])
        all_files = [
            "IMOS/ANMN/data/file_20230101.nc",
            "IMOS/ANMN/data/file_20230102.nc",
            "IMOS/ANMN/data/readme.txt",
        ]
        with patch(
            "aodn_cloud_optimised.bin.generic_cloud_optimised_creation.s3_ls",
            return_value=all_files,
        ):
            result = collect_files(
                path_cfg,
                suffix=None,
                exclude=None,
                bucket_raw="imos-data",
                s3_client_opts={},
            )
        self.assertEqual(len(result), 2)
        self.assertTrue(all(f.endswith(".nc") for f in result))

    def test_collect_files_no_filter_returns_all(self):
        """Without a filter all listed files are returned."""
        from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
            collect_files,
        )

        path_cfg = self._make_path_config([])
        all_files = ["IMOS/ANMN/a.nc", "IMOS/ANMN/b.txt"]
        with patch(
            "aodn_cloud_optimised.bin.generic_cloud_optimised_creation.s3_ls",
            return_value=all_files,
        ):
            result = collect_files(
                path_cfg,
                suffix=None,
                exclude=None,
                bucket_raw="imos-data",
                s3_client_opts={},
            )
        self.assertEqual(result, all_files)


if __name__ == "__main__":
    unittest.main()
